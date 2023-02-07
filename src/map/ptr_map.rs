use std::cell::Cell;
use std::intrinsics::forget;
use std::mem::MaybeUninit;

use crate::obj_alloc::{self, Aligned, AllocGuard, Allocator};

use super::base::*;
use super::*;

pub type PtrTable<K, V, ALLOC, H> =
    Table<K, (), PtrValAttachment<K, V, ALLOC>, ALLOC, H, PTR_KV_OFFSET, PTR_KV_OFFSET>;
const ALLOC_BUFFER_SIZE: usize = 256;

pub struct PtrHashMap<
    K: Clone + Hash + Eq,
    V: Clone,
    ALLOC: GlobalAlloc + Default = System,
    H: Hasher + Default = DefaultHasher,
> {
    pub(crate) table: PtrTable<K, V, ALLOC, H>,
    allocator: Box<obj_alloc::Allocator<PtrValueNode<V>, ALLOC_BUFFER_SIZE>>,
    epoch: AtomicUsize, // Global epoch for VBR
}

#[repr(align(8))]
struct PtrValueNode<V> {
    birth_ver: AtomicUsize,
    retire_ver: AtomicUsize,
    value: Cell<V>,
}

impl<K: Clone + Hash + Eq, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default>
    PtrHashMap<K, V, ALLOC, H>
{
    const VAL_NODE_LOW_BITS: usize = PtrValAttachmentItem::<K, V>::VAL_NODE_LOW_BITS;
    const INV_VAL_NODE_LOW_BITS: usize = PtrValAttachmentItem::<K, V>::INV_VAL_NODE_LOW_BITS;

    #[inline(always)]
    fn insert_with_op(
        &self,
        op: InsertOp,
        key: K,
        value: V,
    ) -> Option<(
        (*mut V, usize),
        AllocGuard<PtrValueNode<V>, ALLOC_BUFFER_SIZE>,
    )> {
        let guard = self.allocator.pin();
        let v_num = self.ref_val(value, &guard);
        debug_assert_ne!(v_num | VAL_MUTEX_BIT, v_num);
        debug_assert_eq!(v_num & WORD_MUTEX_DATA_BIT_MASK, v_num);
        self.table
            .insert(op, &key, Some(&()), 0 as FKey, v_num as FVal)
            .map(|(fv, _)| (self.ptr_of_val(fv), guard))
    }

    pub(crate) fn now_epoch(&self, key: &K) -> usize {
        self.table.now_epoch(&key, 0)
    }

    pub fn lock(&self, key: &K) -> Option<PtrMutexGuard<K, V, ALLOC, H>> {
        PtrMutexGuard::new(&self, key)
    }

    pub fn locked_with_upsert(
        &self,
        key: &K,
        value: V,
    ) -> Result<(PtrMutexGuard<K, V, ALLOC, H>, V), PtrMutexGuard<K, V, ALLOC, H>> {
        loop {
            match self.lock(key) {
                Some(mut guard) => {
                    let old_value = mem::replace(&mut *guard, value);
                    return Ok((guard, old_value));
                }
                None => {
                    let new_guard = PtrMutexGuard::create(&self, key, value.clone());
                    if let Some(guard) = new_guard {
                        return Err(guard);
                    }
                }
            }
        }
    }

    #[inline(always)]
    fn ref_val(&self, d: V, guard: &AllocGuard<PtrValueNode<V>, ALLOC_BUFFER_SIZE>) -> usize {
        unsafe {
            let node_ptr = guard.alloc();
            let node_ref = &*node_ptr;
            let node_ver = node_ref.retire_ver.load(Relaxed);
            let mut current_ver = self.epoch.load(Relaxed);
            debug_assert_ne!(
                node_ptr as usize & Self::VAL_NODE_LOW_BITS,
                node_ptr as usize
            );
            loop {
                if node_ver >= current_ver {
                    // Bump global apoch when the reclaimed node version is higher than current version
                    let new_ver = current_ver + 1;
                    if let Err(actual_ver) =
                        self.epoch
                            .compare_exchange(current_ver, new_ver, AcqRel, Relaxed)
                    {
                        if actual_ver >= new_ver {
                            current_ver = actual_ver;
                            continue;
                        }
                    } else {
                        current_ver = new_ver;
                        break;
                    }
                }
                break;
            }
            debug_assert!(
                node_ver < current_ver,
                "Version node {} vs. current {}",
                node_ver,
                current_ver
            );
            node_ref.birth_ver.store(current_ver, Relaxed);
            node_ref.retire_ver.store(0, Release);
            let obj_ptr = node_ref.value.as_ptr();
            if node_ver > 0 {
                // Free existing object
                drop(ptr::read(obj_ptr));
            }
            ptr::write(obj_ptr, d);
            Self::compose_value(node_ptr as usize, current_ver)
        }
    }

    #[inline(always)]
    pub fn deref_val(&self, val: usize) -> Option<V> {
        unsafe {
            let pre_ver = self.epoch.load(Relaxed);
            let (addr, val_ver) = decompose_value::<K, V>(val);
            let node_ptr = addr as *mut PtrValueNode<V>;
            let node_ref = &*node_ptr;
            fence(Acquire); // Acquire: We want to get the version AFTER we read the value and other thread may changed the version in the process
            let node_ver = node_ref.birth_ver.load(Relaxed) & Self::VAL_NODE_LOW_BITS;
            if node_ver != val_ver || // checking node version for consistency
                self.epoch.load(Acquire) != pre_ver
            // Checking on epoch changing to avoid ABA and other problems
            {
                return None;
            }
            return Some(node_ref.value.as_ptr().as_ref().unwrap().clone());
        }
    }

    #[inline(always)]
    fn ptr_of_val(&self, val: usize) -> (*mut V, usize) {
        unsafe {
            let (addr, _val_ver) = decompose_value::<K, V>(val);
            let node_ptr = addr as *mut PtrValueNode<V>;
            let node_ref = &*node_ptr;
            let val_ptr = node_ref.value.as_ptr();
            debug_assert!(!node_ptr.is_null());
            debug_assert!(!val_ptr.is_null());
            (val_ptr, addr)
        }
    }

    #[inline(always)]
    fn compose_value(ptr: usize, ver: usize) -> usize {
        let ptr_part = ptr & Self::INV_VAL_NODE_LOW_BITS;
        let ver_part = ver & Self::VAL_NODE_LOW_BITS;
        let val = ptr_part | ver_part;
        val & WORD_MUTEX_DATA_BIT_MASK
    }

    fn free_node(&self, node_addr: usize, guard: AllocGuard<PtrValueNode<V>, ALLOC_BUFFER_SIZE>) {
        debug_assert_eq!(
            node_addr,
            node_addr
                & WORD_MUTEX_DATA_BIT_MASK
                & PtrValAttachmentItem::<K, V>::INV_VAL_NODE_LOW_BITS
        );
        let node_ptr = node_addr as *const PtrValueNode<V>;
        let node_ref = unsafe { node_ptr.as_ref().unwrap() };
        node_ref.retire_ver.store(self.epoch.load(Acquire), Relaxed);
        guard.buffered_free(node_ptr);
    }
}

#[inline(always)]
fn decompose_value<K: Clone + Hash + Eq, V: Clone>(value: usize) -> (usize, usize) {
    let value = value & WORD_MUTEX_DATA_BIT_MASK;
    (
        value & PtrValAttachmentItem::<K, V>::INV_VAL_NODE_LOW_BITS,
        value & PtrValAttachmentItem::<K, V>::VAL_NODE_LOW_BITS,
    )
}

impl<K: Clone + Hash + Eq, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Map<K, V>
    for PtrHashMap<K, V, ALLOC, H>
{
    fn with_capacity(cap: usize) -> Self {
        let alloc = Box::new(obj_alloc::Allocator::new());
        Self {
            table: PtrTable::with_capacity(cap, ()),
            allocator: alloc,
            epoch: AtomicUsize::new(1),
        }
    }

    #[inline(always)]
    fn get(&self, key: &K) -> Option<V> {
        let (fkey, hash) = self.table.get_hash(0, key);
        let guard = crossbeam_epoch::pin();
        let backoff = crossbeam_utils::Backoff::new();
        loop {
            if let Some((fv, _, _addr)) = self
                .table
                .get_with_hash(key, fkey, hash, false, &guard, &backoff)
            {
                if fv | VAL_MUTEX_BIT == fv {
                    // If the value is locked, spin
                } else if let Some(val) = self.deref_val(fv) {
                    return Some(val);
                }
                backoff.spin();
                // None would be value changed
            } else {
                return None;
            }
        }
    }

    #[inline(always)]
    fn insert(&self, key: K, value: V) -> Option<V> {
        self.insert_with_op(InsertOp::Insert, key, value)
            .map(|((ptr, node_addr), guard)| unsafe {
                debug_assert!(!ptr.is_null());
                let val = (*ptr).clone();
                self.free_node(node_addr, guard);
                val
            })
    }

    #[inline(always)]
    fn try_insert(&self, key: K, value: V) -> Option<V> {
        self.insert_with_op(InsertOp::TryInsert, key, value)
            .map(|((ptr, _), _)| unsafe { (*ptr).clone() })
    }

    #[inline(always)]
    fn remove(&self, key: &K) -> Option<V> {
        self.table.remove(key, 0).map(|(fv, _)| {
            let (ptr, node_addr) = self.ptr_of_val(fv);
            let guard = self.allocator.pin();
            unsafe {
                let val = (*ptr).clone();
                self.free_node(node_addr, guard);
                val
            }
        })
    }

    #[inline(always)]
    fn entries(&self) -> Vec<(K, V)> {
        self.table
            .entries()
            .into_iter()
            .filter_map(|(_, fv, k, _)| {
                // TODO: reload?
                self.deref_val(fv).map(|v| (k, v))
            })
            .collect()
    }

    #[inline(always)]
    fn contains_key(&self, key: &K) -> bool {
        self.table.get(key, 0, false).is_some()
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.table.len()
    }

    #[inline(always)]
    fn clear(&self) {
        self.table.clear();
    }
}

unsafe impl<K: Clone + Hash + Eq, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Send
    for PtrHashMap<K, V, ALLOC, H>
{
}
unsafe impl<K: Clone + Hash + Eq, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Sync
    for PtrHashMap<K, V, ALLOC, H>
{
}

#[derive(Clone)]
pub struct PtrValAttachment<K: Clone + Hash + Eq, V: Clone, A: GlobalAlloc + Default> {
    key_chunk: usize,
    _marker: PhantomData<(K, V, A)>,
}

#[derive(Clone)]
pub struct PtrValAttachmentItem<K, V> {
    addr: usize,
    _marker: PhantomData<(K, V)>,
}

impl<K: Clone + Hash + Eq, V: Clone, A: GlobalAlloc + Default> PtrValAttachment<K, V, A> {
    const KEY_SIZE: usize = mem::size_of::<Aligned<K>>();

    #[inline(always)]
    fn addr_by_index(&self, index: usize) -> usize {
        self.key_chunk + index * Self::KEY_SIZE
    }
}

impl<K: Clone + Hash + Eq, V: Clone, A: GlobalAlloc + Default> Attachment<K, ()>
    for PtrValAttachment<K, V, A>
{
    type Item = PtrValAttachmentItem<K, V>;
    type InitMeta = ();

    fn heap_entry_size() -> usize {
        Self::KEY_SIZE // only keys on the heap
    }

    fn new(heap_ptr: usize, _meta: &Self::InitMeta) -> Self {
        Self {
            key_chunk: heap_ptr,
            _marker: PhantomData,
        }
    }

    #[inline(always)]
    fn prefetch(&self, index: usize) -> Self::Item {
        let addr = self.addr_by_index(index);
        // unsafe {
        //     intrinsics::prefetch_read_data(addr as *const K, 3);
        // }
        PtrValAttachmentItem {
            addr,
            _marker: PhantomData,
        }
    }
}

impl<K: Clone + Hash + Eq, V: Clone> PtrValAttachmentItem<K, V> {
    const VAL_NODE_ALIGN: usize = mem::align_of::<PtrValueNode<V>>();
    const VAL_NODE_LOW_BITS: usize = (1 << Self::VAL_NODE_ALIGN.trailing_zeros()) - 1;
    const INV_VAL_NODE_LOW_BITS: usize = !Self::VAL_NODE_LOW_BITS;
}

impl<K: Clone + Hash + Eq, V: Clone> AttachmentItem<K, ()> for PtrValAttachmentItem<K, V> {
    fn get_key(self) -> K {
        let addr = self.addr;
        fence(Acquire);
        unsafe { (*(addr as *mut K)).clone() }
    }

    fn get_value(self) -> () {}

    fn set_key(self, key: K) {
        let addr = self.addr;
        unsafe { ptr::write_volatile(addr as *mut K, key) }
        fence(Release);
    }

    fn set_value(self, _value: (), _old_fval: FVal) {
        // self.erase(old_fval)
    }

    fn erase_value(self, _old_fval: FVal) {}

    fn probe(self, probe_key: &K) -> bool {
        let key = unsafe { &*(self.addr as *mut K) };
        key == probe_key
    }

    fn prep_write(self) {}

    fn moveout_key(self) -> K {
        let addr = self.addr;
        trace!("Erasing key with addr {}", addr);
        unsafe { ptr::read(addr as *mut K) }
    }
}

impl<K: Clone, V: Clone> Copy for PtrValAttachmentItem<K, V> {}

pub struct PtrMutexGuard<
    'a,
    K: Clone + Hash + Eq,
    V: Clone,
    ALLOC: GlobalAlloc + Default,
    H: Hasher + Default,
> {
    map: &'a PtrHashMap<K, V, ALLOC, H>,
    key: K,
    value: V,
}

impl<'a, K: Clone + Hash + Eq, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default>
    PtrMutexGuard<'a, K, V, ALLOC, H>
{
    fn new(map: &'a PtrHashMap<K, V, ALLOC, H>, key: &K) -> Option<Self> {
        let backoff = crossbeam_utils::Backoff::new();
        let guard = crossbeam_epoch::pin();
        let (fkey, hash) = map.table.get_hash(0, key);
        let value;
        loop {
            let swap_res = map.table.swap_with_hash(
                fkey,
                hash,
                key,
                move |fast_value| {
                    let locked_val = fast_value | VAL_MUTEX_BIT;
                    if fast_value == locked_val {
                        // Locked, unchanged
                        None
                    } else {
                        // Obtain lock
                        Some(locked_val)
                    }
                },
                &guard,
            );
            match swap_res {
                SwapResult::Succeed(val, _idx, _chunk) => {
                    loop {
                        match map.deref_val(val) {
                            Some(v) => {
                                value = v;
                                break;
                            }
                            None => {
                                backoff.spin();
                            }
                        };
                    }
                    break;
                }
                SwapResult::Aborted(fval) => {
                    //let val = map.deref_val(fval);
                    // debug!("Locking key {:?} failed, value {:?}", key, val);
                    backoff.spin();
                    continue;
                }
                SwapResult::Failed => {
                    backoff.spin();
                    continue;
                }
                SwapResult::NotFound => {
                    return None;
                }
            }
        }
        let key = key.clone();
        Some(Self { map, key, value })
    }

    fn create(map: &'a PtrHashMap<K, V, ALLOC, H>, key: &K, value: V) -> Option<Self> {
        let guard = map.allocator.pin();
        let fvalue = map.ref_val(value.clone(), &guard);
        match map.table.insert(
            InsertOp::TryInsert,
            key,
            Some(&()),
            0,
            fvalue | VAL_MUTEX_BIT,
        ) {
            None | Some((TOMBSTONE_VALUE, ())) | Some((EMPTY_VALUE, ())) => Some(Self {
                map,
                key: key.clone(),
                value,
            }),
            _ => None,
        }
    }

    pub fn remove(mut self) -> V {
        let (key, value) = unsafe {
            (
                mem::replace(&mut self.key, MaybeUninit::zeroed().assume_init()),
                mem::replace(&mut self.value, MaybeUninit::zeroed().assume_init()),
            )
        };
        let guard = self.map.allocator.pin();
        let fval = self.map.table.remove(&key, 0).unwrap().0;
        let (val_ptr, node_addr) = self.map.ptr_of_val(fval);
        let r = unsafe { (*val_ptr).clone() };
        self.map.free_node(node_addr, guard);
        forget(self);
        drop(value);
        return r;
    }
}
impl<'a, K: Clone + Hash + Eq, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Deref
    for PtrMutexGuard<'a, K, V, ALLOC, H>
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a, K: Clone + Hash + Eq, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> DerefMut
    for PtrMutexGuard<'a, K, V, ALLOC, H>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}
impl<'a, K: Clone + Hash + Eq, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Drop
    for PtrMutexGuard<'a, K, V, ALLOC, H>
{
    fn drop(&mut self) {
        let guard = self.map.allocator.pin();
        let fval = self.map.ref_val(self.value.clone(), &guard);
        debug_assert_ne!(fval | VAL_MUTEX_BIT, fval); // Assert not locked
        if let Some((fv, _)) =
            self.map
                .table
                .insert(InsertOp::Insert, &self.key, Some(&()), 0, fval)
        {
            let (val_ptr, node_addr) = self.map.ptr_of_val(fv);
            // debug!(
            //     "Mutex key {:?} switched from {:?} to {:?}",
            //     self.key,
            //     unsafe { &*val_ptr },
            //     self.value
            // );
            self.map.free_node(node_addr, guard);
        }
    }
}

impl<K: Clone + Hash + Eq, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Drop
    for PtrHashMap<K, V, ALLOC, H>
{
    fn drop(&mut self) {
        for (_fk, fv, _k, _v) in self.table.entries() {
            let (val_ptr, _) = self.ptr_of_val(fv);
            unsafe {
                ptr::read(val_ptr);
            }
        }
        for node_ptr in self.allocator.all_freed_ptr() {
            // Here we need to drop all objects in free lists
            unsafe { ptr::drop_in_place(node_ptr) };
        }
    }
}

#[cfg(test)]
pub mod tests {
    use std::panic;
    use std::process;
    use std::sync::atomic::*;
    use test::Bencher;

    use crate::{
        map::{
            base::{get_delayed_log, InsertOp, WORD_MUTEX_DATA_BIT_MASK},
            *,
        },
        tests_misc::*,
    };
    use std::{alloc::System, sync::Arc, thread};

    macro_rules! ptr_map_tests {
        (
            $name: ident,
            $mty: ty,
            $kty: ty,
            $vty: ty,
            $minit: block,
            $kinit: block,
            $vinit: block
        ) => {
            mod $name {
                use super::*;

                pub type Key = $kty;
                pub type Value = $vty;

                fn key_from(num: usize) -> Key {
                    ($kinit)(num)
                }

                fn val_from(key: &Key, num: usize) -> Value {
                    ($vinit)(key, num)
                }

                fn map_init(cap: usize) -> $mty {
                    ($minit)(cap)
                }

                #[test]
                fn no_resize() {
                    let _ = env_logger::try_init();
                    let map = map_init(4096);
                    for i in 5..2048 {
                        let k = key_from(i);
                        let v = val_from(&k, i * 2);
                        map.insert(k, v);
                    }
                    for i in 5..2048 {
                        let k = key_from(i);
                        let v = val_from(&k, i * 2);
                        match map.get(&k) {
                            Some(r) => assert_eq!(r, v),
                            None => panic!("{}", i),
                        }
                    }
                }

                #[test]
                fn resize() {
                    let _ = env_logger::try_init();
                    let map = map_init(16);
                    for i in 5..2048 {
                        let k = key_from(i);
                        let v = val_from(&k, i * 2);
                        map.insert(k, v);
                    }
                    for i in 5..2048 {
                        let k = key_from(i);
                        let v = val_from(&k, i * 2);
                        match map.get(&k) {
                            Some(r) => assert_eq!(r, v),
                            None => panic!("{}", i),
                        }
                    }
                }

                #[test]
                fn parallel_no_resize() {
                    let _ = env_logger::try_init();
                    let map = Arc::new(map_init(65536));
                    let mut threads = vec![];
                    for i in 5..99 {
                        let k = key_from(i);
                        let v = val_from(&k, i * 10);
                        map.insert(k, v);
                    }
                    for i in 100..900 {
                        let map = map.clone();
                        threads.push(thread::spawn(move || {
                            for j in 5..60 {
                                let k = key_from(i * 100 + j);
                                let v = val_from(&k, i * j);
                                map.insert(k, v);
                            }
                        }));
                    }
                    for i in 5..9 {
                        for j in 1..10 {
                            let k = key_from(i * j);
                            map.remove(&k);
                        }
                    }
                    for thread in threads {
                        let _ = thread.join();
                    }
                    for i in 100..900 {
                        for j in 5..60 {
                            let k = key_from(i * 100 + j);
                            let v = val_from(&k, i * j);
                            assert_eq!(map.get(&k), Some(v))
                        }
                    }
                    for i in 5..9 {
                        for j in 1..10 {
                            let k = key_from(i * j);
                            assert!(map.get(&k).is_none())
                        }
                    }
                }

                #[test]
                fn exaust_versions() {
                    let map = map_init(16);
                    for i in 0..255 {
                        let key = key_from(1);
                        let val = val_from(&key, i);
                        map.insert(key.clone(), val.clone());
                        debug_assert_eq!(map.get(&key).unwrap(), val);
                    }
                    for i in 255..2096 {
                        let key = key_from(1);
                        let val = val_from(&key, i);
                        map.insert(key.clone(), val.clone());
                        debug_assert_eq!(map.get(&key).unwrap(), val);
                    }
                }

                #[test]
                fn parallel_with_resize() {
                    let _ = env_logger::try_init();
                    let num_threads = num_cpus::get();
                    let test_load = 1024;
                    let repeat_load = 16;
                    let map = Arc::new(map_init(32));
                    let mut threads = vec![];
                    for i in 0..num_threads {
                        let map = map.clone();
                        threads.push(thread::spawn(move || {
                          for j in 5..test_load {
                              let key = key_from(i * 10000000 + j);
                              let value_prefix = i * j * 100;
                              for k in 1..repeat_load {
                                  let value_num = value_prefix + k;
                                  if k != 1 {
                                      assert_eq!(map.get(&key), Some(val_from(&key, value_num - 1)));
                                  }
                                  let value = val_from(&key, value_num);
                                  let pre_insert_epoch = map.now_epoch(&key);
                                  map.insert(key.clone(), value.clone());
                                  let post_insert_epoch = map.now_epoch(&key);
                                  for l in 1..128 {
                                      let pre_fail_get_epoch = map.now_epoch(&key);
                                      let left = map.get(&key);
                                      let post_fail_get_epoch = map.now_epoch(&key);
                                      let right = Some(&value);
                                      if left.as_ref() != right {
                                          error!("Discovered mismatch key {:?}, analyzing", &key);
                                          let error_checking = || {
                                            for m in 1..1024 {
                                                let mleft = map.get(&key);
                                                let mright = Some(&value);
                                                if mleft.as_ref() == mright {
                                                    panic!(
                                                        "Recovered at turn {} for {:?}, epoch {} to {}, now {}, PIE: {} to {}. Expecting {:?} got {:?}. Migration problem!!!",
                                                        m,
                                                        &key,
                                                        pre_fail_get_epoch,
                                                        post_fail_get_epoch,
                                                        map.now_epoch(&key),
                                                        pre_insert_epoch,
                                                        post_insert_epoch,
                                                        right, left
                                                     );
                                                    // panic!("Late value change on {:?}", key);
                                                }
                                            }
                                          };
                                          error_checking();
                                          panic!("Unable to recover for {:?}, round {}. Expecting {:?} got {:?}.", &key, l , right, left);
                                          // panic!("Unrecoverable value change for {:?}", key);
                                      }
                                  }
                                  if j % 8 == 0 {
                                    let pre_rm_epoch = map.now_epoch(&key);
                                    assert_eq!(
                                        map.remove(&key).as_ref(),
                                        Some(&value),
                                        "Remove result, get {:?}, round {}",
                                        map.get(&key),
                                        k
                                    );
                                    let post_rm_epoch = map.now_epoch(&key);
                                    assert_eq!(map.get(&key), None, "Remove recursion, value was {:?}. Epoch pre {}, post {}, get {}, last logs {:?}", value, pre_rm_epoch, post_rm_epoch, map.now_epoch(&key), get_delayed_log(4));
                                    assert!(map.lock(&key).is_none(), "Remove recursion with lock");
                                    map.insert(key.clone(), value.clone());
                                  }
                                  if j % 4 == 0 {
                                      let updating = || {
                                        let new_value = val_from(&key, value_num + 7);
                                        let pre_insert_epoch = map.now_epoch(&key);
                                        map.insert(key.clone(), new_value.clone());
                                        let post_insert_epoch = map.now_epoch(&key);
                                        assert_eq!(
                                            map.get(&key).as_ref(),
                                            Some(&new_value),
                                            "Checking immediate update, key {:?}, epoch {} to {}",
                                            key, pre_insert_epoch, post_insert_epoch
                                        );
                                        map.insert(key.clone(), value.clone());
                                      };
                                      updating();
                                  }
                              }
                          }
                      }));
                    }
                    info!("Waiting for intensive insertion to finish");
                    for thread in threads {
                        let _ = thread.join();
                    }
                    info!("Checking final value");
                    (0..num_threads).for_each(|i| {
                        for j in 5..test_load {
                            let k_num = i * 10000000 + j;
                            let v_num = i * j * 100 + repeat_load - 1;
                            let k = key_from(k_num);
                            let v = val_from(&k, v_num);
                            let get_res = map.get(&k);
                            assert_eq!(
                                Some(v),
                                get_res,
                                "Final val mismatch. k {:?}, i {}, j {}, epoch {}",
                                k,
                                i,
                                j,
                                map.now_epoch(&k)
                            );
                        }
                    });
                }

                #[test]
                fn ptr_checking_inserion_with_migrations() {
                    let _ = env_logger::try_init();
                    hook_panic();
                    let repeats: usize = 20480;
                    let map = Arc::new(map_init(8));
                    let mut threads = vec![];
                    for i in 1..64 {
                        let map = map.clone();
                        threads.push(thread::spawn(move || {
                            for j in 0..repeats {
                                let n = i * 100000 + j;
                                let key = key_from(n);
                                let value = val_from(&key, n);
                                let prev_epoch = map.now_epoch(&key);
                                assert_eq!(map.insert(key.clone(), value.clone()), None, "inserting at key {}", key);
                                let post_insert_epoch = map.now_epoch(&key);
                                {
                                    let get_test_res = map.get(&key);
                                    let get_epoch = map.now_epoch(&key);
                                    let expecting = Some(value.clone());
                                    if get_test_res != expecting {
                                        let all_pairs = map.entries().into_iter().collect::<std::collections::HashMap<_, _>>();
                                        panic!(
                                            "Value mismatch {:?} expecting {:?}. Reading after insertion at key {}, epoch {}/{}/{}, last log {:?}. Dumped containing {:?}",
                                            get_test_res, expecting,
                                            key, get_epoch, post_insert_epoch, prev_epoch,
                                            get_delayed_log(4),
                                            all_pairs.get(&key)
                                        );
                                    }
                                }
                                let post_insert_epoch = map.now_epoch(&key);
                                assert_eq!(
                                    map.insert(key.clone(), value.clone()),
                                    Some(value.clone()),
                                    "reinserting at key {}, get {:?}, epoch {}/{}/{}, last log {:?}, i {}",
                                    key,
                                    map.get(&key),
                                    map.now_epoch(&key),
                                    post_insert_epoch,
                                    prev_epoch,
                                    get_delayed_log(3), i
                                );
                            }
                            for j in 0..repeats {
                                let n = i * 100000 + j;
                                let key = key_from(n);
                                let value = val_from(&key, n);
                                assert_eq!(
                                    map.insert(key.clone(), value.clone()),
                                    Some(value),
                                    "reinserting at key {}, get {:?}, epoch {}, last log {:?}, i {}",
                                    key,
                                    map.get(&key),
                                    map.now_epoch(&key),
                                    get_delayed_log(3), i
                                );
                            }
                            for j in 0..repeats {
                                let n = i * 100000 + j;
                                let key = key_from(n);
                                let value = val_from(&key, n);
                                assert_eq!(
                                    map.get(&key),
                                    Some(value),
                                    "reading at key {}, epoch {}",
                                    key,
                                    map.now_epoch(&key)
                                );
                            }
                        }));
                    }
                    assert_all_thread_passed(threads);
                }
            }
        };
    }

    ptr_map_tests!(
        usize_test,
        PtrHashMap<usize, usize>,
        usize, usize,
        {
            |cap| PtrHashMap::with_capacity(cap)
        },
        {
            |k| k as usize
        }, {
            |_k, v| v as usize
        }
    );

    ptr_map_tests!(
        string_val_test,
        PtrHashMap<usize, Vec<char>>,
        usize, Vec<char>,
        {
            |cap| PtrHashMap::with_capacity(cap)
        },
        {
            |k| k as usize
        }, {
            |k, v| {
                let str = format!("{:>32}{:>32}", k, v);
                let mut res = ['0'; 64];
                for (i, c) in str.chars().enumerate() {
                    res[i] = c;
                }
                Vec::from(res)
            }
        }
    );

    ptr_map_tests!(
        string_key_val_test,
        PtrHashMap<String, Vec<char>>,
        String, Vec<char>,
        {
            |cap| PtrHashMap::with_capacity(cap)
        },
        {
            |k| format!("{}", k)
        }, {
            |k, v| {
                let str = format!("{:>32}{:>32}", k, v);
                let mut res = ['0'; 64];
                for (i, c) in str.chars().enumerate() {
                    res[i] = c;
                }
                Vec::from(res)
            }
        }
    );

    #[test]
    fn mutex_single_key() {
        let _ = env_logger::try_init();
        let map = Arc::new(PtrHashMap::<usize, usize, System>::with_capacity(8));
        let key = 10;
        let num_threads = 256;
        let num_rounds = 1024;
        let mut threads = vec![];
        assert_eq!(map.insert(key, 0), None);
        for _ in 0..num_threads {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                for _ in 0..num_rounds {
                    let mut mutex = map.lock(&key).unwrap();
                    *mutex += 1;
                }
            }));
        }
        assert_all_thread_passed(threads);
        assert_eq!(map.get(&key), Some(num_threads * num_rounds));
    }

    #[test]
    fn mutex_upsert_single_key() {
        let _ = env_logger::try_init();
        let map = Arc::new(PtrHashMap::<usize, usize, System>::with_capacity(8));
        let key = 10;
        let num_threads = 256;
        let num_rounds = 1024;
        let mut threads = vec![];
        {
            let mutex = map.locked_with_upsert(&key, 0).err().unwrap();
            assert_eq!(*mutex, 0);
        }
        for _ in 0..num_threads {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                for _ in 0..num_rounds {
                    let mut mutex = map.lock(&key).unwrap();
                    *mutex += 1;
                }
            }));
        }
        assert_all_thread_passed(threads);
        assert_eq!(map.get(&key), Some(num_threads * num_rounds));
    }

    #[bench]
    fn resizing_before(b: &mut Bencher) {
        let _ = env_logger::try_init();
        let map = Arc::new(PtrHashMap::<usize, usize, System>::with_capacity(65536));
        let mut i = 5;
        b.iter(|| {
            map.insert(i, i);
            i += 1;
        });
    }

    #[bench]
    fn resizing_after(b: &mut Bencher) {
        let _ = env_logger::try_init();
        let prefill = 1000000;
        let map = Arc::new(PtrHashMap::<usize, usize, System>::with_capacity(8));
        for i in 0..prefill {
            map.insert(i, i);
        }
        let mut i = prefill;
        b.iter(|| {
            map.insert(i, i);
            i += 1;
        });
    }

    #[bench]
    fn resizing_with(b: &mut Bencher) {
        let _ = env_logger::try_init();
        let map = Arc::new(PtrHashMap::<usize, usize, System>::with_capacity(2));
        let mut i = 5;
        b.iter(|| {
            map.insert(i, i);
            i += 1;
        });
    }

    #[test]
    fn parallel_ptr_map_multi_mutex() {
        let _ = env_logger::try_init();
        let map = Arc::new(PtrHashMap::<usize, usize, System>::with_capacity(16));
        let mut threads = vec![];
        let num_threads = 16;
        let test_load = 4096;
        let update_load = 128;
        for thread_id in 0..num_threads {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                let target = thread_id;
                for i in 0..test_load {
                    let key = target * 1000000 + i;
                    {
                        let mut mutex = map.locked_with_upsert(&key, 0).err().unwrap();
                        *mutex = 1;
                        drop(mutex);
                    }
                    for j in 1..update_load {
                        assert!(
                            map.get(&key).is_some(),
                            "Pre getting value for mutex, key {}, epoch {}",
                            key,
                            map.now_epoch(&key)
                        );
                        let val = {
                            let mut mutex = map.lock(&key).expect(&format!(
                                "Locking key {}, epoch {}",
                                key,
                                map.now_epoch(&key),
                            ));
                            assert_eq!(*mutex, j);
                            *mutex += 1;
                            let num = *mutex;
                            drop(mutex);
                            num
                        };
                        assert!(
                            map.get(&key).is_some(),
                            "Post getting value for mutex, key {}, epoch {}",
                            key,
                            map.now_epoch(&key)
                        );
                        if j % 7 == 0 {
                            {
                                let mutex = map.lock(&key).expect(&format!(
                                    "Remove locking key {}, epoch {}",
                                    key,
                                    map.now_epoch(&key)
                                ));
                                mutex.remove();
                            }
                            assert!(map.lock(&key).is_none());
                            *map.locked_with_upsert(&key, 0).err().unwrap() = val;
                        }
                    }
                    assert_eq!(*map.lock(&key).unwrap(), update_load);
                }
            }));
        }
        assert_all_thread_passed(threads);
    }

    #[test]
    fn parallel_ptr_map_multi_mutex_2() {
        let _ = env_logger::try_init();
        let map = Arc::new(PtrHashMap::<usize, usize, System>::with_capacity(16));
        let mut threads = vec![];
        let num_threads = 2;
        let test_load = 4096;
        for thread_id in 0..num_threads {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                for i in 0..test_load {
                    let key = thread_id * 1000000 + i;
                    assert!(map.insert(key, key).is_none());
                    assert_eq!(
                        map.get(&key),
                        Some(key),
                        "Pre getting value for mutex, key {}, epoch {}",
                        key,
                        map.now_epoch(&key)
                    );
                    let get_fval = map.table.get(&key, 0, false).unwrap().0;
                    assert_eq!(get_fval, get_fval & WORD_MUTEX_DATA_BIT_MASK);
                    let val = {
                        let mut mutex = map.lock(&key).expect(&format!(
                            "Locking key {}, epoch {}",
                            key,
                            map.now_epoch(&key),
                        ));
                        assert_eq!(*mutex, key);
                        *mutex = key * 1000;
                        let num = *mutex;
                        drop(mutex);
                        num
                    };
                    assert_eq!(
                        map.get(&key),
                        Some(val),
                        "Post getting value for mutex, key {}, epoch {}",
                        key,
                        map.now_epoch(&key)
                    );
                    //assert_eq!(*map.lock(&key).unwrap(), val);
                }
            }));
        }
        assert_all_thread_passed(threads);
    }
}
