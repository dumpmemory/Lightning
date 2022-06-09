use std::cell::Cell;

use crate::obj_alloc::{self, Aligned, AllocGuard, Allocator};

use super::base::*;
use super::*;

pub type PtrTable<K, V, ALLOC, H> = Table<K, (), PtrValAttachment<K, V, ALLOC>, ALLOC, H>;
const ALLOC_BUFFER_SIZE: usize = 256;

pub struct PtrHashMap<
    K: Clone + Hash + Eq,
    V: Clone,
    ALLOC: GlobalAlloc + Default = System,
    H: Hasher + Default = DefaultHasher,
> {
    pub(crate) table: PtrTable<K, V, ALLOC, H>,
    allocator: Box<obj_alloc::Allocator<PtrValueNode<V>, ALLOC_BUFFER_SIZE>>,
    shadow: PhantomData<(K, V, H)>,
}

#[repr(align(8))]
struct PtrValueNode<V> {
    value: Cell<V>,
    ver: AtomicUsize,
}

struct MapConsts<K, V> {
    _marker: PhantomData<(K, V)>,
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
        self.table
            .insert(op, &key, Some(&()), 0 as FKey, v_num as FVal)
            .map(|(fv, _)| (self.ptr_of_val(fv), guard))
    }

    pub fn lock(&self, key: &K) -> Option<PtrMutexGuard<K, V, ALLOC, H>> {
        PtrMutexGuard::new(&self, key)
    }

    pub fn insert_locked(&self, key: &K, value: V) -> Option<PtrMutexGuard<K, V, ALLOC, H>> {
        PtrMutexGuard::create(&self, key, value)
    }

    #[inline(always)]
    fn ref_val(&self, d: V, guard: &AllocGuard<PtrValueNode<V>, ALLOC_BUFFER_SIZE>) -> usize {
        unsafe {
            let node_ptr = guard.alloc();
            let node_ref = &*node_ptr;
            // Release: No other thread is changing the version, but we want the value assignment happened AFTER the version is changed
            let next_ver = node_ref.ver.fetch_add(1, Release).wrapping_add(1);
            ptr::write(node_ref.value.as_ptr(), d);
            Self::compose_value(node_ptr as usize, next_ver)
        }
    }

    #[inline(always)]
    fn deref_val<T: Clone>(&self, val: usize) -> Option<T> {
        unsafe {
            let (addr, val_ver) = decompose_value::<K, V>(val);
            let node_ptr = addr as *mut PtrValueNode<T>;
            let node_ref = &*node_ptr;
            let val_ptr = node_ref.value.as_ptr();
            let v_shadow = ptr::read(val_ptr); // Use a shadow data to cope with impl Clone data types
            fence(Acquire); // Acquire: We want to get the version AFTER we read the value and other thread may changed the version in the process
            let ver_ptr = node_ref.ver.as_mut_ptr();
            let node_ver = *ver_ptr & Self::VAL_NODE_LOW_BITS;
            if node_ver != val_ver {
                return None;
            }
            let v = v_shadow.clone();
            mem::forget(v_shadow);
            Some(v)
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
        val
    }

    #[inline(never)]
    fn retry_get(&self, fv: &mut usize, addr: usize, backoff: &Backoff) -> Option<V> {
        backoff.spin();
        loop {
            let new_fval = PtrTable::<K, V, ALLOC, H>::get_fast_value(addr);
            if new_fval.val > NUM_FIX_V {
                let now_val = self.deref_val::<V>(*fv);
                if now_val.is_some() {
                    return now_val;
                } else {
                    *fv = new_fval.val;
                    backoff.spin();
                    continue;
                }
            }
            return None;
        }
    }
}

#[inline(always)]
fn decompose_value<K: Clone + Hash + Eq, V: Clone>(value: usize) -> (usize, usize) {
    (
        value & PtrValAttachmentItem::<K, V>::INV_VAL_NODE_LOW_BITS,
        value & PtrValAttachmentItem::<K, V>::VAL_NODE_LOW_BITS,
    )
}

impl<K: Clone + Hash + Eq, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Map<K, V>
    for PtrHashMap<K, V, ALLOC, H>
{
    fn with_capacity(cap: usize) -> Self {
        let mut alloc = Box::new(obj_alloc::Allocator::new());
        let alloc_ptr: *mut Allocator<PtrValueNode<V>, ALLOC_BUFFER_SIZE> = &mut *alloc.as_mut();
        let attachment_init_meta = PtrValAttachmentMeta { alloc: alloc_ptr };
        Self {
            table: PtrTable::with_capacity(cap, attachment_init_meta),
            allocator: alloc,
            shadow: PhantomData,
        }
    }

    #[inline(always)]
    fn get(&self, key: &K) -> Option<V> {
        let (fkey, hash) = self.table.get_hash(0, key);
        let guard = crossbeam_epoch::pin();
        let backoff = crossbeam_utils::Backoff::new();
        loop {
            if let Some((mut fv, _, addr)) = self
                .table
                .get_with_hash(key, fkey, hash, false, &guard, &backoff)
            {
                if let Some(val) = self.deref_val(fv & WORD_MUTEX_DATA_BIT_MASK) {
                    return Some(val);
                }
                let retry_val = self.retry_get(&mut fv, addr, &backoff);
                if retry_val.is_some() {
                    return retry_val;
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
                guard.buffered_free(node_addr);
                ptr::read(ptr)
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
            let (val_ptr, node_addr) = self.ptr_of_val(fv);
            unsafe {
                let value = ptr::read(val_ptr);
                self.allocator.buffered_free(node_addr as _);
                value
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
    alloc: *mut obj_alloc::Allocator<PtrValueNode<V>, ALLOC_BUFFER_SIZE>,
    _marker: PhantomData<(K, V, A)>,
}

#[derive(Clone)]
pub struct PtrValAttachmentItem<K, V> {
    addr: usize,
    alloc: *mut obj_alloc::Allocator<PtrValueNode<V>, ALLOC_BUFFER_SIZE>,
    _marker: PhantomData<(K, V)>,
}

#[derive(Clone)]
pub struct PtrValAttachmentMeta<V> {
    alloc: *mut obj_alloc::Allocator<PtrValueNode<V>, ALLOC_BUFFER_SIZE>,
}

unsafe impl<V> Send for PtrValAttachmentMeta<V> {}
unsafe impl<V> Sync for PtrValAttachmentMeta<V> {}

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
    type InitMeta = PtrValAttachmentMeta<V>;

    fn heap_size_of(cap: usize) -> usize {
        cap * Self::KEY_SIZE // only keys on the heap
    }

    fn new(heap_ptr: usize, meta: &Self::InitMeta) -> Self {
        Self {
            key_chunk: heap_ptr,
            alloc: meta.alloc,
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
            alloc: self.alloc,
            _marker: PhantomData,
        }
    }

    #[inline(always)]
    fn manually_drop(&self, fvalue: usize) {
        unsafe {
            let (addr, _val_ver) = decompose_value::<K, V>(fvalue & WORD_MUTEX_DATA_BIT_MASK);
            let node_ptr = addr as *mut PtrValueNode<V>;
            let node_ref = &*node_ptr;
            let val_ptr = node_ref.value.as_ptr();
            debug_assert!(!node_ptr.is_null(), "fval is {}", fvalue);
            debug_assert!(!val_ptr.is_null());
            mem::drop(ptr::read(val_ptr));
            (&*self.alloc).buffered_free(node_ptr);
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
        unsafe { (*(addr as *mut K)).clone() }
    }

    fn get_value(self) -> () {}

    fn set_key(self, key: K) {
        let addr = self.addr;
        unsafe { ptr::write_volatile(addr as *mut K, key) }
    }

    fn set_value(self, _value: (), _old_fval: FVal) {
        // self.erase(old_fval)
    }

    fn erase(self, _old_fval: FVal) {}

    fn probe(self, probe_key: &K) -> bool {
        let key = unsafe { &*(self.addr as *mut K) };
        key == probe_key
    }

    fn prep_write(self) {}
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
        let value;
        loop {
            let swap_res = map.table.swap(
                0,
                key,
                move |fast_value| {
                    let locked_val = fast_value | MUTEX_BIT_MASK;
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
                    value = if let Some(v) = map.deref_val(val & WORD_MUTEX_DATA_BIT_MASK) {
                        v
                    } else {
                        continue;
                    };
                    break;
                }
                SwapResult::Failed | SwapResult::Aborted => {
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
            fvalue | MUTEX_BIT_MASK,
        ) {
            None | Some((TOMBSTONE_VALUE, ())) | Some((EMPTY_VALUE, ())) => Some(Self {
                map,
                key: key.clone(),
                value: value,
            }),
            _ => None,
        }
    }

    pub fn remove(self) -> V {
        let guard = self.map.allocator.pin();
        let fval = self.map.table.remove(&self.key, 0).unwrap().0 & WORD_MUTEX_DATA_BIT_MASK;
        let (val_ptr, node_addr) = self.map.ptr_of_val(fval);
        let r = unsafe { ptr::read(val_ptr) };
        guard.buffered_free(node_addr as _);
        mem::forget(self);
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
        let val = self.value.clone();
        let key = self.key.clone();
        let fval = self.map.ref_val(val, &guard) & WORD_MUTEX_DATA_BIT_MASK;
        self.map
            .table
            .insert(InsertOp::Insert, &key, Some(&()), 0, fval);
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
    }
}
#[cfg(test)]
mod ptr_map {
    use test::Bencher;

    use crate::map::*;
    use std::{alloc::System, sync::Arc, thread};

    #[test]
    fn no_resize() {
        let _ = env_logger::try_init();
        let map = PtrHashMap::<usize, usize, System>::with_capacity(4096);
        for i in 5..2048 {
            let k = i;
            let v = i * 2;
            map.insert(k, v);
        }
        for i in 5..2048 {
            let k = i;
            let v = i * 2;
            match map.get(&k) {
                Some(r) => assert_eq!(r, v),
                None => panic!("{}", i),
            }
        }
    }

    #[test]
    fn resize() {
        let _ = env_logger::try_init();
        let map = PtrHashMap::<usize, usize, System>::with_capacity(16);
        for i in 5..2048 {
            let k = i;
            let v = i * 2;
            map.insert(k, v);
        }
        for i in 5..2048 {
            let k = i;
            let v = i * 2;
            match map.get(&k) {
                Some(r) => assert_eq!(r, v),
                None => panic!("{}", i),
            }
        }
    }

    #[test]
    fn parallel_no_resize() {
        let _ = env_logger::try_init();
        let map = Arc::new(FatHashMap::with_capacity(65536));
        let mut threads = vec![];
        for i in 5..99 {
            let k = key_from(i);
            let v = val_from(i * 10);
            map.insert(k, v);
        }
        for i in 100..900 {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                for j in 5..60 {
                    let k = key_from(i * 100 + j);
                    let v = val_from(i * j);
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
                let v = val_from(i * j);
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
    fn parallel_arc_no_resize() {
        let _ = env_logger::try_init();
        let map = Arc::new(PtrHashMap::<Key, Arc<Value>, System>::with_capacity(65536));
        let mut threads = vec![];
        for i in 5..99 {
            let k = key_from(i);
            let v = Arc::new(val_from(i * 10));
            map.insert(k, v);
        }
        let insert_term = 16;
        for i in 100..200 {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                for j in 5..256 {
                    let key = key_from(i * 10000 + j);
                    for k in 0..=insert_term {
                        let v = Arc::new(val_from(i * j + k));
                        map.insert(key, v);
                    }
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
        for i in 100..200 {
            for j in 5..256 {
                let k = key_from(i * 10000 + j);
                let v = Arc::new(val_from(i * j + insert_term));
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
    fn parallel_with_arc_resize() {
        let _ = env_logger::try_init();
        let num_threads = num_cpus::get();
        let test_load = 2048;
        let repeat_load = 64;
        let map = Arc::new(PtrHashMap::<Key, Arc<Value>, System>::with_capacity(32));
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
                          assert_eq!(&*map.get(&key).unwrap(), &val_from(value_num - 1));
                      }
                      let value = Arc::new(val_from(value_num));
                      let pre_insert_epoch = map.table.now_epoch();
                      map.insert(key, value.clone());
                      let post_insert_epoch = map.table.now_epoch();
                      for l in 1..32 {
                          let pre_fail_get_epoch = map.table.now_epoch();
                          let left = map.get(&key);
                          let post_fail_get_epoch = map.table.now_epoch();
                          let right = Some(&value);
                          if left.as_ref() != right {
                              error!("Discovered mismatch key {:?}, analyzing", &key);
                              for m in 1..1024 {
                                  let mleft = map.get(&key);
                                  let mright = Some(&value);
                                  if mleft.as_ref() == mright {
                                      panic!(
                                          "Recovered at turn {} for {:?}, copying {}, epoch {} to {}, now {}, PIE: {} to {}. Expecting {:?} got {:?}. Migration problem!!!", 
                                          m, 
                                          &key, 
                                          map.table.map_is_copying(),
                                          pre_fail_get_epoch,
                                          post_fail_get_epoch,
                                          map.table.now_epoch(),
                                          pre_insert_epoch, 
                                          post_insert_epoch,
                                          right, left
                                      );
                                      // panic!("Late value change on {:?}", key);
                                  }
                              }
                              panic!("Unable to recover for {:?}, round {}, copying {}. Expecting {:?} got {:?}.", &key, l , map.table.map_is_copying(), right, left);
                              // panic!("Unrecoverable value change for {:?}", key);
                          }
                      }
                      if j % 7 == 0 {
                          assert_eq!(
                              map.remove(&key),
                              Some(value.clone()),
                              "Remove result, get {:?}, copying {}, round {}",
                              map.get(&key),
                              map.table.map_is_copying(),
                              k
                          );
                        assert_eq!(map.get(&key), None, "Remove recursion");
                        assert!(map.lock(&key).is_none(), "Remove recursion with lock");
                        map.insert(key, value.clone());
                      }
                      if j % 3 == 0 {
                          let new_value = val_from(value_num + 7);
                          let pre_insert_epoch = map.table.now_epoch();
                          map.insert(key, Arc::new(new_value));
                          let post_insert_epoch = map.table.now_epoch();
                          assert_eq!(
                              &*map.get(&key).unwrap(), 
                              &new_value, 
                              "Checking immediate update, key {:?}, epoch {} to {}",
                              key, pre_insert_epoch, post_insert_epoch
                          );
                          map.insert(key, value);
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
                let v = val_from(v_num);
                let get_res = map.get(&k);
                assert_eq!(
                    &v,
                    &*get_res.unwrap(),
                    "Final val mismatch. k {:?}, i {}, j {}, epoch {}",
                    k,
                    i,
                    j,
                    map.table.now_epoch()
                );
            }
        });
    }

    #[test]
    fn exaust_versions() {
        let map = PtrHashMap::<usize, usize, System>::with_capacity(16);
        for i in 0..255 {
            if i == 2 {
                println!("watch out");
            }
            map.insert(1, i);
            debug_assert_eq!(map.get(&1).unwrap(), i);
        }
        for i in 255..2096 {
            map.insert(1, i);
            debug_assert_eq!(map.get(&1).unwrap(), i);
        }
    }

    #[test]
    fn parallel_with_resize() {
        let _ = env_logger::try_init();
        let num_threads = num_cpus::get();
        let test_load = 1024;
        let repeat_load = 32;
        let map = Arc::new(FatHashMap::with_capacity(32));
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
                          assert_eq!(map.get(&key), Some(val_from(value_num - 1)));
                      }
                      let value = val_from(value_num);
                      let pre_insert_epoch = map.table.now_epoch();
                      map.insert(key, value);
                      let post_insert_epoch = map.table.now_epoch();
                      for l in 1..128 {
                          let pre_fail_get_epoch = map.table.now_epoch();
                          let left = map.get(&key);
                          let post_fail_get_epoch = map.table.now_epoch();
                          let right = Some(value);
                          if left != right {
                              error!("Discovered mismatch key {:?}, analyzing", &key);
                              for m in 1..1024 {
                                  let mleft = map.get(&key);
                                  let mright = Some(value);
                                  if mleft == mright {
                                      panic!(
                                          "Recovered at turn {} for {:?}, copying {}, epoch {} to {}, now {}, PIE: {} to {}. Expecting {:?} got {:?}. Migration problem!!!", 
                                          m, 
                                          &key, 
                                          map.table.map_is_copying(),
                                          pre_fail_get_epoch,
                                          post_fail_get_epoch,
                                          map.table.now_epoch(),
                                          pre_insert_epoch, 
                                          post_insert_epoch,
                                          right, left
                                      );
                                      // panic!("Late value change on {:?}", key);
                                  }
                              }
                              panic!("Unable to recover for {:?}, round {}, copying {}. Expecting {:?} got {:?}.", &key, l , map.table.map_is_copying(), right, left);
                              // panic!("Unrecoverable value change for {:?}", key);
                          }
                      }
                      if j % 7 == 0 {
                          assert_eq!(
                              map.remove(&key),
                              Some(value),
                              "Remove result, get {:?}, copying {}, round {}",
                              map.get(&key),
                              map.table.map_is_copying(),
                              k
                          );
                          assert_eq!(map.get(&key), None, "Remove recursion");
                          assert!(map.lock(&key).is_none(), "Remove recursion with lock");
                          map.insert(key, value);
                      }
                      if j % 3 == 0 {
                          let new_value = val_from(value_num + 7);
                          let pre_insert_epoch = map.table.now_epoch();
                          map.insert(key, new_value);
                          let post_insert_epoch = map.table.now_epoch();
                          assert_eq!(
                              map.get(&key), 
                              Some(new_value), 
                              "Checking immediate update, key {:?}, epoch {} to {}",
                              key, pre_insert_epoch, post_insert_epoch
                          );
                          map.insert(key, value);
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
                let v = val_from(v_num);
                let get_res = map.get(&k);
                assert_eq!(
                    Some(v),
                    get_res,
                    "Final val mismatch. k {:?}, i {}, j {}, epoch {}",
                    k,
                    i,
                    j,
                    map.table.now_epoch()
                );
            }
        });
    }

    #[test]
    fn parallel_overflow() {
        let _ = env_logger::try_init();
        let map = Arc::new(FatHashMap::with_capacity(32));
        for tid in 0..8 {}
    }

    const VAL_SIZE: usize = 2048;
    pub type Key = [u8; 128];
    pub type Value = [u8; VAL_SIZE];
    pub type FatHashMap = PtrHashMap<Key, Value, System>;

    fn key_from(num: usize) -> Key {
        let mut r = [0u8; 128];
        for (i, b) in num.to_be_bytes().iter().enumerate() {
            r[i] = *b
        }
        r
    }

    fn val_from(num: usize) -> Value {
        let mut r = [0u8; VAL_SIZE];
        for (i, b) in num.to_be_bytes().iter().enumerate() {
            r[i] = *b
        }
        r
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
                        let mut mutex = map.insert_locked(&key, 0).unwrap();
                        *mutex = 1;
                    }
                    for j in 1..update_load {
                        assert!(
                            map.get(&key).is_some(),
                            "Pre getting value for mutex, key {}, epoch {}",
                            key,
                            map.table.now_epoch()
                        );
                        let val = {
                            let mut mutex = map.lock(&key).expect(&format!(
                                "Locking key {}, epoch {}, copying {}",
                                key,
                                map.table.now_epoch(),
                                map.table.map_is_copying()
                            ));
                            assert_eq!(*mutex, j);
                            *mutex += 1;
                            *mutex
                        };
                        assert!(
                            map.get(&key).is_some(),
                            "Post getting value for mutex, key {}, epoch {}",
                            key,
                            map.table.now_epoch()
                        );
                        if j % 7 == 0 {
                            {
                                let mutex = map.lock(&key).expect(&format!(
                                    "Remove locking key {}, copying {}",
                                    key,
                                    map.table.now_epoch()
                                ));
                                mutex.remove();
                            }
                            assert!(map.lock(&key).is_none());
                            *map.insert_locked(&key, 0).unwrap() = val;
                        }
                    }
                    assert_eq!(*map.lock(&key).unwrap(), update_load);
                }
            }));
        }
        for thread in threads {
            let _ = thread.join();
        }
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
        debug!("Len: {}, occ {:?}", map.len(), map.table.occupation());
        map.table.dump_dist();
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
}
