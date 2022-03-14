use super::base::*;
use super::*;

type ObjectTable<V, ALLOC, H> = Table<(), V, WordObjectAttachment<V, ALLOC>, ALLOC, H>;

impl<T, A: GlobalAlloc + Default> WordObjectAttachment<T, A> {
    fn addr_by_index(&self, index: usize) -> usize {
        self.obj_chunk + index * mem::size_of::<T>()
    }
}

#[derive(Clone)]
pub struct ObjectMap<
    V: Clone,
    ALLOC: GlobalAlloc + Default = System,
    H: Hasher + Default = DefaultHasher,
> {
    table: ObjectTable<V, ALLOC, H>,
}

impl<V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> ObjectMap<V, ALLOC, H> {
    #[inline(always)]
    fn insert_with_op(&self, op: InsertOp, key: &FKey, value: &V) -> Option<V> {
        self.table
            .insert(op, &(), Some(value), key + NUM_FIX_K, PLACEHOLDER_VAL)
            .map(|(_, v)| v)
    }

    pub fn read(&self, key: FKey) -> Option<ObjectMapReadGuard<V, ALLOC, H>> {
        ObjectMapReadGuard::new(&self.table, key)
    }

    pub fn write(&self, key: FKey) -> Option<ObjectMapWriteGuard<V, ALLOC, H>> {
        ObjectMapWriteGuard::new(&self.table, key)
    }
}

impl<V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Map<FKey, V>
    for ObjectMap<V, ALLOC, H>
{
    fn with_capacity(cap: usize) -> Self {
        Self {
            table: Table::with_capacity(cap, ()),
        }
    }

    #[inline(always)]
    fn get(&self, key: &FKey) -> Option<V> {
        self.table
            .get(&(), key + NUM_FIX_K, true)
            .map(|v| v.1.unwrap())
    }

    #[inline(always)]
    fn insert(&self, key: &FKey, value: &V) -> Option<V> {
        self.insert_with_op(InsertOp::Insert, key, value)
    }

    #[inline(always)]
    fn try_insert(&self, key: &FKey, value: &V) -> Option<V> {
        self.insert_with_op(InsertOp::TryInsert, key, value)
    }

    #[inline(always)]
    fn remove(&self, key: &FKey) -> Option<V> {
        self.table.remove(&(), key + NUM_FIX_K).map(|(_, v)| v)
    }

    #[inline(always)]
    fn entries(&self) -> Vec<(FKey, V)> {
        self.table
            .entries()
            .into_iter()
            .map(|(k, _, _, v)| (k - NUM_FIX_K, v))
            .collect()
    }

    #[inline(always)]
    fn contains_key(&self, key: &FKey) -> bool {
        self.table.get(&(), key + NUM_FIX_K, false).is_some()
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.table.len()
    }

    fn clear(&self) {
        self.table.clear();
    }
}

pub(crate) struct WordObjectAttachment<T, A: GlobalAlloc + Default> {
    obj_chunk: usize,
    shadow: PhantomData<(T, A)>,
}

#[derive(Clone)]
pub struct WordObjectAttachmentItem<T> {
    addr: usize,
    _makrer: PhantomData<T>,
}

impl<T: Clone, A: GlobalAlloc + Default> Attachment<(), T> for WordObjectAttachment<T, A> {
    type Item = WordObjectAttachmentItem<T>;
    type InitMeta = ();

    fn heap_size_of(cap: usize) -> usize {
        let obj_size = mem::size_of::<T>();
        cap * obj_size
    }

    fn new(heap_ptr: usize, meta: &()) -> Self {
        Self {
            obj_chunk: heap_ptr,
            shadow: PhantomData,
        }
    }

    #[inline(always)]
    fn prefetch(&self, index: usize) -> Self::Item {
        let addr = self.addr_by_index(index);
        unsafe {
            intrinsics::prefetch_read_data(addr as *const T, 2);
        }
        WordObjectAttachmentItem {
            addr,
            _makrer: PhantomData,
        }
    }
}

impl<T: Clone> AttachmentItem<(), T> for WordObjectAttachmentItem<T> {
    #[inline(always)]
    fn get_value(self) -> T {
        let addr = self.addr;
        let v = unsafe { (*(addr as *mut T)).clone() };
        v
    }

    #[inline(always)]
    fn set_value(self, value: T, _old_fval: FVal) {
        let addr = self.addr;
        unsafe { ptr::write(addr as *mut T, value) }
    }

    #[inline(always)]
    fn erase(self, _old_fval: FVal) {
        drop(self.addr as *mut T)
    }

    #[inline(always)]
    fn probe(self, _value: &()) -> bool {
        true
    }

    #[inline(always)]
    fn get_key(self) -> () {
        ()
    }

    #[inline(always)]
    fn set_key(self, _key: ()) {}

    #[inline(always)]
    fn prep_write(self) {
        unsafe {
            intrinsics::prefetch_write_data(self.addr as *const T, 2);
        }
    }
}

impl<T: Clone> Copy for WordObjectAttachmentItem<T> {}

pub struct ObjectMapReadGuard<
    'a,
    V: Clone,
    ALLOC: GlobalAlloc + Default = System,
    H: Hasher + Default = DefaultHasher,
> {
    table: &'a ObjectTable<V, ALLOC, H>,
    key: FKey,
    value: V,
    _mark: PhantomData<H>,
}

impl<'a, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default>
    ObjectMapReadGuard<'a, V, ALLOC, H>
{
    fn new(table: &'a ObjectTable<V, ALLOC, H>, key: FKey) -> Option<Self> {
        let backoff = crossbeam_utils::Backoff::new();
        let guard = crossbeam_epoch::pin();
        let value: V;
        let key = key + NUM_FIX_K;
        loop {
            let swap_res = table.swap(
                key,
                &(),
                move |fast_value| {
                    if fast_value != PLACEHOLDER_VAL - 1 {
                        // Not write locked, can bump it by one
                        trace!("Key {} is not write locked, will read lock", key);
                        Some(fast_value + 1)
                    } else {
                        trace!("Key {} is write locked, unchanged", key);
                        None
                    }
                },
                &guard,
            );
            match swap_res {
                SwapResult::Succeed(_, idx, chunk) => {
                    let chunk_ref = unsafe { chunk.deref() };
                    let attachment = chunk_ref.attachment.prefetch(idx);
                    let v = attachment.get_value();
                    value = v;
                    break;
                }
                SwapResult::Failed | SwapResult::Aborted => {
                    trace!("Lock on key {} failed, retry", key);
                    backoff.spin();
                    continue;
                }
                SwapResult::NotFound => {
                    trace!("Cannot found hash key {} to lock", key);
                    return None;
                }
            }
        }
        Some(Self {
            table,
            key,
            value,
            _mark: Default::default(),
        })
    }
}

impl<'a, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Deref
    for ObjectMapReadGuard<'a, V, ALLOC, H>
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Drop
    for ObjectMapReadGuard<'a, V, ALLOC, H>
{
    fn drop(&mut self) {
        trace!("Release read lock for hash key {}", self.key);
        let guard = crossbeam_epoch::pin();
        self.table.swap(
            self.key,
            &(),
            |fast_value| {
                debug_assert!(fast_value > PLACEHOLDER_VAL);
                Some(fast_value - 1)
            },
            &guard,
        );
    }
}

pub struct ObjectMapWriteGuard<
    'a,
    V: Clone,
    ALLOC: GlobalAlloc + Default = System,
    H: Hasher + Default = DefaultHasher,
> {
    table: &'a ObjectTable<V, ALLOC, H>,
    key: FKey,
    value: V,
    _mark: PhantomData<H>,
}

impl<'a, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default>
    ObjectMapWriteGuard<'a, V, ALLOC, H>
{
    fn new(table: &'a ObjectTable<V, ALLOC, H>, key: FKey) -> Option<Self> {
        let backoff = crossbeam_utils::Backoff::new();
        let guard = crossbeam_epoch::pin();
        let value: V;
        let key = key + NUM_FIX_K;
        loop {
            let swap_res = table.swap(
                key,
                &(),
                move |fast_value| {
                    if fast_value == PLACEHOLDER_VAL {
                        // Not write locked, can bump it by one
                        trace!("Key {} is write lockable, will write lock", key);
                        Some(fast_value - 1)
                    } else {
                        trace!("Key {} is write locked, unchanged", key);
                        None
                    }
                },
                &guard,
            );
            match swap_res {
                SwapResult::Succeed(_, idx, chunk) => {
                    let chunk_ref = unsafe { chunk.deref() };
                    let attachment = chunk_ref.attachment.prefetch(idx);
                    let v = attachment.get_value();
                    value = v;
                    break;
                }
                SwapResult::Failed | SwapResult::Aborted => {
                    trace!("Lock on key {} failed, retry", key);
                    backoff.spin();
                    continue;
                }
                SwapResult::NotFound => {
                    trace!("Cannot found key {} to lock", key);
                    return None;
                }
            }
        }
        Some(Self {
            table,
            key,
            value,
            _mark: Default::default(),
        })
    }

    pub fn remove(self) -> V {
        let res = self.table.remove(&(), self.key).unwrap().1;
        mem::forget(self);
        res
    }
}

impl<'a, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Deref
    for ObjectMapWriteGuard<'a, V, ALLOC, H>
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> DerefMut
    for ObjectMapWriteGuard<'a, V, ALLOC, H>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

impl<'a, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Drop
    for ObjectMapWriteGuard<'a, V, ALLOC, H>
{
    fn drop(&mut self) {
        trace!("Release read lock for key {}", self.key);
        self.table.insert(
            InsertOp::Insert,
            &(),
            Some(&self.value),
            self.key,
            PLACEHOLDER_VAL,
        );
    }
}

#[cfg(test)]
mod test {

    use crate::map::tests::Obj;

    use super::*;
    use std::{sync::Arc, thread};

    #[test]
    fn parallel_obj_map_rwlock() {
        let _ = env_logger::try_init();
        let map_cont = ObjectMap::<Obj, System, DefaultHasher>::with_capacity(4);
        let map = Arc::new(map_cont);
        map.insert(&1, &Obj::new(0));
        let mut threads = vec![];
        let num_threads = 256;
        for i in 0..num_threads {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                let mut guard = map.write(1).unwrap();
                let val = guard.get();
                guard.set(val + 1);
                trace!("Dealt with {}", i);
            }));
        }
        for thread in threads {
            let _ = thread.join();
        }
        map.get(&1).unwrap().validate(num_threads);
    }

    #[test]
    fn obj_map() {
        let _ = env_logger::try_init();
        let map = ObjectMap::<Obj>::with_capacity(16);
        for i in 5..2048 {
            map.insert(&i, &Obj::new(i));
        }
        for i in 5..2048 {
            match map.get(&i) {
                Some(r) => r.validate(i),
                None => panic!("{}", i),
            }
        }
    }

    #[test]
    fn parallel_obj_hybrid() {
        let _ = env_logger::try_init();
        let map = Arc::new(ObjectMap::<Obj>::with_capacity(4));
        for i in 5..128 {
            map.insert(&i, &Obj::new(i * 10));
        }
        let mut threads = vec![];
        for i in 256..265 {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                for j in 5..60 {
                    map.insert(&(i * 10 + j), &Obj::new(10));
                }
            }));
        }
        for i in 5..8 {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                for j in 5..8 {
                    map.remove(&(i * j));
                }
            }));
        }
        for thread in threads {
            let _ = thread.join();
        }
        for i in 256..265 {
            for j in 5..60 {
                match map.get(&(i * 10 + j)) {
                    Some(r) => r.validate(10),
                    None => panic!("{}", i),
                }
            }
        }
    }
}
