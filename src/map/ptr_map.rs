use super::base::*;
use super::*;

pub type PtrTable<K, V, ALLOC, H> =
    Table<K, (), PtrValAttachment<K, V, ALLOC>, ALLOC, H>;

pub struct PtrHashMap<
    K: Clone + Hash + Eq,
    V: Clone,
    ALLOC: GlobalAlloc + Default = System,
    H: Hasher + Default = DefaultHasher,
> {
    table: PtrTable<K, V, ALLOC, H>,
    shadow: PhantomData<(K, V, H)>,
}

impl<K: Clone + Hash + Eq, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default>
    PtrHashMap<K, V, ALLOC, H>
{
    #[inline(always)]
    pub fn insert_with_op(&self, op: InsertOp, key: &K, value: &V) -> Option<V> {
        let guard = crossbeam_epoch::pin();
        let v_num = Self::ref_val(value, &guard);
        self.table
            .insert(op, key, None, 0 as FKey, v_num as FVal)
            .map(|(fv, _)| Self::deref_val(fv as usize))
    }

    // pub fn write(&self, key: &K) -> Option<HashMapWriteGuard<K, V, ALLOC, H>> {
    //     HashMapWriteGuard::new(&self.table, key)
    // }
    // pub fn read(&self, key: &K) -> Option<HashMapReadGuard<K, V, ALLOC, H>> {
    //     HashMapReadGuard::new(&self.table, key)
    // }

    #[inline(always)]
    fn ref_val<T: Clone>(d: &T, guard: &Guard) -> usize {
        let ptr = Owned::new(d.clone()).into_shared(&guard);
        ptr.into_usize()
    }

    #[inline(always)]
    fn deref_val<T: Clone>(num: usize) -> T {
        unsafe {
            let ptr = Shared::<T>::from_usize(num);
            ptr.deref().clone()
        }
    }
}

impl<K: Clone + Hash + Eq, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Map<K, V>
    for PtrHashMap<K, V, ALLOC, H>
{
    fn with_capacity(cap: usize) -> Self {
        Self {
            table: PtrTable::with_capacity(cap),
            shadow: PhantomData,
        }
    }

    fn get(&self, key: &K) -> Option<V> {
        self.table.get(key, 0, false).map(|(fv, _)| Self::deref_val(fv))
    }

    fn insert(&self, key: &K, value: &V) -> Option<V> {
        self.insert_with_op(InsertOp::Insert, key, value)
    }

    fn try_insert(&self, key: &K, value: &V) -> Option<V> {
        self.insert_with_op(InsertOp::TryInsert, key, value)
    }

    fn remove(&self, key: &K) -> Option<V> {
        self.table.remove(key, 0).map(|(fv, _)| Self::deref_val(fv))
    }

    fn entries(&self) -> Vec<(K, V)> {
        self.table
            .entries()
            .into_iter()
            .map(|(_, fv, k, _)| (k, Self::deref_val(fv)))
            .collect()
    }

    fn contains_key(&self, key: &K) -> bool {
        self.table.get(key, 0, false).is_some()
    }

    fn len(&self) -> usize {
        self.table.len()
    }

    fn clear(&self) {
        self.table.clear();
    }
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
    const KEY_SIZE: usize = mem::size_of::<K>();

    fn addr_by_index(&self, index: usize) -> usize {
        self.key_chunk + index * Self::KEY_SIZE
    }
}

impl<K: Clone + Hash + Eq, V: Clone, A: GlobalAlloc + Default> Attachment<K, ()>
    for PtrValAttachment<K, V, A>
{
    type Item = PtrValAttachmentItem<K, V>;

    fn heap_size_of(cap: usize) -> usize {
        cap * Self::KEY_SIZE // only keys on the heap
    }

    fn new(heap_ptr: usize) -> Self {
        Self {
            key_chunk: heap_ptr,
            _marker: PhantomData,
        }
    }

    fn prefetch(&self, index: usize) -> Self::Item {
        let addr = self.addr_by_index(index);
        unsafe {
            intrinsics::prefetch_read_data(addr as *const K, 2);
        }
        PtrValAttachmentItem {
            addr,
            _marker: PhantomData,
        }
    }

    fn dealloc(&self) {}
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

    fn set_value(self, _value: (), old_fval: FVal) {
        self.erase(old_fval)
    }

    fn erase(self, old_fval: FVal) {
        if old_fval > 0 {
            unsafe {
                let guard = crossbeam_epoch::pin();
                let ptr = Shared::<V>::from_usize(old_fval as usize);
                guard.defer_destroy(ptr);
            }
        }
    }

    fn probe(self, probe_key: &K) -> bool {
        unsafe { (&*(self.addr as *mut K)) == probe_key }
    }

    fn prep_write(self) {}
}

impl<K: Clone, V: Clone> Copy for PtrValAttachmentItem<K, V> {}
