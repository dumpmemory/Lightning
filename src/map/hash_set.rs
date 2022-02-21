use super::base::hash_table::*;
use super::hash_map::HashTable;
use super::*;

pub struct HashSet<
    T: Clone + Hash + Eq,
    ALLOC: GlobalAlloc + Default = System,
    H: Hasher + Default = DefaultHasher,
> {
    table: HashTable<T, (), ALLOC>,
    shadow: PhantomData<H>,
}

impl<T: Clone + Hash + Eq, ALLOC: GlobalAlloc + Default, H: Hasher + Default> HashSet<T, ALLOC, H> {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            table: Table::with_capacity(cap),
            shadow: PhantomData,
        }
    }

    pub fn contains(&self, item: &T) -> bool {
        let hash = hash_key::<T, H>(item) as FKey;
        self.table.get(item, hash, false).is_some()
    }

    pub fn insert(&self, item: &T) -> bool {
        let hash = hash_key::<T, H>(item) as FKey;
        self.table
            .insert(InsertOp::TryInsert, item, None, hash, !0)
            .is_none()
    }

    pub fn remove(&self, item: &T) -> bool {
        let hash = hash_key::<T, H>(item) as FKey;
        self.table.remove(item, hash).is_some()
    }

    pub fn items(&self) -> std::collections::HashSet<T> {
        self.table
            .entries()
            .into_iter()
            .map(|(_, _, item, _)| item)
            .collect()
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.table.len()
    }
}
