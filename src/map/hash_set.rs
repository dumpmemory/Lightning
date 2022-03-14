use super::base::*;
use super::hash_map::HashTable;
use super::*;

pub struct HashSet<
    T: Clone + Hash + Eq,
    ALLOC: GlobalAlloc + Default = System,
    H: Hasher + Default = DefaultHasher,
> {
    table: HashTable<T, (), ALLOC, H>,
    shadow: PhantomData<H>,
}

impl<T: Clone + Hash + Eq, ALLOC: GlobalAlloc + Default, H: Hasher + Default> HashSet<T, ALLOC, H> {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            table: Table::with_capacity(cap, ()),
            shadow: PhantomData,
        }
    }

    pub fn contains(&self, item: &T) -> bool {
        self.table.get(item, 0, false).is_some()
    }

    pub fn insert(&self, item: &T) -> bool {
        self.table
            .insert(InsertOp::TryInsert, item, None, 0, !0)
            .is_none()
    }

    pub fn remove(&self, item: &T) -> bool {
        self.table.remove(item, 0).is_some()
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
