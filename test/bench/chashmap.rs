use bustle::*;
use chashmap::CHashMap;
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Clone)]
pub struct Table<K>(std::sync::Arc<CHashMap<K, ()>>);

impl<K> Collection for Table<K>
where
    K: Send + From<u64> + Copy + 'static + std::hash::Hash + Eq + Sync,
{
    type Handle = Self;
    fn with_capacity(capacity: usize) -> Self {
        Self(std::sync::Arc::new(CHashMap::with_capacity(capacity)))
    }

    fn pin(&self) -> Self::Handle {
        self.clone()
    }
}

impl<K> CollectionHandle for Table<K>
where
    K: Send + From<u64> + Copy + 'static + std::hash::Hash + Eq,
{
    type Key = K;

    fn get(&mut self, key: &Self::Key) -> bool {
        self.0.get(key).is_some()
    }

    fn insert(&mut self, key: &Self::Key) -> bool {
        self.0.insert(*key, ()).is_none()
    }

    fn remove(&mut self, key: &Self::Key) -> bool {
        self.0.remove(key).is_some()
    }

    fn update(&mut self, key: &Self::Key) -> bool {
        if let Some(mut v) = self.0.get_mut(key) {
            *v = ();
            true
        } else {
            false
        }
    }
}
