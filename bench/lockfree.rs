use bustle::*;
use lockfree::map::Map;
use std::collections::hash_map::RandomState;
use std::sync::Arc;

#[derive(Clone)]
pub struct Table(Arc<Map<usize, usize>>);

impl Collection for Table {
    type Handle = Self;
    fn with_capacity(_capacity: usize) -> Self {
        Self(Arc::new(Map::with_hasher(RandomState::default())))
    }

    fn pin(&self) -> Self::Handle {
        self.clone()
    }
}

impl CollectionHandle for Table {
    fn get(&mut self, key: &usize) -> bool {
        let k = *key as usize;
        self.0.get(&k).is_some()
    }

    fn insert(&mut self, key: &usize, value: &usize) -> bool {
        let k = *key as usize;
        let v = *value as usize;
        self.0.insert(k, v).is_none()
    }

    fn remove(&mut self, key: &usize) -> bool {
        let k = *key as usize;
        self.0.remove(&k).is_some()
    }

    fn update(&mut self, key: &usize, value: &usize) -> bool {
        let k = *key as usize;
        let v = *value as usize;
        self.0.insert(k, v).is_none()
    }
}
