use std::collections::hash_map::RandomState;

use bustle::*;
use cht::HashMap;

#[derive(Clone)]
pub struct Table(std::sync::Arc<HashMap<usize, usize, RandomState>>);

impl Collection for Table {
    type Handle = Self;
    fn with_capacity(capacity: usize) -> Self {
        Self(std::sync::Arc::new(HashMap::with_capacity_and_hasher(
            capacity,
            RandomState::default(),
        )))
    }

    fn pin(&self) -> Self::Handle {
        self.clone()
    }
}

impl CollectionHandle for Table {
    fn get(&mut self, key: &usize) -> bool {
        self.0.get(key).is_some()
    }

    fn insert(&mut self, key: &usize, value: &usize) -> bool {
        self.0.insert(*key, *value).is_none()
    }

    fn remove(&mut self, key: &usize) -> bool {
        self.0.remove(key).is_some()
    }

    fn update(&mut self, key: &usize, value: &usize) -> bool {
        self.0.modify_entry(*key, |_k, _v| *value).is_some()
    }
}
