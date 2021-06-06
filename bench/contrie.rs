use std::{collections::hash_map::RandomState, sync::Arc};

use bustle::*;
use contrie::ConMap;

#[derive(Clone)]
pub struct Table(Arc<ConMap<usize, usize>>);

impl Collection for Table {
    type Handle = Self;
    fn with_capacity(capacity: usize) -> Self {
        Self(Arc::new(ConMap::with_hasher(
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
        self.0.insert(*key, *value).is_some()
    }
}