use bustle::*;
use lightning::map::{Map, ObjectMap, WordMap};
use smallvec::alloc::sync::Arc;
use std::alloc::System;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;

#[derive(Clone)]
pub struct TestTable(Arc<WordMap<System, DefaultHasher>>);

impl Collection for TestTable {
    type Handle = Self;
    fn with_capacity(capacity: usize) -> Self {
        Self(Arc::new(WordMap::with_capacity(capacity)))
    }

    fn pin(&self) -> Self::Handle {
        self.clone()
    }
}

impl CollectionHandle for TestTable {
    type Key = u64;

    fn get(&mut self, key: &Self::Key) -> bool {
        let k = *key as usize;
        self.0.get(&k).is_some()
    }

    fn insert(&mut self, key: &Self::Key) -> bool {
        let k = *key as usize;
        self.0.insert(&k, k).is_none()
    }

    fn remove(&mut self, key: &Self::Key) -> bool {
        let k = *key as usize;
        self.0.remove(&k).is_some()
    }

    fn update(&mut self, key: &Self::Key) -> bool {
        let k = *key as usize;
        let map = &mut self.0;
        if map.contains_key(&k) {
            map.insert(&k, k);
            true
        } else {
            false
        }
    }
}
