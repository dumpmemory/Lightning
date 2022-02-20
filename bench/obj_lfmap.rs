use bustle::*;
use lightning::map::{Map, ObjectMap};
use std::alloc::System;
use std::collections::hash_map::DefaultHasher;
use std::sync::Arc;

#[derive(Clone)]
pub struct TestTable(Arc<ObjectMap<usize, System, DefaultHasher>>);

impl Collection for TestTable {
    type Handle = Self;
    fn with_capacity(capacity: usize) -> Self {
        Self(Arc::new(ObjectMap::with_capacity(capacity)))
    }

    fn pin(&self) -> Self::Handle {
        self.clone()
    }
}

impl CollectionHandle for TestTable {
    fn get(&mut self, key: &usize) -> bool {
        self.0.get(key).is_some()
    }

    fn insert(&mut self, key: &usize, value: &usize) -> bool {
        self.0.insert(key, value).is_none()
    }

    fn remove(&mut self, key: &usize) -> bool {
        self.0.remove(key).is_some()
    }

    fn update(&mut self, key: &usize, value: &usize) -> bool {
        self.0.insert(key, value).is_none()
    }
}
