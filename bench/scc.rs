use bustle::*;
use scc::HashMap;
use std::sync::Arc;

#[derive(Clone)]
pub struct Table(Arc<HashMap<usize, usize>>);

impl Collection for Table {
    type Handle = Self;
    fn with_capacity(capacity: usize) -> Self {
        Self(Arc::new(HashMap::with_capacity(capacity)))
    }

    fn pin(&self) -> Self::Handle {
        self.clone()
    }
}

impl CollectionHandle for Table {
    fn get(&mut self, key: &usize) -> bool {
        let k = *key as usize;
        self.0.read(&k, |_, v| *v).is_some()
    }

    fn insert(&mut self, key: &usize, value: &usize) -> bool {
        let k = *key as usize;
        let v = *value as usize;
        self.0.insert(k, v).is_ok()
    }

    fn remove(&mut self, key: &usize) -> bool {
        let k = *key as usize;
        self.0.remove(&k).is_some()
    }

    fn update(&mut self, key: &usize, value: &usize) -> bool {
        let k = *key as usize;
        let v = *value as usize;
        self.0.upsert(k, || v, |_, rv| *rv = v);
        true
    }
}
