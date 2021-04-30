use bustle::*;
use std::collections::HashMap;
use std::sync::RwLock;

#[derive(Clone)]
pub struct Table(std::sync::Arc<RwLock<HashMap<usize, usize>>>);

impl Collection for Table {
    type Handle = Self;
    fn with_capacity(capacity: usize) -> Self {
        Self(std::sync::Arc::new(RwLock::new(HashMap::with_capacity(
            capacity,
        ))))
    }

    fn pin(&self) -> Self::Handle {
        self.clone()
    }
}

impl CollectionHandle for Table {
    fn get(&mut self, key: &usize) -> bool {
        self.0.read().unwrap().get(key).is_some()
    }

    fn insert(&mut self, key: &usize, value: &usize) -> bool {
        self.0.write().unwrap().insert(*key, *value).is_none()
    }

    fn remove(&mut self, key: &usize) -> bool {
        self.0.write().unwrap().remove(key).is_some()
    }

    fn update(&mut self, key: &usize, value: &usize) -> bool {
        use std::collections::hash_map::Entry;
        let mut map = self.0.write().unwrap();
        if let Entry::Occupied(mut e) = map.entry(*key) {
            e.insert(*value);
            true
        } else {
            false
        }
    }
}
