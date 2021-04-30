use bustle::*;
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Clone)]
pub struct Table(std::sync::Arc<Mutex<HashMap<usize, usize>>>);

impl Collection for Table {
    type Handle = Self;
    fn with_capacity(capacity: usize) -> Self {
        Self(std::sync::Arc::new(Mutex::new(HashMap::with_capacity(
            capacity,
        ))))
    }

    fn pin(&self) -> Self::Handle {
        self.clone()
    }
}

impl CollectionHandle for Table {
    fn get(&mut self, key: &usize) -> bool {
        self.0.lock().unwrap().get(key).is_some()
    }

    fn insert(&mut self, key: &usize, value: &usize) -> bool {
        self.0.lock().unwrap().insert(*key, *value).is_none()
    }

    fn remove(&mut self, key: &usize) -> bool {
        self.0.lock().unwrap().remove(key).is_some()
    }

    fn update(&mut self, key: &usize, value: &usize) -> bool {
        use std::collections::hash_map::Entry;
        let mut map = self.0.lock().unwrap();
        if let Entry::Occupied(mut e) = map.entry(*key) {
            e.insert(*value);
            true
        } else {
            false
        }
    }
}
