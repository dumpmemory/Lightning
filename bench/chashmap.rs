use bustle::*;
use chashmap::CHashMap;

#[derive(Clone)]
pub struct Table(std::sync::Arc<CHashMap<usize, usize>>);

impl Collection for Table {
    type Handle = Self;
    fn with_capacity(capacity: usize) -> Self {
        Self(std::sync::Arc::new(CHashMap::with_capacity(capacity)))
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
        if let Some(mut v) = self.0.get_mut(key) {
            *v = *value;
            true
        } else {
            false
        }
    }
}
