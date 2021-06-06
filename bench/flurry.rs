use bustle::*;
use flurry::HashMap;

#[derive(Clone)]
pub struct Table(std::sync::Arc<HashMap<usize, usize>>);

impl Collection for Table {
    type Handle = Self;
    fn with_capacity(capacity: usize) -> Self {
        Self(std::sync::Arc::new(HashMap::with_capacity(capacity)))
    }

    fn pin(&self) -> Self::Handle {
        self.clone()
    }
}

impl CollectionHandle for Table {
    fn get(&mut self, key: &usize) -> bool {
        let mref = self.0.pin();
        mref.get(key).is_some()
    }

    fn insert(&mut self, key: &usize, value: &usize) -> bool {
        let mref = self.0.pin();
        mref.insert(*key, *value).is_none()
    }

    fn remove(&mut self, key: &usize) -> bool {
        let mref = self.0.pin();
        mref.remove(key).is_some()
    }

    fn update(&mut self, key: &usize, value: &usize) -> bool {
        let mref = self.0.pin();
        mref.compute_if_present(key, |_k, _v| Some(*value))
            .is_some()
    }
}
