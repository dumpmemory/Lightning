use bustle::*;
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Clone)]
pub struct Table<K>(std::sync::Arc<Mutex<HashMap<K, K>>>);

impl<K> Collection for Table<K>
where
    K: Send + From<u64> + Copy + 'static + std::hash::Hash + Eq,
{
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

impl<K> CollectionHandle for Table<K>
where
    K: Send + From<u64> + Copy + 'static + std::hash::Hash + Eq,
{
    type Key = K;

    fn get(&mut self, key: &Self::Key) -> bool {
        self.0.lock().unwrap().get(key).is_some()
    }

    fn insert(&mut self, key: &Self::Key, value: &Self::Key) -> bool {
        self.0.lock().unwrap().insert(*key, *value).is_none()
    }

    fn remove(&mut self, key: &Self::Key) -> bool {
        self.0.lock().unwrap().remove(key).is_some()
    }

    fn update(&mut self, key: &Self::Key, value: &Self::Key) -> bool {
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
