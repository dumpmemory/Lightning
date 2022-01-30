use crate::{linked_map::LinkedObjectMap, list::ListIter};

pub struct LRUCache<T: Clone + Default, const N: usize> {
    map: LinkedObjectMap<T, N>,
    capacity: usize,
    fetch_fn: Box<dyn Fn(usize) -> Option<T>>,
    evict_fn: Box<dyn Fn(usize, T)>,
}

impl<T: Clone + Default, const N: usize> LRUCache<T, N> {
    pub fn new<FF: Fn(usize) -> Option<T> + 'static, EF: Fn(usize, T) + 'static>(
        capacity: usize,
        fetch_fn: FF,
        evict_fn: EF,
    ) -> LRUCache<T, N> {
        Self {
            map: LinkedObjectMap::with_capacity(capacity),
            fetch_fn: Box::new(fetch_fn),
            evict_fn: Box::new(evict_fn),
            capacity,
        }
    }

    pub fn get(&self, key: usize) -> Option<T> {
        let res = self.map.get_to_front(key);
        if res.is_none() {
            if let Some(v) = (self.fetch_fn)(key) {
                self.map.insert_front(key, v.clone());
                while self.map.len() > self.capacity {
                    if let Some(evic) = self.map.pop_back() {
                        (self.evict_fn)(evic.0, evic.1)
                    }
                }
                return Some(v);
            } else {
                return None;
            }
        }
        res
    }

    pub fn remove(&self, key: usize) -> Option<T> {
        self.map.remove(key)
    }

    pub fn iter(&self) -> ListIter<(usize, T), N> {
        self.map.iter_front()
    }
}
