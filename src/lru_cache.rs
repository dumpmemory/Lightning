use crate::{
    linked_map::LinkedHashMap,
    list::ListIter,
};
use std::hash::Hash;

pub struct LRUCache<K: Clone + Hash + Eq + Default, V: Clone + Default, const N: usize> {
    map: LinkedHashMap<K, V, N>,
    capacity: usize,
}

impl<K: Clone + Hash + Eq + Default, V: Clone + Default, const N: usize> LRUCache<K, V, N> {
    pub fn new(capacity: usize) -> LRUCache<K, V, N> {
        Self {
            map: LinkedHashMap::with_capacity(capacity),
            capacity,
        }
    }

    pub fn get<FF: Fn(&K) -> Option<V> + Send, EF: Fn(K, V) + Send>(
        &self,
        key: &K,
        fetch_fn: FF,
        evict_fn: EF,
    ) -> Option<V> {
        let res = self.map.get_to_front(key);
        if res.is_none() {
            if let Some(v) = fetch_fn(key) {
                self.map.insert_front(key.clone(), v.clone());
                while self.map.len() > self.capacity {
                    if let Some(evic) = self.map.pop_back() {
                        evict_fn(evic.0, evic.1)
                    }
                }
                return Some(v);
            } else {
                return None;
            }
        }
        res
    }

    pub fn remove(&self, key: &K) -> Option<V> {
        self.map.remove(key)
    }

    pub fn iter(&self) -> impl Iterator<Item = (K, V)> + '_ {
        self.map.iter_front()
    }
}

unsafe impl<K: Clone + Hash + Eq + Default, V: Clone + Default, const N: usize> Send
    for LRUCache<K, V, N>
{
}
