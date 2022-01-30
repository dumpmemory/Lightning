use crate::{linked_map::{LinkedHashMap, KVPair}, list::ListIter};
use std::hash::Hash;

pub struct LRUCache<K: Clone + Hash + Eq + Default, V: Clone + Default, const N: usize> {
    map: LinkedHashMap<K, V, N>,
    capacity: usize,
    fetch_fn: Box<dyn Fn(&K) -> Option<V>>,
    evict_fn: Box<dyn Fn(K, V)>,
}

impl <K: Clone + Hash + Eq + Default, V: Clone + Default, const N: usize> LRUCache<K, V, N> {
    pub fn new<FF: Fn(&K) -> Option<V> + 'static, EF: Fn(K, V) + 'static>(
        capacity: usize,
        fetch_fn: FF,
        evict_fn: EF,
    ) -> LRUCache<K, V, N> {
        Self {
            map: LinkedHashMap::with_capacity(capacity),
            fetch_fn: Box::new(fetch_fn),
            evict_fn: Box::new(evict_fn),
            capacity,
        }
    }

    pub fn get(&self, key: &K) -> Option<V> {
        let res = self.map.get_to_front(key);
        if res.is_none() {
            if let Some(v) = (self.fetch_fn)(key) {
                self.map.insert_front(key.clone(), v.clone());
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

    pub fn remove(&self, key: &K) -> Option<V> {
        self.map.remove(key)
    }

    pub fn iter(&self) -> ListIter<KVPair<K, V>, N> {
        self.map.iter_front()
    }
}

unsafe impl<K: Clone + Hash + Eq + Default, V: Clone + Default, const N: usize> Send
    for LRUCache<K, V, N>
{
}