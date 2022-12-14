// A concurrent linked hash map, fast and lock-free on iterate
use crate::list::{LinkedRingBufferList, ListIter};
use crate::map::{Map, PtrHashMap, PtrMutexGuard};
use crate::ring_buffer::ItemPtr;
use std::hash::Hash;

#[derive(Clone, Default)]
pub struct KVPair<K: Clone + Default, V: Clone + Default>(pub K, pub V);

pub struct LinkedHashMap<K: Clone + Hash + Eq + Default, V: Clone + Default, const N: usize> {
    map: PtrHashMap<K, ItemPtr<KVPair<K, V>, N>>,
    list: LinkedRingBufferList<KVPair<K, V>, N>,
}

impl<K: Clone + Hash + Eq + Default, V: Clone + Default, const N: usize> LinkedHashMap<K, V, N> {
    pub fn with_capacity(cap: usize) -> Self {
        LinkedHashMap {
            map: PtrHashMap::with_capacity(cap),
            list: LinkedRingBufferList::new(),
        }
    }

    pub fn insert_front(&self, key: K, value: V) -> Option<V> {
        let pair = KVPair(key.clone(), value);
        let list_ref = self.list.push_front(pair);
        if let Some(old) = self.map.insert(key, list_ref) {
            unsafe { old.remove().map(|KVPair(_, v)| v) }
        } else {
            None
        }
    }

    pub fn insert_back(&self, key: K, value: V) -> Option<V> {
        let pair = KVPair(key.clone(), value);
        let list_ref = self.list.push_back(pair);
        if let Some(old) = self.map.insert(key, list_ref) {
            unsafe { old.remove().map(|KVPair(_, v)| v) }
        } else {
            None
        }
    }

    pub fn get(&self, key: &K) -> Option<V> {
        self.map.get(key).map(|l| unsafe { l.deref().unwrap().1.clone() })
    }

    pub fn get_to_front(&self, key: &K) -> Option<V> {
        self.get_to_general(key, true)
    }

    pub fn get_to_back(&self, key: &K) -> Option<V> {
        self.get_to_general(key, false)
    }

    #[inline(always)]
    pub fn get_to_general(&self, key: &K, forwarding: bool) -> Option<V> {
        self.map.lock(key).and_then(|mut l| {
            let pair = unsafe { l.deref() }.unwrap();
            let new_ref = if forwarding {
                self.list.push_front(pair)
            } else {
                self.list.push_back(pair)
            };
            let old_ref = l.clone();
            *l = new_ref;
            unsafe { old_ref.remove().map(|KVPair(_, v)| v) }
        })
    }

    pub fn remove(&self, key: &K) -> Option<V> {
        self.map
            .lock(key)
            .and_then(|l| unsafe { PtrMutexGuard::remove(l).remove().map(|KVPair(_, v)| v) })
    }

    pub fn pop_front(&self) -> Option<KVPair<K, V>> {
        self.pop_general(true)
    }

    pub fn pop_back(&self) -> Option<KVPair<K, V>> {
        self.pop_general(false)
    }

    #[inline(always)]
    fn pop_general(&self, forwarding: bool) -> Option<KVPair<K, V>> {
        loop {
            let list_item = if forwarding {
                self.list.peek_front()
            } else {
                self.list.peek_back()
            };
            if let Some(pair) = list_item {
                if let Some(KVPair(k, v)) = pair.deref() {
                    if let Some(l) = self.map.lock(&k) {
                        unsafe {
                            PtrMutexGuard::remove(l).remove();
                            return Some(KVPair(k, v));
                        }
                    }
                }
            } else {
                return None;
            }
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.map.contains_key(key)
    }

    pub fn iter_front(&self) -> ListIter<KVPair<K, V>, N> {
        self.list.iter_front()
    }

    pub fn iter_back(&self) -> ListIter<KVPair<K, V>, N> {
        self.list.iter_back()
    }

    pub fn iter_front_keys(&self) -> KeyIter<K, V, N> {
        KeyIter {
            iter: self.iter_front(),
        }
    }

    pub fn iter_back_keys(&self) -> KeyIter<K, V, N> {
        KeyIter {
            iter: self.iter_back(),
        }
    }

    pub fn iter_front_values(&self) -> ValueIter<K, V, N> {
        ValueIter {
            iter: self.iter_front(),
        }
    }

    pub fn iter_back_values(&self) -> ValueIter<K, V, N> {
        ValueIter {
            iter: self.iter_back(),
        }
    }
}

pub struct KeyIter<'a, K: Clone + Hash + Default, V: Clone + Default, const N: usize> {
    iter: ListIter<'a, KVPair<K, V>, N>,
}

impl<'a, K: Clone + Hash + Default, V: Clone + Default, const N: usize> Iterator
    for KeyIter<'a, K, V, N>
{
    type Item = K;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .and_then(|i| i.deref())
            .map(|KVPair(k, _)| k)
    }
}

pub struct ValueIter<'a, K: Clone + Hash + Default, V: Clone + Default, const N: usize> {
    iter: ListIter<'a, KVPair<K, V>, N>,
}

impl<'a, K: Clone + Hash + Default, V: Clone + Default, const N: usize> Iterator
    for ValueIter<'a, K, V, N>
{
    type Item = V;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .and_then(|i| i.deref())
            .map(|KVPair(_, v)| v)
    }
}

impl<K: Clone + Default, V: Clone + Default> KVPair<K, V> {
    pub fn key(&self) -> &K {
        &self.0
    }

    pub fn value(&self) -> &V {
        &self.1
    }

    pub fn pair(&self) -> (&K, &V) {
        (&self.0, &self.1)
    }
}

unsafe impl<K: Clone + Hash + Eq + Default, V: Clone + Default, const N: usize> Send
    for LinkedHashMap<K, V, N>
{
}

#[cfg(test)]
mod test {
    use itertools::Itertools;

    use super::*;
    use std::{collections::HashSet, sync::Arc, thread};

    const CAP: usize = 16;

    #[test]
    pub fn linked_map_serial() {
        let map = LinkedHashMap::<_, _, CAP>::with_capacity(16);
        for i in 0..1024 {
            map.insert_front(i, i);
        }
        for i in 1024..2048 {
            map.insert_back(i, i);
        }
    }

    #[test]
    pub fn linked_map_insertions() {
        let _ = env_logger::try_init();
        let linked_map = Arc::new(LinkedHashMap::<_, _, CAP>::with_capacity(4));
        let num_threads = 32;
        let mut threads = vec![];
        let num_data = 32;
        for i in 0..num_threads {
            let map = linked_map.clone();
            threads.push(thread::spawn(move || {
                for j in 0..num_data {
                    let num = i * 1000 + j;
                    debug!("Insert {}", num);
                    if j % 2 == 1 {
                        map.insert_back(num, Arc::new(num));
                    } else {
                        map.insert_front(num, Arc::new(num));
                    }
                    assert_eq!(map.get(&num).map(|n| *n), Some(num));
                }
                map.iter_front_keys().collect_vec();
                map.iter_front_values().collect_vec();
                map.iter_front().collect_vec();
                map.iter_back_keys().collect_vec();
                map.iter_back_values().collect_vec();
                map.iter_back().collect_vec();
            }));
        }
        info!("Waiting for threads to finish");
        for t in threads {
            t.join().unwrap();
        }
        for i in 0..num_threads {
            for j in 0..num_data {
                let num = i * 1000 + j;
                let first_round = linked_map.get(&num).map(|n| *n);
                if first_round == Some(num) {
                    continue;
                }
                for round_count in 0..99 {
                    let following_round = linked_map.get(&num).map(|n| *n);
                    if following_round == Some(num) {
                        info!("Falling back for i {}, j {}, at {}", i, j, round_count);
                        break;
                    }
                }
                linked_map.map.table.dump_dist();
                error!("Cannot fall back for i {}, j {}", i, j);
            }
        }
        let mut num_set = HashSet::new();
        for pair in linked_map.iter_front() {
            let KVPair(key, node) = pair.deref().unwrap();
            let value = node;
            assert_eq!(key, *value);
            num_set.insert(key);
        }
        assert_eq!(num_set.len(), num_threads * num_data);
    }
}
