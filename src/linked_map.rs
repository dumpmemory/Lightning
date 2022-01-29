// A concurrent linked hash map, fast and lock-free on iterate
use crate::list::{LinkedRingBufferList, ListIter};
use crate::map::{Map, ObjectMap, ObjectMapWriteGuard};
use crate::ring_buffer::ItemPtr;

pub struct LinkedObjectMap<T: Clone + Default, const N: usize> {
    map: ObjectMap<ItemPtr<(usize, T), N>>,
    list: LinkedRingBufferList<(usize, T), N>,
}

impl<T: Clone + Default, const N: usize> LinkedObjectMap<T, N> {
    pub fn with_capacity(cap: usize) -> Self {
        LinkedObjectMap {
            map: ObjectMap::with_capacity(cap),
            list: LinkedRingBufferList::new(),
        }
    }

    pub fn insert_front(&self, key: usize, value: T) {
        let pair = (key, value);
        let list_ref = self.list.push_front(pair);
        if let Some(old) = self.map.insert(&key, list_ref) {
            unsafe {
                old.remove();
            }
        }
    }

    pub fn insert_back(&self, key: usize, value: T) {
        let pair = (key, value);
        let list_ref = self.list.push_back(pair);
        if let Some(old) = self.map.insert(&key, list_ref) {
            unsafe {
                old.remove();
            }
        }
    }

    pub fn get(&self, key: usize) -> Option<T> {
        self.map
            .read(key)
            .map(|l| unsafe { (&*l).deref().clone().1 })
    }

    pub fn get_to_front(&self, key: usize) -> Option<T> {
        self.get_to_general(key, true)
    }

    pub fn get_to_back(&self, key: usize) -> Option<T> {
        self.get_to_general(key, false)
    }

    #[inline(always)]
    pub fn get_to_general(&self, key: usize, forwarding: bool) -> Option<T> {
        self.map
            .write(key)
            .and_then(|mut l| {
                let pair = unsafe { l.deref() }.clone();
                let new_ref = if forwarding {
                    self.list.push_front(pair)
                } else {
                    self.list.push_back(pair)
                };
                let old_ref = l.clone();
                *l = new_ref;
                unsafe {
                    old_ref.remove().map(|(_, v)| v)
                }
            })
    }

    pub fn remove(&self, key: usize) -> Option<T> {
        self.map
            .write(key)
            .and_then(|l| unsafe {
                ObjectMapWriteGuard::remove(l).remove().map(|(_, v)| v) 
            })
    }

    pub fn pop_front(&self) -> Option<(usize, T)> {
        self.pop_general(true)
    }

    pub fn pop_back(&self) -> Option<(usize, T)> {
        self.pop_general(false)
    }

    #[inline(always)]
    fn pop_general(&self, forwarding: bool) -> Option<(usize, T)> {
        loop {
            let list_item = if forwarding {
                self.list.peek_front()
            } else {
                self.list.peek_back()
            };
            if let Some(pair) = list_item {
                if let Some((k, v)) = pair.deref() {
                    if let Some(l) = self.map.write(k) {
                        unsafe {
                            ObjectMapWriteGuard::remove(l).remove();
                            return Some((k, v));
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

    pub fn contains_key(&self, key: &usize) -> bool {
        self.map.contains_key(key)
    }

    pub fn iter_front(&self) -> ListIter<(usize, T), N> {
        self.list.iter_front()
    }

    pub fn iter_back(&self) -> ListIter<(usize, T), N> {
        self.list.iter_back()
    }

    pub fn iter_front_keys(&self) -> KeyIter<T, N> {
        KeyIter { iter: self.iter_front() }
    }

    pub fn iter_back_keys(&self) -> KeyIter<T, N> {
        KeyIter { iter: self.iter_back() }
    }

    pub fn iter_front_values(&self) -> ValueIter<T, N> {
        ValueIter { iter: self.iter_front() }
    }

    pub fn iter_back_values(&self) -> ValueIter<T, N> {
        ValueIter { iter: self.iter_back() }
    }
}

pub struct KeyIter<'a, T: Clone + Default, const N: usize> {
    iter: ListIter<'a, (usize, T), N>
}

impl <'a, T: Clone + Default, const N: usize> Iterator for KeyIter<'a, T, N> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().and_then(|i| i.deref()).map(|(k, _)| k)
    }
}

pub struct ValueIter<'a, T: Clone + Default, const N: usize> {
    iter: ListIter<'a, (usize, T), N>
}

impl <'a, T: Clone + Default, const N: usize> Iterator for ValueIter<'a, T, N> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().and_then(|i| i.deref()).map(|(_, v)| v)
    }
}

#[cfg(test)]
mod test {
    use itertools::Itertools;

    use super::*;
    use std::{collections::HashSet, sync::Arc, thread};

    const CAP: usize = 16;

    #[test]
    pub fn linked_map_serial() {
        let map = LinkedObjectMap::<_, CAP>::with_capacity(16);
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
        let linked_map = Arc::new(LinkedObjectMap::<_, CAP>::with_capacity(16));
        let num_threads = num_cpus::get();
        let mut threads = vec![];
        let num_data = 999;
        for i in 0..num_threads {
            let map = linked_map.clone();
            threads.push(thread::spawn(move || {
                for j in 0..num_data {
                    let num = i * 1000 + j;
                    debug!("Insert {}", num);
                    if j % 2 == 1 {
                        map.insert_back(num, num);
                    } else {
                        map.insert_front(num, num);
                    }
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
        let mut num_set = HashSet::new();
        for pair in linked_map.iter_front() {
            let (key, node) = pair.deref().unwrap();
            let value = node;
            assert_eq!(key, value);
            num_set.insert(key);
        }
        assert_eq!(num_set.len(), num_threads * num_data);
    }
}
