// A concurrent linked hash map, fast and lock-free on iterate

use crate::map::{Map, ObjectMap};
use crate::spin::SpinLock;
use std::ops::Deref;
use std::sync::atomic::Ordering::{Acquire, AcqRel, Release};
use std::sync::atomic::{fence, AtomicUsize};
use std::sync::Arc;

const NONE_KEY: usize = !0 >> 1;

pub type NodeRef<T> = Arc<Node<T>>;

pub struct Node<T> {
    // Prev and next node keys
    lock: SpinLock<()>,
    prev: AtomicUsize,
    next: AtomicUsize,
    obj: T,
}

pub struct LinkedObjectMap<T> {
    map: ObjectMap<NodeRef<T>>,
    head: AtomicUsize,
    tail: AtomicUsize,
}

impl<T> LinkedObjectMap<T> {
    pub fn with_capacity(cap: usize) -> Self {
        LinkedObjectMap {
            map: ObjectMap::with_capacity(cap),
            head: AtomicUsize::new(NONE_KEY),
            tail: AtomicUsize::new(NONE_KEY),
        }
    }

    pub fn insert_front(&self, key: &usize, value: T) {
        debug_assert_ne!(*key, NONE_KEY);
        let backoff = crossbeam_utils::Backoff::new();
        let new_front = Node::new(value, NONE_KEY, NONE_KEY);
        if let Some(existed_node) = self.map.insert(key, new_front.clone()) {
            // TODO: Eliminate the hazard when the node does not existed in the linked list
            self.remove_node(*key, existed_node);
        }
        let _new_guard = new_front.lock.lock();
        loop {
            let front = self.head.load(Acquire);
            let front_node = self.map.get(&front);
            let _front_guard = front_node.as_ref().map(|n| n.lock.lock());
            if let Some(ref front_node) = front_node {
                if front_node.get_prev() != NONE_KEY {
                    backoff.spin();
                    continue;
                }
            } else if front != NONE_KEY {
                // Inconsistent with map, will spin wait
                backoff.spin();
                continue;
            }
            new_front.set_next(front);
            if self.head.compare_and_swap(front, *key, AcqRel) == front {
                if let Some(ref front_node) = front_node {
                    front_node.prev.store(*key, Release);
                } else {
                    debug_assert_eq!(front, NONE_KEY);
                    self.tail.compare_and_swap(NONE_KEY, *key, AcqRel);
                }
                break;
            } else {
                backoff.spin();
            }
        }
    }

    pub fn insert_back(&self, key: &usize, value: T) {
        debug_assert_ne!(*key, NONE_KEY);
        let backoff = crossbeam_utils::Backoff::new();
        let new_back = Node::new(value, NONE_KEY, NONE_KEY);
        let _new_guard = new_back.lock.lock();
        if let Some(existed_node) = self.map.insert(key, new_back.clone()) {
            // TODO: Eliminate the hazard when the node does not existed in the linked list
            self.remove_node(*key, existed_node);
        }
        loop {
            let back = self.tail.load(Acquire);
            let back_node = self.map.get(&back);
            let _back_guard = back_node.as_ref().map(|n| n.lock.lock());
            if let Some(ref back_node) = back_node {
                if back_node.get_next() != NONE_KEY {
                    backoff.spin();
                    continue;
                }
            } else if back != NONE_KEY {
                backoff.spin();
                continue;
            }
            new_back.set_prev(back);
            if self.tail.compare_and_swap(back, *key, AcqRel) == back {
                if let Some(ref back_node) = back_node {
                    back_node.next.store(*key, Release);
                } else {
                    debug_assert_eq!(back, NONE_KEY);
                    self.head.compare_and_swap(NONE_KEY, *key, AcqRel);
                }
                break;
            } else {
                backoff.spin();
            }
        }
    }

    pub fn get(&self, key: &usize) -> Option<NodeRef<T>> {
        self.map.get(key)
    }

    pub fn remove(&self, key: &usize) -> Option<NodeRef<T>> {
        let val = self.map.get(key);
        if let Some(val_node) = val {
            self.remove_node(*key, val_node);
            return self.map.remove(key);
        } else {
            return val;
        }
    }

    fn remove_node(&self, key: usize, val_node: NodeRef<T>) {
        let backoff = crossbeam_utils::Backoff::new();
        loop {
            let prev = val_node.get_prev();
            let next = val_node.get_next();
            let prev_node = self.map.get(&prev);
            let next_node = self.map.get(&next);
            if (prev != NONE_KEY && prev_node.is_none())
                || (next != NONE_KEY && next_node.is_none())
            {
                backoff.spin();
                continue;
            }
            // Lock 3 nodes, from left to right to avoid dead lock
            let _prev_guard = prev_node.as_ref().map(|n| n.lock.lock());
            let _self_guard = val_node.lock.lock();
            let _next_guard = next_node.as_ref().map(|n| n.lock.lock());
            // Validate 3 nodes, retry on failure
            if {
                prev_node
                    .as_ref()
                    .map(|n| n.get_next() != key)
                    .unwrap_or(false)
                    | (val_node.get_prev() != prev)
                    | (val_node.get_next() != next)
                    | next_node
                        .as_ref()
                        .map(|n| n.get_prev() != key)
                        .unwrap_or(false)
            } {
                backoff.spin();
                continue;
            }
            // Bacause all the nodes we are about to modify are locked, we shall use store
            // instead of CAS
            prev_node.as_ref().map(|n| n.set_next(next));
            next_node.as_ref().map(|n| n.set_prev(prev));
            if prev_node.is_none() {
                debug_assert_eq!(self.head.load(Acquire), key);
                self.head.store(next, Release);
            }
            if next_node.is_none() {
                debug_assert_eq!(self.tail.load(Acquire), key);
                self.tail.store(prev, Release);
            }
            return;
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn contains_key(&self, key: &usize) -> bool {
        self.map.contains_key(key)
    }

    pub fn all_pairs(&self) -> Vec<(usize, NodeRef<T>)> {
        let mut res = vec![];
        let mut node_key = self.head.load(Acquire);
        loop {
            if let Some(node) = self.map.get(&node_key) {
                let new_node_key = node.get_next();
                res.push((node_key, node));
                node_key = new_node_key;
            } else if node_key == NONE_KEY {
                break;
            } else {
                unreachable!();
            }
        }
        res
    }

    pub fn all_keys(&self) -> Vec<usize> {
        let mut res = vec![];
        let mut node_key = self.head.load(Acquire);
        loop {
            if let Some(node) = self.map.get(&node_key) {
                res.push(node_key);
                node_key = node.get_next();
            } else if node_key == NONE_KEY {
                break;
            } else {
                unreachable!();
            }
        }
        res
    }

    pub fn all_values(&self) -> Vec<NodeRef<T>> {
        let mut res = vec![];
        let mut node_key = self.head.load(Acquire);
        loop {
            if let Some(node) = self.map.get(&node_key) {
                node_key = node.get_next();
                res.push(node);
            } else if node_key == NONE_KEY {
                break;
            } else {
                unreachable!();
            }
        }
        res
    }
}

impl<T> Node<T> {
    pub fn new(obj: T, prev: usize, next: usize) -> NodeRef<T> {
        Arc::new(Self {
            obj,
            lock: SpinLock::new(()),
            prev: AtomicUsize::new(prev),
            next: AtomicUsize::new(next),
        })
    }

    fn get_next(&self) -> usize {
        self.next.load(Acquire)
    }

    fn get_prev(&self) -> usize {
        self.prev.load(Acquire)
    }

    fn set_next(&self, new: usize) {
        self.next.store(new, Release)
    }

    fn set_prev(&self, new: usize) {
        self.prev.store(new, Release)
    }
}

impl<T> Deref for Node<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.obj
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::thread;

    #[test]
    pub fn linked_map_serial() {
        let map = LinkedObjectMap::with_capacity(16);
        for i in 0..1024 {
            map.insert_front(&i, i);
        }
        for i in 1024..2048 {
            map.insert_back(&i, i);
        }
    }

    #[test]
    pub fn linked_map_insertions() {
        let _ = env_logger::try_init();
        let linked_map = Arc::new(LinkedObjectMap::with_capacity(16));
        let num_threads = 16;
        let mut threads = vec![];
        for i in 0..num_threads {
            let map = linked_map.clone();
            threads.push(thread::spawn(move || {
                for j in 0..999 {
                    let num = i * 1000 + j;
                    debug!("Insert {}", num);
                    if j % 2 == 1 {
                        map.insert_back(&num, num);
                    } else {
                        map.insert_front(&num, num);
                    }
                    map.all_keys();
                    map.all_values();
                    map.all_pairs();
                }
            }));
        }
        info!("Waiting for threads to finish");
        for t in threads {
            t.join().unwrap();
        }
    }
}
