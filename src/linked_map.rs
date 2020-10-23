use crate::map::{ObjectMap, Map};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use std::ops::Deref;
use parking_lot::Mutex;

const NONE_KEY: usize = 0;

type NodeRef<T> = Arc<Node<T>>;

pub struct Node<T> {
    // Prev and next node keys
    lock: Mutex<()>,
    prev: AtomicUsize,
    next: AtomicUsize,
    obj: T,
}

pub struct LinkedWordMap<T> {
    map: ObjectMap<NodeRef<T>>,
    head: AtomicUsize,
    tail: AtomicUsize,
}

impl <T>LinkedWordMap<T> {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            map: ObjectMap::with_capacity(cap),
            head: AtomicUsize::new(NONE_KEY),
            tail: AtomicUsize::new(NONE_KEY),
        }
    }

    pub fn put_front(&self, key: &usize, value: T) {
        let backoff = crossbeam_utils::Backoff::new();
        let new_front = Node::new(value, NONE_KEY, NONE_KEY);
        let _new_guard = new_front.lock.lock();
        self.map.insert(key, new_front.clone());
        loop {
            let front = self.head.load(Relaxed);
            let front_node = self.map.get(&front);
            let _front_guard = front_node.as_ref().map(|n| n.lock.lock());
            if let Some(ref front_node) = front_node {
                if front_node.get_prev() != NONE_KEY {
                    continue;
                }
            }
            new_front.set_next(front);
            if self.head.compare_and_swap(front, *key, Relaxed) == front {
                if let Some(ref front_node) = front_node {
                    front_node.prev.store(*key, SeqCst);
                } else {
                    debug_assert_eq!(front, NONE_KEY);
                    self.tail.compare_and_swap(NONE_KEY, *key, SeqCst);
                }
                break;
            } else {
                backoff.spin();
            }
        }
    }

    pub fn put_back(&self, key: &usize, value: T) {
        let backoff = crossbeam_utils::Backoff::new();
        let new_back = Node::new(value, NONE_KEY, NONE_KEY);
        let _new_guard = new_back.lock.lock();
        self.map.insert(key, new_back.clone());
        loop {
            let back = self.tail.load(Relaxed);
            let back_node = self.map.get(&back);
            let _back_guard = back_node.as_ref().map(|n| n.lock.lock());
            if let Some(ref back_node) = back_node {
                if back_node.get_next() != NONE_KEY {
                    continue;
                }
            }
            new_back.set_prev(back);
            if self.tail.compare_and_swap(back, *key, Relaxed) == back {
                if let Some(ref back_node) = back_node{
                    back_node.next.store(*key, SeqCst);
                } else {
                    debug_assert_eq!(back, NONE_KEY);
                    self.head.compare_and_swap(NONE_KEY, *key, SeqCst);
                }
                break;
            } else {
                backoff.spin();
            }
        }
    }
}

impl <T> Node<T> {
    pub fn new(obj: T, prev: usize, next: usize) -> NodeRef<T> {
        Arc::new(Self {
            obj, 
            lock: Mutex::new(()),
            prev: AtomicUsize::new(prev),
            next: AtomicUsize::new(next)
        })
    }
    
    fn get_next(&self) -> usize {
        self.next.load(Relaxed)
    }

    fn get_prev(&self) -> usize {
        self.prev.load(Relaxed)
    }
    
    fn cas_next(&self, current: usize, new: usize) -> usize {
        self.next.compare_and_swap(current, new, Relaxed)
    }
    
    fn cas_prev(&self, current: usize, new: usize) -> usize {
        self.prev.compare_and_swap(current, new, Relaxed)
    }

    fn set_next(&self, new: usize) {
        self.next.store(new, Relaxed)
    }
    
    fn set_prev(&self, new: usize) {
        self.prev.store(new, Relaxed)
    }
}

impl <T> Deref for Node<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.obj
    }
}