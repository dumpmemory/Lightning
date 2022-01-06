// Design reference: https://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.140.4693&rep=rep1&type=pdf
// Lock-Free and Practical Doubly Linked List-Based Deques Using Single-Word Compare-and-Swap
// By: Hakan Sundell and Philippas Tsigas

use std::sync::atomic::{AtomicUsize, Ordering::*};

use crossbeam_epoch::{self as epoch, Atomic, Owned, Shared};
use epoch::Guard;

pub struct Node<T: Clone> {
    value: T,
    prev: Atomic<Self>,
    next: Atomic<Self>,
}

pub struct Deque<T: Clone> {
    head: Atomic<Node<T>>,
    tail: Atomic<Node<T>>,
    len: AtomicUsize,
}

const DELETED_TAG: usize = 1;
const EXISTED_TAG: usize = 0;

impl<T: Clone> Deque<T> {
    pub fn new() -> Self {
        Self {
            head: Atomic::null(),
            tail: Atomic::null(),
            len: AtomicUsize::new(0),
        }
    }

    pub fn insert_front(&mut self, value: T) {
        let guard = crossbeam_epoch::pin();
        let curr = Owned::new(Node::new(value)).into_shared(&guard);
        let mut prev = self.head.load(Acquire, &guard);
        let mut next = unsafe { prev.deref().next.load(Acquire, &guard) };
        let curr_node = unsafe { curr.deref() };
        loop {
            let (prev_node, prev_tag) = decomp_ptr(&prev);
            if prev_node.next.load(Acquire, &guard) != next || prev_tag != EXISTED_TAG {
                next = prev_node.next.load(Acquire, &guard);
                continue;
            }
            curr_node.prev.store(prev.with_tag(EXISTED_TAG), Release);
            curr_node.next.store(next.with_tag(EXISTED_TAG), Release);
            if prev_node
                .next
                .compare_exchange(next.with_tag(EXISTED_TAG), curr, AcqRel, Acquire, &guard)
                .is_ok()
            {
                break;
            }
        }
        
    }
}

impl<T: Clone> Node<T> {
    fn new(value: T) -> Self {
        Self {
            value,
            prev: Atomic::null(),
            next: Atomic::null(),
        }
    }
}

pub fn decomp_ptr<'a, T: Clone>(ptr: &Shared<'a, Node<T>>) -> (&'a Node<T>, usize) {
    unsafe { (ptr.deref(), ptr.tag()) }
}

pub fn decomp_atomic<'a, T: Clone>(
    atomic: &Atomic<Node<T>>,
    guard: &'a Guard,
) -> (&'a Node<T>, usize) {
    let node_ref = atomic.load(Acquire, guard);
    decomp_ptr(&node_ref)
}
