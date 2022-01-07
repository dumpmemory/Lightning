// Design reference: https://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.140.4693&rep=rep1&type=pdf
// Lock-Free and Practical Doubly Linked List-Based Deques Using Single-Word Compare-and-Swap
// By: Hakan Sundell and Philippas Tsigas

use std::{
    mem,
    sync::atomic::{AtomicUsize, Ordering::*},
};

use crossbeam_epoch::{self as epoch, Atomic, Owned, Shared};
use crossbeam_utils::Backoff;
use epoch::Guard;

pub struct Node<T> {
    value: T,
    prev: Atomic<Self>,
    next: Atomic<Self>,
}

pub struct Deque<T: Clone> {
    head: Atomic<Node<T>>,
    tail: Atomic<Node<T>>,
}

const DELETED_TAG: usize = 1;
const EXISTED_TAG: usize = 0;

impl<T: Clone> Deque<T> {
    pub fn new() -> Self {
        let guard = crossbeam_epoch::pin();
        let head = Atomic::new(Node::null());
        let tail = Atomic::new(Node::null());
        let head_ptr = head.load(Relaxed, &guard);
        let tail_ptr = tail.load(Relaxed, &guard);
        unsafe {
            let head_node = head_ptr.deref();
            let tail_node = tail_ptr.deref();
            head_node.next.store(tail_ptr, Relaxed);
            tail_node.prev.store(head_ptr, Relaxed);
        }
        Self { head, tail }
    }

    pub fn insert_front(&mut self, value: T) {
        let backoff = crossbeam_utils::Backoff::new();
        let guard = crossbeam_epoch::pin();
        let curr = Owned::new(Node::new(value)).into_shared(&guard);
        let prev = self.head.load(Acquire, &guard);
        let mut next = unsafe { prev.deref().next.load(Acquire, &guard) };
        let curr_node = unsafe { curr.deref() };
        loop { // Find the prev node
            let (prev_node, prev_tag) = decomp_ptr(&prev);
            {
                let prev_next = prev_node.next.load(Acquire, &guard);
                if decomp_with_ptr(&prev_next) != (&next, EXISTED_TAG) {
                    next = prev_next;
                    backoff.spin();
                    continue;
                }
            }
            curr_node.prev.store(prev.with_tag(EXISTED_TAG), Relaxed);
            curr_node.next.store(next.with_tag(EXISTED_TAG), Relaxed);
            if prev_node
                .next
                .compare_exchange(next.with_tag(EXISTED_TAG), curr, AcqRel, Acquire, &guard)
                .is_ok()
            {
                break;
            }
            backoff.spin();
        }
        Self::insert_next_ops(&curr, &next, &guard, &backoff)
    }

    fn insert_next_ops<'a>(
        node_ptr: &Shared<'a, Node<T>>,
        next_ptr: &Shared<'a, Node<T>>,
        guard: &'a Guard,
        backoff: &Backoff,
    ) {
        loop {
            unsafe {
                let node = node_ptr.deref();
                let next = next_ptr.deref();
                let next_prev_ptr = next.prev.load(Acquire, guard);
                if next_prev_ptr.tag() == DELETED_TAG
                    || decomp_with_ptr(&node.next.load(Acquire, guard)) != (next_ptr, EXISTED_TAG)
                {
                    // When the previous node is deleted or target node is changed, do nothing
                    break;
                }
                if next
                    .prev
                    .compare_exchange(
                        next_prev_ptr,
                        node_ptr.with_tag(EXISTED_TAG),
                        AcqRel,
                        Acquire,
                        guard,
                    )
                    .is_ok() 
                    // Changed the prev pointer of next node to the node_ptr
                {
                    if node.prev
                }
                backoff.spin();
            }
        }
    }

    fn mark_prev_deleted<'a>(node_ptr: &'a Shared<Node<T>>, guard: &'a Guard) {
        loop {
            unsafe {
                let node = node_ptr.deref();
                let node_prev = node.prev.load(Acquire, guard);
                if node_prev.tag() == DELETED_TAG
                    || node
                        .prev
                        .compare_exchange(
                            node_prev,
                            node_prev.with_tag(DELETED_TAG),
                            AcqRel,
                            Acquire,
                            guard,
                        )
                        .is_ok()
                {
                    break;
                }
            }
        }
    }

    fn connect_prev<'a>(prev_ptr: &Shared<'a, Node<T>>, node_ptr: &Shared<'a, Node<T>>, guard: &'a Guard) -> Shared<'a, Node<T>> {
        unsafe {
            let mut last = Shared::null();
            let prev = prev_ptr.deref();
            loop {
                let prev_next_ptr = prev.next.load(Acquire, guard);
                if prev_next_ptr.is_null() {
                    if !last.is_null() {
                        Self::mark_prev_deleted(prev_ptr, guard);

                    }
                }
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
    fn null() -> Self {
        Self::new(unsafe { mem::zeroed() })
    }
}

pub fn decomp_ptr<'a, T: Clone>(ptr: &Shared<'a, Node<T>>) -> (&'a Node<T>, usize) {
    unsafe { (ptr.deref(), ptr.tag()) }
}

pub fn decomp_with_ptr<'a, 'b, T: Clone>(
    ptr: &'b Shared<'a, Node<T>>,
) -> (&'b Shared<'a, Node<T>>, usize) {
    (ptr, ptr.tag())
}

pub fn decomp_atomic<'a, T: Clone>(
    atomic: &Atomic<Node<T>>,
    guard: &'a Guard,
) -> (&'a Node<T>, usize) {
    let node_ref = atomic.load(Acquire, guard);
    decomp_ptr(&node_ref)
}
