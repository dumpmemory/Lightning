// Design reference: https://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.140.4693&rep=rep1&type=pdf
// Lock-Free and Practical Doubly Linked List-Based Deques Using Single-Word Compare-and-Swap
// By: Hakan Sundell and Philippas Tsigas

use std::{
    mem,
    sync::atomic::{fence, AtomicUsize, Ordering::*},
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

    pub fn insert_front(&self, value: T, guard: &Guard) {
        let backoff = crossbeam_utils::Backoff::new();
        let curr = Owned::new(Node::new(value)).into_shared(&guard);
        let prev = self.head.load(Relaxed, &guard);
        let prev_node = unsafe { prev.deref() };
        let mut next = unsafe { prev.deref().next.load(Acquire, &guard) };
        let curr_node = unsafe { curr.deref() };
        loop {
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
                .compare_exchange(
                    next.with_tag(EXISTED_TAG),
                    curr.with_tag(EXISTED_TAG),
                    AcqRel,
                    Acquire,
                    &guard,
                )
                .is_ok()
            {
                break;
            }
            backoff.spin();
        } // I
        fence(SeqCst);
        Self::insert_next_ops(&curr, &next, &guard, &backoff)
    }

    pub fn insert_back(&self, value: T, guard: &Guard) {
        let backoff = crossbeam_utils::Backoff::new();
        let curr = Owned::new(Node::new(value)).into_shared(&guard);
        let next = self.tail.load(Relaxed, &guard);
        let curr_node = unsafe { curr.deref() };
        loop {
            let prev = unsafe { next.deref().prev.load(Acquire, &guard) };
            let prev_node = unsafe { prev.deref() };
            let prev_next = prev_node.next.load(Acquire, &guard);
            if decomp_with_ptr(&prev_next) != (&next, EXISTED_TAG) {
                // prev node does not have tail node as its next node
                // TODO: make sure deletion will fix this
                continue;
            }
            curr_node.prev.store(prev.with_tag(EXISTED_TAG), Relaxed);
            curr_node.next.store(next.with_tag(EXISTED_TAG), Relaxed);
            if prev_node
                .next
                .compare_exchange(
                    next.with_tag(EXISTED_TAG),
                    curr.with_tag(EXISTED_TAG),
                    AcqRel,
                    Acquire,
                    &guard,
                )
                .is_ok()
            {
                break;
            }
            backoff.spin();
        }
        fence(SeqCst); // I
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
                    // II
                    // TODO: Analyze scenarios when current node is removed
                    // if node.prev.load(Acquire, guard).tag() == DELETED_TAG {
                    //     // Current node is removed
                    // }
                }
                backoff.spin();
            }
        }
    }

    pub fn remove_front<'a>(&self, guard: &'a Guard) -> Option<Shared<'a, Node<T>>> {
        let backoff = crossbeam_utils::Backoff::new();
        let prev = self.head.load(Acquire, guard);
        let prev_node = unsafe { prev.deref() };
        loop {
            let curr = prev_node.next.load(Acquire, guard);
            if curr == self.tail.load(Relaxed, guard) {
                // End of list
                return None;
            }
            let curr_node = unsafe { curr.deref() };
            let curr_next = curr_node.next.load(Acquire, guard);
            if curr_next.tag() == DELETED_TAG {
                // TODO: Make sure current node is properly removed
                backoff.spin();
                continue;
            }
            if curr_node
                .next
                .compare_exchange(
                    curr_next,
                    curr_next.with_tag(DELETED_TAG),
                    AcqRel,
                    Acquire,
                    guard,
                )
                .is_ok()
            {
                // I
                Self::unlink_node(&curr, guard, &backoff);
                return Some(curr);
            }
        }
    }

    pub fn remove_back<'a>(&self, guard: &'a Guard) -> Option<Shared<'a, Node<T>>> {
        let backoff = crossbeam_utils::Backoff::new();
        let next_ptr = self.tail.load(Relaxed, guard);
        let next = unsafe { next_ptr.deref() };
        loop {
            let node_ptr = next.prev.load(Acquire, guard);
            let node = unsafe { node_ptr.deref() };
            if node.next.load(Acquire, guard) != next_ptr.with_tag(EXISTED_TAG) {
                Self::link_prev(&node_ptr, &next_ptr, guard, &backoff);
                continue;
            }
            if node_ptr == self.head.load(Relaxed, guard) {
                return None;
            }
            if node
                .next
                .compare_exchange(
                    next_ptr.with_tag(EXISTED_TAG),
                    next_ptr.with_tag(DELETED_TAG),
                    AcqRel,
                    Acquire,
                    guard,
                )
                .is_ok()
            {
                Self::unlink_node(&node_ptr, guard, &backoff);
                return Some(node_ptr.clone());
            }
            backoff.spin();
        }
    }

    fn mark_prev_deleted<'a>(
        node_ptr: &'a Shared<'a, Node<T>>,
        guard: &'a Guard,
        backoff: &Backoff,
    ) {
        unsafe {
            let node = node_ptr.deref();
            loop {
                let node_prev_ptr = node.prev.load(Acquire, guard);
                if node_prev_ptr.tag() == DELETED_TAG
                    || node
                        .prev
                        .compare_exchange(
                            node_prev_ptr,
                            node_prev_ptr.with_tag(DELETED_TAG),
                            AcqRel,
                            Acquire,
                            guard,
                        )
                        .is_ok()
                {
                    break;
                }
                backoff.spin();
            }
        }
    }

    fn link_prev<'a>(
        prev_ptr: &Shared<'a, Node<T>>,
        curr_ptr: &Shared<'a, Node<T>>,
        guard: &'a Guard,
        backoff: &Backoff,
    ) {
        let mut prev_ptr = prev_ptr.clone();
        loop {
            let prev = unsafe { prev_ptr.deref() };
            let prev_next_ptr = prev.next.load(Acquire, guard);
            if prev_next_ptr.tag() == DELETED_TAG {
                prev_ptr = prev.prev.load(Acquire, guard);
                continue;
            }
            if let Err(other_prev_next) = prev.next.compare_exchange(
                prev_next_ptr,
                curr_ptr.with_tag(EXISTED_TAG),
                AcqRel,
                Acquire,
                guard,
            ) {
                prev_ptr = other_prev_next.current;
                continue;
            }
            break;
        }
    }

    fn unlink_node<'a>(node_ptr: &Shared<'a, Node<T>>, guard: &'a Guard, backoff: &Backoff) {
        fence(SeqCst);
        Self::mark_prev_deleted(node_ptr, guard, backoff); // II
        fence(SeqCst);
        unsafe {
            let node = node_ptr.deref();
            let mut prev_ptr = node.prev.load(Acquire, guard);
            let mut next_ptr = node.next.load(Acquire, guard);
            loop {
                let prev = prev_ptr.deref();
                let next = next_ptr.deref();
                let next_prev_ptr = next.prev.load(Acquire, guard);
                let next_next_ptr = next.next.load(Acquire, guard);
                if next_next_ptr.tag() == DELETED_TAG {
                    // the next node we are going to work on is also deleted
                    // move on to the next of the next node
                    next_ptr = next_next_ptr;
                    backoff.spin();
                    continue;
                }
                debug_assert_eq!(next_prev_ptr.tag(), EXISTED_TAG);
                let prev_prev_ptr = prev.prev.load(Acquire, guard);
                let prev_next_ptr = prev.next.load(Acquire, guard);
                if prev_next_ptr.tag() == DELETED_TAG {
                    prev_ptr = prev_prev_ptr;
                    backoff.spin();
                    continue;
                }
                debug_assert_eq!(prev_prev_ptr.tag(), EXISTED_TAG);
                if let Err(new_prev_next) = prev.next.compare_exchange(
                    prev_next_ptr,
                    next_ptr.with_tag(EXISTED_TAG),
                    AcqRel,
                    Acquire,
                    guard,
                ) {
                    if new_prev_next.current.tag() == EXISTED_TAG {
                        // prev next have to been changed to other node
                        // Move prev to that node next to current prev
                        prev_ptr = new_prev_next.current;
                    } else {
                        // prev have been deleted
                        prev_ptr = prev_prev_ptr;
                    }
                    backoff.spin();
                    continue;
                } // III
                if next
                    .prev
                    .compare_exchange(
                        next_prev_ptr,
                        prev_ptr.with_tag(EXISTED_TAG),
                        AcqRel,
                        Release,
                        guard,
                    )
                    .is_ok()
                {
                    break;
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
