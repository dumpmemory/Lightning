use crossbeam_epoch::*;
use crossbeam_utils::Backoff;

use crate::{ring_buffer::{RingBuffer, ItemRef}};
use parking_lot::Mutex;
use std::sync::atomic::Ordering::*;

// A mostly lock-free list with linked ring buffers

pub struct LinkedRingBufferList<T, const B: usize> {
    head: Atomic<RingBufferNode<T, B>>,
    tail: Atomic<RingBufferNode<T, B>>,
}

pub struct RingBufferNode<T, const N: usize> {
    buffer: RingBuffer<T, N>,
    prev: Atomic<Self>,
    next: Atomic<Self>,
    lock: Mutex<()>,
}

impl<T: Clone + Default, const N: usize> LinkedRingBufferList<T, N> {
    pub fn new() -> Self {
        let guard = crossbeam_epoch::pin();
        let head_ptr = Owned::new(RingBufferNode::new()).into_shared(&guard);
        let tail_ptr = Owned::new(RingBufferNode::new()).into_shared(&guard);
        let head_node = unsafe { head_ptr.deref() };
        let tail_node = unsafe { tail_ptr.deref() };
        head_node.next.store(tail_ptr, Relaxed);
        tail_node.prev.store(head_ptr, Relaxed);
        Self {
            head: Atomic::from(head_ptr),
            tail: Atomic::from(tail_ptr),
        }
    }
    pub fn push_front(&self, mut val: T) {
        let guard = crossbeam_epoch::pin();
        let backoff = Backoff::new();
        loop {
            let head_ptr = self.head.load(Acquire, &guard);
            let head_node = unsafe { head_ptr.deref() };
            if let Err(v) = head_node.buffer.push_front(val) {
                let head_lock = head_node.lock.try_lock();
                if head_lock.is_some() && head_node.prev.load(Acquire, &guard).is_null() {
                    let new_node = RingBufferNode::new();
                    new_node.next.store(head_ptr, Relaxed);
                    let new_node_ptr = Owned::new(new_node).into_shared(&guard);
                    let _new_node_lock = unsafe { new_node_ptr.deref().lock.lock() };
                    if self
                        .head
                        .compare_exchange(head_ptr, new_node_ptr, AcqRel, Acquire, &guard)
                        .is_ok()
                    {
                        head_node.prev.store(new_node_ptr, Release);
                    }
                }
                val = v;
                backoff.spin();
            } else {
                return;
            }
        }
    }

    pub fn push_back(&self, mut val: T) {
        let guard = crossbeam_epoch::pin();
        let backoff = Backoff::new();
        loop {
            let tail_ptr = self.tail.load(Acquire, &guard);
            let tail_node = unsafe { tail_ptr.deref() };
            if let Err(v) = tail_node.buffer.push_back(val) {
                let tail_lock = tail_node.lock.try_lock();
                if tail_lock.is_some() && tail_node.next.load(Acquire, &guard).is_null() {
                    let new_node = RingBufferNode::new();
                    new_node.prev.store(tail_ptr, Relaxed);
                    let new_node_ptr = Owned::new(new_node).into_shared(&guard);
                    let _new_node_lock = unsafe { new_node_ptr.deref().lock.lock() };
                    if self
                        .tail
                        .compare_exchange(tail_ptr, new_node_ptr, AcqRel, Acquire, &guard)
                        .is_ok()
                    {
                        tail_node.next.store(new_node_ptr, Release);
                    }
                }
                val = v;
                backoff.spin();
            } else {
                return;
            }
        }
    }

    pub fn pop_front(&self) -> Option<T> {
        let guard = crossbeam_epoch::pin();
        let backoff = Backoff::new();
        loop {
            let head_ptr = self.head.load(Acquire, &guard);
            let head_node = unsafe { head_ptr.deref() };
            if let Some(obj) = head_node.buffer.pop_front() {
                return Some(obj);
            }
            // Prev node is empty, shall move to next node
            let mut remains = vec![];
            {
                let head_next = head_node.next.load(Acquire, &guard);
                if head_next.is_null() {
                    backoff.spin();
                    continue;
                }
                let head_next_node = unsafe { head_next.deref() };
                if head_next_node.next.load(Acquire, &guard).is_null() {
                    // Approching back most node, shall not update head
                    return head_next_node.buffer.pop_front();
                }
                let head_lock = head_node.lock.try_lock();
                let head_next_lock = head_next_node.lock.try_lock();
                if head_lock.is_some()
                    && head_next_lock.is_some()
                    && head_node.prev.load(Acquire, &guard).is_null()
                    && head_node.next.load(Acquire, &guard) == head_next
                    && head_next_node.prev.load(Acquire, &guard) == head_ptr
                    && self
                        .head
                        .compare_exchange(head_ptr, head_next, AcqRel, Acquire, &guard)
                        .is_ok()
                {
                    head_next_node.prev.store(Shared::null(), Release);
                    head_node.prev.store(Shared::null(), Release);
                    head_node.next.store(Shared::null(), Release);
                    remains = head_node.buffer.pop_all();
                    unsafe {
                        guard.defer_destroy(head_ptr);
                    }
                }
            }
            for r in remains {
                self.push_front(r);
            }
            backoff.spin();
        }
    }

    pub fn pop_back(&self) -> Option<T> {
        let guard = crossbeam_epoch::pin();
        let backoff = Backoff::new();
        loop {
            let tail_ptr = self.tail.load(Acquire, &guard);
            let tail_node = unsafe { tail_ptr.deref() };
            if let Some(obj) = tail_node.buffer.pop_back() {
                return Some(obj);
            }
            // Prev node is empty, shall move to next node
            let mut remains = vec![];
            {
                let tail_prev = tail_node.prev.load(Acquire, &guard);
                if tail_prev.is_null() {
                    backoff.spin();
                    continue;
                }
                let tail_prev_node = unsafe { tail_prev.deref() };
                if tail_prev_node.prev.load(Acquire, &guard).is_null() {
                    // Approching back most node, shall not update head
                    return tail_prev_node.buffer.pop_back();
                }
                let tail_prev_lock = tail_prev_node.lock.try_lock();
                let tail_lock = tail_node.lock.try_lock();
                if tail_prev_lock.is_some()
                    && tail_lock.is_some()
                    && tail_node.next.load(Acquire, &guard).is_null()
                    && tail_node.prev.load(Acquire, &guard) == tail_prev
                    && tail_prev_node.next.load(Acquire, &guard) == tail_ptr
                    && self
                        .tail
                        .compare_exchange(tail_ptr, tail_prev, AcqRel, Acquire, &guard)
                        .is_ok()
                {
                    tail_prev_node.next.store(Shared::null(), Release);
                    tail_node.prev.store(Shared::null(), Release);
                    tail_node.next.store(Shared::null(), Release);
                    remains = tail_node.buffer.pop_all();
                    unsafe {
                        guard.defer_destroy(tail_ptr);
                    }
                }
            }
            for r in remains {
                self.push_back(r);
            }
            backoff.spin();
        }
    }
    
    // pub fn peek_front(&self) -> ListItemRef<T, N> {
    //     let guard = crossbeam_epoch::pin();
    //     let backoff = Backoff::new();
    //     let obj;
    //     let mut node_ptr = self.head.load(Acquire, &guard);
    //     loop {
    //         let node = unsafe { node_ptr.deref() };
    //         if let Some(o) = node.buffer.peek_front() {
    //             obj = o;
    //             break;
    //         }
    //     }
    //     ListItemRef {
    //         obj_ref: obj,
    //         node_ptr
    //     }
    // }
}

pub struct ListItemRef<'a, T: Clone, const N: usize> {
    obj_ref: ItemRef<'a, T, N>,
    node_ptr: Shared<'a, RingBufferNode<T, N>>
}

impl<T: Clone + Default, const N: usize> RingBufferNode<T, N> {
    pub fn new() -> Self {
        Self {
            prev: Atomic::null(),
            next: Atomic::null(),
            buffer: RingBuffer::<T, N>::new(),
            lock: Mutex::new(()),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::par_list_tests;

    use super::*;

    #[test]
    pub fn general() {
        let nums = 204800;
        let list = LinkedRingBufferList::<_, 32>::new();
        for i in 0..nums {
            list.push_front(i);
        }
        for i in (0..nums).rev() {
            debug_assert_eq!(list.pop_front(), Some(i));
        }
        debug_assert_eq!(list.pop_front(), None);
        debug_assert_eq!(list.pop_back(), None);
        for i in 0..nums {
            list.push_back(i)
        }
        for i in (0..nums).rev() {
            debug_assert_eq!(list.pop_back(), Some(i));
        }
        debug_assert_eq!(list.pop_front(), None);
        debug_assert_eq!(list.pop_back(), None);

        for i in 0..nums {
            list.push_front(i);
        }
        for i in 0..nums {
            list.push_back(i)
        }
        for i in (0..nums).rev() {
            debug_assert_eq!(list.pop_front(), Some(i));
        }
        for i in (0..nums).rev() {
            debug_assert_eq!(list.pop_back(), Some(i));
        }
        debug_assert_eq!(list.pop_front(), None);
        debug_assert_eq!(list.pop_back(), None);

        for i in 0..nums {
            list.push_front(i);
        }
        for i in 0..nums {
            list.push_back(i)
        }
        for i in (0..nums).rev() {
            debug_assert_eq!(list.pop_front(), Some(i));
        }
        for i in 0..nums {
            debug_assert_eq!(list.pop_front(), Some(i));
        }
        debug_assert_eq!(list.pop_front(), None);
        debug_assert_eq!(list.pop_back(), None);


        for i in 0..nums {
            list.push_back(i)
        }
        for i in 0..nums {
            list.push_front(i);
        }
        for i in (0..nums).rev() {
            debug_assert_eq!(list.pop_back(), Some(i));
        }
        for i in 0..nums {
            debug_assert_eq!(list.pop_back(), Some(i));
        }
        debug_assert_eq!(list.pop_front(), None);
        debug_assert_eq!(list.pop_back(), None);
    }

    const NUM: usize = 409600;
    const CAP: usize = 128;

    par_list_tests!(
        {
            LinkedRingBufferList::<_, CAP>::new()
        },
        NUM
    );
}
