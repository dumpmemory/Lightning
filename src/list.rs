use crossbeam_epoch::*;
use crossbeam_utils::Backoff;

use crate::ring_buffer::{ItemIter, ItemPtr, ItemRef, RingBuffer};
use parking_lot::Mutex;
use std::sync::atomic::Ordering::*;

// A mostly lock-free list with linked ring buffers

pub struct LinkedRingBufferList<T, const B: usize> {
    head: Atomic<RingBufferNode<T, B>>,
    tail: Atomic<RingBufferNode<T, B>>,
}

pub struct RingBufferNode<T, const N: usize> {
    pub buffer: RingBuffer<T, N>,
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
    pub fn push_front(&self, mut val: T) -> ItemPtr<T, N> {
        let guard = crossbeam_epoch::pin();
        let backoff = Backoff::new();
        loop {
            let head_ptr = self.head.load(Acquire, &guard);
            let head_node = unsafe { head_ptr.deref() };
            match head_node.buffer.push_front(val) {
                Ok(r) => {
                    return r.to_ptr();
                }
                Err(v) => {
                    let head_lock = head_node.lock.try_lock();
                    if head_lock.is_some() && head_node.prev.load(Acquire, &guard).is_null() {
                        let new_node = RingBufferNode::new();
                        new_node.next.store(head_ptr, Relaxed);
                        let new_node_ptr = Owned::new(new_node).into_shared(&guard);
                        let new_node_lock = unsafe { new_node_ptr.deref().lock.lock() };
                        if self
                            .head
                            .compare_exchange(head_ptr, new_node_ptr, AcqRel, Acquire, &guard)
                            .is_ok()
                        {
                            head_node.prev.store(new_node_ptr, Release);
                        } else {
                            unsafe {
                                drop(new_node_lock);
                                new_node_ptr.into_owned();
                            }
                        }
                    }
                    val = v;
                    backoff.spin();
                }
            }
        }
    }

    pub fn push_back(&self, mut val: T) -> ItemPtr<T, N> {
        let guard = crossbeam_epoch::pin();
        let backoff = Backoff::new();
        loop {
            let tail_ptr = self.tail.load(Acquire, &guard);
            let tail_node = unsafe { tail_ptr.deref() };
            match tail_node.buffer.push_back(val) {
                Ok(r) => {
                    return r.to_ptr();
                }
                Err(v) => {
                    debug!("Locking on tail ptr: {:?}", tail_ptr);
                    let tail_lock = tail_node.lock.try_lock();
                    if tail_lock.is_some() && tail_node.next.load(Acquire, &guard).is_null() {
                        let new_node = RingBufferNode::new();
                        new_node.prev.store(tail_ptr, Relaxed);
                        let new_node_ptr = Owned::new(new_node).into_shared(&guard);
                        let new_node_lock = unsafe { new_node_ptr.deref().lock.lock() };
                        if self
                            .tail
                            .compare_exchange(tail_ptr, new_node_ptr, AcqRel, Acquire, &guard)
                            .is_ok()
                        {
                            tail_node.next.store(new_node_ptr, Release);
                        } else {
                            unsafe {
                                // Must drop the lock it before reclaim the node
                                drop(new_node_lock);
                                new_node_ptr.into_owned();
                            }
                        }
                    }
                    val = v;
                    backoff.spin();
                }
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
                    // Need to keep prev and next reference for iterator
                    remains = head_node.buffer.pop_all();
                    unsafe {
                        logged_defer_destory(&guard, head_ptr, "pop front with head ptr");
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
                    // Need to keep prev and next reference for iterator
                    remains = tail_node.buffer.pop_all();
                    unsafe {
                        logged_defer_destory(&guard, tail_ptr, "pop back with tail ptr");
                    }
                }
            }
            for r in remains {
                self.push_back(r);
            }
            backoff.spin();
        }
    }

    pub fn peek_front(&self) -> Option<ListItemRef<T, N>> {
        let guard = crossbeam_epoch::pin();
        let backoff = Backoff::new();
        let mut node_ptr = self.head.load(Acquire, &guard);
        let obj_idx;
        loop {
            let node = unsafe { node_ptr.deref() };
            if let Some(o) = node.buffer.peek_front() {
                obj_idx = o.idx;
                break;
            } else {
                node_ptr = node.next.load(Acquire, &guard);
                if node_ptr.is_null() {
                    return None;
                }
            }
            backoff.spin();
        }
        let node_ptr = node_ptr.as_raw();
        Some(ListItemRef {
            guard,
            obj_idx,
            node_ptr,
            list: self,
        })
    }

    pub fn peek_back(&self) -> Option<ListItemRef<T, N>> {
        let guard = crossbeam_epoch::pin();
        let backoff = Backoff::new();
        let mut node_ptr = self.tail.load(Acquire, &guard);
        let obj_idx;
        loop {
            let node = unsafe { node_ptr.deref() };
            if let Some(o) = node.buffer.peek_back() {
                obj_idx = o.idx;
                break;
            } else {
                node_ptr = node.prev.load(Acquire, &guard);
                if node_ptr.is_null() {
                    return None;
                }
            }
            backoff.spin();
        }
        let node_ptr = node_ptr.as_raw();
        Some(ListItemRef {
            guard,
            obj_idx,
            node_ptr,
            list: self,
        })
    }

    pub fn iter_front(&self) -> ListIter<T, N> {
        self.iter_general(true)
    }

    pub fn iter_back(&self) -> ListIter<T, N> {
        self.iter_general(false)
    }

    #[inline(always)]
    fn iter_general(&self, forwarding: bool) -> ListIter<T, N> {
        let (node_ptr, buffer_iter, guard) = {
            let guard = crossbeam_epoch::pin();
            let end = if forwarding { &self.head } else { &self.tail };
            let node_ref = end.load(Acquire, &guard);
            let node_ptr = node_ref.as_raw();
            let node = unsafe { Shared::from(node_ptr).deref() };
            let iter = if forwarding {
                node.buffer.iter_front()
            } else {
                node.buffer.iter_back()
            };
            (node_ptr, iter, guard)
        };
        ListIter {
            node_ptr,
            buffer_iter,
            forwarding,
            list: self,
            guard,
        }
    }
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

pub struct ListItemRef<'a, T: Clone, const N: usize> {
    guard: Guard,
    obj_idx: usize,
    list: &'a LinkedRingBufferList<T, N>,
    node_ptr: *const RingBufferNode<T, N>,
}

impl<'a, T: Clone + Default, const N: usize> ListItemRef<'a, T, N> {
    pub fn deref(&self) -> Option<T> {
        self.item_ref().deref()
    }

    pub fn remove(&self) -> Option<T> {
        let guard = &self.guard;
        let node_ref = Shared::from(self.node_ptr);
        let node = unsafe { node_ref.deref() };
        let item_ref = ItemRef {
            buffer: &node.buffer,
            idx: self.obj_idx,
        };
        if let Some(obj) = item_ref.remove() {
            if node.buffer.peek_back().is_none() {
                // Internal node is empty, shall remove the node
                loop {
                    let prev_ptr = node.prev.load(Acquire, guard);
                    let next_ptr = node.next.load(Acquire, guard);
                    let prev = unsafe { prev_ptr.as_ref() };
                    let next = unsafe { next_ptr.as_ref() };
                    let _prev_lock = prev.as_ref().map(|n| n.lock.lock());
                    let _node_lock = node.lock.lock();
                    let _next_lock = next.as_ref().map(|n| n.lock.lock());
                    if prev.map_or(true, |n| n.next.load(Acquire, guard) == node_ref)
                        && next.map_or(true, |n| n.prev.load(Acquire, guard) == node_ref)
                    {
                        prev.map(|n| n.next.store(next_ptr, Release));
                        next.map(|n| n.prev.store(prev_ptr, Release));
                        let remains = node.buffer.pop_all();
                        for v in remains {
                            // Compensate, order may changed but not much choice here
                            if prev.is_none() {
                                // Possibly removing head node
                                self.list.push_front(v);
                            } else {
                                // Possibly removing tail or internal node
                                self.list.push_back(v);
                            }
                        }
                        unsafe {
                            logged_defer_destory(guard, node_ref, "Remove list item ref");
                        }
                        break;
                    }
                }
            }
            Some(obj)
        } else {
            None
        }
    }

    pub fn item_ref(&self) -> ItemRef<T, N> {
        let node_ref = Shared::from(self.node_ptr);
        let node = unsafe { node_ref.deref() };
        ItemRef {
            buffer: &node.buffer,
            idx: self.obj_idx,
        }
    }
}

pub unsafe fn logged_defer_destory<T>(guard: &Guard, ptr: Shared<'_, T>, msg: &'_ str) {
    guard.defer_unchecked(move || {
        debug!("LOGGED_DEFER_DESTORY: {:?}, msg: {}", ptr, msg);
        ptr.into_owned()
    })
}

pub struct ListIter<'a, T: Clone, const N: usize> {
    guard: Guard,
    node_ptr: *const RingBufferNode<T, N>,
    list: &'a LinkedRingBufferList<T, N>,
    buffer_iter: ItemIter<'a, T, N>,
    forwarding: bool,
}

impl<'a, T: Clone + Default, const N: usize> Iterator for ListIter<'a, T, N> {
    type Item = ListItemRef<'a, T, N>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let item = self.buffer_iter.next();
            if let Some(item) = item {
                let guard = crossbeam_epoch::pin();
                return Some(ListItemRef {
                    guard,
                    obj_idx: item.idx,
                    list: self.list,
                    node_ptr: self.node_ptr,
                });
            } else {
                let node_ref = Shared::from(self.node_ptr);
                let node = unsafe { node_ref.deref() };
                let next_ptr = if self.forwarding {
                    node.next.load(Acquire, &self.guard)
                } else {
                    node.prev.load(Acquire, &self.guard)
                }
                .as_raw();
                if next_ptr.is_null() {
                    return None;
                }
                let next_node = unsafe { Shared::from(next_ptr).deref() };
                let buffer_iter = if self.forwarding {
                    next_node.buffer.iter_front()
                } else {
                    next_node.buffer.iter_back()
                };
                self.node_ptr = next_ptr;
                self.buffer_iter = buffer_iter;
            }
        }
    }
}

unsafe impl<T: Clone + Default, const N: usize> Send for LinkedRingBufferList<T, N> {}

impl<T, const N: usize> Drop for LinkedRingBufferList<T, N> {
    fn drop(&mut self) {
        let guard = crossbeam_epoch::pin();
        let mut node = self.head.load(Acquire, &guard);
        while !node.is_null() {
            unsafe {
                let node_ins = node.deref();
                let next = node_ins.next.load(Acquire, &guard);
                logged_defer_destory(&guard, node, "Drop liked buffer list");
                node = next;
            }
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
            list.push_back(i);
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
            list.push_back(i);
        }
        let mut iter = list.iter_front();
        for i in (0..nums).rev() {
            debug_assert_eq!(iter.next().and_then(|r| r.deref()), Some(i));
        }
        for i in 0..nums {
            debug_assert_eq!(iter.next().and_then(|r| r.deref()), Some(i));
        }
        debug_assert!(iter.next().is_none());
        let mut iter = list.iter_back();
        for i in (0..nums).rev() {
            debug_assert_eq!(iter.next().and_then(|r| r.deref()), Some(i));
        }
        for i in 0..nums {
            debug_assert_eq!(iter.next().and_then(|r| r.deref()), Some(i));
        }
        debug_assert!(iter.next().is_none());
        for i in (0..nums).rev() {
            debug_assert_eq!(list.peek_front().and_then(|r| r.deref()), Some(i));
            debug_assert_eq!(list.pop_front(), Some(i));
        }
        for i in (0..nums).rev() {
            debug_assert_eq!(list.peek_back().and_then(|r| r.deref()), Some(i));
            debug_assert_eq!(list.pop_back(), Some(i));
        }
        debug_assert_eq!(list.pop_front(), None);
        debug_assert_eq!(list.pop_back(), None);
        debug_assert!(list.peek_front().is_none());
        debug_assert!(list.peek_back().is_none());

        for i in 0..nums {
            list.push_front(i);
        }
        for i in 0..nums {
            list.push_back(i);
        }
        for i in (0..nums).rev() {
            debug_assert_eq!(list.peek_front().and_then(|r| r.deref()), Some(i));
            debug_assert_eq!(list.pop_front(), Some(i));
        }
        for i in 0..nums {
            debug_assert_eq!(list.peek_front().and_then(|r| r.deref()), Some(i));
            debug_assert_eq!(list.pop_front(), Some(i));
        }
        debug_assert_eq!(list.pop_front(), None);
        debug_assert_eq!(list.pop_back(), None);
        debug_assert!(list.peek_front().is_none());
        debug_assert!(list.peek_back().is_none());

        for i in 0..nums {
            list.push_back(i);
        }
        for i in 0..nums {
            list.push_front(i);
        }
        for i in (0..nums).rev() {
            debug_assert_eq!(list.peek_back().and_then(|r| r.deref()), Some(i));
            debug_assert_eq!(list.pop_back(), Some(i));
        }
        for i in 0..nums {
            debug_assert_eq!(list.peek_back().and_then(|r| r.deref()), Some(i));
            debug_assert_eq!(list.pop_back(), Some(i));
        }
        debug_assert_eq!(list.pop_front(), None);
        debug_assert_eq!(list.pop_back(), None);
        debug_assert!(list.peek_front().is_none());
        debug_assert!(list.peek_back().is_none());
    }

    const NUM: usize = 409600;
    const CAP: usize = 128;

    par_list_tests!(
        usize_test, 
        usize, 
        {
            |n| n as usize
        }, 
        { 
            LinkedRingBufferList::<usize, CAP>::new() 
        }, 
        NUM
    );

    par_list_tests!(
        on_heap_test,
        Vec<usize>,
        {
            |n| vec![n as usize]
        },
        { 
            LinkedRingBufferList::<Vec<usize>, CAP>::new() 
        },
        NUM / 10
    );
}
