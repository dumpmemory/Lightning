use std::{cmp::min, mem, ops::Deref};

use crossbeam_epoch::*;
use crossbeam_utils::Backoff;

use crate::{ring_buffer::RingBuffer, spin::SpinLock};
use parking_lot::Mutex;
use std::sync::atomic::Ordering::*;

// A mostly lock-free list with linked ring buffers

const ALIGEMENT_SIZE: usize = 4096;

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
                if head_lock.is_some() {
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
                if tail_lock.is_some() {
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
