// A lock-free stack

use crossbeam_epoch::Atomic;
use crossbeam_epoch::Owned;
use std::sync::atomic::Ordering::*;

use crate::ring_buffer::RingBuffer;

pub struct LinkedRingBufferStack<T, const B: usize> {
    head: Atomic<RingBufferNode<T, B>>,
}

pub struct RingBufferNode<T, const N: usize> {
    buffer: RingBuffer<T, N>,
    next: Atomic<Self>,
}

impl<T: Clone + Default, const B: usize> LinkedRingBufferStack<T, B> {
    pub fn new() -> Self {
        Self {
            head: Atomic::null(),
        }
    }

    pub fn pop(&self) -> Option<T> {
        let guard = crossbeam_epoch::pin();
        loop {
            let node_ptr = self.head.load(Acquire, &guard);
            if node_ptr.is_null() {
                return None;
            }
            let node = unsafe { node_ptr.deref() };
            let res = node.buffer.pop_front();
            if res.is_none() {
                let next = node.next.load(Acquire, &guard);
                let _ = self.head.compare_exchange(
                    node_ptr,
                    next,
                    AcqRel,
                    Relaxed,
                    &guard,
                );
            } else {
                return res;
            }
        }
    }

    pub fn push(&self, mut data: T) {
        let guard = crossbeam_epoch::pin();
        loop {
            let node_ptr = self.head.load(Acquire, &guard);
            if !node_ptr.is_null() {
                let node = unsafe { node_ptr.deref() };
                if let Err(v) = node.buffer.push_front(data) {
                    data =  v;
                } else {
                    return;
                }
            }
            // Push into current buffer does not succeed. Need new buffer.
            let _ = self.head.compare_exchange(node_ptr, Owned::new(RingBufferNode {
                buffer: RingBuffer::new(), next: Atomic::from(node_ptr)
            }), AcqRel, Relaxed, &guard);
        }
    }

    pub fn attach_buffer(&self, buffer: RingBuffer<T, B>) {
        let guard = crossbeam_epoch::pin();
        let new_node_ptr = Owned::new(RingBufferNode {
            buffer, next: Atomic::null()
        }).into_shared(&guard);
        let new_node = unsafe {
            new_node_ptr.deref()
        };
        loop {
            let node_ptr = self.head.load(Acquire, &guard);
            new_node.next.store(node_ptr, Relaxed);
            if self.head.compare_exchange(node_ptr, new_node_ptr, AcqRel, Relaxed, &guard).is_ok() {
                break;
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::{sync::Arc, thread, collections::HashSet};

    use itertools::Itertools;

    use super::LinkedRingBufferStack;

    #[test]
    pub fn general() {
        let nums = 204800;
        let list = LinkedRingBufferStack::<_, 32>::new();
        for i in 0..nums {
            list.push(i);
        }
        for i in (0..nums).rev() {
            debug_assert_eq!(list.pop(), Some(i));
        }
    }


    #[test]
    pub fn multithread_push_pop() {
        let num: usize = 4096000;
        let deque = Arc::new(LinkedRingBufferStack::<_, 32>::new());
        let threshold = (num as f64 * 0.5) as usize;
        for i in 0..threshold {
            deque.push(i);
        }
        let ths = (threshold..num)
            .chunks(256)
            .into_iter()
            .map(|nums| {
                let nums = nums.collect_vec();
                let deque = deque.clone();
                thread::spawn(move || {
                    nums.into_iter()
                        .map(|i| {
                            if i % 2 == 0 {
                                deque.push(i);
                                None
                            } else {
                                Some(deque.pop().unwrap())
                            }
                        })
                        .collect_vec()
                })
            })
            .collect::<Vec<_>>();
        let results = ths
            .into_iter()
            .map(|j| j.join().unwrap().into_iter())
            .flatten()
            .filter_map(|n| n)
            .collect::<Vec<_>>();
        let results_len = results.len();
        assert_eq!(results_len, (num - threshold) / 2);
        let set = results.into_iter().collect::<HashSet<_>>();
        assert_eq!(results_len, set.len());
    }
}