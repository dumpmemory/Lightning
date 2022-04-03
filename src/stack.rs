// A lock-free stack
use std::mem;

use crate::aarc::{Arc, AtomicArc};
use crate::ring_buffer::RingBuffer;

pub struct LinkedRingBufferStack<T, const B: usize> {
    head: AtomicArc<RingBufferNode<T, B>>,
}

pub struct RingBufferNode<T, const N: usize> {
    pub buffer: RingBuffer<T, N>,
    pub next: AtomicArc<Self>,
}

impl<T: Clone + Default, const B: usize> LinkedRingBufferStack<T, B> {
    pub fn new() -> Self {
        Self {
            head: AtomicArc::null(),
        }
    }

    pub const fn const_new() -> Self {
        unsafe {
            Self {
                head: mem::transmute(0u128),
            }
        }
    }

    pub fn pop(&self) -> Option<T> {
        loop {
            let node = self.head.load();
            if node.is_null() {
                return None;
            }
            let res = node.buffer.pop_back();
            if res.is_none() {
                let next = node.next.load();
                if self.head.compare_exchange_is_ok(&node, &next) {
                    while let Some(v) = node.buffer.pop_front() {
                        self.push(v);
                    }
                }
            } else {
                return res;
            }
        }
    }

    pub fn push(&self, mut data: T) {
        loop {
            let node = self.head.load();
            if !node.is_null() {
                if let Err(v) = node.buffer.push_back(data) {
                    data = v;
                } else {
                    return;
                }
            }
            // Push into current buffer does not succeed. Need new buffer.
            let new_node = RingBufferNode {
                buffer: RingBuffer::new(),
                next: AtomicArc::from_rc(node.clone()),
            };
            let _ = self.head.compare_exchange_value_is_ok(&node, new_node);
        }
    }

    pub fn attach_buffer<'a>(&self, new_node: Arc<RingBufferNode<T, B>>) {
        loop {
            let node = self.head.load();
            new_node.next.store_ref(node.clone());
            if self.head.compare_exchange(&node, &new_node).is_ok() {
                break;
            }
        }
    }

    pub fn pop_buffer<'a>(&self) -> Option<Arc<RingBufferNode<T, B>>> {
        loop {
            let node = self.head.load();
            if node.is_null() {
                return None;
            }
            let next = node.next.load();
            if self.head.compare_exchange_is_ok(&node, &next) {
                return Some(node);
            }
        }
    }
}

unsafe impl<T, const B: usize> Sync for LinkedRingBufferStack<T, B> {}
unsafe impl<T, const B: usize> Send for LinkedRingBufferStack<T, B> {}

#[cfg(test)]
mod test {
    use std::{collections::HashSet, sync::Arc, thread};

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
