// Design reference: https://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.140.4693&rep=rep1&type=pdf
// Lock-Free and Practical Doubly Linked List-Based Deques Using Single-Word Compare-and-Swap
// By: Hakan Sundell and Philippas Tsigas

use std::{
    mem,
    ops::Deref,
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

const TRANS_TAG: usize = 1;
const STABLE_TAG: usize = 0;

impl<T: Clone> Deque<T> {
    pub fn new() -> Self {
        let guard = crossbeam_epoch::pin();
        let head = Atomic::new(Node::null());
        let tail = Atomic::new(Node::null());
        let head_ptr = head.load(Relaxed, &guard);
        let tail_ptr = tail.load(Relaxed, &guard);
        debug!("Initialized deque with head: {:?} and tail: {:?}", head, tail);
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
                if decomp_with_ptr(&prev_next) != (&next, STABLE_TAG) {
                    next = prev_next;
                    backoff.spin();
                    continue;
                }
            }
            curr_node.prev.store(prev.with_tag(STABLE_TAG), Relaxed);
            curr_node.next.store(next.with_tag(TRANS_TAG), Relaxed);
            if prev_node
                .next
                .compare_exchange(
                    next.with_tag(STABLE_TAG),
                    curr.with_tag(STABLE_TAG),
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
        if let Some(next) = Self::insert_next_ops(&curr, next, prev, &guard, &backoff) {
            curr_node.next.store(next.with_tag(STABLE_TAG), Release);
        }
    }

    pub fn insert_back(&self, value: T, guard: &Guard) {
        let backoff = crossbeam_utils::Backoff::new();
        let curr = Owned::new(Node::new(value)).into_shared(&guard);
        let next = self.tail.load(Relaxed, &guard);
        let curr_node = unsafe { curr.deref() };
        let mut prev;
        loop {
            let next_node = unsafe { next.deref() };
            prev = next_node.prev.load(Acquire, &guard);
            let prev_node = unsafe { prev.deref() };
            curr_node.prev.store(prev.with_tag(STABLE_TAG), Relaxed);
            curr_node.next.store(next.with_tag(TRANS_TAG), Relaxed);
            debug_assert_eq!(next.tag(), STABLE_TAG);
            if prev_node
                .next
                .compare_exchange(
                    next.with_tag(STABLE_TAG),
                    curr.with_tag(STABLE_TAG),
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
        if let Some(next) = Self::insert_next_ops(&curr, next, prev, &guard, &backoff) {
            curr_node.next.store(next.with_tag(STABLE_TAG), Release);
        }
    }

    fn insert_next_ops<'a>(
        node_ptr: &Shared<'a, Node<T>>,
        mut next_ptr: Shared<'a, Node<T>>,
        mut next_prev_ptr: Shared<'a, Node<T>>,
        guard: &'a Guard,
        backoff: &Backoff,
    ) -> Option<Shared<'a, Node<T>>> {
        loop {
            unsafe {
                let node = node_ptr.deref();
                let next = next_ptr.deref();
                if node.prev.load(Acquire, guard).tag() == TRANS_TAG {
                    warn!("RARE condition on insert next ops. node removed.");
                    return None;
                }
                if next_prev_ptr.tag() == TRANS_TAG {
                    next_ptr = next_prev_ptr
                } else {
                    if let Err(new_next_prev) = next.prev.compare_exchange(
                        next_prev_ptr,
                        node_ptr.with_tag(STABLE_TAG),
                        AcqRel,
                        Acquire,
                        guard,
                    )
                    // Changed the prev pointer of next node to the node_ptr
                    {
                        next_ptr = new_next_prev.current;
                    } else {
                        // II
                        return Some(next_ptr);
                    }
                }
                // Refresh next prev from the new next node
                let next = next_ptr.deref();
                next_prev_ptr = next.prev.load(Acquire, guard);
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
            debug_assert_ne!(prev, curr, "head node is linking it self");
            if curr == self.tail.load(Relaxed, guard) {
                // End of list
                return None;
            }
            let curr_node = unsafe { curr.deref() };
            let curr_prev = curr_node.prev.load(Acquire, guard);
            if curr_prev.tag() == TRANS_TAG {
                // TODO: Make sure current node is properly removed
                trace!("Current node is removed at {:?}, retry", curr);
                backoff.spin();
                continue;
            }
            if curr_node
                .prev
                .compare_exchange(
                    curr_prev, // Stable tag ensured
                    curr_prev.with_tag(TRANS_TAG),
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
            trace!("Marking current deleted failed {:?}, retry", curr);
            backoff.spin();
        }
    }

    pub fn remove_back<'a>(&self, guard: &'a Guard) -> Option<Shared<'a, Node<T>>> {
        let backoff = crossbeam_utils::Backoff::new();
        let next_ptr = self.tail.load(Relaxed, guard);
        let head_ptr = self.head.load(Relaxed, guard);
        let next = unsafe { next_ptr.deref() };
        debug_assert!(!next_ptr.is_null());
        loop {
            let node_ptr = next.prev.load(Acquire, guard);
            debug_assert!(!node_ptr.is_null());
            if node_ptr == head_ptr  {
                return None;
            }
            let node = unsafe { node_ptr.deref() };
            let node_prev_ptr = node.prev.load(Acquire, guard);
            if node_prev_ptr.tag() == TRANS_TAG {
                backoff.spin();
                continue;
            }
            let node_prev_ptr = node.prev.load(Acquire, guard);
            if node
                .prev
                .compare_exchange(
                    node_prev_ptr.with_tag(STABLE_TAG),
                    node_prev_ptr.with_tag(TRANS_TAG),
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

    fn unlink_node<'a>(node_ptr: &Shared<'a, Node<T>>, guard: &'a Guard, backoff: &Backoff) {
        fence(SeqCst);
        unsafe {
            let node = node_ptr.deref();
            let mut prev_ptr = node.prev.load(Acquire, guard);
            let mut next_ptr = node.next.load(Acquire, guard);
            let mut marked_prev = false;
            loop {
                let prev_node = prev_ptr.deref();
                let next_node = next_ptr.deref();
                let prev_prev = prev_node.prev.load(Acquire, guard);
                let prev_next = prev_node.next.load(Acquire, guard);
                if !marked_prev {
                    // check update prev
                    if prev_prev.tag() == TRANS_TAG {
                        prev_ptr = prev_prev;
                        backoff.spin();
                        continue;
                    }
                    if prev_next.tag() == TRANS_TAG {
                        // Inserting, need to wait until it finished
                        backoff.spin();
                        continue;
                    }
                }
                let next_prev = next_node.prev.load(Acquire, guard);
                let next_next = next_node.next.load(Acquire, guard);
                {
                    // check update next
                    if next_prev.tag() == TRANS_TAG {
                        // Deleting or deleted, shift to next_next
                        next_ptr = next_next;
                        backoff.spin();
                        continue;
                    }
                    // Don't care about insertion by next next because we are not going to touch it
                }
                debug_assert_ne!(prev_next.tag(), TRANS_TAG);
                debug_assert_ne!(next_prev.tag(), TRANS_TAG);
                if !marked_prev
                    && prev_node
                        .next
                        .compare_exchange(
                            prev_next,
                            next_ptr.with_tag(STABLE_TAG),
                            AcqRel,
                            Acquire,
                            guard,
                        )
                        .is_err()
                {
                    backoff.spin();
                    continue;
                } else {
                    marked_prev = true;
                }
                if next_node
                    .prev
                    .compare_exchange(
                        next_prev,
                        prev_ptr.with_tag(STABLE_TAG),
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
            fence(SeqCst);
            Self::mark_next_trans(node_ptr, guard, backoff);
        }
    }

    fn mark_next_trans<'a>(node_ptr: &'a Shared<'a, Node<T>>, guard: &'a Guard, backoff: &Backoff) {
        unsafe {
            let node = node_ptr.deref();
            loop {
                let node_next_ptr = node.next.load(Acquire, guard);
                if node_next_ptr.tag() == TRANS_TAG
                    || node
                        .next
                        .compare_exchange(
                            node_next_ptr,
                            node_next_ptr.with_tag(TRANS_TAG),
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

impl<T> Deref for Node<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use itertools::Itertools;
    use std::{collections::HashSet, sync::Arc, thread};

    #[test]
    pub fn insert_front() {
        let guard = crossbeam_epoch::pin();
        let deque = Deque::new();
        deque.insert_front(1, &guard);
        deque.insert_front(2, &guard);
        deque.insert_front(4, &guard);
    }

    #[test]
    pub fn insert_back() {
        let guard = crossbeam_epoch::pin();
        let deque = Deque::new();
        deque.insert_back(1, &guard);
        deque.insert_back(2, &guard);
    }

    #[test]
    pub fn remove_front() {
        let guard = crossbeam_epoch::pin();
        let deque = Deque::new();
        deque.insert_front(1, &guard);
        deque.insert_front(2, &guard);
        assert_eq!(deque.remove_front(&guard).map(|s| unsafe { **s.deref() }), Some(2));
        assert_eq!(deque.remove_front(&guard).map(|s| unsafe { **s.deref() }), Some(1));
    }

    #[test]
    pub fn remove_back() {
        let guard = crossbeam_epoch::pin();
        let deque = Deque::new();
        deque.insert_back(1, &guard);
        deque.insert_back(2, &guard);
        assert_eq!(deque.remove_back(&guard).map(|s| unsafe { **s.deref() }), Some(2));
        assert_eq!(deque.remove_back(&guard).map(|s| unsafe { **s.deref() }), Some(1));
    }

    #[test]
    pub fn single_threaded_push_pop() {
        let guard = crossbeam_epoch::pin();
        let deque = Deque::new();
        deque.insert_front(1, &guard);
        deque.insert_back(2, &guard);
        assert_eq!(
            deque.remove_front(&guard).map(|s| unsafe { **s.deref() }),
            Some(1)
        );
        assert_eq!(
            deque.remove_back(&guard).map(|s| unsafe { **s.deref() }),
            Some(2)
        );
        assert!(deque.remove_front(&guard).is_none());
        assert!(deque.remove_back(&guard).is_none());
    }

    #[test]
    pub fn push_front_pop_front() {
        let num = 100;
        let guard = crossbeam_epoch::pin();
        let deque = Deque::new();
        for i in 0..num {
            deque.insert_front(i, &guard);
        }
        for i in (0..num).rev() {
            assert_eq!(
                deque.remove_front(&guard).map(|s| unsafe { **s.deref() }),
                Some(i)
            );
        }
        assert!(deque.remove_front(&guard).is_none());
        assert!(deque.remove_back(&guard).is_none());
    }

    #[test]
    pub fn push_front_pop_back() {
        let num = 100;
        let guard = crossbeam_epoch::pin();
        let deque = Deque::new();
        for i in 0..num {
            deque.insert_front(i, &guard);
        }
        for i in 0..num {
            assert_eq!(
                deque.remove_back(&guard).map(|s| unsafe { **s.deref() }),
                Some(i)
            );
        }
        assert!(deque.remove_front(&guard).is_none());
        assert!(deque.remove_back(&guard).is_none());
    }

    #[test]
    pub fn push_back_pop_back() {
        let num = 100;
        let guard = crossbeam_epoch::pin();
        let deque = Deque::new();
        for i in 0..num {
            deque.insert_back(i, &guard);
        }
        for i in (0..num).rev() {
            assert_eq!(
                deque.remove_back(&guard).map(|s| unsafe { **s.deref() }),
                Some(i)
            );
        }
        assert!(deque.remove_front(&guard).is_none());
        assert!(deque.remove_back(&guard).is_none());
    }

    #[test]
    pub fn push_back_pop_front() {
        let num = 100;
        let guard = crossbeam_epoch::pin();
        let deque = Deque::new();
        for i in 0..num {
            deque.insert_back(i, &guard);
        }
        for i in 0..num {
            assert_eq!(
                deque.remove_front(&guard).map(|s| unsafe { **s.deref() }),
                Some(i)
            );
        }
        assert!(deque.remove_front(&guard).is_none());
        assert!(deque.remove_back(&guard).is_none());
    }

    #[test]
    pub fn multithread_push_front_single_thread_pop_front() {
        let num = 40960;
        let deque = Arc::new(Deque::new());
        let ths = (0..num)
            .chunks(128)
            .into_iter()
            .map(|nums| {
                let nums = nums.collect_vec();
                let deque = deque.clone();
                thread::spawn(move || {
                    for i in nums {
                        let guard = crossbeam_epoch::pin();
                        deque.insert_front(i, &guard);
                    }
                })
            })
            .collect::<Vec<_>>();
        ths.into_iter().for_each(|t| {
            t.join().unwrap();
        });
        let mut all_nums = HashSet::new();
        let guard = crossbeam_epoch::pin();
        for _ in 0..num {
            all_nums.insert(
                deque
                    .remove_front(&guard)
                    .map(|s| unsafe { **s.deref() })
                    .unwrap(),
            );
        }
        assert_eq!(all_nums.len(), num);
        for i in 0..num {
            assert!(all_nums.contains(&i));
        }
    }

    #[test]
    pub fn multithread_push_front_single_thread_pop_back() {
        let num = 40960;
        let deque = Arc::new(Deque::new());
        let ths = (0..num)
            .chunks(128)
            .into_iter()
            .map(|nums| {
                let nums = nums.collect_vec();
                let deque = deque.clone();
                thread::spawn(move || {
                    for i in nums {
                        let guard = crossbeam_epoch::pin();
                        deque.insert_front(i, &guard);
                    }
                })
            })
            .collect::<Vec<_>>();
        ths.into_iter().for_each(|t| {
            t.join().unwrap();
        });
        let mut all_nums = HashSet::new();
        let guard = crossbeam_epoch::pin();
        for _ in 0..num {
            all_nums.insert(
                deque
                    .remove_back(&guard)
                    .map(|s| unsafe { **s.deref() })
                    .unwrap(),
            );
        }
        assert_eq!(all_nums.len(), num);
        for i in 0..num {
            assert!(all_nums.contains(&i));
        }
    }

    #[test]
    pub fn multithread_push_back_single_thread_pop_front() {
        let num = 40960;
        let deque = Arc::new(Deque::new());
        let ths = (0..num)
            .chunks(128)
            .into_iter()
            .map(|nums| {
                let nums = nums.collect_vec();
                let deque = deque.clone();
                thread::spawn(move || {
                    for i in nums {
                        let guard = crossbeam_epoch::pin();
                        deque.insert_back(i, &guard);
                    }
                })
            })
            .collect::<Vec<_>>();
        ths.into_iter().for_each(|t| {
            t.join().unwrap();
        });
        let mut all_nums = HashSet::new();
        let guard = crossbeam_epoch::pin();
        for _ in 0..num {
            all_nums.insert(
                deque
                    .remove_front(&guard)
                    .map(|s| unsafe { **s.deref() })
                    .unwrap(),
            );
        }
        assert_eq!(all_nums.len(), num);
        for i in 0..num {
            assert!(all_nums.contains(&i));
        }
    }

    #[test]
    pub fn multithread_push_back_single_thread_pop_back() {
        let num = 40960;
        let deque = Arc::new(Deque::new());
        let ths = (0..num)
            .chunks(128)
            .into_iter()
            .map(|nums| {
                let nums = nums.collect_vec();
                let deque = deque.clone();
                thread::spawn(move || {
                    for i in nums {
                        let guard = crossbeam_epoch::pin();
                        deque.insert_back(i, &guard);
                    }
                })
            })
            .collect::<Vec<_>>();
        ths.into_iter().for_each(|t| {
            t.join().unwrap();
        });
        let mut all_nums = HashSet::new();
        let guard = crossbeam_epoch::pin();
        for _ in 0..num {
            all_nums.insert(
                deque
                    .remove_back(&guard)
                    .map(|s| unsafe { **s.deref() })
                    .unwrap(),
            );
        }
        assert_eq!(all_nums.len(), num);
        for i in 0..num {
            assert!(all_nums.contains(&i));
        }
    }

    #[test]
    pub fn multithread_pop_front() {
        let _ = env_logger::try_init();
        let num = 40960;
        let guard = crossbeam_epoch::pin();
        let deque = Arc::new(Deque::new());
        for i in 0..num {
            deque.insert_front(i, &guard);
        }
        let ths = (0..num)
            .chunks(256)
            .into_iter()
            .map(|nums| {
                let deque = deque.clone();
                let nums = nums.collect_vec();
                thread::spawn(move || {
                    nums.into_iter()
                        .map(|_| {
                            let guard = crossbeam_epoch::pin();
                            unsafe { **deque.remove_front(&guard).unwrap().deref() }
                        })
                        .collect_vec()
                })
            })
            .collect::<Vec<_>>();
        let mut all_nums = HashSet::new();
        ths.into_iter()
            .map(|t| t.join().unwrap().into_iter())
            .flatten()
            .for_each(|n| {
                all_nums.insert(n);
            });
        assert!(deque.remove_front(&guard).is_none());
        assert!(deque.remove_back(&guard).is_none());
        assert_eq!(all_nums.len(), num);
        for i in 0..num {
            assert!(all_nums.contains(&i));
        }
    }

    #[test]
    pub fn multithread_pop_back() {
        let num = 4096;
        let guard = crossbeam_epoch::pin();
        let deque = Arc::new(Deque::new());
        for i in 0..num {
            deque.insert_front(i, &guard);
        }
        let ths = (0..num)
            .chunks(128)
            .into_iter()
            .map(|nums| {
                let deque = deque.clone();
                let nums = nums.collect_vec();
                thread::spawn(move || {
                    nums.into_iter()
                        .map(|_| {
                            let guard = crossbeam_epoch::pin();
                            unsafe { **deque.remove_back(&guard).unwrap().deref() }
                        })
                        .collect_vec()
                })
            })
            .collect::<Vec<_>>();
        let mut all_nums = HashSet::new();
        ths.into_iter()
            .map(|t| t.join().unwrap().into_iter())
            .flatten()
            .for_each(|n| {
                all_nums.insert(n);
            });
        assert!(deque.remove_front(&guard).is_none());
        assert!(deque.remove_back(&guard).is_none());
        assert_eq!(all_nums.len(), num);
        for i in 0..num {
            assert!(all_nums.contains(&i));
        }
    }

    #[test]
    pub fn multithread_push_front_and_back_single_thread_pop_front() {
        let num = 40960;
        let deque = Arc::new(Deque::new());
        let ths = (0..num)
            .chunks(128)
            .into_iter()
            .map(|nums| {
                let nums = nums.collect_vec();
                let deque = deque.clone();
                thread::spawn(move || {
                    let guard = crossbeam_epoch::pin();
                    nums.into_iter().for_each(|i| {
                        if i % 2 == 0 {
                            deque.insert_front(i, &guard);
                        } else {
                            deque.insert_back(i, &guard);
                        }
                    });
                })
            })
            .collect::<Vec<_>>();
        ths.into_iter().for_each(|t| {
            t.join().unwrap();
        });
        let mut all_nums = HashSet::new();
        let guard = crossbeam_epoch::pin();
        for _ in 0..num {
            all_nums.insert(
                deque
                    .remove_front(&guard)
                    .map(|s| unsafe { **s.deref() })
                    .unwrap(),
            );
        }
        assert_eq!(all_nums.len(), num);
        for i in 0..num {
            assert!(all_nums.contains(&i));
        }
    }

    #[test]
    pub fn multithread_push_front_and_back_single_thread_pop_back() {
        let num = 40960;
        let deque = Arc::new(Deque::new());
        let ths = (0..num)
            .chunks(128)
            .into_iter()
            .map(|nums| {
                let nums = nums.collect_vec();
                let deque = deque.clone();
                thread::spawn(move || {
                    let guard = crossbeam_epoch::pin();
                    nums.into_iter().for_each(|i| {
                        if i % 2 == 0 {
                            deque.insert_front(i, &guard);
                        } else {
                            deque.insert_back(i, &guard);
                        }
                    });
                })
            })
            .collect::<Vec<_>>();
        ths.into_iter().for_each(|t| {
            t.join().unwrap();
        });
        let mut all_nums = HashSet::new();
        let guard = crossbeam_epoch::pin();
        for _ in 0..num {
            all_nums.insert(
                deque
                    .remove_back(&guard)
                    .map(|s| unsafe { **s.deref() })
                    .unwrap(),
            );
        }
        assert_eq!(all_nums.len(), num);
        for i in 0..num {
            assert!(all_nums.contains(&i));
        }
    }

    #[test]
    pub fn multithread_pop_back_front() {
        let num = 40960;
        let guard = crossbeam_epoch::pin();
        let deque = Arc::new(Deque::new());
        for i in 0..num {
            deque.insert_front(i, &guard);
        }
        let ths = (0..num)
            .chunks(32)
            .into_iter()
            .map(|nums| {
                let deque = deque.clone();
                let nums = nums.collect_vec();
                thread::spawn(move || {
                    nums.into_iter()
                        .map(|i| {
                            let guard = crossbeam_epoch::pin();
                            unsafe {
                                if i % 2 == 0 {
                                    **deque.remove_front(&guard).unwrap().deref()
                                } else {
                                    **deque.remove_back(&guard).unwrap().deref()
                                }
                            }
                        })
                        .collect_vec()
                })
            })
            .collect::<Vec<_>>();
        let mut all_nums = HashSet::new();
        ths.into_iter()
            .map(|t| t.join().unwrap().into_iter())
            .flatten()
            .for_each(|n| {
                all_nums.insert(n);
            });
        assert!(deque.remove_front(&guard).is_none());
        assert!(deque.remove_back(&guard).is_none());
        assert_eq!(all_nums.len(), num);
        for i in 0..num {
            assert!(all_nums.contains(&i));
        }
    }

    #[test]
    pub fn multithread_push_pop_front() {
        let num = 4096;
        let threshold = (num as f64 * 0.5) as usize;
        let deque = Arc::new(Deque::new());
        let guard = crossbeam_epoch::pin();
        for i in 0..threshold {
            deque.insert_front(i, &guard);
        }
        let ths = (threshold..num)
            .chunks(32)
            .into_iter()
            .map(|nums| {
                let nums = nums.collect_vec();
                let deque = deque.clone();
                thread::spawn(move || {
                    nums.into_iter()
                        .map(|i| {
                            let guard = crossbeam_epoch::pin();
                            if i % 2 == 0 {
                                deque.insert_front(i, &guard);
                                None
                            } else {
                                Some(unsafe { **deque.remove_front(&guard).unwrap().deref() })
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

    #[test]
    pub fn multithread_push_pop_back() {
        let num = 40960;
        let threshold = (num as f64 * 0.5) as usize;
        let deque = Arc::new(Deque::new());
        let guard = crossbeam_epoch::pin();
        for i in 0..threshold {
            deque.insert_back(i, &guard);
        }
        let ths = (threshold..num)
            .chunks(64)
            .into_iter()
            .map(|nums| {
                let nums = nums.collect_vec();
                let deque = deque.clone();
                thread::spawn(move || {
                    nums.into_iter()
                        .map(|i| {
                            let guard = crossbeam_epoch::pin();
                            if i % 2 == 0 {
                                deque.insert_back(i, &guard);
                                None
                            } else {
                                Some(unsafe { **deque.remove_back(&guard).unwrap().deref() })
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
