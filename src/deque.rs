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

const NOM_TAG: usize = 0;
const DEL_TAG: usize = 1;
const ADD_TAG: usize = 2;
const LCK_TAG: usize = 3;

impl<T: Clone> Deque<T> {
    pub fn new() -> Self {
        let guard = crossbeam_epoch::pin();
        let head = Atomic::new(Node::null());
        let tail = Atomic::new(Node::null());
        let head_ptr = head.load(Relaxed, &guard);
        let tail_ptr = tail.load(Relaxed, &guard);
        debug!(
            "Initialized deque with head: {:?} and tail: {:?}",
            head, tail
        );
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
        let curr_node = unsafe { curr.deref() };
        let mut next;
        curr_node.prev.store(prev.with_tag(NOM_TAG), Relaxed);
        loop {
            next = prev_node.next.load(Acquire, &guard);
            curr_node.next.store(next.with_tag(ADD_TAG), Relaxed);
            debug_assert_ne!(prev.with_tag(NOM_TAG), next.with_tag(NOM_TAG));
            if prev_node
                .next
                .compare_exchange(
                    next.with_tag(NOM_TAG),
                    curr.with_tag(LCK_TAG),
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
        next = Self::insert_next_ops(&curr, next, &guard, &backoff);
        prev_node.next.store(curr.with_tag(NOM_TAG), Release);
        curr_node.next.store(next.with_tag(NOM_TAG), Release);
    }

    pub fn insert_back(&self, value: T, guard: &Guard) {
        let backoff = crossbeam_utils::Backoff::new();
        let curr = Owned::new(Node::new(value)).into_shared(&guard);
        let mut next = self.tail.load(Relaxed, &guard);
        let next_node = unsafe { next.deref() };
        let curr_node = unsafe { curr.deref() };
        let mut prev;
        let mut prev_node;
        let mut locked = false; // TODO: Aviod locking tail sentinel
        loop {
            prev = next_node.prev.load(Acquire, &guard);
            prev_node = unsafe { prev.deref() };
            if !locked
                && next_node
                    .next
                    .compare_exchange(
                        Shared::null().with_tag(NOM_TAG),
                        Shared::null().with_tag(LCK_TAG),
                        AcqRel,
                        Acquire,
                        guard,
                    )
                    .is_ok()
            {
                locked = true;
            }
            curr_node.prev.store(prev.with_tag(NOM_TAG), Relaxed);
            curr_node.next.store(next.with_tag(ADD_TAG), Relaxed);
            if prev_node
                .next
                .compare_exchange(
                    next.with_tag(NOM_TAG),
                    curr.with_tag(LCK_TAG),
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
        next = Self::insert_next_ops(&curr, next, &guard, &backoff);
        prev_node.next.store(curr.with_tag(NOM_TAG), Release);
        curr_node.next.store(next.with_tag(NOM_TAG), Release);
        next_node
            .next
            .store(Shared::null().with_tag(NOM_TAG), Release);
    }

    fn insert_next_ops<'a>(
        node_ptr: &Shared<'a, Node<T>>,
        mut next_ptr: Shared<'a, Node<T>>,
        guard: &'a Guard,
        backoff: &Backoff,
    ) -> Shared<'a, Node<T>> {
        fence(SeqCst);
        loop {
            unsafe {
                let next = next_ptr.deref();
                let next_prev_ptr = next.prev.load(Acquire, guard);
                let next_next_ptr = next.next.load(Acquire, guard);
                if let Err(new_next_prev) = next.prev.compare_exchange(
                    next_prev_ptr.with_tag(NOM_TAG),
                    node_ptr.with_tag(NOM_TAG),
                    AcqRel,
                    Acquire,
                    guard,
                )
                // Changed the prev pointer of next node to the node_ptr
                {
                    let new_next_prev = new_next_prev.current;
                    if new_next_prev.tag() == DEL_TAG {
                        next_ptr = next_next_ptr;
                        backoff.spin();
                    } else {
                        next_ptr = new_next_prev;
                        backoff.spin();
                    }
                } else {
                    // II
                    return next_ptr;
                }
            }
        }
    }

    pub fn remove_front<'a>(&self, guard: &'a Guard) -> Option<Shared<'a, Node<T>>> {
        let backoff = crossbeam_utils::Backoff::new();
        let tail = self.tail.load(Relaxed, guard);
        let prev = self.head.load(Acquire, guard);
        loop {
            let prev_node = unsafe { prev.deref() };
            let curr = prev_node.next.load(Acquire, guard);
            let curr_node = unsafe { curr.deref() };
            debug_assert_ne!(
                prev.with_tag(NOM_TAG),
                curr.with_tag(NOM_TAG),
                "head node is linking itself"
            );
            if curr == tail {
                // End of list
                return None;
            }
            let curr_next = curr_node.next.load(Acquire, guard);
            if let Err(_new_curr_next) = curr_node.next.compare_exchange(
                curr_next.with_tag(NOM_TAG), // Stable tag ensured
                curr_next.with_tag(DEL_TAG),
                AcqRel,
                Acquire,
                guard,
            ) {
                // retry
            } else {
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
        let mut locked = false; // TODO: Aviod locking tail sentinel
        debug_assert!(!next_ptr.is_null());
        loop {
            if !locked
                && next
                    .next
                    .compare_exchange(
                        Shared::null().with_tag(NOM_TAG),
                        Shared::null().with_tag(LCK_TAG),
                        AcqRel,
                        Acquire,
                        guard,
                    )
                    .is_ok()
            {
                locked = true;
            }
            let node_ptr = next.prev.load(Acquire, guard);
            debug_assert!(!node_ptr.is_null());
            debug_assert_ne!(next_ptr.with_tag(NOM_TAG), node_ptr.with_tag(NOM_TAG));
            if node_ptr == head_ptr {
                return None;
            }
            let node = unsafe { node_ptr.deref() };
            let node_next_ptr = node.next.load(Acquire, guard);
            if node
                .next
                .compare_exchange(
                    node_next_ptr.with_tag(NOM_TAG),
                    node_next_ptr.with_tag(DEL_TAG),
                    AcqRel,
                    Acquire,
                    guard,
                )
                .is_ok()
            {
                Self::unlink_node(&node_ptr, guard, &backoff);
                next.next.store(Shared::null().with_tag(NOM_TAG), Release);
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
                let prev_next = prev_node.next.load(Acquire, guard);
                debug_assert_ne!(prev_ptr.with_tag(NOM_TAG), prev_next.with_tag(NOM_TAG));
                if !marked_prev {
                    // check update prev
                    if let Err(exg_prev_next) = prev_node.next.compare_exchange(
                        prev_next.with_tag(NOM_TAG),
                        prev_next.with_tag(LCK_TAG),
                        AcqRel,
                        Acquire,
                        guard,
                    ) {
                        let new_prev_next = exg_prev_next.current;
                        let new_prev_next_tag = new_prev_next.tag();
                        if new_prev_next_tag == DEL_TAG {
                            let prev_prev = prev_node.prev.load(Acquire, guard);
                            debug_assert_ne!(
                                prev_ptr.with_tag(NOM_TAG),
                                prev_prev.with_tag(NOM_TAG)
                            );
                            debug_assert!(!prev_prev.is_null());
                            if new_prev_next_tag == NOM_TAG {
                                trace!(
                                    "Shifting from {:?} to prev next {:?} for {:?}",
                                    prev_ptr,
                                    new_prev_next,
                                    node_ptr
                                );
                                prev_ptr = new_prev_next;
                                backoff.spin();
                            } else {
                                trace!(
                                    "Shifting from {:?} to prev prev {:?} for {:?}",
                                    prev_ptr,
                                    prev_prev,
                                    node_ptr
                                );
                                debug_assert_ne!(
                                    prev_prev.with_tag(NOM_TAG),
                                    node_ptr.with_tag(NOM_TAG)
                                );
                                prev_ptr = prev_prev;
                                backoff.spin();
                            }
                        } else if new_prev_next_tag == ADD_TAG {
                            backoff.spin();
                        } else if new_prev_next_tag == LCK_TAG {
                            backoff.spin();
                        } else {
                            backoff.spin();
                        }
                        continue;
                    } else {
                        marked_prev = true;
                    }
                }
                let next_prev = next_node.prev.load(Acquire, guard);
                let next_next = next_node.next.load(Acquire, guard);
                if let Err(exg_next_prev) = next_node.prev.compare_exchange(
                    next_prev.with_tag(NOM_TAG),
                    prev_ptr.with_tag(NOM_TAG),
                    AcqRel,
                    Acquire,
                    guard,
                ) {
                    let new_next_prev = exg_next_prev.current;
                    if new_next_prev.tag() == DEL_TAG {
                        next_ptr = next_next;
                    } else {
                        next_ptr = new_next_prev;
                    }
                } else {
                    prev_node.next.store(next_ptr.with_tag(NOM_TAG), Release);
                    break;
                }
                backoff.spin();
            }
            fence(SeqCst);
            Self::mark_prev_del(node_ptr, guard, backoff);
        }
    }

    fn mark_prev_del<'a>(node_ptr: &'a Shared<'a, Node<T>>, guard: &'a Guard, backoff: &Backoff) {
        unsafe {
            let node = node_ptr.deref();
            loop {
                let node_prev_ptr = node.prev.load(Acquire, guard);
                debug_assert_ne!(node_prev_ptr.tag(), ADD_TAG);
                if node_prev_ptr.tag() == DEL_TAG
                    || node
                        .prev
                        .compare_exchange(
                            node_prev_ptr,
                            node_prev_ptr.with_tag(DEL_TAG),
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
        assert_eq!(
            deque.remove_front(&guard).map(|s| unsafe { **s.deref() }),
            Some(2)
        );
        assert_eq!(
            deque.remove_front(&guard).map(|s| unsafe { **s.deref() }),
            Some(1)
        );
    }

    #[test]
    pub fn remove_back() {
        let guard = crossbeam_epoch::pin();
        let deque = Deque::new();
        deque.insert_back(1, &guard);
        deque.insert_back(2, &guard);
        assert_eq!(
            deque.remove_back(&guard).map(|s| unsafe { **s.deref() }),
            Some(2)
        );
        assert_eq!(
            deque.remove_back(&guard).map(|s| unsafe { **s.deref() }),
            Some(1)
        );
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
        let num = 409600;
        let guard = crossbeam_epoch::pin();
        let deque = Arc::new(Deque::new());
        for i in 0..num {
            deque.insert_front(i, &guard);
        }
        let ths = (0..num)
            .chunks(1024)
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
        assert_eq!(all_nums.len(), num);
        for i in 0..num {
            assert!(all_nums.contains(&i));
        }
        assert!(deque.remove_front(&guard).is_none());
        assert!(deque.remove_back(&guard).is_none());
    }

    #[test]
    pub fn multithread_pop_back() {
        let num = 409600;
        let guard = crossbeam_epoch::pin();
        let deque = Arc::new(Deque::new());
        for i in 0..num {
            deque.insert_front(i, &guard);
        }
        let ths = (0..num)
            .chunks(1024)
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
        deque.insert_back(1, &guard);
        deque.insert_back(2, &guard);
        deque.insert_back(3, &guard);
        unsafe {
            assert_eq!(**deque.remove_back(&guard).unwrap().deref(), 3);
            assert_eq!(**deque.remove_back(&guard).unwrap().deref(), 2);
            assert_eq!(**deque.remove_back(&guard).unwrap().deref(), 1);
        }
        assert!(deque.remove_back(&guard).is_none());
    }

    #[test]
    pub fn multithread_push_front_and_back_single_thread_pop_front() {
        let num = 409600;
        let deque = Arc::new(Deque::new());
        let ths = (0..num)
            .chunks(1024)
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
        let num = 409600;
        let deque = Arc::new(Deque::new());
        let ths = (0..num)
            .chunks(1024)
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
            .chunks(128)
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
        let num = 40960;
        let threshold = (num as f64 * 0.5) as usize;
        let deque = Arc::new(Deque::new());
        let guard = crossbeam_epoch::pin();
        for i in 0..threshold {
            deque.insert_front(i, &guard);
        }
        let ths = (threshold..num)
            .chunks(1024)
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
        let _ = env_logger::try_init();
        let num = 40960;
        let threshold = (num as f64 * 0.5) as usize;
        let deque = Arc::new(Deque::new());
        let guard = crossbeam_epoch::pin();
        for i in 0..threshold {
            deque.insert_back(i, &guard);
        }
        let ths = (threshold..num)
            .chunks(2048)
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
