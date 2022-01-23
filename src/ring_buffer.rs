use std::cell::Cell;
use std::sync::atomic::Ordering::*;
use std::{mem::MaybeUninit, sync::atomic::*};

use crossbeam_utils::Backoff;

// A lock-free double sided ring buffer

const EMPTY: u8 = 0;
const SENTINEL: u8 = 1;
const ACQUIRED: u8 = 2;
const EMPTY_SLOT: AtomicU8 = AtomicU8::new(EMPTY);

pub struct RingBuffer<T: Clone, const N: usize> {
    head: AtomicUsize,
    tail: AtomicUsize,
    elements: [Cell<T>; N],
    flags: [AtomicU8; N],
}

impl<T: Clone, const N: usize> RingBuffer<T, N> {
    pub fn new() -> Self {
        Self {
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
            elements: unsafe { MaybeUninit::uninit().assume_init() },
            flags: [EMPTY_SLOT; N],
        }
    }

    pub fn push_back(&self, data: T) -> Result<(), T> {
        self.push_general(data, &self.tail, &self.head, Self::incr, false)
    }

    pub fn push_front(&self, data: T) -> Result<(), T> {
        self.push_general(data, &self.head, &self.tail, Self::decr, true)
    }

    #[inline(always)]
    fn push_general<S>(
        &self,
        data: T,
        target: &AtomicUsize,
        other_side: &AtomicUsize,
        shift: S,
        ahead: bool
    ) -> Result<(), T>
    where
        S: Fn(usize) -> usize,
    {
        let backoff = Backoff::new();
        loop {
            let target_val = target.load(Acquire);
            let other_val = other_side.load(Acquire);
            let new_target_val = shift(target_val);
            let pos = if ahead { new_target_val } else { target_val };
            if new_target_val == other_val {
                // overflow
                return Err(data);
            } else if target
                .compare_exchange(target_val, new_target_val, AcqRel, Acquire)
                .is_ok()
            {
                let flag = &self.flags[pos];
                let obj = &self.elements[pos];
                obj.set(data);
                flag.store(ACQUIRED, Release);
                return Ok(());
            }
            backoff.spin();
        }
    }

    pub fn pop_front(&self) -> Option<T> {
        self.pop_general(&self.head, &self.tail, Self::incr, false)
    }

    pub fn pop_back(&self) -> Option<T> {
        self.pop_general(&self.tail, &self.head, Self::decr, true) 
    }

    #[inline(always)]
    fn pop_general<S>(&self, target: &AtomicUsize, other_side: &AtomicUsize, shift: S, ahead: bool) -> Option<T>
    where
        S: Fn(usize) -> usize,
    {
        let backoff = Backoff::new();
        loop {
            let target_val = target.load(Acquire);
            let other_val = other_side.load(Acquire);
            if target_val == other_val {
                return None;
            }
            let new_target_val = shift(target_val);
            let pos = if ahead { new_target_val } else { target_val }; // target value is always on step ahead
            let flag = &self.flags[pos];
            let obj = &self.elements[pos];
            let flag_val = flag.load(Acquire);
            if flag_val != EMPTY
                && flag
                    .compare_exchange(flag_val, EMPTY, AcqRel, Acquire)
                    .is_ok()
            {
                let mut res = unsafe { MaybeUninit::uninit().assume_init() };
                if flag_val != SENTINEL {
                    res = obj.replace(unsafe { MaybeUninit::uninit().assume_init() });
                }
                if target
                    .compare_exchange(target_val, new_target_val, AcqRel, Acquire)
                    .is_err()
                {
                    flag.store(SENTINEL, Release);
                }
                if flag_val != SENTINEL {
                    return Some(res)
                }
            }
            backoff.spin();
        }
    }

    fn incr(num: usize) -> usize {
        (num + 1) % N
    }

    fn decr(num: usize) -> usize {
        if num == 0 {
            N - 1
        } else {
            num - 1
        }
    }
}

unsafe impl <T: Clone, const N: usize> Sync for RingBuffer <T, N> {}

#[cfg(test)]
mod test {
    use std::{sync::Arc, thread, collections::HashSet};

    use itertools::Itertools;

    use super::*;

    #[test]
    pub fn general() {
        const CAPACITY: usize = 32;
        let ring = RingBuffer::<_, CAPACITY>::new();
        assert!(ring.push_back(1).is_ok());
        assert!(ring.push_back(2).is_ok());
        assert!(ring.push_back(3).is_ok());
        assert!(ring.push_back(4).is_ok());
        assert_eq!(ring.pop_back(), Some(4));
        assert_eq!(ring.pop_back(), Some(3));
        assert_eq!(ring.pop_back(), Some(2));
        assert_eq!(ring.pop_back(), Some(1));
        assert_eq!(ring.pop_back(), None);
        assert!(ring.push_front(1).is_ok());
        assert!(ring.push_front(2).is_ok());
        assert!(ring.push_front(3).is_ok());
        assert!(ring.push_front(4).is_ok());
        assert_eq!(ring.pop_front(), Some(4));
        assert_eq!(ring.pop_front(), Some(3));
        assert_eq!(ring.pop_front(), Some(2));
        assert_eq!(ring.pop_front(), Some(1));
        assert_eq!(ring.pop_back(), None);
        assert!(ring.push_back(1).is_ok());
        assert!(ring.push_back(2).is_ok());
        assert!(ring.push_back(3).is_ok());
        assert!(ring.push_back(4).is_ok());
        assert!(ring.push_front(5).is_ok());
        assert!(ring.push_front(6).is_ok());
        assert!(ring.push_front(7).is_ok());
        assert!(ring.push_front(8).is_ok());
        assert_eq!(ring.pop_back(), Some(4));
        assert_eq!(ring.pop_back(), Some(3));
        assert_eq!(ring.pop_back(), Some(2));
        assert_eq!(ring.pop_back(), Some(1));
        assert_eq!(ring.pop_back(), Some(5));
        assert_eq!(ring.pop_back(), Some(6));
        assert_eq!(ring.pop_back(), Some(7));
        assert_eq!(ring.pop_back(), Some(8));
        assert_eq!(ring.pop_back(), None);
        assert!(ring.push_back(1).is_ok());
        assert!(ring.push_back(2).is_ok());
        assert!(ring.push_back(3).is_ok());
        assert!(ring.push_back(4).is_ok());
        assert!(ring.push_front(5).is_ok());
        assert!(ring.push_front(6).is_ok());
        assert!(ring.push_front(7).is_ok());
        assert!(ring.push_front(8).is_ok());
        assert_eq!(ring.pop_front(), Some(8));
        assert_eq!(ring.pop_front(), Some(7));
        assert_eq!(ring.pop_front(), Some(6));
        assert_eq!(ring.pop_front(), Some(5));
        assert_eq!(ring.pop_front(), Some(1));
        assert_eq!(ring.pop_front(), Some(2));
        assert_eq!(ring.pop_front(), Some(3));
        assert_eq!(ring.pop_front(), Some(4));
        assert_eq!(ring.pop_back(), None);
        assert_eq!(ring.pop_front(), None);
        for i in 0..CAPACITY - 1 {
            assert!(ring.push_back(i).is_ok(), "on {}", i)
        }
        assert_eq!(ring.push_front(CAPACITY), Err(CAPACITY));
        assert_eq!(ring.push_back(CAPACITY + 1), Err(CAPACITY + 1));
        for i in 0..CAPACITY - 1 {
            assert_eq!(ring.pop_front(), Some(i));
        }
        assert_eq!(ring.pop_back(), None);
    }

    #[test]
    pub fn multithread_push_front_single_thread_pop_front() {
        const NUM: usize = 20480;
        const CAPACITY: usize = NUM * 2;
        let deque = Arc::new(RingBuffer::<_, CAPACITY>::new());
        let ths = (0..NUM)
            .chunks(256)
            .into_iter()
            .map(|nums| {
                let nums = nums.collect_vec();
                let deque = deque.clone();
                thread::spawn(move || {
                    for i in nums {
                        deque.push_front(i).unwrap();
                    }
                })
            })
            .collect::<Vec<_>>();
        ths.into_iter().for_each(|t| {
            t.join().unwrap();
        });
        let mut all_nums = HashSet::new();
        for _ in 0..NUM {
            all_nums.insert(
                deque
                    .pop_front()
                    .unwrap(),
            );
        }
        assert_eq!(all_nums.len(), NUM);
        for i in 0..NUM {
            assert!(all_nums.contains(&i));
        }
    }

    #[test]
    pub fn multithread_push_front_single_thread_pop_back() {
        const NUM: usize = 20480;
        const CAPACITY: usize = NUM * 2;
        let deque = Arc::new(RingBuffer::<_, CAPACITY>::new());
        let ths = (0..NUM)
            .chunks(256)
            .into_iter()
            .map(|nums| {
                let nums = nums.collect_vec();
                let deque = deque.clone();
                thread::spawn(move || {
                    for i in nums {
                        deque.push_front(i).unwrap();
                    }
                })
            })
            .collect::<Vec<_>>();
        ths.into_iter().for_each(|t| {
            t.join().unwrap();
        });
        let mut all_nums = HashSet::new();
        for _ in 0..NUM {
            all_nums.insert(
                deque
                    .pop_back()
                    .unwrap(),
            );
        }
        assert_eq!(all_nums.len(), NUM);
        for i in 0..NUM {
            assert!(all_nums.contains(&i));
        }
    }

    #[test]
    pub fn multithread_push_back_single_thread_pop_front() {
        const NUM: usize = 20480;
        const CAPACITY: usize = NUM * 2;
        let deque = Arc::new(RingBuffer::<_, CAPACITY>::new());
        let ths = (0..NUM)
            .chunks(256)
            .into_iter()
            .map(|nums| {
                let nums = nums.collect_vec();
                let deque = deque.clone();
                thread::spawn(move || {
                    for i in nums {
                        deque.push_back(i).unwrap();
                    }
                })
            })
            .collect::<Vec<_>>();
        ths.into_iter().for_each(|t| {
            t.join().unwrap();
        });
        let mut all_nums = HashSet::new();
        for _ in 0..NUM {
            all_nums.insert(
                deque
                    .pop_front()
                    .unwrap(),
            );
        }
        assert_eq!(all_nums.len(), NUM);
        for i in 0..NUM {
            assert!(all_nums.contains(&i));
        }
    }

    #[test]
    pub fn multithread_push_back_single_thread_pop_back() {
        const NUM: usize = 20480;
        const CAPACITY: usize = NUM * 2;
        let deque = Arc::new(RingBuffer::<_, CAPACITY>::new());
        let ths = (0..NUM)
            .chunks(256)
            .into_iter()
            .map(|nums| {
                let nums = nums.collect_vec();
                let deque = deque.clone();
                thread::spawn(move || {
                    for i in nums {
                        deque.push_back(i).unwrap();
                    }
                })
            })
            .collect::<Vec<_>>();
        ths.into_iter().for_each(|t| {
            t.join().unwrap();
        });
        let mut all_nums = HashSet::new();
        for _ in 0..NUM {
            all_nums.insert(
                deque
                    .pop_back()
                    .unwrap(),
            );
        }
        assert_eq!(all_nums.len(), NUM);
        for i in 0..NUM {
            assert!(all_nums.contains(&i));
        }
    }

    #[test]
    pub fn multithread_pop_front() {
        let _ = env_logger::try_init();
        const NUM: usize = 20480;
        const CAPACITY: usize = NUM * 2;
        let deque = Arc::new(RingBuffer::<_, CAPACITY>::new());
        for i in 0..NUM {
            deque.push_front(i).unwrap();
        }
        let ths = (0..NUM)
            .chunks(256)
            .into_iter()
            .map(|nums| {
                let deque = deque.clone();
                let nums = nums.collect_vec();
                thread::spawn(move || {
                    nums.into_iter()
                        .map(|_| {
                            deque.pop_front().unwrap()
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
        assert_eq!(all_nums.len(), NUM);
        for i in 0..NUM {
            assert!(all_nums.contains(&i));
        }
        assert!(deque.pop_front().is_none());
        assert!(deque.pop_back().is_none());
    }

    #[test]
    pub fn multithread_pop_back() {
        const NUM: usize = 20480;
        const CAPACITY: usize = NUM * 2;
        let deque = Arc::new(RingBuffer::<_, CAPACITY>::new());
        for i in 0..NUM {
            deque.push_front(i).unwrap();
        }
        let ths = (0..NUM)
            .chunks(256)
            .into_iter()
            .map(|nums| {
                let deque = deque.clone();
                let nums = nums.collect_vec();
                thread::spawn(move || {
                    nums.into_iter()
                        .map(|_| {
                            deque.pop_back().unwrap()
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
        assert!(deque.pop_front().is_none());
        assert!(deque.pop_back().is_none());
        assert_eq!(all_nums.len(), NUM);
        for i in 0..NUM {
            assert!(all_nums.contains(&i));
        }
        deque.push_back(1).unwrap();
        deque.push_back(2).unwrap();
        deque.push_back(3).unwrap();
        assert_eq!(deque.pop_back().unwrap(), 3);
        assert_eq!(deque.pop_back().unwrap(), 2);
        assert_eq!(deque.pop_back().unwrap(), 1);
        assert!(deque.pop_back().is_none());
    }

    #[test]
    pub fn multithread_push_front_and_back_single_thread_pop_front() {
        const NUM: usize = 20480;
        const CAPACITY: usize = NUM * 2;
        let deque = Arc::new(RingBuffer::<_, CAPACITY>::new());
        let ths = (0..NUM)
            .chunks(256)
            .into_iter()
            .map(|nums| {
                let nums = nums.collect_vec();
                let deque = deque.clone();
                thread::spawn(move || {
                    nums.into_iter().for_each(|i| {
                        if i % 2 == 0 {
                            deque.push_front(i).unwrap();
                        } else {
                            deque.push_back(i).unwrap();
                        }
                    });
                })
            })
            .collect::<Vec<_>>();
        ths.into_iter().for_each(|t| {
            t.join().unwrap();
        });
        let mut all_nums = HashSet::new();
        for _ in 0..NUM {
            all_nums.insert(
                deque
                    .pop_front()
                    .unwrap(),
            );
        }
        assert_eq!(all_nums.len(), NUM);
        for i in 0..NUM {
            assert!(all_nums.contains(&i));
        }
    }

    #[test]
    pub fn multithread_push_front_and_back_single_thread_pop_back() {
        const NUM: usize = 20480;
        const CAPACITY: usize = NUM * 2;
        let deque = Arc::new(RingBuffer::<_, CAPACITY>::new());
        let ths = (0..NUM)
            .chunks(256)
            .into_iter()
            .map(|nums| {
                let nums = nums.collect_vec();
                let deque = deque.clone();
                thread::spawn(move || {
                    nums.into_iter().for_each(|i| {
                        if i % 2 == 0 {
                            deque.push_front(i).unwrap();
                        } else {
                            deque.push_back(i).unwrap();
                        }
                    });
                })
            })
            .collect::<Vec<_>>();
        ths.into_iter().for_each(|t| {
            t.join().unwrap();
        });
        let mut all_nums = HashSet::new();
        for _ in 0..NUM {
            all_nums.insert(
                deque
                    .pop_back()
                    .unwrap(),
            );
        }
        assert_eq!(all_nums.len(), NUM);
        for i in 0..NUM {
            assert!(all_nums.contains(&i));
        }
    }

    #[test]
    pub fn multithread_pop_back_front() {
        const NUM: usize = 20480;
        const CAPACITY: usize = NUM * 2;
        let deque = Arc::new(RingBuffer::<_, CAPACITY>::new());
        for i in 0..NUM {
            deque.push_front(i).unwrap();
        }
        let ths = (0..NUM)
            .chunks(512)
            .into_iter()
            .map(|nums| {
                let deque = deque.clone();
                let nums = nums.collect_vec();
                thread::spawn(move || {
                    nums.into_iter()
                        .map(|i| {
                            if i % 2 == 0 {
                                deque.pop_front().unwrap()
                            } else {
                                deque.pop_back().unwrap()
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
        assert!(deque.pop_front().is_none());
        assert!(deque.pop_back().is_none());
        assert_eq!(all_nums.len(), NUM);
        for i in 0..NUM {
            assert!(all_nums.contains(&i));
        }
    }

    #[test]
    pub fn multithread_push_pop_front() {
        const NUM: usize = 20480;
        const CAPACITY: usize = NUM * 2;
        let deque = Arc::new(RingBuffer::<_, CAPACITY>::new());
        let threshold = (NUM as f64 * 0.5) as usize;
        for i in 0..threshold {
            deque.push_front(i).unwrap();
        }
        let ths = (threshold..NUM)
            .chunks(256)
            .into_iter()
            .map(|nums| {
                let nums = nums.collect_vec();
                let deque = deque.clone();
                thread::spawn(move || {
                    nums.into_iter()
                        .map(|i| {
                            if i % 2 == 0 {
                                deque.push_front(i).unwrap();
                                None
                            } else {
                                Some(deque.pop_front().unwrap())
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
        assert_eq!(results_len, (NUM - threshold) / 2);
        let set = results.into_iter().collect::<HashSet<_>>();
        assert_eq!(results_len, set.len());
    }

    #[test]
    pub fn multithread_push_pop_back() {
        let _ = env_logger::try_init();
        const NUM: usize = 20480;
        const CAPACITY: usize = NUM * 2;
        let deque = Arc::new(RingBuffer::<_, CAPACITY>::new());
        let threshold = (NUM as f64 * 0.5) as usize;
        for i in 0..threshold {
            deque.push_back(i).unwrap();
        }
        let ths = (threshold..NUM)
            .chunks(256)
            .into_iter()
            .map(|nums| {
                let nums = nums.collect_vec();
                let deque = deque.clone();
                thread::spawn(move || {
                    nums.into_iter()
                        .map(|i| {
                            if i % 2 == 0 {
                                deque.push_back(i).unwrap();
                                None
                            } else {
                                Some(deque.pop_back().unwrap())
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
        assert_eq!(results_len, (NUM - threshold) / 2);
        let set = results.into_iter().collect::<HashSet<_>>();
        assert_eq!(results_len, set.len());
    }
}