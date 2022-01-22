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
        assert!(N.is_power_of_two());
        Self {
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
            elements: unsafe { MaybeUninit::uninit().assume_init() },
            flags: [EMPTY_SLOT; N],
        }
    }

    pub fn push_back(&self, data: T) -> Result<(), T> {
        self.push_general(data, &self.tail, &self.head, Self::incr)
    }

    pub fn push_front(&self, data: T) -> Result<(), T> {
        self.push_general(data, &self.head, &self.tail, Self::decr)
    }

    #[inline(always)]
    fn push_general<S>(
        &self,
        data: T,
        target: &AtomicUsize,
        other_side: &AtomicUsize,
        shift: S,
    ) -> Result<(), T>
    where
        S: Fn(usize) -> usize,
    {
        let backoff = Backoff::new();
        loop {
            let target_val = target.load(Acquire);
            let other_val = other_side.load(Acquire);
            let new_target_val = shift(target_val);
            if new_target_val == other_val {
                // overflow
                return Err(data);
            } else if target
                .compare_exchange(target_val, new_target_val, AcqRel, Acquire)
                .is_ok()
            {
                let flag = &self.flags[target_val];
                let obj = &self.elements[target_val];
                obj.set(data);
                flag.store(ACQUIRED, Release);
                return Ok(());
            }
            backoff.spin();
        }
    }

    pub fn pop_front(&self) -> Option<T> {
        self.pop_general(&self.head, &self.tail, Self::incr)
    }

    pub fn pop_back(&self) -> Option<T> {
        self.pop_general(&self.tail, &self.head, Self::decr) 
    }

    #[inline(always)]
    fn pop_general<S>(&self, target: &AtomicUsize, other_side: &AtomicUsize, shift: S) -> Option<T>
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
            let flag = &self.flags[target_val];
            let obj = &self.elements[target_val];
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
        (num + 1) & (N - 1)
    }

    fn decr(num: usize) -> usize {
        if num == 0 {
            N
        } else {
            num - 1
        }
    }
}
