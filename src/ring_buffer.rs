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
        (num + 1) & (N - 1)
    }

    fn decr(num: usize) -> usize {
        if num == 0 {
            N - 1
        } else {
            num - 1
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    pub fn general() {
        let ring = RingBuffer::<_, 32>::new();
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
    }
}