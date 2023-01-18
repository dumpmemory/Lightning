use std::cell::UnsafeCell;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::ptr;
use std::sync::atomic::Ordering::*;
use std::{mem, sync::atomic::*};

use crossbeam_utils::Backoff;

// A lock-free double sided ring buffer

pub const EMPTY: u8 = 0;
pub const SENTINEL: u8 = 1;
pub const ACQUIRED: u8 = 2;
const EMPTY_SLOT: AtomicU8 = AtomicU8::new(EMPTY);

pub struct RingBuffer<T, const N: usize> {
    pub head: AtomicUsize,
    pub tail: AtomicUsize,
    pub flags: [AtomicU8; N],
    elements: [ManuallyDrop<UnsafeCell<T>>; N],
}

impl<T: Clone + Sized, const N: usize> RingBuffer<T, N> {
    pub fn new() -> Self {
        let elements = unsafe { MaybeUninit::uninit().assume_init() };
        Self {
            head: AtomicUsize::new(0),
            tail: AtomicUsize::new(0),
            flags: [EMPTY_SLOT; N],
            elements,
        }
    }

    #[inline(always)]
    pub fn count(&self) -> usize {
        let head = self.head.load(Acquire);
        let tail = self.tail.load(Acquire);
        if tail <= head {
            head - tail
        } else {
            // wrapped
            head + (N - tail)
        }
    }

    #[inline(always)]
    pub fn push_back(&self, data: T) -> Result<ItemRef<T, N>, T> {
        self.push_general(data, &self.tail, &self.head, Self::incr, false)
    }

    #[inline(always)]
    pub fn push_front(&self, data: T) -> Result<ItemRef<T, N>, T> {
        self.push_general(data, &self.head, &self.tail, Self::decr, true)
    }

    #[inline(always)]
    pub unsafe fn push_back_unsafe(&self, data: T) -> Result<ItemRef<T, N>, T> {
        self.push_unsafe_general(data, &self.tail, &self.head, Self::incr, false)
    }

    #[inline(always)]
    pub unsafe fn push_front_unsafe(&self, data: T) -> Result<ItemRef<T, N>, T> {
        self.push_unsafe_general(data, &self.head, &self.tail, Self::decr, true)
    }

    #[inline(always)]
    fn push_general<S>(
        &self,
        data: T,
        target: &AtomicUsize,
        other_side: &AtomicUsize,
        shift: S,
        ahead: bool,
    ) -> Result<ItemRef<T, N>, T>
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
                unsafe {
                    ptr::write(obj.get(), data);
                }
                flag.store(ACQUIRED, Release);
                return Ok(ItemRef {
                    buffer: self,
                    idx: pos,
                });
            }
            backoff.spin();
        }
    }

    #[inline(always)]
    pub fn pop_front(&self) -> Option<T> {
        self.pop_general(&self.head, &self.tail, Self::incr, false)
    }

    #[inline(always)]
    pub fn pop_back(&self) -> Option<T> {
        self.pop_general(&self.tail, &self.head, Self::decr, true)
    }

    #[inline(always)]
    pub unsafe fn pop_front_unsafe(&self) -> Option<T> {
        self.pop_unsafe_general(&self.head, &self.tail, Self::incr, false)
    }

    #[inline(always)]
    pub unsafe fn pop_back_unsafe(&self) -> Option<T> {
        self.pop_unsafe_general(&self.tail, &self.head, Self::decr, true)
    }

    #[inline(always)]
    fn pop_general<S>(
        &self,
        target: &AtomicUsize,
        other_side: &AtomicUsize,
        shift: S,
        ahead: bool,
    ) -> Option<T>
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
                let change_target = || {
                    if target
                        .compare_exchange(target_val, new_target_val, AcqRel, Acquire)
                        .is_err()
                    {
                        flag.store(SENTINEL, Release);
                    }
                };
                if flag_val != SENTINEL {
                    let res;
                    unsafe {
                        res = ptr::read(obj.get());
                    }
                    change_target();
                    return Some(res);
                } else {
                    change_target();
                }
            }
            backoff.spin();
        }
    }

    #[inline(always)]
    fn pop_unsafe_general<S>(
        &self,
        target: &AtomicUsize,
        other_side: &AtomicUsize,
        shift: S,
        ahead: bool,
    ) -> Option<T>
    where
        S: Fn(usize) -> usize,
    {
        loop {
            let target_val = target.load(Relaxed);
            let other_val = other_side.load(Relaxed);
            if target_val == other_val {
                return None;
            }
            let new_target_val = shift(target_val);
            let pos = if ahead { new_target_val } else { target_val }; // target value is always on step ahead
            let flag = &self.flags[pos];
            let obj = &self.elements[pos];
            let flag_val = flag.load(Relaxed);
            debug_assert_ne!(flag_val, EMPTY);
            flag.store(EMPTY, Relaxed);
            if flag_val != SENTINEL {
                let res;
                unsafe {
                    res = ptr::read(obj.get());
                }
                target.store(new_target_val, Relaxed);
                return Some(res);
            } else {
                target.store(new_target_val, Relaxed);
            }
        }
    }

    #[inline(always)]
    fn push_unsafe_general<S>(
        &self,
        data: T,
        target: &AtomicUsize,
        other_side: &AtomicUsize,
        shift: S,
        ahead: bool,
    ) -> Result<ItemRef<T, N>, T>
    where
        S: Fn(usize) -> usize,
    {
        let target_val = target.load(Relaxed);
        let other_val = other_side.load(Relaxed);
        let new_target_val = shift(target_val);
        let pos = if ahead { new_target_val } else { target_val };
        if new_target_val == other_val {
            // overflow
            return Err(data);
        }
        target.store(new_target_val, Relaxed);
        let flag = &self.flags[pos];
        let obj = &self.elements[pos];
        unsafe {
            ptr::write(obj.get(), data);
        }
        flag.store(ACQUIRED, Relaxed);
        return Ok(ItemRef {
            buffer: self,
            idx: pos,
        });
    }

    #[inline(always)]
    pub fn peek_back(&self) -> Option<ItemRef<T, N>> {
        let tail = self.tail.load(Acquire);
        self.peek_general(tail, &self.head, Self::decr, true)
    }

    #[inline(always)]
    pub fn peek_front(&self) -> Option<ItemRef<T, N>> {
        let head = self.head.load(Acquire);
        self.peek_general(head, &self.tail, Self::incr, false)
    }

    #[inline(always)]
    fn peek_general<S>(
        &self,
        mut exp_pos: usize,
        other_side: &AtomicUsize,
        shift: S,
        ahead: bool,
    ) -> Option<ItemRef<T, N>>
    where
        S: Fn(usize) -> usize,
    {
        let backoff = Backoff::new();
        loop {
            let head = other_side.load(Acquire);
            if head == exp_pos {
                return None;
            }
            let next_pos = shift(exp_pos);
            let pos = if ahead { next_pos } else { exp_pos };
            let flag = &self.flags[pos];
            let flag_val = flag.load(Acquire);
            if flag_val != ACQUIRED {
                exp_pos = next_pos;
            } else {
                return Some(ItemRef {
                    buffer: self,
                    idx: pos,
                });
            }
            backoff.spin();
        }
    }

    pub fn iter_back(&self) -> ItemIter<T, N> {
        let shift = Self::decr;
        let item = self.peek_back();
        ItemIter {
            buffer: self,
            other_side: &self.head,
            shift,
            idx: item.as_ref().map(|item| item.idx).unwrap_or(0),
            ahead: true,
            current: item,
        }
    }

    pub fn iter_front(&self) -> ItemIter<T, N> {
        let shift = Self::incr;
        let item = self.peek_front();
        ItemIter {
            buffer: self,
            other_side: &self.tail,
            shift,
            idx: item.as_ref().map(|item| item.idx).unwrap_or(0),
            ahead: false,
            current: item,
        }
    }

    pub fn pop_all(&self) -> Vec<T> {
        let mut res = vec![];
        while let Some(v) = self.pop_front() {
            res.push(v);
        }
        res
    }

    #[inline(always)]
    fn incr(num: usize) -> usize {
        (num + 1) % N
    }

    #[inline(always)]
    fn decr(num: usize) -> usize {
        if num == 0 {
            N - 1
        } else {
            num - 1
        }
    }
}

impl<T, const N: usize> Drop for RingBuffer<T, N> {
    fn drop(&mut self) {
        for (i, f) in self.flags.iter().enumerate() {
            if f.load(Relaxed) == ACQUIRED {
                let ele = &self.elements[i];
                unsafe {
                    ptr::read(ele.get());
                }
            }
        }
    }
}

pub struct ItemRef<'a, T: Clone, const N: usize> {
    pub buffer: &'a RingBuffer<T, N>,
    pub idx: usize,
}

impl<'a, T: Clone + Default, const N: usize> ItemRef<'a, T, N> {
    pub fn deref(&self) -> Option<T> {
        let idx = self.idx;
        let buffer = self.buffer;
        let flag = &buffer.flags[idx];
        let ele = &buffer.elements[idx];
        let flag_val = flag.load(Acquire);
        if flag_val == ACQUIRED {
            let obj = unsafe { (&*ele.get()).clone() };
            return Some(obj);
        } else {
            return None;
        }
    }

    pub unsafe fn to_ref<'b>(self) -> &'b T {
        let idx = self.idx;
        let buffer = self.buffer;
        let ele = &buffer.elements[idx];
        unsafe { &*ele.get() }
    }

    pub fn remove(&self) -> Option<T> {
        let idx = self.idx;
        let buffer = self.buffer;
        let flag = &buffer.flags[idx];
        let ele = &buffer.elements[idx];
        if flag
            .compare_exchange(ACQUIRED, SENTINEL, AcqRel, Acquire)
            .is_ok()
        {
            let val = unsafe { ptr::read(ele.get()) };
            let head = buffer.head.load(Acquire);
            let tail = buffer.tail.load(Acquire);
            let tail_decr = RingBuffer::<T, N>::decr(tail);
            if tail_decr == idx {
                if buffer
                    .tail
                    .compare_exchange(tail, tail_decr, AcqRel, Acquire)
                    .is_ok()
                {
                    let _succ = flag.compare_exchange(SENTINEL, EMPTY, AcqRel, Acquire);
                }
            }
            if head == idx {
                let new_head = RingBuffer::<T, N>::incr(head);
                if buffer
                    .head
                    .compare_exchange(head, new_head, AcqRel, Acquire)
                    .is_ok()
                {
                    let _succ = flag.compare_exchange(SENTINEL, EMPTY, AcqRel, Acquire);
                }
            }
            return Some(val);
        } else {
            return None;
        }
    }

    pub fn set(&self, value: T) -> Result<T, ()> {
        let idx = self.idx;
        let flag = &self.buffer.flags[idx];
        let ele = &self.buffer.elements[idx];
        if flag
            .compare_exchange(ACQUIRED, SENTINEL, AcqRel, Acquire)
            .is_err()
        {
            return Err(());
        } else {
            unsafe {
                let old = mem::replace(&mut *ele.get(), value);
                if flag
                    .compare_exchange(SENTINEL, ACQUIRED, AcqRel, Acquire)
                    .is_err()
                {
                    return Err(());
                } else {
                    return Ok(old);
                }
            }
        }
    }

    pub fn to_ptr(&self) -> ItemPtr<T, N> {
        ItemPtr {
            buffer: &*self.buffer,
            idx: self.idx,
        }
    }
}

pub struct ItemIter<'a, T: Clone, const N: usize> {
    buffer: &'a RingBuffer<T, N>,
    other_side: &'a AtomicUsize,
    shift: fn(usize) -> usize,
    idx: usize,
    ahead: bool,
    current: Option<ItemRef<'a, T, N>>,
}

impl<'a, T: Clone + Default, const N: usize> Iterator for ItemIter<'a, T, N> {
    type Item = ItemRef<'a, T, N>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current.is_none() {
            return None;
        }
        let shift = self.shift;
        let curr_pos = if !self.ahead {
            shift(self.idx)
        } else {
            self.idx
        };
        let other_side = self.other_side;
        let new_item = self
            .buffer
            .peek_general(curr_pos, other_side, shift, self.ahead);
        new_item.as_ref().map(|item| self.idx = item.idx);
        mem::replace(&mut self.current, new_item)
    }
}

#[derive(Clone, Debug)]
pub struct ItemPtr<T: Clone, const N: usize> {
    buffer: *const RingBuffer<T, N>,
    idx: usize,
}

impl<T: Clone, const N: usize> PartialEq for ItemPtr<T, N> {
    fn eq(&self, other: &Self) -> bool {
        self.buffer as usize == other.buffer as usize && self.idx == other.idx
    }
}

impl<T: Clone + Default, const N: usize> ItemPtr<T, N> {
    pub unsafe fn deref(&self) -> Option<T> {
        self.to_ref().deref()
    }

    pub unsafe fn to_ref(&self) -> ItemRef<T, N> {
        ItemRef {
            buffer: &*self.buffer,
            idx: self.idx,
        }
    }

    pub unsafe fn remove(&self) -> Option<T> {
        self.to_ref().remove()
    }

    pub unsafe fn set(&self, data: T) -> Result<T, ()> {
        self.to_ref().set(data)
    }
}

unsafe impl<T: Clone, const N: usize> Sync for RingBuffer<T, N> {}
unsafe impl<T: Clone, const N: usize> Sync for ItemPtr<T, N> {}
unsafe impl<T: Clone, const N: usize> Send for ItemPtr<T, N> {}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::par_list_tests;
    use itertools::*;
    use std::collections::HashSet;
    use std::thread;

    #[test]
    pub fn general() {
        const CAPACITY: usize = 32;
        let ring = RingBuffer::<_, CAPACITY>::new();
        assert!(ring.push_back(1).is_ok());
        assert!(ring.push_back(2).is_ok());
        assert!(ring.push_back(3).is_ok());
        assert!(ring.push_back(4).is_ok());
        assert_eq!(ring.peek_back().unwrap().deref(), Some(4));
        assert_eq!(ring.peek_back().unwrap().deref(), Some(4));
        assert_eq!(ring.peek_front().unwrap().deref(), Some(1));
        assert_eq!(ring.peek_front().unwrap().deref(), Some(1));
        assert_eq!(ring.pop_back(), Some(4));
        assert_eq!(ring.pop_back(), Some(3));
        assert_eq!(ring.peek_back().unwrap().deref(), Some(2));
        assert_eq!(ring.peek_front().unwrap().deref(), Some(1));
        assert_eq!(ring.pop_back(), Some(2));
        assert_eq!(ring.pop_back(), Some(1));
        assert_eq!(ring.pop_back(), None);
        assert!(ring.peek_back().is_none());
        assert!(ring.peek_front().is_none());
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

        // Testing iterator
        let mut front_iter = ring.iter_front();
        let mut back_iter = ring.iter_back();
        for i in 1..=4 {
            assert_eq!(
                front_iter.next().unwrap().deref(),
                Some(i),
                "at front {}",
                i
            );
        }
        assert!(front_iter.next().is_none());
        for i in (1..=4).rev() {
            assert_eq!(back_iter.next().unwrap().deref(), Some(i), "at back {}", i);
        }
        assert!(back_iter.next().is_none());
        assert!(front_iter.next().is_none());

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
        assert!(ring.push_front(1).is_ok());
        assert!(ring.push_front(2).is_ok());
        assert!(ring.push_front(3).is_ok());
        assert!(ring.push_front(4).is_ok());
        assert_eq!(ring.peek_front().unwrap().remove(), Some(4));
        assert_eq!(ring.peek_front().unwrap().deref(), Some(3));
        assert_eq!(ring.pop_front(), Some(3));
        assert_eq!(ring.peek_back().unwrap().remove(), Some(1));
        assert_eq!(ring.peek_front().unwrap().deref(), Some(2));
        assert_eq!(ring.peek_back().unwrap().set(4), Ok(2));
        assert_eq!(ring.peek_front().unwrap().deref(), Some(4));
        assert_eq!(ring.pop_back(), Some(4));
        assert_eq!(ring.pop_front(), None);
        for i in 0..CAPACITY - 1 {
            assert!(ring.push_back(i).is_ok(), "on {}", i)
        }
        assert_eq!(ring.push_front(CAPACITY).err(), Some(CAPACITY));
        assert_eq!(ring.push_back(CAPACITY + 1).err(), Some(CAPACITY + 1));
        for i in 0..CAPACITY - 1 {
            assert_eq!(ring.pop_front(), Some(i));
        }
        assert_eq!(ring.pop_back(), None);
    }

    #[test]
    pub fn arc() {
        const CAPACITY: usize = 32;
        let ring = RingBuffer::<Arc<u32>, CAPACITY>::new();
        ring.push_front(Arc::new(42)).unwrap();
        assert_eq!(*ring.pop_front().unwrap(), 42);
    }

    const NUM: usize = 8192;
    const CAP: usize = NUM * 2;
    const ROUNDS: usize = NUM;

    par_list_tests!(
        usize_test,
        usize,
        { |n| n as usize },
        { RingBuffer::<_, CAP>::new() },
        NUM
    );

    par_list_tests!(
        on_heap_test,
        Vec<usize>,
        { |n| vec![n as usize] },
        { RingBuffer::<_, CAP>::new() },
        NUM
    );

    fn item_from(n: usize) -> Vec<usize> {
        vec![n]
    }

    #[test]
    #[ignore]
    pub fn multithread_push_front_remove() {
        let num: usize = NUM;
        let deque = Arc::new(RingBuffer::<_, CAP>::new());
        let threshold = (num as f64 * 0.5) as usize;
        let threads = 256;
        (0..ROUNDS).for_each(|round| {
            let deposits = (0..threshold)
                .map(|i| {
                    let item = item_from(i);
                    deque.push_front(item).unwrap().to_ptr()
                })
                .chunks(threads)
                .into_iter()
                .map(|chunk| chunk.collect_vec())
                .collect_vec();
            let ths = (threshold..num)
                .chunks(threads)
                .into_iter()
                .zip(deposits)
                .map(|(nums, thread_deposit)| {
                    let nums = nums.collect_vec();
                    let deque = deque.clone();
                    thread::spawn(move || {
                        let to_remove = thread_deposit;
                        let mut rm_idx = 0;
                        nums.into_iter()
                            .map(|i| {
                                let item = item_from(i);
                                if i % 2 == 0 {
                                    (Some(deque.push_front(item).unwrap().deref().unwrap()), None)
                                } else {
                                    rm_idx += 1;
                                    unsafe {
                                        (None, to_remove[rm_idx - 1].to_ref().remove())
                                    }
                                }
                            })
                            .collect_vec()
                    })
                })
                .collect::<Vec<_>>();
            let (inserted, removed) : (Vec<_>, Vec<_>) = ths
                .into_iter()
                .map(|j| j.join().unwrap().into_iter())
                .flatten()
                .unzip();
            let remove_result = removed
                .into_iter()
                .filter_map(|n| n)
                .collect::<Vec<_>>();
            let inserted_result = inserted
                .into_iter()
                .filter_map(|n| n)
                .collect::<Vec<_>>();
            let remove_result_len = remove_result.len();
            let inserted_result_len = inserted_result.len();
            let remains_len = threshold + inserted_result_len - remove_result_len;
            assert_eq!(remove_result_len, (num - threshold) / 2);
            assert_eq!(inserted_result_len, (num - threshold) / 2);
            let remove_set = remove_result.into_iter().collect::<HashSet<_>>();
            assert_eq!(remove_result_len, remove_set.len());
            assert!(deque.count() >= remains_len, "At round {}, should have dequue count {} >= {}", round, deque.count(), remains_len);
            let mut removed_remains = 0;
            while let Some(r) = deque.peek_front() {
                r.remove().unwrap();
                removed_remains += 1;
            }
            assert_eq!(removed_remains, remains_len, "At round {round}");
            assert_eq!(deque.count(), 0);
        });
    }

    #[test]
    pub fn multithread_push_back_remove() {
        let num: usize = NUM;
        let deque = Arc::new(RingBuffer::<_, CAP>::new());
        let threshold = (num as f64 * 0.5) as usize;
        let threads = 256;
        let deposits = (0..threshold)
            .map(|i| {
                let item = item_from(i);
                deque.push_back(item).unwrap().to_ptr()
            })
            .chunks(threads)
            .into_iter()
            .map(|chunk| chunk.collect_vec())
            .collect_vec();
        let ths = (threshold..num)
            .chunks(threads)
            .into_iter()
            .zip(deposits)
            .map(|(nums, thread_deposit)| {
                let nums = nums.collect_vec();
                let deque = deque.clone();
                thread::spawn(move || {
                    let to_remove = thread_deposit;
                    let mut rm_idx = 0;
                    nums.into_iter()
                        .map(|i| {
                            let item = item_from(i);
                            if i % 2 == 0 {
                                (Some(deque.push_back(item).unwrap().deref().unwrap()), None)
                            } else {
                                rm_idx += 1;
                                unsafe {
                                    (None, to_remove[rm_idx - 1].to_ref().remove())
                                }
                            }
                        })
                        .collect_vec()
                })
            })
            .collect::<Vec<_>>();
        let (inserted, removed) : (Vec<_>, Vec<_>) = ths
            .into_iter()
            .map(|j| j.join().unwrap().into_iter())
            .flatten()
            .unzip();
        let remove_result = removed
            .into_iter()
            .filter_map(|n| n)
            .collect::<Vec<_>>();
        let remove_result_len = remove_result.len();
        assert_eq!(remove_result_len, (num - threshold) / 2);
        let remove_set = remove_result.into_iter().collect::<HashSet<_>>();
        assert_eq!(remove_result_len, remove_set.len());
    }
}
