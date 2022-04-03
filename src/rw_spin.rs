use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{
    AtomicUsize,
    Ordering::*,
};

pub struct RwSpinLock<T> {
    mark: AtomicUsize,
    obj: UnsafeCell<T>,
}

pub struct WriteSpinLockGuard<'a, T> {
    lock: &'a RwSpinLock<T>,
}

pub struct ReadSpinLockGuard<'a, T> {
    lock: &'a RwSpinLock<T>,
}

impl<T> RwSpinLock<T> {
    pub fn new(obj: T) -> Self {
        Self {
            mark: AtomicUsize::new(0),
            obj: UnsafeCell::new(obj),
        }
    }

    #[inline(always)]
    pub fn write(&self) -> WriteSpinLockGuard<T> {
        let backoff = crossbeam_utils::Backoff::new();
        while self.mark.compare_exchange(0, 1, AcqRel, Relaxed).is_err() {
            backoff.spin();
        }
        WriteSpinLockGuard { lock: self }
    }

    #[inline(always)]
    pub fn read(&self) -> ReadSpinLockGuard<T> {
        let backoff = crossbeam_utils::Backoff::new();
        loop {
            let mark = self.mark.load(Acquire);
            if mark != 1 && self.mark.compare_exchange(mark, mark + 2, AcqRel, Relaxed).is_ok() {
                break;
            }
            backoff.spin();
        }
        ReadSpinLockGuard { lock: self }
    }
}

impl<'a, T> Deref for WriteSpinLockGuard<'a, T> {
    type Target = T;
    fn deref(&self) -> &'a Self::Target {
        unsafe { &*self.lock.obj.get() }
    }
}

impl<'a, T> DerefMut for WriteSpinLockGuard<'a, T> {
    fn deref_mut(&mut self) -> &'a mut T {
        unsafe { &mut *self.lock.obj.get() }
    }
}

impl<'a, T> Drop for WriteSpinLockGuard<'a, T> {
    #[inline(always)]
    fn drop(&mut self) {
        self.lock.mark.store(0, Release);
    }
}

impl<'a, T> Deref for ReadSpinLockGuard<'a, T> {
    type Target = T;
    fn deref(&self) -> &'a Self::Target {
        unsafe { &*self.lock.obj.get() }
    }
}

impl<'a, T> Drop for ReadSpinLockGuard<'a, T> {
    #[inline(always)]
    fn drop(&mut self) {
        self.lock.mark.fetch_sub(2, Release);
    }
}

unsafe impl<T> Sync for RwSpinLock<T> {}

#[cfg(test)]
mod test {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn lot_load_of_lock() {
        let lock = Arc::new(RwSpinLock::new(0));
        let num_threads = 32;
        let thread_turns = 2048;
        let mut threads = vec![];
        for _ in 0..num_threads {
            let lock = lock.clone();
            threads.push(thread::spawn(move || {
                for _ in 0..thread_turns {
                    *lock.write() += 1;
                }
            }));
        }
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(*lock.write(), num_threads * thread_turns);
    }
}
