use std::sync::atomic::AtomicIsize;
use std::sync::atomic::Ordering::AcqRel;
use std::sync::atomic::Ordering::Relaxed;
use std::{cell::Cell, sync::atomic::AtomicUsize};
use std::{intrinsics, mem};

use crate::stack::LinkedRingBufferStack;

static GLOBAL_COUNTER: AtomicUsize = AtomicUsize::new(1);
static FREE_LIST: LinkedRingBufferStack<usize, 64> = LinkedRingBufferStack::const_new();

thread_local! {
    static THREAD_META: ThreadMeta = ThreadMeta::new();
}

struct ThreadMeta {
    tid: usize,
}

const FAST_THREADS: usize = 64;
const THREADS_MASK: usize = FAST_THREADS - 1;

pub struct Counter {
    counters: Box<[Cell<*mut AtomicIsize>; FAST_THREADS]>,
}

impl Counter {
    pub fn new() -> Self {
        Self {
            counters: Box::new(unsafe { mem::transmute([0isize; FAST_THREADS]) }),
        }
    }
    #[inline(always)]
    fn tl_get(&self) -> &AtomicIsize {
        let tid = ThreadMeta::get_id();
        let arr_id = tid & THREADS_MASK;
        let cell = &self.counters[arr_id];
        unsafe {
            if cell.get().is_null() {
                let cell_ptr = cell.as_ptr();
                let new_cell = Box::into_raw(Box::new(AtomicIsize::new(0)));
                if !intrinsics::atomic_cxchg_acqrel_relaxed(
                    cell_ptr as *mut usize,
                    0,
                    new_cell as usize,
                )
                .1
                {
                    // there is another new cell there
                    drop(Box::from_raw(new_cell));
                }
            }
            return &*(cell.get() as *mut _);
        }
    }
    pub fn incr(&self, num: isize) {
        self.tl_get().fetch_add(num, Relaxed);
    }
    pub fn decr(&self, num: isize) {
        self.tl_get().fetch_sub(num, Relaxed);
    }
    pub fn count(&self) -> isize {
        self.counters
            .iter()
            .filter_map(|c| unsafe { c.get().as_ref().map(|c| c.load(Relaxed)) })
            .sum()
    }
}

impl ThreadMeta {
    #[inline(always)]
    fn new() -> Self {
        let tid = FREE_LIST
            .pop()
            .unwrap_or_else(|| GLOBAL_COUNTER.fetch_add(1, AcqRel));
        ThreadMeta { tid }
    }

    #[inline(always)]
    pub fn get_id() -> usize {
        THREAD_META.with(|m| m.tid)
    }
}

impl Drop for ThreadMeta {
    fn drop(&mut self) {
        FREE_LIST.push(self.tid);
    }
}

impl Drop for Counter {
    fn drop(&mut self) {
        for cell in self.counters.iter() {
            let addr = cell.get();
            if addr.is_null() {
                continue;
            }
            unsafe {
                drop(Box::from_raw(addr));
            }
            cell.set(0 as *mut _);
        }
    }
}

unsafe impl Sync for Counter {}
unsafe impl Send for Counter {}
