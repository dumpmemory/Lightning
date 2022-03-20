// Lock-free struct local thread local

use crate::map::{Map, PassthroughHasher};
use crate::{map::LiteHashMap, stack::LinkedRingBufferStack};
use std::alloc::System;
use std::cell::Cell;
use std::marker::PhantomData;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::*;
use std::{mem, ptr};

static GLOBAL_COUNTER: AtomicUsize = AtomicUsize::new(1);
static FREE_LIST: LinkedRingBufferStack<usize, 64> = LinkedRingBufferStack::const_new();

thread_local! {
  static THREAD_META: ThreadMeta = ThreadMeta::new();
}

struct ThreadMeta {
    tid: usize,
}

const FAST_THREADS: usize = 512;

pub struct ThreadLocal<T> {
    fast_map: [Cell<usize>; FAST_THREADS],
    reserve_map: LiteHashMap<usize, usize, System, PassthroughHasher>,
    _marker: PhantomData<T>,
}

impl<T> ThreadLocal<T> {
    const OBJ_SIZE: usize = mem::size_of::<T>();

    #[inline(always)]
    pub fn new() -> Self {
        Self {
            fast_map: unsafe { mem::transmute([0usize; FAST_THREADS]) },
            reserve_map: LiteHashMap::with_capacity(num_cpus::get().next_power_of_two()),
            _marker: PhantomData,
        }
    }

    #[inline(always)]
    pub fn get_or<F: Fn() -> T>(&self, new: F) -> &mut T {
        unsafe {
            let tid = ThreadMeta::get_id();
            if tid < FAST_THREADS {
                let cell = &self.fast_map[tid];
                if cell.get() == 0 {
                    let ptr = libc::malloc(Self::OBJ_SIZE) as *mut T;
                    ptr::write(ptr, new());
                    cell.set(ptr as usize);
                }
                &mut *(cell.get() as *mut T)
            } else {
                let obj_ptr = self.reserve_map.get_or_insert(&tid, move || {
                    let ptr = libc::malloc(Self::OBJ_SIZE) as *mut T;
                    ptr::write(ptr, new());
                    ptr as usize
                });
                &mut *(obj_ptr as *mut T)
            }
        }
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

impl<T> Drop for ThreadLocal<T> {
    fn drop(&mut self) {
        for (_, v) in self.reserve_map.entries() {
            unsafe {
                libc::free(v as *mut libc::c_void);
            }
        }
        for cell in self.fast_map.iter() {
            let addr = cell.get();
            if addr == 0 {
                continue;
            }
            unsafe {
                libc::free(addr as *mut libc::c_void);
            }
            cell.set(0);
        }
    }
}

unsafe impl<T> Sync for ThreadLocal<T> {}
unsafe impl<T> Send for ThreadLocal<T> {}
