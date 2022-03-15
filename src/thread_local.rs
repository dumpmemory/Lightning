// Lock-free struct local thread local

use crate::map::Map;
use crate::{map::LiteHashMap, stack::LinkedRingBufferStack};
use std::marker::PhantomData;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering::*;
use std::{mem, ptr};

static GLOBAL_COUNTER: AtomicU64 = AtomicU64::new(0);
static FREE_LIST: LinkedRingBufferStack<u64, 64> = LinkedRingBufferStack::const_new();

thread_local! {
  static THREAD_META: ThreadMeta = ThreadMeta::new();
}

struct ThreadMeta {
    hash: u64,
}

pub struct ThreadLocal<T> {
    map: LiteHashMap<u64, usize>,
    _marker: PhantomData<T>,
}

impl<T> ThreadLocal<T> {
    const OBJ_SIZE: usize = mem::size_of::<T>();

    pub fn new() -> Self {
        Self {
            map: LiteHashMap::with_capacity(num_cpus::get()),
            _marker: PhantomData,
        }
    }

    pub fn get_or<F: Fn() -> T>(&self, new: F) -> &T {
        unsafe {
            let hash = ThreadMeta::get_hash();
            let ptr = self.map.get_or_insert(&hash, move || {
                let ptr = libc::malloc(Self::OBJ_SIZE) as *mut T;
                ptr::write(ptr, new());
                ptr as usize
            });
            &*(ptr as *const T)
        }
    }
}

impl ThreadMeta {
    fn new() -> Self {
        let hash = FREE_LIST
            .pop()
            .unwrap_or_else(|| GLOBAL_COUNTER.fetch_add(1, AcqRel));
        ThreadMeta { hash }
    }

    pub fn get_hash() -> u64 {
        THREAD_META.with(|m| m.hash)
    }
}

impl Drop for ThreadMeta {
    fn drop(&mut self) {
        FREE_LIST.push(self.hash);
    }
}

impl<T> Drop for ThreadLocal<T> {
    fn drop(&mut self) {
        for (_, v) in self.map.entries() {
            unsafe {
                libc::free(v as *mut libc::c_void);
            }
        }
    }
}

unsafe impl<T> Sync for ThreadLocal<T> {}
unsafe impl<T> Send for ThreadLocal<T> {}
