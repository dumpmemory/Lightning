// Lock-free struct local thread local

use crate::map::{Map, PassthroughHasher};
use crate::{map::LiteHashMap, stack::LinkedRingBufferStack};
use std::alloc::System;
use std::cell::{Cell, RefCell, UnsafeCell};
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
    freed_maps: RefCell<Vec<Box<Fn()>>>,
}

pub struct ThreadLocal<T> {
    fast_map: *mut [Cell<usize>],
    reserve_map: LiteHashMap<u64, usize, System, PassthroughHasher>,
    _marker: PhantomData<T>,
}

impl<T> ThreadLocal<T> {
    const OBJ_SIZE: usize = mem::size_of::<T>();

    pub fn new() -> Self {
        let fast_map_size = Self::fast_map_size();
        let fast_map_data_size = fast_map_size * mem::size_of::<usize>();
        let fast_map_alloc = unsafe { libc::malloc(fast_map_data_size) };
        let fast_map = unsafe {
            libc::memset(fast_map_alloc, 0, fast_map_data_size);
            ptr::slice_from_raw_parts(fast_map_alloc as *const usize, fast_map_size)
                as *mut [Cell<usize>]
        };
        Self {
            fast_map: fast_map,
            reserve_map: LiteHashMap::with_capacity(num_cpus::get()),
            _marker: PhantomData,
        }
    }

    pub fn get_or<F: Fn() -> T>(&self, new: F) -> &T {
        unsafe {
            let hash = ThreadMeta::get_hash();
            let idx = hash as usize;
            let fast_map = &mut *self.fast_map;
            let obj_ptr = if idx < fast_map.len() {
                let cell = &fast_map[idx];
                if cell.get() == 0 {
                    let ptr = libc::malloc(Self::OBJ_SIZE) as *mut T;
                    ptr::write(ptr, new());
                    cell.set(ptr as usize);
                }
                cell.get()
            } else {
                self.reserve_map.get_or_insert(&hash, move || {
                    let ptr = libc::malloc(Self::OBJ_SIZE) as *mut T;
                    ptr::write(ptr, new());
                    ptr as usize
                })
            };
            return &*(obj_ptr as *const T);
        }
    }

    fn fast_map_size() -> usize {
        num_cpus::get() * 4
    }
}

impl ThreadMeta {
    fn new() -> Self {
        let hash = FREE_LIST
            .pop()
            .unwrap_or_else(|| GLOBAL_COUNTER.fetch_add(1, AcqRel));
        ThreadMeta {
            hash,
            freed_maps: RefCell::new(vec![]),
        }
    }

    pub fn get_hash() -> u64 {
        THREAD_META.with(|m| m.hash)
    }
}

impl Drop for ThreadMeta {
    fn drop(&mut self) {
        FREE_LIST.push(self.hash);
        for free_map in self.freed_maps.borrow().iter() {
            free_map()
        }
    }
}

impl<T> Drop for ThreadLocal<T> {
    fn drop(&mut self) {
        for (_, v) in self.reserve_map.entries() {
            unsafe {
                libc::free(v as *mut libc::c_void);
            }
        }
        let fast_map = unsafe { &mut *self.fast_map };
        for cell in fast_map.iter() {
            let addr = cell.get();
            if addr == 0 {
                continue;
            }
            unsafe {
                libc::free(addr as *mut libc::c_void);
            }
            cell.set(0);
        }
        let fast_map_ptr = self.fast_map as *mut libc::c_void as usize;
        THREAD_META.with(move |m| {
            let mut free_maps = m.freed_maps.borrow_mut();
            free_maps.push(Box::new(move || unsafe {
                libc::free(fast_map_ptr as *mut libc::c_void);
            }))
        });
    }
}

unsafe impl<T> Sync for ThreadLocal<T> {}
unsafe impl<T> Send for ThreadLocal<T> {}
