// A simple pump pointer lock-free allocator

use libc::c_void;

use crate::list::LinkedRingBufferList;
use std::{
    mem,
    sync::atomic::{AtomicU32, Ordering::*},
};

pub struct Allocator<T> {
    buffer: *const T,
    free_list: LinkedRingBufferList<u32, 64>,
    pointer: AtomicU32,
    capacity: u32,
}

impl<T> Allocator<T> {
    pub fn new(capacity: u32) -> Self {
        let buffer_size = mem::size_of::<T>() * (capacity as usize);
        let buffer = unsafe { libc::malloc(buffer_size) as *const T };
        let free_list = LinkedRingBufferList::new();
        let pointer = AtomicU32::new(0);
        Self {
            buffer,
            free_list,
            capacity,
            pointer,
        }
    }
    pub fn allocate(&self) -> u32 {
        loop {
            if let Some(ptr) = self.free_list.pop_front() {
                return ptr;
            }
            let bump = self.pointer.load(Acquire);
            if bump < self.capacity {
                if let Err(_) = self
                    .pointer
                    .compare_exchange(bump, bump + 1, AcqRel, Acquire)
                {
                    continue;
                } else {
                    return bump;
                }
            }
        }
    }

    pub fn free(&self, ptr: u32) {
        self.free_list.push_front(ptr);
    }

    pub fn raw_ptr_of(&self, ptr: u32) -> *mut T {
        (self.buffer as usize + (ptr as usize) * mem::size_of::<T>()) as *mut T
    }

    pub unsafe fn get_ref(&self, ptr: u32) -> &T {
        &*(self.raw_ptr_of(ptr))
    }

    pub unsafe fn get_mut(&self, ptr: u32) -> &T {
        &mut *(self.raw_ptr_of(ptr))
    }

    pub unsafe fn set_ptr(&self, ptr: u32) {
        self.pointer.store(ptr, Release);
    }
}

impl<T> Drop for Allocator<T> {
    fn drop(&mut self) {
        unsafe { libc::free(self.buffer as *mut c_void) }
    }
}
