#![feature(core_intrinsics)]
#![feature(allocator_api)]
#![feature(test)]
#![feature(once_cell)]
#![feature(atomic_mut_ptr)]
#![feature(thread_id_value)]
#![feature(maybe_uninit_uninit_array)]

#[macro_use]
extern crate log;
extern crate alloc;
extern crate test;

#[macro_use]
extern crate static_assertions;
#[macro_use]
extern crate lazy_static;
// pub mod deque;
pub mod linked_map;
pub mod list;
pub mod lru_cache;
pub mod map;
pub mod ring_buffer;
pub mod rw_spin;
pub mod spin;
pub mod stack;
pub mod ttl_cache;

pub mod aarc;
pub mod obj_alloc;
pub mod rand;
pub mod thread_local;
pub mod counter;

#[cfg(test)]
mod tests_misc;

#[macro_use]
mod par_list_test_macros;

pub const fn align_padding(len: usize, align: usize) -> usize {
    let len_rounded_up = len.wrapping_add(align).wrapping_sub(1) & !align.wrapping_sub(1);
    len_rounded_up.wrapping_sub(len)
}

fn thread_id() -> u64 {
    std::thread::current().id().as_u64().get()
}
