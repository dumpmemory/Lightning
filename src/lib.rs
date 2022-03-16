#![feature(core_intrinsics)]
#![feature(allocator_api)]
#![feature(const_fn_trait_bound)]
#![feature(test)]
#![feature(once_cell)]
#[macro_use]
extern crate log;
extern crate alloc;
extern crate test;

#[macro_use]
extern crate static_assertions;
// pub mod deque;
pub mod linked_map;
pub mod list;
pub mod lru_cache;
pub mod map;
pub mod ring_buffer;
pub mod spin;
pub mod stack;
pub mod ttl_cache;

pub mod obj_alloc;
pub mod rand;
pub mod thread_local;
pub mod aarc;

#[macro_use]
mod par_list_test_macros;

pub const fn align_padding(len: usize, align: usize) -> usize {
    let len_rounded_up = len.wrapping_add(align).wrapping_sub(1) & !align.wrapping_sub(1);
    len_rounded_up.wrapping_sub(len)
}
