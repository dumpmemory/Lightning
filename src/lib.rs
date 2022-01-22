#![feature(core_intrinsics)]
#![feature(allocator_api)]
#![feature(const_fn_trait_bound)]
#![feature(test)]
#[macro_use]
extern crate log;
extern crate alloc;
extern crate test;

// pub mod deque;
pub mod linked_map;
pub mod list;
pub mod map;
pub mod spin;
pub mod ttl_cache;
pub mod ring_buffer;

pub mod rand;

pub fn align_padding(len: usize, align: usize) -> usize {
    let len_rounded_up = len.wrapping_add(align).wrapping_sub(1) & !align.wrapping_sub(1);
    len_rounded_up.wrapping_sub(len)
}
