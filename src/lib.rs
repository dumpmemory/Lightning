#![feature(core_intrinsics)]
#![feature(allocator_api)]
#![feature(test)]
#[macro_use]
extern crate log;
extern crate alloc;
extern crate test;

pub mod map;
pub mod list;

pub mod rand;

pub fn align_padding(len: usize, align: usize) -> usize {
    let len_rounded_up = len.wrapping_add(align).wrapping_sub(1) & !align.wrapping_sub(1);
    len_rounded_up.wrapping_sub(len)
}
