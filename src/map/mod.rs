use crate::align_padding;
use alloc::vec::Vec;
use core::alloc::{GlobalAlloc, Layout};
use core::hash::Hasher;
use core::marker::PhantomData;
use core::ops::Deref;
use core::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release, SeqCst};
use core::sync::atomic::{compiler_fence, fence, AtomicUsize};
use core::{intrinsics, mem, ptr};
use crossbeam_epoch::*;
use crossbeam_utils::Backoff;
use rand::Rng;
use std::alloc::System;
use std::collections::hash_map::DefaultHasher;
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::DerefMut;
use std::os::raw::c_void;

pub(crate) mod base;
pub(crate) mod fat_map;
pub(crate) mod hash_set;
pub(crate) mod lite_map;
pub(crate) mod obj_map;
pub(crate) mod ptr_map;

pub type FKey = usize;
pub type FVal = usize;

#[cfg(test)]
mod tests;
mod word_map;

pub use fat_map::*;
pub use hash_set::*;
pub use lite_map::*;
pub use obj_map::*;
pub use ptr_map::*;
pub use word_map::*;

pub trait Map<K, V: Clone> {
    fn with_capacity(cap: usize) -> Self;
    fn get(&self, key: &K) -> Option<V>;
    fn insert(&self, key: K, value: V) -> Option<V>;
    // Return None if insertion successful
    fn try_insert(&self, key: K, value: V) -> Option<V>;
    fn remove(&self, key: &K) -> Option<V>;
    fn entries(&self) -> Vec<(K, V)>;
    fn contains_key(&self, key: &K) -> bool;
    fn len(&self) -> usize;
    // The func function should  be pure and have no side effect
    fn get_or_insert<F: Fn() -> V>(&self, key: K, func: F) -> V {
        loop {
            if self.contains_key(&key) {
                if let Some(value) = self.get(&key) {
                    return value;
                }
            } else {
                let value = func();
                if let Some(value) = self.try_insert(key, value.clone()) {
                    return value;
                }
                return value;
            }
        }
    }
    fn clear(&self);
}

#[inline(always)]
pub fn hash_num<H: Hasher + Default>(num: usize) -> usize {
    let mut hasher = H::default();
    hasher.write_usize(num);
    hasher.finish() as usize
}

#[inline(always)]
pub fn hash_key<K: Hash, H: Hasher + Default>(key: &K) -> usize {
    let mut hasher = H::default();
    key.hash(&mut hasher);
    hasher.finish() as usize
}

#[inline(always)]
fn dfence() {
    compiler_fence(SeqCst);
    fence(SeqCst);
}

pub trait Attachment<K, V> {
    type InitMeta: Clone;
    type Item: AttachmentItem<K, V> + Copy;

    fn heap_entry_size() -> usize;
    fn new(heap_ptr: usize, meta: &Self::InitMeta) -> Self;
    fn prefetch(&self, index: usize) -> Self::Item;
}

pub trait AttachmentItem<K, V> {
    fn get_key(self) -> K;
    fn get_value(self) -> V;
    fn set_key(self, key: K);
    fn set_value(self, value: V, old_fval: FVal);
    fn erase_value(self, old_fval: FVal);
    fn moveout_key(self) -> K;
    fn probe(self, probe_key: &K) -> bool;
    fn prep_write(self);
}

pub struct PassthroughHasher {
    num: u64,
}

impl Hasher for PassthroughHasher {
    fn finish(&self) -> u64 {
        self.num
    }

    fn write(&mut self, _bytes: &[u8]) {
        unimplemented!()
    }

    fn write_usize(&mut self, i: usize) {
        self.num = i as u64
    }
}

impl Default for PassthroughHasher {
    fn default() -> Self {
        Self { num: 0 }
    }
}

#[inline(always)]
fn occupation_limit(cap: usize) -> u32 {
    //let mut rng = rand::thread_rng();
    let ratio = 0.85; // rng.gen_range(0.75..0.85);
    (cap as f64 * ratio) as _
}

#[inline(always)]
fn alloc_mem<A: GlobalAlloc + Default>(size: usize) -> usize {
    let align = 64;
    let layout = Layout::from_size_align(size, align).unwrap();
    let alloc = A::default();
    // must be all zeroed
    unsafe {
        let addr = alloc.alloc(layout) as usize;
        debug_assert_eq!(addr % 64, 0);
        addr
    }
}

#[inline(always)]
unsafe fn fill_zeros(addr: usize, size: usize) {
    libc::memset(addr as _, 0, size);
}
