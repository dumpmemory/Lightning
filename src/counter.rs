// A Thread-safe efficient counter

use std::ptr;
use std::sync::atomic::AtomicIsize;
use std::sync::atomic::Ordering::*;

use crate::thread_local::ThreadMeta;

const SPREAD_COUNT: usize = 64;
const ID_MASK: usize = SPREAD_COUNT - 1;
const DEFAULT_CNT: AtomicIsize = AtomicIsize::new(0);

pub struct Counter {
  subcnt: [AtomicIsize; SPREAD_COUNT]
}

impl Counter {

  #[inline(always)]
  pub fn new() -> Self {
    Self {
      subcnt: [DEFAULT_CNT; SPREAD_COUNT]
    }
  }

  #[inline(always)]
  pub fn incr(&self, amount: usize) {
    let id = ThreadMeta::get_id() & ID_MASK;
    self.subcnt[id].fetch_add(amount as isize, Relaxed);
  }


  #[inline(always)]
  pub fn decr(&self, amount: usize) {
    let id = ThreadMeta::get_id() & ID_MASK;
    self.subcnt[id].fetch_sub(amount as isize, Relaxed);
  }


  #[inline(always)]
  pub fn sum(&self) -> usize {
    self.subcnt.iter().map(|c| unsafe {
      ptr::read(c.as_mut_ptr())
    }).sum::<isize>() as usize
  }

  #[inline(always)]
  pub fn sum_strong(&self) -> usize {
    self.subcnt.iter().map(|c| c.load(Acquire)).sum::<isize>() as usize
  }
}