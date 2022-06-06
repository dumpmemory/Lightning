// A Thread-safe efficient counter

use std::ptr;
use std::sync::atomic::AtomicIsize;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::*;

use crate::thread_local::ThreadMeta;

const SPREAD_COUNT: usize = 32;
const ID_MASK: usize = SPREAD_COUNT - 1;
const DEFAULT_CNT: AtomicIsize = AtomicIsize::new(0);
const DICE_MASK: usize = !(!0 << 2);

pub struct Counter {
  subcnt: [AtomicIsize; SPREAD_COUNT],
  approx_sum: AtomicUsize
}

impl Counter {

  #[inline(always)]
  pub fn new() -> Self {
    Self {
      subcnt: [DEFAULT_CNT; SPREAD_COUNT],
      approx_sum: AtomicUsize::new(0)
    }
  }

  #[inline(always)]
  pub fn incr(&self, amount: usize) {
    let id = ThreadMeta::get_id() & ID_MASK;
    let n = self.subcnt[id].fetch_add(amount as isize, Relaxed);
    self.update_approx(amount, n);
  }


  #[inline(always)]
  pub fn decr(&self, amount: usize) {
    let id = ThreadMeta::get_id() & ID_MASK;
    let n = self.subcnt[id].fetch_sub(amount as isize, Relaxed);
    self.update_approx(amount, n);
  }


  #[inline(always)]
  pub fn sum(&self) -> usize {
    self.subcnt.iter().map(|c| unsafe {
      ptr::read(c.as_mut_ptr())
    }).sum::<isize>() as usize
  }

  #[inline(always)]
  pub fn sum_approx(&self) -> usize {
    unsafe {
      ptr::read(self.approx_sum.as_mut_ptr())
    }
  }

  #[inline(always)]
  pub fn sum_strong(&self) -> usize {
    self.subcnt.iter().map(|c| c.load(Acquire)).sum::<isize>() as usize
  }

  #[inline(always)]
  fn update_approx(&self, change: usize, n: isize) {
    if n as usize & DICE_MASK == 0b10 || change > 10 {
      unsafe {
        ptr::write(self.approx_sum.as_mut_ptr(), self.sum())
      }
    }
  }

  #[inline(always)]
  pub fn store(&self, num: usize) {
    let now_count = self.sum_strong() as isize;
    let delta = num as isize - now_count;
    let id = ThreadMeta::get_id() & ID_MASK;
    self.subcnt[id].fetch_add(delta, Relaxed);
    self.approx_sum.store(self.sum(), Relaxed);
  }
}