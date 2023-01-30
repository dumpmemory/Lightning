use std::intrinsics::forget;
use std::mem::{self};
use std::sync::atomic::Ordering::*;
use std::{
    ops::Deref,
    sync::atomic::{AtomicPtr, AtomicUsize},
};

use crate::spin::SpinLock;
use crossbeam_utils::atomic::AtomicConsume;

pub struct Arc<T> {
    ptr: *const Inner<T>,
}

impl<T> Arc<T> {
    #[inline(always)]
    pub fn new(val: T) -> Self {
        Self {
            ptr: Box::into_raw(Box::new(Inner::new(val))),
        }
    }

    #[inline(always)]
    fn from_ptr(ptr: *const Inner<T>) -> Self {
        Self { ptr }
    }

    #[inline(always)]
    pub fn null() -> Self {
        Self::from_ptr(0 as *const Inner<T>)
    }

    #[inline(always)]
    pub fn is_null(&self) -> bool {
        self.ptr.is_null()
    }

    #[inline(always)]
    pub unsafe fn as_mut(&self) -> &mut T {
        // assert_eq!((*self.ptr).count.load(Relaxed), 1);
        &mut (*(self.ptr as *mut Inner<T>)).val
    }
}

impl<T> Deref for Arc<T> {
    type Target = T;

    #[inline(always)]
    fn deref(&self) -> &Self::Target {
        &Inner::from_ptr(self.ptr).val
    }
}

impl<T> Drop for Arc<T> {
    #[inline(always)]
    fn drop(&mut self) {
        decr_ref(self.ptr)
    }
}

impl<T> Drop for AtomicArc<T> {
    #[inline(always)]
    fn drop(&mut self) {
        decr_ref(self.ptr.load_consume())
    }
}

impl<T> Clone for Arc<T> {
    #[inline(always)]
    fn clone(&self) -> Self {
        incr_ref(self.ptr);
        Self {
            ptr: self.ptr.clone(),
        }
    }
}

pub struct AtomicArc<T> {
    lock: SpinLock<()>,
    ptr: AtomicPtr<Inner<T>>,
}

impl<T> AtomicArc<T> {
    #[inline(always)]
    pub fn new(val: T) -> Self {
        Self {
            lock: SpinLock::new(()),
            ptr: AtomicPtr::new(Box::into_raw(Box::new(Inner::new(val)))),
        }
    }

    #[inline(always)]
    pub fn from_rc(rc: Arc<T>) -> Self {
        let res = Self {
            lock: SpinLock::new(()),
            ptr: AtomicPtr::new(rc.ptr as *mut Inner<T>),
        };
        mem::forget(rc);
        return res;
    }

    #[inline(always)]
    pub fn null() -> Self {
        Self {
            lock: SpinLock::new(()),
            ptr: AtomicPtr::new(0 as *mut Inner<T>),
        }
    }

    #[inline(always)]
    pub fn is_null(&self) -> bool {
        self.ptr.load(Acquire).is_null()
    }

    #[inline(always)]
    pub fn swap_ref(&self, r: Arc<T>) -> Arc<T> {
        let _g = self.lock.lock();
        Arc {
            ptr: self.ptr.swap(r.ptr as *mut Inner<T>, Relaxed),
        }
    }

    #[inline(always)]
    pub fn store(&self, val: T) {
        let inner = Box::new(Inner::new(val));
        let _g = self.lock.lock();
        let old = { self.ptr.swap(Box::into_raw(inner), Relaxed) };
        decr_ref(old)
    }

    #[inline(always)]
    pub fn store_ref(&self, val: Arc<T>) {
        let _g = self.lock.lock();
        let old = { self.ptr.swap(val.ptr as *mut Inner<T>, Relaxed) };
        mem::forget(val);
        decr_ref(old)
    }

    #[inline(always)]
    pub fn load(&self) -> Arc<T> {
        let _g = self.lock.lock();
        let ptr = {
            let ptr = self.ptr.load(Relaxed);
            incr_ref(ptr);
            ptr
        };
        Arc::from_ptr(ptr)
    }

    #[inline(always)]
    pub fn compare_exchange(&self, current: &Arc<T>, new: &Arc<T>) -> Result<(), Arc<T>> {
        let _g = self.lock.lock();
        let cas_res = {
            self.ptr.compare_exchange(
                current.ptr as *mut Inner<T>,
                new.ptr as *mut Inner<T>,
                AcqRel,
                Relaxed,
            )
        };
        match cas_res {
            Ok(current) => {
                decr_ref(current);
                incr_ref(new.ptr);
                Ok(())
            }
            Err(current) => {
                incr_ref(current);
                Err(Arc::from_ptr(current))
            }
        }
    }

    #[inline(always)]
    pub fn compare_exchange_is_ok(&self, current: &Arc<T>, new: &Arc<T>) -> bool {
        let _g = self.lock.lock();
        let cas_res = {
            self.ptr.compare_exchange(
                current.ptr as *mut Inner<T>,
                new.ptr as *mut Inner<T>,
                AcqRel,
                Relaxed,
            )
        };
        match cas_res {
            Ok(current) => {
                decr_ref(current);
                incr_ref(new.ptr);
                true
            }
            Err(_current) => false,
        }
    }

    #[inline(always)]
    pub fn compare_exchange_value(&self, current: &Arc<T>, new: T) -> Result<(), Arc<T>> {
        let new = Box::into_raw(Box::new(Inner::new(new)));
        let _g = self.lock.lock();
        let cas_res = {
            self.ptr
                .compare_exchange(current.ptr as *mut Inner<T>, new, Relaxed, Relaxed)
        };
        match cas_res {
            Ok(current) => {
                decr_ref(current);
                Ok(())
            }
            Err(current) => {
                unsafe {
                    drop(Box::from_raw(new));
                }
                incr_ref(current);
                Err(Arc::from_ptr(current))
            }
        }
    }

    #[inline(always)]
    pub fn compare_exchange_value_is_ok(&self, current: &Arc<T>, new: T) -> bool {
        let new = Box::into_raw(Box::new(Inner::new(new)));
        let _g = self.lock.lock();
        let cas_res =
            self.ptr
                .compare_exchange(current.ptr as *mut Inner<T>, new, Relaxed, Relaxed);
        match cas_res {
            Ok(current) => {
                decr_ref(current);
                true
            }
            Err(_current) => {
                unsafe {
                    drop(Box::from_raw(new));
                }
                false
            }
        }
    }

    // #[inline(always)]
    // pub unsafe fn as_mut(&self) -> &mut T {
    //     let inner: &mut Inner<T> = mem::transmute_copy(&self.ptr);
    //     &mut inner.val
    // }

    #[inline(always)]
    pub fn into_arc(self) -> Arc<T> {
        let ptr = self.ptr.load(Acquire);
        forget(self);
        Arc { ptr }
    }

    #[inline(always)]
    pub(crate) unsafe fn inner(&self) -> *mut Inner<T> {
        self.ptr.load(Acquire)
    }
}

pub(crate) struct Inner<T> {
    val: T,
    count: AtomicUsize,
}

impl<T> Inner<T> {
    #[inline(always)]
    fn new(val: T) -> Self {
        Self {
            val,
            count: AtomicUsize::new(1),
        }
    }

    #[inline(always)]
    fn from_ptr<'a>(ptr: *const Self) -> &'a Self {
        unsafe { &*(ptr) }
    }
}

#[inline(always)]
fn decr_ref<T>(ptr: *const Inner<T>) {
    if ptr.is_null() {
        return;
    }
    let count = Inner::from_ptr(ptr).count.fetch_sub(1, AcqRel);
    if count <= 1 {
        unsafe {
            drop(Box::from_raw(ptr as *mut Inner<T>));
        }
    }
}

#[inline(always)]
fn incr_ref<T>(ptr: *const Inner<T>) {
    if ptr.is_null() {
        return;
    }
    Inner::from_ptr(ptr).count.fetch_add(1, AcqRel);
}

impl<T> Default for Arc<T> {
    fn default() -> Self {
        Self::null()
    }
}

unsafe impl<T> Send for Arc<T> {}
