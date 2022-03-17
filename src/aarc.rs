use std::mem;
use std::sync::atomic::Ordering::*;
use std::{
    ops::Deref,
    sync::atomic::{AtomicPtr, AtomicUsize},
};

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
    ptr: AtomicPtr<Inner<T>>,
}

impl<T> AtomicArc<T> {


    #[inline(always)]
    pub fn new(val: T) -> Self {
        Self {
            ptr: AtomicPtr::new(Box::into_raw(Box::new(Inner::new(val)))),
        }
    }

    #[inline(always)]
    pub fn from_rc(rc: Arc<T>) -> Self {
        let res = Self {
            ptr: AtomicPtr::new(rc.ptr as *mut Inner<T>),
        };
        mem::forget(rc);
        return res;
    }

    #[inline(always)]
    pub fn null() -> Self {
        Self {
            ptr: AtomicPtr::new(0 as *mut Inner<T>),
        }
    }

    #[inline(always)]
    pub fn is_null(&self) -> bool {
        self.ptr.load(Acquire).is_null()
    }

    #[inline(always)]
    pub fn store(&self, val: T) {
        let inner = Box::new(Inner::new(val));
        let old = self.ptr.swap(Box::into_raw(inner), AcqRel);
        decr_ref(old)
    }

    #[inline(always)]
    pub fn store_ref(&self, val: Arc<T>) {
        let old = self.ptr.swap(val.ptr as *mut Inner<T>, AcqRel);
        mem::forget(val);
        decr_ref(old)
    }

    #[inline(always)]
    pub fn load(&self) -> Arc<T> {
        let ptr = self.ptr.load(Acquire);
        incr_ref(ptr);
        Arc::from_ptr(ptr)
    }

    #[inline(always)]
    pub fn compare_exchange(&self, current: &Arc<T>, new: &Arc<T>) -> Result<(), Arc<T>> {
        match self.ptr.compare_exchange(
            current.ptr as *mut Inner<T>,
            new.ptr as *mut Inner<T>,
            AcqRel,
            Acquire,
        ) {
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
        match self.ptr.compare_exchange(
            current.ptr as *mut Inner<T>,
            new.ptr as *mut Inner<T>,
            AcqRel,
            Acquire,
        ) {
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
        match self
            .ptr
            .compare_exchange(current.ptr as *mut Inner<T>, new, AcqRel, Acquire)
        {
            Ok(current) => {
                decr_ref(current);
                Ok(())
            }
            Err(current) => {
                incr_ref(current);
                Err(Arc::from_ptr(current))
            }
        }
    }

    #[inline(always)]
    pub fn compare_exchange_value_is_ok(&self, current: &Arc<T>, new: T) -> bool {
        let new = Box::into_raw(Box::new(Inner::new(new)));
        match self
            .ptr
            .compare_exchange(current.ptr as *mut Inner<T>, new, AcqRel, Acquire)
        {
            Ok(current) => {
                decr_ref(current);
                true
            }
            Err(_current) => false,
        }
    }

    #[inline(always)]
    pub unsafe fn as_mut(&self) -> &mut T {
        &mut (*self.ptr.load(Relaxed)).val
    }
}

struct Inner<T> {
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
            Box::from_raw(ptr as *mut Inner<T>);
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
