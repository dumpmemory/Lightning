// A supposed to be fast and lock-free object allocator without size class

use std::{cell::UnsafeCell, marker::PhantomData, mem};

use crate::{
    aarc::{Arc, AtomicArc},
    ring_buffer::{ACQUIRED, EMPTY},
    thread_local::ThreadLocal,
};
use std::sync::atomic::Ordering::Relaxed;

use crate::{
    ring_buffer::RingBuffer,
    stack::{LinkedRingBufferStack, RingBufferNode},
};

pub struct Allocator<T, const B: usize> {
    shared: Arc<SharedAlloc<T, B>>,
    thread: ThreadLocal<TLAlloc<T, B>>,
}

pub struct TLAlloc<T, const B: usize> {
    inner: UnsafeCell<TLAllocInner<T, B>>,
}

pub struct TLAllocInner<T, const B: usize> {
    buffer: usize,
    buffer_limit: usize,
    free_list: TLBufferedStack<usize, B>,
    shared: Arc<SharedAlloc<T, B>>,
    guard_count: usize,
    defer_free: Vec<usize>,
    _marker: PhantomData<T>,
}

pub struct SharedAlloc<T, const B: usize> {
    free_obj: LinkedRingBufferStack<usize, B>,
    free_buffer: LinkedRingBufferStack<(usize, usize), B>,
    all_buffers: LinkedRingBufferStack<usize, B>,
    _marker: PhantomData<T>,
}

impl<T, const B: usize> Allocator<T, B> {
    pub fn new() -> Self {
        Self {
            shared: Arc::new(SharedAlloc::new()),
            thread: ThreadLocal::new(),
        }
    }

    pub fn alloc(&self) -> *mut T {
        let tl_alloc = self.tl_alloc().expect("Cannot alloc for no tl");
        unsafe {
            let alloc_ref = &mut *tl_alloc.get();
            alloc_ref.alloc() as *mut T
        }
    }

    pub fn free(&self, addr: *mut T) {
        let tl_alloc = self.tl_alloc().expect("Cannot free for no tl");
        unsafe {
            let alloc_ref = &mut *tl_alloc.get();
            alloc_ref.free(addr as usize)
        }
    }

    pub fn pin(&self) -> AllocGuard<T, B> {
        let tl_alloc = self.tl_alloc().expect("Cannot pin for no tl");
        unsafe {
            let alloc_ref = &mut *tl_alloc.get();
            alloc_ref.guard_count += 1;
            AllocGuard {
                alloc: tl_alloc.get(),
            }
        }
    }

    #[inline(always)]
    fn tl_alloc(&self) -> Option<&TLAlloc<T, B>> {
        self.thread
            .get_or(|| TLAlloc::new(0, 0, self.shared.clone()))
    }
}

impl<T, const B: usize> SharedAlloc<T, B> {
    const OBJ_SIZE: usize = mem::size_of::<T>();
    const BUMP_SIZE: usize = 4096 * Self::OBJ_SIZE;

    fn new() -> Self {
        Self {
            free_obj: LinkedRingBufferStack::new(),
            free_buffer: LinkedRingBufferStack::new(),
            all_buffers: LinkedRingBufferStack::new(),
            _marker: PhantomData,
        }
    }

    fn alloc_buffer(&self) -> (usize, usize) {
        if let Some(pair) = self.free_buffer.pop() {
            return pair;
        }
        let ptr = unsafe { libc::malloc(Self::BUMP_SIZE) } as usize;
        self.all_buffers.push(ptr);
        (ptr, ptr + Self::BUMP_SIZE)
    }

    fn free_objs<'a>(&self) -> Option<Arc<RingBufferNode<usize, B>>> {
        self.free_obj.pop_buffer()
    }
}

impl<T, const B: usize> TLAllocInner<T, B> {
    const OBJ_SIZE: usize = mem::size_of::<T>() as usize;

    pub fn new(buffer: usize, limit: usize, shared: Arc<SharedAlloc<T, B>>) -> Self {
        Self {
            free_list: TLBufferedStack::new(),
            buffer,
            buffer_limit: limit,
            shared,
            guard_count: 0,
            defer_free: Vec::with_capacity(64),
            _marker: PhantomData,
        }
    }

    pub fn alloc(&mut self) -> usize {
        if let Some(addr) = self.free_list.pop() {
            return addr;
        }
        if self.buffer + Self::OBJ_SIZE > self.buffer_limit {
            // Allocate new buffer
            if let Some(free_buffer) = self.shared.free_objs() {
                if let Some(ptr) = free_buffer.buffer.pop_front() {
                    debug_assert_eq!(self.free_list.num_buffer, 0);
                    free_buffer.next.store_ref(Arc::null());
                    self.free_list.head = free_buffer;
                    self.free_list.num_buffer += 1;
                    return ptr;
                }
            }
            let (new_buffer, new_limit) = self.shared.alloc_buffer();
            self.buffer = new_buffer;
            self.buffer_limit = new_limit;
        }
        let obj_addr = self.buffer;
        self.buffer += Self::OBJ_SIZE;
        debug_assert!(self.buffer <= self.buffer_limit);
        return obj_addr;
    }

    pub fn free(&mut self, addr: usize) {
        if cfg!(test) && addr > self.buffer_limit {
            warn!("Freeing address out of limit: {}", addr);
        }
        if let Some(overflow_buffer) = self.free_list.push(addr) {
            self.shared.free_obj.attach_buffer(overflow_buffer);
        }
    }

    pub fn return_resources(&mut self) {
        // Return buffer space
        if self.buffer != self.buffer_limit {
            self.shared
                .free_buffer
                .push((self.buffer, self.buffer_limit))
        }

        // return free list
        let local_free = &mut self.free_list;
        let head = &local_free.head;
        if !head.is_null() {
            self.shared.free_obj.attach_buffer(head.clone());
            let mut next = head.next.load();
            while !next.is_null() {
                let next_next = next.next.load();
                if next.buffer.count() > 0 {
                    self.shared.free_obj.attach_buffer(next);
                }
                next = next_next;
            }
        }
    }
}

unsafe impl<T, const B: usize> Send for TLAllocInner<T, B> {}

impl<T, const B: usize> Drop for TLAllocInner<T, B> {
    fn drop(&mut self) {
        self.return_resources();
    }
}

struct TLBufferedStack<T, const B: usize> {
    head: Arc<RingBufferNode<T, B>>,
    num_buffer: usize,
}

impl<T: Clone + Default, const B: usize> TLBufferedStack<T, B> {
    const MAX_BUFFERS: usize = 32;
    pub fn new() -> Self {
        Self {
            head: Arc::null(),
            num_buffer: 0,
        }
    }

    pub fn push(&mut self, val: T) -> Option<Arc<RingBufferNode<T, B>>> {
        let mut res = None;
        if self.head.is_null() {
            self.head = Arc::new(RingBufferNode {
                buffer: RingBuffer::new(),
                next: AtomicArc::null(),
            });
        }
        if let Err(val) = self.head.buffer.lite_push_back(val) {
            // Current buffer is full, need a new one
            if self.num_buffer >= Self::MAX_BUFFERS {
                let head_next = self.head.next.load();
                let overflow_buffer_node = mem::replace(&mut self.head, head_next);
                res = Some(overflow_buffer_node);
                self.num_buffer -= 1;
            }
            debug_assert!(!self.head.is_null());
            let new_buffer = RingBufferNode {
                buffer: RingBuffer::new(),
                next: AtomicArc::from_rc(self.head.clone()),
            };
            let _ = new_buffer.buffer.push_back(val);
            self.head = Arc::new(new_buffer);
            self.num_buffer += 1;
        }
        return res;
    }

    pub fn pop(&mut self) -> Option<T> {
        if self.head.is_null() {
            return None;
        }
        loop {
            let head_pop = self.head.buffer.lite_pop_back();
            if head_pop.is_some() {
                return head_pop;
            }
            // Need to pop from next buffer
            let next_buffer = self.head.next.load();
            if next_buffer.is_null() {
                return None;
            }
            mem::replace(&mut self.head, next_buffer);
            self.num_buffer -= 1;
        }
    }
}

pub struct AllocGuard<T, const B: usize> {
    alloc: *mut TLAllocInner<T, B>,
}

impl<'a, T, const B: usize> Drop for AllocGuard<T, B> {
    fn drop(&mut self) {
        let alloc = unsafe { &mut *self.alloc };
        alloc.guard_count -= 1;
        if alloc.guard_count == 0 {
            while let Some(ptr) = alloc.defer_free.pop() {
                alloc.free(ptr);
            }
        }
    }
}

impl<'a, T, const B: usize> AllocGuard<T, B> {
    pub fn defer_free(&self, ptr: *mut T) {
        let alloc = unsafe { &mut *self.alloc };
        alloc.defer_free.push(ptr as usize);
    }

    pub fn alloc(&self) -> *mut T {
        let alloc = unsafe { &mut *self.alloc };
        alloc.alloc() as *mut T
    }

    pub fn free(&self, addr: usize) {
        let alloc = unsafe { &mut *self.alloc };
        alloc.free(addr)
    }
}

impl<T, const B: usize> Drop for Allocator<T, B> {
    fn drop(&mut self) {
        unsafe {
            while let Some(b) = self.shared.all_buffers.pop_buffer() {
                while let Some(alloc_bufer) = b.buffer.lite_pop_back() {
                    libc::free(alloc_bufer as *mut libc::c_void);
                }
            }
        }
    }
}

impl<T, const B: usize> TLAlloc<T, B> {
    pub fn new(buffer: usize, limit: usize, shared: Arc<SharedAlloc<T, B>>) -> Self {
        Self {
            inner: UnsafeCell::new(TLAllocInner::new(buffer, limit, shared)),
        }
    }

    fn get(&self) -> *mut TLAllocInner<T, B> {
        self.inner.get()
    }
}

impl<T: Default, const N: usize> RingBuffer<T, N> {
    fn lite_push_back(&self, data: T) -> Result<(), T> {
        let pos = self.tail.load(Relaxed);
        if pos >= self.elements.len() {
            return Err(data);
        }
        self.elements[pos].set(data);
        self.tail.store(pos + 1, Relaxed);
        Ok(())
    }
    fn lite_pop_back(&self) -> Option<T> {
        let mut pos = self.tail.load(Relaxed);
        if pos == 0 {
            return None;
        }
        pos -= 1;
        let val = self.elements[pos].take();
        self.tail.store(pos, Relaxed);
        Some(val)
    }
}
