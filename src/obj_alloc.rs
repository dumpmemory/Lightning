use parking_lot::Mutex;

// A supposed to be fast and lock-free object allocator without size class
use crate::{
    aarc::{Arc, AtomicArc},
    thread_local::ThreadLocal,
};
use std::{marker::PhantomData, mem, collections::HashSet};

use crate::stack::LinkedRingBufferStack;

pub struct Allocator<T, const B: usize> {
    shared: Arc<SharedAlloc<T, B>>,
    thread: ThreadLocal<TLAlloc<T, B>>,
    #[cfg(debug_assertions)]
    asan: Arc<ASan>,
}

pub struct TLAlloc<T, const B: usize> {
    buffer_addr: usize,
    buffer_limit: usize,
    free_list: TLBufferedStack<B>,
    buffered_free: Arc<ThreadLocalPage<B>>,
    shared: Arc<SharedAlloc<T, B>>,
    guard_count: usize,
    defer_free: Vec<usize>,
    #[cfg(debug_assertions)]
    asan: Arc<ASan>,
    _marker: PhantomData<T>,
}

pub struct SharedAlloc<T, const B: usize> {
    free_obj: AtomicArc<ThreadLocalPage<B>>,
    free_buffer: LinkedRingBufferStack<(usize, usize), B>,
    all_buffers: LinkedRingBufferStack<usize, B>,
    _marker: PhantomData<T>,
}

impl<T, const B: usize> Allocator<T, B> {
    pub fn new() -> Self {
        Self {
            shared: Arc::new(SharedAlloc::new()),
            thread: ThreadLocal::new(),
            #[cfg(debug_assertions)]
            asan: Arc::new(ASan::new()),
        }
    }

    #[inline(always)]
    pub fn alloc(&self) -> *mut T {
        let tl_alloc = self.tl_alloc();
        tl_alloc.alloc() as *mut T
    }

    #[inline(always)]
    pub fn free(&self, ptr: *mut T) {
        let tl_alloc = self.tl_alloc();
        tl_alloc.free(ptr)
    }

    #[inline(always)]
    pub fn buffered_free(&self, ptr: *mut T) {
        let tl_alloc = self.tl_alloc();
        tl_alloc.buffered_free(ptr)
    }

    #[inline(always)]
    pub fn pin(&self) -> AllocGuard<T, B> {
        let tl_alloc = self.tl_alloc();
        tl_alloc.guard_count += 1;
        AllocGuard {
            alloc: tl_alloc as *const TLAlloc<T, B> as *mut TLAlloc<T, B>,
        }
    }

    #[inline(always)]
    fn tl_alloc(&self) -> &mut TLAlloc<T, B> {
        self.thread.get_or(|| TLAlloc::new(0, 0, &self))
    }
}

#[repr(align(8))]
pub struct Aligned<T>(T);

impl<T, const B: usize> SharedAlloc<T, B> {
    const OBJ_SIZE: usize = mem::size_of::<Aligned<T>>();
    const BUMP_SIZE: usize = 4096 * Self::OBJ_SIZE;

    fn new() -> Self {
        Self {
            free_obj: AtomicArc::new(ThreadLocalPage::new()),
            free_buffer: LinkedRingBufferStack::new(),
            all_buffers: LinkedRingBufferStack::new(),
            _marker: PhantomData,
        }
    }

    #[inline(always)]
    fn alloc_buffer(&self) -> (usize, usize) {
        if let Some(pair) = self.free_buffer.pop() {
            return pair;
        }
        let ptr = unsafe {
            let heap = libc::malloc(Self::BUMP_SIZE);
            libc::memset(heap, 0, Self::BUMP_SIZE);
            heap as usize
        };
        self.all_buffers.push(ptr);
        (ptr, ptr + Self::BUMP_SIZE)
    }

    #[inline(always)]
    fn free_objs(&self) -> Option<Arc<ThreadLocalPage<B>>> {
        let backoff = crossbeam_utils::Backoff::new();
        loop {
            let head = self.free_obj.load();
            if head.is_null() {
                return None;
            }
            let next = head.next.load();
            if self.free_obj.compare_exchange_is_ok(&head, &next) {
                head.next.store_ref(Arc::null());
                return Some(head);
            }
            backoff.spin();
        }
    }

    #[inline(always)]
    fn attach_objs(&self, objs: &Arc<ThreadLocalPage<B>>) {
        let backoff = crossbeam_utils::Backoff::new();
        loop {
            let head = self.free_obj.load();
            unsafe {
                objs.as_mut().next.store_ref(head.clone());
            }
            if self.free_obj.compare_exchange_is_ok(&head, objs) {
                return;
            }
            backoff.spin();
        }
    }
}

impl<T, const B: usize> TLAlloc<T, B> {
    const OBJ_SIZE: usize = mem::size_of::<Aligned<T>>();

    #[inline(always)]
    pub fn new<'a>(buffer: usize, limit: usize, alloc: &'a Allocator<T, B>) -> Self {
        Self {
            free_list: TLBufferedStack::new(),
            buffered_free: Arc::new(ThreadLocalPage::new()),
            buffer_addr: buffer,
            buffer_limit: limit,
            shared: alloc.shared.clone(),
            guard_count: 0,
            defer_free: Vec::with_capacity(64),
            #[cfg(debug_assertions)]
            asan: alloc.asan.clone(), 
            _marker: PhantomData,
        }
    }

    #[inline(always)]
    pub fn alloc(&mut self) -> usize {
        if let Some(addr) = self.free_list.pop() {
            #[cfg(debug_assertions)]
            self.asan.alloc(addr);
            return addr;
        }
        if self.buffer_addr + Self::OBJ_SIZE > self.buffer_limit {
            // Allocate new buffer
            unsafe {
                if let Some(free_buffer) = (&*self.shared).free_objs() {
                    let free_buffer_ref = free_buffer.as_mut();
                    if let Some(ptr) = free_buffer_ref.pop_back() {
                        free_buffer_ref.next = AtomicArc::null();
                        self.free_list.head = free_buffer;
                        self.free_list.num_buffer += 1;
                        #[cfg(debug_assertions)]
                        self.asan.alloc(ptr);
                        return ptr;
                    }
                }
                loop {
                    let (new_buffer, new_limit) = (&*self.shared).alloc_buffer();
                    if new_buffer + Self::OBJ_SIZE <= new_limit {
                        self.buffer_addr = new_buffer;
                        self.buffer_limit = new_limit;
                        break;
                    }
                }
            }
        }
        unsafe {
            if let Some(addr) = self.buffered_free.as_mut().pop_back() {
                #[cfg(debug_assertions)]
                self.asan.alloc(addr);
                return addr;
            }
        }
        let obj_addr = self.buffer_addr;
        self.buffer_addr += Self::OBJ_SIZE;
        debug_assert!(self.buffer_addr <= self.buffer_limit);
        #[cfg(debug_assertions)]
        self.asan.alloc(obj_addr);
        return obj_addr;
    }

    #[inline(always)]
    pub fn free(&mut self, ptr: *const T) {
        if let Some(overflow_buffer) = self.free_list.push(ptr as usize) {
            (&*self.shared).attach_objs(&overflow_buffer);
        }
        #[cfg(debug_assertions)]
        self.asan.free(ptr as usize);
    }

    #[inline(always)]
    pub fn buffered_free(&mut self, ptr: *const T) {
        unsafe {
            if let Err(overflowed_addr) = self.buffered_free.as_mut().push_back(ptr as usize) {
                // The buffer is full, moving current buffer to the free list and alloc a new bufferend free
                if let Some(overflow_buffer) = self.free_list.append_page(mem::replace(
                    &mut self.buffered_free,
                    Arc::new(ThreadLocalPage::new()),
                )) {
                    (&*self.shared).attach_objs(&overflow_buffer);
                }
                self.buffered_free
                    .as_mut()
                    .push_back(overflowed_addr)
                    .unwrap();
            }
        }
        #[cfg(debug_assertions)]
        self.asan.free(ptr as usize);
    }

    #[inline(always)]
    pub fn return_resources(&mut self) {
        unsafe {
            // Return buffer space
            if self.buffer_addr < self.buffer_limit {
                (&*self.shared)
                    .free_buffer
                    .push((self.buffer_addr, self.buffer_limit))
            }

            // return free list
            let head = mem::replace(&mut self.free_list.head, Arc::null());
            if !head.is_null() {
                if head.pos > 0 {
                    (&*self.shared).attach_objs(&head);
                }
                let mut next = mem::replace(&mut head.as_mut().next, AtomicArc::null()).into_arc();
                while !next.is_null() {
                    let next_next =
                        mem::replace(&mut next.as_mut().next, AtomicArc::null()).into_arc();
                    if next.pos > 0 {
                        (&*self.shared).attach_objs(&next);
                    }
                    next = next_next;
                }
            }
            if self.buffered_free.pos > 0 {
                (&*self.shared).attach_objs(&self.buffered_free);
            }
        }
    }
}

unsafe impl<T, const B: usize> Send for TLAlloc<T, B> {}

impl<T, const B: usize> Drop for TLAlloc<T, B> {
    #[inline(always)]
    fn drop(&mut self) {
        self.return_resources();
    }
}

struct TLBufferedStack<const B: usize> {
    head: Arc<ThreadLocalPage<B>>,
    num_buffer: usize,
}

impl<const B: usize> TLBufferedStack<B> {
    const MAX_BUFFERS: usize = 64;

    #[inline(always)]
    pub fn new() -> Self {
        Self {
            head: Arc::new(ThreadLocalPage::new()),
            num_buffer: 0,
        }
    }

    #[inline(always)]
    pub fn push(&mut self, val: usize) -> Option<Arc<ThreadLocalPage<B>>> {
        let mut res = None;
        unsafe {
            if let Err(val) = self.head.as_mut().push_back(val) {
                // Current buffer is full, need a new one
                if self.num_buffer >= Self::MAX_BUFFERS {
                    let head_next = {
                        let head_mut = self.head.as_mut();
                        mem::replace(&mut head_mut.next, AtomicArc::null()).into_arc()
                    };
                    let overflow_buffer_node = mem::replace(&mut self.head, head_next);
                    res = Some(overflow_buffer_node);
                    self.num_buffer -= 1;
                }
                debug_assert!(!self.head.is_null());
                let mut new_buffer = ThreadLocalPage::new();
                let _ = new_buffer.push_back(val);
                new_buffer.next = AtomicArc::from_rc(self.head.clone());
                self.head = Arc::new(new_buffer);
                self.num_buffer += 1;
            }
        }
        return res;
    }

    #[inline(always)]
    pub fn append_page(
        &mut self,
        page: Arc<ThreadLocalPage<B>>,
    ) -> Option<Arc<ThreadLocalPage<B>>> {
        unsafe {
            let head_mut = self.head.as_mut();
            if self.num_buffer >= Self::MAX_BUFFERS {
                let head_next = head_mut.next.as_mut();
                let head_next_next = mem::replace(&mut head_next.next, AtomicArc::null());
                page.as_mut().next = head_next_next;
                Some(mem::replace(&mut head_mut.next, AtomicArc::from_rc(page)).into_arc())
            } else {
                let head_next = mem::replace(&mut head_mut.next, AtomicArc::null());
                page.as_mut().next = head_next;
                head_mut.next = AtomicArc::from_rc(page);
                self.num_buffer += 1;
                None
            }
        }
    }

    #[inline(always)]
    pub fn pop(&mut self) -> Option<usize> {
        loop {
            let head_pop = unsafe { self.head.as_mut().pop_back() };
            if head_pop.is_some() {
                return head_pop;
            }
            // Need to pop from next buffer
            unsafe {
                if self.head.next.inner().is_null() {
                    return None;
                }
                let next_node =
                    mem::replace(&mut self.head.as_mut().next, AtomicArc::null()).into_arc();
                self.head = next_node;
                self.num_buffer -= 1;
            }
        }
    }
}

pub struct AllocGuard<T, const B: usize> {
    alloc: *mut TLAlloc<T, B>,
}

impl<'a, T, const B: usize> Drop for AllocGuard<T, B> {
    #[inline(always)]
    fn drop(&mut self) {
        let alloc = unsafe { &mut *self.alloc };
        alloc.guard_count -= 1;
        if alloc.guard_count == 0 {
            while let Some(ptr) = alloc.defer_free.pop() {
                alloc.free(ptr as *const T);
            }
        }
    }
}

impl<'a, T, const B: usize> AllocGuard<T, B> {
    #[inline(always)]
    pub fn defer_free(&self, ptr: *mut T) {
        let alloc = unsafe { &mut *self.alloc };
        alloc.defer_free.push(ptr as usize);
    }

    #[inline(always)]
    pub fn alloc(&self) -> *mut T {
        let alloc = unsafe { &mut *self.alloc };
        alloc.alloc() as *mut T
    }

    #[inline(always)]
    pub fn free(&self, ptr: *const T) {
        let alloc = unsafe { &mut *self.alloc };
        alloc.free(ptr)
    }

    #[inline(always)]
    pub fn buffered_free(&self, ptr: *const T) {
        let alloc = unsafe { &mut *self.alloc };
        alloc.buffered_free(ptr)
    }
}

impl<T, const B: usize> Drop for Allocator<T, B> {
    fn drop(&mut self) {
        unsafe {
            while let Some(b) = (&*self.shared).all_buffers.pop_buffer() {
                while let Some(alloc_bufer) = b.buffer.pop_back() {
                    libc::free(alloc_bufer as *mut libc::c_void);
                }
            }
        }
    }
}

struct ThreadLocalPage<const B: usize> {
    buffer: [usize; B],
    pos: usize,
    next: AtomicArc<Self>,
}

impl<const B: usize> ThreadLocalPage<B> {
    #[inline(always)]
    fn new() -> Self {
        Self {
            buffer: [0usize; B],
            pos: 0,
            next: AtomicArc::null(),
        }
    }

    #[inline(always)]
    fn push_back(&mut self, addr: usize) -> Result<(), usize> {
        if self.pos >= B {
            return Err(addr);
        }
        self.buffer[self.pos] = addr;
        self.pos += 1;
        Ok(())
    }

    #[inline(always)]
    fn pop_back(&mut self) -> Option<usize> {
        if self.pos == 0 {
            return None;
        }
        self.pos -= 1;
        Some(self.buffer[self.pos])
    }
}

unsafe impl <T, const B: usize> Send for  Allocator<T, B> {}

struct ASan {
    inner: Mutex<ASanInner>,
}

struct ASanInner {
    created: HashSet<usize>,
    allocated: HashSet<usize>,
    reclaimed: HashSet<usize>,
}

impl ASan {
    fn new() -> Self {
        Self {
            inner: Mutex::new(ASanInner {
                created: HashSet::new(),
                allocated: HashSet::new(),
                reclaimed: HashSet::new(),
            })
        }
    }
    fn alloc(&self, addr: usize) {
        let mut inner = self.inner.lock();
        if !inner.created.contains(&addr) {
            // address never allocated
            assert!(!inner.allocated.contains(&addr), "address {}", addr);
            assert!(!inner.reclaimed.contains(&addr), "address {}", addr);
            inner.created.insert(addr);
            inner.allocated.insert(addr);
            return;
        } else if inner.reclaimed.contains(&addr) {
            assert!(inner.created.contains(&addr), "address {}", addr);
            assert!(!inner.allocated.contains(&addr), "address {}", addr);
            inner.reclaimed.remove(&addr);
            inner.allocated.insert(addr);
            return;
        }
        let create = inner.created.contains(&addr);
        let allocated = inner.allocated.contains(&addr);
        let reclaimed = inner.reclaimed.contains(&addr);
        panic!("Invalid alloc state: created {}, allocated {}, reclaimed {} address {}", create, allocated, reclaimed, addr);
    }

    fn free(&self, addr: usize) {
        let mut inner = self.inner.lock();
        assert!(inner.created.contains(&addr), "address {}", addr);
        assert!(inner.allocated.contains(&addr), "address {}", addr);
        assert!(!inner.reclaimed.contains(&addr), "address {}", addr);
        inner.allocated.remove(&addr);
        inner.reclaimed.insert(addr);
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashSet, thread};

    use super::*;

    const BUFFER_SIZE: usize = 4;

    #[test]
    fn recycle_thread_local_free_alloc() {
        let shared_alloc = Allocator::new();
        let mut thread_local = TLAlloc::new(0, 0, &shared_alloc);
        let test_size = BUFFER_SIZE * 1024;
        for i in 0..test_size {
            thread_local.free(i as *const usize);
        }
        let mut reallocated = HashSet::new();
        for i in 0..test_size {
            let alloc_res = thread_local.alloc();
            assert!(alloc_res < test_size, "Reallocated {} at {}", alloc_res, i);
            assert!(!reallocated.contains(&alloc_res));
            reallocated.insert(alloc_res);
        }
        assert!(thread_local.alloc() > test_size);
    }

    #[test]
    fn recycle_thread_local_buffered_free_alloc() {
        let shared_alloc = Allocator::new();
        let mut thread_local = TLAlloc::new(0, 0, &shared_alloc);
        let test_size = BUFFER_SIZE * 1024;
        for i in 0..test_size {
            thread_local.buffered_free(i as *const usize);
        }
        let mut reallocated = HashSet::new();
        for i in 0..test_size {
            let alloc_res = thread_local.alloc();
            assert!(alloc_res < test_size, "Reallocated {} at {}", alloc_res, i);
            assert!(!reallocated.contains(&alloc_res));
            reallocated.insert(alloc_res);
        }
        assert!(thread_local.alloc() > test_size);
    }

    #[test]
    fn checking_thread_local_alloc() {
        let shared_alloc = Allocator::new();
        let mut thread_local = TLAlloc::new(0, 0, &shared_alloc);
        let test_size = BUFFER_SIZE * 1024;
        let mut allocated = HashSet::new();
        for _ in 0..test_size {
            let addr = thread_local.alloc();
            assert!(!allocated.contains(&addr));
            allocated.insert(addr);
        }
        assert_eq!(test_size, allocated.len());
        for addr in &allocated.clone() {
            thread_local.free(*addr as *const usize);
        }
        for _ in 0..test_size {
            let addr = thread_local.alloc();
            assert!(allocated.remove(&addr));
        }
        assert!(!allocated.contains(&thread_local.alloc()));
        assert!(allocated.is_empty());
    }

    #[test]
    fn checking_thread_local_alter_alloc() {
        let shared_alloc = Allocator::new();
        let mut thread_local = TLAlloc::new(0, 0, &shared_alloc);
        let test_size = BUFFER_SIZE * 1024;
        let mut allocated = HashSet::new();
        for _ in 0..test_size {
            let addr = thread_local.alloc();
            assert!(!allocated.contains(&addr));
            allocated.insert(addr);
        }
        let mut reallocated = HashSet::new();
        for addr in &allocated {
            let realloc_addr = thread_local.alloc();
            thread_local.free(*addr as *const usize);
            assert!(!reallocated.contains(&realloc_addr));
            reallocated.insert(realloc_addr);
        }
    }

    #[test]
    fn checking_multithread_alter_alloc() {
        let allocator = Arc::new(Allocator::<usize, BUFFER_SIZE>::new());
        let num_threads = num_cpus::get();
        let mut threads = vec![];
        for _ in 0..num_threads {
            let allocator = allocator.clone();
            threads.push(thread::spawn(move || {
                let test_size = BUFFER_SIZE * 10240;
                let mut allocated = HashSet::new();
                for _ in 0..test_size {
                    let addr = allocator.alloc();
                    assert!(!allocated.contains(&addr));
                    allocated.insert(addr);
                }
                let mut reallocated = HashSet::new();
                for addr in &allocated {
                    let realloc_addr = allocator.alloc();
                    allocator.free(*addr as *mut usize);
                    assert!(!reallocated.contains(&realloc_addr));
                    reallocated.insert(realloc_addr);
                }
            }))
        }
        threads.into_iter().for_each(|t| t.join().unwrap());
    }
}