use itertools::Itertools;
#[cfg(asan)]
use libc::c_void;
#[cfg(asan)]
use parking_lot::Mutex;
#[cfg(asan)]
use std::collections::HashSet;

// A supposed to be fast and lock-free object allocator without size class
use crate::{
    aarc::{Arc, AtomicArc},
    thread_local::ThreadLocal,
};
use std::{collections::VecDeque, marker::PhantomData, mem, ptr};

use crate::stack::LinkedRingBufferStack;

pub struct Allocator<T, const B: usize> {
    shared: Arc<SharedAlloc<T, B>>,
    thread: ThreadLocal<TLAlloc<T, B>>,
    #[cfg(debug_assertions)]
    #[cfg(asan)]
    asan: Arc<ASan>,
}

pub struct TLAlloc<T, const B: usize> {
    buffer_addr: usize,
    buffer_limit: usize,
    free_list: TLBufferedStack<B>,
    buffered_free: [Arc<ThreadLocalPage<B>>; 2],
    shared: Arc<SharedAlloc<T, B>>,
    defer_free: Vec<usize>,
    guard_count: usize,
    buffered_idx: u8,
    #[cfg(debug_assertions)]
    #[cfg(asan)]
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
            #[cfg(asan)]
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

    pub fn all_freed_ptr(&self) -> Vec<*mut T> {
        let mut res = vec![];
        let thread_locals = self.thread.all_threads();
        let threed_local_ptrs = thread_locals
            .into_iter()
            .map(|thread| {
                let mut thread_res = vec![];
                thread_res.append(&mut thread.buffered_free[0].all());
                thread_res.append(&mut thread.buffered_free[1].all());
                thread_res.append(&mut thread.defer_free.clone());
                thread_res.append(&mut thread.free_list.all());
                return thread_res;
            })
            .flatten()
            .collect_vec();
        let shared_ptrs = self.shared.all_shared_free();
        res.append(&mut threed_local_ptrs.into_iter().map(|p| p as _).collect_vec());
        res.append(&mut shared_ptrs.into_iter().map(|p| p as _).collect_vec());
        return res;
    }
}

const BUMP_NUM: usize = 16 * 1024;

#[repr(align(8))]
pub struct Aligned<T>(T);

impl<T, const B: usize> SharedAlloc<T, B> {
    const OBJ_SIZE: usize = mem::size_of::<Aligned<T>>();
    const BUMP_BYTES: usize = BUMP_NUM * Self::OBJ_SIZE;

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
            let heap = libc::malloc(Self::BUMP_BYTES);
            libc::memset(heap, 0, Self::BUMP_BYTES);
            heap as usize
        };
        self.all_buffers.push(ptr);
        (ptr, ptr + Self::BUMP_BYTES)
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
    fn attach_objs(&self, objs: Arc<ThreadLocalPage<B>>) {
        let backoff = crossbeam_utils::Backoff::new();
        loop {
            let head = self.free_obj.load();
            objs.next.store_ref(head.clone());
            if self.free_obj.compare_exchange_is_ok(&head, &objs) {
                return;
            }
            backoff.spin();
        }
    }

    pub fn all_shared_free(&self) -> Vec<usize> {
        let mut res = Vec::new();
        // return free list
        let head = &self.free_obj;
        if !head.is_null() {
            let head_ref = head.load();
            res.append(&mut head_ref.all());
            let mut next = head_ref.next.load();
            while !next.is_null() {
                res.append(&mut next.all());
                next = next.next.load();
            }
        }
        res.sort();
        res.dedup();
        return res;
    }
}

impl<T, const B: usize> Drop for SharedAlloc<T, B> {
    fn drop(&mut self) {
        let mut node = self.free_obj.load();
        while !node.is_null() {
            node = node.next.swap_ref(Arc::null())
        }
    }
}

impl<T, const B: usize> TLAlloc<T, B> {
    const OBJ_SIZE: usize = mem::size_of::<Aligned<T>>();

    #[inline(always)]
    pub fn new<'a>(buffer: usize, limit: usize, alloc: &'a Allocator<T, B>) -> Self {
        let buffered_free = [
            Arc::new(ThreadLocalPage::new()),
            Arc::new(ThreadLocalPage::new()),
        ];
        Self {
            free_list: TLBufferedStack::new(),
            buffered_free,
            buffer_addr: buffer,
            buffer_limit: limit,
            shared: alloc.shared.clone(),
            defer_free: Vec::with_capacity(64),
            guard_count: 0,
            buffered_idx: 0,
            #[cfg(debug_assertions)]
            #[cfg(asan)]
            asan: alloc.asan.clone(),
            _marker: PhantomData,
        }
    }

    #[inline(always)]
    pub fn alloc(&mut self) -> usize {
        if let Some(addr) = self.free_list.pop() {
            #[cfg(debug_assertions)]
            #[cfg(asan)]
            self.asan.alloc(addr);
            return addr;
        }
        unsafe {
            // Take an object from alternative buffered free
            let buffer_idx = ((self.buffered_idx + 1) & 1) as usize;
            if let Some(addr) = self.buffered_free[buffer_idx].as_mut().pop_back() {
                #[cfg(debug_assertions)]
                #[cfg(asan)]
                self.asan.alloc(addr);
                return addr;
            }
        }
        if self.buffer_addr + Self::OBJ_SIZE > self.buffer_limit {
            // Allocate new buffer
            unsafe {
                if let Some(free_buffer) = (&*self.shared).free_objs() {
                    let free_buffer_ref = free_buffer.as_mut();
                    if let Some(ptr) = free_buffer_ref.pop_back() {
                        free_buffer_ref.next = AtomicArc::null();
                        self.free_list.append_page(free_buffer);
                        #[cfg(debug_assertions)]
                        #[cfg(asan)]
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
        let obj_addr = self.buffer_addr;
        self.buffer_addr += Self::OBJ_SIZE;
        debug_assert!(self.buffer_addr <= self.buffer_limit);
        #[cfg(debug_assertions)]
        #[cfg(asan)]
        self.asan.alloc(obj_addr);
        return obj_addr;
    }

    #[inline(always)]
    pub fn free(&mut self, ptr: *const T) {
        if let Some(overflow_buffer) = self.free_list.push(ptr as usize) {
            (&*self.shared).attach_objs(overflow_buffer);
        }
        #[cfg(debug_assertions)]
        #[cfg(asan)]
        self.asan.free(ptr as usize);
        // unsafe { libc::free(ptr as *mut c_void) }
    }

    #[inline(always)]
    pub fn buffered_free(&mut self, ptr: *const T) {
        unsafe {
            let idx = self.buffered_idx as usize;
            if let Err(overflowed_addr) = self.buffered_free[idx].as_mut().push_back(ptr as usize) {
                // current buffer is full, moving to the other buffer
                let new_idx = (idx + 1) & 1;
                self.buffered_idx = new_idx as _;
                if let Err(overflowed_addr) = self.buffered_free[new_idx]
                    .as_mut()
                    .push_back(overflowed_addr)
                {
                    // The other buffer is full, moving the buffer to the free list and alloc a new bufferend free
                    if let Some(overflow_buffer) = self.free_list.append_page(mem::replace(
                        &mut self.buffered_free[new_idx],
                        Arc::new(ThreadLocalPage::new()),
                    )) {
                        (&*self.shared).attach_objs(overflow_buffer);
                    }
                    self.buffered_free[new_idx]
                        .as_mut()
                        .push_back(overflowed_addr)
                        .unwrap();
                } else {
                    return;
                }
            }
        }
        #[cfg(debug_assertions)]
        #[cfg(asan)]
        self.asan.free(ptr as usize);
    }

    #[inline(always)]
    pub fn return_resources(&mut self) {
        // Return buffer space
        if self.buffer_addr < self.buffer_limit {
            (&*self.shared)
                .free_buffer
                .push((self.buffer_addr, self.buffer_limit))
        }
        if self.buffered_free[0].pos > 0 {
            (&*self.shared).attach_objs(mem::replace(
                &mut self.buffered_free[0],
                Arc::new(ThreadLocalPage::new()),
            ));
        }
        if self.buffered_free[1].pos > 0 {
            (&*self.shared).attach_objs(mem::replace(
                &mut self.buffered_free[1],
                Arc::new(ThreadLocalPage::new()),
            ));
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
    deque: VecDeque<Arc<ThreadLocalPage<B>>>,
}

impl<const B: usize> TLBufferedStack<B> {
    const MAX_BUFFERS: usize = 64;

    #[inline(always)]
    pub fn new() -> Self {
        let mut deque = VecDeque::with_capacity(Self::MAX_BUFFERS);
        deque.push_front(Arc::new(ThreadLocalPage::new()));
        Self { deque }
    }

    fn head(&mut self) -> &Arc<ThreadLocalPage<B>> {
        self.deque.front().unwrap()
    }

    #[inline(always)]
    pub fn push(&mut self, val: usize) -> Option<Arc<ThreadLocalPage<B>>> {
        let mut res = None;
        unsafe {
            if let Err(val) = self.head().as_mut().push_back(val) {
                // Current buffer is full, need a new one
                if self.deque.len() >= Self::MAX_BUFFERS {
                    // Need to move current buffer from buffer chain to shared buffer chain
                    res = self.deque.pop_back();
                }
                let mut new_buffer = ThreadLocalPage::new();
                let _ = new_buffer.push_back(val);
                self.deque.push_front(Arc::new(new_buffer));
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
            if self.deque.len() >= Self::MAX_BUFFERS {
                let res = self.deque.pop_front();
                self.deque.push_back(page);
                return res;
            } else {
                self.deque.push_back(page);
                None
            }
        }
    }

    #[inline(always)]
    pub fn pop(&mut self) -> Option<usize> {
        loop {
            unsafe {
                if let Some(val) = self.head().as_mut().pop_back() {
                    return Some(val);
                } else {
                    // Last empty buffer, just return None
                    if self.deque.len() == 1 {
                        return None;
                    }
                    // Current head buffer is empty, pop the head buffer
                    self.deque.pop_front();
                    // And try again
                }
            }
        }
    }

    pub fn all(&self) -> Vec<usize> {
        self.deque
            .iter()
            .map(|buffer| buffer.all())
            .flatten()
            .sorted()
            .dedup()
            .collect_vec()
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

    fn all(&self) -> Vec<usize> {
        self.buffer[..self.pos].to_vec()
    }
}

unsafe impl<T, const B: usize> Send for Allocator<T, B> {}

#[cfg(asan)]
pub(crate) struct ASan {
    inner: Mutex<ASanInner>,
}

#[cfg(asan)]
pub(crate) struct ASanInner {
    created: HashSet<usize>,
    allocated: HashSet<usize>,
    reclaimed: HashSet<usize>,
}

#[cfg(asan)]
impl ASanInner {
    pub fn new() -> Self {
        ASanInner {
            created: HashSet::new(),
            allocated: HashSet::new(),
            reclaimed: HashSet::new(),
        }
    }

    pub fn alloc(&mut self, addr: usize) {
        if !self.created.contains(&addr) {
            // address never allocated
            assert!(!self.allocated.contains(&addr), "address {}", addr);
            assert!(!self.reclaimed.contains(&addr), "address {}", addr);
            self.created.insert(addr);
            self.allocated.insert(addr);
            return;
        } else if self.reclaimed.contains(&addr) {
            assert!(self.created.contains(&addr), "address {}", addr);
            assert!(!self.allocated.contains(&addr), "address {}", addr);
            self.reclaimed.remove(&addr);
            self.allocated.insert(addr);
            return;
        }
        let create = self.created.contains(&addr);
        let allocated = self.allocated.contains(&addr);
        let reclaimed = self.reclaimed.contains(&addr);
        panic!(
            "Invalid alloc state: created {}, allocated {}, reclaimed {} address {}",
            create, allocated, reclaimed, addr
        );
    }

    pub fn free(&mut self, addr: usize) {
        assert!(self.created.contains(&addr), "address {}", addr);
        assert!(self.allocated.contains(&addr), "address {}", addr);
        assert!(!self.reclaimed.contains(&addr), "address {}", addr);
        self.allocated.remove(&addr);
        self.reclaimed.insert(addr);
    }
}

#[cfg(asan)]
impl ASan {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(ASanInner::new()),
        }
    }

    #[cfg(asan)]
    fn alloc(&self, addr: usize) {
        let mut inner = self.inner.lock();
        inner.alloc(addr)
    }

    #[cfg(asan)]
    fn free(&self, addr: usize) {
        let mut inner = self.inner.lock();
        inner.free(addr)
    }

    #[cfg(not(asan))]
    fn alloc(&self, _addr: usize) {}

    #[cfg(not(asan))]
    fn free(&self, _addr: usize) {}
}

#[cfg(test)]
mod test {
    use crate::tests_misc::{assert_all_thread_passed, hook_panic};
    use crossbeam_channel::unbounded;
    use std::{collections::HashSet, thread, time::Duration};

    use super::*;

    const BUFFER_SIZE: usize = 4;

    #[test]
    #[cfg(not(asan))]
    fn recycle_thread_local_free_alloc() {
        let shared_alloc = Allocator::<_, BUFFER_SIZE>::new();
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
    fn checking_thread_local_alloc() {
        let shared_alloc = Allocator::<_, BUFFER_SIZE>::new();
        let mut thread_local = TLAlloc::new(0, 0, &shared_alloc);
        let test_size = 8 * BUMP_NUM;
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
        let shared_alloc = Allocator::<_, BUFFER_SIZE>::new();
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
        let num_threads = 128;
        let mut threads = vec![];
        for _ in 0..num_threads {
            let allocator = allocator.clone();
            threads.push(thread::spawn(move || {
                let test_size = BUFFER_SIZE * 10240;
                let allocator = allocator.pin();
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
        assert_all_thread_passed(threads);
    }

    #[test]
    fn allocator_no_blow_up() {
        let _ = env_logger::try_init();
        hook_panic();
        let (sender, receiver) = unbounded();
        let num_threads = 128;
        let test_load = 10240;
        let mut threads = vec![];
        let allocator = Arc::new(Allocator::<String, BUFFER_SIZE>::new());
        let mut senders = vec![sender];
        for _ in 1..num_threads {
            senders.push(senders[0].clone());
        }
        for (tid, sender) in senders.into_iter().enumerate() {
            let allocator = allocator.clone();
            let receiver = receiver.clone();
            threads.push(thread::spawn(move || {
                let allocator = allocator.pin();
                if tid | 1 == tid {
                    for _ in 0..test_load {
                        let ptr = allocator.alloc();
                        unsafe {
                            ptr::write(ptr, format!("{}", tid));
                        }
                        sender.send(ptr as usize).unwrap();
                    }
                } else {
                    while let Ok(addr) = receiver.recv_timeout(Duration::from_secs(5)) {
                        let ptr = addr as _;
                        unsafe {
                            drop(ptr::read(ptr));
                        }
                        allocator.free(ptr);
                    }
                }
                drop(sender);
            }));
        }
        assert_all_thread_passed(threads);
    }
}
