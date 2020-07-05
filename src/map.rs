// usize to usize lock-free, wait free table
use crate::align_padding;
use alloc::vec::Vec;
use core::alloc::{GlobalAlloc, Layout};
use core::hash::Hasher;
use core::marker::PhantomData;
use core::ops::Deref;
use core::sync::atomic::Ordering::{Relaxed, SeqCst};
use core::sync::atomic::{fence, AtomicUsize};
use core::{intrinsics, mem, ptr};
use crossbeam_epoch::*;
use std::alloc::System;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::os::raw::c_void;
use std::ops::DerefMut;

pub struct EntryTemplate(usize, usize);

const EMPTY_KEY: usize = 0;
const SENTINEL_VALUE: usize = 1;

struct Value {
    raw: usize,
    parsed: ParsedValue,
}

enum ParsedValue {
    Val(usize),
    Prime(usize),
    Sentinel,
    Empty,
}

#[derive(Debug)]
enum ModResult<V> {
    Replaced(usize, V),
    Fail(usize, Option<V>),
    Sentinel,
    NotFound,
    Done(usize, Option<V>),
    TableFull(usize, Option<V>),
    Aborted
}

enum ModOp<V> {
    Insert(usize, V),
    UpsertFastVal(usize),
    AttemptInsert(usize, V),
    SwapFastVal(Box<dyn Fn(usize) -> Option<usize>>),
    Sentinel,
    Empty,
}

pub enum  InsertOp {
    Insert,
    UpsertFast,
}

enum ResizeResult {
    NoNeed,
    SwapFailed,
    ChunkChanged,
    Done,
}

enum SwapResult {
    Succeed(usize),
    NotFound,
    Failed,   
    Aborted,
}

pub struct Chunk<K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> {
    capacity: usize,
    base: usize,
    occu_limit: usize,
    occupation: AtomicUsize,
    empty_entries: AtomicUsize,
    total_size: usize,
    attachment: A,
    shadow: PhantomData<(K, V, ALLOC)>,
}

pub struct ChunkPtr<K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> {
    ptr: *mut Chunk<K, V, A, ALLOC>,
}

pub struct Table<K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default, H: Hasher + Default> {
    new_chunk: Atomic<ChunkPtr<K, V, A, ALLOC>>,
    chunk: Atomic<ChunkPtr<K, V, A, ALLOC>>,
    val_bit_mask: usize, // 0111111..
    inv_bit_mask: usize, // 1000000..
    mark: PhantomData<H>,
}

impl<
        K: Clone + Hash + Eq,
        V: Clone,
        A: Attachment<K, V>,
        ALLOC: GlobalAlloc + Default,
        H: Hasher + Default,
    > Table<K, V, A, ALLOC, H>
{
    pub fn with_capacity(cap: usize) -> Self {
        if !is_power_of_2(cap) {
            panic!("capacity is not power of 2");
        }
        // Each entry key value pair is 2 words
        // steal 1 bit in the MSB of value indicate Prime(1)
        let val_bit_mask = !0 << 1 >> 1;
        let chunk = Chunk::alloc_chunk(cap);
        Self {
            chunk: Atomic::new(ChunkPtr::new(chunk)),
            new_chunk: Atomic::null(),
            val_bit_mask,
            inv_bit_mask: !val_bit_mask,
            mark: PhantomData,
        }
    }

    pub fn new() -> Self {
        Self::with_capacity(64)
    }

    pub fn get(&self, key: &K, fkey: usize, read_attachment: bool) -> Option<(usize, Option<V>)> {
        let guard = crossbeam_epoch::pin();
        let mut chunk_ref = self.chunk.load(Relaxed, &guard);
        loop {
            debug_assert!(!chunk_ref.is_null());
            let chunk = unsafe { chunk_ref.deref() };
            let (val, idx) = self.get_from_chunk(&*chunk, key, fkey);
            match val.parsed {
                ParsedValue::Prime(val) | ParsedValue::Val(val) => {
                    return Some((
                        val,
                        if read_attachment {
                            Some(chunk.attachment.get(idx).1)
                        } else {
                            None
                        },
                    ))
                }
                ParsedValue::Sentinel => {
                    chunk_ref = self.new_chunk.load(Relaxed, &guard);
                    if chunk_ref.is_null() {
                        return None;
                    }
                }
                ParsedValue::Empty => return None,
            }
        }
    }

    pub fn insert(&self, op: InsertOp, key: &K, mut value: Option<V>, fkey: usize, fvalue: usize) -> Option<(usize, V)> {
        let backoff = crossbeam_utils::Backoff::new();
        let guard = crossbeam_epoch::pin();
        loop {
            trace!("Inserting key: {}, value: {}", fkey, fvalue);
            let chunk_ptr = self.chunk.load(Relaxed, &guard);
            let new_chunk_ptr = self.new_chunk.load(Relaxed, &guard);
            let copying = Self::is_copying(&chunk_ptr, &new_chunk_ptr);
            if !copying {
                match self.check_resize(chunk_ptr, &guard) {
                    ResizeResult::Done | ResizeResult::SwapFailed | ResizeResult::ChunkChanged => {
                        debug!("Retry insert due to resize");
                        backoff.spin();
                        continue;
                    }
                    ResizeResult::NoNeed => {}
                }
            }
            let chunk = unsafe { chunk_ptr.deref() };
            let new_chunk = unsafe { new_chunk_ptr.deref() };

            let modify_chunk = if copying { new_chunk } else { chunk };
            let mod_op = match op {
                InsertOp::Insert => ModOp::Insert(fvalue & self.val_bit_mask, value.unwrap()),
                InsertOp::UpsertFast => ModOp::UpsertFastVal(fvalue & self.val_bit_mask),
            };
            let value_insertion = self.modify_entry(
                &*modify_chunk,
                key,
                fkey,
                mod_op,
                &guard
            );
            let mut result = None;
            match value_insertion {
                ModResult::Done(_, _) => {
                    modify_chunk.occupation.fetch_add(1, Relaxed);
                }
                ModResult::Replaced(fv, v) => result = Some((fv, v)),
                ModResult::Fail(_, rvalue) => {
                    // If fail insertion then retry
                    warn!("Insertion failed, retry. Copying {}, cap {}, count {}, dump: {}, old {:?}, new {:?}",
                        copying,
                        modify_chunk.capacity,
                        modify_chunk.occupation.load(Relaxed),
                        self.dump(modify_chunk.base, modify_chunk.capacity),
                        chunk_ptr,
                        new_chunk_ptr
                    );
                    value = rvalue;
                    backoff.spin();
                    continue;
                }
                ModResult::TableFull(_, rvalue) => {
                    warn!(
                        "Insertion is too fast, copying {}, cap {}, count {}, dump: {}, old {:?}, new {:?}.",
                        copying,
                        modify_chunk.capacity,
                        modify_chunk.occupation.load(Relaxed),
                        self.dump(modify_chunk.base, modify_chunk.capacity),
                        chunk_ptr,
                        new_chunk_ptr
                    );
                    value = rvalue;
                    backoff.spin();
                    continue;
                }
                ModResult::Sentinel => {
                    debug!("Insert new and see sentinel, abort");
                    return None;
                }
                ModResult::NotFound => unreachable!("Not Found on insertion is impossible"),
                ModResult::Aborted => unreachable!("Should no abort")
            }
            if copying && chunk_ptr != new_chunk_ptr {
                fence(SeqCst);
                if self.chunk.load(Relaxed, &guard) == chunk_ptr {
                    self.modify_entry(chunk, key, fkey, ModOp::Sentinel, &guard);
                }
            }
            return result;
        }
    }

    fn is_copying(chunk_ptr: &Shared<ChunkPtr<K, V, A, ALLOC>>, new_chunk_ptr: &Shared<ChunkPtr<K, V, A, ALLOC>>) -> bool {
        !new_chunk_ptr.is_null() || chunk_ptr == new_chunk_ptr
    }

    fn swap<'a, F: Fn(usize) -> Option<usize> + 'static>(&self, fkey: usize, key: &K, func: F, guard: &'a Guard) -> SwapResult {
        let chunk_ptr = self.chunk.load(Relaxed, &guard);
        let new_chunk_ptr = self.new_chunk.load(Relaxed, &guard);
        let copying = Self::is_copying(&chunk_ptr, &new_chunk_ptr);
        let chunk = unsafe { chunk_ptr.deref() };
        let new_chunk = unsafe { new_chunk_ptr.deref() };
        let modify_chunk = if copying { new_chunk } else { chunk };
        match self.modify_entry(&modify_chunk, key, fkey, ModOp::SwapFastVal(Box::new(func)), guard) {
            ModResult::Replaced(v, _) => SwapResult::Succeed(v & self.val_bit_mask),
            ModResult::Aborted => SwapResult::Aborted,
            ModResult::Fail(_, _) => SwapResult::Failed,
            ModResult::NotFound => SwapResult::NotFound,
            _ => unreachable!()
        }
    }

    pub fn remove(&self, key: &K, fkey: usize) -> Option<(usize, V)> {
        let guard = crossbeam_epoch::pin();
        let new_chunk_ptr = self.new_chunk.load(Relaxed, &guard);
        let old_chunk_ptr = self.chunk.load(Relaxed, &guard);
        let copying = Self::is_copying(&old_chunk_ptr, &new_chunk_ptr);
        let new_chunk = unsafe { new_chunk_ptr.deref() };
        let old_chunk = unsafe { old_chunk_ptr.deref() };
        let modify_chunk = if copying { &new_chunk } else { &old_chunk };
        let res = self.modify_entry(&*modify_chunk, key, fkey, ModOp::Empty, &guard);
        let mut retr = None;
        match res {
            ModResult::Done(fvalue, Some(value)) | ModResult::Replaced(fvalue, value) => {
                retr = Some((fvalue, value));
                if copying && new_chunk_ptr != old_chunk_ptr {
                    fence(SeqCst);
                    self.modify_entry(&*old_chunk, key, fkey, ModOp::Sentinel, &guard);
                }
            }
            ModResult::Done(_, None) => unreachable!(),
            ModResult::NotFound => {
                let remove_from_old =
                    self.modify_entry(&*old_chunk, key, fkey, ModOp::Empty, &guard);
                match remove_from_old {
                    ModResult::Done(fvalue, Some(value)) | ModResult::Replaced(fvalue, value) => {
                        retr = Some((fvalue, value));
                    }
                    ModResult::Done(_, None) => unreachable!(),
                    _ => {}
                }
            }
            ModResult::TableFull(_, _) => unreachable!("TableFull on remove is not possible"),
            _ => {}
        };
        retr
    }

    fn get_from_chunk(
        &self,
        chunk: &Chunk<K, V, A, ALLOC>,
        key: &K,
        fkey: usize,
    ) -> (Value, usize) {
        assert_ne!(chunk as *const Chunk<K, V, A, ALLOC> as usize, 0);
        let mut idx = hash::<H>(fkey);
        let entry_size = mem::size_of::<EntryTemplate>();
        let cap = chunk.capacity;
        let base = chunk.base;
        let cap_mask = chunk.cap_mask();
        let mut counter = 0;
        while counter < cap {
            idx &= cap_mask;
            let addr = base + idx * entry_size;
            let k = self.get_fast_key(addr);
            if k == fkey && chunk.attachment.probe(idx, key) {
                let val_res = self.get_fast_value(addr);
                match val_res.parsed {
                    ParsedValue::Empty => {}
                    _ => return (val_res, idx),
                }
            }
            if k == EMPTY_KEY {
                return (Value::new(0, self), 0);
            }
            idx += 1; // reprobe
            counter += 1;
        }

        // not found
        return (Value::new(0, self), 0);
    }

    fn modify_entry<'a>(
        &self,
        chunk: &'a Chunk<K, V, A, ALLOC>,
        key: &K,
        fkey: usize,
        mut op: ModOp<V>,
        _guard: &'a Guard,
    ) -> ModResult<V> {
        let cap = chunk.capacity;
        let base = chunk.base;
        let mut idx = hash::<H>(fkey);
        let entry_size = mem::size_of::<EntryTemplate>();
        let mut count = 0;
        let cap_mask = chunk.cap_mask();
        let attempt_insertion = match &op {
            &ModOp::AttemptInsert(_, _) => true,
            _ => false,
        };
        while count <= cap {
            idx &= cap_mask;
            let addr = base + idx * entry_size;
            let k = self.get_fast_key(addr);
            if k == fkey && chunk.attachment.probe(idx, &key) {
                // Probing non-empty entry
                let val = self.get_fast_value(addr);
                match &val.parsed {
                    ParsedValue::Val(v) | ParsedValue::Prime(v) => {
                        match &op {
                            &ModOp::Sentinel => {
                                self.set_sentinel(addr);
                                let (_, value) = chunk.attachment.get(idx);
                                chunk.attachment.erase(idx);
                                return ModResult::Done(*v, Some(value));
                            }
                            &ModOp::Empty => {
                                if !self.set_tombstone(addr, val.raw) {
                                    // this insertion have conflict with others
                                    // other thread changed the value (empty)
                                    // should fail
                                    let (_, value) = chunk.attachment.get(idx);
                                    return ModResult::Fail(*v, Some(value));
                                } else {
                                    // we have put tombstone on the value, get the attachment and erase it
                                    let (_, value) = chunk.attachment.get(idx);
                                    chunk.attachment.erase(idx);
                                    chunk.empty_entries.fetch_add(1, Relaxed);
                                    return ModResult::Replaced(*v, value);
                                }
                            }
                            &ModOp::UpsertFastVal(ref fv) => {
                                if self.cas_value(addr, val.raw, *fv) {
                                    let (_, value) = chunk.attachment.get(idx);
                                    return ModResult::Replaced(*fv, value);
                                }
                            }
                            &ModOp::AttemptInsert(_, _) => {
                                // Attempting insert existed entry, skip
                                let (_, value) = chunk.attachment.get(idx);
                                return ModResult::Fail(*v, Some(value));
                            }
                            &ModOp::SwapFastVal(ref swap) => {
                                let val = self.get_fast_value(addr);
                                match &val.parsed {
                                    ParsedValue::Val(pval) => {
                                        let aval = chunk.attachment.get(idx).1;
                                        if let Some(v) = swap(val.raw) {
                                            if self.cas_value(addr, *pval, v) {
                                                // swap success
                                                return ModResult::Replaced(val.raw, aval);
                                            } else {
                                                return ModResult::Fail(*pval, Some(aval));
                                            }
                                        } else {
                                            return ModResult::Aborted
                                        }
                                    },
                                    _ => {
                                        return ModResult::Fail(val.raw, None);
                                    }
                                }
                            }
                            &ModOp::Insert(_, _) => {
                                // Insert with attachment should insert into new slot
                                // When duplicate key discovered, put tombstone
                                chunk.empty_entries.fetch_add(1, Relaxed);
                                self.set_tombstone(addr, val.raw);
                            }
                        }
                    }
                    ParsedValue::Empty => {
                        // found the key with empty value, shall do nothing and continue probing
                    }
                    ParsedValue::Sentinel => return ModResult::Sentinel, // should not reachable for insertion happens on new list
                }
            } else if k == EMPTY_KEY {
                match op {
                    ModOp::Insert(fval, val) | ModOp::AttemptInsert(fval, val) => {
                        trace!(
                            "Inserting entry key: {}, value: {}, raw: {:b}, addr: {}",
                            fkey,
                            fval & self.val_bit_mask,
                            fval,
                            addr
                        );
                        if self.cas_value(addr, 0, fval) {
                            // CAS value succeed, shall store key
                            chunk.attachment.set(idx, key.clone(), val);
                            unsafe { intrinsics::atomic_store_relaxed(addr as *mut usize, fkey) }
                            return ModResult::Done(addr, None);
                        } else {
                            op = if attempt_insertion {
                                ModOp::AttemptInsert(fval, val)
                            } else {
                                ModOp::Insert(fval, val)
                            }
                        }
                    }
                    ModOp::UpsertFastVal(fval) => {
                        trace!(
                            "Upserting entry key: {}, value: {}, raw: {:b}, addr: {}",
                            fkey,
                            fval & self.val_bit_mask,
                            fval,
                            addr
                        );
                        if self.cas_value(addr, 0, fval) {
                            unsafe { intrinsics::atomic_store_relaxed(addr as *mut usize, fkey) }
                            return ModResult::Done(addr, None);
                        } else {
                            op = ModOp::UpsertFastVal(fval);
                        }
                    }
                    ModOp::Sentinel => {
                        if self.cas_value(addr, 0, SENTINEL_VALUE) {
                            // CAS value succeed, shall store key
                            unsafe { intrinsics::atomic_store_relaxed(addr as *mut usize, fkey) }
                            return ModResult::Done(addr, None);
                        } else {
                            op = ModOp::Sentinel
                        }
                    }
                    ModOp::Empty => return ModResult::Fail(0, None),
                    ModOp::SwapFastVal(_) => return ModResult::NotFound
                };
            }
            idx += 1; // reprobe
            count += 1;
        }
        match op {
            ModOp::Insert(fv, v) | ModOp::AttemptInsert(fv, v) => ModResult::TableFull(fv, Some(v)),
            ModOp::UpsertFastVal(fv) => ModResult::TableFull(fv, None),
            _ => ModResult::NotFound,
        }
    }

    fn all_from_chunk(&self, chunk: &Chunk<K, V, A, ALLOC>) -> Vec<(usize, usize, K, V)> {
        let mut idx = 0;
        let entry_size = mem::size_of::<EntryTemplate>();
        let cap = chunk.capacity;
        let base = chunk.base;
        let mut counter = 0;
        let mut res = Vec::with_capacity(chunk.occupation.load(Relaxed));
        let cap_mask = chunk.cap_mask();
        while counter < cap {
            idx &= cap_mask;
            let addr = base + idx * entry_size;
            let k = self.get_fast_key(addr);
            if k != EMPTY_KEY {
                let val_res = self.get_fast_value(addr);
                match val_res.parsed {
                    ParsedValue::Val(v) | ParsedValue::Prime(v) => {
                        let (key, value) = chunk.attachment.get(idx);
                        res.push((k, v, key, value))
                    }
                    _ => {}
                }
            }
            idx += 1; // reprobe
            counter += 1;
        }
        return res;
    }

    fn entries(&self) -> Vec<(usize, usize, K, V)> {
        let guard = crossbeam_epoch::pin();
        let old_chunk_ref = self.chunk.load(Relaxed, &guard);
        let new_chunk_ref = self.new_chunk.load(Relaxed, &guard);
        let old_chunk = unsafe { old_chunk_ref.deref() };
        let new_chunk = unsafe { new_chunk_ref.deref() };
        let mut res = self.all_from_chunk(&*old_chunk);
        if old_chunk_ref != new_chunk_ref {
            res.append(&mut self.all_from_chunk(&*new_chunk));
        }
        return res;
    }

    #[inline(always)]
    fn get_fast_key(&self, entry_addr: usize) -> usize {
        debug_assert!(entry_addr > 0);
        unsafe { intrinsics::atomic_load_relaxed(entry_addr as *mut usize) }
    }

    #[inline(always)]
    fn get_fast_value(&self, entry_addr: usize) -> Value {
        debug_assert!(entry_addr > 0);
        let addr = entry_addr + mem::size_of::<usize>();
        let val = unsafe { intrinsics::atomic_load_relaxed(addr as *mut usize) };
        Value::new(val, self)
    }

    #[inline(always)]
    fn set_tombstone(&self, entry_addr: usize, original: usize) -> bool {
        debug_assert!(entry_addr > 0);
        self.cas_value(entry_addr, original, 0)
    }
    #[inline(always)]
    fn set_sentinel(&self, entry_addr: usize) {
        debug_assert!(entry_addr > 0);
        let addr = entry_addr + mem::size_of::<usize>();
        unsafe { intrinsics::atomic_store_relaxed(addr as *mut usize, SENTINEL_VALUE) }
    }
    #[inline(always)]
    fn cas_value(&self, entry_addr: usize, original: usize, value: usize) -> bool {
        debug_assert!(entry_addr > 0);
        let addr = entry_addr + mem::size_of::<usize>();
        unsafe {
            intrinsics::atomic_cxchg_relaxed(addr as *mut usize, original, value).0 == original
        }
    }

    /// Failed return old shared
    #[inline(always)]
    fn check_resize<'a>(
        &self,
        old_chunk_ptr: Shared<'a, ChunkPtr<K, V, A, ALLOC>>,
        guard: &crossbeam_epoch::Guard,
    ) -> ResizeResult {
        let old_chunk_ins = unsafe { old_chunk_ptr.deref() };
        let occupation = old_chunk_ins.occupation.load(Relaxed);
        let empty_entries = old_chunk_ins.empty_entries.load(Relaxed);
        let occu_limit = old_chunk_ins.occu_limit;
        if occupation <= occu_limit {
            return ResizeResult::NoNeed;
        }
        let old_cap = old_chunk_ins.capacity;
        let new_cap = if empty_entries > (old_cap >> 1) {
            // clear empty
            debug!("Clearing empty entries");
            old_cap
        } else {
            // resize
            debug!("Resizing");
            let mult = if old_cap < 2048 { 4 } else { 1 };
            old_cap << mult
        };
        // Swap in old chunk as placeholder for the lock
        if let Err(e) = self
            .new_chunk
            .compare_and_set(Shared::null(), old_chunk_ptr, SeqCst, guard)
        {
            // other thread have allocated new chunk and wins the competition, exit
            warn!("Conflict on swapping new chunk, expecting it is null: {:?}", e);
            return ResizeResult::SwapFailed;
        }
        if self.chunk.load(SeqCst, guard) != old_chunk_ptr {
            warn!("Give up on resize due to old chunk changed after lock obtained");
            self.new_chunk.store(Shared::null(), SeqCst);
            return ResizeResult::ChunkChanged;
        }
        let new_chunk_ptr =
            Owned::new(ChunkPtr::new(Chunk::alloc_chunk(new_cap))).into_shared(guard);
        self.new_chunk.store(new_chunk_ptr, SeqCst); // Stump becasue we have the lock already
        let new_chunk_ins = unsafe { new_chunk_ptr.deref() };
        // Migrate entries
        self.migrate_entries(old_chunk_ins, new_chunk_ins, guard);
        // Assertion check
        debug_assert_ne!(old_chunk_ins.ptr as usize, new_chunk_ins.base);
        debug_assert_ne!(old_chunk_ins.ptr, unsafe { new_chunk_ptr.deref().ptr });
        debug_assert!(!new_chunk_ptr.is_null());
        let swap_old = self
            .chunk
            .compare_and_set(old_chunk_ptr, new_chunk_ptr, SeqCst, guard);
        if let Err(e) = swap_old {
            // Should not happend, we cannot fix this
            panic!("Resize swap pointer failed: {:?}", e);
        }
        unsafe {
            guard.defer_destroy(old_chunk_ptr);
        }
        self.new_chunk.store(Shared::null(), SeqCst);
        // assert!(self
        //     .new_chunk
        //     .compare_and_set(new_chunk_ptr, Shared::null(), SeqCst, guard)
        //     .is_ok());
        ResizeResult::Done
    }

    fn migrate_entries(
        &self,
        old_chunk_ins: &Chunk<K, V, A, ALLOC>,
        new_chunk_ins: &Chunk<K, V, A, ALLOC>,
        guard: &crossbeam_epoch::Guard,
    ) -> usize {
        let mut old_address = old_chunk_ins.base as usize;
        let boundary = old_address + chunk_size_of(old_chunk_ins.capacity);
        let mut effective_copy = 0;
        let mut idx = 0;
        while old_address < boundary {
            // iterate the old chunk to extract entries that is NOT empty
            let fkey = self.get_fast_key(old_address);
            let fvalue = self.get_fast_value(old_address);
            if fkey != EMPTY_KEY
            // Empty entry, skip
            {
                // Reasoning value states
                match &fvalue.parsed {
                    ParsedValue::Val(v) => {
                        // Insert entry into new chunk, in case of failure, skip this entry
                        // Value should be primed
                        trace!("Moving key: {}, value: {}", fkey, v);
                        let primed_fval = fvalue.raw | self.inv_bit_mask;
                        let (key, value) = old_chunk_ins.attachment.get(idx);
                        let new_chunk_insertion = self.modify_entry(
                            &*new_chunk_ins,
                            &key,
                            fkey,
                            ModOp::AttemptInsert(primed_fval, value),
                            guard,
                        );
                        let inserted_addr = match new_chunk_insertion {
                            ModResult::Done(addr, _) => Some(addr), // continue procedure
                            ModResult::Fail(_, _) => None,
                            ModResult::Replaced(_, _) => {
                                unreachable!("Attempt insert does not replace anything");
                            }
                            ModResult::Sentinel => {
                                warn!("New chunk should not have sentinel");
                                None
                            }
                            ModResult::NotFound => unreachable!("Not found on resize"),
                            ModResult::TableFull(_, _) => unreachable!("Table full when resize"),
                            ModResult::Aborted => unreachable!("Should never abort")
                        };
                        if let Some(new_entry_addr) = inserted_addr {
                            fence(SeqCst);
                            // CAS to ensure sentinel into old chunk (spec)
                            // Use CAS for old threads may working on this one
                            if self.cas_value(old_address, fvalue.raw, SENTINEL_VALUE) {
                                // strip prime
                                let stripped = primed_fval & self.val_bit_mask;
                                debug_assert_ne!(stripped, SENTINEL_VALUE);
                                if self.cas_value(new_entry_addr, primed_fval, stripped) {
                                    trace!(
                                        "Effective copy key: {}, value {}, addr: {}",
                                        fkey,
                                        stripped,
                                        new_entry_addr
                                    );
                                    old_chunk_ins.attachment.erase(idx);
                                    effective_copy += 1;
                                }
                            } else {
                                continue; // retry this entry
                            }
                        }
                    }
                    ParsedValue::Prime(_) => {
                        // Should never have prime in old chunk
                        panic!("Prime in old chunk when resizing")
                    }
                    ParsedValue::Sentinel => {
                        // Sentinel, skip
                        // Sentinel in old chunk implies its new value have already in the new chunk
                        trace!("Skip copy sentinel");
                    }
                    ParsedValue::Empty => {
                        // Empty, skip
                        trace!("Skip copy empty, key: {}", fkey);
                    }
                }
            }
            old_address += entry_size();
            idx += 1;
        }
        // resize finished, make changes on the numbers
        debug!("Migrated {} entries to new chunk", effective_copy);
        new_chunk_ins.occupation.fetch_add(effective_copy, Relaxed);
        return effective_copy;
    }

    fn dump(&self, base: usize, cap: usize) -> &str {
        for i in 0..cap {
            let addr = base + i * entry_size();
            trace!(
                "{}\t-{}-{}\t",
                i,
                self.get_fast_key(addr),
                self.get_fast_value(addr).raw
            );
        }
        "DUMPED"
    }
}

impl Value {
    pub fn new<K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default, H: Hasher + Default>(
        val: usize,
        table: &Table<K, V, A, ALLOC, H>,
    ) -> Self {
        let res = {
            if val == 0 {
                ParsedValue::Empty
            } else {
                let actual_val = val & table.val_bit_mask;
                let flag = val & table.inv_bit_mask;
                if flag != 0 {
                    ParsedValue::Prime(actual_val)
                } else if actual_val == 1 {
                    ParsedValue::Sentinel
                } else {
                    ParsedValue::Val(actual_val)
                }
            }
        };
        Value {
            raw: val,
            parsed: res,
        }
    }
}

impl<K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> Chunk<K, V, A, ALLOC> {
    fn alloc_chunk(capacity: usize) -> *mut Self {
        let capacity = capacity;
        let self_size = mem::size_of::<Self>();
        let self_align = align_padding(self_size, 64);
        let self_size_aligned = self_size + self_align;
        let chunk_size = chunk_size_of(capacity);
        let attachment_heap = A::heap_size_of(capacity);
        let total_size = self_size_aligned + chunk_size + attachment_heap;
        let ptr = alloc_mem::<ALLOC>(total_size) as *mut Self;
        let addr = ptr as usize;
        let data_base = addr + self_size_aligned;
        let attachment_base = data_base + chunk_size;
        unsafe {
            ptr::write(
                ptr,
                Self {
                    base: data_base,
                    capacity,
                    occupation: AtomicUsize::new(0),
                    empty_entries: AtomicUsize::new(0),
                    occu_limit: occupation_limit(capacity),
                    total_size,
                    attachment: A::new(capacity, attachment_base, attachment_heap),
                    shadow: PhantomData,
                },
            )
        };
        ptr
    }

    unsafe fn gc(ptr: *mut Chunk<K, V, A, ALLOC>) {
        debug_assert_ne!(ptr as usize, 0);
        let chunk = &*ptr;
        chunk.attachment.dealloc();
        dealloc_mem::<ALLOC>(ptr as usize, chunk.total_size);
    }

    #[inline]
    fn cap_mask(&self) -> usize {
        self.capacity - 1
    }
}

impl<K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Clone
    for Table<K, V, A, ALLOC, H>
{
    fn clone(&self) -> Self {
        let mut new_table = Table {
            chunk: Default::default(),
            new_chunk: Default::default(),
            val_bit_mask: 0,
            inv_bit_mask: 0,
            mark: PhantomData,
        };
        let guard = crossbeam_epoch::pin();
        let old_chunk_ptr = self.chunk.load(Relaxed, &guard);
        let new_chunk_ptr = self.new_chunk.load(Relaxed, &guard);
        unsafe {
            // Hold references first so they won't get reclaimed
            let old_chunk = old_chunk_ptr.deref();
            let old_total_size = old_chunk.total_size;

            let cloned_old_ptr = alloc_mem::<ALLOC>(old_total_size) as *mut Chunk<K, V, A, ALLOC>;
            debug_assert_ne!(cloned_old_ptr as usize, 0);
            debug_assert_ne!(old_chunk.ptr as usize, 0);
            libc::memcpy(
                cloned_old_ptr as *mut c_void,
                old_chunk.ptr as *const c_void,
                old_total_size,
            );
            let cloned_old_ref = Owned::new(ChunkPtr::new(cloned_old_ptr));
            new_table.chunk.store(cloned_old_ref, Relaxed);

            if new_chunk_ptr != Shared::null() {
                let new_chunk = new_chunk_ptr.deref();
                let new_total_size = new_chunk.total_size;
                let cloned_new_ptr =
                    alloc_mem::<ALLOC>(new_total_size) as *mut Chunk<K, V, A, ALLOC>;
                libc::memcpy(
                    cloned_new_ptr as *mut c_void,
                    new_chunk.ptr as *const c_void,
                    new_total_size,
                );
                let cloned_new_ref = Owned::new(ChunkPtr::new(cloned_new_ptr));
                new_table.new_chunk.store(cloned_new_ref, Relaxed);
            } else {
                new_table.new_chunk.store(Shared::null(), Relaxed);
            }
        }
        new_table.val_bit_mask = self.val_bit_mask;
        new_table.inv_bit_mask = self.inv_bit_mask;
        new_table
    }
}

impl<K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Drop
    for Table<K, V, A, ALLOC, H>
{
    fn drop(&mut self) {
        let guard = crossbeam_epoch::pin();
        unsafe {
            guard.defer_destroy(self.chunk.load(Relaxed, &guard));
            let new_chunk_ptr = self.new_chunk.load(Relaxed, &guard);
            if new_chunk_ptr != Shared::null() {
                guard.defer_destroy(new_chunk_ptr);
            }
        }
    }
}

unsafe impl<K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> Send
    for ChunkPtr<K, V, A, ALLOC>
{
}
unsafe impl<K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> Sync
    for ChunkPtr<K, V, A, ALLOC>
{
}

impl<K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> Drop for ChunkPtr<K, V, A, ALLOC> {
    fn drop(&mut self) {
        debug_assert_ne!(self.ptr as usize, 0);

        unsafe {
            Chunk::gc(self.ptr);
        }
    }
}

impl<K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> Deref for ChunkPtr<K, V, A, ALLOC> {
    type Target = Chunk<K, V, A, ALLOC>;

    fn deref(&self) -> &Self::Target {
        debug_assert_ne!(self.ptr as usize, 0);
        unsafe { &*self.ptr }
    }
}

impl<K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> ChunkPtr<K, V, A, ALLOC> {
    fn new(ptr: *mut Chunk<K, V, A, ALLOC>) -> Self {
        debug_assert_ne!(ptr as usize, 0);
        Self { ptr }
    }
}

#[inline(always)]
fn is_power_of_2(x: usize) -> bool {
    (x != 0) && ((x & (x - 1)) == 0)
}

#[inline(always)]
fn occupation_limit(cap: usize) -> usize {
    (cap as f64 * 0.70f64) as usize
}

#[inline(always)]
fn entry_size() -> usize {
    mem::size_of::<EntryTemplate>()
}

#[inline(always)]
fn chunk_size_of(cap: usize) -> usize {
    cap * entry_size()
}

#[inline(always)]
pub fn hash<H: Hasher + Default>(num: usize) -> usize {
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

pub trait Attachment<K, V> {
    fn heap_size_of(cap: usize) -> usize;
    fn new(cap: usize, heap_ptr: usize, heap_size: usize) -> Self;
    fn get(&self, index: usize) -> (K, V);
    fn set(&self, index: usize, key: K, value: V);
    fn erase(&self, index: usize);
    fn dealloc(&self);
    fn probe(&self, index: usize, probe_key: &K) -> bool;
}

pub struct WordAttachment;

// this attachment basically do nothing and sized zero
impl Attachment<(), ()> for WordAttachment {
    fn heap_size_of(_cap: usize) -> usize {
        0
    }

    fn new(_cap: usize, _heap_ptr: usize, _heap_size: usize) -> Self {
        Self
    }

    #[inline(always)]
    fn get(&self, _index: usize) -> ((), ()) {
        ((), ())
    }

    #[inline(always)]
    fn set(&self, _index: usize, _key: (), _value: ()) {}

    #[inline(always)]
    fn erase(&self, _index: usize) {}

    #[inline(always)]
    fn dealloc(&self) {}

    #[inline(always)]
    fn probe(&self, _index: usize, _value: &()) -> bool {
        true
    }
}

pub type WordTable<H, ALLOC> = Table<(), (), WordAttachment, H, ALLOC>;

pub struct WordObjectAttachment<T, A: GlobalAlloc + Default> {
    obj_chunk: usize,
    obj_size: usize,
    shadow: PhantomData<(T, A)>,
}

impl<T: Clone, A: GlobalAlloc + Default> Attachment<(), T> for WordObjectAttachment<T, A> {
    fn heap_size_of(cap: usize) -> usize {
        let obj_size = mem::size_of::<T>();
        cap * obj_size
    }

    fn new(_cap: usize, heap_ptr: usize, _heap_size: usize) -> Self {
        Self {
            obj_chunk: heap_ptr,
            obj_size: mem::size_of::<T>(),
            shadow: PhantomData,
        }
    }

    #[inline(always)]
    fn get(&self, index: usize) -> ((), T) {
        let addr = self.addr_by_index(index);
        let v = unsafe { (*(addr as *mut T)).clone() };
        ((), v)
    }

    #[inline(always)]
    fn set(&self, index: usize, _key: (), val: T) {
        let addr = self.addr_by_index(index);
        unsafe { ptr::write(addr as *mut T, val) }
    }

    #[inline(always)]
    fn erase(&self, index: usize) {
        drop(self.addr_by_index(index) as *mut T)
    }

    #[inline(always)]
    fn dealloc(&self) {}

    fn probe(&self, _index: usize, _value: &()) -> bool {
        true
    }
}

pub type HashTable<K, V, ALLOC> =
    Table<K, V, HashKVAttachment<K, V, ALLOC>, ALLOC, PassthroughHasher>;

pub struct HashKVAttachment<K, V, A: GlobalAlloc + Default> {
    obj_chunk: usize,
    obj_size: usize,
    shadow: PhantomData<(K, V, A)>,
}

impl<K: Clone + Hash + Eq, V: Clone, A: GlobalAlloc + Default> Attachment<K, V>
    for HashKVAttachment<K, V, A>
{
    fn heap_size_of(cap: usize) -> usize {
        let obj_size = mem::size_of::<(K, V)>();
        cap * obj_size
    }

    fn new(_cap: usize, heap_ptr: usize, _heap_size: usize) -> Self {
        Self {
            obj_chunk: heap_ptr,
            obj_size: mem::size_of::<(K, V)>(),
            shadow: PhantomData,
        }
    }

    #[inline(always)]
    fn get(&self, index: usize) -> (K, V) {
        let addr = self.addr_by_index(index);
        unsafe { (*(addr as *mut (K, V))).clone() }
    }

    #[inline(always)]
    fn set(&self, index: usize, key: K, val: V) {
        let addr = self.addr_by_index(index);
        unsafe { ptr::write(addr as *mut (K, V), (key, val)) }
    }

    #[inline(always)]
    fn erase(&self, index: usize) {
        drop(self.addr_by_index(index) as *mut (K, V))
    }

    #[inline(always)]
    fn dealloc(&self) {}

    fn probe(&self, index: usize, key: &K) -> bool {
        let addr = self.addr_by_index(index);
        let pos_key = unsafe { &*(addr as *mut K) };
        pos_key == key
    }
}

impl<K: Clone + Hash + Eq, V: Clone, A: GlobalAlloc + Default> HashKVAttachment<K, V, A> {
    fn addr_by_index(&self, index: usize) -> usize {
        self.obj_chunk + index * self.obj_size
    }
}

pub struct HashMap<
    K: Clone + Hash + Eq,
    V: Clone,
    ALLOC: GlobalAlloc + Default = System,
    H: Hasher + Default = DefaultHasher,
> {
    table: HashTable<K, V, ALLOC>,
    shadow: PhantomData<H>,
}

impl<K: Clone + Hash + Eq, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Map<K, V>
    for HashMap<K, V, ALLOC, H>
{
    fn with_capacity(cap: usize) -> Self {
        Self {
            table: Table::with_capacity(cap),
            shadow: PhantomData,
        }
    }

    #[inline(always)]
    fn get(&self, key: &K) -> Option<V> {
        let hash = hash_key::<K, H>(key);
        self.table.get(key, hash, true).map(|v| v.1.unwrap())
    }

    #[inline(always)]
    fn insert(&self, key: &K, value: V) -> Option<V> {
        let hash = hash_key::<K, H>(&key);
        self.table.insert(InsertOp::Insert, key, Some(value), hash, !0).map(|(_, v)| v)
    }

    #[inline(always)]
    fn remove(&self, key: &K) -> Option<V> {
        let hash = hash_key::<K, H>(&key);
        self.table.remove(key, hash).map(|(_, v)| v)
    }

    #[inline(always)]
    fn entries(&self) -> Vec<(usize, V)> {
        self.table
            .entries()
            .into_iter()
            .map(|(_, k, _, v)| (k - NUM_KEY_FIX, v))
            .collect()
    }

    #[inline(always)]
    fn contains(&self, key: &K) -> bool {
        let hash = hash_key::<K, H>(&key);
        self.table.get(key, hash, false).is_some()
    }
}

impl<T, A: GlobalAlloc + Default> WordObjectAttachment<T, A> {
    fn addr_by_index(&self, index: usize) -> usize {
        self.obj_chunk + index * self.obj_size
    }
}

pub trait Map<K, V> {
    fn with_capacity(cap: usize) -> Self;
    fn get(&self, key: &K) -> Option<V>;
    fn insert(&self, key: &K, value: V) -> Option<V>;
    fn remove(&self, key: &K) -> Option<V>;
    fn entries(&self) -> Vec<(usize, V)>;
    fn contains(&self, key: &K) -> bool;
}

const NUM_KEY_FIX: usize = 5;

#[derive(Clone)]
pub struct ObjectMap<
    V: Clone,
    ALLOC: GlobalAlloc + Default = System,
    H: Hasher + Default = DefaultHasher,
> {
    table: Table<(), V, WordObjectAttachment<V, ALLOC>, ALLOC, H>,
}

impl<V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Map<usize, V>
    for ObjectMap<V, ALLOC, H>
{
    fn with_capacity(cap: usize) -> Self {
        Self {
            table: Table::with_capacity(cap),
        }
    }

    #[inline(always)]
    fn get(&self, key: &usize) -> Option<V> {
        self.table
            .get(&(), key + NUM_KEY_FIX, true)
            .map(|v| v.1.unwrap())
    }

    #[inline(always)]
    fn insert(&self, key: &usize, value: V) -> Option<V> {
        self.table
            .insert(InsertOp::Insert, &(), Some(value), key + NUM_KEY_FIX, !0)
            .map(|(_, v)| v)
    }

    #[inline(always)]
    fn remove(&self, key: &usize) -> Option<V> {
        self.table.remove(&(), key + NUM_KEY_FIX).map(|(_, v)| v)
    }

    #[inline(always)]
    fn entries(&self) -> Vec<(usize, V)> {
        self.table
            .entries()
            .into_iter()
            .map(|(_, k, _, v)| (k - NUM_KEY_FIX, v))
            .collect()
    }

    #[inline(always)]
    fn contains(&self, key: &usize) -> bool {
        self.table.get(&(), key + NUM_KEY_FIX, false).is_some()
    }
}

#[derive(Clone)]
pub struct WordMap<ALLOC: GlobalAlloc + Default = System, H: Hasher + Default = DefaultHasher> {
    table: WordTable<ALLOC, H>,
}

impl<ALLOC: GlobalAlloc + Default, H: Hasher + Default> Map<usize, usize> for WordMap<ALLOC, H> {
    fn with_capacity(cap: usize) -> Self {
        Self {
            table: Table::with_capacity(cap),
        }
    }

    #[inline(always)]
    fn get(&self, key: &usize) -> Option<usize> {
        self.table.get(&(), key + NUM_KEY_FIX, false).map(|v| v.0)
    }

    #[inline(always)]
    fn insert(&self, key: &usize, value: usize) -> Option<usize> {
        self.table
            .insert(InsertOp::UpsertFast, &(), None, key + NUM_KEY_FIX, value)
            .map(|(v, _)| v)
    }

    #[inline(always)]
    fn remove(&self, key: &usize) -> Option<usize> {
        self.table.remove(&(), key + NUM_KEY_FIX).map(|(v, _)| v)
    }
    fn entries(&self) -> Vec<(usize, usize)> {
        self.table
            .entries()
            .into_iter()
            .map(|(k, v, _, _)| (k - NUM_KEY_FIX, v))
            .collect()
    }

    #[inline(always)]
    fn contains(&self, key: &usize) -> bool {
        self.get(key).is_some()
    }
}

const WORD_MUTEX_DATA_BIT_MASK: usize = !0 << 2 >> 2;

pub struct WordMutexGuard<'a, ALLOC: GlobalAlloc + Default, H: Hasher + Default> {
    table: &'a WordTable<ALLOC, H>, 
    key: usize,
    value: usize
}

impl <'a, ALLOC: GlobalAlloc + Default, H: Hasher + Default> WordMutexGuard<'a, ALLOC, H> {
    pub fn new(table: &'a WordTable<ALLOC, H>, key: usize) -> Option<Self> {
         let lock_bit_mask = !WORD_MUTEX_DATA_BIT_MASK & table.val_bit_mask;
         let backoff = crossbeam_utils::Backoff::new();
         let guard = crossbeam_epoch::pin();
         let mut value = 0;
         loop {
             let swap_res = table.swap(
                 key, 
                 &(),
                 move |fast_value| {
                    if fast_value & lock_bit_mask > 0 {
                        // Locked, unchanged
                        None
                    } else {
                        // Obtain lock
                        Some(fast_value | lock_bit_mask)
                    }
                },
                &guard,
            );
            match swap_res {
                SwapResult::Succeed(val) => {
                    value = val & WORD_MUTEX_DATA_BIT_MASK;
                    break;
                },
                SwapResult::Failed => {
                    backoff.spin();
                    continue;
                }
                SwapResult::NotFound => {
                    return None;
                }
                SwapResult::Aborted => unreachable!()
            }
         }
         debug_assert_ne!(value, 0);
         Some(Self { table, key, value })
    }

    pub fn remove(self) -> usize {
        let res = self.table.remove(&(), self.key).unwrap().0;
        mem::forget(self);
        res
    }
}

impl <'a, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Deref for WordMutexGuard<'a, ALLOC, H> {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl <'a, ALLOC: GlobalAlloc + Default, H: Hasher + Default> DerefMut for WordMutexGuard<'a, ALLOC, H> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

impl <'a, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Drop for WordMutexGuard<'a, ALLOC, H> {
    fn drop(&mut self) {
        self.table.insert(InsertOp::UpsertFast, &(), None, self.key & WORD_MUTEX_DATA_BIT_MASK, self.value);
    }
}

impl<ALLOC: GlobalAlloc + Default, H: Hasher + Default>WordMap<ALLOC, H> {
    fn lock(&self, key: &usize) -> Option<WordMutexGuard<ALLOC, H>> {
        WordMutexGuard::new(&self.table, *key)
    }
}

#[inline(always)]
fn alloc_mem<A: GlobalAlloc + Default>(size: usize) -> usize {
    let align = 64;
    let layout = Layout::from_size_align(size, align).unwrap();
    let alloc = A::default();
    // must be all zeroed
    unsafe {
        let addr = alloc.alloc(layout) as usize;
        ptr::write_bytes(addr as *mut u8, 0, size);
        debug_assert_eq!(addr % 64, 0);
        addr
    }
}

#[inline(always)]
fn dealloc_mem<A: GlobalAlloc + Default + Default>(ptr: usize, size: usize) {
    let align = 64;
    let layout = Layout::from_size_align(size, align).unwrap();
    let alloc = A::default();
    unsafe { alloc.dealloc(ptr as *mut u8, layout) }
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

#[cfg(test)]
mod tests {
    use crate::map::*;
    use alloc::sync::Arc;
    use std::alloc::System;
    use std::collections::HashMap;
    use std::thread;
    use test::Bencher;

    #[test]
    fn will_not_overflow() {
        let _ = env_logger::try_init();
        let table = WordMap::<System>::with_capacity(16);
        for i in 50..60 {
            assert_eq!(table.insert(&i, i), None);
        }
        for i in 50..60 {
            assert_eq!(table.get(&i), Some(i));
        }
        for i in 50..60 {
            assert_eq!(table.remove(&i), Some(i));
        }
    }

    #[test]
    fn resize() {
        let _ = env_logger::try_init();
        let map = WordMap::<System>::with_capacity(16);
        for i in 5..2048 {
            map.insert(&i, i * 2);
        }
        for i in 5..2048 {
            match map.get(&i) {
                Some(r) => assert_eq!(r, i * 2),
                None => panic!("{}", i),
            }
        }
    }

    #[test]
    fn parallel_no_resize() {
        let _ = env_logger::try_init();
        let map = Arc::new(WordMap::<System>::with_capacity(65536));
        let mut threads = vec![];
        for i in 5..99 {
            map.insert(&i, i * 10);
        }
        for i in 100..900 {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                for j in 5..60 {
                    map.insert(&(i * 100 + j), i * j);
                }
            }));
        }
        for i in 5..9 {
            for j in 1..10 {
                map.remove(&(i * j));
            }
        }
        for thread in threads {
            let _ = thread.join();
        }
        for i in 100..900 {
            for j in 5..60 {
                assert_eq!(map.get(&(i * 100 + j)), Some(i * j))
            }
        }
        for i in 5..9 {
            for j in 1..10 {
                assert!(map.get(&(i * j)).is_none())
            }
        }
    }

    #[test]
    fn parallel_with_resize() {
        let _ = env_logger::try_init();
        let map = Arc::new(WordMap::<System>::with_capacity(32));
        let mut threads = vec![];
        for i in 5..24 {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                for j in 5..1000 {
                    map.insert(&(i + j * 100), i * j);
                }
            }));
        }
        for thread in threads {
            let _ = thread.join();
        }
        for i in 5..24 {
            for j in 5..1000 {
                let k = i + j * 100;
                match map.get(&k) {
                    Some(v) => assert_eq!(v, i * j),
                    None => panic!("Value should not be None for key: {}", k),
                }
            }
        }
    }

    #[test]
    fn parallel_hybrid() {
        let _ = env_logger::try_init();
        let map = Arc::new(WordMap::<System>::with_capacity(4));
        for i in 5..128 {
            map.insert(&i, i * 10);
        }
        let mut threads = vec![];
        for i in 256..265 {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                for j in 5..60 {
                    map.insert(&(i * 10 + j), 10);
                }
            }));
        }
        for i in 5..8 {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                for j in 5..8 {
                    map.remove(&(i * j));
                }
            }));
        }
        for thread in threads {
            let _ = thread.join();
        }
        for i in 256..265 {
            for j in 5..60 {
                assert_eq!(map.get(&(i * 10 + j)), Some(10))
            }
        }
    }

    #[derive(Copy, Clone)]
    struct Obj {
        a: usize,
        b: usize,
        c: usize,
        d: usize,
    }
    impl Obj {
        fn new(num: usize) -> Self {
            Obj {
                a: num,
                b: num + 1,
                c: num + 2,
                d: num + 3,
            }
        }
        fn validate(&self, num: usize) {
            assert_eq!(self.a, num);
            assert_eq!(self.b, num + 1);
            assert_eq!(self.c, num + 2);
            assert_eq!(self.d, num + 3);
        }
    }

    #[test]
    fn obj_map() {
        let _ = env_logger::try_init();
        let map = ObjectMap::<Obj>::with_capacity(16);
        for i in 5..2048 {
            map.insert(&i, Obj::new(i));
        }
        for i in 5..2048 {
            match map.get(&i) {
                Some(r) => r.validate(i),
                None => panic!("{}", i),
            }
        }
    }

    #[test]
    fn parallel_obj_hybrid() {
        let _ = env_logger::try_init();
        let map = Arc::new(ObjectMap::<Obj>::with_capacity(4));
        for i in 5..128 {
            map.insert(&i, Obj::new(i * 10));
        }
        let mut threads = vec![];
        for i in 256..265 {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                for j in 5..60 {
                    map.insert(&(i * 10 + j), Obj::new(10));
                }
            }));
        }
        for i in 5..8 {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                for j in 5..8 {
                    map.remove(&(i * j));
                }
            }));
        }
        for thread in threads {
            let _ = thread.join();
        }
        for i in 256..265 {
            for j in 5..60 {
                match map.get(&(i * 10 + j)) {
                    Some(r) => r.validate(10),
                    None => panic!("{}", i),
                }
            }
        }
    }

    #[test]
    fn parallel_hashmap_hybrid() {
        let _ = env_logger::try_init();
        let map = Arc::new(super::HashMap::<u32, Obj>::with_capacity(4));
        for i in 5..128u32 {
            map.insert(&i, Obj::new((i * 10) as usize));
        }
        let mut threads = vec![];
        for i in 256..265u32 {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                for j in 5..60u32 {
                    map.insert(&(i * 10 + j), Obj::new(10usize));
                }
            }));
        }
        for i in 5..8 {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                for j in 5..8 {
                    map.remove(&(i * j));
                }
            }));
        }
        for thread in threads {
            let _ = thread.join();
        }
        for i in 256..265 {
            for j in 5..60 {
                match map.get(&(i * 10 + j)) {
                    Some(r) => r.validate(10),
                    None => panic!("{}", i),
                }
            }
        }
    }

    #[bench]
    fn lfmap(b: &mut Bencher) {
        let _ = env_logger::try_init();
        let map = WordMap::<System>::with_capacity(1024);
        let mut i = 5;
        b.iter(|| {
            map.insert(&i, i);
            i += 1;
        });
    }

    #[bench]
    fn hashmap(b: &mut Bencher) {
        let _ = env_logger::try_init();
        let mut map = HashMap::new();
        let mut i = 5;
        b.iter(|| {
            map.insert(i, i);
            i += 1;
        });
    }

    #[bench]
    fn default_hasher(b: &mut Bencher) {
        let _ = env_logger::try_init();
        b.iter(|| {
            let mut hasher = DefaultHasher::default();
            hasher.write_u64(123);
            hasher.finish();
        });
    }
}
