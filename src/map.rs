// usize to usize lock-free, wait free table
use crate::align_padding;
use alloc::vec::Vec;
use core::alloc::{GlobalAlloc, Layout};
use core::hash::Hasher;
use core::marker::PhantomData;
use core::ops::Deref;
use core::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release, SeqCst};
use core::sync::atomic::{fence, AtomicU64, AtomicUsize};
use core::{intrinsics, mem, ptr};
use crossbeam_epoch::*;
use std::alloc::System;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::ops::DerefMut;
use std::os::raw::c_void;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct EntryTemplate(usize, usize);

const EMPTY_KEY: usize = 0;
const EMPTY_VALUE: usize = 0;
const SENTINEL_VALUE: usize = 1;
const VAL_BIT_MASK: usize = !0 << 1 >> 1;
const INV_VAL_BIT_MASK: usize = !VAL_BIT_MASK;
const MUTEX_BIT_MASK: usize = !WORD_MUTEX_DATA_BIT_MASK & VAL_BIT_MASK;

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
    Replaced(usize, V, usize), // (origin fval, val, index)
    Existed(usize, V),
    Fail,
    Sentinel,
    NotFound,
    Done(usize, Option<V>),
    TableFull,
    Aborted,
}

enum ModOp<'a, V> {
    Insert(usize, &'a V),
    UpsertFastVal(usize),
    AttemptInsert(usize, &'a V),
    SwapFastVal(Box<dyn Fn(usize) -> Option<usize>>),
    Sentinel,
    Empty,
}

pub enum InsertOp {
    Insert,
    UpsertFast,
    TryInsert,
}

enum ResizeResult {
    NoNeed,
    SwapFailed,
    ChunkChanged,
    Done,
}

enum SwapResult<'a, K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> {
    Succeed(usize, usize, Shared<'a, ChunkPtr<K, V, A, ALLOC>>),
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
    count: AtomicUsize,
    epoch: AtomicUsize,
    timestamp: AtomicU64,
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
        let chunk = Chunk::alloc_chunk(cap);
        Self {
            chunk: Atomic::new(ChunkPtr::new(chunk)),
            new_chunk: Atomic::null(),
            count: AtomicUsize::new(0),
            epoch: AtomicUsize::new(0),
            timestamp: AtomicU64::new(timestamp()),
            mark: PhantomData,
        }
    }

    pub fn new() -> Self {
        Self::with_capacity(64)
    }

    pub fn get(&self, key: &K, fkey: usize, read_attachment: bool) -> Option<(usize, Option<V>)> {
        let guard = crossbeam_epoch::pin();
        let backoff = crossbeam_utils::Backoff::new();
        loop {
            let epoch = self.now_epoch();
            let chunk_ptr = self.chunk.load(Acquire, &guard);
            let new_chunk_ptr = self.new_chunk.load(Acquire, &guard);
            let copying = Self::is_copying(&chunk_ptr, &new_chunk_ptr);
            debug_assert!(!chunk_ptr.is_null());
            let get_from = |chunk_ptr: &Shared<ChunkPtr<K, V, A, ALLOC>>| {
                let chunk = unsafe { chunk_ptr.deref() };
                let (val, idx) = self.get_from_chunk(&*chunk, key, fkey);
                match val.parsed {
                    ParsedValue::Prime(val) | ParsedValue::Val(val) => Some((
                        val,
                        if read_attachment {
                            Some(chunk.attachment.get(idx).1)
                        } else {
                            None
                        },
                    )),
                    ParsedValue::Empty | ParsedValue::Sentinel => {
                        None
                    }
                }
            };
            let res = {
                let chunk_res = get_from(&chunk_ptr);
                if chunk_res.is_some() {
                    chunk_res
                } else if copying {
                    get_from(&new_chunk_ptr)
                } else {
                    chunk_res
                }
                
            };
            if self.expired_epoch(epoch) {
                backoff.spin();
                continue;
            }
            return res;
        }
    }

    pub fn insert(
        &self,
        op: InsertOp,
        key: &K,
        value: Option<V>,
        fkey: usize,
        fvalue: usize,
    ) -> Option<(usize, V)> {
        let backoff = crossbeam_utils::Backoff::new();
        let guard = crossbeam_epoch::pin();
        loop {
            let epoch = self.now_epoch();
            trace!("Inserting key: {}, value: {}", fkey, fvalue);
            let chunk_ptr = self.chunk.load(Acquire, &guard);
            let new_chunk_ptr = self.new_chunk.load(Acquire, &guard);
            let copying = Self::is_copying(&chunk_ptr, &new_chunk_ptr);
            if !copying {
                match self.check_migration(chunk_ptr, &guard) {
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
            let masked_value = fvalue & VAL_BIT_MASK;
            let mod_op = match op {
                InsertOp::Insert => ModOp::Insert(masked_value, value.as_ref().unwrap()),
                InsertOp::UpsertFast => ModOp::UpsertFastVal(masked_value),
                InsertOp::TryInsert => ModOp::AttemptInsert(masked_value, value.as_ref().unwrap()),
            };
            let value_insertion = self.modify_entry(&*modify_chunk, key, fkey, mod_op, &guard);
            let mut result = None;
            match value_insertion {
                ModResult::Done(_, _) => {
                    modify_chunk.occupation.fetch_add(1, Relaxed);
                    self.count.fetch_add(1, AcqRel);
                }
                ModResult::Replaced(fv, v, _) | ModResult::Existed(fv, v) => result = Some((fv, v)),
                ModResult::Fail => {
                    // If fail insertion then retry
                    warn!(
                        "Insertion failed, retry. Copying {}, cap {}, count {}, old {:?}, new {:?}",
                        copying,
                        modify_chunk.capacity,
                        modify_chunk.occupation.load(Relaxed),
                        chunk_ptr,
                        new_chunk_ptr
                    );
                    backoff.spin();
                    continue;
                }
                ModResult::TableFull => {
                    trace!(
                        "Insertion is too fast, copying {}, cap {}, count {}, old {:?}, new {:?}.",
                        copying,
                        modify_chunk.capacity,
                        modify_chunk.occupation.load(Relaxed),
                        chunk_ptr,
                        new_chunk_ptr
                    );
                    backoff.spin();
                    continue;
                }
                ModResult::Sentinel => {
                    trace!("Discovered sentinel on insertion table upon probing, retry");
                    backoff.spin();
                    continue;
                }
                ModResult::NotFound => unreachable!("Not Found on insertion is impossible"),
                ModResult::Aborted => unreachable!("Should no abort"),
            }
            if self.expired_epoch(epoch) {
                backoff.spin();
                continue;
            }
            if copying {
                self.modify_entry(chunk, key, fkey, ModOp::Sentinel, &guard);
            }
            return result;
        }
    }

    fn is_copying(
        chunk_ptr: &Shared<ChunkPtr<K, V, A, ALLOC>>,
        new_chunk_ptr: &Shared<ChunkPtr<K, V, A, ALLOC>>,
    ) -> bool {
        (!new_chunk_ptr.is_null()) && (chunk_ptr != new_chunk_ptr)
    }

    fn swap<'a, F: Fn(usize) -> Option<usize> + Copy + 'static>(
        &self,
        fkey: usize,
        key: &K,
        func: F,
        guard: &'a Guard,
    ) -> SwapResult<'a, K, V, A, ALLOC> {
        let backoff = crossbeam_utils::Backoff::new();
        loop {
            let epoch = self.now_epoch();
            let chunk_ptr = self.chunk.load(Acquire, &guard);
            let new_chunk_ptr = self.new_chunk.load(Acquire, &guard);
            let copying = Self::is_copying(&chunk_ptr, &new_chunk_ptr);
            let chunk = unsafe { chunk_ptr.deref() };
            let modify_chunk_ptr = if copying { new_chunk_ptr } else { chunk_ptr };
            let modify_chunk = unsafe { modify_chunk_ptr.deref() };
            trace!("Swaping for key {}, copying {}", fkey, copying);
            let mod_res = self.modify_entry(
                modify_chunk,
                key,
                fkey,
                ModOp::SwapFastVal(Box::new(func)),
                guard,
            );
            if self.expired_epoch(epoch) {
                backoff.spin();
                continue;
            }
            if copying {
                self.modify_entry(chunk, key, fkey, ModOp::Sentinel, &guard);
            }
            return match mod_res {
                ModResult::Replaced(v, _, idx) => {
                    SwapResult::Succeed(v & VAL_BIT_MASK, idx, modify_chunk_ptr)
                }
                ModResult::Aborted => SwapResult::Aborted,
                ModResult::Fail => SwapResult::Failed,
                ModResult::NotFound => SwapResult::NotFound,
                ModResult::Sentinel => {
                    backoff.spin();
                    continue;
                }
                ModResult::Existed(_, _) => unreachable!("Swap have existed result"),
                ModResult::Done(_, _) => unreachable!("Swap Done"),
                ModResult::TableFull => unreachable!("Swap table full"),
            };
        }
    }

    #[inline(always)]
    fn now_epoch(&self) -> usize {
        fence(SeqCst);
        self.epoch.load(Acquire)
    }

    fn expired_epoch(&self, old_epoch: usize) -> bool {
        let now = self.now_epoch();
        let expired = now - old_epoch > 1;
        if expired {
            debug!("Found expired epoch now: {}, old: {}", now, old_epoch);
        }
        expired
    }

    pub fn remove(&self, key: &K, fkey: usize) -> Option<(usize, V)> {
        let guard = crossbeam_epoch::pin();
        let backoff = crossbeam_utils::Backoff::new();
        loop {
            let epoch = self.now_epoch();
            let new_chunk_ptr = self.new_chunk.load(Acquire, &guard);
            let old_chunk_ptr = self.chunk.load(Acquire, &guard);
            if new_chunk_ptr == old_chunk_ptr {
                backoff.spin();
                continue;
            }
            let copying = Self::is_copying(&old_chunk_ptr, &new_chunk_ptr);
            let new_chunk = unsafe { new_chunk_ptr.deref() };
            let old_chunk = unsafe { old_chunk_ptr.deref() };
            let modify_chunk = if copying { &new_chunk } else { &old_chunk };
            let res = self.modify_entry(&*modify_chunk, key, fkey, ModOp::Empty, &guard);
            let mut retr = None;
            match res {
                ModResult::Done(fvalue, Some(value)) | ModResult::Replaced(fvalue, value, _) => {
                    retr = Some((fvalue, value));
                    self.count.fetch_sub(1, AcqRel);
                }
                ModResult::Done(_, None) => unreachable!("Remove shall not have done"),
                ModResult::NotFound => {}
                ModResult::Sentinel => {
                    backoff.spin();
                    continue;
                }
                ModResult::TableFull => unreachable!("TableFull on remove is not possible"),
                _ => {}
            };
            if self.expired_epoch(epoch) {
                if retr.is_none() {
                    return self.remove(key, fkey);
                } else {
                    let re_retr = self.remove(key, fkey);
                    return if re_retr.is_some() { re_retr } else { retr };
                }
            }
            if copying {
                trace!("Put sentinel in old chunk for removal");
                let remove_from_old =
                    self.modify_entry(&*old_chunk, key, fkey, ModOp::Sentinel, &guard);
                match remove_from_old {
                    ModResult::Done(fvalue, Some(value))
                    | ModResult::Replaced(fvalue, value, _) => {
                        trace!("Sentinal placed");
                        retr = Some((fvalue, value));
                    }
                    ModResult::Done(_, None) => {}
                    _ => {
                        trace!("Sentinal not placed");
                    }
                }
            }
            return retr;
        }
    }

    pub fn len(&self) -> usize {
        self.count.load(Acquire)
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
                return (Value::new::<K, V, A, ALLOC, H>(0), 0);
            }
            idx += 1; // reprobe
            counter += 1;
        }

        // not found
        return (Value::new::<K, V, A, ALLOC, H>(0), 0);
    }

    fn modify_entry<'a>(
        &self,
        chunk: &'a Chunk<K, V, A, ALLOC>,
        key: &K,
        fkey: usize,
        op: ModOp<V>,
        _guard: &'a Guard,
    ) -> ModResult<V> {
        let cap = chunk.capacity;
        let base = chunk.base;
        let mut idx = hash::<H>(fkey);
        let entry_size = mem::size_of::<EntryTemplate>();
        let mut count = 0;
        let cap_mask = chunk.cap_mask();
        let backoff = crossbeam_utils::Backoff::new();
        while count <= cap {
            idx &= cap_mask;
            let addr = base + idx * entry_size;
            let k = self.get_fast_key(addr);
            let v = self.get_fast_value(addr);
            {
                // Early exit upon sentinel discovery
                match v.parsed {
                    ParsedValue::Sentinel => match &op {
                        &ModOp::Sentinel => {
                            // Sentinel op is allowed on old chunk
                        }
                        _ => return ModResult::Sentinel,
                    },
                    _ => {}
                }
            }
            if k == fkey && chunk.attachment.probe(idx, &key) {
                // Probing non-empty entry
                let val = v;
                match &val.parsed {
                    ParsedValue::Val(v) => {
                        match &op {
                            &ModOp::Sentinel => {
                                if self.cas_sentinel(addr, val.raw) {
                                    let (_, value) = chunk.attachment.get(idx);
                                    chunk.attachment.erase(idx);
                                    return ModResult::Done(*v, Some(value));
                                } else {
                                    return ModResult::Fail;
                                }
                            }
                            &ModOp::Empty => {
                                if !self.cas_tombstone(addr, val.raw) {
                                    // this insertion have conflict with others
                                    // other thread changed the value (empty)
                                    // should fail
                                    return ModResult::Fail;
                                } else {
                                    // we have put tombstone on the value, get the attachment and erase it
                                    let (_, value) = chunk.attachment.get(idx);
                                    chunk.attachment.erase(idx);
                                    chunk.empty_entries.fetch_add(1, Relaxed);
                                    return ModResult::Replaced(*v, value, idx);
                                }
                            }
                            &ModOp::UpsertFastVal(ref fv) => {
                                if self.cas_value(addr, val.raw, *fv) {
                                    let (_, value) = chunk.attachment.get(idx);
                                    return ModResult::Replaced(*fv, value, idx);
                                } else {
                                    trace!("Cannot upsert fast value in place for {}", fkey);
                                    return ModResult::Fail;
                                }
                            }
                            &ModOp::AttemptInsert(iv, _) => {
                                trace!(
                                    "Attempting insert existed entry {}, {}, have key {}, skip",
                                    k,
                                    iv,
                                    v
                                );
                                let (_, value) = chunk.attachment.get(idx);
                                return ModResult::Existed(*v, value);
                            }
                            &ModOp::SwapFastVal(ref swap) => {
                                trace!(
                                    "Swaping found key {} have original value {:#064b}",
                                    fkey,
                                    val.raw
                                );
                                match &val.parsed {
                                    ParsedValue::Val(pval) => {
                                        let aval = chunk.attachment.get(idx).1;
                                        if let Some(v) = swap(*pval) {
                                            if self.cas_value(addr, *pval, v) {
                                                // swap success
                                                return ModResult::Replaced(val.raw, aval, idx);
                                            } else {
                                                return ModResult::Fail;
                                            }
                                        } else {
                                            return ModResult::Aborted;
                                        }
                                    }
                                    _ => {
                                        return ModResult::Fail;
                                    }
                                }
                            }
                            &ModOp::Insert(fval, ref v) => {
                                // Insert with attachment should prime value first when
                                // duplicate key discovered
                                debug!("Inserting in place for {}", fkey);
                                let primed_fval = fval | INV_VAL_BIT_MASK;
                                if self.cas_value(addr, val.raw, primed_fval) {
                                    let (_, prev_val) = chunk.attachment.get(idx);
                                    chunk.attachment.set(idx, key.clone(), (*v).clone());
                                    let stripped_prime = self.cas_value(addr, primed_fval, fval);
                                    debug_assert!(stripped_prime);
                                    return ModResult::Replaced(val.raw, prev_val, idx);
                                } else {
                                    trace!("Cannot insert in place for {}", fkey);
                                    return ModResult::Fail;
                                }
                            }
                        }
                    }
                    ParsedValue::Empty => {
                        // found the key with empty value, shall do nothing and continue probing
                        // because other thread is trying to write value into it
                    }
                    ParsedValue::Sentinel => return ModResult::Sentinel,
                    ParsedValue::Prime(v) => {
                        trace!(
                            "Discovered prime for key {} with value {:#064b}, retry",
                            fkey,
                            v
                        );
                        backoff.spin();
                        continue;
                    }
                }
            } else if k == EMPTY_KEY {
                match op {
                    ModOp::Insert(fval, val) | ModOp::AttemptInsert(fval, val) => {
                        trace!(
                            "Inserting entry key: {}, value: {}, raw: {:b}, addr: {}",
                            fkey,
                            fval & VAL_BIT_MASK,
                            fval,
                            addr
                        );
                        if self.cas_value(addr, EMPTY_VALUE, fval) {
                            // CAS value succeed, shall store key
                            chunk.attachment.set(idx, key.clone(), (*val).clone());
                            unsafe { intrinsics::atomic_store_rel(addr as *mut usize, fkey) }
                            return ModResult::Done(addr, None);
                        } else {
                            backoff.spin();
                            continue;
                        }
                    }
                    ModOp::UpsertFastVal(fval) => {
                        trace!(
                            "Upserting entry key: {}, value: {}, raw: {:b}, addr: {}",
                            fkey,
                            fval & VAL_BIT_MASK,
                            fval,
                            addr
                        );
                        if self.cas_value(addr, EMPTY_VALUE, fval) {
                            unsafe { intrinsics::atomic_store_rel(addr as *mut usize, fkey) }
                            return ModResult::Done(addr, None);
                        } else {
                            backoff.spin();
                            continue;
                        }
                    }
                    ModOp::Sentinel => {
                        if self.cas_sentinel(addr, 0) {
                            // CAS value succeed, shall store key
                            unsafe { intrinsics::atomic_store_rel(addr as *mut usize, fkey) }
                            return ModResult::Done(addr, None);
                        } else {
                            backoff.spin();
                            continue;
                        }
                    }
                    ModOp::Empty => return ModResult::Fail,
                    ModOp::SwapFastVal(_) => return ModResult::NotFound,
                };
            }
            idx += 1; // reprobe
            count += 1;
        }
        match op {
            ModOp::Insert(_fv, _v) | ModOp::AttemptInsert(_fv, _v) => ModResult::TableFull,
            ModOp::UpsertFastVal(_fv) => ModResult::TableFull,
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
        let old_chunk_ref = self.chunk.load(Acquire, &guard);
        let new_chunk_ref = self.new_chunk.load(Acquire, &guard);
        let old_chunk = unsafe { old_chunk_ref.deref() };
        let new_chunk = unsafe { new_chunk_ref.deref() };
        let mut res = self.all_from_chunk(&*old_chunk);
        if !new_chunk_ref.is_null() && old_chunk_ref != new_chunk_ref {
            res.append(&mut self.all_from_chunk(&*new_chunk));
        }
        return res;
    }

    #[inline(always)]
    fn get_fast_key(&self, entry_addr: usize) -> usize {
        debug_assert!(entry_addr > 0);
        unsafe { intrinsics::atomic_load_acq(entry_addr as *mut usize) }
    }

    #[inline(always)]
    fn get_fast_value(&self, entry_addr: usize) -> Value {
        debug_assert!(entry_addr > 0);
        let addr = entry_addr + mem::size_of::<usize>();
        let val = unsafe { intrinsics::atomic_load_acq(addr as *mut usize) };
        Value::new::<K, V, A, ALLOC, H>(val)
    }

    #[inline(always)]
    fn cas_tombstone(&self, entry_addr: usize, original: usize) -> bool {
        debug_assert!(entry_addr > 0);
        self.cas_value(entry_addr, original, EMPTY_VALUE)
    }
    #[inline(always)]
    fn cas_value(&self, entry_addr: usize, original: usize, value: usize) -> bool {
        debug_assert!(entry_addr > 0);
        let addr = entry_addr + mem::size_of::<usize>();
        unsafe {
            intrinsics::atomic_cxchg_acqrel(addr as *mut usize, original, value).0 == original
        }
    }
    #[inline(always)]
    fn cas_sentinel(&self, entry_addr: usize, original: usize) -> bool {
        debug_assert!(entry_addr > 0);
        let addr = entry_addr + mem::size_of::<usize>();
        fence(SeqCst);
        let (val, done) = unsafe {
            intrinsics::atomic_cxchg_acqrel(addr as *mut usize, original, SENTINEL_VALUE)
        };
        fence(SeqCst);
        done || val == SENTINEL_VALUE
    }

    /// Failed return old shared
    fn check_migration<'a>(
        &self,
        old_chunk_ptr: Shared<'a, ChunkPtr<K, V, A, ALLOC>>,
        guard: &crossbeam_epoch::Guard,
    ) -> ResizeResult {
        let old_chunk_ins = unsafe { old_chunk_ptr.deref() };
        let occupation = old_chunk_ins.occupation.load(Relaxed);
        let occu_limit = old_chunk_ins.occu_limit;
        if occupation <= occu_limit {
            return ResizeResult::NoNeed;
        }
        let epoch = self.now_epoch();
        let empty_entries = old_chunk_ins.empty_entries.load(Relaxed);
        let old_cap = old_chunk_ins.capacity;
        let new_cap = if empty_entries > (old_cap >> 1) {
            // Clear tombstones
            old_cap
        } else {
            let mut cap = old_cap << 1;
            if cap < 2048 {
                cap <<= 1;
            }
            if epoch < 5 {
                cap <<= 1;
            }
            if timestamp() - self.timestamp.load(Acquire) < 1000 {
                cap <<= 1;
            }
            cap
        };
        debug!(
            "New size for {:?} is {}, was {}",
            old_chunk_ptr, new_cap, old_cap
        );
        // Swap in old chunk as placeholder for the lock
        if let Err(_) = self
            .new_chunk
            .compare_and_set(Shared::null(), old_chunk_ptr, AcqRel, guard)
        {
            // other thread have allocated new chunk and wins the competition, exit
            trace!("Cannot obtain lock for resize, will retry");
            return ResizeResult::SwapFailed;
        }
        if self.chunk.load(Acquire, guard) != old_chunk_ptr {
            warn!("Give up on resize due to old chunk changed after lock obtained");
            self.new_chunk.store(Shared::null(), Release);
            fence(SeqCst);
            return ResizeResult::ChunkChanged;
        }
        debug!("Resizing {:?}", old_chunk_ptr);
        let new_chunk_ptr =
            Owned::new(ChunkPtr::new(Chunk::alloc_chunk(new_cap))).into_shared(guard);
        self.new_chunk.store(new_chunk_ptr, Release); // Stump becasue we have the lock already
        self.epoch.fetch_add(1, AcqRel); // Increase epoch by one
        let new_chunk_ins = unsafe { new_chunk_ptr.deref() };
        // Migrate entries
        self.migrate_entries(old_chunk_ins, new_chunk_ins, guard);
        // Assertion check
        debug_assert_ne!(old_chunk_ins.ptr as usize, new_chunk_ins.base);
        debug_assert_ne!(old_chunk_ins.ptr, unsafe { new_chunk_ptr.deref().ptr });
        debug_assert!(!new_chunk_ptr.is_null());
        let swap_old = self
            .chunk
            .compare_and_set(old_chunk_ptr, new_chunk_ptr, AcqRel, guard);
        if let Err(e) = swap_old {
            // Should not happend, we cannot fix this
            panic!("Resize swap pointer failed: {:?}", e);
        }
        unsafe {
            guard.defer_destroy(old_chunk_ptr);
        }
        self.timestamp.store(timestamp(), Release);
        self.new_chunk.store(Shared::null(), Release);
        self.epoch.fetch_add(1, AcqRel); // Increase epoch by one
        debug!(
            "Migration for {:?} completed, new chunk is {:?}, size from {} to {}",
            old_chunk_ptr, new_chunk_ptr, old_cap, new_cap
        );
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
        let backoff = crossbeam_utils::Backoff::new();
        while old_address < boundary {
            // iterate the old chunk to extract entries that is NOT empty
            let fkey = self.get_fast_key(old_address);
            let fvalue = self.get_fast_value(old_address);
            // Reasoning value states
            match &fvalue.parsed {
                ParsedValue::Val(v) => {
                    if fkey == EMPTY_KEY {
                        // Value have no key, insertion in progress
                        backoff.spin();
                        continue;
                    }

                    // Insert entry into new chunk, in case of failure, skip this entry
                    // Value should be primed
                    trace!("Moving key: {}, value: {}", fkey, v);
                    let primed_fval = fvalue.raw | INV_VAL_BIT_MASK;
                    let (key, value) = old_chunk_ins.attachment.get(idx);
                    let new_chunk_insertion = self.modify_entry(
                        &*new_chunk_ins,
                        &key,
                        fkey,
                        ModOp::AttemptInsert(primed_fval, &value),
                        guard,
                    );
                    let inserted_addr = match new_chunk_insertion {
                        ModResult::Done(addr, _) => Some(addr), // continue procedure
                        ModResult::Existed(_, _) => None,
                        ModResult::Fail => unreachable!("Should not fail"),
                        ModResult::Replaced(_, _, _) => {
                            unreachable!("Attempt insert does not replace anything")
                        }
                        ModResult::Sentinel => unreachable!("New chunk should not have sentinel"),
                        ModResult::NotFound => unreachable!("Not found on resize"),
                        ModResult::TableFull => unreachable!("Table full when resize"),
                        ModResult::Aborted => unreachable!("Should not abort"),
                    };
                    // CAS to ensure sentinel into old chunk (spec)
                    // Use CAS for old threads may working on this one
                    if self.cas_sentinel(old_address, fvalue.raw) {
                        if let Some(new_entry_addr) = inserted_addr {
                            // strip prime
                            let stripped = primed_fval & VAL_BIT_MASK;
                            debug_assert_ne!(stripped, SENTINEL_VALUE);
                            if self.cas_value(new_entry_addr, primed_fval, stripped) {
                                trace!(
                                    "Effective copy key: {}, value {}, addr: {}",
                                    fkey,
                                    stripped,
                                    new_entry_addr
                                );
                                effective_copy += 1;
                            } else {
                                trace!("Value changed before strip prime for key {}", fkey);
                            }
                            old_chunk_ins.attachment.erase(idx);
                            fence(SeqCst);
                        }
                    } else {
                        panic!("Sentinel CAS should always succeed but failed {}", fkey);
                    }
                }
                ParsedValue::Prime(_) => {
                    unreachable!("Shall not have prime in old table");
                }
                ParsedValue::Sentinel => {
                    // Sentinel, skip
                    // Sentinel in old chunk implies its new value have already in the new chunk
                    trace!("Skip copy sentinel");
                }
                ParsedValue::Empty => {
                    if !self.cas_sentinel(old_address, fvalue.raw) {
                        warn!("Filling empty with sentinel for old table should succeed but not, retry");
                        backoff.spin();
                        continue;
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

    pub fn map_is_copying(&self) -> bool {
        let guard = crossbeam_epoch::pin();
        let chunk_ptr = self.chunk.load(Acquire, &guard);
        let new_chunk_ptr = self.new_chunk.load(Acquire, &guard);
        Self::is_copying(&chunk_ptr, &new_chunk_ptr)
    }
}

impl Value {
    pub fn new<K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default, H: Hasher + Default>(
        val: usize,
    ) -> Self {
        let res = {
            if val == 0 {
                ParsedValue::Empty
            } else {
                let actual_val = val & VAL_BIT_MASK;
                let flag = val & INV_VAL_BIT_MASK;
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
        let new_table = Table {
            chunk: Default::default(),
            new_chunk: Default::default(),
            count: AtomicUsize::new(0),
            epoch: AtomicUsize::new(0),
            timestamp: AtomicU64::new(timestamp()),
            mark: PhantomData,
        };
        let guard = crossbeam_epoch::pin();
        let old_chunk_ptr = self.chunk.load(Acquire, &guard);
        let new_chunk_ptr = self.new_chunk.load(Acquire, &guard);
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
            new_table.chunk.store(cloned_old_ref, Release);

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
                new_table.new_chunk.store(cloned_new_ref, Release);
            } else {
                new_table.new_chunk.store(Shared::null(), Release);
            }
        }
        new_table.count.store(self.count.load(Acquire), Release);
        new_table
    }
}

impl<K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Drop
    for Table<K, V, A, ALLOC, H>
{
    fn drop(&mut self) {
        let guard = crossbeam_epoch::pin();
        unsafe {
            guard.defer_destroy(self.chunk.load(Acquire, &guard));
            let new_chunk_ptr = self.new_chunk.load(Acquire, &guard);
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
    (cap as f64 * 0.60f64) as usize
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

pub trait Map<K, V: Clone> {
    fn with_capacity(cap: usize) -> Self;
    fn get(&self, key: &K) -> Option<V>;
    fn insert(&self, key: &K, value: V) -> Option<V>;
    // Return None if insertion successful
    fn try_insert(&self, key: &K, value: V) -> Option<V>;
    fn remove(&self, key: &K) -> Option<V>;
    fn entries(&self) -> Vec<(K, V)>;
    fn contains_key(&self, key: &K) -> bool;
    fn len(&self) -> usize;
    // The func function should  be pure and have no side effect
    fn get_or_insert<F: Fn() -> V>(&self, key: &K, func: F) -> V {
        loop {
            if self.contains_key(key) {
                if let Some(value) = self.get(key) {
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
}

const NUM_FIX: usize = 5;
const PLACEHOLDER_VAL: usize = NUM_FIX + 1;

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

impl<K: Clone + Hash + Eq, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default>
    HashMap<K, V, ALLOC, H>
{
    pub fn insert_with_op(&self, op: InsertOp, key: &K, value: V) -> Option<V> {
        let hash = hash_key::<K, H>(&key);
        self.table
            .insert(op, key, Some(value), hash, PLACEHOLDER_VAL)
            .map(|(_, v)| v)
    }

    pub fn write(&self, key: &K) -> Option<HashMapWriteGuard<K, V, ALLOC, H>> {
        HashMapWriteGuard::new(&self.table, key)
    }
    pub fn read(&self, key: &K) -> Option<HashMapReadGuard<K, V, ALLOC, H>> {
        HashMapReadGuard::new(&self.table, key)
    }
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
        self.insert_with_op(InsertOp::Insert, key, value)
    }

    #[inline(always)]
    fn try_insert(&self, key: &K, value: V) -> Option<V> {
        self.insert_with_op(InsertOp::TryInsert, key, value)
    }

    #[inline(always)]
    fn remove(&self, key: &K) -> Option<V> {
        let hash = hash_key::<K, H>(&key);
        self.table.remove(key, hash).map(|(_, v)| v)
    }

    #[inline(always)]
    fn entries(&self) -> Vec<(K, V)> {
        self.table
            .entries()
            .into_iter()
            .map(|(_, _, k, v)| (k, v))
            .collect()
    }

    #[inline(always)]
    fn contains_key(&self, key: &K) -> bool {
        let hash = hash_key::<K, H>(&key);
        self.table.get(key, hash, false).is_some()
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.table.len()
    }
}

impl<T, A: GlobalAlloc + Default> WordObjectAttachment<T, A> {
    fn addr_by_index(&self, index: usize) -> usize {
        self.obj_chunk + index * self.obj_size
    }
}

type ObjectTable<V, ALLOC, H> = Table<(), V, WordObjectAttachment<V, ALLOC>, ALLOC, H>;

#[derive(Clone)]
pub struct ObjectMap<
    V: Clone,
    ALLOC: GlobalAlloc + Default = System,
    H: Hasher + Default = DefaultHasher,
> {
    table: ObjectTable<V, ALLOC, H>,
}

impl<V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> ObjectMap<V, ALLOC, H> {
    fn insert_with_op(&self, op: InsertOp, key: &usize, value: V) -> Option<V> {
        self.table
            .insert(op, &(), Some(value), key + NUM_FIX, PLACEHOLDER_VAL)
            .map(|(_, v)| v)
    }

    pub fn read(&self, key: usize) -> Option<ObjectMapReadGuard<V, ALLOC, H>> {
        ObjectMapReadGuard::new(&self.table, key)
    }

    pub fn write(&self, key: usize) -> Option<ObjectMapWriteGuard<V, ALLOC, H>> {
        ObjectMapWriteGuard::new(&self.table, key)
    }
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
            .get(&(), key + NUM_FIX, true)
            .map(|v| v.1.unwrap())
    }

    #[inline(always)]
    fn insert(&self, key: &usize, value: V) -> Option<V> {
        self.insert_with_op(InsertOp::Insert, key, value)
    }

    #[inline(always)]
    fn try_insert(&self, key: &usize, value: V) -> Option<V> {
        self.insert_with_op(InsertOp::TryInsert, key, value)
    }

    #[inline(always)]
    fn remove(&self, key: &usize) -> Option<V> {
        self.table.remove(&(), key + NUM_FIX).map(|(_, v)| v)
    }

    #[inline(always)]
    fn entries(&self) -> Vec<(usize, V)> {
        self.table
            .entries()
            .into_iter()
            .map(|(_, k, _, v)| (k - NUM_FIX, v))
            .collect()
    }

    #[inline(always)]
    fn contains_key(&self, key: &usize) -> bool {
        self.table.get(&(), key + NUM_FIX, false).is_some()
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.table.len()
    }
}

#[derive(Clone)]
pub struct WordMap<ALLOC: GlobalAlloc + Default = System, H: Hasher + Default = DefaultHasher> {
    table: WordTable<ALLOC, H>,
}

impl<ALLOC: GlobalAlloc + Default, H: Hasher + Default> WordMap<ALLOC, H> {
    fn insert_with_op(&self, op: InsertOp, key: &usize, value: usize) -> Option<usize> {
        self.table
            .insert(op, &(), None, key + NUM_FIX, value + NUM_FIX)
            .map(|(v, _)| v)
    }

    pub fn get_from_mutex(&self, key: &usize) -> Option<usize> {
        self.get(key).map(|v| v & WORD_MUTEX_DATA_BIT_MASK)
    }
}

impl<ALLOC: GlobalAlloc + Default, H: Hasher + Default> Map<usize, usize> for WordMap<ALLOC, H> {
    fn with_capacity(cap: usize) -> Self {
        Self {
            table: Table::with_capacity(cap),
        }
    }

    #[inline(always)]
    fn get(&self, key: &usize) -> Option<usize> {
        self.table
            .get(&(), key + NUM_FIX, false)
            .map(|v| v.0 - NUM_FIX)
    }

    #[inline(always)]
    fn insert(&self, key: &usize, value: usize) -> Option<usize> {
        self.insert_with_op(InsertOp::UpsertFast, key, value)
    }

    #[inline(always)]
    fn try_insert(&self, key: &usize, value: usize) -> Option<usize> {
        self.insert_with_op(InsertOp::TryInsert, key, value)
    }

    #[inline(always)]
    fn remove(&self, key: &usize) -> Option<usize> {
        self.table
            .remove(&(), key + NUM_FIX)
            .map(|(v, _)| v - NUM_FIX)
    }
    fn entries(&self) -> Vec<(usize, usize)> {
        self.table
            .entries()
            .into_iter()
            .map(|(k, v, _, _)| (k - NUM_FIX, v - NUM_FIX))
            .collect()
    }

    #[inline(always)]
    fn contains_key(&self, key: &usize) -> bool {
        self.get(key).is_some()
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.table.len()
    }
}

const WORD_MUTEX_DATA_BIT_MASK: usize = !0 << 2 >> 2;

pub struct WordMutexGuard<
    'a,
    ALLOC: GlobalAlloc + Default = System,
    H: Hasher + Default = DefaultHasher,
> {
    table: &'a WordTable<ALLOC, H>,
    key: usize,
    value: usize,
}

impl<'a, ALLOC: GlobalAlloc + Default, H: Hasher + Default> WordMutexGuard<'a, ALLOC, H> {
    fn create(table: &'a WordTable<ALLOC, H>, key: usize) -> Option<Self> {
        let key = key + NUM_FIX;
        let value = 0;
        if table
            .insert(
                InsertOp::TryInsert,
                &(),
                Some(()),
                key,
                value | MUTEX_BIT_MASK,
            )
            .is_none()
        {
            trace!("Created locked key {}", key);
            Some(Self { table, key, value })
        } else {
            trace!("Cannot create locked key {} ", key);
            None
        }
    }
    fn new(table: &'a WordTable<ALLOC, H>, key: usize) -> Option<Self> {
        let key = key + NUM_FIX;
        let backoff = crossbeam_utils::Backoff::new();
        let guard = crossbeam_epoch::pin();
        let value;
        loop {
            let swap_res = table.swap(
                key,
                &(),
                move |fast_value| {
                    trace!("The key {} have value {}", key, fast_value);
                    if fast_value & MUTEX_BIT_MASK > 0 {
                        // Locked, unchanged
                        trace!("The key {} have locked, unchanged and try again", key);
                        None
                    } else {
                        // Obtain lock
                        trace!(
                            "The key {} have obtained, with value {}",
                            key,
                            fast_value & WORD_MUTEX_DATA_BIT_MASK
                        );
                        Some(fast_value | MUTEX_BIT_MASK)
                    }
                },
                &guard,
            );
            match swap_res {
                SwapResult::Succeed(val, _idx, _chunk) => {
                    trace!("Lock on key {} succeed with value {}", key, val);
                    value = val & WORD_MUTEX_DATA_BIT_MASK;
                    break;
                }
                SwapResult::Failed | SwapResult::Aborted => {
                    trace!("Lock on key {} failed, retry", key);
                    backoff.spin();
                    continue;
                }
                SwapResult::NotFound => {
                    trace!("Cannot found key {} to lock", key);
                    return None;
                }
            }
        }
        debug_assert_ne!(value, 0);
        let value = value - NUM_FIX;
        Some(Self { table, key, value })
    }

    pub fn remove(self) -> usize {
        trace!("Removing {}", self.key);
        let res = self.table.remove(&(), self.key).unwrap().0;
        mem::forget(self);
        res | MUTEX_BIT_MASK
    }
}

impl<'a, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Deref for WordMutexGuard<'a, ALLOC, H> {
    type Target = usize;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a, ALLOC: GlobalAlloc + Default, H: Hasher + Default> DerefMut
    for WordMutexGuard<'a, ALLOC, H>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

impl<'a, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Drop for WordMutexGuard<'a, ALLOC, H> {
    fn drop(&mut self) {
        self.value += NUM_FIX;
        trace!(
            "Release lock for key {} with value {}",
            self.key,
            self.value
        );
        self.table.insert(
            InsertOp::UpsertFast,
            &(),
            None,
            self.key,
            self.value & WORD_MUTEX_DATA_BIT_MASK,
        );
    }
}

impl<ALLOC: GlobalAlloc + Default, H: Hasher + Default> WordMap<ALLOC, H> {
    pub fn lock(&self, key: usize) -> Option<WordMutexGuard<ALLOC, H>> {
        WordMutexGuard::new(&self.table, key)
    }
    pub fn try_insert_locked(&self, key: usize) -> Option<WordMutexGuard<ALLOC, H>> {
        WordMutexGuard::create(&self.table, key)
    }
}

pub struct HashMapReadGuard<
    'a,
    K: Clone + Eq + Hash,
    V: Clone,
    ALLOC: GlobalAlloc + Default = System,
    H: Hasher + Default = DefaultHasher,
> {
    table: &'a HashTable<K, V, ALLOC>,
    hash: usize,
    key: K,
    value: V,
    _mark: PhantomData<H>,
}

impl<'a, K: Clone + Eq + Hash, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default>
    HashMapReadGuard<'a, K, V, ALLOC, H>
{
    fn new(table: &'a HashTable<K, V, ALLOC>, key: &K) -> Option<Self> {
        let backoff = crossbeam_utils::Backoff::new();
        let guard = crossbeam_epoch::pin();
        let hash = hash_key::<K, H>(&key);
        let value: V;
        loop {
            let swap_res = table.swap(
                hash,
                key,
                move |fast_value| {
                    if fast_value != PLACEHOLDER_VAL - 1 {
                        // Not write locked, can bump it by one
                        trace!("Key hash {} is not write locked, will read lock", hash);
                        Some(fast_value + 1)
                    } else {
                        trace!("Key hash {} is write locked, unchanged", hash);
                        None
                    }
                },
                &guard,
            );
            match swap_res {
                SwapResult::Succeed(_, idx, chunk) => {
                    let chunk_ref = unsafe { chunk.deref() };
                    let (_, v) = chunk_ref.attachment.get(idx);
                    value = v;
                    break;
                }
                SwapResult::Failed | SwapResult::Aborted => {
                    trace!("Lock on key hash {} failed, retry", hash);
                    backoff.spin();
                    continue;
                }
                SwapResult::NotFound => {
                    debug!("Cannot found hash key {} to lock", hash);
                    return None;
                }
            }
        }
        Some(Self {
            table,
            key: key.clone(),
            value,
            hash,
            _mark: Default::default(),
        })
    }
}

impl<'a, K: Clone + Eq + Hash, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Deref
    for HashMapReadGuard<'a, K, V, ALLOC, H>
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a, K: Clone + Eq + Hash, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Drop
    for HashMapReadGuard<'a, K, V, ALLOC, H>
{
    fn drop(&mut self) {
        trace!("Release read lock for hash key {}", self.hash);
        let guard = crossbeam_epoch::pin();
        self.table.swap(
            self.hash,
            &self.key,
            |fast_value| {
                debug_assert!(fast_value > PLACEHOLDER_VAL);
                Some(fast_value - 1)
            },
            &guard,
        );
    }
}

pub struct HashMapWriteGuard<
    'a,
    K: Clone + Eq + Hash,
    V: Clone,
    ALLOC: GlobalAlloc + Default = System,
    H: Hasher + Default = DefaultHasher,
> {
    table: &'a HashTable<K, V, ALLOC>,
    hash: usize,
    key: K,
    value: V,
    _mark: PhantomData<H>,
}

impl<'a, K: Clone + Eq + Hash, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default>
    HashMapWriteGuard<'a, K, V, ALLOC, H>
{
    fn new(table: &'a HashTable<K, V, ALLOC>, key: &K) -> Option<Self> {
        let backoff = crossbeam_utils::Backoff::new();
        let guard = crossbeam_epoch::pin();
        let hash = hash_key::<K, H>(&key);
        let value: V;
        loop {
            let swap_res = table.swap(
                hash,
                key,
                move |fast_value| {
                    if fast_value == PLACEHOLDER_VAL {
                        // Not write locked, can bump it by one
                        trace!("Key hash {} is write lockable, will write lock", hash);
                        Some(fast_value - 1)
                    } else {
                        trace!("Key hash {} is write locked, unchanged", hash);
                        None
                    }
                },
                &guard,
            );
            match swap_res {
                SwapResult::Succeed(_, idx, chunk) => {
                    let chunk_ref = unsafe { chunk.deref() };
                    let (_, v) = chunk_ref.attachment.get(idx);
                    value = v;
                    break;
                }
                SwapResult::Failed | SwapResult::Aborted => {
                    trace!("Lock on key hash {} failed, retry", hash);
                    backoff.spin();
                    continue;
                }
                SwapResult::NotFound => {
                    debug!("Cannot found hash key {} to lock", hash);
                    return None;
                }
            }
        }
        Some(Self {
            table,
            key: key.clone(),
            value,
            hash,
            _mark: Default::default(),
        })
    }

    pub fn remove(self) -> V {
        let res = self.table.remove(&self.key, self.hash).unwrap().1;
        mem::forget(self);
        res
    }
}

impl<'a, K: Clone + Eq + Hash, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Deref
    for HashMapWriteGuard<'a, K, V, ALLOC, H>
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a, K: Clone + Eq + Hash, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> DerefMut
    for HashMapWriteGuard<'a, K, V, ALLOC, H>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

impl<'a, K: Clone + Eq + Hash, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Drop
    for HashMapWriteGuard<'a, K, V, ALLOC, H>
{
    fn drop(&mut self) {
        trace!("Release read lock for hash key {}", self.hash);
        let hash = hash_key::<K, H>(&self.key);
        self.table.insert(
            InsertOp::Insert,
            &self.key,
            Some(self.value.clone()),
            hash,
            PLACEHOLDER_VAL,
        );
    }
}

pub struct ObjectMapReadGuard<
    'a,
    V: Clone,
    ALLOC: GlobalAlloc + Default = System,
    H: Hasher + Default = DefaultHasher,
> {
    table: &'a ObjectTable<V, ALLOC, H>,
    key: usize,
    value: V,
    _mark: PhantomData<H>,
}

impl<'a, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default>
    ObjectMapReadGuard<'a, V, ALLOC, H>
{
    fn new(table: &'a ObjectTable<V, ALLOC, H>, key: usize) -> Option<Self> {
        let backoff = crossbeam_utils::Backoff::new();
        let guard = crossbeam_epoch::pin();
        let hash = hash_key::<usize, H>(&key);
        let value: V;
        loop {
            let swap_res = table.swap(
                key,
                &(),
                move |fast_value| {
                    if fast_value != PLACEHOLDER_VAL - 1 {
                        // Not write locked, can bump it by one
                        trace!("Key {} is not write locked, will read lock", hash);
                        Some(fast_value + 1)
                    } else {
                        trace!("Key {} is write locked, unchanged", hash);
                        None
                    }
                },
                &guard,
            );
            match swap_res {
                SwapResult::Succeed(_, idx, chunk) => {
                    let chunk_ref = unsafe { chunk.deref() };
                    let (_, v) = chunk_ref.attachment.get(idx);
                    value = v;
                    break;
                }
                SwapResult::Failed | SwapResult::Aborted => {
                    trace!("Lock on key {} failed, retry", hash);
                    backoff.spin();
                    continue;
                }
                SwapResult::NotFound => {
                    debug!("Cannot found hash key {} to lock", hash);
                    return None;
                }
            }
        }
        Some(Self {
            table,
            key,
            value,
            _mark: Default::default(),
        })
    }
}

impl<'a, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Deref
    for ObjectMapReadGuard<'a, V, ALLOC, H>
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Drop
    for ObjectMapReadGuard<'a, V, ALLOC, H>
{
    fn drop(&mut self) {
        trace!("Release read lock for hash key {}", self.key);
        let guard = crossbeam_epoch::pin();
        self.table.swap(
            self.key,
            &(),
            |fast_value| {
                debug_assert!(fast_value > PLACEHOLDER_VAL);
                Some(fast_value - 1)
            },
            &guard,
        );
    }
}

pub struct ObjectMapWriteGuard<
    'a,
    V: Clone,
    ALLOC: GlobalAlloc + Default = System,
    H: Hasher + Default = DefaultHasher,
> {
    table: &'a ObjectTable<V, ALLOC, H>,
    key: usize,
    value: V,
    _mark: PhantomData<H>,
}

impl<'a, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default>
    ObjectMapWriteGuard<'a, V, ALLOC, H>
{
    fn new(table: &'a ObjectTable<V, ALLOC, H>, key: usize) -> Option<Self> {
        let backoff = crossbeam_utils::Backoff::new();
        let guard = crossbeam_epoch::pin();
        let value: V;
        let key = key + NUM_FIX;
        loop {
            let swap_res = table.swap(
                key,
                &(),
                move |fast_value| {
                    if fast_value == PLACEHOLDER_VAL {
                        // Not write locked, can bump it by one
                        trace!("Key {} is write lockable, will write lock", key);
                        Some(fast_value - 1)
                    } else {
                        trace!("Key {} is write locked, unchanged", key);
                        None
                    }
                },
                &guard,
            );
            match swap_res {
                SwapResult::Succeed(_, idx, chunk) => {
                    let chunk_ref = unsafe { chunk.deref() };
                    let (_, v) = chunk_ref.attachment.get(idx);
                    value = v;
                    break;
                }
                SwapResult::Failed | SwapResult::Aborted => {
                    trace!("Lock on key {} failed, retry", key);
                    backoff.spin();
                    continue;
                }
                SwapResult::NotFound => {
                    debug!("Cannot found key {} to lock", key);
                    return None;
                }
            }
        }
        Some(Self {
            table,
            key,
            value,
            _mark: Default::default(),
        })
    }

    pub fn remove(self) -> V {
        let res = self.table.remove(&(), self.key).unwrap().1;
        mem::forget(self);
        res
    }
}

impl<'a, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Deref
    for ObjectMapWriteGuard<'a, V, ALLOC, H>
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> DerefMut
    for ObjectMapWriteGuard<'a, V, ALLOC, H>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

impl<'a, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Drop
    for ObjectMapWriteGuard<'a, V, ALLOC, H>
{
    fn drop(&mut self) {
        trace!("Release read lock for key {}", self.key);
        self.table.insert(
            InsertOp::Insert,
            &(),
            Some(self.value.clone()),
            self.key,
            PLACEHOLDER_VAL,
        );
    }
}

pub struct HashSet<
    T: Clone + Hash + Eq,
    ALLOC: GlobalAlloc + Default = System,
    H: Hasher + Default = DefaultHasher,
> {
    table: HashTable<T, (), ALLOC>,
    shadow: PhantomData<H>,
}

impl<T: Clone + Hash + Eq, ALLOC: GlobalAlloc + Default, H: Hasher + Default> HashSet<T, ALLOC, H> {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            table: Table::with_capacity(cap),
            shadow: PhantomData,
        }
    }

    pub fn contains(&self, item: &T) -> bool {
        let hash = hash_key::<T, H>(item);
        self.table.get(item, hash, false).is_some()
    }

    pub fn insert(&self, item: &T) -> bool {
        let hash = hash_key::<T, H>(item);
        self.table
            .insert(InsertOp::TryInsert, item, None, hash, !0)
            .is_none()
    }

    pub fn remove(&self, item: &T) -> bool {
        let hash = hash_key::<T, H>(item);
        self.table.remove(item, hash).is_some()
    }

    pub fn items(&self) -> std::collections::HashSet<T> {
        self.table
            .entries()
            .into_iter()
            .map(|(_, _, item, _)| item)
            .collect()
    }

    #[inline(always)]
    pub fn len(&self) -> usize {
        self.table.len()
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

fn timestamp() -> u64 {
    let start = SystemTime::now();
    let since_the_epoch = start.duration_since(UNIX_EPOCH).unwrap();
    since_the_epoch.as_millis() as u64
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
        let num_threads = 64;
        let test_load = 1048576;
        let map = Arc::new(WordMap::<System>::with_capacity(32));
        let mut threads = vec![];
        for i in 0..num_threads {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                for j in 5..test_load {
                    let key = i * 10000000 + j;
                    let value = i * j;
                    map.insert(&key, value);
                    assert_eq!(
                        map.get(&key),
                        Some(value),
                        "Is copying: {}",
                        map.table.map_is_copying()
                    );
                    if j % 13 == 0 {
                        assert_eq!(
                            map.remove(&key),
                            Some(value),
                            "Remove result, copying {}",
                            map.table.map_is_copying()
                        );
                        assert_eq!(map.get(&key), None, "Remove recursion");
                    }
                    if j % 3 == 0 {
                        let new_value = value + 7;
                        map.insert(&key, new_value);
                        assert_eq!(map.get(&key), Some(new_value));
                    }
                }
            }));
        }
        info!("Waiting for intensive insertion to finish");
        for thread in threads {
            let _ = thread.join();
        }
        info!("Checking final value");
        for i in 0..num_threads {
            for j in 5..test_load {
                let k = i * 10000000 + j;
                let value = i * j;
                let get_res = map.get(&k);
                if j % 3 == 0 {
                    assert_eq!(get_res, Some(value + 7), "Mod k :{}, i {}, j {}", k, i, j);
                } else if j % 13 == 0 {
                    assert_eq!(get_res, None, "Remove k {}, i {}, j {}", k, i, j);
                } else {
                    assert_eq!(get_res, Some(value), "New k {}, i {}, j {}", k, i, j)
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

    #[test]
    fn parallel_word_map_mutex() {
        let _ = env_logger::try_init();
        let map = Arc::new(WordMap::<System>::with_capacity(4));
        map.insert(&1, 0);
        let mut threads = vec![];
        let num_threads = 256;
        for _ in 0..num_threads {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                let mut guard = map.lock(1).unwrap();
                *guard += 1;
            }));
        }
        for thread in threads {
            let _ = thread.join();
        }
        assert_eq!(map.get(&1).unwrap(), num_threads);
    }

    #[test]
    fn parallel_obj_map_rwlock() {
        let _ = env_logger::try_init();
        let map_cont = ObjectMap::<Obj, System, DefaultHasher>::with_capacity(4);
        let map = Arc::new(map_cont);
        map.insert(&1, Obj::new(0));
        let mut threads = vec![];
        let num_threads = 256;
        for i in 0..num_threads {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                let mut guard = map.write(1).unwrap();
                let val = guard.get();
                guard.set(val + 1);
                trace!("Dealt with {}", i);
            }));
        }
        for thread in threads {
            let _ = thread.join();
        }
        map.get(&1).unwrap().validate(num_threads);
    }

    #[test]
    fn parallel_hash_map_rwlock() {
        let _ = env_logger::try_init();
        let map_cont = super::HashMap::<u32, Obj, System, DefaultHasher>::with_capacity(4);
        let map = Arc::new(map_cont);
        map.insert(&1, Obj::new(0));
        let mut threads = vec![];
        let num_threads = 256;
        for i in 0..num_threads {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                let mut guard = map.write(&1u32).unwrap();
                let val = guard.get();
                guard.set(val + 1);
                trace!("Dealt with {}", i);
            }));
        }
        for thread in threads {
            let _ = thread.join();
        }
        map.get(&1).unwrap().validate(num_threads);
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
        fn get(&self) -> usize {
            self.a
        }
        fn set(&mut self, num: usize) {
            *self = Self::new(num)
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
