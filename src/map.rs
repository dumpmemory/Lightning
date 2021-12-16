// usize to usize lock-free, wait free table
use crate::align_padding;
use alloc::vec::Vec;
use core::alloc::{GlobalAlloc, Layout};
use core::hash::Hasher;
use core::marker::PhantomData;
use core::ops::Deref;
use core::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release, SeqCst};
use core::sync::atomic::{compiler_fence, fence, AtomicU64, AtomicUsize};
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
const TOMBSTONE_VALUE: usize = 2;
const VAL_BIT_MASK: usize = !0 << 1 >> 1;
const INV_VAL_BIT_MASK: usize = !VAL_BIT_MASK;
const MUTEX_BIT_MASK: usize = !WORD_MUTEX_DATA_BIT_MASK & VAL_BIT_MASK;
const ENTRY_SIZE: usize = mem::size_of::<EntryTemplate>();

struct Value {
    raw: usize,
    parsed: ParsedValue,
}

enum ParsedValue {
    Val(usize), // None for tombstone
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
    Done(usize, Option<V>, usize), // _, value, index
    TableFull,
    Aborted,
}

enum ModOp<'a, V> {
    Insert(usize, &'a V),
    UpsertFastVal(usize),
    AttemptInsert(usize, &'a V),
    SwapFastVal(Box<dyn Fn(usize) -> Option<usize>>),
    Sentinel,
    Tombstone,
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
    init_cap: usize,
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
            init_cap: cap,
            mark: PhantomData,
        }
    }

    pub fn new() -> Self {
        Self::with_capacity(64)
    }

    pub fn get(&self, key: &K, fkey: usize, read_attachment: bool) -> Option<(usize, Option<V>)> {
        enum FromChunkRes<V> {
            Value(usize, Value, Option<V>, usize, usize), // Last one is idx
            Prime,
            None,
            Sentinel,
        }
        let guard = crossbeam_epoch::pin();
        let backoff = crossbeam_utils::Backoff::new();
        let hash = hash::<H>(fkey);
        loop {
            let epoch = self.now_epoch();
            let chunk_ptr = self.chunk.load(Acquire, &guard);
            let new_chunk_ptr = self.new_chunk.load(Acquire, &guard);
            let chunk = unsafe { chunk_ptr.deref() };
            let new_chunk = Self::to_chunk_ref(epoch, &chunk_ptr, &new_chunk_ptr);
            debug_assert!(!chunk_ptr.is_null());
            let get_from =
                |chunk: &Chunk<K, V, A, ALLOC>, migrating: Option<&ChunkPtr<K, V, A, ALLOC>>| {
                    let (val, idx, addr) = self.get_from_chunk(&*chunk, hash, key, fkey, migrating);
                    match val.parsed {
                        ParsedValue::Empty | ParsedValue::Val(0) => FromChunkRes::None,
                        ParsedValue::Val(v) => FromChunkRes::Value(
                            v,
                            val,
                            if Self::can_attach() && read_attachment {
                                Some(chunk.attachment.get(idx).1)
                            } else {
                                None
                            },
                            idx,
                            addr,
                        ),
                        ParsedValue::Prime(_) => FromChunkRes::Prime,
                        ParsedValue::Sentinel => FromChunkRes::Sentinel,
                    }
                };
            return match get_from(&chunk, new_chunk) {
                FromChunkRes::Value(fval, val, attach_val, idx, addr) => {
                    if let Some(new_chunk) = new_chunk {
                        self.migrate_entry(fkey, idx, val, chunk, new_chunk, addr, &mut 0);
                    }
                    Some((fval, attach_val))
                }
                FromChunkRes::Sentinel => {
                    if let Some(new_chunk) = new_chunk {
                        dfence();
                        match get_from(&new_chunk, None) {
                            FromChunkRes::Value(fval, _, val, _, _) => Some((fval, val)),
                            FromChunkRes::Sentinel => {
                                // Sentinel in new chunk, should retry
                                backoff.spin();
                                continue;
                            }
                            FromChunkRes::None => {
                                trace!(
                                    "Got non from new chunk for {} at epoch {}",
                                    fkey - NUM_FIX,
                                    epoch
                                );
                                None
                            }
                            FromChunkRes::Prime => {
                                backoff.spin();
                                continue;
                            }
                        }
                    } else {
                        warn!(
                            "Got sentinel on get but new chunk is null for {}, retry. Copying {}, epoch {}, now epoch {}",
                            fkey,
                            new_chunk.is_some(),
                            epoch,
                            self.epoch.load(Acquire)
                        );
                        backoff.spin();
                        continue;
                    }
                }
                FromChunkRes::None => {
                    if let Some(chunk) = new_chunk {
                        dfence();
                        match get_from(chunk, None) {
                            FromChunkRes::Value(fval, _, val, _, _) => Some((fval, val)),
                            FromChunkRes::Sentinel => {
                                // Sentinel in new chunk, should retry
                                backoff.spin();
                                continue;
                            }
                            FromChunkRes::None => {
                                trace!(
                                    "Got non from new chunk for {} at epoch {}",
                                    fkey - 5,
                                    epoch
                                );
                                None
                            }
                            FromChunkRes::Prime => {
                                backoff.spin();
                                continue;
                            }
                        }
                    } else {
                        None
                    }
                }
                FromChunkRes::Prime => {
                    backoff.spin();
                    continue;
                }
            };
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
        let hash = hash::<H>(fkey);
        loop {
            let epoch = self.now_epoch();
            // trace!("Inserting key: {}, value: {}", fkey, fvalue);
            let chunk_ptr = self.chunk.load(Acquire, &guard);
            let new_chunk_ptr = self.new_chunk.load(Acquire, &guard);
            let new_chunk = Self::to_chunk_ref(epoch, &chunk_ptr, &new_chunk_ptr);
            if new_chunk.is_none() {
                match self.check_migration(chunk_ptr, &guard) {
                    ResizeResult::Done | ResizeResult::SwapFailed | ResizeResult::ChunkChanged => {
                        debug!("Retry insert due to resize");
                        backoff.spin();
                        continue;
                    }
                    ResizeResult::NoNeed => {}
                }
            } else if new_chunk_ptr.is_null() {
                // Copying, must have new chunk
                warn!("Chunk ptrs does not consist with epoch");
                continue;
            }
            let chunk = unsafe { chunk_ptr.deref() };
            let modify_chunk = if let Some(new_chunk) = new_chunk {
                new_chunk
            } else {
                chunk
            };
            let masked_value = fvalue & VAL_BIT_MASK;
            let mod_op = match op {
                InsertOp::Insert => ModOp::Insert(masked_value, value.as_ref().unwrap()),
                InsertOp::UpsertFast => ModOp::UpsertFastVal(masked_value),
                InsertOp::TryInsert => ModOp::AttemptInsert(masked_value, value.as_ref().unwrap()),
            };
            let value_insertion =
                self.modify_entry(&*modify_chunk, hash, key, fkey, mod_op, None, &guard);
            let mut result = None;
            match value_insertion {
                ModResult::Done(_, _, _) => {
                    modify_chunk.occupation.fetch_add(1, Relaxed);
                    self.count.fetch_add(1, Relaxed);
                }
                ModResult::Replaced(fv, v, _) | ModResult::Existed(fv, v) => result = Some((fv, v)),
                ModResult::Fail => {
                    // If fail insertion then retry
                    warn!(
                        "Insertion failed, do migration and retry. Copying {}, cap {}, count {}, old {:?}, new {:?}",
                        new_chunk.is_some(),
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
                        new_chunk.is_some(),
                        modify_chunk.capacity,
                        modify_chunk.occupation.load(Relaxed),
                        chunk_ptr,
                        new_chunk_ptr
                    );
                    self.do_migration(chunk_ptr, &guard);
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
            if new_chunk.is_some() {
                dfence();
                debug_assert_ne!(
                    chunk_ptr, new_chunk_ptr,
                    "at epoch {}, inserting k:{}, v:{}",
                    epoch, fkey, fvalue
                );
                debug_assert_ne!(
                    new_chunk_ptr,
                    Shared::null(),
                    "at epoch {}, inserting k:{}, v:{}",
                    epoch,
                    fkey,
                    fvalue
                );
                self.modify_entry(chunk, hash, key, fkey, ModOp::Sentinel, new_chunk, &guard);
            }
            // trace!("Inserted key {}, with value {}", fkey, fvalue);
            return result;
        }
    }

    pub fn clear(&self) {
        let backoff = crossbeam_utils::Backoff::new();
        let guard = crossbeam_epoch::pin();
        loop {
            let epoch = self.now_epoch();
            if Self::is_copying(epoch) {
                backoff.spin();
                continue;
            }
            let len = self.len();
            let owned_new = Owned::new(ChunkPtr::new(Chunk::alloc_chunk(self.init_cap)));
            self.chunk.store(owned_new.into_shared(&guard), AcqRel);
            self.new_chunk.store(Shared::null(), AcqRel);
            dfence();
            self.count.fetch_sub(len, AcqRel);
        }
    }

    #[inline(always)]
    fn is_copying(epoch: usize) -> bool {
        epoch | 1 == epoch
    }

    #[inline(always)]
    fn epoch_changed(&self, epoch: usize) -> bool {
        self.now_epoch() != epoch
    }

    fn swap<'a, F: Fn(usize) -> Option<usize> + Copy + 'static>(
        &self,
        fkey: usize,
        key: &K,
        func: F,
        guard: &'a Guard,
    ) -> SwapResult<'a, K, V, A, ALLOC> {
        let backoff = crossbeam_utils::Backoff::new();
        let hash = hash::<H>(fkey);
        loop {
            let epoch = self.now_epoch();
            let chunk_ptr = self.chunk.load(Acquire, &guard);
            let new_chunk_ptr = self.new_chunk.load(Acquire, &guard);
            let chunk = unsafe { chunk_ptr.deref() };
            let new_chunk = Self::to_chunk_ref(epoch, &chunk_ptr, &new_chunk_ptr);
            if let Some(new_chunk) = new_chunk {
                // && self.now_epoch() == epoch
                // Copying is on the way, should try to get old value from old chunk then put new value in new chunk
                let (old_parsed_val, old_index, _) =
                    self.get_from_chunk(chunk, hash, key, fkey, Some(new_chunk));
                let old_fval = old_parsed_val.raw;
                if old_fval != SENTINEL_VALUE
                    && old_fval != EMPTY_VALUE
                    && old_fval != TOMBSTONE_VALUE
                {
                    if let Some(new_val) = func(old_fval) {
                        let val = chunk.attachment.get(old_index).1;
                        match self.modify_entry(
                            new_chunk,
                            hash,
                            key,
                            fkey,
                            ModOp::AttemptInsert(new_val, &val),
                            None,
                            guard,
                        ) {
                            ModResult::Done(_, _, new_index)
                            | ModResult::Replaced(_, _, new_index) => {
                                let old_addr = chunk.base + old_index * ENTRY_SIZE;
                                if self.cas_sentinel(old_addr, old_fval) {
                                    // Put a sentinel in the old chunk
                                    return SwapResult::Succeed(
                                        old_fval & VAL_BIT_MASK,
                                        new_index,
                                        new_chunk_ptr,
                                    );
                                } else {
                                    // If fail, we may have some problem here
                                    // The best strategy can be CAS a tombstone to the new index and try everything again
                                    // Note that we use attempt insert, it will be safe to just `remove` it
                                    let new_addr = new_chunk.base + new_index * ENTRY_SIZE;
                                    self.cas_tombstone(new_addr, new_val);
                                    continue;
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
            let modify_chunk_ptr = if new_chunk.is_some() {
                new_chunk_ptr
            } else {
                chunk_ptr
            };
            let modify_chunk = if let Some(new_chunk) = new_chunk {
                new_chunk
            } else {
                chunk
            };
            trace!("Swaping for key {}, copying {}", fkey, new_chunk.is_some());
            let mod_res = self.modify_entry(
                modify_chunk,
                hash,
                key,
                fkey,
                ModOp::SwapFastVal(Box::new(func)),
                None,
                guard,
            );
            if new_chunk.is_some() {
                debug_assert_ne!(chunk_ptr, new_chunk_ptr);
                debug_assert_ne!(new_chunk_ptr, Shared::null());
                self.modify_entry(chunk, hash, key, fkey, ModOp::Sentinel, new_chunk, &guard);
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
                ModResult::Done(_, _, _) => unreachable!("Swap Done"),
                ModResult::TableFull => unreachable!("Swap table full"),
            };
        }
    }

    #[inline(always)]
    fn to_chunk_ref<'a>(
        epoch: usize,
        old_chunk_ptr: &'a Shared<ChunkPtr<K, V, A, ALLOC>>,
        new_chunk_ptr: &'a Shared<ChunkPtr<K, V, A, ALLOC>>,
    ) -> Option<&'a ChunkPtr<K, V, A, ALLOC>> {
        if (Self::is_copying(epoch)) && (!old_chunk_ptr.eq(new_chunk_ptr)) {
            unsafe { new_chunk_ptr.as_ref() }
        } else {
            None
        }
    }

    #[inline(always)]
    fn now_epoch(&self) -> usize {
        self.epoch.load(Acquire)
    }

    pub fn remove(&self, key: &K, fkey: usize) -> Option<(usize, V)> {
        let guard = crossbeam_epoch::pin();
        let backoff = crossbeam_utils::Backoff::new();
        let hash = hash::<H>(fkey);
        loop {
            let epoch = self.now_epoch();
            let new_chunk_ptr = self.new_chunk.load(Acquire, &guard);
            let old_chunk_ptr = self.chunk.load(Acquire, &guard);
            let copying = Self::is_copying(epoch);
            if copying && (new_chunk_ptr.is_null() || new_chunk_ptr == old_chunk_ptr) {
                continue;
            }
            let new_chunk = unsafe { new_chunk_ptr.deref() };
            let old_chunk = unsafe { old_chunk_ptr.deref() };
            let mut retr = None;
            if copying {
                // Put sentinel to the old before putting tombstone to the new
                // If not migration might put the old value back
                trace!("Put sentinel in old chunk for removal");
                debug_assert_ne!(new_chunk_ptr, Shared::null());
                let remove_from_old = self.modify_entry(
                    &*old_chunk,
                    hash,
                    key,
                    fkey,
                    ModOp::Sentinel,
                    Some(&new_chunk),
                    &guard,
                );
                match remove_from_old {
                    ModResult::Done(fvalue, Some(value), _)
                    | ModResult::Replaced(fvalue, value, _) => {
                        trace!("Sentinal placed");
                        retr = Some((fvalue, value));
                    }
                    ModResult::Done(_, None, _) => {}
                    _ => {
                        trace!("Sentinal not placed");
                    }
                }
            }
            let modify_chunk = if copying { &new_chunk } else { &old_chunk };
            let res = self.modify_entry(
                &*modify_chunk,
                hash,
                key,
                fkey,
                ModOp::Tombstone,
                None,
                &guard,
            );
            match res {
                ModResult::Replaced(fvalue, value, _) => {
                    retr = Some((fvalue, value));
                    self.count.fetch_sub(1, Relaxed);
                }
                ModResult::Done(_, _, _) => unreachable!("Remove shall not have done"),
                ModResult::NotFound => {}
                ModResult::Sentinel => {
                    backoff.spin();
                    continue;
                }
                ModResult::TableFull => unreachable!("TableFull on remove is not possible"),
                _ => {}
            };
            if self.epoch_changed(epoch) {
                if retr.is_none() {
                    return self.remove(key, fkey);
                }
            }
            return retr;
        }
    }

    pub fn len(&self) -> usize {
        self.count.load(Relaxed)
    }

    fn get_from_chunk(
        &self,
        chunk: &Chunk<K, V, A, ALLOC>,
        hash: usize,
        key: &K,
        fkey: usize,
        migrating: Option<&ChunkPtr<K, V, A, ALLOC>>,
    ) -> (Value, usize, usize) {
        debug_assert_ne!(chunk as *const Chunk<K, V, A, ALLOC> as usize, 0);
        let mut idx = hash;
        let cap = chunk.capacity;
        let base = chunk.base;
        let cap_mask = chunk.cap_mask();
        let mut counter = 0;
        while counter < cap {
            idx &= cap_mask;
            let addr = base + idx * ENTRY_SIZE;
            let k = self.get_fast_key(addr);
            if k == fkey && chunk.attachment.probe(idx, key) {
                let val_res = self.get_fast_value(addr);
                match val_res.parsed {
                    ParsedValue::Empty => {}
                    _ => return (val_res, idx, addr),
                }
            } else if k == EMPTY_KEY {
                return (Value::new::<K, V, A, ALLOC, H>(0), 0, addr);
            } else if let Some(new_chunk_ins) = migrating {
                debug_assert!(new_chunk_ins.base != chunk.base);
                let val_res = self.get_fast_value(addr);
                if let &ParsedValue::Val(_) = &val_res.parsed {
                    self.migrate_entry(k, idx, val_res, chunk, new_chunk_ins, addr, &mut 0);
                }
            }
            idx += 1; // reprobe
            counter += 1;
        }

        // not found
        return (Value::new::<K, V, A, ALLOC, H>(0), 0, 0);
    }

    #[inline(always)]
    fn modify_entry<'a>(
        &self,
        chunk: &'a Chunk<K, V, A, ALLOC>,
        hash: usize,
        key: &K,
        fkey: usize,
        op: ModOp<V>,
        migration_chunk: Option<&ChunkPtr<K, V, A, ALLOC>>,
        _guard: &'a Guard,
    ) -> ModResult<V> {
        let cap = chunk.capacity;
        let base = chunk.base;
        let mut idx = hash;
        let mut count = 0;
        let cap_mask = chunk.cap_mask();
        let backoff = crossbeam_utils::Backoff::new();
        while count <= cap {
            idx &= cap_mask;
            let addr = base + idx * ENTRY_SIZE;
            let k = self.get_fast_key(addr);
            let v = self.get_fast_value(addr);
            {
                // Early exit upon sentinel discovery
                match v.parsed {
                    ParsedValue::Sentinel => match &op {
                        &ModOp::Sentinel => {
                            // Sentinel op is allowed on old chunk
                        }
                        _ => {
                            // Confirmed, this is possible
                            return ModResult::Sentinel;
                        }
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
                                    if *v == 0 {
                                        return ModResult::Done(addr, None, idx);
                                    } else {
                                        return ModResult::Done(*v, Some(value), idx);
                                    }
                                } else {
                                    return ModResult::Fail;
                                }
                            }
                            &ModOp::Tombstone => {
                                if *v == 0 {
                                    // Already tombstone
                                    return ModResult::NotFound;
                                }
                                if !self.cas_tombstone(addr, val.raw).1 {
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
                                if self.cas_value(addr, val.raw, *fv).1 {
                                    let (_, value) = chunk.attachment.get(idx);
                                    if *v == 0 {
                                        return ModResult::Done(addr, None, idx);
                                    } else {
                                        return ModResult::Replaced(*v, value, idx);
                                    }
                                } else {
                                    trace!("Cannot upsert fast value in place for {}", fkey);
                                    return ModResult::Fail;
                                }
                            }
                            &ModOp::AttemptInsert(fval, oval) => {
                                if *v == 0 {
                                    let primed_fval = if Self::can_attach() {
                                        fval | INV_VAL_BIT_MASK
                                    } else {
                                        fval
                                    };
                                    let (act_val, replaced) =
                                        self.cas_value(addr, val.raw, primed_fval);
                                    if replaced {
                                        let (_, prev_val) = chunk.attachment.get(idx);
                                        if Self::can_attach() {
                                            chunk.attachment.set(idx, key.clone(), (*oval).clone());
                                            let stripped_prime =
                                                self.cas_value(addr, primed_fval, fval).1;
                                            debug_assert!(stripped_prime);
                                        }
                                        return ModResult::Replaced(val.raw, prev_val, idx);
                                    } else {
                                        let (_, value) = chunk.attachment.get(idx);
                                        return ModResult::Existed(act_val, value);
                                    }
                                } else {
                                    trace!(
                                        "Attempting insert existed entry {}, {}, have key {:?}, skip",
                                        k,
                                        fval,
                                        v
                                    );
                                    let (_, value) = chunk.attachment.get(idx);
                                    return ModResult::Existed(*v, value);
                                }
                            }
                            &ModOp::SwapFastVal(ref swap) => {
                                trace!(
                                    "Swaping found key {} have original value {:#064b}",
                                    fkey,
                                    val.raw
                                );
                                match &val.parsed {
                                    ParsedValue::Val(pval) => {
                                        let pval = *pval;
                                        if pval == 0 {
                                            return ModResult::NotFound;
                                        }
                                        let aval = chunk.attachment.get(idx).1;
                                        if let Some(v) = swap(pval) {
                                            if self.cas_value(addr, pval, v).1 {
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
                                let primed_fval = if Self::can_attach() {
                                    fval | INV_VAL_BIT_MASK
                                } else {
                                    fval
                                };
                                if self.cas_value(addr, val.raw, primed_fval).1 {
                                    let (_, prev_val) = chunk.attachment.get(idx);
                                    if Self::can_attach() {
                                        chunk.attachment.set(idx, key.clone(), (*v).clone());
                                        let stripped_prime =
                                            self.cas_value(addr, primed_fval, fval).1;
                                        debug_assert!(stripped_prime);
                                    }
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
                        if self.cas_value(addr, EMPTY_VALUE, fval).1 {
                            // CAS value succeed, shall store key
                            chunk.attachment.set(idx, key.clone(), (*val).clone());
                            unsafe { intrinsics::atomic_store_rel(addr as *mut usize, fkey) }
                            return ModResult::Done(addr, None, idx);
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
                        if self.cas_value(addr, EMPTY_VALUE, fval).1 {
                            unsafe { intrinsics::atomic_store_rel(addr as *mut usize, fkey) }
                            return ModResult::Done(addr, None, idx);
                        } else {
                            backoff.spin();
                            continue;
                        }
                    }
                    ModOp::Sentinel => {
                        if self.cas_sentinel(addr, 0) {
                            // CAS value succeed, shall store key
                            unsafe { intrinsics::atomic_store_rel(addr as *mut usize, fkey) }
                            return ModResult::Done(addr, None, idx);
                        } else {
                            backoff.spin();
                            continue;
                        }
                    }
                    ModOp::Tombstone => return ModResult::Fail,
                    ModOp::SwapFastVal(_) => return ModResult::NotFound,
                };
            } else if let (Some(migration_chunk), &ParsedValue::Val(_)) =
                (migration_chunk, &v.parsed)
            {
                self.migrate_entry(k, idx, v, chunk, migration_chunk, addr, &mut 0);
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
        let cap = chunk.capacity;
        let base = chunk.base;
        let mut counter = 0;
        let mut res = Vec::with_capacity(chunk.occupation.load(Relaxed));
        let cap_mask = chunk.cap_mask();
        while counter < cap {
            idx &= cap_mask;
            let addr = base + idx * ENTRY_SIZE;
            let k = self.get_fast_key(addr);
            if k != EMPTY_KEY {
                let val_res = self.get_fast_value(addr);
                match val_res.parsed {
                    ParsedValue::Val(0) => {}
                    ParsedValue::Val(v) => {
                        let (key, value) = chunk.attachment.get(idx);
                        res.push((k, v, key, value))
                    }
                    ParsedValue::Prime(_) => {
                        continue;
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
    fn cas_tombstone(&self, entry_addr: usize, original: usize) -> (usize, bool) {
        debug_assert!(entry_addr > 0);
        self.cas_value(entry_addr, original, TOMBSTONE_VALUE)
    }
    #[inline(always)]
    fn cas_value(&self, entry_addr: usize, original: usize, value: usize) -> (usize, bool) {
        debug_assert!(entry_addr > 0);
        debug_assert_ne!(value & VAL_BIT_MASK, SENTINEL_VALUE);
        let addr = entry_addr + mem::size_of::<usize>();
        unsafe { intrinsics::atomic_cxchg_acqrel(addr as *mut usize, original, value) }
    }
    #[inline(always)]
    fn cas_sentinel(&self, entry_addr: usize, original: usize) -> bool {
        if cfg!(debug_assert) {
            assert!(entry_addr > 0);
            let guard = crossbeam_epoch::pin();
            assert!(Self::is_copying(self.epoch.load(Acquire)));
            assert!(!self.new_chunk.load(Acquire, &guard).is_null());
            let chunk = self.chunk.load(Acquire, &guard);
            let chunk_ref = unsafe { chunk.deref() };
            assert!(entry_addr >= chunk_ref.base);
            assert!(entry_addr < chunk_ref.base + chunk_ref.total_size);
        }
        let addr = entry_addr + mem::size_of::<usize>();
        let (val, done) = unsafe {
            intrinsics::atomic_cxchg_acqrel(addr as *mut usize, original, SENTINEL_VALUE)
        };
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
        self.do_migration(old_chunk_ptr, guard)
    }

    fn do_migration<'a>(
        &self,
        old_chunk_ptr: Shared<'a, ChunkPtr<K, V, A, ALLOC>>,
        guard: &crossbeam_epoch::Guard,
    ) -> ResizeResult {
        let epoch = self.now_epoch();
        let old_chunk_ins = unsafe { old_chunk_ptr.deref() };
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
        dfence();
        if self.chunk.load(Acquire, guard) != old_chunk_ptr {
            warn!(
                "Give up on resize due to old chunk changed after lock obtained, epoch {} to {}",
                epoch,
                self.now_epoch()
            );
            self.new_chunk.store(Shared::null(), Release);
            dfence();
            debug_assert_eq!(self.now_epoch() % 2, 0);
            return ResizeResult::ChunkChanged;
        }
        debug!("Resizing {:?}", old_chunk_ptr);
        let new_chunk_ptr =
            Owned::new(ChunkPtr::new(Chunk::alloc_chunk(new_cap))).into_shared(guard);
        let new_chunk_ins = unsafe { new_chunk_ptr.deref() };
        debug_assert_ne!(new_chunk_ptr, old_chunk_ptr);
        self.new_chunk.store(new_chunk_ptr, Release); // Stump becasue we have the lock already
        dfence();
        let prev_epoch = self.epoch.fetch_add(1, AcqRel); // Increase epoch by one
        debug_assert_eq!(prev_epoch % 2, 0);
        dfence();
        // Migrate entries
        self.migrate_entries(old_chunk_ins, new_chunk_ins, guard);
        // Assertion check
        debug_assert_ne!(old_chunk_ins.ptr as usize, new_chunk_ins.base);
        debug_assert_ne!(old_chunk_ins.ptr, unsafe { new_chunk_ptr.deref().ptr });
        debug_assert!(!new_chunk_ptr.is_null());
        dfence();
        let prev_epoch = self.epoch.fetch_add(1, AcqRel); // Increase epoch by one
        debug_assert_eq!(prev_epoch % 2, 1);
        dfence();
        self.chunk.store(new_chunk_ptr, Release);
        self.timestamp.store(timestamp(), Release);
        dfence();
        unsafe {
            guard.defer_destroy(old_chunk_ptr);
            guard.flush();
        }
        self.new_chunk.store(Shared::null(), Release);
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
        _guard: &crossbeam_epoch::Guard,
    ) -> usize {
        let mut old_address = old_chunk_ins.base as usize;
        let boundary = old_address + chunk_size_of(old_chunk_ins.capacity);
        let mut effective_copy = 0;
        let mut idx = 0;
        let backoff = crossbeam_utils::Backoff::new();
        while old_address < boundary {
            // iterate the old chunk to extract entries that is NOT empty
            let fvalue = self.get_fast_value(old_address);
            let fkey = self.get_fast_key(old_address);
            // Reasoning value states
            match &fvalue.parsed {
                ParsedValue::Empty | ParsedValue::Val(0) => {
                    if !self.cas_sentinel(old_address, fvalue.raw) {
                        warn!("Filling empty with sentinel for old table should succeed but not, retry");
                        backoff.spin();
                        continue;
                    }
                }
                ParsedValue::Val(_) => {
                    if !self.migrate_entry(
                        fkey,
                        idx,
                        fvalue,
                        old_chunk_ins,
                        new_chunk_ins,
                        old_address,
                        &mut effective_copy,
                    ) {
                        continue;
                    }
                }
                ParsedValue::Prime(_) => {
                    unreachable!("Shall not have prime in old table");
                }
                ParsedValue::Sentinel => {
                    // Sentinel, skip
                    // Sentinel in old chunk implies its new value have already in the new chunk
                    // It can also be other thread have moved this key-value pair to the new chunk
                    trace!("Skip copy sentinel");
                }
            }
            old_address += ENTRY_SIZE;
            idx += 1;
            dfence();
        }
        // resize finished, make changes on the numbers
        debug!("Migrated {} entries to new chunk", effective_copy);
        new_chunk_ins.occupation.fetch_add(effective_copy, Relaxed);
        return effective_copy;
    }

    #[inline(always)]
    fn migrate_entry(
        &self,
        fkey: usize,
        old_idx: usize,
        fvalue: Value,
        old_chunk_ins: &Chunk<K, V, A, ALLOC>,
        new_chunk_ins: &Chunk<K, V, A, ALLOC>,
        old_address: usize,
        effective_copy: &mut usize,
    ) -> bool {
        debug_assert_ne!(old_chunk_ins.base, new_chunk_ins.base);
        if fkey == EMPTY_KEY {
            // Value have no key, insertion in progress
            return false;
        }
        // Insert entry into new chunk, in case of failure, skip this entry
        // Value should be primed
        debug_assert_ne!(fvalue.raw & VAL_BIT_MASK, SENTINEL_VALUE);
        let (key, value) = old_chunk_ins.attachment.get(old_idx);
        let inserted_addr = {
            // Make insertion for migration inlined, hopefully the ordering will be right
            let cap = new_chunk_ins.capacity;
            let base = new_chunk_ins.base;
            let mut idx = hash::<H>(fkey);
            let cap_mask = new_chunk_ins.cap_mask();
            let mut count = 0;
            let mut res = None;
            while count < cap {
                idx &= cap_mask;
                let addr = base + idx * ENTRY_SIZE;
                let k = self.get_fast_key(addr);
                if k == fkey && new_chunk_ins.attachment.probe(idx, &key) {
                    // New value existed, skip with None result
                    break;
                } else if k == EMPTY_KEY {
                    // Try insert to this slot
                    let (val, done) = self.cas_value(addr, EMPTY_VALUE, fvalue.raw);
                    debug_assert_ne!(val & VAL_BIT_MASK, SENTINEL_VALUE);
                    if done {
                        new_chunk_ins.attachment.set(idx, key, value);
                        unsafe { intrinsics::atomic_store_rel(addr as *mut usize, fkey) }
                        res = Some(addr);
                        break;
                    }
                }
                idx += 1; // reprobe
                count += 1;
            }
            res
        };
        // CAS to ensure sentinel into old chunk (spec)
        // Use CAS for old threads may working on this one
        dfence(); // fence to ensure sentinel appears righr after pair copied to new chunk
        trace!("Copied key {} to new chunk", fkey);
        if self.cas_sentinel(old_address, fvalue.raw) {
            dfence();
            if let Some(_new_entry_addr) = inserted_addr {
                old_chunk_ins.attachment.erase(old_idx);
                *effective_copy += 1;
                return true;
            }
        }
        false
    }

    pub fn map_is_copying(&self) -> bool {
        Self::is_copying(self.now_epoch())
    }

    #[inline(always)]
    fn can_attach() -> bool {
        can_attach::<K, V, A>()
    }
}

impl Value {
    pub fn new<K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default, H: Hasher + Default>(
        val: usize,
    ) -> Self {
        let res = {
            if val == EMPTY_VALUE {
                ParsedValue::Empty
            } else if val == TOMBSTONE_VALUE {
                ParsedValue::Val(0)
            } else {
                let actual_val = val & VAL_BIT_MASK;
                let flag = val & INV_VAL_BIT_MASK;
                if flag != 0 {
                    ParsedValue::Prime(actual_val)
                } else if actual_val == SENTINEL_VALUE {
                    ParsedValue::Sentinel
                } else if actual_val == TOMBSTONE_VALUE {
                    unreachable!("");
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
            init_cap: self.init_cap,
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
            guard.flush();
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
    (cap as f64 * 0.75f64) as usize
}

#[inline(always)]
fn chunk_size_of(cap: usize) -> usize {
    cap * ENTRY_SIZE
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

#[inline(always)]
fn dfence() {
    compiler_fence(SeqCst);
    fence(SeqCst);
}

const fn can_attach<K, V, A: Attachment<K, V>>() -> bool {
    mem::size_of::<(K, V)>() != 0
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
    fn clear(&self);
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

    fn clear(&self) {
        self.table.clear();
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

    fn clear(&self) {
        self.table.clear();
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
            .map(|(v, _)| v - NUM_FIX)
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

    fn clear(&self) {
        self.table.clear();
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
        match table.insert(
            InsertOp::TryInsert,
            &(),
            Some(()),
            key,
            value | MUTEX_BIT_MASK,
        ) {
            None | Some((TOMBSTONE_VALUE, ())) | Some((EMPTY_VALUE, ())) => {
                trace!("Created locked key {}", key);
                Some(Self { table, key, value })
            }
            _ => {
                trace!("Cannot create locked key {} ", key);
                None
            }
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
                    let locked_val = fast_value | MUTEX_BIT_MASK;
                    if fast_value == locked_val {
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
                        Some(locked_val)
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
    use chashmap::CHashMap;
    use rayon::prelude::*;
    use std::collections::HashMap;
    use std::thread;
    use std::{
        alloc::System,
        sync::{Mutex, RwLock},
    };
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
        let num_threads = num_cpus::get();
        let test_load = 4096;
        let repeat_load = 16;
        let map = Arc::new(WordMap::<System>::with_capacity(32));
        let mut threads = vec![];
        for i in 0..num_threads {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                for j in 5..test_load {
                    let key = i * 10000000 + j;
                    let value_prefix = i * j * 100;
                    for k in 1..repeat_load {
                        let value = value_prefix + k;
                        if k != 1 {
                            assert_eq!(map.get(&key), Some(value - 1));
                        }
                        let pre_insert_epoch = map.table.now_epoch();
                        map.insert(&key, value);
                        let post_insert_epoch = map.table.now_epoch();
                        for l in 1..128 {
                            let pre_fail_get_epoch = map.table.now_epoch();
                            let left = map.get(&key);
                            let post_fail_get_epoch = map.table.now_epoch();
                            let right = Some(value);
                            if left != right {
                                for m in 1..1024 {
                                    let left = map.get(&key);
                                    let right = Some(value);
                                    if left == right {
                                        panic!(
                                            "Recovered at turn {} for {}, copying {}, epoch {} to {}, now {}, PIE: {} to {}. Migration problem!!!", 
                                            m, 
                                            key, 
                                            map.table.map_is_copying(),
                                            pre_fail_get_epoch,
                                            post_fail_get_epoch,
                                            map.table.now_epoch(),
                                            pre_insert_epoch, 
                                            post_insert_epoch
                                        );
                                    }
                                }
                                panic!("Unable to recover for {}, round {}, copying {}", key, l , map.table.map_is_copying());
                            }
                        }
                        if j % 7 == 0 {
                            assert_eq!(
                                map.remove(&key),
                                Some(value),
                                "Remove result, get {:?}, copying {}, round {}",
                                map.get(&key),
                                map.table.map_is_copying(),
                                k
                            );
                            assert_eq!(map.get(&key), None, "Remove recursion");
                            assert!(map.lock(key).is_none(), "Remove recursion with lock");
                            map.insert(&key, value);
                        }
                        if j % 3 == 0 {
                            let new_value = value + 7;
                            let pre_insert_epoch = map.table.now_epoch();
                            map.insert(&key, new_value);
                            let post_insert_epoch = map.table.now_epoch();
                            assert_eq!(
                                map.get(&key), 
                                Some(new_value), 
                                "Checking immediate update, key {}, epoch {} to {}",
                                key, pre_insert_epoch, post_insert_epoch
                            );
                            map.insert(&key, value);
                        }
                    }
                }
            }));
        }
        info!("Waiting for intensive insertion to finish");
        for thread in threads {
            let _ = thread.join();
        }
        info!("Checking final value");
        (0..num_threads)
            .collect::<Vec<_>>()
            .par_iter()
            .for_each(|i| {
                for j in 5..test_load {
                    let k = i * 10000000 + j;
                    let value = i * j * 100 + repeat_load - 1;
                    let get_res = map.get(&k);
                    assert_eq!(
                        get_res,
                        Some(value),
                        "New k {}, i {}, j {}, epoch {}",
                        k,
                        i,
                        j,
                        map.table.now_epoch()
                    );
                }
            });
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
    fn parallel_word_map_multi_mutex() {
        let _ = env_logger::try_init();
        let map = Arc::new(WordMap::<System>::with_capacity(4));
        let mut threads = vec![];
        let num_threads = num_cpus::get();
        let test_load = 4096;
        let update_load = 128;
        for thread_id in 0..num_threads {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                let target = thread_id;
                for i in 0..test_load {
                    let key = target * 1000000 + i;
                    {
                        let mut mutex = map.try_insert_locked(key).unwrap();
                        *mutex = 1;
                    }
                    for j in 1..update_load {
                        assert!(
                            map.get(&key).is_some(),
                            "Pre getting value for mutex, key {}, epoch {}",
                            key,
                            map.table.now_epoch()
                        );
                        let val = {
                            let mut mutex = map.lock(key).expect(&format!(
                                "Locking key {}, copying {}",
                                key,
                                map.table.now_epoch()
                            ));
                            assert_eq!(*mutex, j);
                            *mutex += 1;
                            *mutex
                        };
                        assert!(
                            map.get(&key).is_some(),
                            "Post getting value for mutex, key {}, epoch {}",
                            key,
                            map.table.now_epoch()
                        );
                        if j % 7 == 0 {
                            {
                                let mutex = map.lock(key).expect(&format!(
                                    "Remove locking key {}, copying {}",
                                    key,
                                    map.table.now_epoch()
                                ));
                                mutex.remove();
                            }
                            assert!(map.lock(key).is_none());
                            *map.try_insert_locked(key).unwrap() = val;
                        }
                    }
                    assert_eq!(*map.lock(key).unwrap(), update_load);
                }
            }));
        }
        for thread in threads {
            let _ = thread.join();
        }
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

    use std::thread::JoinHandle;
    #[test]
    fn atomic_ordering() {
        let test_load = 102400;
        let epoch = Arc::new(AtomicUsize::new(0));
        let old_ptr = Arc::new(AtomicUsize::new(0));
        let new_ptr = Arc::new(AtomicUsize::new(0));
        let write = || -> JoinHandle<()> {
            let epoch = epoch.clone();
            let old_ptr = old_ptr.clone();
            let new_ptr = new_ptr.clone();
            thread::spawn(move || {
                for _ in 0..test_load {
                    let old = old_ptr.load(Acquire);
                    if new_ptr.compare_and_swap(0, old, AcqRel) != 0 {
                        return;
                    }
                    dfence();
                    if old_ptr.load(Acquire) != old {
                        new_ptr.store(0, Release);
                        dfence();
                        return;
                    }
                    let new = old + 1;
                    new_ptr.store(new, Release);
                    dfence();
                    assert_eq!(epoch.fetch_add(1, AcqRel) % 2, 0);
                    // Do something
                    for _ in 0..1000 {
                        std::sync::atomic::spin_loop_hint();
                    }
                    old_ptr.store(new, Release);
                    dfence();
                    assert_eq!(epoch.fetch_add(1, AcqRel) % 2, 1);
                    dfence();
                    new_ptr.store(0, Release);
                }
            })
        };
        let read = || -> JoinHandle<()> {
            let epoch = epoch.clone();
            let old_ptr = old_ptr.clone();
            let new_ptr = new_ptr.clone();
            thread::spawn(move || {
                for _ in 0..test_load {
                    let epoch_val = epoch.load(Acquire);
                    let old = old_ptr.load(Acquire);
                    let new = new_ptr.load(Acquire);
                    let changing = epoch_val % 2 == 1;
                    for _ in 0..500 {
                        std::sync::atomic::spin_loop_hint();
                    }
                    if changing && epoch.load(Acquire) == epoch_val {
                        assert_ne!(old, new);
                        assert_ne!(new, 0);
                    }
                }
            })
        };
        let num_writers = 5;
        let mut writers = Vec::with_capacity(num_writers);
        for _ in 0..num_writers {
            writers.push(write());
        }
        let num_readers = num_cpus::get();
        let mut readers = Vec::with_capacity(num_readers);
        for _ in 0..num_readers {
            readers.push(read());
        }
        for reader in readers {
            reader.join().unwrap();
        }
        for writer in writers {
            writer.join().unwrap();
        }
    }

    #[test]
    fn insert_with_num_fixes() {
        let map = WordMap::<System, DefaultHasher>::with_capacity(32);
        assert_eq!(map.insert(&24, 0), None);   
        assert_eq!(map.insert(&24, 1), Some(0));   
        assert_eq!(map.insert(&0, 0), None);
        assert_eq!(map.insert(&0, 1), Some(0))
    }

    #[bench]
    fn lfmap(b: &mut Bencher) {
        let _ = env_logger::try_init();
        let map = WordMap::<System, DefaultHasher>::with_capacity(8);
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
    fn mutex_hashmap(b: &mut Bencher) {
        let _ = env_logger::try_init();
        let map = Mutex::new(HashMap::new());
        let mut i = 5;
        b.iter(|| {
            map.lock().unwrap().insert(i, i);
            i += 1;
        });
    }

    #[bench]
    fn rwlock_hashmap(b: &mut Bencher) {
        let _ = env_logger::try_init();
        let map = RwLock::new(HashMap::new());
        let mut i = 5;
        b.iter(|| {
            map.write().unwrap().insert(i, i);
            i += 1;
        });
    }

    #[bench]
    fn chashmap(b: &mut Bencher) {
        let _ = env_logger::try_init();
        let map = CHashMap::new();
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
