// usize to usize lock-free, wait free table
use crate::align_padding;
use alloc::vec::Vec;
use num::cast::AsPrimitive;
use num::traits::WrappingAdd;
use core::alloc::{GlobalAlloc, Layout};
use core::hash::Hasher;
use core::marker::PhantomData;
use core::ops::Deref;
use core::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed, Release, SeqCst};
use core::sync::atomic::{compiler_fence, fence, AtomicUsize};
use core::{intrinsics, mem, ptr};
use crossbeam_epoch::*;
use crossbeam_utils::Backoff;
use std::alloc::System;
use std::collections::hash_map::DefaultHasher;
use std::fmt::{Debug, Binary};
use std::hash::Hash;
use std::ops::*;
use std::fmt::Display;
use std::os::raw::c_void;
use num::*;


struct Consts<FK: Atom, FV: Atom> {
    _marker: PhantomData<(FK, FV)>
}

impl <FK: Atom, FV: Atom> Consts<FK, FV> {

    fn empty_key() -> FK {
        FK::atom_from_usize(0)
    }

    fn tombstone_value() -> FV {
        FV::atom_from_usize(1)
    }

    fn locked_value() -> FV {
        FV::atom_from_usize(2)
    }

    fn migrating_value() -> FV {
        FV::atom_from_usize(3)
    }

    fn sentinel_value() -> FV {
        FV::atom_from_usize(4)
    }

    fn val_two() -> FV {
        FV::atom_from_usize(2)
    }

    fn val_bit_mask() -> FV {
        !FV::zero() << FV::one() >> FV::one()
    }

    fn inv_val_bit_mask() -> FV {
        !Self::val_bit_mask()
    }

    const INV_VAL_BIT_MASK: FV = !Self::VAL_BIT_MASK;
    const WORD_MUTEX_DATA_BIT_MASK: FV = !FV::zero() << Self::VAL_TWO >> Self::VAL_TWO;
    const MUTEX_BIT_MASK: FV = !Self::WORD_MUTEX_DATA_BIT_MASK & Self::VAL_BIT_MASK;

    const FVAL_BITS: usize = mem::size_of::<Self>() * 8;
    const FVAL_VER_POS: FV = FV::atom_from_usize(Self::FVAL_BITS) / Self::VAL_TWO;
    const FVAL_VER_BIT_MASK: FV = !FV::zero() << Self::FVAL_VER_POS & Self::VAL_BIT_MASK;
    const FVAL_VAL_BIT_MASK: FV = !Self::FVAL_VER_BIT_MASK;
    const ENTRY_SIZE: usize = mem::size_of::<(FK, FV)>();

    const NUM_FIX_K: FK = FK::atom_from_usize(5);
    const NUM_FIX_V: FV = FV::atom_from_usize(5);
    const PLACEHOLDER_VAL: FV = Self::NUM_FIX_V + FV::one();
}

enum ModResult<FV: Atom, V> {
    Replaced(FV, Option<V>, usize), // (origin fval, val, index)
    Existed(FV, Option<V>),
    Fail,
    Sentinel,
    NotFound,
    Done(FV, Option<V>, usize), // _, value, index
    TableFull,
    Aborted,
}

enum ModOp<'a, FV: Atom, V> {
    Insert(FV, &'a V),
    UpsertFastVal(FV),
    AttemptInsert(FV, &'a V),
    SwapFastVal(Box<dyn Fn(FV) -> Option<FV>>),
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
    Done,
}

pub enum SwapResult<'a, FK: Atom, FV: Atom, K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> {
    Succeed(FV, usize, Shared<'a, ChunkPtr<FK, FV, K, V, A, ALLOC>>),
    NotFound,
    Failed,
    Aborted,
}

pub struct Chunk<FK: Atom, FV: Atom, K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> {
    capacity: usize,
    base: usize,
    occu_limit: usize,
    occupation: AtomicUsize,
    empty_entries: AtomicUsize,
    total_size: usize,
    attachment: A,
    shadow: PhantomData<(FK, FV, K, V, ALLOC)>,
}

pub struct ChunkPtr<FK: Atom, FV: Atom, K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> {
    ptr: *mut Chunk<FK, FV, K, V, A, ALLOC>,
}

pub struct Table<FK: Atom, FV: Atom, K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default, H: Hasher + Default> {
    new_chunk: Atomic<ChunkPtr<FK, FV, K, V, A, ALLOC>>,
    chunk: Atomic<ChunkPtr<FK, FV, K, V, A, ALLOC>>,
    count: AtomicUsize,
    epoch: AtomicUsize,
    init_cap: usize,
    mark: PhantomData<H>,
}

macro_rules! gen_const{
    ($name: ident, $t: ident, $k: ident, $v: ident) => {
        const $name: $t = Consts::<$k, $v>::$name;
    };
}

// macro_rules! gen_consts {
//     ($k: ident, $v: ident) => {
//         gen_const!(EMPTY_KEY, $k, $k, $v);

//         gen_const!(EMPTY_VALUE, $v, $k, $v);
//         gen_const!(TOMBSTONE_VALUE, $v, $k, $v);
//         gen_const!(LOCKED_VALUE, $v, $k, $v);
//         gen_const!(MIGRATING_VALUE, $v, $k, $v);
//         gen_const!(SENTINEL_VALUE, $v, $k, $v);
//         gen_const!(VAL_BIT_MASK, $v, $k, $v);
//         gen_const!(INV_VAL_BIT_MASK, $v, $k, $v);
//         gen_const!(WORD_MUTEX_DATA_BIT_MASK, $v, $k, $v);
//         gen_const!(MUTEX_BIT_MASK, $v, $k, $v);
//         gen_const!(FVAL_VER_POS, $v, $k, $v);
//         gen_const!(FVAL_VER_BIT_MASK, $v, $k, $v);
//         gen_const!(FVAL_VAL_BIT_MASK, $v, $k, $v);
//         gen_const!(NUM_FIX_K, $k, $k, $v);
//         gen_const!(NUM_FIX_V, $v, $k, $v);
//         gen_const!(PLACEHOLDER_VAL, $v, $k, $v);
//         gen_const!(ENTRY_SIZE, usize, $k, $v);
        
//     };
// }

impl<
        FK: Atom, FV: Atom,
        K: Clone + Hash + Eq,
        V: Clone,
        A: Attachment<K, V>,
        ALLOC: GlobalAlloc + Default,
        H: Hasher + Default,
    > Table<FK, FV, K, V, A, ALLOC, H>
{
    gen_consts!(FK, FV);
    const CAN_ATTACH: bool = can_attach::<K, V>();

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
            init_cap: cap,
            mark: PhantomData,
        }
    }

    pub fn new() -> Self {
        Self::with_capacity(64)
    }

    pub fn get(&self, key: &K, fkey: FK, read_attachment: bool) -> Option<(FV, Option<V>)> {
        let guard = crossbeam_epoch::pin();
        let backoff = crossbeam_utils::Backoff::new();
        let hash = Self::hash(fkey);
        'OUTER: loop {
            let epoch = self.now_epoch();
            let chunk_ptr = self.chunk.load(Acquire, &guard);
            let new_chunk_ptr = self.new_chunk.load(Acquire, &guard);
            let chunk = unsafe { chunk_ptr.deref() };
            let new_chunk = Self::new_chunk_ref(epoch, &new_chunk_ptr, &chunk_ptr);
            debug_assert!(!chunk_ptr.is_null());

            if let Some((mut val, _idx, addr, aitem)) =
                self.get_from_chunk(&*chunk, hash, key, fkey, &backoff)
            {
                'SPIN: loop {
                    let act_val = val.act_val();
                    match act_val {
                        Self::EMPTY_VALUE | Self::TOMBSTONE_VALUE => {
                            trace!("Found empty for key {}", fkey);
                            break 'SPIN;
                        }
                        Self::SENTINEL_VALUE => {
                            if new_chunk.is_none() {
                                warn!("Discovered sentinel but new chunk is null for key {}", fkey);
                                backoff.spin();
                                continue 'OUTER;
                            }
                            trace!("Found sentinel, moving to new chunk for key {}", fkey);
                            break 'SPIN;
                        }
                        Self::LOCKED_VALUE | Self::MIGRATING_VALUE => {
                            backoff.spin();
                            val = Self::get_fast_value(addr);
                            continue 'SPIN;
                        }
                        _ => {
                            let mut attachment = None;
                            if Self::CAN_ATTACH && read_attachment {
                                attachment = Some(aitem.get_value());
                                let new_val = Self::get_fast_value(addr);
                                if new_val.val != val.val {
                                    val = new_val;
                                    continue 'SPIN;
                                }
                            }
                            return Some((act_val, attachment));
                        }
                    }
                }
            }

            // Looking into new chunk
            if let Some(new_chunk) = new_chunk {
                if let Some((mut val, _idx, addr, aitem)) =
                    self.get_from_chunk(&*new_chunk, hash, key, fkey, &backoff)
                {
                    'SPIN_NEW: loop {
                        let act_val = val.act_val();
                        match act_val {
                            Self::EMPTY_VALUE | Self::TOMBSTONE_VALUE => {
                                break 'SPIN_NEW;
                            }
                            Self::LOCKED_VALUE | Self::MIGRATING_VALUE => {
                                backoff.spin();
                                val = Self::get_fast_value(addr);
                                continue 'SPIN_NEW;
                            }
                            SENTINEL_VALUE => {
                                warn!("Found sentinel in new chunks for key {}", fkey);
                                backoff.spin();
                                continue 'OUTER;
                            }
                            _ => {
                                let mut attachment = None;
                                if Self::CAN_ATTACH && read_attachment {
                                    attachment = Some(aitem.get_value());
                                    let new_val = Self::get_fast_value(addr);
                                    if new_val.val != val.val {
                                        val = new_val;
                                        continue 'SPIN_NEW;
                                    }
                                }
                                return Some((act_val, attachment));
                            }
                        }
                    }
                }
            }
            let new_epoch = self.now_epoch();
            if new_epoch != epoch {
                backoff.spin();
                continue 'OUTER;
            }
            trace!(
                "Find nothing for key {}, rt new chunk {:?}, now {:?}. Epoch {} to {}",
                fkey,
                new_chunk_ptr,
                self.new_chunk.load(Acquire, &guard),
                epoch,
                new_epoch
            );
            return None;
        }
    }

    pub fn insert(
        &self,
        op: InsertOp,
        key: &K,
        value: Option<&V>,
        fkey: FK,
        fvalue: FV,
    ) -> Option<(FV, V)> {
        let backoff = crossbeam_utils::Backoff::new();
        let guard = crossbeam_epoch::pin();
        let hash = Self::hash(fkey);
        loop {
            let epoch = self.now_epoch();
            // trace!("Inserting key: {}, value: {}", fkey, fvalue);
            let chunk_ptr = self.chunk.load(Acquire, &guard);
            let new_chunk_ptr = self.new_chunk.load(Acquire, &guard);
            let new_chunk = Self::new_chunk_ref(epoch, &new_chunk_ptr, &chunk_ptr);
            if new_chunk.is_none() {
                match self.check_migration(chunk_ptr, &guard) {
                    ResizeResult::Done | ResizeResult::SwapFailed => {
                        trace!("Retry insert due to resize");
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
            let masked_value = fvalue & Self::VAL_BIT_MASK;
            let mod_op = match op {
                InsertOp::Insert => ModOp::Insert(masked_value, value.unwrap()),
                InsertOp::UpsertFast => ModOp::UpsertFastVal(masked_value),
                InsertOp::TryInsert => ModOp::AttemptInsert(masked_value, value.unwrap()),
            };
            let value_insertion =
                self.modify_entry(&*modify_chunk, hash, key, fkey, mod_op, true, &guard);
            let mut result = None;
            match value_insertion {
                ModResult::Done(_, _, _) => {
                    modify_chunk.occupation.fetch_add(1, Relaxed);
                    self.count.fetch_add(1, Relaxed);
                }
                ModResult::Replaced(fv, v, _) | ModResult::Existed(fv, v) => {
                    result = Some((fv, v.unwrap()))
                }
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
                let old_sent =
                    self.modify_entry(chunk, hash, key, fkey, ModOp::Sentinel, false, &guard);
                trace!("Put sentinel to old chunk for {} got {:?}", fkey, old_sent);
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
            self.chunk.store(owned_new.into_shared(&guard), Release);
            self.new_chunk.store(Shared::null(), Release);
            dfence();
            self.count.fetch_sub(len, AcqRel);
            break;
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

    pub fn swap<'a, F: Fn(FV) -> Option<FV> + Copy + 'static>(
        &self,
        fkey: FK,
        key: &K,
        func: F,
        guard: &'a Guard,
    ) -> SwapResult<'a, FK, FV, K, V, A, ALLOC> {
        let backoff = crossbeam_utils::Backoff::new();
        let hash = Self::hash(fkey);
        loop {
            let epoch = self.now_epoch();
            let chunk_ptr = self.chunk.load(Acquire, &guard);
            let new_chunk_ptr = self.new_chunk.load(Acquire, &guard);
            let chunk = unsafe { chunk_ptr.deref() };
            let new_chunk = Self::new_chunk_ref(epoch, &new_chunk_ptr, &chunk_ptr);
            if let Some(new_chunk) = new_chunk {
                // && self.now_epoch() == epoch
                // Copying is on the way, should try to get old value from old chunk then put new value in new chunk
                if let Some((old_parsed_val, _old_index, old_addr, attachment)) =
                    self.get_from_chunk(chunk, hash, key, fkey, &backoff)
                {
                    let old_fval = old_parsed_val.act_val();
                    if old_fval == Self::LOCKED_VALUE {
                        backoff.spin();
                        continue;
                    }
                    if old_fval >= Self::NUM_FIX_V {
                        if let Some(new_val) = func(old_fval) {
                            let val = attachment.get_value();
                            match self.modify_entry(
                                new_chunk,
                                hash,
                                key,
                                fkey,
                                ModOp::AttemptInsert(new_val, &val),
                                false,
                                guard,
                            ) {
                                ModResult::Done(_, _, new_index)
                                | ModResult::Replaced(_, _, new_index) => {
                                    if Self::cas_sentinel(old_addr, old_parsed_val.val) {
                                        // Put a sentinel in the old chunk
                                        return SwapResult::Succeed(
                                            old_fval,
                                            new_index,
                                            new_chunk_ptr,
                                        );
                                    } else {
                                        // If fail, we may have some problem here
                                        // The best strategy can be CAS a tombstone to the new index and try everything again
                                        // Note that we use attempt insert, it will be safe to just `remove` it
                                        let new_addr = new_chunk.entry_addr(new_index);
                                        let _ = Self::cas_tombstone(new_addr, new_val);
                                        continue;
                                    }
                                }
                                _ => {}
                            }
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
                false,
                guard,
            );
            if new_chunk.is_some() {
                debug_assert_ne!(chunk_ptr, new_chunk_ptr);
                debug_assert_ne!(new_chunk_ptr, Shared::null());
                self.modify_entry(chunk, hash, key, fkey, ModOp::Sentinel, false, &guard);
            }
            return match mod_res {
                ModResult::Replaced(v, _, idx) => SwapResult::Succeed(v, idx, modify_chunk_ptr),
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
    fn new_chunk_ref<'a>(
        epoch: usize,
        new_chunk_ptr: &'a Shared<ChunkPtr<FK, FV, K, V, A, ALLOC>>,
        old_chunk_ptr: &'a Shared<ChunkPtr<FK, FV, K, V, A, ALLOC>>,
    ) -> Option<&'a ChunkPtr<FK, FV, K, V, A, ALLOC>> {
        if Self::is_copying(epoch) && !old_chunk_ptr.with_tag(0).eq(new_chunk_ptr) {
            unsafe { new_chunk_ptr.as_ref() } // null ptr will be handled by as_ref
        } else {
            None
        }
    }

    #[inline(always)]
    fn now_epoch(&self) -> usize {
        self.epoch.load(Acquire)
    }

    pub fn remove(&self, key: &K, fkey: FK) -> Option<(FV, V)> {
        let guard = crossbeam_epoch::pin();
        let backoff = crossbeam_utils::Backoff::new();
        let hash = Self::hash(fkey);
        loop {
            let epoch = self.now_epoch();
            let new_chunk_ptr = self.new_chunk.load(Acquire, &guard);
            let old_chunk_ptr = self.chunk.load(Acquire, &guard);
            let copying = Self::is_copying(epoch);
            if copying {
                continue;
            }
            let new_chunk = unsafe { new_chunk_ptr.deref() };
            let old_chunk = unsafe { old_chunk_ptr.deref() };
            let mut retr = None;
            if copying {
                // Put sentinel to the old beformodify_entrye putting tombstone to the new
                // If not migration might put the old value back
                trace!("Put sentinel in old chunk for removal");
                debug_assert_ne!(new_chunk_ptr, Shared::null());
                let remove_from_old =
                    self.modify_entry(&*old_chunk, hash, key, fkey, ModOp::Sentinel, true, &guard);
                match remove_from_old {
                    ModResult::Done(fvalue, value, _) | ModResult::Replaced(fvalue, value, _) => {
                        trace!("Sentinal placed");
                        retr = Some((fvalue, value.unwrap()));
                    }
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
                true,
                &guard,
            );
            match res {
                ModResult::Replaced(fvalue, value, _) => {
                    retr = Some((fvalue, value.unwrap()));
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
        chunk: &Chunk<FK, FV, K, V, A, ALLOC>,
        hash: usize,
        key: &K,
        fkey: FK,
        backoff: &Backoff,
    ) -> Option<(FastValue<FK, FV, K, V>, usize, usize, A::Item)> {
        debug_assert_ne!(chunk as *const Chunk<FK, FV, K, V, A, ALLOC> as usize, 0);
        let mut idx = hash;
        let cap = chunk.capacity;
        let cap_mask = chunk.cap_mask();
        let mut counter = 0;
        while counter < cap {
            idx &= cap_mask;
            let attachment = chunk.attachment.prefetch(idx);
            let addr = chunk.entry_addr(idx);
            let k = Self::get_fast_key(addr);
            if k == fkey && attachment.probe(key) {
                loop {
                    let val_res = Self::get_fast_value(addr);
                    let act_val = val_res.act_val();
                    if act_val == Self::LOCKED_VALUE || act_val == Self::MIGRATING_VALUE {
                        backoff.spin();
                        continue;
                    }
                    return Some((val_res, idx, addr, attachment));
                }
            } else if k == Self::EMPTY_KEY {
                return None;
            }
            idx += 1; // reprobe
            counter += 1;
        }

        // not found
        return None;
    }

    #[inline(always)]
    fn modify_entry<'a>(
        &self,
        chunk: &'a Chunk<FK, FV, K, V, A, ALLOC>,
        hash: usize,
        key: &K,
        fkey: FK,
        op: ModOp<FV, V>,
        read_attachment: bool,
        _guard: &'a Guard,
    ) -> ModResult<FV, V> {
        let cap = chunk.capacity;
        let mut idx = hash;
        let mut count = 0;
        let cap_mask = chunk.cap_mask();
        let backoff = crossbeam_utils::Backoff::new();
        while count <= cap {
            idx &= cap_mask;
            let attachment = chunk.attachment.prefetch(idx);
            let addr = chunk.entry_addr(idx);
            let k = Self::get_fast_key(addr);
            if k == fkey && attachment.probe(&key) {
                loop {
                    let v = Self::get_fast_value(addr);
                    let raw = v.val;
                    match raw {
                        Self::LOCKED_VALUE | Self::MIGRATING_VALUE | Self::EMPTY_VALUE => {
                            backoff.spin();
                            continue;
                        }
                        SENTINEL_VALUE => return ModResult::Sentinel,
                        _ => {
                            let act_val = v.act_val();
                            match op {
                                ModOp::Insert(fval, ov) => {
                                    // Insert with attachment should prime value first when
                                    // duplicate key discovered
                                    trace!("Inserting in place for {}", fkey);
                                    let primed_fval = Self::if_attach_then_val(Self::LOCKED_VALUE, fval);
                                    if Self::cas_value(addr, v.val, primed_fval) {
                                        let prev_val = read_attachment.then(|| attachment.get_value());
                                        if Self::CAN_ATTACH {
                                            attachment.set_value(ov.clone());
                                            Self::store_value(addr, v.val, fval);
                                        }
                                        return ModResult::Replaced(act_val, prev_val, idx);
                                    } else {
                                        backoff.spin();
                                        continue;
                                    }
                                }
                                ModOp::Sentinel => {
                                    if Self::cas_sentinel(addr, v.val) {
                                        attachment.erase();
                                        return ModResult::Done(act_val, None, idx);
                                    } else {
                                        return ModResult::Fail;
                                    }
                                }
                                ModOp::Tombstone => {
                                    if act_val == Self::TOMBSTONE_VALUE {
                                        // Already tombstone
                                        return ModResult::NotFound;
                                    }
                                    if !Self::cas_tombstone(addr, v.val) {
                                        // this insertion have conflict with others
                                        // other thread changed the value (empty)
                                        // should fail
                                        return ModResult::Fail;
                                    } else {
                                        // we have put tombstone on the value, get the attachment and erase it
                                        let value = read_attachment.then(|| attachment.get_value());
                                        attachment.erase();
                                        chunk.empty_entries.fetch_add(1, Relaxed);
                                        return ModResult::Replaced(act_val, value, idx);
                                    }
                                }
                                ModOp::UpsertFastVal(ref fv) => {
                                    if Self::cas_value(addr, v.val, *fv) {
                                        if (act_val == Self::TOMBSTONE_VALUE) | (act_val == Self::EMPTY_VALUE) {
                                            return ModResult::Done(act_val, None, idx);
                                        } else {
                                            let attachment = read_attachment.then(|| attachment.get_value());
                                            return ModResult::Replaced(act_val, attachment, idx);
                                        }
                                    } else {
                                        backoff.spin();
                                        continue;
                                    }
                                }
                                ModOp::AttemptInsert(fval, oval) => {
                                    if act_val == Self::TOMBSTONE_VALUE {
                                        let primed_fval = Self::if_attach_then_val(Self::LOCKED_VALUE, fval);
                                        let prev_val =
                                            read_attachment.then(|| attachment.get_value());
                                        if Self::cas_value(addr, v.val, primed_fval) {
                                            if Self::CAN_ATTACH {
                                                attachment.set_value((*oval).clone());
                                                Self::store_value(addr, v.val, fval);
                                            }
                                            return ModResult::Replaced(act_val, prev_val, idx);
                                        } else {
                                            if Self::CAN_ATTACH && read_attachment {
                                                // Fast value changed, cannot obtain stable fat value
                                                backoff.spin();
                                                continue;
                                            }
                                            return ModResult::Existed(act_val, prev_val);
                                        }
                                    } else {
                                        trace!(
                                        "Attempting insert existed entry {}, {}, have key {:?}, skip",
                                        k,
                                        fval,
                                        act_val,
                                    );
                                        if Self::CAN_ATTACH
                                            && read_attachment
                                            && Self::get_fast_value(addr).val != v.val
                                        {
                                            backoff.spin();
                                            continue;
                                        }
                                        let value = read_attachment.then(|| attachment.get_value());
                                        return ModResult::Existed(act_val, value);
                                    }
                                }
                                ModOp::SwapFastVal(ref swap) => {
                                    trace!(
                                        "Swaping found key {} have original value {:#064b}",
                                        fkey,
                                        act_val
                                    );
                                    if act_val == Self::TOMBSTONE_VALUE {
                                        return ModResult::NotFound;
                                    }
                                    if act_val >= Self::NUM_FIX_V {
                                        if let Some(sv) = swap(act_val) {
                                            if Self::cas_value(addr, v.val, sv) {
                                                // swap success
                                                return ModResult::Replaced(act_val, None, idx);
                                            } else {
                                                return ModResult::Fail;
                                            }
                                        } else {
                                            return ModResult::Aborted;
                                        }
                                    } else {
                                        return ModResult::Fail;
                                    }
                                }
                            }
                        }
                    }
                }
            } else if k == Self::EMPTY_KEY {
                match op {
                    ModOp::Insert(fval, val) | ModOp::AttemptInsert(fval, val) => {
                        trace!(
                            "Inserting entry key: {}, value: {}, raw: {:b}, addr: {}",
                            fkey,
                            fval & Self::VAL_BIT_MASK,
                            fval,
                            addr
                        );
                        let primed_fval = Self::if_attach_then_val(Self::LOCKED_VALUE, fval);
                        if Self::cas_value(addr, Self::EMPTY_VALUE, primed_fval) {
                            if Self::CAN_ATTACH {
                                attachment.set_key(key.clone());
                                compiler_fence(Acquire);
                                Self::store_key(addr, fkey);
                                attachment.set_value((*val).clone());
                                Self::store_value_raw(addr, fval);
                            } else {
                                Self::store_key(addr, fkey);
                            }
                            return ModResult::Done(FV::zero(), None, idx);
                        } else {
                            debug!("Retry insert to new slot, now val {}", Self::get_fast_value(addr).val);
                            backoff.spin();
                            continue;
                        }
                    }
                    ModOp::UpsertFastVal(fval) => {
                        trace!(
                            "Upserting entry key: {}, value: {}, raw: {:b}, addr: {}",
                            fkey,
                            fval & Self::VAL_BIT_MASK,
                            fval,
                            addr
                        );
                        if Self::cas_value(addr, Self::EMPTY_VALUE, fval) {
                            Self::store_key(addr, fkey);
                            return ModResult::Done(FV::zero(), None, idx);
                        } else {
                            backoff.spin();
                            continue;
                        }
                    }
                    ModOp::Sentinel => {
                        if Self::cas_sentinel(addr, Self::EMPTY_VALUE) {
                            // CAS value succeed, shall store key
                            Self::store_key(addr, fkey);
                            return ModResult::Done(FV::zero(), None, idx);
                        } else {
                            backoff.spin();
                            continue;
                        }
                    }
                    ModOp::Tombstone => return ModResult::Fail,
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

    fn all_from_chunk(&self, chunk: &Chunk<FK, FV, K, V, A, ALLOC>) -> Vec<(FK, FV, K, V)> {
        let mut idx = 0;
        let cap = chunk.capacity;
        let mut counter = 0;
        let mut res = Vec::with_capacity(chunk.occupation.load(Relaxed));
        let cap_mask = chunk.cap_mask();
        while counter < cap {
            idx &= cap_mask;
            let addr = chunk.entry_addr(idx);
            let k = Self::get_fast_key(addr);
            if k != Self::EMPTY_KEY {
                let attachment = chunk.attachment.prefetch(idx);
                let val_res = Self::get_fast_value(addr);
                let act_val = val_res.act_val();
                if act_val >= Self::NUM_FIX_V {
                    let key = attachment.get_key();
                    let value = attachment.get_value();
                    if Self::CAN_ATTACH && Self::get_fast_value(addr).val != val_res.val {
                        continue;
                    }
                    res.push((k, act_val, key, value))
                }
            }
            idx += 1; // reprobe
            counter += 1;
        }
        return res;
    }

    fn entries(&self) -> Vec<(FK, FV, K, V)> {
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
    fn get_fast_key(entry_addr: usize) -> FK {
        debug_assert!(entry_addr > 0);
        unsafe { intrinsics::atomic_load_acq(entry_addr as *mut FK) }
    }

    #[inline(always)]
    fn get_fast_value(entry_addr: usize) -> FastValue<FK, FV, K, V> {
        debug_assert!(entry_addr > 0);
        let addr = entry_addr + mem::size_of::<FK>();
        let val = unsafe { intrinsics::atomic_load_acq(addr as *mut FV) };
        FastValue::<FK, FV, K, V>::new(val)
    }

    #[inline(always)]
    fn cas_tombstone(entry_addr: usize, original: FV) -> bool {
        let addr = entry_addr + mem::size_of::<FK>();
        unsafe {
            intrinsics::atomic_cxchg_acqrel_failrelaxed(
                addr as *mut FV,
                original,
                Self::TOMBSTONE_VALUE,
            ).1
        }
    }
    #[inline(always)]
    fn cas_value(entry_addr: usize, original: FV, value: FV) -> bool {
        debug_assert!(entry_addr > 0);
        let addr = entry_addr + mem::size_of::<FK>();
        unsafe { intrinsics::atomic_cxchg_acqrel_failrelaxed(addr as *mut FV, original, value).1 }
    }

    #[inline(always)]
    fn cas_value_rt_new(entry_addr: usize, original: FV, value: FV) -> Option<FV> {
        debug_assert!(entry_addr > 0);
        let addr = entry_addr + mem::size_of::<FK>();
        unsafe { intrinsics::atomic_cxchg_acqrel_failrelaxed(addr as *mut FV, original, value).1.then(|| value) }
    }

    #[inline(always)]
    fn store_value(entry_addr: usize, original: FV, value: FV) {
        debug_assert!(value >= Self::NUM_FIX_V);
        let addr = entry_addr + mem::size_of::<FK>();
        let new_value = if Self::CAN_ATTACH {
            FastValue::<FK, FV, K, V>::next_version(original, value)
        } else {
            value
        };
        unsafe { intrinsics::atomic_store_rel(addr as *mut FV, new_value) };
    }

    #[inline(always)]
    fn store_value_raw(entry_addr: usize, value: FV) {
        debug_assert!(value >= Self::NUM_FIX_V);
        let addr = entry_addr + mem::size_of::<usize>();
        unsafe { intrinsics::atomic_store_rel(addr as *mut FV, value) };
    }

    #[inline(always)]
    fn store_sentinel(entry_addr: usize) {
        debug_assert!(entry_addr > 0);
        let addr = entry_addr + mem::size_of::<usize>();
        unsafe { intrinsics::atomic_store_rel(addr as *mut FV, Self::SENTINEL_VALUE) };
    }

    #[inline(always)]
    fn store_key(addr: usize, fkey: FK) {
        debug_assert!(fkey >= Self::NUM_FIX_K);
        unsafe { intrinsics::atomic_store_rel(addr as *mut FK, fkey) }
    }

    #[inline(always)]
    fn cas_sentinel(entry_addr: usize, original: FV) -> bool {
        let addr = entry_addr + mem::size_of::<usize>();
        let (val, done) = unsafe {
            intrinsics::atomic_cxchg_acqrel_failrelaxed(
                addr as *mut FV,
                original,
                Self::SENTINEL_VALUE,
            )
        };
        done || ((val & Self::FVAL_VAL_BIT_MASK) == Self::SENTINEL_VALUE)
    }

    /// Failed return old shared
    fn check_migration<'a>(
        &self,
        old_chunk_ptr: Shared<'a, ChunkPtr<FK, FV, K, V, A, ALLOC>>,
        guard: &crossbeam_epoch::Guard,
    ) -> ResizeResult {
        if old_chunk_ptr.tag() == 1 {
            return ResizeResult::SwapFailed;
        }
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
        old_chunk_ptr: Shared<'a, ChunkPtr<FK, FV, K, V, A, ALLOC>>,
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
            cap
        };
        trace!(
            "New size for {:?} is {}, was {}",
            old_chunk_ptr,
            new_cap,
            old_cap
        );
        // Swap in old chunk as placeholder for the lock
        let old_chunk_lock = old_chunk_ptr.with_tag(1);
        if let Err(_) =
            self.chunk
                .compare_exchange(old_chunk_ptr, old_chunk_lock, AcqRel, Relaxed, guard)
        {
            // other thread have allocated new chunk and wins the competition, exit
            trace!("Cannot obtain lock for resize, will retry");
            return ResizeResult::SwapFailed;
        }
        dfence();
        trace!("Resizing {:?}", old_chunk_ptr);
        let new_chunk_ptr = Owned::new(ChunkPtr::new(Chunk::alloc_chunk(new_cap)))
            .into_shared(guard)
            .with_tag(0);
        let new_chunk_ins = unsafe { new_chunk_ptr.deref() };
        dfence();
        self.epoch.fetch_add(1, AcqRel);
        self.new_chunk.store(new_chunk_ptr, Release); // Stump becasue we have the lock already
        dfence();
        // Migrate entries
        self.migrate_entries(old_chunk_ins, new_chunk_ins, guard);
        dfence();
        let swap_chunk = self.chunk.compare_exchange(
            old_chunk_lock,
            new_chunk_ptr.with_tag(0),
            AcqRel,
            Relaxed,
            &guard,
        );
        if let Err(ec) = swap_chunk {
            panic!(
                "Must swap chunk, got {:?}, expecting {:?}",
                ec, old_chunk_ptr
            );
        }
        dfence();
        self.new_chunk.store(Shared::null(), Release);
        dfence();
        self.epoch.fetch_add(1, AcqRel);
        dfence();
        trace!(
            "Migration for {:?} completed, new chunk is {:?}, size from {} to {}",
            old_chunk_ptr, new_chunk_ptr, old_cap, new_cap
        );
        unsafe {
            guard.defer_destroy(old_chunk_ptr);
            guard.flush();
        }
        ResizeResult::Done
    }

    fn migrate_entries(
        &self,
        old_chunk_ins: &Chunk<FK, FV, K, V, A, ALLOC>,
        new_chunk_ins: &Chunk<FK, FV, K, V, A, ALLOC>,
        _guard: &crossbeam_epoch::Guard,
    ) -> usize {
        trace!(
            "Migrating entries from {:?} to {:?}",
            old_chunk_ins.base, new_chunk_ins.base
        );
        let mut old_address = old_chunk_ins.base as usize;
        let boundary = old_address + Chunk::<FK, FV, K, V, A, ALLOC>::chunk_size_of(old_chunk_ins.capacity);
        let mut effective_copy = 0;
        let mut idx = 0;
        let backoff = crossbeam_utils::Backoff::new();
        while old_address < boundary {
            // iterate the old chunk to extract entries that is NOT empty
            let fvalue = Self::get_fast_value(old_address);
            let fkey = Self::get_fast_key(old_address);
            debug_assert_eq!(old_address, old_chunk_ins.entry_addr(idx));
            // Reasoning value states
            match fvalue.act_val() {
                Self::EMPTY_VALUE | Self::TOMBSTONE_VALUE => {
                    if !Self::cas_sentinel(old_address, fvalue.val) {
                        warn!("Filling empty with sentinel for old table should succeed but not, retry");
                        backoff.spin();
                        continue;
                    }
                }
                LOCKED_VALUE => {
                    backoff.spin();
                    continue;
                }
                SENTINEL_VALUE => {
                    // Sentinel, skip
                    // Sentinel in old chunk implies its new value have already in the new chunk
                    // It can also be other thread have moved this key-value pair to the new chunk
                    trace!("Skip copy sentinel");
                }
                MIGRATING_VALUE => {}
                _ => {
                    if !self.migrate_entry(
                        fkey,
                        idx,
                        fvalue,
                        old_chunk_ins,
                        new_chunk_ins,
                        old_address,
                        &mut effective_copy,
                    ) {
                        trace!("Migration failed for entry {:?}", fkey);
                        backoff.spin();
                        continue;
                    }
                }
            }
            old_address += Self::ENTRY_SIZE;
            idx += 1;
        }
        // resize finished, make changes on the numbers
        trace!("Migrated {} entries to new chunk", effective_copy);
        new_chunk_ins.occupation.fetch_add(effective_copy, Relaxed);
        return effective_copy;
    }

    #[inline(always)]
    fn migrate_entry(
        &self,
        fkey: FK,
        old_idx: usize,
        fvalue: FastValue<FK, FV, K, V>,
        old_chunk_ins: &Chunk<FK, FV, K, V, A, ALLOC>,
        new_chunk_ins: &Chunk<FK, FV, K, V, A, ALLOC>,
        old_address: usize,
        effective_copy: &mut usize,
    ) -> bool {
        debug_assert_ne!(old_chunk_ins.base, new_chunk_ins.base);
        let old_attachment = old_chunk_ins.attachment.prefetch(old_idx);
        if fkey == Self::EMPTY_KEY {
            // Value have no key, insertion in progress
            return false;
        }
        // Insert entry into new chunk, in case of failure, skip this entry
        // Value should be locked
        let key = old_attachment.get_key();
        let value = old_attachment.get_value();
        let mut curr_orig = fvalue.val;
        let orig = curr_orig;
        // Make insertion for migration inlined, hopefully the ordering will be right
        let cap = new_chunk_ins.capacity;
        let mut idx = Self::hash(fkey);
        let cap_mask = new_chunk_ins.cap_mask();
        let mut count = 0;
        while count < cap {
            idx &= cap_mask;
            let new_attachment = new_chunk_ins.attachment.prefetch(idx);
            let addr = new_chunk_ins.entry_addr(idx);
            let k = Self::get_fast_key(addr);
            if k == fkey && new_attachment.probe(&key) {
                // New value existed, skip with None result
                break;
            } else if k == Self::EMPTY_KEY {
                // Try insert to this slot
                if curr_orig == orig {
                    match Self::cas_value_rt_new(old_address, orig, Self::MIGRATING_VALUE) {
                        Some(n) => {
                            trace!("Primed value for migration: {}", fkey);
                            curr_orig = n;
                        }
                        None => {
                            trace!("Value changed on locating new slot, key {}", fkey);
                            return false;
                        }
                    }
                }
                if Self::cas_value(addr, Self::EMPTY_VALUE, orig) {
                    new_attachment.set_key(key);
                    new_attachment.set_value(value);
                    fence(Acquire);
                    Self::store_key(addr, fkey);
                    // CAS to ensure sentinel into old chunk (spec)
                    // Use CAS for old threads may working on this one
                    trace!("Copied key {} to new chunk", fkey);
                    break;
                }
            }
            idx += 1; // reprobe
            count += 1;
        }
        if curr_orig != orig {
            Self::store_sentinel(old_address);
        } else if Self::cas_sentinel(old_address, curr_orig) {
            // continue
        } else {
            return false;
        }
        old_attachment.erase();
        *effective_copy += 1;
        return true;
    }

    pub fn map_is_copying(&self) -> bool {
        Self::is_copying(self.now_epoch())
    }

    #[inline(always)]
    fn hash(fkey: FK) -> usize {
        if Self::CAN_ATTACH && mem::size_of::<K>() == 0 {
            hash::<H>(fkey.as_())
        } else if Self::CAN_ATTACH {
            fkey.as_() // Prevent double hashing
        } else {
            hash::<H>(fkey.as_())
        }
    }

    #[inline(always)]
    const fn if_attach_then_val<T: Copy>(then: T, els: T) -> T {
        if Self::CAN_ATTACH { then } else { els }
    }
}

struct FastValue<FK: Atom, FV: Atom, K, V> {
    val: FV,
    _marker: PhantomData<(FK, K, V)>,
}

impl<FK: Atom, FV: Atom, K, V> FastValue<FK, FV, K, V> {
    gen_consts!(FK, FV);
    pub fn new(val: FV) -> Self {
        Self {
            val,
            _marker: PhantomData,
        }
    }

    #[inline]
    fn act_val(&self) -> FV {
        if can_attach::<K, V>() {
            self.val & Self::FVAL_VAL_BIT_MASK
        } else {
            self.val
        }
    }

    #[inline(always)]
    fn next_version(old: FV, new: FV) -> FV {
        debug_assert!(can_attach::<K, V>());
        let new_ver = (old | Self::FVAL_VAL_BIT_MASK).wrapping_add(&FV::one());
        new & Self::FVAL_VAL_BIT_MASK | (new_ver & Self::FVAL_VER_BIT_MASK)
    }
}

impl<FK: Atom, FV: Atom, K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> Chunk<FK, FV, K, V, A, ALLOC> {
    gen_consts!(FK, FV);
    fn alloc_chunk(capacity: usize) -> *mut Self {
        let self_size = mem::size_of::<Self>();
        let self_align = align_padding(self_size, 8);
        let self_size_aligned = self_size + self_align;
        let chunk_size = Self::chunk_size_of(capacity);
        let chunk_align = align_padding(chunk_size, 8);
        let chunk_size_aligned = chunk_size + chunk_align;
        let attachment_heap = A::heap_size_of(capacity);
        let total_size = self_size_aligned + chunk_size_aligned + attachment_heap;
        let ptr = alloc_mem::<ALLOC>(total_size) as *mut Self;
        let addr = ptr as usize;
        let data_base = addr + self_size_aligned;
        let attachment_base = data_base + chunk_size_aligned;
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
                    attachment: A::new(attachment_base),
                    shadow: PhantomData,
                },
            )
        };
        ptr
    }

    unsafe fn gc(ptr: *mut Chunk<FK, FV, K, V, A, ALLOC>) {
        debug_assert_ne!(ptr as usize, 0);
        let chunk = &*ptr;
        chunk.attachment.dealloc();
        dealloc_mem::<ALLOC>(ptr as usize, chunk.total_size);
    }

    #[inline(always)]
    fn entry_addr(&self, idx: usize) -> usize {
        self.base + idx * Self::ENTRY_SIZE
    }

    #[inline]
    fn cap_mask(&self) -> usize {
        self.capacity - 1
    }
    #[inline(always)]
    fn chunk_size_of<>(cap: usize) -> usize {
        cap * Self::ENTRY_SIZE
    }
}

impl<FK: Atom, FV: Atom, K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Clone
    for Table<FK, FV, K, V, A, ALLOC, H>
{
    fn clone(&self) -> Self {
        let new_table = Table {
            chunk: Default::default(),
            new_chunk: Default::default(),
            count: AtomicUsize::new(0),
            epoch: AtomicUsize::new(0),
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

            let cloned_old_ptr = alloc_mem::<ALLOC>(old_total_size) as *mut Chunk<FK, FV, K, V, A, ALLOC>;
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
                    alloc_mem::<ALLOC>(new_total_size) as *mut Chunk<FK, FV, K, V, A, ALLOC>;
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

impl<FK: Atom, FV: Atom, K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Drop
    for Table<FK, FV, K, V, A, ALLOC, H>
{
    fn drop(&mut self) {
        let guard = crossbeam_epoch::pin();
        unsafe {
            guard.defer_destroy(self.chunk.load(Acquire, &guard));
            let new_chunk_ptr = self.new_chunk.load(Acquire, &guard);
            if !new_chunk_ptr.is_null() {
                guard.defer_destroy(new_chunk_ptr);
            }
            guard.flush();
        }
    }
}

unsafe impl<FK: Atom, FV: Atom, K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> Send
    for ChunkPtr<FK, FV, K, V, A, ALLOC>
{
}
unsafe impl<FK: Atom, FV: Atom, K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> Sync
    for ChunkPtr<FK, FV, K, V, A, ALLOC>
{
}

impl<FK: Atom, FV: Atom, K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> Drop for ChunkPtr<FK, FV, K, V, A, ALLOC> {
    fn drop(&mut self) {
        debug_assert_ne!(self.ptr as usize, 0);

        unsafe {
            Chunk::gc(self.ptr);
        }
    }
}

impl<FK: Atom, FV: Atom, K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> Deref for ChunkPtr<FK, FV, K, V, A, ALLOC> {
    type Target = Chunk<FK, FV, K, V, A, ALLOC>;

    fn deref(&self) -> &Self::Target {
        debug_assert_ne!(self.ptr as usize, 0);
        unsafe { &*self.ptr }
    }
}

impl<FK: Atom, FV: Atom, K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> ChunkPtr<FK, FV, K, V, A, ALLOC> {
    fn new(ptr: *mut Chunk<FK, FV, K, V, A, ALLOC>) -> Self {
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
    (cap as f64 * 0.8f64) as usize
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

const fn can_attach<K, V>() -> bool {
    mem::size_of::<(K, V)>() != 0
}

pub trait Atom: 
    'static +
    Integer + Copy + Eq + Ord + 
    BitOr<Output = Self> + 
    BitAnd<Output = Self> + 
    Not<Output = Self> + 
    Shl<Output = Self> + 
    Shr<Output = Self> +
    WrappingAdd +
    Debug +
    Display +
    Binary +
    AddAssign +
    From<u8> +
    AsPrimitive<usize> +
    FromPrimitive +
    {
        fn atom_from_usize(n: usize) -> Self;
    }

pub trait Attachment<K, V> {
    type Item: AttachmentItem<K, V> + Copy;
    fn heap_size_of(cap: usize) -> usize;
    fn new(heap_ptr: usize) -> Self;
    fn prefetch(&self, index: usize) -> Self::Item;
    fn dealloc(&self);
}

pub trait AttachmentItem<K, V> {
    fn get_key(self) -> K;
    fn get_value(self) -> V;
    fn set_key(self, key: K);
    fn set_value(self, value: V);
    fn erase(self);
    fn probe(self, probe_key: &K) -> bool;
    fn prep_write(self);
}

pub struct WordAttachment;
#[derive(Copy, Clone)]
pub struct WordAttachmentItem;

// this attachment basically do nothing and sized zero
impl Attachment<(), ()> for WordAttachment {
    type Item = WordAttachmentItem;

    #[inline(always)]
    fn heap_size_of(_cap: usize) -> usize {
        0
    }

    #[inline(always)]
    fn new(_heap_ptr: usize) -> Self {
        Self
    }

    #[inline(always)]
    fn dealloc(&self) {}

    #[inline(always)]
    fn prefetch(&self, _index: usize) -> Self::Item {
        WordAttachmentItem
    }
}

impl AttachmentItem<(), ()> for WordAttachmentItem {
    #[inline(always)]
    fn get_key(self) -> () {
        ()
    }

    #[inline(always)]
    fn get_value(self) -> () {
        ()
    }

    #[inline(always)]
    fn set_key(self, _key: ()) {}

    #[inline(always)]
    fn set_value(self, _value: ()) {}

    #[inline(always)]
    fn erase(self) {}

    #[inline(always)]
    fn probe(self, _value: &()) -> bool {
        true
    }

    #[inline(always)]
    fn prep_write(self) {}
}

pub type WordTable<FK, FV, H, ALLOC> = Table<FK, FV, (), (), WordAttachment, H, ALLOC>;

pub struct WordObjectAttachment<T, A: GlobalAlloc + Default> {
    obj_chunk: usize,
    shadow: PhantomData<(T, A)>,
}

#[derive(Clone)]
pub struct WordObjectAttachmentItem<T> {
    addr: usize,
    _makrer: PhantomData<T>,
}

impl<T: Clone, A: GlobalAlloc + Default> Attachment<(), T> for WordObjectAttachment<T, A> {
    type Item = WordObjectAttachmentItem<T>;

    fn heap_size_of(cap: usize) -> usize {
        let obj_size = mem::size_of::<T>();
        cap * obj_size
    }

    fn new(heap_ptr: usize) -> Self {
        Self {
            obj_chunk: heap_ptr,
            shadow: PhantomData,
        }
    }

    #[inline(always)]
    fn dealloc(&self) {}

    #[inline(always)]
    fn prefetch(&self, index: usize) -> Self::Item {
        let addr = self.addr_by_index(index);
        unsafe {
            intrinsics::prefetch_read_data(addr as *const T, 2);
        }
        WordObjectAttachmentItem {
            addr,
            _makrer: PhantomData,
        }
    }
}

impl<T: Clone> AttachmentItem<(), T> for WordObjectAttachmentItem<T> {
    #[inline(always)]
    fn get_value(self) -> T {
        let addr = self.addr;
        let v = unsafe { (*(addr as *mut T)).clone() };
        v
    }

    #[inline(always)]
    fn set_value(self, value: T) {
        let addr = self.addr;
        unsafe { ptr::write(addr as *mut T, value) }
    }

    #[inline(always)]
    fn erase(self) {
        drop(self.addr as *mut T)
    }

    #[inline(always)]
    fn probe(self, _value: &()) -> bool {
        true
    }

    #[inline(always)]
    fn get_key(self) -> () {
        ()
    }

    #[inline(always)]
    fn set_key(self, _key: ()) {}

    #[inline(always)]
    fn prep_write(self) {
        unsafe {
            intrinsics::prefetch_write_data(self.addr as *const T, 2);
        }
    }
}

impl<T: Clone> Copy for WordObjectAttachmentItem<T> {}

type ObjAtom = u16;

pub type HashTable<K, V, ALLOC> =
    Table<ObjAtom, ObjAtom, K, V, HashKVAttachment<K, V, ALLOC>, ALLOC, PassthroughHasher>;

pub struct HashKVAttachment<K, V, A: GlobalAlloc + Default> {
    obj_chunk: usize,
    shadow: PhantomData<(K, V, A)>,
}

#[derive(Clone)]
pub struct HashKVAttachmentItem<K, V> {
    addr: usize,
    _marker: PhantomData<(K, V)>,
}

impl<K: Clone + Hash + Eq, V: Clone, A: GlobalAlloc + Default> HashKVAttachment<K, V, A> {
    const KEY_SIZE: usize = mem::size_of::<K>();
    const VAL_SIZE: usize = mem::size_of::<V>();
    const VAL_OFFSET: usize = Self::value_offset();
    const PAIR_SIZE: usize = Self::pair_size();

    const fn value_offset() -> usize {
        let padding = align_padding(Self::KEY_SIZE, 2);
        Self::KEY_SIZE + padding
    }

    const fn pair_size() -> usize {
        let raw_size = Self::value_offset() + Self::VAL_SIZE;
        let pair_padding = align_padding(raw_size, 2);
        raw_size + pair_padding
    }
}

impl<K: Clone + Hash + Eq, V: Clone, A: GlobalAlloc + Default> Attachment<K, V>
    for HashKVAttachment<K, V, A>
{
    type Item = HashKVAttachmentItem<K, V>;

    fn heap_size_of(cap: usize) -> usize {
        cap * Self::PAIR_SIZE
    }

    fn new(heap_ptr: usize) -> Self {
        Self {
            obj_chunk: heap_ptr,
            shadow: PhantomData,
        }
    }

    #[inline(always)]
    fn dealloc(&self) {}

    #[inline(always)]
    fn prefetch(&self, index: usize) -> Self::Item {
        let addr = self.addr_by_index(index);
        unsafe {
            intrinsics::prefetch_read_data(addr as *const K, 2);
        }
        HashKVAttachmentItem {
            addr,
            _marker: PhantomData,
        }
    }
}

impl<K: Clone + Hash + Eq, V: Clone> HashKVAttachmentItem<K, V> {
    const VAL_OFFSET: usize = { HashKVAttachment::<K, V, System>::VAL_OFFSET };
}

impl<K: Clone + Hash + Eq, V: Clone> AttachmentItem<K, V> for HashKVAttachmentItem<K, V> {
    #[inline(always)]
    fn get_key(self) -> K {
        let addr = self.addr;
        unsafe { (*(addr as *mut K)).clone() }
    }

    #[inline(always)]
    fn set_key(self, key: K) {
        let addr = self.addr;
        unsafe { ptr::write_volatile(addr as *mut K, key) }
    }

    #[inline(always)]
    fn get_value(self) -> V {
        let addr = self.addr;
        let val_addr = addr + Self::VAL_OFFSET;
        unsafe { (*(val_addr as *mut V)).clone() }
    }

    #[inline(always)]
    fn set_value(self, value: V) {
        let addr = self.addr;
        let val_addr = addr + Self::VAL_OFFSET;
        unsafe { ptr::write(val_addr as *mut V, value) }
    }

    #[inline(always)]
    fn erase(self) {
        let addr = self.addr;
        // drop(addr as *mut K);
        drop((addr + Self::VAL_OFFSET) as *mut V);
    }

    #[inline(always)]
    fn probe(self, key: &K) -> bool {
        unsafe { (&*(self.addr as *mut K)) == key }
    }

    #[inline(always)]
    fn prep_write(self) {
        let addr = self.addr;
        unsafe {
            intrinsics::prefetch_write_data(addr as *const (K, V), 2);
        }
    }
}

impl<K: Clone, V: Clone> Copy for HashKVAttachmentItem<K, V> {}

pub trait Map<K, V: Clone> {
    fn with_capacity(cap: usize) -> Self;
    fn get(&self, key: &K) -> Option<V>;
    fn insert(&self, key: &K, value: &V) -> Option<V>;
    // Return None if insertion successful
    fn try_insert(&self, key: &K, value: &V) -> Option<V>;
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
                if let Some(value) = self.try_insert(key, &value) {
                    return value;
                }
                return value;
            }
        }
    }
    fn clear(&self);
}

impl<K: Clone + Hash + Eq, V: Clone, A: GlobalAlloc + Default> HashKVAttachment<K, V, A> {
    fn addr_by_index(&self, index: usize) -> usize {
        self.obj_chunk + index * Self::PAIR_SIZE
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

    gen_consts!(ObjAtom, ObjAtom);

    #[inline(always)]
    pub fn insert_with_op(&self, op: InsertOp, key: &K, value: &V) -> Option<V> {
        let hash = Self::hash(&key) as ObjAtom;
        self.table
            .insert(op, key, Some(value), hash, Self::PLACEHOLDER_VAL)
            .map(|(_, v)| v)
    }

    pub fn write(&self, key: &K) -> Option<HashMapWriteGuard<K, V, ALLOC, H>> {
        HashMapWriteGuard::new(&self.table, key)
    }
    pub fn read(&self, key: &K) -> Option<HashMapReadGuard<K, V, ALLOC, H>> {
        HashMapReadGuard::new(&self.table, key)
    }

    #[inline(always)]
    fn hash(key: &K) -> usize {
        hash_key::<K, H>(key)
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
        let hash = Self::hash(key) as ObjAtom; 
        self.table.get(key, hash, true).map(|v| v.1.unwrap())
    }

    #[inline(always)]
    fn insert(&self, key: &K, value: &V) -> Option<V> {
        self.insert_with_op(InsertOp::Insert, key, value)
    }

    #[inline(always)]
    fn try_insert(&self, key: &K, value: &V) -> Option<V> {
        self.insert_with_op(InsertOp::TryInsert, key, value)
    }

    #[inline(always)]
    fn remove(&self, key: &K) -> Option<V> {
        let hash = Self::hash(key) as ObjAtom;
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
        let hash = Self::hash(key) as ObjAtom;
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
        self.obj_chunk + index * mem::size_of::<T>()
    }
}

pub type ObjectTable<FK: Atom, V, ALLOC, H> = Table<FK, ObjAtom, (), V, WordObjectAttachment<V, ALLOC>, ALLOC, H>;

#[derive(Clone)]
pub struct ObjectMap<
    FK: Atom,
    V: Clone,
    ALLOC: GlobalAlloc + Default = System,
    H: Hasher + Default = DefaultHasher,
> {
    table: ObjectTable<FK, V, ALLOC, H>,
}

impl<FK: Atom, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> ObjectMap<FK, V, ALLOC, H> {
    
    gen_consts!(FK, ObjAtom);
    
    #[inline(always)]
    fn insert_with_op(&self, op: InsertOp, key: &FK, value: &V) -> Option<V> {
        self.table
            .insert(op, &(), Some(value), *key + Self::NUM_FIX_K, Self::PLACEHOLDER_VAL)
            .map(|(_, v)| v)
    }

    pub fn read(&self, key: FK) -> Option<ObjectMapReadGuard<FK, V, ALLOC, H>> {
        ObjectMapReadGuard::new(&self.table, key)
    }

    pub fn write(&self, key: FK) -> Option<ObjectMapWriteGuard<FK, V, ALLOC, H>> {
        ObjectMapWriteGuard::new(&self.table, key)
    }
}

impl<FK: Atom, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Map<FK, V>
    for ObjectMap<FK, V, ALLOC, H>
{
    fn with_capacity(cap: usize) -> Self {
        Self {
            table: Table::with_capacity(cap),
        }
    }

    #[inline(always)]
    fn get(&self, key: &FK) -> Option<V> {
        self.table
            .get(&(), *key + Self::NUM_FIX_K, true)
            .map(|v| v.1.unwrap())
    }

    #[inline(always)]
    fn insert(&self, key: &FK, value: &V) -> Option<V> {
        self.insert_with_op(InsertOp::Insert, key, value)
    }

    #[inline(always)]
    fn try_insert(&self, key: &FK, value: &V) -> Option<V> {
        self.insert_with_op(InsertOp::TryInsert, key, value)
    }

    #[inline(always)]
    fn remove(&self, key: &FK) -> Option<V> {
        self.table.remove(&(), *key + Self::NUM_FIX_K).map(|(_, v)| v)
    }

    #[inline(always)]
    fn entries(&self) -> Vec<(FK, V)> {
        self.table
            .entries()
            .into_iter()
            .map(|(k, _, _, v)| (k - Self::NUM_FIX_K, v))
            .collect()
    }

    #[inline(always)]
    fn contains_key(&self, key: &FK) -> bool {
        self.table.get(&(), *key + Self::NUM_FIX_K, false).is_some()
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
pub struct WordMap<FK: Atom, FV: Atom, ALLOC: GlobalAlloc + Default = System, H: Hasher + Default = DefaultHasher> {
    table: WordTable<FK, FV, ALLOC, H>,
}

impl<FK: Atom, FV: Atom, ALLOC: GlobalAlloc + Default, H: Hasher + Default> WordMap<FK, FV, ALLOC, H> {
    
    gen_consts!(FK, FV);
    
    #[inline(always)]
    fn insert_with_op(&self, op: InsertOp, key: &FK, value: FV) -> Option<FV> {
        self.table
            .insert(op, &(), None, *key + Self::NUM_FIX_K, value + Self::NUM_FIX_V)
            .map(|(v, _)| v - Self::NUM_FIX_V)
    }

    pub fn get_from_mutex(&self, key: &FK) -> Option<FV> {
        self.get(key).map(|v| v & Self::WORD_MUTEX_DATA_BIT_MASK)
    }
}

impl<FK: Atom, FV: Atom, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Map<FK, FV> for WordMap<FK, FV, ALLOC, H> {
    fn with_capacity(cap: usize) -> Self {
        Self {
            table: Table::with_capacity(cap),
        }
    }

    #[inline(always)]
    fn get(&self, key: &FK) -> Option<FV> {
        self.table
            .get(&(), *key + Self::NUM_FIX_K, false)
            .map(|v| v.0 - Self::NUM_FIX_V)
    }

    #[inline(always)]
    fn insert(&self, key: &FK, value: &FV) -> Option<FV> {
        self.insert_with_op(InsertOp::UpsertFast, key, *value)
    }

    #[inline(always)]
    fn try_insert(&self, key: &FK, value: &FV) -> Option<FV> {
        self.insert_with_op(InsertOp::TryInsert, key, *value)
    }

    #[inline(always)]
    fn remove(&self, key: &FK) -> Option<FV> {
        self.table
            .remove(&(), *key + Self::NUM_FIX_K)
            .map(|(v, _)| v - Self::NUM_FIX_V)
    }
    fn entries(&self) -> Vec<(FK, FV)> {
        self.table
            .entries()
            .into_iter()
            .map(|(k, v, _, _)| (k - Self::NUM_FIX_K, v - Self::NUM_FIX_V))
            .collect()
    }

    #[inline(always)]
    fn contains_key(&self, key: &FK) -> bool {
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

pub struct WordMutexGuard<
    'a,
    FK: Atom, FV: Atom,
    ALLOC: GlobalAlloc + Default = System,
    H: Hasher + Default = DefaultHasher,
> {
    table: &'a WordTable<FK, FV, ALLOC, H>,
    key: FK,
    value: FV,
}

impl<'a, FK: Atom, FV: Atom, ALLOC: GlobalAlloc + Default, H: Hasher + Default> WordMutexGuard<'a, FK, FV, ALLOC, H> {
    
    gen_consts!(FK, FV);
    
    fn create(table: &'a WordTable<FK, FV, ALLOC, H>, key: FK) -> Option<Self> {
        let key = key + Self::NUM_FIX_K;
        let value = FV::zero();
        match table.insert(
            InsertOp::TryInsert,
            &(),
            Some(&()),
            key,
            value | Self::MUTEX_BIT_MASK,
        ) {
            None | Some((Self::TOMBSTONE_VALUE, ())) | Some((Self::EMPTY_VALUE, ())) => {
                trace!("Created locked key {}", key);
                Some(Self { table, key, value })
            }
            _ => {
                trace!("Cannot create locked key {} ", key);
                None
            }
        }
    }
    fn new(table: &'a WordTable<FK, FV, ALLOC, H>, key: FK) -> Option<Self> {
        let key = key + Self::NUM_FIX_K;
        let backoff = crossbeam_utils::Backoff::new();
        let guard = crossbeam_epoch::pin();
        let value;
        loop {
            let swap_res = table.swap(
                key,
                &(),
                move |fast_value| {
                    trace!("The key {} have value {}", key, fast_value);
                    let locked_val = fast_value | Self::MUTEX_BIT_MASK;
                    if fast_value == locked_val {
                        // Locked, unchanged
                        trace!("The key {} have locked, unchanged and try again", key);
                        None
                    } else {
                        // Obtain lock
                        trace!(
                            "The key {} have obtained, with value {}",
                            key,
                            fast_value & Self::WORD_MUTEX_DATA_BIT_MASK
                        );
                        Some(locked_val)
                    }
                },
                &guard,
            );
            match swap_res {
                SwapResult::Succeed(val, _idx, _chunk) => {
                    trace!("Lock on key {} succeed with value {}", key, val);
                    value = val & Self::WORD_MUTEX_DATA_BIT_MASK;
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
        debug_assert!(!value.is_zero());
        let value = value - Self::NUM_FIX_V;
        Some(Self { table, key, value })
    }

    pub fn remove(self) -> FV {
        trace!("Removing {}", self.key);
        let res = self.table.remove(&(), self.key).unwrap().0;
        mem::forget(self);
        res | Self::MUTEX_BIT_MASK
    }
}

impl<'a, FK: Atom, FV: Atom,  ALLOC: GlobalAlloc + Default, H: Hasher + Default> Deref for WordMutexGuard<'a, FK, FV, ALLOC, H> {
    type Target = FV;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a, FK: Atom, FV: Atom, ALLOC: GlobalAlloc + Default, H: Hasher + Default> DerefMut
    for WordMutexGuard<'a, FK, FV, ALLOC, H>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

impl<'a, FK: Atom, FV: Atom, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Drop for WordMutexGuard<'a, FK, FV, ALLOC, H> {
    fn drop(&mut self) {
        self.value += Self::NUM_FIX_V;
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
            self.value & Self::WORD_MUTEX_DATA_BIT_MASK,
        );
    }
}

impl<FK: Atom, FV: Atom, ALLOC: GlobalAlloc + Default, H: Hasher + Default> WordMap<FK, FV, ALLOC, H> {
    pub fn lock(&self, key: FK) -> Option<WordMutexGuard<FK, FV, ALLOC, H>> {
        WordMutexGuard::new(&self.table, key)
    }
    pub fn try_insert_locked(&self, key: FK) -> Option<WordMutexGuard<FK, FV, ALLOC, H>> {
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
    hash: ObjAtom,
    key: K,
    value: V,
    _mark: PhantomData<H>,
}

impl<'a, K: Clone + Eq + Hash, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default>
    HashMapReadGuard<'a, K, V, ALLOC, H>
{

    gen_consts!(ObjAtom, ObjAtom);

    fn new(table: &'a HashTable<K, V, ALLOC>, key: &K) -> Option<Self> {
        let backoff = crossbeam_utils::Backoff::new();
        let guard = crossbeam_epoch::pin();
        let hash = hash_key::<K, H>(&key) as ObjAtom;
        let value: V;
        loop {
            let swap_res = table.swap(
                hash,
                key,
                move |fast_value| {
                    if fast_value != Self::PLACEHOLDER_VAL - ObjAtom::one() {
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
                    let attachment = chunk_ref.attachment.prefetch(idx);
                    let v = attachment.get_value();
                    value = v;
                    break;
                }
                SwapResult::Failed | SwapResult::Aborted => {
                    trace!("Lock on key hash {} failed, retry", hash);
                    backoff.spin();
                    continue;
                }
                SwapResult::NotFound => {
                    trace!("Cannot found hash key {} to lock", hash);
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
                debug_assert!(fast_value > Self::PLACEHOLDER_VAL);
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
    hash: ObjAtom,
    key: K,
    value: V,
    _mark: PhantomData<H>,
}

impl<'a, K: Clone + Eq + Hash, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default>
    HashMapWriteGuard<'a, K, V, ALLOC, H>
{

    gen_consts!(ObjAtom, ObjAtom);

    fn new(table: &'a HashTable<K, V, ALLOC>, key: &K) -> Option<Self> {
        let backoff = crossbeam_utils::Backoff::new();
        let guard = crossbeam_epoch::pin();
        let hash = hash_key::<K, H>(&key) as ObjAtom;
        let value: V;
        loop {
            let swap_res = table.swap(
                hash,
                key,
                move |fast_value| {
                    if fast_value == Self::PLACEHOLDER_VAL {
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
                    let attachment = chunk_ref.attachment.prefetch(idx);
                    let v = attachment.get_value();
                    value = v;
                    break;
                }
                SwapResult::Failed | SwapResult::Aborted => {
                    trace!("Lock on key hash {} failed, retry", hash);
                    backoff.spin();
                    continue;
                }
                SwapResult::NotFound => {
                    trace!("Cannot found hash key {} to lock", hash);
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
        let hash = hash_key::<K, H>(&self.key) as ObjAtom;
        self.table.insert(
            InsertOp::Insert,
            &self.key,
            Some(&self.value),
            hash,
            Self::PLACEHOLDER_VAL,
        );
    }
}

pub struct ObjectMapReadGuard<
    'a,
    FK: Atom,
    V: Clone,
    ALLOC: GlobalAlloc + Default = System,
    H: Hasher + Default = DefaultHasher,
> {
    table: &'a ObjectTable<FK, V, ALLOC, H>,
    key: FK,
    value: V,
    _mark: PhantomData<H>,
}

impl<'a, FK: Atom, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default>
    ObjectMapReadGuard<'a, FK, V, ALLOC, H>
{

    gen_consts!(FK, ObjAtom);

    fn new(table: &'a ObjectTable<FK, V, ALLOC, H>, key: FK) -> Option<Self> {
        let backoff = crossbeam_utils::Backoff::new();
        let guard = crossbeam_epoch::pin();
        let value: V;
        let key = key + Self::NUM_FIX_K;
        loop {
            let swap_res = table.swap(
                key,
                &(),
                move |fast_value| {
                    if fast_value != Self::PLACEHOLDER_VAL - 1 {
                        // Not write locked, can bump it by one
                        trace!("Key {} is not write locked, will read lock", key);
                        Some(fast_value + 1)
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
                    let attachment = chunk_ref.attachment.prefetch(idx);
                    let v = attachment.get_value();
                    value = v;
                    break;
                }
                SwapResult::Failed | SwapResult::Aborted => {
                    trace!("Lock on key {} failed, retry", key);
                    backoff.spin();
                    continue;
                }
                SwapResult::NotFound => {
                    trace!("Cannot found hash key {} to lock", key);
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

impl<'a, FK: Atom, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Deref
    for ObjectMapReadGuard<'a, FK, V, ALLOC, H>
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a, FK: Atom, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Drop
    for ObjectMapReadGuard<'a, FK, V, ALLOC, H>
{
    fn drop(&mut self) {
        trace!("Release read lock for hash key {}", self.key);
        let guard = crossbeam_epoch::pin();
        self.table.swap(
            self.key,
            &(),
            |fast_value| {
                debug_assert!(fast_value > Self::PLACEHOLDER_VAL);
                Some(fast_value - 1)
            },
            &guard,
        );
    }
}

pub struct ObjectMapWriteGuard<
    'a,
    FK: Atom, 
    V: Clone,
    ALLOC: GlobalAlloc + Default = System,
    H: Hasher + Default = DefaultHasher,
> {
    table: &'a ObjectTable<FK, V, ALLOC, H>,
    key: FK,
    value: V,
    _mark: PhantomData<H>,
}

impl<'a, FK: Atom, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default>
    ObjectMapWriteGuard<'a, FK, V, ALLOC, H>
{

    gen_consts!(FK, ObjAtom);
    
    fn new(table: &'a ObjectTable<FK, V, ALLOC, H>, key: FK) -> Option<Self> {
        let backoff = crossbeam_utils::Backoff::new();
        let guard = crossbeam_epoch::pin();
        let value: V;
        let key = key + Self::NUM_FIX_K;
        loop {
            let swap_res = table.swap(
                key,
                &(),
                move |fast_value| {
                    if fast_value == Self::PLACEHOLDER_VAL {
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
                    let attachment = chunk_ref.attachment.prefetch(idx);
                    let v = attachment.get_value();
                    value = v;
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

impl<'a, FV: Atom, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Deref
    for ObjectMapWriteGuard<'a, FV, V, ALLOC, H>
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a, FV: Atom, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> DerefMut
    for ObjectMapWriteGuard<'a, FV, V, ALLOC, H>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}

impl<'a, FV: Atom, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Drop
    for ObjectMapWriteGuard<'a, FV, V, ALLOC, H>
{
    fn drop(&mut self) {
        trace!("Release read lock for key {}", self.key);
        self.table.insert(
            InsertOp::Insert,
            &(),
            Some(&self.value),
            self.key,
            Self::PLACEHOLDER_VAL,
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
        let hash = hash_key::<T, H>(item) as ObjAtom;
        self.table.get(item, hash, false).is_some()
    }

    pub fn insert(&self, item: &T) -> bool {
        let hash = hash_key::<T, H>(item) as ObjAtom;
        self.table
            .insert(InsertOp::TryInsert, item, None, hash, !0)
            .is_none()
    }

    pub fn remove(&self, item: &T) -> bool {
        let hash = hash_key::<T, H>(item) as ObjAtom;
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

#[repr(C, align(8))]
struct AlignedLiteObj<T> {
    data: T,
    _marker: PhantomData<T>,
}

impl<T> AlignedLiteObj<T> {
    pub fn new(obj: T) -> Self {
        Self {
            data: obj,
            _marker: PhantomData,
        }
    }
}

pub struct LiteHashMap<
    K: Clone + Hash + Eq,
    V: Clone,
    ALLOC: GlobalAlloc + Default = System,
    H: Hasher + Default = DefaultHasher,
> {
    table: WordTable<usize, usize, ALLOC, H>,
    shadow: PhantomData<(K, V, H)>,
}

impl<K: Clone + Hash + Eq, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default>
    LiteHashMap<K, V, ALLOC, H>
{
    gen_consts!(usize, usize);
    const K_SIZE: usize = mem::size_of::<AlignedLiteObj<K>>();
    const V_SIZE: usize = mem::size_of::<AlignedLiteObj<V>>();

    #[inline(always)]
    pub fn insert_with_op(&self, op: InsertOp, key: &K, value: &V) -> Option<V> {
        let k_num = Self::encode(key);
        let v_num = Self::encode(value);
        self.table
            .insert(op, &(), None, k_num, v_num)
            .map(|(fv, _)| Self::decode::<V>(fv))
    }

    // pub fn write(&self, key: &K) -> Option<HashMapWriteGuard<K, V, ALLOC, H>> {
    //     HashMapWriteGuard::new(&self.table, key)
    // }
    // pub fn read(&self, key: &K) -> Option<HashMapReadGuard<K, V, ALLOC, H>> {
    //     HashMapReadGuard::new(&self.table, key)
    // }

    #[inline(always)]
    fn encode<T: Clone>(d: &T) -> usize {
        let mut num: u64 = 0;
        let obj_ptr = &mut num as *mut u64 as *mut T;
        unsafe {
            ptr::write(obj_ptr, d.clone());
        }
        return num as usize + Self::NUM_FIX_K;
    }

    #[inline(always)]
    fn decode<T: Clone>(num: usize) -> T {
        let num = (num - Self::NUM_FIX_K) as u64;
        let ptr = &num as *const u64 as *const AlignedLiteObj<T>;
        let aligned = unsafe { &*ptr };
        let obj = aligned.data.clone();
        return obj;
    }
}

impl<K: Clone + Hash + Eq, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Map<K, V>
    for LiteHashMap<K, V, ALLOC, H>
{
    fn with_capacity(cap: usize) -> Self {
        assert_eq!(Self::K_SIZE, 8);
        assert_eq!(Self::V_SIZE, 8);
        Self {
            table: Table::with_capacity(cap),
            shadow: PhantomData,
        }
    }

    #[inline(always)]
    fn get(&self, key: &K) -> Option<V> {
        let k_num = Self::encode(key);
        self.table
            .get(&(), k_num, false)
            .map(|(fv, _)| Self::decode::<V>(fv))
    }

    #[inline(always)]
    fn insert(&self, key: &K, value: &V) -> Option<V> {
        self.insert_with_op(InsertOp::UpsertFast, key, value)
    }

    #[inline(always)]
    fn try_insert(&self, key: &K, value: &V) -> Option<V> {
        self.insert_with_op(InsertOp::TryInsert, key, value)
    }

    #[inline(always)]
    fn remove(&self, key: &K) -> Option<V> {
        let k_num = Self::encode(key);
        self.table
            .remove(&(), k_num)
            .map(|(fv, _)| Self::decode(fv))
    }

    #[inline(always)]
    fn entries(&self) -> Vec<(K, V)> {
        self.table
            .entries()
            .into_iter()
            .map(|(fk, fv, _, _)| (Self::decode(fk), Self::decode::<V>(fv)))
            .collect()
    }

    #[inline(always)]
    fn contains_key(&self, key: &K) -> bool {
        let k_num = Self::encode(key);
        self.table.get(&(), k_num, false).is_some()
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.table.len()
    }

    fn clear(&self) {
        self.table.clear();
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

impl<FV: Atom, V> Debug for ModResult<FV, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Replaced(arg0, _arg1, arg2) => {
                f.debug_tuple("Replaced").field(arg0).field(arg2).finish()
            }
            Self::Existed(arg0, _arg1) => f.debug_tuple("Existed").field(arg0).finish(),
            Self::Fail => write!(f, "Fail"),
            Self::Sentinel => write!(f, "Sentinel"),
            Self::NotFound => write!(f, "NotFound"),
            Self::Done(arg0, _arg1, arg2) => f.debug_tuple("Done").field(arg0).field(arg2).finish(),
            Self::TableFull => write!(f, "TableFull"),
            Self::Aborted => write!(f, "Aborted"),
        }
    }
}

macro_rules! atom_types {
    ($($t: ty),+) => {
        $(
            impl const Atom for $t{
                #[inline(always)]
                fn atom_from_usize(n: usize) -> Self {
                    n as Self
                }
            }
        )+
    };
}

atom_types!(u8, u16, u32, u64, usize);

mod lite_tests {
    use super::{LiteHashMap, Map};
    use std::{alloc::System, sync::Arc};

    #[test]
    fn no_resize() {
        let _ = env_logger::try_init();
        let map = LiteHashMap::<usize, usize, System>::with_capacity(4096);
        for i in 5..2048 {
            let k = i;
            let v = i * 2;
            map.insert(&k, &v);
        }
        for i in 5..2048 {
            let k = i;
            let v = i * 2;
            match map.get(&k) {
                Some(r) => assert_eq!(r, v),
                None => panic!("{}", i),
            }
        }
    }

    #[derive(Clone)]
    struct SlimStruct {
        a: u32,
        b: u32,
    }

    impl SlimStruct {
        fn new(n: u32) -> Self {
            Self { a: n, b: n * 2 }
        }
    }

    struct FatStruct {
        a: usize,
        b: usize,
    }
    impl FatStruct {
        fn new(n: usize) -> Self {
            Self { a: n, b: n * 2 }
        }
    }

    #[test]
    fn no_resize_arc() {
        let _ = env_logger::try_init();
        let map = LiteHashMap::<usize, Arc<FatStruct>, System>::with_capacity(4096);
        for i in 5..2048 {
            let k = i;
            let v = i * 2;
            let d = Arc::new(FatStruct::new(v));
            map.insert(&k, &d);
        }
        for i in 5..2048 {
            let k = i;
            let v = i * 2;
            match map.get(&k) {
                Some(r) => {
                    assert_eq!(r.a as usize, v);
                    assert_eq!(r.b as usize, v * 2);
                }
                None => panic!("{}", i),
            }
        }
    }

    #[test]
    fn no_resize_small_type() {
        let _ = env_logger::try_init();
        let map = LiteHashMap::<u8, u8, System>::with_capacity(4096);
        for i in 0..2048 {
            let k = i as u8;
            let v = (i * 2) as u8;
            map.insert(&k, &v);
        }
        for i in 0..2048 {
            let k = i as u8;
            let v = (i * 2) as u8;
            match map.get(&k) {
                Some(r) => {
                    assert_eq!(r, v);
                }
                None => panic!("{}", i),
            }
        }
    }
}

#[cfg(test)]
mod fat_tests {
    use crate::map::*;
    use std::sync::Arc;
    use std::thread;

    const VAL_SIZE: usize = 2048;

    pub type Key = [u8; 128];
    pub type Value = [u8; VAL_SIZE];
    pub type FatHashMap = HashMap<Key, Value, System>;

    #[test]
    fn no_resize() {
        let _ = env_logger::try_init();
        let map = FatHashMap::with_capacity(4096);
        for i in 5..2048 {
            let k = key_from(i);
            let v = val_from(i * 2);
            map.insert(&k, &v);
        }
        for i in 5..2048 {
            let k = key_from(i);
            let v = val_from(i * 2);
            match map.get(&k) {
                Some(r) => assert_eq!(r, v),
                None => panic!("{}", i),
            }
        }
    }

    #[test]
    fn resize() {
        let _ = env_logger::try_init();
        let map = FatHashMap::with_capacity(16);
        for i in 5..2048 {
            let k = key_from(i);
            let v = val_from(i * 2);
            map.insert(&k, &v);
        }
        for i in 5..2048 {
            let k = key_from(i);
            let v = val_from(i * 2);
            match map.get(&k) {
                Some(r) => assert_eq!(r, v),
                None => panic!("{}", i),
            }
        }
    }

    #[test]
    fn resize_obj_map() {
        let _ = env_logger::try_init();
        let map = ObjectMap::<usize, usize, System>::with_capacity(16);
        let turns = 40960;
        for i in 5..turns {
            let k = i;
            let v = i * 2;
            map.insert(&k, &v);
        }
        for i in 5..turns {
            let k = i;
            let v = i * 2;
            match map.get(&k) {
                Some(r) => assert_eq!(r, v),
                None => panic!("{}", i),
            }
        }
    }

    #[test]
    fn parallel_no_resize() {
        let _ = env_logger::try_init();
        let map = Arc::new(FatHashMap::with_capacity(65536));
        let mut threads = vec![];
        for i in 5..99 {
            let k = key_from(i);
            let v = val_from(i * 10);
            map.insert(&k, &v);
        }
        for i in 100..900 {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                for j in 5..60 {
                    let k = key_from(i * 100 + j);
                    let v = val_from(i * j);
                    map.insert(&k, &v);
                }
            }));
        }
        for i in 5..9 {
            for j in 1..10 {
                let k = key_from(i * j);
                map.remove(&k);
            }
        }
        for thread in threads {
            let _ = thread.join();
        }
        for i in 100..900 {
            for j in 5..60 {
                let k = key_from(i * 100 + j);
                let v = val_from(i * j);
                assert_eq!(map.get(&k), Some(v))
            }
        }
        for i in 5..9 {
            for j in 1..10 {
                let k = key_from(i * j);
                assert!(map.get(&k).is_none())
            }
        }
    }

    #[test]
    fn parallel_with_resize() {
        let _ = env_logger::try_init();
        let num_threads = num_cpus::get();
        let test_load = 1024;
        let repeat_load = 32;
        let map = Arc::new(FatHashMap::with_capacity(32));
        let mut threads = vec![];
        for i in 0..num_threads {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                for j in 5..test_load {
                    let key = key_from(i * 10000000 + j);
                    let value_prefix = i * j * 100;
                    for k in 1..repeat_load {
                        let value_num = value_prefix + k;
                        if k != 1 {
                            assert_eq!(map.get(&key), Some(val_from(value_num - 1)));
                        }
                        let value = val_from(value_num);
                        let pre_insert_epoch = map.table.now_epoch();
                        map.insert(&key, &value);
                        let post_insert_epoch = map.table.now_epoch();
                        for l in 1..128 {
                            let pre_fail_get_epoch = map.table.now_epoch();
                            let left = map.get(&key);
                            let post_fail_get_epoch = map.table.now_epoch();
                            let right = Some(value);
                            if left != right {
                                error!("Discovered mismatch key {:?}, analyzing", FatHashMap::hash(&key));
                                for m in 1..1024 {
                                    let mleft = map.get(&key);
                                    let mright = Some(value);
                                    if mleft == mright {
                                        panic!(
                                            "Recovered at turn {} for {:?}, copying {}, epoch {} to {}, now {}, PIE: {} to {}. Expecting {:?} got {:?}. Migration problem!!!", 
                                            m, 
                                            FatHashMap::hash(&key), 
                                            map.table.map_is_copying(),
                                            pre_fail_get_epoch,
                                            post_fail_get_epoch,
                                            map.table.now_epoch(),
                                            pre_insert_epoch, 
                                            post_insert_epoch,
                                            right, left
                                        );
                                        // panic!("Late value change on {:?}", key);
                                    }
                                }
                                panic!("Unable to recover for {:?}, round {}, copying {}. Expecting {:?} got {:?}.", FatHashMap::hash(&key), l , map.table.map_is_copying(), right, left);
                                // panic!("Unrecoverable value change for {:?}", key);
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
                            assert!(map.read(&key).is_none(), "Remove recursion with lock");
                            map.insert(&key, &value);
                        }
                        if j % 3 == 0 {
                            let new_value = val_from(value_num + 7);
                            let pre_insert_epoch = map.table.now_epoch();
                            map.insert(&key, &new_value);
                            let post_insert_epoch = map.table.now_epoch();
                            assert_eq!(
                                map.get(&key), 
                                Some(new_value), 
                                "Checking immediate update, key {:?}, epoch {} to {}",
                                key, pre_insert_epoch, post_insert_epoch
                            );
                            map.insert(&key, &value);
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
        (0..num_threads).for_each(|i| {
            for j in 5..test_load {
                let k_num = i * 10000000 + j;
                let v_num = i * j * 100 + repeat_load - 1;
                let k = key_from(k_num);
                let v = val_from(v_num);
                let get_res = map.get(&k);
                assert_eq!(
                    Some(v),
                    get_res,
                    "Final val mismatch. k {:?}, i {}, j {}, epoch {}",
                    k,
                    i,
                    j,
                    map.table.now_epoch()
                );
            }
        });
    }

    #[test]
    fn attachment_size() {
        type Attachment = HashKVAttachment<usize, usize, System>;
        assert_eq!(Attachment::KEY_SIZE, 8);
        assert_eq!(Attachment::VAL_SIZE, 8);
        assert_eq!(Attachment::VAL_OFFSET, 8);
        assert_eq!(Attachment::PAIR_SIZE, 16);
    }

    fn key_from(num: usize) -> Key {
        let mut r = [0u8; 128];
        for (i, b) in num.to_be_bytes().iter().enumerate() {
            r[i] = *b
        }
        r
    }

    fn val_from(num: usize) -> Value {
        let mut r = [0u8; VAL_SIZE];
        for (i, b) in num.to_be_bytes().iter().enumerate() {
            r[i] = *b
        }
        r
    }
}

#[cfg(test)]
mod word_tests {
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
        let table = WordMap::<usize, usize, System>::with_capacity(16);
        for i in 50..60 {
            assert_eq!(table.insert(&i, &i), None);
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
        let map = WordMap::<usize, usize, System>::with_capacity(16);
        for i in 5..2048 {
            map.insert(&i, &(i * 2));
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
        let map = Arc::new(WordMap::<usize, usize, System>::with_capacity(65536));
        let mut threads = vec![];
        for i in 5..99 {
            map.insert(&i, &(i * 10));
        }
        for i in 100..900 {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                for j in 5..60 {
                    map.insert(&(i * 100 + j), &(i * j));
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
        let map = Arc::new(WordMap::<usize, usize, System>::with_capacity(32));
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
                        map.insert(&key, &value);
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
                            map.insert(&key, &value);
                        }
                        if j % 3 == 0 {
                            let new_value = value + 7;
                            let pre_insert_epoch = map.table.now_epoch();
                            map.insert(&key, &new_value);
                            let post_insert_epoch = map.table.now_epoch();
                            assert_eq!(
                                map.get(&key), 
                                Some(new_value), 
                                "Checking immediate update, key {}, epoch {} to {}",
                                key, pre_insert_epoch, post_insert_epoch
                            );
                            map.insert(&key, &value);
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
        let map = Arc::new(WordMap::<usize, usize, System>::with_capacity(4));
        for i in 5..128 {
            map.insert(&i, &(i * 10));
        }
        let mut threads = vec![];
        for i in 256..265 {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                for j in 5..60 {
                    map.insert(&(i * 10 + j), &10);
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
        let map = Arc::new(WordMap::<usize, usize, System>::with_capacity(4));
        map.insert(&1, &0);
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
        let map = Arc::new(WordMap::<usize, usize, System>::with_capacity(16));
        let mut threads = vec![];
        let num_threads = 16;
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
        let map_cont = ObjectMap::<usize, Obj, System, DefaultHasher>::with_capacity(4);
        let map = Arc::new(map_cont);
        map.insert(&1, &Obj::new(0));
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
        map.insert(&1, &Obj::new(0));
        let mut threads = vec![];
        let num_threads = 16;
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
        let map = ObjectMap::<usize, Obj>::with_capacity(16);
        for i in 5..2048 {
            map.insert(&i, &Obj::new(i));
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
        let map = Arc::new(ObjectMap::<usize, Obj>::with_capacity(4));
        for i in 5..128 {
            map.insert(&i, &Obj::new(i * 10));
        }
        let mut threads = vec![];
        for i in 256..265 {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                for j in 5..60 {
                    map.insert(&(i * 10 + j), &Obj::new(10));
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
            map.insert(&i, &Obj::new((i * 10) as usize));
        }
        let mut threads = vec![];
        for i in 256..265u32 {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                for j in 5..60u32 {
                    map.insert(&(i * 10 + j), &Obj::new(10usize));
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
    fn insert_with_num_fixes() {
        let map = WordMap::<usize, usize, System, DefaultHasher>::with_capacity(32);
        assert_eq!(map.insert(&24, &0), None);
        assert_eq!(map.insert(&24, &1), Some(0));
        assert_eq!(map.insert(&0, &0), None);
        assert_eq!(map.insert(&0, &1), Some(0))
    }

    #[bench]
    fn lfmap(b: &mut Bencher) {
        let _ = env_logger::try_init();
        let map = WordMap::<usize, usize, System, DefaultHasher>::with_capacity(8);
        let mut i = 5;
        b.iter(|| {
            map.insert(&i, &i);
            i += 1;
        });
    }

    #[bench]
    fn lite_lfmap(b: &mut Bencher) {
        let _ = env_logger::try_init();
        let map = LiteHashMap::<usize, usize, System, DefaultHasher>::with_capacity(8);
        let mut i = 5;
        b.iter(|| {
            map.insert(&i, &i);
            i += 1;
        });
    }

    #[bench]
    fn fat_lfmap(b: &mut Bencher) {
        let _ = env_logger::try_init();
        let map = super::HashMap::<usize, usize, System, DefaultHasher>::with_capacity(8);
        let mut i = 5;
        b.iter(|| {
            map.insert(&i, &i);
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
