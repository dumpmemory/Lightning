use std::{sync::Arc, thread};

use super::*;

pub struct EntryTemplate(FKey, FVal);
pub type HopBits = u32;
pub type HopVer = ();
pub type HopTuple = (HopBits, HopVer);

pub const ENABLE_HOPSOTCH: bool = false;
pub const ENABLE_SKIPPING: bool = true & ENABLE_HOPSOTCH;

pub const EMPTY_KEY: FKey = 0;
pub const DISABLED_KEY: FKey = 2;

pub const EMPTY_VALUE: FVal = 0b000;
pub const SENTINEL_VALUE: FVal = 0b010;

// Last bit set indicates locking
pub const LOCKED_VALUE: FVal = 0b001;
pub const BACKWARD_SWAPPING_VALUE: FVal = 0b011;
pub const FORWARD_SWAPPING_VALUE: FVal = 0b101;

pub const TOMBSTONE_VALUE: FVal = 0b110;

pub const NUM_FIX_K: FKey = 0b1000; // = 8
pub const NUM_FIX_V: FVal = 0b1000; // = 8

pub const VAL_BIT_MASK: FVal = (!0 << 1 >> 1) & (!VAL_KEY_DIGEST_MASK);
pub const VAL_PRIME_MASK: FVal = !VAL_BIT_MASK;
pub const MUTEX_BIT_MASK: FVal = !WORD_MUTEX_DATA_BIT_MASK & VAL_BIT_MASK;
pub const ENTRY_SIZE: usize = mem::size_of::<EntryTemplate>();
pub const WORD_MUTEX_DATA_BIT_MASK: FVal = !0 << 2 >> 2;

// For key digest
pub const KEY_DIGEST_DIGITS: usize = 4; // Steal 4 bits for key digest in value
pub const KEY_DIGEST_MASK: usize = !(!0 << KEY_DIGEST_DIGITS >> KEY_DIGEST_DIGITS); // Take leading bits
pub const VAL_KEY_DIGEST_SHIFT: usize = 2;
pub const VAL_KEY_DIGEST_MASK: usize = KEY_DIGEST_MASK >> VAL_KEY_DIGEST_SHIFT;

// For fat objects
pub const FVAL_BITS: usize = mem::size_of::<FVal>() * 8;
pub const FVAL_VER_POS: FVal = (FVAL_BITS as FVal) / 2;
pub const FVAL_VER_BIT_MASK: FVal = !0 << FVAL_VER_POS & VAL_BIT_MASK;
pub const FVAL_VAL_BIT_MASK: FVal = !FVAL_VER_BIT_MASK;
pub const PLACEHOLDER_VAL: FVal = NUM_FIX_V + 1;

pub const HOP_BYTES: usize = mem::size_of::<HopBits>();
pub const HOP_TUPLE_BYTES: usize = mem::size_of::<HopTuple>();
pub const NUM_HOPS: usize = HOP_BYTES * 8;
pub const ALL_HOPS_TAKEN: HopBits = !0;

enum ModResult<V> {
    Replaced(FVal, Option<V>, usize), // (origin fval, val, index)
    Existed(FVal, Option<V>),
    Fail,
    Sentinel,
    NotFound,
    Done(FVal, Option<V>, usize), // _, value, index
    TableFull,
    Aborted,
}

enum ModOp<'a, V> {
    Insert(FVal, Option<&'a V>),
    UpsertFastVal(FVal),
    AttemptInsert(FVal, Option<&'a V>),
    SwapFastVal(Box<dyn Fn(FVal) -> Option<FVal>>),
    Sentinel,
    Tombstone,
}

pub enum InsertOp {
    Insert,
    UpsertFast,
    TryInsert,
    Tombstone,
}

enum ResizeResult {
    NoNeed,
    SwapFailed,
    InProgress,
}

pub enum SwapResult<'a, K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> {
    Succeed(FVal, usize, Shared<'a, ChunkPtr<K, V, A, ALLOC>>),
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
    hop_base: usize,
    pub attachment: A,
    shadow: PhantomData<(K, V, ALLOC)>,
}

pub struct ChunkPtr<K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> {
    ptr: *mut Chunk<K, V, A, ALLOC>,
}

pub struct Table<K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default, H: Hasher + Default> {
    meta: Arc<ChunkMeta<K, V, A, ALLOC>>,
    attachment_init_meta: A::InitMeta,
    count: AtomicUsize,
    init_cap: usize,
    mark: PhantomData<H>,
}

struct ChunkMeta<K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> {
    new_chunk: Atomic<ChunkPtr<K, V, A, ALLOC>>,
    pub chunk: Atomic<ChunkPtr<K, V, A, ALLOC>>,
    epoch: AtomicUsize,
}

impl<
        K: Clone + Hash + Eq,
        V: Clone,
        A: Attachment<K, V>,
        ALLOC: GlobalAlloc + Default,
        H: Hasher + Default,
    > Table<K, V, A, ALLOC, H>
{
    const FAT_VAL: bool = mem::size_of::<V>() != 0;
    const WORD_KEY: bool = mem::size_of::<K>() == 0;

    pub fn with_capacity(cap: usize, attachment_init_meta: A::InitMeta) -> Self {
        trace!("Creating chunk with capacity {}", cap);
        if !is_power_of_2(cap) {
            panic!("capacity is not power of 2");
        }
        // Each entry key value pair is 2 words
        // steal 1 bit in the MSB of value indicate Prime(1)
        let chunk = Chunk::alloc_chunk(cap, &attachment_init_meta);
        Self {
            meta: Arc::new(ChunkMeta {
                chunk: Atomic::new(ChunkPtr::new(chunk)),
                new_chunk: Atomic::null(),
                epoch: AtomicUsize::new(0),
            }),
            count: AtomicUsize::new(0),
            init_cap: cap,
            attachment_init_meta,
            mark: PhantomData,
        }
    }

    pub fn new(attachment_init_meta: A::InitMeta) -> Self {
        Self::with_capacity(64, attachment_init_meta)
    }

    pub fn get_with_hash(
        &self,
        key: &K,
        fkey: FKey,
        hash: usize,
        read_attachment: bool,
        guard: &Guard,
        backoff: &Backoff,
    ) -> Option<(FVal, Option<V>, usize)> {
        'OUTER: loop {
            let epoch = self.now_epoch();
            let chunk_ptr = self.meta.chunk.load(Acquire, &guard);
            let new_chunk_ptr = self.meta.new_chunk.load(Acquire, &guard);
            let chunk = unsafe { chunk_ptr.deref() };
            let new_chunk = Self::new_chunk_ref(epoch, &new_chunk_ptr, &chunk_ptr);
            debug_assert!(!chunk_ptr.is_null());
            if let Some((mut val, addr, aitem)) = self.get_from_chunk(
                &*chunk,
                hash,
                key,
                fkey,
                &backoff,
                new_chunk.map(|c| c.deref()),
            ) {
                'SPIN: loop {
                    let v = val.val;
                    if val.is_valued() {
                        let act_val = val.act_val::<V>();
                        let mut attachment = None;
                        if Self::FAT_VAL && read_attachment {
                            attachment = Some(aitem.get_value());
                            let new_val = Self::get_fast_value(addr);
                            if new_val.val != val.val {
                                val = new_val;
                                continue 'SPIN;
                            }
                        }
                        return Some((act_val, attachment, addr));
                    } else if v == SENTINEL_VALUE {
                        if new_chunk.is_none() {
                            backoff.spin();
                            continue 'OUTER;
                        }
                        break 'SPIN;
                    } else {
                        break 'SPIN;
                    }
                }
            }

            // Looking into new chunk
            if let Some(new_chunk) = new_chunk {
                if let Some((mut val, addr, aitem)) =
                    self.get_from_chunk(&*new_chunk, hash, key, fkey, &backoff, None)
                {
                    'SPIN_NEW: loop {
                        let v = val.val;
                        if val.is_valued() {
                            let act_val = val.act_val::<V>();
                            let mut attachment = None;
                            if Self::FAT_VAL && read_attachment {
                                attachment = Some(aitem.get_value());
                                let new_val = Self::get_fast_value(addr);
                                if new_val.val != val.val {
                                    val = new_val;
                                    continue 'SPIN_NEW;
                                }
                            }
                            return Some((act_val, attachment, addr));
                        } else if v == SENTINEL_VALUE {
                            backoff.spin();
                            continue 'OUTER;
                        } else {
                            break 'SPIN_NEW;
                        }
                    }
                }
            }
            let new_epoch = self.now_epoch();
            if new_epoch != epoch {
                backoff.spin();
                continue 'OUTER;
            }
            return None;
        }
    }

    #[inline(always)]
    pub fn get(&self, key: &K, fkey: FKey, read_attachment: bool) -> Option<(FVal, Option<V>)> {
        let (fkey, hash) = Self::hash(fkey, key);
        let guard = crossbeam_epoch::pin();
        let backoff = crossbeam_utils::Backoff::new();
        self.get_with_hash(key, fkey, hash, read_attachment, &guard, &backoff)
            .map(|(a, b, _)| (a, b))
    }

    pub fn insert(
        &self,
        op: InsertOp,
        key: &K,
        value: Option<&V>,
        fkey: FKey,
        fvalue: FVal,
    ) -> Option<(FVal, V)> {
        let backoff = crossbeam_utils::Backoff::new();
        let guard = crossbeam_epoch::pin();
        let (fkey, hash) = Self::hash(fkey, key);
        loop {
            let epoch = self.now_epoch();
            let chunk_ptr = self.meta.chunk.load(Acquire, &guard);
            let chunk = unsafe { chunk_ptr.deref() };
            let new_chunk_ptr = self.meta.new_chunk.load(Acquire, &guard);
            let new_chunk = Self::new_chunk_ref(epoch, &new_chunk_ptr, &chunk_ptr);
            // trace!("Insert {} at {:?}-{:?}", fkey, chunk_ptr, new_chunk_ptr);
            if let Some(new_chunk) = new_chunk {
                if new_chunk.occupation.load(Acquire) >= new_chunk.occu_limit {
                    backoff.spin();
                    continue;
                }
            } else {
                match self.check_migration(chunk_ptr, chunk, &guard) {
                    ResizeResult::InProgress | ResizeResult::SwapFailed => {
                        trace!("Retry insert due to resize");
                        backoff.spin();
                        continue;
                    }
                    ResizeResult::NoNeed => {}
                }
            }
            let modify_chunk = new_chunk.unwrap_or(chunk);
            let masked_value = fvalue & VAL_BIT_MASK;
            let mod_op = match op {
                InsertOp::Insert => ModOp::Insert(masked_value, value),
                InsertOp::UpsertFast => ModOp::UpsertFastVal(masked_value),
                InsertOp::TryInsert => ModOp::AttemptInsert(masked_value, value),
                InsertOp::Tombstone => ModOp::Tombstone,
            };
            let old_chunk_val = new_chunk.map(|_| {
                self.get_from_chunk(&*chunk, hash, key, fkey, &backoff, new_chunk.map(|c| &**c))
            });
            let value_insertion =
                self.modify_entry(&*modify_chunk, hash, key, fkey, mod_op, true, &guard, None);
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
                    if new_chunk.is_none() {
                        self.do_migration(chunk_ptr, &guard);
                    } else {
                        backoff.spin();
                    }
                    continue;
                }
                ModResult::Sentinel => {
                    trace!("Discovered sentinel on insertion table upon probing, retry");
                    backoff.spin();
                    continue;
                }
                ModResult::NotFound => {
                    // Only possible for Tombstone op
                    if new_chunk.is_some() {
                        // Cannot find the item to delete during migration
                        // If can be in the old chunk which we can try to jut a sentinel
                        match self.modify_entry(
                            chunk,
                            hash,
                            key,
                            fkey,
                            ModOp::Sentinel,
                            true,
                            &guard,
                            new_chunk.map(|c| &**c),
                        ) {
                            ModResult::Done(_, _, _) => {
                                chunk.occupation.fetch_add(1, AcqRel);
                            }
                            ModResult::Replaced(fv, val, _) => {
                                if fv > TOMBSTONE_VALUE {
                                    return Some((fv, val.unwrap()));
                                }
                            }
                            _ => {}
                        }
                        backoff.spin();
                        continue;
                    }
                }
                ModResult::Aborted => unreachable!("Should no abort"),
            }
            match old_chunk_val {
                Some(Some((fval, addr, attachment))) => {
                    if !Self::cas_sentinel(addr, fval.val) {
                        backoff.spin();
                        continue;
                    }
                    if result.is_none() && fval.is_valued() {
                        return Some((fval.act_val::<V>(), attachment.get_value()));
                    }
                }
                Some(None) => {
                    match self.modify_entry(
                        chunk,
                        hash,
                        key,
                        fkey,
                        ModOp::Sentinel,
                        true,
                        &guard,
                        new_chunk.map(|c| &**c),
                    ) {
                        ModResult::Replaced(fval, val, _) | ModResult::Existed(fval, val) => {
                            if result.is_none() && fval >= NUM_FIX_V {
                                return Some((fval, val.unwrap().clone()));
                            }
                        }
                        _ => {}
                    }
                }
                None => {}
            }
            // trace!("Inserted key {}, with value {}", fkey, fvalue);
            return result;
        }
    }

    fn manually_drop_sentinel_res(&self, res: &ModResult<V>, chunk: &Chunk<K, V, A, ALLOC>) {
        match res {
            ModResult::Done(fval, _, _) | ModResult::Replaced(fval, _, _) => {
                if *fval <= TOMBSTONE_VALUE {
                    return;
                }
                chunk.attachment.manually_drop(*fval);
            }
            _ => {}
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
            let owned_new = Owned::new(ChunkPtr::new(Chunk::alloc_chunk(
                self.init_cap,
                &self.attachment_init_meta,
            )));
            self.meta
                .chunk
                .store(owned_new.into_shared(&guard), Release);
            self.meta.new_chunk.store(Shared::null(), Release);
            dfence();
            self.count.fetch_sub(len, AcqRel);
            break;
        }
    }

    pub fn swap<'a, F: Fn(FVal) -> Option<FVal> + Copy + 'static>(
        &self,
        fkey: FKey,
        key: &K,
        func: F,
        guard: &'a Guard,
    ) -> SwapResult<'a, K, V, A, ALLOC> {
        let backoff = crossbeam_utils::Backoff::new();
        let (fkey, hash) = Self::hash(fkey, key);
        loop {
            let epoch = self.now_epoch();
            let chunk_ptr = self.meta.chunk.load(Acquire, &guard);
            let new_chunk_ptr = self.meta.new_chunk.load(Acquire, &guard);
            let chunk = unsafe { chunk_ptr.deref() };
            let new_chunk = Self::new_chunk_ref(epoch, &new_chunk_ptr, &chunk_ptr);

            // Comment this for better performance.
            // if new_chunk.is_some() {
            //     backoff.spin();
            //     continue;
            // }

            // First try UPDATE new_chunk if it exists
            // This is the fast path
            let update_chunk_ptr = if new_chunk_ptr.is_null() {
                chunk_ptr
            } else {
                new_chunk_ptr
            };
            let update_chunk = new_chunk.unwrap_or(chunk);
            let mut result = None;
            let fast_mod_res = self.modify_entry(
                update_chunk,
                hash,
                key,
                fkey,
                ModOp::SwapFastVal(Box::new(func)),
                false,
                &guard,
                None,
            );
            match fast_mod_res {
                ModResult::Replaced(fval, _, idx) => {
                    // That's it
                    result = Some((fval, idx, update_chunk_ptr));
                }
                ModResult::Fail | ModResult::Sentinel => {
                    // The key exists in the chunk, retry
                    backoff.spin();
                    continue;
                }
                ModResult::Aborted => {
                    // Key exists but aborted by user function, just return
                    return SwapResult::Aborted;
                }
                ModResult::NotFound => {
                    // Probably should try the old chunk see if it is there
                    trace!("Cannot find {} to swap", fkey);
                }
                _ => unreachable!("{:?}", fast_mod_res),
            }
            if let Some(new_chunk_de) = new_chunk {
                let new_chunk_ref = new_chunk.map(|c| &**c);
                if let Some((fval, idx, mod_chunk_ptr)) = result {
                    // Just try to CAS a sentinel in the old chunk and we are done
                    self.modify_entry(
                        chunk,
                        hash,
                        key,
                        fkey,
                        ModOp::Sentinel,
                        false,
                        &guard,
                        new_chunk_ref,
                    );
                    return SwapResult::Succeed(fval, idx, mod_chunk_ptr);
                }
                // Try get value from the old chunk and insert into new chunk
                let old_chunk_val = new_chunk.map(|_| {
                    self.get_from_chunk(&*chunk, hash, key, fkey, &backoff, new_chunk_ref)
                });
                match old_chunk_val {
                    Some(Some((fval, addr, _))) => {
                        if fval.is_valued() {
                            let mut fval_actual = fval.act_val::<V>();
                            if let Some(mut new_fval) = func(fval_actual) {
                                // Do two things after:
                                // 1. try insert the new value into the new chunk with lock value
                                // 2. Put the sentinel into the old chunk.
                                // 3. Store the new value into the locked value
                                match self.modify_entry(
                                    new_chunk_de,
                                    hash,
                                    key,
                                    fkey,
                                    ModOp::AttemptInsert(LOCKED_VALUE, None),
                                    false,
                                    guard,
                                    new_chunk_ref,
                                ) {
                                    ModResult::Replaced(_, _, idx) | ModResult::Done(_, _, idx) => {
                                        result = Some((fval_actual, idx, new_chunk_ptr));
                                    }
                                    _ => {
                                        // Cannot put new value into new chunk, retry
                                        backoff.spin();
                                        continue;
                                    }
                                }
                                let lock_idx = result.as_ref().unwrap().1;
                                let lock_addr = new_chunk_de.entry_addr(lock_idx);
                                if !Self::cas_sentinel(addr, fval.val) {
                                    // Cannot CAS with the address, try CAS by searching
                                    // Because the lock have already gone through to the new chunk, need the guarantee
                                    loop {
                                        match self.modify_entry(
                                            chunk,
                                            hash,
                                            key,
                                            fkey,
                                            ModOp::Sentinel,
                                            false,
                                            &guard,
                                            None,
                                        ) {
                                            ModResult::Replaced(fv, _, idx)
                                            | ModResult::Done(fv, _, idx) => {
                                                if fv > NUM_FIX_V {
                                                    // Got another new value, should recompute the function result
                                                    fval_actual = FastValue::new(fv).act_val::<V>();
                                                    if let Some(nfv) = func(fv) {
                                                        new_fval = nfv;
                                                        result =
                                                            Some((fval_actual, idx, chunk_ptr));
                                                        break;
                                                    } else {
                                                        // Aborted, revert the slot to old value implies migration
                                                        Self::store_value(lock_addr, fv);
                                                        return SwapResult::Aborted;
                                                    }
                                                }
                                            }
                                            _ => {
                                                backoff.spin();
                                                continue;
                                            }
                                        }
                                    }
                                }
                                // Here, just store the value on the locked slot;
                                Self::store_value(lock_addr, new_fval);
                                let (fval_actual, idx, chunk_ptr) = result.unwrap();
                                return SwapResult::Succeed(fval_actual, idx, chunk_ptr);
                            } else {
                                return SwapResult::Aborted;
                            }
                        } else {
                            // Key exists but not useful to compute a replacement, retry
                            backoff.spin();
                            continue;
                        }
                    }
                    // Cannot find anything used in old chunk or no new chunk, continue to return not found
                    Some(None) => {}
                    None => {}
                }
            } else if let Some((fval_actual, idx, chunk_ptr)) = result {
                return SwapResult::Succeed(fval_actual, idx, chunk_ptr);
            }
            // Here both old and new chunk cannot find it, return
            return SwapResult::NotFound;
        }
    }

    #[inline(always)]
    pub fn remove(&self, key: &K, fkey: FKey) -> Option<(FVal, V)> {
        let tagging_res = self.insert(InsertOp::Tombstone, key, None, fkey, TOMBSTONE_VALUE);
        if tagging_res.is_some() {
            self.count.fetch_sub(1, AcqRel);
        }
        return tagging_res;
    }

    #[inline]
    fn is_copying(epoch: usize) -> bool {
        epoch | 1 == epoch
    }

    #[inline]
    fn epoch_changed(&self, epoch: usize) -> bool {
        self.now_epoch() != epoch
    }

    #[inline]
    fn new_chunk_ref<'a>(
        epoch: usize,
        new_chunk_ptr: &'a Shared<ChunkPtr<K, V, A, ALLOC>>,
        old_chunk_ptr: &'a Shared<ChunkPtr<K, V, A, ALLOC>>,
    ) -> Option<&'a ChunkPtr<K, V, A, ALLOC>> {
        if Self::is_copying(epoch) && !old_chunk_ptr.with_tag(0).eq(new_chunk_ptr) {
            unsafe { new_chunk_ptr.as_ref() } // null ptr will be handled by as_ref
        } else {
            None
        }
    }

    pub(crate) fn occupation(&self) -> (usize, usize, usize) {
        let guard = crossbeam_epoch::pin();
        let chunk_ptr = self.meta.chunk.load(Acquire, &guard);
        let chunk = unsafe { chunk_ptr.deref() };
        (
            chunk.occu_limit,
            chunk.occupation.load(Relaxed),
            chunk.capacity,
        )
    }

    pub(crate) fn dump_dist(&self) {
        let guard = crossbeam_epoch::pin();
        let chunk_ptr = self.meta.chunk.load(Acquire, &guard);
        let chunk = unsafe { chunk_ptr.deref() };
        let cap = chunk.capacity;
        let mut res = String::new();
        for i in 0..cap {
            let addr = chunk.entry_addr(i);
            let k = Self::get_fast_key(addr);
            if k != EMPTY_VALUE {
                res.push('1');
            } else {
                res.push('0');
            }
        }
        info!("Chunk dump: {}", res);
    }

    #[inline]
    pub(crate) fn now_epoch(&self) -> usize {
        self.meta.epoch.load(Acquire)
    }

    pub fn len(&self) -> usize {
        self.count.load(Relaxed)
    }

    fn get_from_chunk(
        &self,
        chunk: &Chunk<K, V, A, ALLOC>,
        hash: usize,
        key: &K,
        fkey: FKey,
        backoff: &Backoff,
        new_chunk: Option<&Chunk<K, V, A, ALLOC>>,
    ) -> Option<(FastValue, usize, A::Item)> {
        debug_assert_ne!(chunk as *const Chunk<K, V, A, ALLOC> as usize, 0);
        let cap_mask = chunk.cap_mask();
        let home_idx = hash & cap_mask;
        let reiter = || chunk.iter_slot_skipable(home_idx, true);
        let mut iter = reiter();
        let (mut idx, _) = iter.next().unwrap();
        let mut addr = chunk.entry_addr(idx);
        loop {
            let k = Self::get_fast_key(addr);
            if k == fkey {
                let attachment = chunk.attachment.prefetch(idx);
                if attachment.probe(key) {
                    'READ_VAL: loop {
                        let val_res = Self::get_fast_value(addr);
                        if val_res.val == FORWARD_SWAPPING_VALUE {
                            Self::wait_entry(addr, k, FORWARD_SWAPPING_VALUE, backoff);
                            iter.refresh_following(chunk);
                            break 'READ_VAL; // Continue probing, slot has been moved forward
                        } else if val_res.val == BACKWARD_SWAPPING_VALUE {
                            // No bother, its inserting and swapping
                            return None;
                        }
                        if val_res.is_locked() {
                            backoff.spin();
                            continue 'READ_VAL;
                        }
                        if Self::get_fast_key(addr) != k {
                            // For hopsotch
                            // Here hash collision is impossible becasue hopsotch only swap with
                            // slots have different hash key
                            backoff.spin();
                            continue;
                        }
                        return Some((val_res, addr, attachment));
                    }
                }
            } else if k == EMPTY_KEY {
                return None;
            }
            new_chunk.map(|new_chunk| {
                let fval = Self::get_fast_value(addr);
                Self::passive_migrate_entry(k, idx, fval, chunk, new_chunk, addr);
            });
            if let Some((new_idx, _)) = iter.next() {
                idx = new_idx;
                addr = chunk.entry_addr(idx);
            } else {
                break;
            }
        }

        // not found
        return None;
    }

    #[inline]
    fn modify_entry<'a>(
        &self,
        chunk: &'a Chunk<K, V, A, ALLOC>,
        hash: usize,
        key: &K,
        fkey: FKey,
        op: ModOp<V>,
        read_attachment: bool,
        _guard: &'a Guard,
        new_chunk: Option<&Chunk<K, V, A, ALLOC>>,
    ) -> ModResult<V> {
        let cap_mask = chunk.cap_mask();
        let backoff = crossbeam_utils::Backoff::new();
        let home_idx = hash & cap_mask;
        let reiter = || chunk.iter_slot_skipable(home_idx, false);
        let mut iter = reiter();
        let (mut idx, mut count) = iter.next().unwrap();
        let mut addr = chunk.entry_addr(idx);
        'MAIN: loop {
            let k = Self::get_fast_key(addr);
            if k == fkey {
                let attachment = chunk.attachment.prefetch(idx);
                let v = Self::get_fast_value(addr);
                let key_probe = attachment.probe(&key);
                if Self::get_fast_key(addr) != k {
                    // For hopsotch
                    // Here hash collision is impossible becasue hopsotch only swap with
                    // slots have different hash key
                    backoff.spin();
                    continue 'MAIN;
                }
                if key_probe {
                    let raw = v.val;
                    let act_val = v.act_val::<V>();
                    if raw >= TOMBSTONE_VALUE {
                        match op {
                            ModOp::Insert(fval, ov) => {
                                // Insert with attachment should prime value first when
                                // duplicate key discovered
                                let primed_fval = Self::if_fat_val_then_val(LOCKED_VALUE, fval);
                                let prev_val = read_attachment.then(|| attachment.get_value());
                                let val_to_store = Self::value_to_store(raw, fval);
                                if Self::cas_value(addr, raw, primed_fval).1 {
                                    ov.map(|ov| attachment.set_value(ov.clone(), raw));
                                    if Self::FAT_VAL {
                                        Self::store_raw_value(addr, val_to_store);
                                    }
                                    if raw != TOMBSTONE_VALUE {
                                        return ModResult::Replaced(act_val, prev_val, idx);
                                    } else {
                                        chunk.empty_entries.fetch_sub(1, Relaxed);
                                        return ModResult::Done(act_val, None, idx);
                                    }
                                } else {
                                    backoff.spin();
                                    continue;
                                }
                            }
                            ModOp::Sentinel => {
                                if Self::cas_sentinel(addr, v.val) {
                                    let prev_val =
                                        read_attachment.then(|| attachment.get_value());
                                    attachment.erase(raw);
                                    if raw == 0 {
                                        return ModResult::Replaced(0, prev_val, idx);
                                    } else {
                                        return ModResult::Replaced(act_val, prev_val, idx);
                                    }
                                } else {
                                    return ModResult::Fail;
                                }
                            }
                            ModOp::Tombstone => {
                                if raw == TOMBSTONE_VALUE {
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
                                    attachment.erase(raw);
                                    chunk.empty_entries.fetch_add(1, Relaxed);
                                    return ModResult::Replaced(act_val, value, idx);
                                }
                            }
                            ModOp::UpsertFastVal(ref fv) => {
                                if Self::cas_value(addr, v.val, *fv).1 {
                                    if (act_val == TOMBSTONE_VALUE) | (act_val == EMPTY_VALUE) {
                                        return ModResult::Done(0, None, idx);
                                    } else {
                                        let attachment =
                                            read_attachment.then(|| attachment.get_value());
                                        return ModResult::Replaced(act_val, attachment, idx);
                                    }
                                } else {
                                    backoff.spin();
                                    continue;
                                }
                            }
                            ModOp::AttemptInsert(fval, oval) => {
                                if act_val == TOMBSTONE_VALUE {
                                    let primed_fval =
                                        Self::if_fat_val_then_val(LOCKED_VALUE, fval);
                                    let prev_val =
                                        read_attachment.then(|| attachment.get_value());
                                    let val_to_store = Self::value_to_store(raw, fval);
                                    if Self::cas_value(addr, v.val, primed_fval).1 {
                                        oval.map(|oval| {
                                            attachment.set_value((*oval).clone(), raw)
                                        });
                                        if Self::FAT_VAL {
                                            Self::store_raw_value(addr, val_to_store);
                                        }
                                        return ModResult::Replaced(act_val, prev_val, idx);
                                    } else {
                                        if Self::FAT_VAL && read_attachment {
                                            // Fast value changed, cannot obtain stable fat value
                                            backoff.spin();
                                            continue;
                                        }
                                        return ModResult::Existed(act_val, prev_val);
                                    }
                                } else {
                                    if Self::FAT_VAL
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
                                if act_val == TOMBSTONE_VALUE {
                                    return ModResult::NotFound;
                                }
                                if let Some(sv) = swap(act_val) {
                                    if Self::cas_value(addr, v.val, sv).1 {
                                        // swap success
                                        return ModResult::Replaced(act_val, None, idx);
                                    } else {
                                        return ModResult::Fail;
                                    }
                                } else {
                                    return ModResult::Aborted;
                                }
                            }
                        }
                    } else if raw == SENTINEL_VALUE || v.is_primed() {
                        return ModResult::Sentinel;
                    } else if raw == BACKWARD_SWAPPING_VALUE {
                        Self::wait_entry(addr, k, BACKWARD_SWAPPING_VALUE, &backoff);
                        iter = reiter();
                        continue 'MAIN;
                    } else if raw == FORWARD_SWAPPING_VALUE {
                        Self::wait_entry(addr, k, BACKWARD_SWAPPING_VALUE, &backoff);
                        iter.refresh_following(chunk);
                        continue 'MAIN;
                    } else {
                        // Other tags (except tombstone and locks)
                        if cfg!(debug_assertions) {
                            match raw {
                                LOCKED_VALUE => {
                                    trace!("Spin on lock");
                                }
                                EMPTY_VALUE => {
                                    trace!("Spin on empty");
                                }
                                _ => {
                                    trace!("Spin on something else");
                                }
                            }
                        }
                        backoff.spin();
                        continue;
                    }
                }
            }
            if k == EMPTY_KEY {
                // trace!("Inserting {}", fkey);
                let hop_adjustment = Self::need_hop_adjustment(chunk, new_chunk, count); // !Self::FAT_VAL && count > NUM_HOPS;
                match op {
                    ModOp::Insert(fval, val) | ModOp::AttemptInsert(fval, val) => {
                        let (store_fkey, cas_fval) = if hop_adjustment {
                            // Use empty key to block probing progression for hops
                            (fkey, BACKWARD_SWAPPING_VALUE)
                        } else {
                            (fkey, fval)
                        };
                        let primed_fval = Self::if_fat_val_then_val(LOCKED_VALUE, cas_fval);
                        match Self::cas_value(addr, EMPTY_VALUE, primed_fval) {
                            (_, true) => {
                                let attachment = chunk.attachment.prefetch(idx);
                                if !hop_adjustment {
                                    attachment.set_key(key.clone());
                                }
                                if Self::FAT_VAL {
                                    Self::store_key(addr, fkey);
                                    val.map(|val| attachment.set_value((*val).clone(), 0));
                                    Self::store_raw_value(addr, fval);
                                } else {
                                    Self::store_key(addr, store_fkey);
                                    match Self::adjust_hops(
                                        hop_adjustment,
                                        chunk,
                                        fkey,
                                        fval,
                                        Some(key),
                                        home_idx,
                                        idx,
                                        count,
                                    ) {
                                        Ok(new_idx) => {
                                            idx = new_idx;
                                        }
                                        Err(new_idx) => {
                                            let new_addr = chunk.entry_addr(new_idx);
                                            Self::store_value(new_addr, TOMBSTONE_VALUE);
                                            Self::store_key(new_addr, DISABLED_KEY);
                                            return ModResult::TableFull;
                                        }
                                    }
                                }
                                return ModResult::Done(0, None, idx);
                            }
                            (SENTINEL_VALUE, false) => return ModResult::Sentinel,
                            (FORWARD_SWAPPING_VALUE, false) => {
                                Self::wait_entry(addr, k, FORWARD_SWAPPING_VALUE, &backoff);
                                iter.refresh_following(chunk);
                                continue 'MAIN;
                            }
                            (BACKWARD_SWAPPING_VALUE, false) => {
                                // Reprobe
                                Self::wait_entry(addr, k, BACKWARD_SWAPPING_VALUE, &backoff);
                                iter = reiter();
                                continue 'MAIN;
                            }
                            (_, false) => {
                                backoff.spin();
                                continue;
                            }
                        }
                    }
                    ModOp::UpsertFastVal(fval) => {
                        let (store_fkey, cas_fval) = if hop_adjustment {
                            (fkey, BACKWARD_SWAPPING_VALUE)
                        } else {
                            (fkey, fval)
                        };
                        match Self::cas_value(addr, EMPTY_VALUE, cas_fval) {
                            (_, true) => {
                                Self::store_key(addr, store_fkey);
                                match Self::adjust_hops(
                                    hop_adjustment,
                                    chunk,
                                    fkey,
                                    fval,
                                    None,
                                    home_idx,
                                    idx,
                                    count,
                                ) {
                                    Ok(new_idx) => {
                                        idx = new_idx;
                                    }
                                    Err(new_idx) => {
                                        let new_addr = chunk.entry_addr(new_idx);
                                        Self::store_value(new_addr, TOMBSTONE_VALUE);
                                        Self::store_key(new_addr, DISABLED_KEY);
                                        return ModResult::TableFull;
                                    }
                                }
                                return ModResult::Done(0, None, idx);
                            }
                            (SENTINEL_VALUE, false) => return ModResult::Sentinel,
                            (FORWARD_SWAPPING_VALUE, false) => {
                                Self::wait_entry(addr, k, FORWARD_SWAPPING_VALUE, &backoff);
                                iter.refresh_following(chunk);
                                continue 'MAIN;
                            }
                            (BACKWARD_SWAPPING_VALUE, false) => {
                                // Reprobe
                                Self::wait_entry(addr, k, BACKWARD_SWAPPING_VALUE, &backoff);
                                iter = reiter();
                                continue 'MAIN;
                            }
                            (_, false) => {
                                backoff.spin();
                                continue;
                            }
                        }
                    }
                    ModOp::Sentinel => {
                        if Self::cas_sentinel(addr, EMPTY_VALUE) {
                            // CAS value succeed, shall store key
                            Self::store_key(addr, fkey);
                            return ModResult::Done(0, None, idx);
                        } else {
                            backoff.spin();
                            continue;
                        }
                    }
                    ModOp::Tombstone => return ModResult::NotFound,
                    ModOp::SwapFastVal(_) => return ModResult::NotFound,
                };
            }
            if k >= NUM_FIX_K {
                let fval = Self::get_fast_value(addr);
                let raw = fval.val;
                if Self::get_fast_key(addr) != k {
                    // Recheck key does not changed
                    backoff.spin();
                    continue 'MAIN;
                }
                match raw {
                    SENTINEL_VALUE => match &op {
                        ModOp::Insert(_, _)
                        | ModOp::AttemptInsert(_, _)
                        | ModOp::UpsertFastVal(_) => {
                            return ModResult::Sentinel;
                        }
                        _ => {}
                    },
                    BACKWARD_SWAPPING_VALUE => {
                        if iter.terminal {
                            // Only check terminal probing
                            Self::wait_entry(addr, k, raw, &backoff);
                            iter = reiter();
                            continue 'MAIN;
                        }
                    }
                    FORWARD_SWAPPING_VALUE => {
                        // Shall NOT wait
                        // There must be a key matching forwarding swapping
                        // Self::wait_entry(addr, k, raw, &backoff);
                        // iter.refresh_following(chunk);
                        // continue 'MAIN;
                    }
                    _ => {}
                }
                //  else if let Some(new_chunk) = new_chunk {
                //     Self::passive_migrate_entry(k, idx, fval, chunk, new_chunk, addr);
                // }
            }
            // trace!("Reprobe inserting {} got {}", fkey, k);
            if let Some(new_iter) = iter.next() {
                (idx, count) = new_iter;
                addr = chunk.entry_addr(idx);
            } else {
                break;
            }
        }
        match op {
            ModOp::Insert(_, _) | ModOp::AttemptInsert(_, _) | ModOp::UpsertFastVal(_) => {
                ModResult::TableFull
            }
            _ => ModResult::NotFound,
        }
    }

    #[inline(always)]
    fn need_hop_adjustment(
        chunk: &Chunk<K, V, A, ALLOC>,
        new_chunk: Option<&Chunk<K, V, A, ALLOC>>,
        count: usize,
    ) -> bool {
        ENABLE_HOPSOTCH
            && !Self::FAT_VAL
            && new_chunk.is_none()
            && chunk.capacity > NUM_HOPS
            && count > NUM_HOPS
    }

    #[inline(always)]
    fn wait_entry(addr: usize, orig_key: FKey, orig_val: FVal, backoff: &Backoff) {
        loop {
            if Self::get_fast_value(addr).val != orig_val
                && (orig_val == BACKWARD_SWAPPING_VALUE || Self::get_fast_key(addr) != orig_key)
            {
                break;
            }
            backoff.spin();
        }
    }

    fn all_from_chunk(&self, chunk: &Chunk<K, V, A, ALLOC>) -> Vec<(FKey, FVal, K, V)> {
        let mut idx = 0;
        let cap = chunk.capacity;
        let mut counter = 0;
        let mut res = Vec::with_capacity(chunk.occupation.load(Relaxed));
        let cap_mask = chunk.cap_mask();
        while counter < cap {
            idx &= cap_mask;
            let addr = chunk.entry_addr(idx);
            let k = Self::get_fast_key(addr);
            if k != EMPTY_KEY {
                let val_res = Self::get_fast_value(addr);
                let act_val = val_res.act_val::<V>();
                if act_val >= NUM_FIX_V {
                    let attachment = chunk.attachment.prefetch(idx);
                    let key = attachment.get_key();
                    let value = attachment.get_value();
                    if Self::FAT_VAL && Self::get_fast_value(addr).val != val_res.val {
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

    pub fn entries(&self) -> Vec<(FKey, FVal, K, V)> {
        let guard = crossbeam_epoch::pin();
        let old_chunk_ref = self.meta.chunk.load(Acquire, &guard);
        let new_chunk_ref = self.meta.new_chunk.load(Acquire, &guard);
        let old_chunk = unsafe { old_chunk_ref.deref() };
        let new_chunk = unsafe { new_chunk_ref.deref() };
        let mut res = self.all_from_chunk(&*old_chunk);
        if !new_chunk_ref.is_null() && old_chunk_ref != new_chunk_ref {
            res.append(&mut self.all_from_chunk(&*new_chunk));
        }
        return res;
    }

    #[inline(always)]
    fn get_fast_key(entry_addr: usize) -> FKey {
        Chunk::<K, V, A, ALLOC>::get_fast_key(entry_addr)
    }

    #[inline(always)]
    pub fn get_fast_value(entry_addr: usize) -> FastValue {
        Chunk::<K, V, A, ALLOC>::get_fast_value(entry_addr)
    }

    #[inline(always)]
    fn cas_tombstone(entry_addr: usize, original: FVal) -> bool {
        let addr = entry_addr + mem::size_of::<FKey>();
        unsafe {
            intrinsics::atomic_cxchg_acqrel_failrelaxed(
                addr as *mut FVal,
                original,
                TOMBSTONE_VALUE,
            )
            .1
        }
    }
    #[inline(always)]
    fn cas_value(entry_addr: usize, original: FVal, value: FVal) -> (FVal, bool) {
        debug_assert!(entry_addr > 0);
        let addr = entry_addr + mem::size_of::<FKey>();
        unsafe { intrinsics::atomic_cxchg_acqrel_failrelaxed(addr as *mut FVal, original, value) }
    }

    #[inline(always)]
    fn store_value(entry_addr: usize, value: FVal) {
        debug_assert!(entry_addr > 0);
        let addr = entry_addr + mem::size_of::<FKey>();
        unsafe { intrinsics::atomic_store_release(addr as *mut FVal, value) }
    }

    #[inline(always)]
    fn value_to_store(original: FVal, value: FVal) -> usize {
        if Self::FAT_VAL {
            FastValue::next_version(original, value)
        } else {
            0
        }
    }

    #[inline(always)]
    fn store_raw_value(entry_addr: usize, value: FVal) {
        let addr = entry_addr + mem::size_of::<FKey>();
        unsafe { intrinsics::atomic_store_release(addr as *mut FVal, value) };
    }

    #[inline(always)]
    fn store_sentinel(entry_addr: usize) {
        debug_assert!(entry_addr > 0);
        let addr = entry_addr + mem::size_of::<FKey>();
        unsafe { intrinsics::atomic_store_release(addr as *mut FVal, SENTINEL_VALUE) };
    }

    #[inline(always)]
    fn store_key(addr: usize, fkey: FKey) {
        unsafe { intrinsics::atomic_store_release(addr as *mut FKey, fkey) }
    }

    #[inline(always)]
    fn cas_sentinel(entry_addr: usize, original: FVal) -> bool {
        let addr = entry_addr + mem::size_of::<FKey>();
        let (val, done) = unsafe {
            intrinsics::atomic_cxchg_acqrel_failrelaxed(addr as *mut FVal, original, SENTINEL_VALUE)
        };
        done || ((val & FVAL_VAL_BIT_MASK) == SENTINEL_VALUE)
    }

    fn adjust_hops(
        needs_adjust: bool,
        chunk: &Chunk<K, V, A, ALLOC>,
        fkey: usize,
        fval: usize,
        key: Option<&K>,
        home_idx: usize,
        mut dest_idx: usize,
        hops: usize,
    ) -> Result<usize, usize> {
        // This algorithm only swap the current indexed slot with the
        // one that has hop bis set to avoid swapping with other swapping slot
        if !ENABLE_HOPSOTCH {
            return Ok(dest_idx);
        }

        if hops < NUM_HOPS {
            chunk.set_hop_bit(home_idx, hops);
            return Ok(dest_idx);
        }

        if !needs_adjust {
            return Ok(dest_idx);
        }

        let cap_mask = chunk.cap_mask();
        let cap = chunk.capacity;
        let target_key_digest = key_digest(fkey);
        'SWAPPING: loop {
            // Need to adjust hops
            let scaled_curr_idx = dest_idx | cap;
            let starting = (scaled_curr_idx - (NUM_HOPS - 1)) & cap_mask;
            let curr_idx = scaled_curr_idx & cap_mask;
            assert_eq!(curr_idx, dest_idx);
            let (probe_start, probe_end) = if starting < curr_idx {
                (starting, curr_idx)
            } else {
                (starting, cap + curr_idx)
            };
            let hop_bits = chunk.get_hop_bits(home_idx);
            if hop_bits == ALL_HOPS_TAKEN {
                // No slots in the neighbour is available
                return Err(dest_idx);
            }
            debug_assert!(probe_start < probe_end);
            // Find a swappable slot
            for i in probe_start..probe_end {
                let idx = i & cap_mask;
                let mut iter = chunk.iter_slot_skipable(idx, true);
                while let Some((candidate_idx, candidate_distance)) = iter.next() {
                    if iter.terminal || iter.pos >= NUM_HOPS {
                        break;
                    }
                    // Range checking
                    if Self::out_of_hop_range(home_idx, dest_idx, candidate_idx, cap) {
                        continue;
                    }
                    // Found a candidate slot
                    let candidate_addr = chunk.entry_addr(candidate_idx);
                    let candidate_fkey = Self::get_fast_key(candidate_addr);
                    let candidate_fval = Self::get_fast_value(candidate_addr);
                    if candidate_fval.val == SENTINEL_VALUE {
                        return Err(dest_idx);
                    }
                    if candidate_fval.val < NUM_FIX_V || candidate_fkey < NUM_FIX_K {
                        // Do not temper with non value slot, try next one
                        continue;
                    }
                    let candidate_key_digest = key_digest(candidate_fkey);
                    if candidate_key_digest == 0 || candidate_key_digest == target_key_digest {
                        // Don't swap with key that have the selected digest
                        // such that write swap can always detect slot shifting by hopsotch
                        // This should be rare but we need to handle it
                        // Also don't need to check the key changed or not after we fetched the value
                        // because digest would prevent the CAS below
                        continue;
                    }
                    // First claim this candidate
                    if !Self::cas_value(candidate_addr, candidate_fval.val, FORWARD_SWAPPING_VALUE)
                        .1
                    {
                        // The slot value have been changed, retry
                        iter.redo();
                        continue;
                    }

                    // Update the hop bits
                    let curr_candidate_distance = hop_distance(idx, curr_idx, cap);
                    chunk.set_hop_bit(idx, curr_candidate_distance);

                    // Starting to copy it co current idx
                    let curr_addr = chunk.entry_addr(curr_idx);
                    // Start from key object in the attachment
                    let candidate_attachment = chunk.attachment.prefetch(candidate_idx);
                    let candidate_key = candidate_attachment.get_key();
                    let curr_attachment = chunk.attachment.prefetch(curr_idx);
                    // And the key object
                    // Then swap the key
                    curr_attachment.set_key(candidate_key);
                    Self::store_key(curr_addr, candidate_fkey);

                    // Enable probing on the candidate with inserting key
                    key.map(|key| candidate_attachment.set_key(key.clone()));
                    Self::store_key(candidate_addr, fkey);

                    chunk.unset_hop_bit(idx, candidate_distance);

                    // Discard swapping value on current address by replace it with new value
                    let primed_candidate_val =
                        syn_val_digest(candidate_key_digest, candidate_fval.val);
                    Self::store_value(curr_addr, primed_candidate_val);

                    //Here we had candidate copied. Need to work on the candidate slot
                    // First check if it is already in range of home neighbourhood
                    let target_hop_distance = hop_distance(home_idx, candidate_idx, cap);
                    if target_hop_distance < NUM_HOPS {
                        // In range, fill the candidate slot with our key and values
                        Self::store_value(candidate_addr, fval);
                        // chunk.incr_hop_ver(home_idx);
                        chunk.set_hop_bit(home_idx, target_hop_distance);
                        fence(AcqRel);
                        return Ok(candidate_idx);
                    } else {
                        // Not in range, need to swap it closer
                        // MUST change the value to swapping backward to avoid duplication
                        Self::store_value(candidate_addr, BACKWARD_SWAPPING_VALUE);
                        dest_idx = candidate_idx;
                        continue 'SWAPPING;
                    }
                }
                break;
            }
            return Err(dest_idx);
        }
    }

    pub fn out_of_hop_range(
        home_idx: usize,
        dest_idx: usize,
        candidate_idx: usize,
        capacity: usize,
    ) -> bool {
        if home_idx < dest_idx {
            // No wrap
            if candidate_idx >= home_idx && candidate_idx < dest_idx {
                return false;
            } else {
                return true;
            }
        } else {
            // Wrapped
            if candidate_idx >= home_idx && candidate_idx < capacity {
                return false;
            } else if candidate_idx < dest_idx {
                return false;
            } else {
                return true;
            }
        }
    }

    /// Failed return old shared
    fn check_migration<'a>(
        &self,
        old_chunk_ptr: Shared<'a, ChunkPtr<K, V, A, ALLOC>>,
        old_chunk_ref: &ChunkPtr<K, V, A, ALLOC>,
        guard: &crossbeam_epoch::Guard,
    ) -> ResizeResult {
        let occupation = old_chunk_ref.occupation.load(Relaxed);
        let occu_limit = old_chunk_ref.occu_limit;
        if occupation < occu_limit {
            return ResizeResult::NoNeed;
        }
        self.do_migration(old_chunk_ptr, guard)
    }

    fn do_migration<'a>(
        &self,
        old_chunk_ptr: Shared<'a, ChunkPtr<K, V, A, ALLOC>>,
        guard: &crossbeam_epoch::Guard,
    ) -> ResizeResult {
        if old_chunk_ptr.tag() != 0 {
            return ResizeResult::SwapFailed;
        }
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
            cap
        };
        // Swap in old chunk as placeholder for the lock
        let old_chunk_lock = old_chunk_ptr.with_tag(1);
        if let Err(_) =
            self.meta
                .chunk
                .compare_exchange(old_chunk_ptr, old_chunk_lock, AcqRel, Relaxed, guard)
        {
            // other thread have allocated new chunk and wins the competition, exit
            trace!("Cannot obtain lock for resize, will retry");
            return ResizeResult::SwapFailed;
        }
        let old_occupation = old_cap;
        trace!(
            "--- Resizing {:?}. New size is {}, was {}",
            old_chunk_ptr,
            new_cap,
            old_cap,
        );
        let new_chunk = Chunk::alloc_chunk(new_cap, &self.attachment_init_meta);
        unsafe {
            (*new_chunk).occupation.store(old_occupation, Relaxed);
        }
        let new_chunk_ptr = Owned::new(ChunkPtr::new(new_chunk))
            .into_shared(guard)
            .with_tag(0);
        debug_assert_eq!(self.meta.new_chunk.load(Acquire, guard), Shared::null());
        self.meta.new_chunk.store(new_chunk_ptr, Release); // Stump becasue we have the lock already
        self.meta.epoch.fetch_add(1, AcqRel);
        let meta = self.meta.clone();
        let old_chunk_addr = old_chunk_ptr.into_usize();
        let new_chunk_addr = new_chunk_ptr.into_usize();
        let old_chunk_lock = old_chunk_lock.into_usize();
        let meta_addr = Arc::into_raw(meta) as usize;
        // Not going to take multithreading resize
        // Experiments shows there is no significant improvement in performance
        trace!("Initialize migration");
        thread::Builder::new()
            .name(format!(
                "map-migration-{}-{}",
                old_chunk_addr, new_chunk_addr
            ))
            .spawn(move || {
                Self::migrate_with_thread(
                    meta_addr,
                    old_chunk_addr,
                    new_chunk_addr,
                    old_chunk_lock,
                    old_occupation,
                );
            })
            .unwrap();
        // Self::migrate_with_thread(
        //     meta_addr,
        //     old_chunk_addr,
        //     new_chunk_addr,
        //     old_chunk_lock,
        // );
        ResizeResult::InProgress
    }

    fn migrate_with_thread(
        meta_addr: usize,
        old_chunk_ptr: usize,
        new_chunk_ptr: usize,
        old_chunk_lock: usize,
        old_occupation: usize,
    ) {
        let guard = crossbeam_epoch::pin();
        let meta = unsafe {
            Arc::<ChunkMeta<K, V, A, ALLOC>>::from_raw(
                meta_addr as *const ChunkMeta<K, V, A, ALLOC>,
            )
        };
        let old_chunk_ptr =
            unsafe { Shared::<ChunkPtr<K, V, A, ALLOC>>::from_usize(old_chunk_ptr) };
        let new_chunk_ptr =
            unsafe { Shared::<ChunkPtr<K, V, A, ALLOC>>::from_usize(new_chunk_ptr) };
        let old_chunk_lock =
            unsafe { Shared::<ChunkPtr<K, V, A, ALLOC>>::from_usize(old_chunk_lock) };
        let new_chunk_ins = unsafe { new_chunk_ptr.deref() };
        let old_chunk_ins = unsafe { old_chunk_ptr.deref() };
        Self::migrate_entries(old_chunk_ins, new_chunk_ins, old_occupation, &guard);
        let swap_chunk = meta.chunk.compare_exchange(
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
        meta.epoch.fetch_add(1, AcqRel);
        meta.new_chunk.store(Shared::null(), Release);
        trace!(
            "!!! Migration for {:?} completed, new chunk is {:?}, size from {} to {}",
            old_chunk_ptr,
            new_chunk_ptr,
            old_chunk_ins.capacity,
            new_chunk_ins.capacity
        );
        unsafe {
            guard.defer_destroy(old_chunk_ptr);
            guard.flush();
        }
    }

    fn migrate_entries(
        old_chunk_ins: &Chunk<K, V, A, ALLOC>,
        new_chunk_ins: &Chunk<K, V, A, ALLOC>,
        old_occupation: usize,
        _guard: &crossbeam_epoch::Guard,
    ) -> usize {
        trace!(
            "Migrating entries from {:?} to {:?}",
            old_chunk_ins.base,
            new_chunk_ins.base
        );
        let mut old_address = old_chunk_ins.base as usize;
        let boundary = old_address + chunk_size_of(old_chunk_ins.capacity);
        let mut effective_copy = 0;
        let mut idx = 0;
        let backoff = crossbeam_utils::Backoff::new();
        while old_address < boundary {
            // iterate the old chunk to extract entries that is NOT empty
            let fkey = Self::get_fast_key(old_address);
            let fvalue = Self::get_fast_value(old_address);
            debug_assert_eq!(old_address, old_chunk_ins.entry_addr(idx));
            // Reasoning value states
            trace!(
                "Migrating entry have key {}",
                Self::get_fast_key(old_address)
            );
            if fkey != DISABLED_KEY {
                match fvalue.val {
                    EMPTY_VALUE | TOMBSTONE_VALUE => {
                        // Probably does not need this anymore
                        // Need to make sure that during migration, empty value always leads to new chunk
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
                    FORWARD_SWAPPING_VALUE | BACKWARD_SWAPPING_VALUE => {
                        backoff.spin();
                        continue;
                    }
                    SENTINEL_VALUE => {
                        // Sentinel, skip
                        // Sentinel in old chunk implies its new value have already in the new chunk
                        // It can also be other thread have moved this key-value pair to the new chunk
                    }
                    _ => {
                        // The values. First check its key is the same
                        if Self::get_fast_key(old_address) != fkey {
                            backoff.spin();
                            continue;
                        }
                        if !Self::migrate_entry(
                            fkey,
                            idx,
                            fvalue,
                            old_chunk_ins,
                            new_chunk_ins,
                            old_address,
                            &mut effective_copy,
                        ) {
                            backoff.spin();
                            continue;
                        }
                    }
                }
            }
            old_address += ENTRY_SIZE;
            idx += 1;
        }
        // resize finished, make changes on the numbers
        if effective_copy > old_occupation {
            let delta = effective_copy - old_occupation;
            new_chunk_ins.occupation.fetch_add(delta, Relaxed);
            debug!(
                "Occupation {}-{} offset {}",
                effective_copy, old_occupation, delta
            );
        } else if effective_copy < old_occupation {
            let delta = old_occupation - effective_copy;
            new_chunk_ins.occupation.fetch_sub(delta, Relaxed);
            debug!(
                "Occupation {}-{} offset neg {}",
                effective_copy, old_occupation, delta
            );
        } else {
            debug!(
                "Occupation {}-{} zero offset",
                effective_copy, old_occupation
            );
        }
        trace!("Migrated {} entries to new chunk", effective_copy);
        return effective_copy;
    }

    #[inline(always)]
    fn passive_migrate_entry(
        fkey: FKey,
        old_idx: usize,
        fvalue: FastValue,
        old_chunk_ins: &Chunk<K, V, A, ALLOC>,
        new_chunk_ins: &Chunk<K, V, A, ALLOC>,
        old_address: usize,
    ) {
        // Note: This does not make migration faster
        // if fvalue.val < NUM_FIX_V {
        //     // Value have no key, insertion in progress
        //     return;
        // }
        // let mut num_moved = 0;
        // Self::migrate_entry(
        //     fkey,
        //     old_idx,
        //     fvalue,
        //     old_chunk_ins,
        //     new_chunk_ins,
        //     old_address,
        //     &mut num_moved,
        // );
        // new_chunk_ins.occupation.fetch_add(num_moved, AcqRel);
    }

    fn migrate_entry(
        fkey: FKey,
        old_idx: usize,
        fvalue: FastValue,
        old_chunk_ins: &Chunk<K, V, A, ALLOC>,
        new_chunk_ins: &Chunk<K, V, A, ALLOC>,
        old_address: usize,
        effective_copy: &mut usize,
    ) -> bool {
        // Will not migrate meta keys
        if fkey < NUM_FIX_K {
            return true;
        }
        // Insert entry into new chunk, in case of failure, skip this entry
        // Value should be locked
        let curr_orig = fvalue.act_val::<V>();
        let primed_orig = fvalue.prime();

        // Prime the old address to avoid modification
        if !Self::cas_value(old_address, fvalue.val, primed_orig.val).1 {
            // here the ownership have been taken by other thread
            trace!("Entry {} has changed", fkey);
            return false;
        }

        let hash = if Self::WORD_KEY {
            hash_key::<_, H>(&fkey)
        } else {
            fkey
        };

        let cap_mask = new_chunk_ins.cap_mask();
        let home_idx = hash & cap_mask;
        let reiter = || new_chunk_ins.iter_slot_skipable(home_idx, false);
        let mut iter = reiter();
        let (mut idx, mut count) = iter.next().unwrap();
        loop {
            let addr = new_chunk_ins.entry_addr(idx);
            let k = Self::get_fast_key(addr);
            if k == fkey {
                let new_attachment = new_chunk_ins.attachment.prefetch(idx);
                let old_attachment = old_chunk_ins.attachment.prefetch(old_idx);
                let key = old_attachment.get_key();
                if Self::get_fast_key(addr) != k {
                    continue;
                }
                if new_attachment.probe(&key) {
                    // New value existed, skip with None result
                    // We also need to drop the fvalue we obtained because it does not fit any where
                    old_chunk_ins
                        .attachment
                        .manually_drop(fvalue.act_val::<V>());
                    // New value in the new chunk, just put a sentinel and abort migration on this slot
                    Self::store_value(old_address, SENTINEL_VALUE);
                    return true;
                }
            } else if k == EMPTY_KEY {
                let hop_adjustment = Self::need_hop_adjustment(new_chunk_ins, None, count);
                let (store_fkey, cas_fval) = if hop_adjustment {
                    // Use empty key to block probing progression for hops
                    (fkey, BACKWARD_SWAPPING_VALUE)
                } else {
                    (fkey, curr_orig)
                };
                if Self::cas_value(addr, EMPTY_VALUE, cas_fval).1 {
                    let new_attachment = new_chunk_ins.attachment.prefetch(idx);
                    let old_attachment = old_chunk_ins.attachment.prefetch(old_idx);
                    let key = old_attachment.get_key();
                    let value = old_attachment.get_value();
                    new_attachment.set_key(key.clone());
                    new_attachment.set_value(value, 0);
                    fence(Acquire);
                    Self::store_key(addr, store_fkey);
                    match Self::adjust_hops(
                        hop_adjustment,
                        new_chunk_ins,
                        fkey,
                        curr_orig,
                        Some(&key),
                        home_idx,
                        idx,
                        count,
                    ) {
                        Err(idx) => {
                            // Nothing else we can do, just take the distant slot
                            let addr = new_chunk_ins.entry_addr(idx);
                            Self::store_value(addr, curr_orig);
                            Self::store_key(addr, fkey);
                        }
                        _ => {}
                    }
                    Self::store_sentinel(old_address);
                    *effective_copy += 1;
                    return true;
                } else {
                    // Here we didn't put the fval into the new chunk due to slot conflict with
                    // other thread. Need to retry
                    warn!("Migrate {} have conflict", fkey);
                    continue;
                }
            } else {
                let v = Self::get_fast_value(addr);
                let raw = v.val;
                if Self::get_fast_key(addr) != k {
                    continue;
                }
                if v.val == BACKWARD_SWAPPING_VALUE {
                    let backoff = crossbeam_utils::Backoff::new();
                    Self::wait_entry(addr, k, raw, &backoff);
                    iter = reiter();
                    continue;
                } else if v.val == FORWARD_SWAPPING_VALUE {
                    let backoff = crossbeam_utils::Backoff::new();
                    Self::wait_entry(addr, k, raw, &backoff);
                    iter.refresh_following(new_chunk_ins);
                    continue;
                }
            }
            if let Some(next) = iter.next() {
                (idx, count) = next;
            } else {
                panic!("Cannot find any slot for migration");
            }
        }
    }

    pub fn map_is_copying(&self) -> bool {
        Self::is_copying(self.now_epoch())
    }

    #[inline]
    pub fn hash(fkey: FKey, key: &K) -> (FKey, usize) {
        if Self::WORD_KEY {
            debug_assert!(fkey > 0);
            (fkey, hash_key::<_, H>(&fkey))
        } else {
            let hash = hash_key::<_, H>(key);
            (hash as FKey, hash)
        }
    }

    #[inline]
    pub fn get_hash(&self, fkey: FKey, key: &K) -> (FKey, usize) {
        Self::hash(fkey, key)
    }

    #[inline]
    const fn if_fat_val_then_val<T: Copy>(then: T, els: T) -> T {
        if Self::FAT_VAL {
            then
        } else {
            els
        }
    }
}

#[derive(Copy, Clone)]
pub struct FastValue {
    pub val: FVal,
}

impl FastValue {
    #[inline(always)]
    pub fn new(val: FVal) -> Self {
        Self { val }
    }

    #[inline(always)]
    pub fn act_val<V>(self) -> FVal {
        if mem::size_of::<V>() != 0 {
            self.val & FVAL_VAL_BIT_MASK
        } else {
            self.val & VAL_BIT_MASK
        }
    }

    #[inline(always)]
    fn next_version(old: FVal, new: FVal) -> FVal {
        let new_ver = (old | FVAL_VAL_BIT_MASK).wrapping_add(1);
        new & FVAL_VAL_BIT_MASK | (new_ver & FVAL_VER_BIT_MASK)
    }

    #[inline(always)]
    fn is_locked(self) -> bool {
        let v = self.val;
        v == LOCKED_VALUE
    }

    #[inline(always)]
    fn is_primed(self) -> bool {
        let v = self.val;
        v & VAL_BIT_MASK != v
    }

    #[inline(always)]
    fn is_swapping(self) -> bool {
        let v = self.val;
        v == FORWARD_SWAPPING_VALUE || v == BACKWARD_SWAPPING_VALUE
    }

    #[inline(always)]
    fn prime(self) -> Self {
        Self {
            val: self.val | VAL_PRIME_MASK,
        }
    }

    #[inline(always)]
    fn is_valued(self) -> bool {
        self.val > TOMBSTONE_VALUE
    }
}

impl<K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> Chunk<K, V, A, ALLOC> {
    const FAT_VAL: bool = mem::size_of::<V>() != 0;

    fn alloc_chunk(capacity: usize, attachment_meta: &A::InitMeta) -> *mut Self {
        let self_size = mem::size_of::<Self>();
        let self_align = align_padding(self_size, 8);
        let self_size_aligned = self_size + self_align;
        let chunk_size = chunk_size_of(capacity);
        let chunk_align = align_padding(chunk_size, 8);
        let chunk_size_aligned = chunk_size + chunk_align;
        let attachment_heap = A::heap_size_of(capacity);
        let hop_size = mem::size_of::<HopTuple>() * capacity;
        let hop_align = align_padding(hop_size, 8);
        let hop_size_aligned = hop_size + hop_align;
        let total_size =
            self_size_aligned + chunk_size_aligned + hop_size_aligned + attachment_heap;
        let ptr = alloc_mem::<ALLOC>(total_size) as *mut Self;
        let addr = ptr as usize;
        let data_base = addr + self_size_aligned;
        let hop_base = data_base + chunk_size_aligned;
        let attachment_base = hop_base + hop_size_aligned;
        unsafe {
            fill_zeros(data_base, chunk_size_aligned + hop_size_aligned);
            ptr::write(
                ptr,
                Self {
                    base: data_base,
                    capacity,
                    occupation: AtomicUsize::new(0),
                    empty_entries: AtomicUsize::new(0),
                    occu_limit: occupation_limit(capacity),
                    total_size,
                    hop_base,
                    attachment: A::new(attachment_base, attachment_meta),
                    shadow: PhantomData,
                },
            )
        };
        ptr
    }

    unsafe fn gc(ptr: *mut Chunk<K, V, A, ALLOC>) {
        debug_assert_ne!(ptr as usize, 0);
        let chunk = &*ptr;
        // chunk.gc_entries();
        dealloc_mem::<ALLOC>(ptr as usize, chunk.total_size);
    }

    #[warn(dead_code)]
    fn gc_entries(&self) {
        let mut old_address = self.base as usize;
        let boundary = old_address + chunk_size_of(self.capacity);
        let mut idx = 0;
        while old_address < boundary {
            let fvalue = Self::get_fast_value(old_address);
            let fkey = Self::get_fast_key(old_address);
            let val = fvalue.val;
            if fkey != EMPTY_KEY && val >= NUM_FIX_V {
                let attachment = self.attachment.prefetch(idx);
                attachment.erase(val);
            }
            old_address += ENTRY_SIZE;
            idx += 1;
        }
    }

    #[inline(always)]
    fn get_fast_key(entry_addr: usize) -> FKey {
        debug_assert!(entry_addr > 0);
        unsafe { intrinsics::atomic_load_acquire(entry_addr as *mut FKey) }
    }

    #[inline(always)]
    fn get_fast_value(entry_addr: usize) -> FastValue {
        debug_assert!(entry_addr > 0);
        let addr = entry_addr + mem::size_of::<FKey>();
        let val = unsafe { intrinsics::atomic_load_acquire(addr as *mut FVal) };
        FastValue::new(val)
    }

    #[inline(always)]
    fn entry_addr(&self, idx: usize) -> usize {
        self.base + idx * ENTRY_SIZE
    }

    #[inline(always)]
    fn cap_mask(&self) -> usize {
        self.capacity - 1
    }

    #[inline(always)]
    fn get_hop_bits(&self, idx: usize) -> HopBits {
        let addr = self.hop_base + HOP_TUPLE_BYTES * idx;
        unsafe { intrinsics::atomic_load_acquire(addr as *mut HopBits) }
    }

    #[inline(always)]
    fn set_hop_bit(&self, idx: usize, pos: usize) {
        let ptr = (self.hop_base + HOP_TUPLE_BYTES * idx) as *mut HopBits;
        let set_bit = 1 << pos;
        unsafe {
            debug_assert!(
                intrinsics::atomic_load_acquire(ptr) & set_bit == 0,
                "bit already set for idx {}, pos {}",
                idx,
                pos
            );
            intrinsics::atomic_or_relaxed(ptr, set_bit);
        }
    }

    #[inline(always)]
    fn unset_hop_bit(&self, idx: usize, pos: usize) {
        let ptr = (self.hop_base + HOP_TUPLE_BYTES * idx) as *mut HopBits;
        let unset_bit = 1 << pos;
        unsafe {
            debug_assert!(
                intrinsics::atomic_load_acquire(ptr) & unset_bit > 0,
                "bit not set for idx {}, pos {}, read {:b}",
                idx,
                pos,
                intrinsics::atomic_load_acquire(ptr)
            );
            intrinsics::atomic_and_relaxed(ptr, !unset_bit);
        }
    }

    #[inline(always)]
    fn swap_hop_bit(&self, idx: usize, src_pos: usize, dest_pos: usize) {
        let ptr = (self.hop_base + HOP_TUPLE_BYTES * idx) as *mut HopBits;
        let set_bit = 1 << dest_pos;
        let unset_mask = !(1 << src_pos);
        loop {
            unsafe {
                let orig_bits = intrinsics::atomic_load_acquire(ptr);
                let target_bits = orig_bits | set_bit & unset_mask;
                if intrinsics::atomic_cxchg_relaxed(ptr, orig_bits, target_bits).1 {
                    return;
                }
            }
        }
    }

    #[inline(always)]
    fn iter_slot_skipable<'a>(&self, home_idx: usize, skip: bool) -> SlotIter {
        let hop_bits = if !Self::FAT_VAL && ENABLE_SKIPPING && skip {
            self.get_hop_bits(home_idx)
        } else {
            0
        };
        let pos = 0;
        SlotIter {
            num_probed: 0,
            cap_mask: self.cap_mask(),
            home_idx,
            hop_bits,
            pos,
            terminal: false,
        }
    }
}

impl<K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Clone
    for Table<K, V, A, ALLOC, H>
{
    fn clone(&self) -> Self {
        let new_table = Table {
            meta: Arc::new(ChunkMeta {
                chunk: Default::default(),
                new_chunk: Default::default(),
                epoch: AtomicUsize::new(0),
            }),
            count: AtomicUsize::new(0),
            init_cap: self.init_cap,
            attachment_init_meta: self.attachment_init_meta.clone(),
            mark: PhantomData,
        };
        let guard = crossbeam_epoch::pin();
        let old_chunk_ptr = self.meta.chunk.load(Acquire, &guard);
        let new_chunk_ptr = self.meta.new_chunk.load(Acquire, &guard);
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
            new_table.meta.chunk.store(cloned_old_ref, Release);

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
                new_table.meta.new_chunk.store(cloned_new_ref, Release);
            } else {
                new_table.meta.new_chunk.store(Shared::null(), Release);
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
            guard.defer_destroy(self.meta.chunk.load(Acquire, &guard));
            let new_chunk_ptr = self.meta.new_chunk.load(Acquire, &guard);
            if !new_chunk_ptr.is_null() {
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

#[inline]
fn dealloc_mem<A: GlobalAlloc + Default + Default>(ptr: usize, size: usize) {
    let align = 64;
    let layout = Layout::from_size_align(size, align).unwrap();
    let alloc = A::default();
    unsafe { alloc.dealloc(ptr as *mut u8, layout) }
}

#[inline]
fn chunk_size_of(cap: usize) -> usize {
    cap * ENTRY_SIZE
}

impl<V> Debug for ModResult<V> {
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

#[inline(always)]
fn syn_val_digest(digest: FVal, val: FVal) -> FVal {
    let res = val & VAL_BIT_MASK | digest;
    debug!(
        "Synthesis value from digest {:064b}, value {:064b} to {:064b}",
        digest, val, res
    );
    res
}

#[inline(always)]
fn syn_val_key(key: FVal, val: FVal) -> FVal {
    val & VAL_BIT_MASK | key_digest(key)
}

#[inline(always)]
fn key_digest(key: FKey) -> FKey {
    key & VAL_KEY_DIGEST_MASK
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn const_check() {
        println!("KEY Digest mask is {:064b}", KEY_DIGEST_MASK);
        println!("VAL Digest mask is {:064b}", VAL_KEY_DIGEST_MASK);
        assert_eq!(VAL_KEY_DIGEST_MASK.count_ones() as usize, KEY_DIGEST_DIGITS);
    }
}

struct SlotIter {
    home_idx: usize,
    pos: usize,
    hop_bits: HopBits,
    cap_mask: usize,
    num_probed: usize,
    terminal: bool,
}

impl Iterator for SlotIter {
    type Item = (usize, usize);

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        let probed = self.num_probed;
        if probed > self.cap_mask {
            return None;
        };
        self.num_probed += 1;
        let pos = self.pos;
        let bits = self.hop_bits;
        if bits == 0 {
            self.pos += 1;
            let rt = (self.home_idx + pos) & self.cap_mask;
            self.terminal = true;
            Some((rt, pos))
        } else {
            // Find the bumps from the bits
            let tailing = self.hop_bits.trailing_zeros() as usize;
            self.pos = tailing + 1;
            self.hop_bits &= !(1 << tailing);
            let rt = (self.home_idx + tailing) & self.cap_mask;
            Some((rt, tailing))
        }
    }
}

fn hop_distance(home_idx: usize, curr_idx: usize, cap: usize) -> usize {
    if curr_idx > home_idx {
        curr_idx - home_idx
    } else {
        curr_idx + (cap - home_idx)
    }
}

impl SlotIter {
    #[inline(always)]
    fn redo(&mut self) {
        if self.terminal {
            self.pos -= 1;
        } else {
            self.hop_bits |= 1 << self.pos;
        }
    }

    #[inline(always)]
    fn refresh_following<K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default>(
        &mut self,
        chunk: &Chunk<K, V, A, ALLOC>,
    ) {
        if self.hop_bits == 0 || self.hop_bits == !(!0 << 1 >> 1) {
            return;
        }
        let checked = self.hop_bits.trailing_zeros();
        let new_bits = chunk.get_hop_bits(self.home_idx);
        self.hop_bits = new_bits >> checked << checked;
    }
}
