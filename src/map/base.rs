use std::{
    cell::RefCell,
    cmp::{max, min},
    collections::VecDeque,
    sync::Arc,
    thread,
};

#[cfg(debug_assertions)]
use parking_lot::Mutex;

use itertools::Itertools;

use crate::thread_id;

use super::*;

pub struct EntryTemplate(FKey, FVal);
pub type HopBits = u32;
pub type HopVer = ();
pub type HopTuple = (HopBits, HopVer);

const HOP_TUPLE_SIZE: usize = mem::size_of::<HopTuple>();

#[cfg(debug_assertions)]
pub type MigratedEntry = ((usize, FastValue), usize, usize, u64);

pub const ENABLE_HOPSOTCH: bool = cfg!(feature = "hopsotch");
pub const ENABLE_SKIPPING: bool = true & ENABLE_HOPSOTCH;

pub const EMPTY_KEY: FKey = 0;
pub const DISABLED_KEY: FKey = 1;

pub const EMPTY_VALUE: FVal = 0b000;
pub const SENTINEL_VALUE: FVal = 0b010;

// Last bit set indicates locking
pub const LOCKED_VALUE: FVal = 0b001;
pub const BACKWARD_SWAPPING_VALUE: FVal = 0b011;
pub const FORWARD_SWAPPING_VALUE: FVal = 0b101;

pub const TOMBSTONE_VALUE: FVal = 0b110;

pub const MAX_META_VAL: FVal = TOMBSTONE_VALUE;
pub const MAX_META_KEY: FKey = DISABLED_KEY;
pub const PLACEHOLDER_VAL: FVal = MAX_META_VAL + 2;
pub const RAW_START_IDX: usize = MAX_META_VAL + 1;

pub const RAW_KV_OFFSET: usize = RAW_START_IDX;
pub const PTR_KV_OFFSET: usize = 0;

pub const HEADING_BIT: FVal = !(!0 << 1 >> 1);
pub const VAL_BIT_MASK: FVal = VAL_PRIME_VAL_MASK & (!VAL_KEY_DIGEST_MASK);
pub const VAL_PRIME_BIT: FVal = HEADING_BIT;
pub const VAL_PRIME_VAL_MASK: FVal = !VAL_PRIME_BIT;
pub const ENTRY_SIZE: usize = mem::size_of::<EntryTemplate>();
pub const VAL_MUTEX_BIT: FVal = HEADING_BIT >> 1; // The second heading bit
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

pub const HOP_BYTES: usize = mem::size_of::<HopBits>();
pub const HOP_TUPLE_BYTES: usize = mem::size_of::<HopTuple>();
pub const NUM_HOPS: usize = HOP_BYTES * 8;
pub const ALL_HOPS_TAKEN: HopBits = !0;

const DELAY_LOG_CAP: usize = 10;
#[cfg(debug_assertions)]
thread_local! {
    static DELAYED_LOG: RefCell<VecDeque<String>> = RefCell::new(VecDeque::new());
}

#[cfg(debug_assertions)]
lazy_static! {
    static ref MIGRATION_LOGS: Mutex<Vec<(usize, Vec<MigratedEntry>)>> = Mutex::new(vec![]);
}

enum ModResult<V> {
    Replaced(FVal, Option<V>, usize), // (origin fval, val, index)
    Existed(FVal, Option<V>, usize),
    Fail,
    Sentinel,
    NotFound,
    Done(FVal, Option<V>, usize), // _, value, index
    TableFull,
    Aborted(FVal),
}

enum ModOp<'a, V> {
    Insert(FVal, Option<&'a V>),
    AttemptInsert(FVal, Option<&'a V>),
    SwapFastVal(Box<dyn Fn(FVal) -> Option<FVal>>),
    Sentinel,
    Tombstone,
    Lock,
}

pub enum InsertOp {
    Insert,
    TryInsert,
}

enum ResizeResult {
    NoNeed,
    SwapFailed,
    InProgress,
}

pub enum SwapResult<'a, K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> {
    Succeed(FVal, usize, ChunkPtr<'a, K, V, A, ALLOC>),
    Aborted(FVal),
    NotFound,
    Failed,
}

pub struct Chunk<K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> {
    capacity: usize,
    base: usize,
    occu_limit: usize,
    occupation: AtomicUsize,
    empty_entries: AtomicUsize,
    total_size: usize,
    hop_base: usize,
    epoch: AtomicUsize,
    pub attachment: A,
    shadow: PhantomData<(K, V, ALLOC)>,
}

pub struct Table<
    K,
    V,
    A: Attachment<K, V>,
    ALLOC: GlobalAlloc + Default,
    H: Hasher + Default,
    const K_OFFSET: usize,
    const V_OFFSET: usize,
> {
    meta: Arc<ChunkMeta>,
    attachment_init_meta: A::InitMeta,
    count: AtomicUsize,
    init_cap: usize,
    max_cap: usize,
    mark: PhantomData<(H, ALLOC)>,
}

pub struct ChunkPtr<'a, K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> {
    ptr: *mut Chunk<K, V, A, ALLOC>,
    _marker: PhantomData<&'a ()>,
}

impl <'a, K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> Clone for ChunkPtr<'a, K, V, A, ALLOC> {
    fn clone(&self) -> Self {
        Self { ptr: self.ptr, _marker: PhantomData }
    }
}

impl <'a, K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> Copy for ChunkPtr<'a, K, V, A, ALLOC> {

}

struct ChunkMeta {
    current_chunks: AtomicUsize,
    history_chunks: AtomicUsize,
    epoch: AtomicUsize,
}

struct ChunkArray<K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> {
    len: usize,
    hash_masking: usize,
    max_cap: usize,
    max_cap_shift: u32,
    _marker: PhantomData<(K, V, A, ALLOC)>, // Rest of the space belongs to array of pointers `*mut Chunk<K, V, A, ALLOC>`
}

macro_rules! delay_log {
    ($($arg:tt)+) => {
        #[cfg(debug_assertions)]
        DELAYED_LOG.with(|cell| {
            let mut list = cell.borrow_mut();
            list.push_back(format!($($arg)+));
            if list.len() > DELAY_LOG_CAP {
                list.pop_front();
            }
        })
    };
}

impl<K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> ChunkArray<K, V, A, ALLOC> {
    fn new(len: usize, max_cap: usize) -> *mut Self {
        debug_assert!(max_cap.is_power_of_two());
        debug_assert!(len.is_power_of_two());
        let obj_size = mem::size_of::<Self>() + len * mem::size_of::<usize>();
        let raw_ptr = unsafe { libc::malloc(obj_size) };
        let arr_ptr = raw_ptr as _;
        let max_cap_shift = max_cap.trailing_zeros();
        let hash_masking = (len - 1) << max_cap_shift;
        unsafe {
            libc::memset(raw_ptr, 0, obj_size);
            ptr::write(
                arr_ptr,
                Self {
                    len,
                    max_cap_shift,
                    hash_masking,
                    max_cap,
                    _marker: PhantomData,
                },
            );
        }
        return arr_ptr;
    }

    fn chunk_ptr_addr_of(&self, chunk_id: usize) -> usize {
        debug_assert!(chunk_id < self.len);
        let base_addr = self as *const Self as usize + mem::size_of::<Self>();
        return base_addr + chunk_id * mem::size_of::<usize>();
    }

    fn chunk_at<'a>(
        &self,
        chunk_id: usize,
        _guard: &'a Guard,
    ) -> Option<ChunkPtr<'a, K, V, A, ALLOC>> {
        unsafe {
            let chunk_addr = self.chunk_ptr_addr_of(chunk_id);
            let chunk_ptr = intrinsics::atomic_load_acquire(chunk_addr as *const usize) as usize;
            if chunk_ptr > 0 {
                Some(ChunkPtr::new(chunk_ptr as _))
            } else {
                None
            }
        }
    }

    fn swap_in_chunk(&self, chunk_id: usize, chunk_ptr: ChunkPtr<K, V, A, ALLOC>) -> bool {
        unsafe {
            let chunk_addr = self.chunk_ptr_addr_of(chunk_id);
            return intrinsics::atomic_cxchg_acqrel_relaxed(
                chunk_addr as _,
                0,
                chunk_ptr.ptr as usize,
            )
            .1;
        }
    }

    fn set_chunk(&self, chunk_id: usize, chunk_ptr: ChunkPtr<K, V, A, ALLOC>) {
        unsafe {
            let chunk_addr = self.chunk_ptr_addr_of(chunk_id);
            intrinsics::atomic_store_release(chunk_addr as *mut _, chunk_ptr.ptr as usize);
        }
    }

    fn erase_chunk<'a>(&self, chunk_id: usize, guard: &'a Guard) {
        unsafe {
            let chunk_addr = self.chunk_ptr_addr_of(chunk_id);
            let old_addr = intrinsics::atomic_xchg_acqrel(chunk_addr as *mut _, 0usize);
            guard.defer_unchecked(move || {
                (ChunkPtr::<'a, K, V, A, ALLOC> {
                    ptr: old_addr as _,
                    _marker: PhantomData,
                })
                .destory();
            });
        }
    }

    fn clear<'a>(&self, guard: &'a Guard) {
        for i in (0..self.len).rev() {
            let chunk_addr = self.chunk_ptr_addr_of(i);
            if chunk_addr > 0 {
                unsafe {
                    let old_addr = intrinsics::atomic_xchg_acqrel(chunk_addr as *mut _, 0usize);
                    guard.defer_unchecked(move || {
                        (ChunkPtr::<'a, K, V, A, ALLOC> {
                            ptr: old_addr as _,
                            _marker: PhantomData,
                        })
                        .destory();
                    });
                }
            }
        }
    }

    fn ref_from_addr<'a>(addr: usize) -> &'a Self {
        unsafe { &*(addr as *const Self) }
    }

    fn id_of_hash(&self, hash: usize) -> usize {
        (hash & self.hash_masking) >> self.max_cap_shift
    }

    fn hash_chunk<'a>(
        &self,
        hash: usize,
        guard: &'a Guard,
    ) -> Option<ChunkPtr<'a, K, V, A, ALLOC>> {
        self.chunk_at(self.id_of_hash(hash), guard)
    }

    fn key_capacity<'a>(&self, guard: &'a Guard) -> usize {
        self.iter_chunks(guard).map(|chunk| chunk.capacity).sum()
    }

    fn iter_chunks<'a, 'b>(
        &'a self,
        guard: &'a Guard,
    ) -> impl Iterator<Item = ChunkPtr<'a, K, V, A, ALLOC>> {
        (0..self.len).filter_map(move |i| self.chunk_at(i, guard))
    }
}

impl<
        K: Clone + Hash + Eq,
        V: Clone,
        A: Attachment<K, V>,
        ALLOC: GlobalAlloc + Default,
        H: Hasher + Default,
        const K_OFFSET: usize,
        const V_OFFSET: usize,
    > Table<K, V, A, ALLOC, H, K_OFFSET, V_OFFSET>
{
    const FAT_VAL: bool = mem::size_of::<V>() != 0;
    const WORD_KEY: bool = mem::size_of::<K>() == 0;

    pub fn with_capacity(cap: usize, attachment_init_meta: A::InitMeta) -> Self {
        Self::with_max_capacity(cap, max(cap * 4, 4096), attachment_init_meta)
    }

    pub fn with_max_capacity(
        cap: usize,
        max_cap: usize,
        attachment_init_meta: A::InitMeta,
    ) -> Self {
        trace!("Creating chunk with capacity {}", cap);
        if !is_power_of_2(cap) {
            error!("Capacity is not power of 2, got {}", cap);
            panic!("Capacity is not power of 2, got {}", cap);
        }
        // Each entry key value pair is 2 words
        // steal 1 bit in the MSB of value indicate Prime(1)
        let chunk = Chunk::<K, V, A, ALLOC>::alloc_chunk(cap, 0, &attachment_init_meta);
        let current_chunk_array = ChunkArray::new(1, max_cap);
        unsafe {
            (*current_chunk_array).set_chunk(0, ChunkPtr::new(chunk));
        }
        let history_chunk_array = ChunkArray::<K, V, A, ALLOC>::new(1, max_cap);
        Self {
            meta: Arc::new(ChunkMeta {
                current_chunks: AtomicUsize::new(current_chunk_array as usize),
                history_chunks: AtomicUsize::new(history_chunk_array as usize),
                epoch: AtomicUsize::new(0),
            }),
            count: AtomicUsize::new(0),
            init_cap: cap,
            max_cap,
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
            let (chunk, new_chunk, epoch) = self.chunk_refs(hash, guard);
            let mut v;
            if let Some((mut val, addr, aitem)) = self.get_from_chunk(
                chunk,
                hash,
                key,
                fkey,
                &backoff,
                new_chunk,
                epoch,
            ) {
                'SPIN: loop {
                    v = val.val;
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
                        return Some((Self::offset_v_out(act_val), attachment, addr));
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
                    self.get_from_chunk(new_chunk, hash, key, fkey, &backoff, None, epoch)
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
                            return Some((Self::offset_v_out(act_val), attachment, addr));
                        } else if v == SENTINEL_VALUE {
                            backoff.spin();
                            continue 'OUTER;
                        } else {
                            break 'SPIN_NEW;
                        }
                    }
                }
            }
            let new_epoch = chunk.now_epoch();
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
        let fvalue = Self::offset_v_in(fvalue);
        loop {
            let (chunk, new_chunk, epoch) = self.chunk_refs(hash, &guard);
            // trace!("Insert {} at {:?}-{:?}", fkey, chunk_ptr, new_chunk_ptr);
            if let Some(new_chunk) = new_chunk {
                if new_chunk.occupation.load(Acquire) >= new_chunk.occu_limit {
                    backoff.spin();
                    continue;
                }
            } else {
                match self.check_migration(hash, chunk, epoch, &guard) {
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
                InsertOp::TryInsert => ModOp::AttemptInsert(masked_value, value),
            };
            let lock_old = new_chunk
                .map(|_| self.modify_entry(chunk, hash, key, fkey, ModOp::Lock, true, &guard));
            // match &lock_old {
            //     Some(ModResult::Sentinel)=> {
            //         continue;
            //     }
            //     _ => {}
            // }
            let value_insertion =
                self.modify_entry(modify_chunk, hash, key, fkey, mod_op, true, &guard);
            let result;
            let reset_locked_old = || match &lock_old {
                Some(ModResult::Replaced(fv, _v, addr)) => {
                    Self::store_value(*addr, *fv);
                }
                Some(ModResult::NotFound) | Some(ModResult::Sentinel) | None => {}
                _ => {
                    error!("Invalid modify entry state");
                    unreachable!("Invalid modify entry state");
                }
            };
            match value_insertion {
                ModResult::Done(fv, v, idx) => {
                    self.count.fetch_add(1, Relaxed);
                    delay_log!(
                        "New val insert key {}, fval {} was {}, idx {}",
                        fkey,
                        fvalue,
                        fv,
                        idx
                    );
                    result = Some((0, v))
                }
                ModResult::Replaced(fv, v, idx) | ModResult::Existed(fv, v, idx) => {
                    delay_log!(
                        "Replace val insert key {}, fval {} was {}, idx {}",
                        fkey,
                        fvalue,
                        fv,
                        idx
                    );
                    result = Some((fv, v))
                }
                ModResult::Fail => {
                    // If fail insertion then retry
                    reset_locked_old();
                    backoff.spin();
                    continue;
                }
                ModResult::TableFull => {
                    reset_locked_old();
                    if new_chunk.is_none() {
                        self.do_migration(hash, chunk, epoch, &guard);
                    }
                    backoff.spin();
                    continue;
                }
                ModResult::Sentinel => {
                    trace!("Discovered sentinel on insertion table upon probing, retry");
                    reset_locked_old();
                    backoff.spin();
                    continue;
                }
                ModResult::NotFound => match &lock_old {
                    Some(ModResult::Replaced(fv, _v, addr)) => {
                        Self::store_value(*addr, *fv);
                        backoff.spin();
                        continue;
                    }
                    Some(ModResult::Sentinel) => {
                        backoff.spin();
                        continue;
                    }
                    Some(ModResult::NotFound) | None => {
                        result = None;
                    }
                    _ => {
                        error!("Invalid value insertion state");
                        unreachable!("Invalid value insertion state")
                    }
                },
                ModResult::Aborted(_) => {
                    error!("Should not abort during insertion");
                    unreachable!("Should no abort during insertion")
                }
            }
            let mut res;
            match (result, lock_old) {
                (Some((0, _)), Some(ModResult::Sentinel)) => {
                    delay_log!(
                        "SHOULD NOT REACHABLE!!! Insert have Some((0, _)), Some(ModResult::Sentinel) key {}, old chunk {}, new chunk {:?}, epoch {}",
                        fkey, chunk.base, new_chunk.map(|c| c.base), epoch
                    ); // Should not reachable
                    res = None;
                }
                (Some((fv, v)), Some(ModResult::Sentinel)) => {
                    delay_log!(
                        "Insert have Some(({}, _)), Some(ModResult::Sentinel) key {}, old chunk {}, new chunk {:?}, epoch {}",
                        fv, fkey, chunk.base, new_chunk.map(|c| c.base), epoch
                    );
                    res = Some((fv, v.unwrap()));
                }
                (None, Some(ModResult::Sentinel)) => {
                    backoff.spin();
                    continue;
                }
                (None, Some(ModResult::Replaced(fv, _v, addr))) => {
                    Self::store_value(addr, fv);
                    backoff.spin();
                    continue;
                }
                (None, Some(_)) => {
                    backoff.spin();
                    continue;
                }
                (Some((0, _)), Some(ModResult::Replaced(fv, v, addr))) => {
                    delay_log!(
                        "Insert have Some((0, _)), Some(ModResult::Replaced({}, _, {})) key {}, old chunk {}, new chunk {:?}, epoch {}",
                        fv, addr, fkey, chunk.base, new_chunk.map(|c| c.base), epoch
                    );
                    // New insertion in new chunk and have stuff in old chunk
                    Self::store_sentinel(addr);
                    res = Some((fv, v.unwrap()))
                }
                (Some((0, _)), None) => {
                    delay_log!(
                        "Insert have Some((0, _)), None key {}, old chunk {}, new chunk {:?}, epoch {}",
                        fkey,
                        chunk.base,
                        new_chunk.map(|c| c.base), epoch
                    );
                    res = None;
                }
                (Some((0, _)), Some(ModResult::NotFound)) => {
                    delay_log!(
                        "Insert have Some((0, _)), Some(ModResult::NotFound) key {}, old chunk {}, new chunk {:?}, epoch {}",
                        fkey, chunk.base, new_chunk.map(|c| c.base), epoch
                    );
                    res = None;
                }
                (Some((0, _)), _) => {
                    delay_log!(
                        "Insert have Some((0, _)), _ key {}, old chunk {}, new chunk {:?}, epoch {}",
                        fkey,
                        chunk.base,
                        new_chunk.map(|c| c.base), epoch
                    );
                    res = None;
                }
                (None, None) => {
                    error!("Should not have none none during insertion");
                    unreachable!("Should not have none none during insertion");
                }
                (Some((fv, v)), Some(ModResult::Replaced(lfv, _v, addr))) => {
                    delay_log!(
                        "Insert have Some(({}, _)), Some(ModResult::Replaced({}, _, {})) key {}, old chunk {}, new chunk {:?}, epoch {}",
                        fv, lfv, addr, fkey, chunk.base, new_chunk.map(|c| c.base), epoch
                    );
                    // Replaced new chunk, should put sentinel in old chunk
                    Self::store_sentinel(addr);
                    res = Some((fv, v.unwrap()))
                }
                (Some((fv, v)), _) => {
                    delay_log!(
                        "Insert have Some(({}, _)), Some(_) key {}, old chunk {}, new chunk {:?}, epoch {}",
                        fv, fkey, chunk.base, new_chunk.map(|c| c.base), epoch
                    );
                    res = Some((fv, v.unwrap()))
                }
            }
            match &mut res {
                Some((fv, _)) => {
                    if *fv <= MAX_META_VAL {
                        delay_log!(
                            "*fv <= NUM_FIX_V. Key {} Val {}, old chunk {}, new chunk {:?}",
                            fkey,
                            fv,
                            chunk.base,
                            new_chunk.map(|c| c.base)
                        );
                        return None;
                    } else {
                        *fv = Self::offset_v_out(*fv);
                    }
                }
                None => {}
            }
            return res;
        }
    }

    pub fn clear(&self) {
        let guard = crossbeam_epoch::pin();
        let chunk =
            Chunk::<K, V, A, ALLOC>::alloc_chunk(self.init_cap, 0, &self.attachment_init_meta);
        let chunk_array = ChunkArray::new(1, self.max_cap);
        unsafe {
            (*chunk_array).set_chunk(0, ChunkPtr::new(chunk));
        }
        let len = self.len();
        let old_history_chunk_addr = self.meta.history_chunks.swap(0, AcqRel);
        let old_current_chunk_addr = self.meta.current_chunks.swap(chunk_array as usize, AcqRel);
        Self::chunk_array_at_addr(old_history_chunk_addr).clear(&guard);
        Self::chunk_array_at_addr(old_current_chunk_addr).clear(&guard);
        self.count.fetch_sub(len, AcqRel);
    }

    pub fn swap<'a, F: Fn(FVal) -> Option<FVal> + Copy + 'static>(
        &'a self,
        fkey: FKey,
        key: &K,
        func: F,
        guard: &'a Guard,
    ) -> SwapResult<K, V, A, ALLOC> {
        let (fkey, hash) = Self::hash(fkey, key);
        self.swap_with_hash(fkey, hash, key, func, guard)
    }

    pub fn swap_with_hash<'a, F: Fn(FVal) -> Option<FVal> + Copy + 'static>(
        &'a self,
        fkey: FKey,
        hash: usize,
        key: &K,
        func: F,
        guard: &'a Guard,
    ) -> SwapResult<K, V, A, ALLOC> {
        let backoff = crossbeam_utils::Backoff::new();
        loop {
            let (chunk, new_chunk, epoch) = self.chunk_refs(hash, guard);
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
            );
            match fast_mod_res {
                ModResult::Replaced(fval, _, idx) => {
                    // That's it
                    result = Some((fval, idx, update_chunk));
                }
                ModResult::Fail | ModResult::Sentinel => {
                    // The key exists in the chunk, retry
                    backoff.spin();
                    continue;
                }
                ModResult::Aborted(fval) => {
                    // Key exists but aborted by user function, just return
                    return SwapResult::Aborted(fval);
                }
                ModResult::NotFound => {
                    // Probably should try the old chunk see if it is there
                    trace!("Cannot find {} to swap", fkey);
                }
                _ => {
                    error!("Swap have {:?}", fast_mod_res);
                    unreachable!("Swap have {:?}", fast_mod_res);
                }
            }
            match (result, new_chunk.is_some()) {
                (Some((fval, idx, mod_chunk)), true) => {
                    loop {
                        // Just try to CAS a sentinel in the old chunk and we are done
                        let sentinel_res = self.modify_entry(
                            chunk,
                            hash,
                            key,
                            fkey,
                            ModOp::Sentinel,
                            false,
                            &guard,
                        );
                        match sentinel_res {
                            ModResult::Fail => {
                                continue;
                            }
                            ModResult::NotFound
                            | ModResult::Replaced(_, _, _)
                            | ModResult::Sentinel => {
                                return SwapResult::Succeed(
                                    Self::offset_v_out(fval),
                                    idx,
                                    mod_chunk,
                                );
                            }
                            _ => {
                                error!("Swap got sentinel {:?}", sentinel_res);
                                unreachable!("Swap got sentinel {:?}", sentinel_res);
                            }
                        }
                    }
                }
                (Some((fval, idx, mod_chunk_ptr)), false) => {
                    return SwapResult::Succeed(Self::offset_v_out(fval), idx, mod_chunk_ptr);
                }
                (None, false) => return SwapResult::NotFound,
                (None, true) => {
                    // Here, we had checked that the new chunk does not have anything to swap
                    // Which can be that the old chunk may have the value.
                    match self.get_from_chunk(chunk, hash, key, fkey, &backoff, None, epoch) {
                        Some((fv, addr, _)) => {
                            // This is a tranision state and we just don't bother for its complexity
                            // Simply retry and wait until the entry is moved to new chunk
                            if fv.is_valued() {
                                Self::wait_entry(addr, fkey, fv.val, &backoff);
                            } else {
                                backoff.spin();
                            }
                            continue;
                        }
                        None => return SwapResult::NotFound,
                    }
                }
            }
        }
    }

    #[inline(always)]
    pub fn remove(&self, key: &K, fkey: FKey) -> Option<(FVal, V)> {
        let guard = crossbeam_epoch::pin();
        let backoff = crossbeam_utils::Backoff::new();
        let (fkey, hash) = Self::hash(fkey, key);
        'OUTER: loop {
            let (chunk, new_chunk, _epoch) = self.chunk_refs(hash, &guard);
            let modify_chunk = new_chunk.unwrap_or(chunk);
            let old_chunk_val = new_chunk.map(|_| {
                self.modify_entry(chunk, hash, key, fkey, ModOp::Sentinel, true, &guard)
            });
            match old_chunk_val {
                Some(ModResult::Fail) => {
                    // Make sure sentinel had succceed first
                    backoff.spin();
                    continue 'OUTER;
                }
                _ => {}
            }
            'INNER: loop {
                let chunk_val = self.modify_entry(
                    modify_chunk,
                    hash,
                    key,
                    fkey,
                    ModOp::Tombstone,
                    true,
                    &guard,
                );
                match chunk_val {
                    ModResult::Replaced(fval, val, idx) => {
                        delay_log!(
                            "Tombstone key {} index {}, old_val {:?}, old chunk {}, new chunk {:?}",
                            fkey,
                            idx,
                            old_chunk_val,
                            chunk.base,
                            new_chunk.map(|c| c.base)
                        );
                        self.count.fetch_sub(1, Relaxed);
                        return Some((Self::offset_v_out(fval), val.unwrap()));
                    }
                    ModResult::Fail => {
                        backoff.spin();
                        continue 'INNER;
                    }
                    ModResult::Sentinel => {
                        backoff.spin();
                        continue 'OUTER;
                    }
                    ModResult::NotFound => {
                        break 'INNER;
                    }
                    _ => {
                        error!("Unreachable result state on remove in chunk");
                        panic!("Unreachable result state on remove in chunk");
                    }
                }
            }
            match old_chunk_val {
                Some(ModResult::Replaced(fval, val, idx)) => {
                    delay_log!(
                        "Tombstone not put but sentinel put at key {} index {}, old chunk {}, new chunk {:?}",
                        fkey,
                        idx,
                        chunk.base, new_chunk.map(|c| c.base)
                    );
                    self.count.fetch_sub(1, Relaxed);
                    return Some((Self::offset_v_out(fval), val.unwrap()));
                }
                Some(ModResult::Fail) | Some(ModResult::Sentinel) => {
                    backoff.spin();
                    continue 'OUTER;
                }
                None | Some(ModResult::NotFound) => {
                    delay_log!(
                        "Did not removed anything, not found {}, old chunk {}, new chunk {:?}",
                        fkey,
                        chunk.base,
                        new_chunk.map(|c| c.base)
                    );
                    return None;
                }
                Some(_) => {
                    error!("Unreachable result state on remove in old chunk");
                    panic!("Unreachable result state on remove in old chunk")
                }
            }
        }
    }

    #[inline]
    fn is_copying(epoch: usize) -> bool {
        epoch | 1 == epoch
    }

    #[inline]
    fn chunk_refs<'a>(
        &self,
        hash: usize,
        guard: &'a Guard,
    ) -> (
        ChunkPtr<'a, K, V, A, ALLOC>,
        Option<ChunkPtr<'a, K, V, A, ALLOC>>,
        usize,
    ) {
        loop {
            let current_chunks_addr = self.meta.current_chunks.load(Acquire);
            let history_chunks_addr = self.meta.history_chunks.load(Acquire);
            if current_chunks_addr == history_chunks_addr {
                continue;
            }
            let current_chunks = Self::chunk_array_at_addr(current_chunks_addr);
            let history_chunks = Self::chunk_array_at_addr(history_chunks_addr);

            let current_chunk = current_chunks.hash_chunk(hash, guard).unwrap();
            let history_chunk = history_chunks.hash_chunk(hash, guard);

            let epoch = current_chunk.now_epoch();
            let is_copying = Self::is_copying(epoch);
            if is_copying && history_chunk.is_none() {
                continue;
            } else if !is_copying && history_chunk.is_some() {
                continue;
            }
            if is_copying {
                return (history_chunk.unwrap(), Some(current_chunk), epoch);
            } else {
                return (current_chunk, None, epoch);
            }
        }
    }

    pub fn len(&self) -> usize {
        self.count.load(Relaxed)
    }

    fn get_from_chunk(
        &self,
        chunk: ChunkPtr<K, V, A, ALLOC>,
        hash: usize,
        key: &K,
        fkey: FKey,
        backoff: &Backoff,
        new_chunk: Option<ChunkPtr<K, V, A, ALLOC>>,
        epoch: usize,
    ) -> Option<(FastValue, usize, A::Item)> {
        debug_assert_ne!(chunk.ptr as usize, 0);
        let cap_mask = chunk.cap_mask();
        let home_idx = hash & cap_mask;
        let reiter = || chunk.iter_slot_skipable(home_idx, false);
        let mut iter = reiter();
        let (mut idx, _count) = iter.next().unwrap();
        let mut addr = chunk.entry_addr(idx);
        loop {
            let k = Self::get_fast_key(addr);
            if k == fkey {
                let val_res = Self::get_fast_value(addr);
                let raw = val_res.val;
                if raw == FORWARD_SWAPPING_VALUE {
                    Self::wait_entry(addr, k, raw, backoff);
                    iter.refresh_following(chunk);
                    continue;
                } else if raw == BACKWARD_SWAPPING_VALUE {
                    // Do NOT use probe here
                    Self::wait_entry(addr, k, raw, backoff);
                    iter = reiter();
                    continue;
                }
                let attachment = chunk.attachment.prefetch(idx);
                let probe = attachment.probe(key);
                if Self::get_fast_key(addr) != k {
                    backoff.spin();
                    continue;
                }
                if probe {
                    if val_res.is_locked() {
                        backoff.spin();
                        continue;
                    }
                    delay_log!(
                        "Get got for key {}, value {} at {}, chunk {}, epoch {}",
                        fkey,
                        raw,
                        idx,
                        chunk.base,
                        epoch
                    );
                    return Some((val_res, addr, attachment));
                }
            } else if k == EMPTY_KEY {
                delay_log!(
                    "Get got nothing due empty key at {} for key {}, hash {}, val -, home {}, chunk {}, epoch {}",
                    idx,
                    fkey,
                    hash,
                    home_idx,
                    chunk.base,
                    epoch
                );
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

        delay_log!(
            "Get got nothing due nothing found at {} for key {}, hash {}, val -, home {}, chunk {}, epoch {}",
            idx,
            fkey,
            hash,
            home_idx,
            chunk.base,
            epoch
        );
        return None;
    }

    #[inline]
    fn modify_entry<'a>(
        &self,
        chunk: ChunkPtr<'a, K, V, A, ALLOC>,
        hash: usize,
        key: &K,
        fkey: FKey,
        op: ModOp<V>,
        read_attachment: bool,
        _guard: &'a Guard,
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
            if k > MAX_META_VAL {
                let v = Self::get_fast_value(addr);
                let raw = v.val;
                if v.is_primed() {
                    backoff.spin();
                    continue 'MAIN;
                }
                let mut is_sentinel = false;
                match raw {
                    SENTINEL_VALUE => {
                        is_sentinel = true;
                    }
                    BACKWARD_SWAPPING_VALUE => {
                        if iter.terminal {
                            // Only check terminal probing
                            Self::wait_entry(addr, k, raw, &backoff);
                            iter = reiter();
                            continue 'MAIN;
                        }
                    }
                    FORWARD_SWAPPING_VALUE => {
                        Self::wait_entry(addr, k, raw, &backoff);
                        iter.refresh_following(chunk);
                        continue 'MAIN;
                    }
                    _ => {}
                }
                if is_sentinel {
                    match &op {
                        ModOp::Insert(_, _) | ModOp::AttemptInsert(_, _) => {
                            return ModResult::Sentinel;
                        }
                        _ => {}
                    }
                }
                if k == fkey {
                    let attachment = chunk.attachment.prefetch(idx);
                    let key_probe = attachment.probe(&key);
                    if Self::get_fast_key(addr) != k {
                        // For hopsotch
                        // Here hash collision is impossible becasue hopsotch only swap with
                        // slots have different hash key
                        backoff.spin();
                        continue 'MAIN;
                    }
                    if key_probe {
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
                                        if act_val != TOMBSTONE_VALUE {
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
                                        // Do not erase key. Future insertion might rese it
                                        // Memory will be reclaimed during migrtion
                                        attachment.erase_value(raw);
                                        if raw == EMPTY_VALUE || raw == TOMBSTONE_VALUE {
                                            return ModResult::NotFound;
                                        } else {
                                            return ModResult::Replaced(act_val, prev_val, idx);
                                        }
                                    } else {
                                        return ModResult::Fail;
                                    }
                                }
                                ModOp::Tombstone => {
                                    debug_assert!(!v.is_primed());
                                    if Self::cas_tombstone(addr, v.val) {
                                        let prev_val =
                                            read_attachment.then(|| attachment.get_value());
                                        // Do not erase key. Future insertion might rese it
                                        // Memory will be reclaimed during migrtion
                                        attachment.erase_value(raw);
                                        if raw == EMPTY_VALUE || raw == TOMBSTONE_VALUE {
                                            return ModResult::NotFound;
                                        } else {
                                            delay_log!("Tombstone replace to key {} index {}, raw val {}, act val {}, chunk {}", fkey, idx, raw, act_val, chunk.base);
                                            return ModResult::Replaced(act_val, prev_val, idx);
                                        }
                                    } else {
                                        return ModResult::Fail;
                                    }
                                }
                                ModOp::Lock => {
                                    if Self::cas_value(addr, v.val, LOCKED_VALUE).1 {
                                        let attachment =
                                            read_attachment.then(|| attachment.get_value());
                                        return ModResult::Replaced(act_val, attachment, addr);
                                    } else {
                                        backoff.spin();
                                        continue;
                                    }
                                }
                                ModOp::AttemptInsert(fval, oval) => {
                                    if act_val == TOMBSTONE_VALUE || act_val == EMPTY_VALUE {
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
                                            // Fast value changed
                                            backoff.spin();
                                            continue;
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
                                        return ModResult::Existed(act_val, value, idx);
                                    }
                                }
                                ModOp::SwapFastVal(ref swap) => {
                                    if raw == TOMBSTONE_VALUE {
                                        return ModResult::NotFound;
                                    }
                                    if let Some(sv) = swap(Self::offset_v_out(act_val)) {
                                        if Self::cas_value(addr, raw, Self::offset_v_in(sv)).1 {
                                            // swap success
                                            return ModResult::Replaced(act_val, None, idx);
                                        } else {
                                            return ModResult::Fail;
                                        }
                                    } else {
                                        return ModResult::Aborted(act_val);
                                    }
                                }
                            }
                        } else if is_sentinel {
                            return ModResult::Sentinel;
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
            }
            if k == EMPTY_KEY {
                // trace!("Inserting {}", fkey);
                let hop_adjustment = Self::need_hop_adjustment(chunk, count); // !Self::FAT_VAL && count > NUM_HOPS;
                match op {
                    ModOp::Insert(fval, val) | ModOp::AttemptInsert(fval, val) => {
                        let cas_fval = if hop_adjustment {
                            // Use empty key to block probing progression for hops
                            BACKWARD_SWAPPING_VALUE
                        } else {
                            fval
                        };
                        let primed_fval = Self::if_fat_val_then_val(LOCKED_VALUE, cas_fval);
                        match Self::cas_value(addr, EMPTY_VALUE, primed_fval) {
                            (_, true) => {
                                chunk.occupation.fetch_add(1, AcqRel);
                                let attachment = chunk.attachment.prefetch(idx);
                                attachment.set_key(key.clone());
                                if Self::FAT_VAL {
                                    val.map(|val| attachment.set_value((*val).clone(), 0));
                                    Self::store_raw_value(addr, fval);
                                    Self::store_key(addr, fkey);
                                } else {
                                    Self::store_key(addr, fkey);
                                    match Self::adjust_hops(
                                        hop_adjustment,
                                        chunk,
                                        fkey,
                                        fval,
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
                    ModOp::Sentinel => return ModResult::NotFound,
                    ModOp::Tombstone => return ModResult::NotFound,
                    ModOp::SwapFastVal(_) => return ModResult::NotFound,
                    ModOp::Lock => {}
                };
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
            ModOp::Insert(_, _) | ModOp::AttemptInsert(_, _) => ModResult::TableFull,
            _ => ModResult::NotFound,
        }
    }

    #[inline(always)]
    fn need_hop_adjustment(chunk: ChunkPtr<K, V, A, ALLOC>, count: usize) -> bool {
        ENABLE_HOPSOTCH && !Self::FAT_VAL && chunk.capacity > NUM_HOPS && count > NUM_HOPS
    }

    #[inline(always)]
    fn wait_entry(addr: usize, orig_key: FKey, orig_val: FVal, backoff: &Backoff) {
        loop {
            if Self::get_fast_value(addr).val != orig_val {
                break;
            }
            backoff.spin();
        }
    }

    fn all_from_chunk(&self, chunk: ChunkPtr<K, V, A, ALLOC>) -> Vec<(FKey, FVal, K, V)> {
        Self::all_from_chunk_(chunk)
    }

    fn all_from_chunk_(chunk: ChunkPtr<K, V, A, ALLOC>) -> Vec<(FKey, FVal, K, V)> {
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
                if act_val > MAX_META_VAL {
                    let attachment = chunk.attachment.prefetch(idx);
                    let key = attachment.get_key();
                    let value = attachment.get_value();
                    if Self::FAT_VAL && Self::get_fast_value(addr).val != val_res.val {
                        continue;
                    }
                    let k = Self::offset_k_out(k);
                    let v = Self::offset_v_out(act_val);
                    res.push((k, v, key, value))
                }
            }
            idx += 1; // reprobe
            counter += 1;
        }
        return res;
    }

    fn debug_assert_no_duplicates(chunk: ChunkPtr<K, V, A, ALLOC>) {
        #[cfg(debug_assertions)]
        {
            let mut no_duplicates = true;
            let all_entries = Self::all_from_chunk_(chunk);
            let mut prev_entries = std::collections::HashMap::new();
            for (k, v, _, _) in &all_entries {
                if let Some(mv) = prev_entries.get(k) {
                    error!("Existing key {} with value {}", k, mv);
                    no_duplicates = false;
                }
                prev_entries.insert(k, v);
            }
            error!(
                "No duplicates? {}, occ {}/{}/{}, num keys {}, entries {}",
                no_duplicates,
                chunk.occupation.load(Acquire),
                chunk.occu_limit,
                chunk.capacity,
                prev_entries.len(),
                all_entries.len()
            );
        }
    }

    pub fn entries(&self) -> Vec<(FKey, FVal, K, V)> {
        let guard = crossbeam_epoch::pin();
        let current_map = Self::chunk_array_at_addr(self.meta.current_chunks.load(Acquire))
            .iter_chunks(&guard)
            .map(|chunk| self.all_from_chunk(chunk))
            .flatten()
            .map(|(k, v, fk, fv)| ((k, fk), (v, fv)))
            .collect::<std::collections::HashMap<_, _>>();
        let history_map = Self::chunk_array_at_addr(self.meta.history_chunks.load(Acquire))
            .iter_chunks(&guard)
            .map(|chunk| self.all_from_chunk(chunk))
            .flatten()
            .map(|(k, v, fk, fv)| ((k, fk), (v, fv)))
            .collect::<std::collections::HashMap<_, _>>();
        let mut chunk_map = history_map;
        current_map.into_iter().for_each(|((fk, k), (fv, v))| {
            chunk_map.insert((fk, k), (fv, v));
        });
        return chunk_map
            .into_iter()
            .map(|((k, fk), (v, fv))| (k, v, fk, fv))
            .collect_vec();
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
            intrinsics::atomic_cxchg_acqrel_relaxed(addr as *mut FVal, original, TOMBSTONE_VALUE).1
        }
    }
    #[inline(always)]
    fn cas_value(entry_addr: usize, original: FVal, value: FVal) -> (FVal, bool) {
        debug_assert!(entry_addr > 0);
        let addr = entry_addr + mem::size_of::<FKey>();
        unsafe { intrinsics::atomic_cxchg_acqrel_relaxed(addr as *mut FVal, original, value) }
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
            intrinsics::atomic_cxchg_acqrel_relaxed(addr as *mut FVal, original, SENTINEL_VALUE)
        };
        done || ((val & FVAL_VAL_BIT_MASK) == SENTINEL_VALUE)
    }

    fn adjust_hops(
        needs_adjust: bool,
        chunk: ChunkPtr<K, V, A, ALLOC>,
        fkey: usize,
        fval: usize,
        home_idx: usize,
        mut dest_idx: usize,
        hops: usize,
    ) -> Result<usize, usize> {
        // This algorithm only swap the current indexed slot with the
        // one that has hop bis set to avoid swapping with other swapping slot
        if !ENABLE_HOPSOTCH {
            delay_log!(
                "No adjustment for disabled, hops {}, home {}, dest {}, chunk {}",
                hops,
                home_idx,
                dest_idx,
                chunk.base
            );
            return Ok(dest_idx);
        }

        if hops < NUM_HOPS {
            debug_assert!(!needs_adjust);
            chunk.set_hop_bit(home_idx, hops);
            delay_log!(
                "No adjustment for within range, hops {}, home {}, dest {}, fval {}, chunk {}",
                hops,
                home_idx,
                dest_idx,
                {
                    let addr = chunk.entry_addr(dest_idx);
                    Self::get_fast_value(addr).val
                },
                chunk.base
            );
            return Ok(dest_idx);
        }

        if !needs_adjust {
            delay_log!(
                "No adjustment for not needed, hops {}, home {}, dest {}, chunk {}",
                hops,
                home_idx,
                dest_idx,
                chunk.base
            );
            return Ok(dest_idx);
        }

        let cap_mask = chunk.cap_mask();
        let cap = chunk.capacity;
        let target_key_digest = key_digest(fkey);
        let overflowing = home_idx + hops >= cap;
        let targeting_idx = dest_idx;
        'SWAPPING: loop {
            // Need to adjust hops
            let candidate_slots = NUM_HOPS - 1;
            let (probe_start, probe_end) = if dest_idx < candidate_slots {
                (cap - (candidate_slots - dest_idx), dest_idx + cap)
            } else {
                (dest_idx - candidate_slots, dest_idx)
            };
            let hop_bits = chunk.get_hop_bits(home_idx);
            if hop_bits == ALL_HOPS_TAKEN {
                // No slots in the neighbour is available
                delay_log!(
                    "No adjustment for all slot taken, home {}, target {}, last {}, overflowing {}, chunk {}",
                    home_idx,
                    targeting_idx,
                    dest_idx,
                    overflowing,
                    chunk.base
                );
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
                    if Self::out_of_hop_range(home_idx, targeting_idx, candidate_idx, overflowing) {
                        continue;
                    }
                    // Found a candidate slot
                    let candidate_addr = chunk.entry_addr(candidate_idx);
                    let candidate_fkey = Self::get_fast_key(candidate_addr);
                    let candidate_fval = Self::get_fast_value(candidate_addr);
                    let candidate_raw_val = candidate_fval.val;
                    if candidate_raw_val <= MAX_META_VAL
                        || candidate_fkey <= MAX_META_KEY
                        || candidate_fval.is_primed()
                    {
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
                    if !Self::cas_value(candidate_addr, candidate_raw_val, FORWARD_SWAPPING_VALUE).1
                    {
                        // The slot value have been changed, retry
                        iter.redo();
                        continue;
                    }
                    if !chunk.is_bit_set(idx, candidate_distance)
                        || Self::get_fast_key(candidate_addr) != candidate_fkey
                    {
                        // Revert the value change, refresh following bits and try again
                        Self::store_value(candidate_addr, candidate_raw_val);
                        iter.refresh_following(chunk);
                        continue;
                    }

                    if cfg!(debug_assertions) && !Self::WORD_KEY {
                        assert_eq!(
                            candidate_fkey & cap_mask,
                            idx,
                            "Home mismatch capacity {}",
                            cap
                        );
                    }

                    // Update the hop bit
                    let curr_candidate_distance = hop_distance(idx, dest_idx, cap);
                    chunk.set_hop_bit(idx, curr_candidate_distance);

                    // Starting to copy it co current idx
                    let curr_addr = chunk.entry_addr(dest_idx);

                    // Should all locked up
                    debug_assert_eq!(
                        Self::get_fast_value(candidate_addr).val,
                        FORWARD_SWAPPING_VALUE
                    );
                    debug_assert_eq!(Self::get_fast_value(curr_addr).val, BACKWARD_SWAPPING_VALUE);

                    // Start from key object in the attachment
                    // All key should be moved out to prevent cloning
                    let candidate_attachment = chunk.attachment.prefetch(candidate_idx);
                    let candidate_key = candidate_attachment.moveout_key();
                    let curr_attachment = chunk.attachment.prefetch(dest_idx);
                    let current_key = curr_attachment.moveout_key();
                    // And the key object
                    // Then swap the key
                    curr_attachment.set_key(candidate_key);
                    Self::store_key(curr_addr, candidate_fkey);

                    chunk.unset_hop_bit(idx, candidate_distance);

                    // Enable probing on the candidate with inserting key
                    candidate_attachment.set_key(current_key);
                    Self::store_key(candidate_addr, fkey);

                    // Set the target bit only after keys are setted
                    let target_hop_distance = hop_distance(home_idx, candidate_idx, cap);
                    let candidate_in_range = target_hop_distance < NUM_HOPS;
                    if candidate_in_range {
                        chunk.set_hop_bit(home_idx, target_hop_distance);
                    }

                    // Should all locked up
                    debug_assert_eq!(
                        Self::get_fast_value(candidate_addr).val,
                        FORWARD_SWAPPING_VALUE
                    );
                    debug_assert_eq!(Self::get_fast_value(curr_addr).val, BACKWARD_SWAPPING_VALUE);

                    // Discard swapping value on current address by replace it with new value
                    let primed_candidate_val =
                        syn_val_digest(candidate_key_digest, candidate_raw_val);
                    Self::store_value(curr_addr, primed_candidate_val);

                    //Here we had candidate copied. Need to work on the candidate slot
                    // First check if it is already in range of home neighbourhood
                    if candidate_in_range {
                        // In range, fill the candidate slot with our key and values
                        debug_assert!(fval >= MAX_META_VAL);
                        Self::store_value(candidate_addr, fval);
                        delay_log!(
                            "Adjusted home {}, target {}, last {}, overflowing {}, chunk {}",
                            home_idx,
                            targeting_idx,
                            dest_idx,
                            overflowing,
                            chunk.base
                        );
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
            debug_assert_eq!(
                {
                    let addr = chunk.entry_addr(dest_idx);
                    Self::get_fast_value(addr).val
                },
                BACKWARD_SWAPPING_VALUE
            );
            delay_log!(
                "No adjustment for no slot found, home {}, target {}, last {}, overflowing {}, chunk {}",
                home_idx,
                targeting_idx,
                dest_idx,
                overflowing,
                chunk.base
            );
            return Err(dest_idx);
        }
    }

    pub fn out_of_hop_range(
        home_idx: usize,
        dest_idx: usize,
        candidate_idx: usize,
        overflowing: bool,
    ) -> bool {
        if overflowing {
            // Wrapped
            candidate_idx >= dest_idx && candidate_idx < home_idx
        } else {
            candidate_idx < home_idx || candidate_idx >= dest_idx
        }
    }

    /// Failed return old shared
    fn check_migration<'a>(
        &self,
        hash: usize,
        old_chunk_ptr: ChunkPtr<'a, K, V, A, ALLOC>,
        chunk_epoch: usize,
        guard: &Guard,
    ) -> ResizeResult {
        let occupation = old_chunk_ptr.occupation.load(Relaxed);
        let occu_limit = old_chunk_ptr.occu_limit;
        if occupation < occu_limit {
            return ResizeResult::NoNeed;
        }
        self.do_migration(hash, old_chunk_ptr, chunk_epoch, guard)
    }

    fn do_migration<'a>(
        &self,
        hash: usize,
        old_chunk_ptr: ChunkPtr<'a, K, V, A, ALLOC>,
        chunk_epoch: usize,
        guard: &crossbeam_epoch::Guard,
    ) -> ResizeResult {
        // Swap in new chunk as placeholder for the lock
        let history_chunk_array =
            Self::chunk_array_at_addr(self.meta.history_chunks.load(Acquire));
        let history_chunk_id = history_chunk_array.id_of_hash(hash);
        let current_chunk_array =
            Self::chunk_array_at_addr(self.meta.current_chunks.load(Acquire));
        let current_chunk_id = current_chunk_array.id_of_hash(hash);
        if !history_chunk_array.swap_in_chunk(history_chunk_id, old_chunk_ptr) {
            // other thread have allocated new chunk and wins the competition, exit
            trace!("Cannot obtain lock for resize, will retry");
            return ResizeResult::SwapFailed;
        }
        let old_epoch = old_chunk_ptr.epoch.load(Acquire);
        debug_assert!(!Self::is_copying(old_epoch));
        if chunk_epoch != old_epoch {
            history_chunk_array.set_chunk(history_chunk_id, ChunkPtr::null());
            debug!(
                "$ Epoch changed from {} to {} after migration lock",
                chunk_epoch, old_epoch
            );
            return ResizeResult::SwapFailed;
        }
        let empty_entries = old_chunk_ptr.empty_entries.load(Relaxed);
        let old_cap = old_chunk_ptr.capacity;
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
        // Preoccupie half of the new capacity for migration
        let mut old_occupation = old_cap;
        if old_occupation <= 256 {
            // The map is small, block all insertions until the migration is completed
            old_occupation = new_cap;
        }
        let new_chunk = Chunk::alloc_chunk(new_cap, old_epoch + 1, &self.attachment_init_meta);
        unsafe {
            (*new_chunk).occupation.store(old_occupation, Relaxed);
        }
        let new_chunk_ptr = ChunkPtr::new(new_chunk);
        let new_chunk_ins = unsafe { new_chunk_ptr.deref() };
        debug_assert_ne!(new_chunk_ptr.base, old_chunk_ptr.base);
        debug!(
            "--- Resizing {} to {}. New size is {}, was {} at old epoch {}",
            old_chunk_ptr.base, new_chunk_ptr.base, new_cap, old_cap, old_epoch
        );
        delay_log!(
            "Migration from {} to {}, cap {}/{} at old epoch {}",
            old_chunk_ptr.base,
            new_chunk_ptr.base,
            old_cap,
            new_cap,
            old_epoch
        );
        current_chunk_array.set_chunk(current_chunk_id, new_chunk_ptr); // Stump becasue we have the lock already
        let meta = self.meta.clone();
        // Not going to take multithreading resize
        // Experiments shows there is no significant improvement in performance
        trace!("Initialize migration");
        let num_migrated = self.migrate_entries(
            old_chunk_ptr,
            new_chunk_ptr,
            old_occupation,
            old_epoch,
            &guard,
        );
        history_chunk_array.erase_chunk(history_chunk_id, guard);
        new_chunk_ptr.epoch.store(old_epoch + 2, Release);
        debug!(
            "!!! Migration for {:?} completed, new chunk is {:?}, size from {} to {}, old epoch {}, num {}",
            old_chunk_ptr.base,
            new_chunk_ins.base,
            old_chunk_ptr.capacity,
            new_chunk_ins.capacity,
            old_epoch, num_migrated
        );
        ResizeResult::InProgress
    }

    fn migrate_entries(
        &self,
        old_chunk: ChunkPtr<K, V, A, ALLOC>,
        new_chunk: ChunkPtr<K, V, A, ALLOC>,
        old_occupation: usize,
        epoch: usize,
        _guard: &crossbeam_epoch::Guard,
    ) -> usize {
        trace!(
            "Migrating entries from {:?} to {:?}",
            old_chunk.base,
            new_chunk.base
        );
        #[cfg(debug_assertions)]
        let mut migrated_entries = vec![];
        let mut old_address = old_chunk.base as usize;
        let boundary = old_address + chunk_size_of(old_chunk.capacity);
        let mut effective_copy = 0;
        let mut idx = 0;
        let backoff = crossbeam_utils::Backoff::new();
        while old_address < boundary {
            // iterate the old chunk to extract entries that is NOT empty
            let fkey = Self::get_fast_key(old_address);
            let fvalue = Self::get_fast_value(old_address);
            debug_assert_eq!(old_address, old_chunk.entry_addr(idx));
            debug_assert_eq!(old_chunk.now_epoch(), epoch + 1);
            // Reasoning value states
            trace!(
                "Migrating entry have key {}",
                Self::get_fast_key(old_address)
            );
            match fvalue.val {
                EMPTY_VALUE => {
                    // Probably does not need this anymore
                    // Need to make sure that during migration, empty value always leads to new chunk
                    if Self::cas_sentinel(old_address, fvalue.val) {
                        Self::store_key(old_address, DISABLED_KEY);
                    } else {
                        backoff.spin();
                        continue;
                    }
                }
                TOMBSTONE_VALUE => {
                    if !Self::cas_sentinel(old_address, fvalue.val) {
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
                    if Self::get_fast_key(old_address) != fkey {
                        backoff.spin();
                        continue;
                    }
                    if !Self::migrate_entry(
                        fkey,
                        idx,
                        fvalue,
                        old_chunk,
                        new_chunk,
                        old_address,
                        &mut effective_copy,
                        #[cfg(debug_assertions)]
                        &mut migrated_entries,
                    ) {
                        backoff.spin();
                        continue;
                    }
                }
            }
            old_address += ENTRY_SIZE;
            idx += 1;
        }
        // resize finished, make changes on the numbers
        if effective_copy > old_occupation {
            let delta = effective_copy - old_occupation;
            new_chunk.occupation.fetch_add(delta, Relaxed);
            trace!(
                "Occupation {}-{} offset {}",
                effective_copy,
                old_occupation,
                delta
            );
        } else if effective_copy < old_occupation {
            let delta = old_occupation - effective_copy;
            new_chunk.occupation.fetch_sub(delta, Relaxed);
            trace!(
                "Occupation {}-{} offset neg {}",
                effective_copy,
                old_occupation,
                delta
            );
        } else {
            trace!(
                "Occupation {}-{} zero offset",
                effective_copy,
                old_occupation
            );
        }
        #[cfg(debug_assertions)]
        {
            debug!(
                "* Migrated {} entries to new chunk, num logs {}",
                effective_copy,
                migrated_entries.len()
            );
            MIGRATION_LOGS.lock().push((epoch, migrated_entries))
        }
        return effective_copy;
    }

    #[inline(always)]
    fn passive_migrate_entry(
        fkey: FKey,
        old_idx: usize,
        fvalue: FastValue,
        old_chunk_ins: ChunkPtr<K, V, A, ALLOC>,
        new_chunk_ins: ChunkPtr<K, V, A, ALLOC>,
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
        old_chunk_ins: ChunkPtr<K, V, A, ALLOC>,
        new_chunk_ins: ChunkPtr<K, V, A, ALLOC>,
        old_address: usize,
        effective_copy: &mut usize,
        #[cfg(debug_assertions)] migrated: &mut Vec<MigratedEntry>,
    ) -> bool {
        if fkey == EMPTY_KEY {
            #[cfg(debug_assertions)]
            migrated.push(((fkey, fvalue), 0, 222, thread_id()));
            return false;
        }
        // Will not migrate meta keys
        if fkey <= MAX_META_KEY || fvalue.is_primed() {
            #[cfg(debug_assertions)]
            migrated.push(((fkey, fvalue), 0, 111, thread_id()));
            return true;
        }
        // Insert entry into new chunk, in case of failure, skip this entry
        // Value should be locked
        let act_val = fvalue.act_val::<V>();
        let primed_orig = fvalue.prime();

        // Prime the old address to avoid modification
        if !Self::cas_value(old_address, fvalue.val, primed_orig).1 {
            // here the ownership have been taken by other thread
            trace!("Entry {} has changed", fkey);
            #[cfg(debug_assertions)]
            migrated.push(((fkey, fvalue), 0, 999, thread_id()));
            return false;
        }

        trace!("Primed {}", fkey);

        // Since the entry is primed, it is safe to read the key
        let old_attachment = old_chunk_ins.attachment.prefetch(old_idx);
        let key = old_attachment.get_key();

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
                let probe = new_attachment.probe(&key);
                if Self::get_fast_key(addr) != k {
                    continue;
                }
                if probe {
                    // New value in the new chunk, just put a sentinel and abort migration on this slot
                    Self::store_sentinel(old_address);
                    #[cfg(debug_assertions)]
                    migrated.push(((fkey, fvalue), idx, 888, thread_id()));
                    return true;
                }
            } else if k == EMPTY_KEY {
                let hop_adjustment = Self::need_hop_adjustment(new_chunk_ins, count);
                let cas_fval = if hop_adjustment {
                    // Use empty key to block probing progression for hops
                    BACKWARD_SWAPPING_VALUE
                } else {
                    act_val
                };
                if Self::cas_value(addr, EMPTY_VALUE, cas_fval).1 {
                    let new_attachment = new_chunk_ins.attachment.prefetch(idx);
                    let old_attachment = old_chunk_ins.attachment.prefetch(old_idx);
                    let value = old_attachment.get_value();
                    new_attachment.set_key(key);
                    new_attachment.set_value(value, 0);
                    Self::store_key(addr, fkey);
                    match Self::adjust_hops(
                        hop_adjustment,
                        new_chunk_ins,
                        fkey,
                        act_val,
                        home_idx,
                        idx,
                        count,
                    ) {
                        Err(distant_idx) => {
                            // Nothing else we can do, just take the distant slot
                            let addr = new_chunk_ins.entry_addr(distant_idx);
                            Self::store_value(addr, act_val);
                            Self::store_key(addr, fkey);
                        }
                        _ => {}
                    }
                    Self::store_sentinel(old_address);
                    *effective_copy += 1;
                    #[cfg(debug_assertions)]
                    migrated.push(((fkey, fvalue), idx, 0, thread_id()));
                    return true;
                } else {
                    // Here we didn't put the fval into the new chunk due to slot conflict with
                    // other thread. Need to retry
                    trace!("Migrate {} have conflict", fkey);
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
                new_chunk_ins.dump_dist();
                new_chunk_ins.dump_kv();
                Self::debug_assert_no_duplicates(new_chunk_ins);
                error!("Cannot find any slot for migration");
                panic!("Cannot find any slot for migration");
            }
        }
    }

    #[inline]
    pub fn hash(fkey: FKey, key: &K) -> (FKey, usize) {
        let fkey = Self::offset_k_in(fkey);
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

    pub fn capacity(&self) -> usize {
        let guard = crossbeam_epoch::pin();
        Self::chunk_array_at_addr(self.meta.current_chunks.load(Acquire)).key_capacity(&guard)
    }

    #[inline(always)]
    pub fn offset_k_in(fkey: usize) -> usize {
        fkey + K_OFFSET
    }

    #[inline(always)]
    pub fn offset_k_out(n: usize) -> usize {
        n - K_OFFSET
    }

    #[inline(always)]
    pub fn offset_v_in(n: usize) -> usize {
        n + V_OFFSET
    }

    #[inline(always)]
    pub fn offset_v_out(n: usize) -> usize {
        n - V_OFFSET
    }

    fn chunk_array_at_addr<'a>(addr: usize) -> &'a ChunkArray<K, V, A, ALLOC> {
        ChunkArray::ref_from_addr(addr)
    }
}

#[derive(Copy, Clone, Debug)]
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
        self.prime() == self.val
    }

    #[inline(always)]
    fn is_swapping(self) -> bool {
        let v = self.val;
        v == FORWARD_SWAPPING_VALUE || v == BACKWARD_SWAPPING_VALUE
    }

    #[inline(always)]
    fn prime(self) -> FVal {
        self.val | VAL_PRIME_BIT
    }

    #[inline(always)]
    fn is_valued(self) -> bool {
        self.val > TOMBSTONE_VALUE
    }
}

impl<K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> Chunk<K, V, A, ALLOC> {
    const FAT_VAL: bool = mem::size_of::<V>() != 0;

    fn alloc_chunk(capacity: usize, epoch: usize, attachment_meta: &A::InitMeta) -> *mut Self {
        let page_size = page_size::get_granularity();
        let self_size = mem::size_of::<Self>();
        let self_align = align_padding(self_size, 8);
        let self_size_aligned = self_size + self_align;
        let chunk_size = chunk_size_of(capacity);
        let chunk_alignment = if chunk_size >= page_size {
            page_size
        } else {
            8
        };
        let chunk_align = align_padding(chunk_size, chunk_alignment);
        let chunk_size_aligned = chunk_size + chunk_align;
        let attachment_heap_size = A::heap_entry_size() * capacity;
        let hop_size = HOP_TUPLE_SIZE * capacity;
        let hop_alignment = if hop_size >= page_size { page_size } else { 8 };
        let hop_align = align_padding(hop_size, hop_alignment);
        let hop_size_aligned = hop_size + hop_align;
        let total_size =
            self_size_aligned + chunk_size_aligned + hop_size_aligned + attachment_heap_size;
        let ptr = alloc_mem::<ALLOC>(total_size) as *mut Self;
        let addr = ptr as usize;
        let data_base = addr + self_size_aligned;
        let hop_base = data_base + chunk_size_aligned;
        let attachment_base = hop_base + hop_size_aligned;
        unsafe {
            Self::fill_zeros(
                data_base,
                hop_base,
                chunk_size_aligned,
                hop_size_aligned,
                page_size,
            );
            // fill_zeros(data_base, chunk_size_aligned + hop_size_aligned);
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
                    epoch: AtomicUsize::new(epoch),
                    attachment: A::new(attachment_base, attachment_meta),
                    shadow: PhantomData,
                },
            )
        };
        ptr
    }

    unsafe fn fill_zeros(
        mut data_base: usize,
        mut hop_base: usize,
        data_size: usize,
        hop_size: usize,
        page_size: usize,
    ) {
        if hop_size <= page_size || data_size <= page_size {
            fill_zeros(data_base, data_size);
            fill_zeros(hop_base, hop_size);
            return;
        }
        let orig_affinity = affinity::get_thread_affinity();
        if orig_affinity.is_err() {
            fill_zeros(data_base, data_size);
            fill_zeros(hop_base, hop_size);
            return;
        }
        debug_assert_eq!(data_size % page_size, 0);
        debug_assert_eq!(hop_size % page_size, 0);
        let num_cpus = num_cpus::get_physical();
        let allocs = min(data_size, hop_size) / page_size;
        let data_fill_size = data_size / allocs;
        let hop_fill_size = hop_size / allocs;
        (0..allocs)
            .map(|ci| {
                // Fill zeros in roundrobin
                let cpu_id = ci % num_cpus;
                let next_data_base = data_base + data_fill_size;
                let next_hop_base = hop_base + hop_fill_size;
                let res = (cpu_id, (data_base, hop_base));
                data_base = next_data_base;
                hop_base = next_hop_base;
                res
            })
            .sorted_by(|(x, _), (y, _)| x.cmp(y))
            .group_by(|(i, _)| *i)
            .into_iter()
            .for_each(|(cpu_id, g)| {
                g.into_iter().for_each(|(_, (data_base, hop_base))| {
                    // Do not assert affinity
                    // Some envorinment, like LXC does not allow affinity settings
                    if let Err(e) = affinity::set_thread_affinity(&vec![cpu_id]) {
                        warn!(
                            "Cannot set affinity on CPU {} during allocation, {:?}",
                            cpu_id, e
                        );
                    }
                    fill_zeros(data_base, data_fill_size);
                    fill_zeros(hop_base, hop_fill_size);
                })
            });
        affinity::set_thread_affinity(orig_affinity.as_ref().unwrap()).unwrap();
    }

    unsafe fn gc(ptr: *mut Chunk<K, V, A, ALLOC>) {
        debug_assert_ne!(ptr as usize, 0);
        let chunk = &*ptr;
        chunk.gc_entries();
        dealloc_mem::<ALLOC>(ptr as usize, chunk.total_size);
    }

    fn gc_entries(&self) {
        let mut old_address = self.base as usize;
        let boundary = old_address + chunk_size_of(self.capacity);
        let mut idx = 0;
        while old_address < boundary {
            let fvalue = Self::get_fast_value(old_address);
            let fkey = Self::get_fast_key(old_address);
            let val = fvalue.val;
            let has_key = fkey > MAX_META_KEY;
            let has_val = val > MAX_META_VAL;
            if has_key || has_val {
                let attachment = self.attachment.prefetch(idx);
                if has_key {
                    trace!("Erase key with fkey {}, idx {}", fkey, idx);
                    attachment.moveout_key();
                }
                if has_val {
                    attachment.erase_value(val);
                }
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
    fn is_bit_set(&self, idx: usize, pos: usize) -> bool {
        unsafe {
            let ptr = (self.hop_base + HOP_TUPLE_BYTES * idx) as *mut HopBits;
            let set_bit = 1 << pos;
            intrinsics::atomic_load_acquire(ptr) & set_bit > 0
        }
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
                if intrinsics::atomic_cxchg_acqrel_relaxed(ptr, orig_bits, target_bits).1 {
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

    fn dump_dist(&self) {
        let cap = self.capacity;
        let mut res = String::new();
        for i in 0..cap {
            let addr = self.entry_addr(i);
            let k = Self::get_fast_key(addr);
            if k != EMPTY_VALUE {
                res.push('1');
            } else {
                res.push('0');
            }
        }
        error!("Chunk dump: {}", res);
    }

    fn dump_kv(&self) {
        let cap = self.capacity;
        let mut res = vec![];
        for i in 0..cap {
            let addr = self.entry_addr(i);
            let k = Self::get_fast_key(addr);
            let v = Self::get_fast_value(addr);
            res.push(format!("{}:{}", k, v.val));
        }
        error!("Chunk dump: {:?}", res);
    }

    #[inline]
    fn epoch_changed(&self, epoch: usize) -> bool {
        self.now_epoch() != epoch
    }

    #[inline]
    pub fn now_epoch(&self) -> usize {
        self.epoch.load(Acquire)
    }

    pub fn occupation(&self) -> (usize, usize, usize) {
        (
            self.occu_limit,
            self.occupation.load(Relaxed),
            self.capacity,
        )
    }
}

impl<
        K,
        V,
        A: Attachment<K, V>,
        ALLOC: GlobalAlloc + Default,
        H: Hasher + Default,
        const K_OFFSET: usize,
        const V_OFFSET: usize,
    > Clone for Table<K, V, A, ALLOC, H, K_OFFSET, V_OFFSET>
{
    fn clone(&self) -> Self {
        let guard = crossbeam_epoch::pin();
        let current_chunks = ChunkArray::<K, V, A, ALLOC>::ref_from_addr(self.meta.current_chunks.load(Acquire));
        let history_chunks = ChunkArray::<K, V, A, ALLOC>::ref_from_addr(self.meta.history_chunks.load(Acquire));
        let new_current_chunk_array = ChunkArray::<K, V, A, ALLOC>::new(current_chunks.len, current_chunks.max_cap);
        let new_history_chunk_array = ChunkArray::<K, V, A, ALLOC>::new(history_chunks.len, history_chunks.max_cap);
        for i in 0..current_chunks.len {
            if let Some(chunk) = current_chunks.chunk_at(i, &guard) {
                unsafe {
                    let total_size = chunk.total_size;
                    let new_chunk_ptr = libc::malloc(total_size);
                    libc::memcpy(new_chunk_ptr, chunk.ptr as _, total_size);
                    &(*new_current_chunk_array).set_chunk(i, ChunkPtr::new(new_chunk_ptr as _));
                }
            }
        }
        for i in 0..history_chunks.len {
            if let Some(chunk) = history_chunks.chunk_at(i, &guard) {
                unsafe {
                    let total_size = chunk.total_size;
                    let new_chunk_ptr = libc::malloc(total_size);
                    libc::memcpy(new_chunk_ptr, chunk.ptr as _, total_size);
                    &(*new_history_chunk_array).set_chunk(i, ChunkPtr::new(new_chunk_ptr as _));
                }
            }
        }
        Self {
            meta: Arc::new(ChunkMeta {
                current_chunks: AtomicUsize::new(new_current_chunk_array as usize),
                history_chunks: AtomicUsize::new(new_history_chunk_array as usize),
                epoch: AtomicUsize::new(0),
            }),
            count: AtomicUsize::new(0),
            init_cap: self.init_cap,
            max_cap: self.max_cap,
            attachment_init_meta: self.attachment_init_meta.clone(),
            mark: PhantomData,
        }
    }
}

impl<
        K,
        V,
        A: Attachment<K, V>,
        ALLOC: GlobalAlloc + Default,
        H: Hasher + Default,
        const K_OFFSET: usize,
        const V_OFFSET: usize,
    > Drop for Table<K, V, A, ALLOC, H, K_OFFSET, V_OFFSET>
{
    fn drop(&mut self) {
        let guard = crossbeam_epoch::pin();
        let current_chunks = ChunkArray::<K, V, A, ALLOC>::ref_from_addr(self.meta.current_chunks.load(Acquire));
        let history_chunks = ChunkArray::<K, V, A, ALLOC>::ref_from_addr(self.meta.history_chunks.load(Acquire));
        &(*current_chunks).clear(&guard);
        &(*history_chunks).clear(&guard);
    }
}

unsafe impl<'a, K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> Send
    for ChunkPtr<'a, K, V, A, ALLOC>
{
}
unsafe impl<'a, K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> Sync
    for ChunkPtr<'a, K, V, A, ALLOC>
{
}

impl<'a, K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> ChunkPtr<'a, K, V, A, ALLOC> {
    fn destory(&mut self) {
        debug_assert_ne!(self.ptr as usize, 0);
        unsafe {
            Chunk::gc(self.ptr);
        }
    }
}

impl<'a, K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> Deref
    for ChunkPtr<'a, K, V, A, ALLOC>
{
    type Target = Chunk<K, V, A, ALLOC>;

    fn deref(&self) -> &Self::Target {
        debug_assert_ne!(self.ptr as usize, 0);
        unsafe { &*self.ptr }
    }
}

impl<'a, K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> ChunkPtr<'a, K, V, A, ALLOC> {
    fn new(ptr: *mut Chunk<K, V, A, ALLOC>) -> Self {
        debug_assert_ne!(ptr as usize, 0);
        Self {
            ptr,
            _marker: PhantomData,
        }
    }

    fn null() -> Self {
        Self {
            ptr: ptr::null_mut(),
            _marker: PhantomData,
        }
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
            Self::Existed(arg0, _arg1, _) => f.debug_tuple("Existed").field(arg0).finish(),
            Self::Fail => write!(f, "Fail"),
            Self::Sentinel => write!(f, "Sentinel"),
            Self::NotFound => write!(f, "NotFound"),
            Self::Done(arg0, _arg1, arg2) => f.debug_tuple("Done").field(arg0).field(arg2).finish(),
            Self::TableFull => write!(f, "TableFull"),
            Self::Aborted(fval) => write!(f, "Aborted({fval})"),
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
fn key_digest(key: FKey) -> FKey {
    key & VAL_KEY_DIGEST_MASK
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
        chunk: ChunkPtr<K, V, A, ALLOC>,
    ) {
        if self.hop_bits == 0 || self.hop_bits == !(!0 << 1 >> 1) {
            return;
        }
        let checked = self.hop_bits.trailing_zeros();
        let new_bits = chunk.get_hop_bits(self.home_idx);
        self.hop_bits = new_bits >> checked << checked;
    }
}

#[cfg(debug_assertions)]
pub fn get_delayed_log<'a>(num: usize) -> Vec<String> {
    DELAYED_LOG.with(|c| {
        let list = c.borrow();
        let len = list.len();
        let s = len - num;
        list.range(s..).cloned().collect()
    })
}

#[cfg(debug_assertions)]
pub fn dump_migration_log() {
    let logs = MIGRATION_LOGS.lock();
    logs.iter().for_each(|(epoch, item)| {
        item.iter().for_each(|((k, v), pos, stat, th)| {
            let v = v.val;
            println!(
                "e {} k {}, v {}, p {} s {} t {}",
                epoch, k, v, pos, stat, th
            );
        });
    });
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

    #[test]
    fn val_prime() {
        let val = FastValue::new(123);
        assert!(!val.is_primed());
        assert!(!val.is_locked());
        assert!(!val.is_swapping());
        assert!(val.is_valued());
        let val = val.prime();
        let val = FastValue { val };
        assert!(val.is_primed());
        assert!(!val.is_locked());
        assert!(!val.is_swapping());
        assert!(val.is_valued());
        assert_eq!(val.act_val::<()>(), 123);
    }

    #[test]
    fn ptr_prime_masking() {
        let num = 123;
        let ptr = &num as *const i32 as usize;
        let val = FastValue::new(ptr);
        assert!(!val.is_primed());
        assert!(!val.is_locked());
        assert!(!val.is_swapping());
        assert!(val.is_valued());
        let val = val.prime();
        let val = FastValue { val };
        assert!(val.is_primed());
        assert!(!val.is_locked());
        assert!(!val.is_swapping());
        assert!(val.is_valued());
        assert_eq!(val.act_val::<()>(), ptr);
    }
}
