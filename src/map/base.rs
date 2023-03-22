use std::{
    cell::RefCell,
    cmp::{max, min},
    collections::{self, VecDeque},
    sync::atomic::{AtomicU32, AtomicU64, AtomicU8},
};

use itertools::Itertools;
use libc::IFA_F_HOMEADDRESS;
use smallvec::{smallvec, SmallVec};

use crate::counter::Counter;
use rand::prelude::*;

use super::*;

pub struct EntryTemplate(FKey, FVal);

pub const EMPTY_KEY: FKey = 0;
pub const DISABLED_KEY: FKey = 1;

pub const EMPTY_VALUE: FVal = 0b000;
pub const SENTINEL_VALUE: FVal = 0b010;

// Last bit set indicates locking
pub const LOCKED_VALUE: FVal = 0b001;

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
pub const ENTRY_SIZE_SHIFT: u32 = ENTRY_SIZE.trailing_zeros();
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

pub const DEFAULT_PARTITION_MAX_CAP: usize = 256 * 1024;
pub const INIT_ARR_VER: usize = 0;
pub const INIT_ARR_SIZE: usize = 8;

pub const DISABLED_CHUNK_PTR: usize = 1;

const CACHE_LINE_SIZE: usize = 64;
const KEY_SIZE: usize = mem::size_of::<FKey>();

type MigrationBits = AtomicU32;
const MIGRATION_BITS_SIZE: usize = mem::size_of::<MigrationBits>() * 8;
const MIGRATION_BITS_SHIFT: u32 = MIGRATION_BITS_SIZE.trailing_zeros();
const MIGRATION_BITS_MASK: usize = MIGRATION_BITS_SIZE - 1;
const MIGRATION_TOTAL_BITS: usize = 32;
const MIGRATION_TOTAL_MASK: usize = MIGRATION_TOTAL_BITS - 1;
const MIGRATION_BITS_ARR_SIZE: usize = MIGRATION_TOTAL_BITS / MIGRATION_BITS_SIZE;
const MIGRATION_TOTAL_BITS_SHIFT: u32 = MIGRATION_TOTAL_BITS.trailing_zeros();

const DELAY_LOG_CAP: usize = 10;
#[cfg(debug_assertions)]
thread_local! {
    static DELAYED_LOG: RefCell<VecDeque<String>> = RefCell::new(VecDeque::new());
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
    Done,
}

pub enum SwapResult<'a, K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> {
    Succeed(FVal, usize, ChunkPtr<'a, K, V, A, ALLOC>),
    Aborted(FVal),
    NotFound,
    Failed,
}

pub struct ChunkSizes {
    capacity: usize,
    total_size: usize,
    self_size_aligned: usize,
    chunk_size_aligned: usize,
    page_size: usize,
}

const COPIED_ARR_SIZE: usize = 8;
pub struct Chunk<K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> {
    capacity: usize,
    cap_mask: usize,
    base: usize,
    occu_limit: u32,
    occupation: AtomicU32,
    empty_entries: AtomicUsize,
    total_size: usize,
    origin: *const PartitionArray<K, V, A, ALLOC>,
    migrating: Migrations,
    migrated: Migrations,
    copied: SmallVec<[AtomicUsize; COPIED_ARR_SIZE]>,
    pub attachment: A,
    shadow: PhantomData<(K, V, ALLOC)>,
}

pub struct Table<
    K: Clone + Hash + Eq,
    V: Clone,
    A: Attachment<K, V>,
    ALLOC: GlobalAlloc + Default,
    H: Hasher + Default,
    const K_OFFSET: usize,
    const V_OFFSET: usize,
> {
    meta: ChunkMeta,
    attachment_init_meta: A::InitMeta,
    count: Counter,
    init_cap: usize,
    init_arr_len: usize,
    max_cap: usize,
    mark: PhantomData<(H, ALLOC)>,
}

pub struct ChunkPtr<'a, K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> {
    ptr: *mut Chunk<K, V, A, ALLOC>,
    _marker: PhantomData<&'a ()>,
}

impl<'a, K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> Clone
    for ChunkPtr<'a, K, V, A, ALLOC>
{
    fn clone(&self) -> Self {
        Self {
            ptr: self.ptr,
            _marker: PhantomData,
        }
    }
}

impl<'a, K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> Copy
    for ChunkPtr<'a, K, V, A, ALLOC>
{
}

impl<'a, K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> PartialEq
    for ChunkPtr<'a, K, V, A, ALLOC>
{
    fn eq(&self, other: &Self) -> bool {
        self.ptr == other.ptr
    }
}

impl<'a, K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> ChunkPtr<'a, K, V, A, ALLOC> {
    fn is_null(&self) -> bool {
        self.ptr as usize == 0
    }

    fn is_disabled(&self) -> bool {
        self.ptr as usize == DISABLED_CHUNK_PTR
    }
}

struct ChunkMeta {
    partitions: AtomicUsize,
    array_version: AtomicUsize,
}

type Migrations = [MigrationBits; MIGRATION_BITS_ARR_SIZE];

struct Partition<K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> {
    current_chunk: AtomicUsize,
    history_chunk: AtomicUsize,
    epoch: AtomicUsize,
    arr_ver: AtomicUsize,
    _marker: PhantomData<(K, V, A, ALLOC)>,
}

struct PartitionArray<K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> {
    len: usize,
    hash_masking: usize,
    version: usize,
    meta_array_offset: usize,
    chunk_offset: usize,
    hash_shift: u32,
    ref_cnt: AtomicUsize,
    chunk_size: usize,
    _marker: PhantomData<(K, V, A, ALLOC)>,
}

#[derive(Debug, Clone, Copy)]
struct ArrId(usize);

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

impl<K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> PartitionArray<K, V, A, ALLOC> {
    const PARTITION_SIZE_BYTES: usize = mem::size_of::<Partition<K, V, A, ALLOC>>();
    const PARTITION_SIZE_SHIFT: u32 = Self::PARTITION_SIZE_BYTES.trailing_zeros();

    fn new(len: usize, version: usize, chunk_size: usize) -> *mut Self {
        debug_assert!(len.is_power_of_two());
        debug_assert_eq!(
            len << Self::PARTITION_SIZE_SHIFT,
            len * Self::PARTITION_SIZE_BYTES
        );
        debug_assert_eq!(2, 2usize.next_power_of_two());

        let parts_chunk_size = len * chunk_size;
        let meta_part_array_size = len << Self::PARTITION_SIZE_SHIFT;

        let self_size = mem::size_of::<Self>();
        let self_padding = align_padding(self_size, 8);
        let self_total_size = self_size + self_padding;

        let meta_parts_size = self_total_size + meta_part_array_size;
        let meta_parts_padding = align_padding(meta_parts_size, CACHE_LINE_SIZE);
        let meta_parts_total_size = meta_parts_size + meta_parts_padding;

        let meta_array_offset = self_total_size;
        let chunk_offset = meta_parts_total_size;

        let total_size = meta_parts_total_size + parts_chunk_size;

        let raw_ptr = unsafe { libc::malloc(total_size) };
        let arr_ptr = raw_ptr as _;
        let hash_shift = (mem::size_of::<usize>() as u32 * 8) - len.trailing_zeros();
        let hash_masking = len - 1;
        unsafe {
            libc::memset(raw_ptr, 0, meta_parts_size);
            ptr::write(
                arr_ptr,
                Self {
                    len,
                    hash_masking,
                    hash_shift,
                    version,
                    chunk_size,
                    meta_array_offset,
                    chunk_offset,
                    ref_cnt: AtomicUsize::new(1),
                    _marker: PhantomData,
                },
            );
        }
        return arr_ptr;
    }

    #[inline(always)]
    fn self_ptr(&self) -> *const Self {
        self as *const Self
    }

    #[inline(always)]
    fn ptr_addr_of_part_chunk(&self, id: ArrId) -> usize {
        let base_addr = self.self_ptr() as usize + self.chunk_offset;
        return base_addr + id.0 * self.chunk_size;
    }

    #[inline(always)]
    fn ptr_addr_of(&self, id: ArrId) -> usize {
        debug_assert!(id.0 < self.len);
        debug_assert_eq!(
            id.0 * Self::PARTITION_SIZE_BYTES,
            id.0 << Self::PARTITION_SIZE_SHIFT
        );
        let base_addr = self.self_ptr() as usize + self.meta_array_offset;
        return base_addr + (id.0 << Self::PARTITION_SIZE_SHIFT);
    }

    #[inline(always)]
    fn at(&self, id: ArrId) -> &Partition<K, V, A, ALLOC> {
        unsafe {
            let addr = self.ptr_addr_of(id);
            &*(addr as *const Partition<K, V, A, ALLOC>)
        }
    }

    #[inline(always)]
    fn ref_from_addr<'a>(addr: usize) -> &'a Self {
        unsafe { &*(addr as *const Self) }
    }

    #[inline(always)]
    fn id_of_hash(&self, hash: usize) -> ArrId {
        let r = (hash >> self.hash_shift) & self.hash_masking;
        debug_assert!(r < self.len);
        return ArrId(r);
    }

    #[inline(always)]
    fn hash_part(&self, hash: usize) -> (&Partition<K, V, A, ALLOC>, ArrId, usize) {
        let hash_id = self.id_of_hash(hash);
        let part = self.at(hash_id);
        let part_epoch = part.epoch();
        return (part, hash_id, part_epoch);
    }

    fn iter<'a>(&'a self) -> impl Iterator<Item = &'a Partition<K, V, A, ALLOC>> + 'a {
        (0..self.len).map(move |i| self.at(ArrId(i)))
    }
    fn clear<'a>(&self, guard: &'a Guard) {
        let mut marked = collections::HashSet::<usize>::new();
        let mut marked_ptrs = Vec::with_capacity(self.len << 1);
        self.iter().for_each(|part| {
            let current = part.current();
            let history = part.history();
            if !current.is_null() && !marked.contains(&current.base) {
                marked.insert(current.base);
                marked_ptrs.push(current);
            }
            if !history.is_null() && !history.is_disabled() && !marked.contains(&history.base) {
                marked.insert(history.base);
                marked_ptrs.push(history);
            }
        });
        unsafe {
            guard.defer_unchecked(move || {
                for ptr in marked_ptrs {
                    ptr.dispose();
                }
            });
        }
    }
    fn key_capacity(&self) -> usize {
        self.iter()
            .filter_map(|part| {
                let current = part.current();
                (!current.is_null()).then(|| current.capacity)
            })
            .sum()
    }

    fn gc<'a>(addr: usize) {
        let parts = Self::ref_from_addr(addr);
        if parts.ref_cnt.fetch_sub(1, AcqRel) == 1 {
            unsafe {
                libc::free(addr as _);
            }
        }
    }
}

impl<K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> Partition<K, V, A, ALLOC> {
    #[inline(always)]
    fn epoch(&self) -> usize {
        self.epoch.load(Relaxed)
    }
    #[inline(always)]
    fn set_epoch(&self, epoch: usize) {
        self.epoch.store(epoch, Release);
    }
    #[inline(always)]
    fn set_arr_ver(&self, epoch: usize) {
        self.arr_ver.store(epoch, Release);
    }
    #[inline(always)]
    fn current<'a>(&self) -> ChunkPtr<'a, K, V, A, ALLOC> {
        ChunkPtr::new(self.current_chunk.load(Relaxed) as _)
    }
    #[inline(always)]
    fn history<'a>(&self) -> ChunkPtr<'a, K, V, A, ALLOC> {
        ChunkPtr::new(self.history_chunk.load(Relaxed) as _)
    }
    #[inline(always)]
    fn swap_in_history<'a>(&self, chunk_ptr: ChunkPtr<'a, K, V, A, ALLOC>) -> Result<usize, usize> {
        self.history_chunk
            .compare_exchange(0, chunk_ptr.ptr as _, AcqRel, Relaxed)
    }
    #[inline(always)]
    fn set_history<'a>(&self, chunk_ptr: ChunkPtr<'a, K, V, A, ALLOC>) {
        self.history_chunk.store(chunk_ptr.ptr as _, Release);
    }
    #[inline(always)]
    fn set_current<'a>(&self, chunk_ptr: ChunkPtr<'a, K, V, A, ALLOC>) {
        self.current_chunk.store(chunk_ptr.ptr as _, Release);
    }
    fn erase_history<'a>(&self, guard: &'a Guard) -> bool {
        unsafe {
            let old_addr = self.history_chunk.swap(0, AcqRel);
            let is_some = old_addr != 0;
            if is_some {
                guard.defer_unchecked(move || {
                    (ChunkPtr::<'a, K, V, A, ALLOC> {
                        ptr: old_addr as _,
                        _marker: PhantomData,
                    })
                    .dispose();
                });
            }
            return is_some;
        }
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
        Self::with_max_capacity(cap, DEFAULT_PARTITION_MAX_CAP, attachment_init_meta)
    }

    fn init_part_nums(user_cap: usize, max_cap: usize) -> usize {
        min(
            max(INIT_ARR_SIZE, user_cap / max_cap),
            num_cpus::get_physical(),
        )
        .next_power_of_two()
    }

    pub fn with_max_capacity(
        cap: usize,
        max_cap: usize,
        attachment_init_meta: A::InitMeta,
    ) -> Self {
        let init_arr_len = Self::init_part_nums(cap, max_cap);
        let partitions = PartitionArray::<K, V, A, ALLOC>::new(init_arr_len, INIT_ARR_VER, 0);
        let init_partition_cap = (cap > (init_arr_len << 2))
            .then(|| cap / init_arr_len)
            .unwrap_or(cap)
            .next_power_of_two();
        unsafe {
            (*partitions).iter().for_each(|part| {
                let chunk =
                    Chunk::<K, V, A, ALLOC>::alloc_chunk(init_partition_cap, &attachment_init_meta);
                part.set_current(ChunkPtr::new(chunk));
                part.set_epoch(2);
            });
        }
        Self {
            meta: ChunkMeta {
                partitions: AtomicUsize::new(partitions as _),
                array_version: AtomicUsize::new(INIT_ARR_VER),
            },
            count: Counter::new(),
            init_cap: init_partition_cap,
            init_arr_len,
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
            let (chunk, new_chunk, epoch, part, _, _arr_ver) = self.chunk_refs(hash, guard);
            let mut v;
            if let Some((mut val, val_addr, aitem)) =
                self.get_from_chunk(chunk, hash, key, fkey, &backoff, epoch)
            {
                'SPIN: loop {
                    v = val.val;
                    if val.is_valued() {
                        let act_val = val.act_val::<V>();
                        let mut attachment = None;
                        if Self::FAT_VAL && read_attachment {
                            attachment = Some(aitem.get_value());
                            let new_val = Self::get_fast_value(val_addr);
                            if new_val.val != val.val {
                                val = new_val;
                                continue 'SPIN;
                            }
                        }
                        return Some((Self::offset_v_out(act_val), attachment, val_addr));
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
                if let Some((mut val, val_addr, aitem)) =
                    self.get_from_chunk(new_chunk, hash, key, fkey, &backoff, epoch)
                {
                    'SPIN_NEW: loop {
                        let v = val.val;
                        if val.is_valued() {
                            let act_val = val.act_val::<V>();
                            let mut attachment = None;
                            if Self::FAT_VAL && read_attachment {
                                attachment = Some(aitem.get_value());
                                let new_val = Self::get_fast_value(val_addr);
                                if new_val.val != val.val {
                                    val = new_val;
                                    continue 'SPIN_NEW;
                                }
                            }
                            return Some((Self::offset_v_out(act_val), attachment, val_addr));
                        } else if v == SENTINEL_VALUE {
                            backoff.spin();
                            continue 'OUTER;
                        } else {
                            break 'SPIN_NEW;
                        }
                    }
                }
            }
            let new_epoch = part.epoch();
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
            let arr_ver = self.current_arr_ver();
            let parts = self.partitions();
            let ((part, _, epoch), _obtained_arr_ver) = (parts.hash_part(hash), parts.version);
            let current_chunk = part.current();
            let history_chunk = part.history();
            let is_copying = Self::is_copying(epoch);
            let chunk;
            let mut new_chunk = None;
            let mut can_migrate_slabs = false;
            if history_chunk.is_disabled() || part.epoch() != epoch {
                backoff.spin();
                continue;
            }
            if history_chunk.is_null() {
                chunk = current_chunk;
            } else if current_chunk.is_null() {
                chunk = history_chunk;
            } else if history_chunk != current_chunk {
                if current_chunk.occupation.load(Relaxed) >= current_chunk.occu_limit {
                    self.iter_migrate_slabs(hash, history_chunk, part, parts, &guard);
                    continue;
                }
                can_migrate_slabs = true;
                new_chunk = Some(current_chunk);
                chunk = history_chunk
            } else {
                chunk = current_chunk;
            }
            let modify_chunk = new_chunk.unwrap_or(chunk);
            let masked_value = fvalue & VAL_BIT_MASK;
            let mod_op = match op {
                InsertOp::Insert => ModOp::Insert(masked_value, value),
                InsertOp::TryInsert => ModOp::AttemptInsert(masked_value, value),
            };
            let lock_old = new_chunk
                .map(|_| self.modify_entry(chunk, hash, key, fkey, ModOp::Lock, true, &guard));
            let value_insertion =
                self.modify_entry(modify_chunk, hash, key, fkey, mod_op, true, &guard);
            let result;
            let reset_locked_old = || match &lock_old {
                Some(ModResult::Replaced(fv, _v, val_addr)) => {
                    Self::store_value(*val_addr, *fv);
                }
                Some(ModResult::NotFound) | Some(ModResult::Sentinel) | None => {}
                _ => {
                    error!("Invalid modify entry state");
                    unreachable!("Invalid modify entry state");
                }
            };
            match value_insertion {
                ModResult::Done(fv, v, idx) => {
                    self.count.incr(1);
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
                    self.do_migration(hash, chunk, epoch, part, parts, arr_ver, &guard);
                    continue;
                }
                ModResult::Sentinel => {
                    trace!("Discovered sentinel on insertion table upon probing, retry");
                    reset_locked_old();
                    backoff.spin();
                    continue;
                }
                ModResult::NotFound => match &lock_old {
                    Some(ModResult::Replaced(fv, _v, val_addr)) => {
                        Self::store_value(*val_addr, *fv);
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
                (None, Some(ModResult::Replaced(fv, _v, val_addr))) => {
                    Self::store_value(val_addr, fv);
                    backoff.spin();
                    continue;
                }
                (None, Some(_)) => {
                    backoff.spin();
                    continue;
                }
                (Some((0, _)), Some(ModResult::Replaced(fv, v, val_addr))) => {
                    delay_log!(
                        "Insert have Some((0, _)), Some(ModResult::Replaced({}, _, {})) key {}, old chunk {}, new chunk {:?}, epoch {}",
                        fv, val_addr, fkey, chunk.base, new_chunk.map(|c| c.base), epoch
                    );
                    // New insertion in new chunk and have stuff in old chunk
                    Self::store_sentinel(val_addr);
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
                (Some((fv, v)), Some(ModResult::Replaced(lfv, _v, val_addr))) => {
                    delay_log!(
                        "Insert have Some(({}, _)), Some(ModResult::Replaced({}, _, {})) key {}, old chunk {}, new chunk {:?}, epoch {}",
                        fv, lfv, val_addr, fkey, chunk.base, new_chunk.map(|c| c.base), epoch
                    );
                    // Replaced new chunk, should put sentinel in old chunk
                    Self::store_sentinel(val_addr);
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
            if can_migrate_slabs {
                self.iter_migrate_slabs(hash, history_chunk, part, parts, &guard);
            } else if !is_copying && new_chunk.is_none() && self.need_migration(chunk) {
                self.do_migration(hash, chunk, epoch, part, parts, arr_ver, &guard);
            }
            return res;
        }
    }

    pub fn clear(&self) {
        let guard = crossbeam_epoch::pin();
        let init_arr_len = self.init_arr_len;
        let new_parts = PartitionArray::new(init_arr_len, INIT_ARR_VER, 0);
        unsafe {
            (*new_parts).iter().for_each(|part| {
                let chunk =
                    Chunk::<K, V, A, ALLOC>::alloc_chunk(self.init_cap, &self.attachment_init_meta);
                part.set_current(ChunkPtr::new(chunk));
                part.set_epoch(2);
            });
        }
        let len = self.len();
        let old_part_addr = self.meta.partitions.swap(new_parts as _, AcqRel);
        PartitionArray::<K, V, A, ALLOC>::ref_from_addr(old_part_addr).clear(&guard);
        self.count.decr(len as _);
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
            let (chunk, new_chunk, epoch, _part, _, _arr_ver) = self.chunk_refs(hash, guard);
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
                    match self.get_from_chunk(chunk, hash, key, fkey, &backoff, epoch) {
                        Some((fv, val_addr, _)) => {
                            // This is a tranision state and we just don't bother for its complexity
                            // Simply retry and wait until the entry is moved to new chunk
                            if fv.is_valued() {
                                Self::wait_entry(val_addr, fkey, fv.val, &backoff);
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
            let (chunk, new_chunk, _epoch, _part, _, _arr_ver) = self.chunk_refs(hash, &guard);
            let modify_chunk = new_chunk.unwrap_or(chunk);
            let old_chunk_val = new_chunk
                .map(|_| self.modify_entry(chunk, hash, key, fkey, ModOp::Sentinel, true, &guard));
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
                        self.count.decr(1);
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
                    self.count.decr(1);
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
    fn partitions<'a>(&self) -> &PartitionArray<K, V, A, ALLOC> {
        PartitionArray::ref_from_addr(self.meta.partitions.load(Relaxed))
    }

    #[inline]
    fn part_of_hash(&self, hash: usize) -> ((&Partition<K, V, A, ALLOC>, ArrId, usize), usize) {
        let parts = self.partitions();
        (parts.hash_part(hash), parts.version)
    }

    #[inline]
    fn chunk_refs<'a>(
        &self,
        hash: usize,
        _guard: &'a Guard,
    ) -> (
        ChunkPtr<'a, K, V, A, ALLOC>,
        Option<ChunkPtr<'a, K, V, A, ALLOC>>,
        usize,
        &Partition<K, V, A, ALLOC>,
        &PartitionArray<K, V, A, ALLOC>,
        usize,
    ) {
        let backoff = Backoff::new();
        loop {
            let arr_ver = self.current_arr_ver();
            let parts = self.partitions();
            let ((part, _, current_epoch), _obtained_arr_ver) =
                (parts.hash_part(hash), parts.version);
            let current_chunk = part.current();
            let history_chunk = part.history();
            let is_copying = Self::is_copying(current_epoch);

            if current_epoch == part.epoch() {
                if !is_copying {
                    if !history_chunk.is_null() && current_chunk.is_null() {
                        // For placeholders of split but unmigrated chunks
                        return (history_chunk, None, current_epoch, part, parts, arr_ver);
                    } else if !current_chunk.is_null() {
                        return (current_chunk, None, current_epoch, part, parts, arr_ver);
                    }
                } else if !history_chunk.is_null()
                    && !current_chunk.is_null()
                    && current_chunk != history_chunk
                {
                    return (
                        history_chunk,
                        Some(current_chunk),
                        current_epoch,
                        part,
                        parts,
                        arr_ver,
                    );
                }
            }
            backoff.spin();
        }
    }

    pub fn len(&self) -> usize {
        self.count.count() as _
    }

    fn get_from_chunk(
        &self,
        chunk: ChunkPtr<K, V, A, ALLOC>,
        hash: usize,
        key: &K,
        fkey: FKey,
        backoff: &Backoff,
        epoch: usize,
    ) -> Option<(FastValue, usize, A::Item)> {
        debug_assert_ne!(chunk.ptr as usize, 0);
        let cap_mask = chunk.cap_mask;
        let home_idx = hash & cap_mask;
        let mut iter = SlotIter::new(home_idx, cap_mask);
        let (mut idx, _count) = iter.next().unwrap();
        let (mut key_addr, mut val_addr) = chunk.entry_addr(idx);
        loop {
            let k = Self::get_fast_key(key_addr);
            if k == fkey {
                let val_res = Self::get_fast_value(val_addr);
                let raw = val_res.val;
                let attachment = chunk.attachment.prefetch(idx);
                let probe = attachment.probe(key);
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
                    return Some((val_res, val_addr, attachment));
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

            if let Some((new_idx, _)) = iter.next() {
                idx = new_idx;
                (key_addr, val_addr) = chunk.entry_addr(idx);
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
        let cap_mask = chunk.cap_mask;
        let backoff = crossbeam_utils::Backoff::new();
        let home_idx = hash & cap_mask;
        let mut iter = SlotIter::new(home_idx, cap_mask);
        let (mut idx, _) = iter.next().unwrap();
        let (mut key_addr, mut val_addr) = chunk.entry_addr(idx);
        loop {
            let k = Self::get_fast_key(key_addr);
            let v = Self::get_fast_value(val_addr);
            let raw = v.val;
            if k == EMPTY_KEY {
                match op {
                    ModOp::Insert(fval, val) | ModOp::AttemptInsert(fval, val) => {
                        let cas_fval = fval;
                        let primed_fval = Self::if_fat_val_then_val(LOCKED_VALUE, cas_fval);
                        match Self::cas_value(val_addr, EMPTY_VALUE, primed_fval) {
                            (_, true) => {
                                chunk.occupation.fetch_add(1, AcqRel);
                                let attachment = chunk.attachment.prefetch(idx);
                                attachment.set_key(key.clone());
                                if Self::FAT_VAL {
                                    val.map(|val| attachment.set_value((*val).clone(), 0));
                                    Self::store_raw_value(val_addr, fval);
                                    Self::store_key(key_addr, fkey);
                                } else {
                                    Self::store_key(key_addr, fkey);
                                }
                                return ModResult::Done(0, None, idx);
                            }
                            (SENTINEL_VALUE, false) => return ModResult::Sentinel,
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
            } else if k == fkey && raw == SENTINEL_VALUE {
                return ModResult::Sentinel;
            } else if raw == SENTINEL_VALUE {
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
                if key_probe {
                    let act_val = v.act_val::<V>();
                    if raw < TOMBSTONE_VALUE || v.is_primed() {
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
                    match op {
                        ModOp::Insert(fval, ov) => {
                            // Insert with attachment should prime value first when
                            // duplicate key discovered
                            let primed_fval = Self::if_fat_val_then_val(LOCKED_VALUE, fval);
                            let prev_val = read_attachment.then(|| attachment.get_value());
                            let val_to_store = Self::value_to_store(raw, fval);
                            if Self::cas_value(val_addr, raw, primed_fval).1 {
                                ov.map(|ov| attachment.set_value(ov.clone(), raw));
                                if Self::FAT_VAL {
                                    Self::store_raw_value(val_addr, val_to_store);
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
                            if Self::cas_sentinel(val_addr, v.val) {
                                let prev_val = read_attachment.then(|| attachment.get_value());
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
                            if Self::cas_tombstone(val_addr, v.val) {
                                let prev_val = read_attachment.then(|| attachment.get_value());
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
                            if Self::cas_value(val_addr, v.val, LOCKED_VALUE).1 {
                                let attachment = read_attachment.then(|| attachment.get_value());
                                return ModResult::Replaced(act_val, attachment, val_addr);
                            } else {
                                backoff.spin();
                                continue;
                            }
                        }
                        ModOp::AttemptInsert(fval, oval) => {
                            if act_val == TOMBSTONE_VALUE || act_val == EMPTY_VALUE {
                                let primed_fval = Self::if_fat_val_then_val(LOCKED_VALUE, fval);
                                let prev_val = read_attachment.then(|| attachment.get_value());
                                let val_to_store = Self::value_to_store(raw, fval);
                                if Self::cas_value(val_addr, v.val, primed_fval).1 {
                                    oval.map(|oval| attachment.set_value((*oval).clone(), raw));
                                    if Self::FAT_VAL {
                                        Self::store_raw_value(val_addr, val_to_store);
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
                                    && Self::get_fast_value(val_addr).val != v.val
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
                                if Self::cas_value(val_addr, raw, Self::offset_v_in(sv)).1 {
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
                }
            }
            // trace!("Reprobe inserting {} got {}", fkey, k);
            if let Some(new_iter) = iter.next() {
                (idx, _) = new_iter;
                (key_addr, val_addr) = chunk.entry_addr(idx);
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
    fn wait_entry(val_addr: usize, orig_key: FKey, orig_val: FVal, backoff: &Backoff) {
        loop {
            if Self::get_fast_value(val_addr).val != orig_val {
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
        let mut res = Vec::with_capacity(chunk.occupation.load(Relaxed) as _);
        let cap_mask = chunk.cap_mask;
        while counter < cap {
            idx &= cap_mask;
            let (key_addr, val_addr) = chunk.entry_addr(idx);
            let k = Self::get_fast_key(key_addr);
            if k != EMPTY_KEY {
                let val_res = Self::get_fast_value(val_addr);
                let act_val = val_res.act_val::<V>();
                if act_val > MAX_META_VAL {
                    let attachment = chunk.attachment.prefetch(idx);
                    let key = attachment.get_key();
                    let value = attachment.get_value();
                    if Self::FAT_VAL && Self::get_fast_value(val_addr).val != val_res.val {
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
        let parts = self.partitions();
        let current_map = parts
            .iter()
            .filter_map(|part| {
                let current = part.current();
                if current.is_null() {
                    None
                } else {
                    Some(self.all_from_chunk(current))
                }
            })
            .flatten()
            .map(|(k, v, fk, fv)| ((k, fk), (v, fv)))
            .collect::<std::collections::HashMap<_, _>>();
        let history_map = parts
            .iter()
            .filter_map(|part| {
                let history = part.history();
                if history.is_null() {
                    None
                } else {
                    Some(self.all_from_chunk(history))
                }
            })
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
    fn get_fast_key(key_addr: usize) -> FKey {
        Chunk::<K, V, A, ALLOC>::get_fast_key(key_addr)
    }

    #[inline(always)]
    pub fn get_fast_value(val_addr: usize) -> FastValue {
        Chunk::<K, V, A, ALLOC>::get_fast_value(val_addr)
    }

    #[inline(always)]
    fn cas_tombstone(val_addr: usize, original: FVal) -> bool {
        let addr = val_addr;
        unsafe {
            intrinsics::atomic_cxchg_acqrel_relaxed(addr as *mut FVal, original, TOMBSTONE_VALUE).1
        }
    }
    #[inline(always)]
    fn cas_value(val_addr: usize, original: FVal, value: FVal) -> (FVal, bool) {
        let addr = val_addr;
        unsafe { intrinsics::atomic_cxchg_acqrel_relaxed(addr as *mut FVal, original, value) }
    }

    #[inline(always)]
    fn store_value(val_addr: usize, value: FVal) {
        let addr = val_addr;
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
    fn store_raw_value(val_addr: usize, value: FVal) {
        let addr = val_addr;
        unsafe { intrinsics::atomic_store_release(addr as *mut FVal, value) };
    }

    #[inline(always)]
    fn store_sentinel(val_addr: usize) {
        let addr = val_addr;
        unsafe { intrinsics::atomic_store_release(addr as *mut FVal, SENTINEL_VALUE) };
    }

    #[inline(always)]
    fn store_key(key_addr: usize, fkey: FKey) {
        unsafe { intrinsics::atomic_store_release(key_addr as *mut FKey, fkey) }
    }

    #[inline(always)]
    fn cas_sentinel(val_addr: usize, original: FVal) -> bool {
        let addr = val_addr;
        let (val, done) = unsafe {
            intrinsics::atomic_cxchg_acqrel_relaxed(addr as *mut FVal, original, SENTINEL_VALUE)
        };
        done || ((val & FVAL_VAL_BIT_MASK) == SENTINEL_VALUE)
    }

    #[inline(always)]
    fn current_arr_ver(&self) -> usize {
        self.meta.array_version.load(Relaxed)
    }

    #[inline(always)]
    fn need_split_chunk<'a>(&self, part: &Partition<K, V, A, ALLOC>, arr_ver: usize) -> bool {
        // Check if the chunk need to be split
        // During array splitting, partition pointers would be duplicated
        // This function however, would detect those duplicates by checking arr_ver with partition version
        part.arr_ver.load(Relaxed) < arr_ver
    }

    #[inline(always)]
    fn need_split_array<'a>(
        &self,
        old_chunk_ptr: ChunkPtr<'a, K, V, A, ALLOC>,
        part: &Partition<K, V, A, ALLOC>,
        arr_ver: usize,
    ) -> bool {
        // Statsticly, a oversized partition indicates that other partitions in the array is also full
        // Such that we don't need to check every partitions in the array for it is expensive
        // Justification: Hash functions should be able to spread entries across all partitions evenly
        debug_assert!(old_chunk_ptr.capacity <= self.max_cap);
        old_chunk_ptr.capacity == self.max_cap && part.arr_ver.load(Relaxed) == arr_ver
    }

    #[inline(always)]
    fn need_migration<'a>(&self, old_chunk_ptr: ChunkPtr<'a, K, V, A, ALLOC>) -> bool {
        let occupation = old_chunk_ptr.occupation.load(Relaxed);
        let occu_limit = old_chunk_ptr.occu_limit;
        return occupation > occu_limit;
    }

    fn do_migration<'a>(
        &self,
        hash: usize,
        old_chunk_ptr: ChunkPtr<'a, K, V, A, ALLOC>,
        old_epoch: usize,
        part: &Partition<K, V, A, ALLOC>,
        parts: &PartitionArray<K, V, A, ALLOC>,
        arr_ver: usize,
        guard: &Guard,
    ) -> ResizeResult {
        if self.need_split_chunk(part, arr_ver) {
            return self.split_migration(
                hash,
                old_chunk_ptr,
                old_epoch,
                arr_ver,
                part,
                parts,
                guard,
            );
        }
        if self.need_split_array(old_chunk_ptr, part, arr_ver) {
            // Fail and let the upper caller try again
            return self
                .split_array(arr_ver, &old_chunk_ptr, guard)
                .map(|_| ResizeResult::Done)
                .unwrap_or_else(|| {
                    debug!("Cannot split array");
                    ResizeResult::SwapFailed
                });
        }
        self.resize_migration(hash, old_chunk_ptr, old_epoch, arr_ver, part, parts, guard)
    }

    fn split_array<'a>(
        &self,
        prev_arr_ver: usize,
        old_chunk: &ChunkPtr<K, V, A, ALLOC>,
        guard: &'a Guard,
    ) -> Option<usize> {
        debug_assert!(!Self::is_copying(prev_arr_ver));
        debug_assert_eq!(old_chunk.capacity, self.max_cap);

        let backoff = Backoff::new();

        // First lock the array by swapping ver+1
        if self
            .meta
            .array_version
            .compare_exchange(prev_arr_ver, prev_arr_ver + 1, AcqRel, Relaxed)
            .is_ok()
        {
            // Lock obtained. Should be safe to allocate new array and store
            let old_part_arr_addr = self.meta.partitions.load(Relaxed);
            let old_part_arr = PartitionArray::<K, V, A, ALLOC>::ref_from_addr(old_part_arr_addr);
            let new_arr_ver = prev_arr_ver + 2;
            debug_assert_eq!(old_part_arr.version, prev_arr_ver);
            let new_size = old_part_arr.len << 2; // x4
            let chunk_sizes = Chunk::<K, V, A, ALLOC>::size_of(self.max_cap);
            let chunk_total_size = chunk_sizes.total_size;
            let new_part_arr =
                PartitionArray::<K, V, A, ALLOC>::new(new_size, new_arr_ver, chunk_total_size);
            debug!(
                "- Resizing array, prev version: {}, size {} to {}, mask {:b}",
                prev_arr_ver,
                old_part_arr.len,
                new_size,
                new_size - 1
            );
            let process_part = |new_idx| unsafe {
                // Copy chunk pointers from the old partition when new_idx is even number and
                // Put shadow chunk when the new_idx is odd number
                debug_assert!(new_idx % 2 == 0); // Assert new_idx is even number
                let old_idx = new_idx >> 2;
                let old_part = old_part_arr.at(ArrId(old_idx));
                let new_part_addr = (*new_part_arr).ptr_addr_of(ArrId(new_idx));
                let old_epoch = old_part.epoch();
                let old_current = old_part.current_chunk.load(Relaxed);
                let old_history = old_part.history_chunk.load(Relaxed);
                let old_ver = old_part.arr_ver.load(Relaxed);
                let expecting_history = (old_current == 0).then_some(old_history).unwrap_or(0);
                if Self::is_copying(old_epoch) || old_current == old_history {
                    return false;
                }
                if !old_part
                    .history_chunk
                    .compare_exchange(expecting_history, DISABLED_CHUNK_PTR, AcqRel, Relaxed)
                    .is_ok()
                {
                    return false;
                }
                if old_epoch != old_part.epoch() {
                    old_part.history_chunk.store(expecting_history, Release);
                    return false;
                }
                debug_assert_eq!(old_part.history_chunk.load(Relaxed), DISABLED_CHUNK_PTR);
                debug_assert_ne!(old_current, old_history);
                ptr::write(
                    new_part_addr as *mut _,
                    Partition::<K, V, A, ALLOC> {
                        current_chunk: AtomicUsize::new(old_current),
                        history_chunk: AtomicUsize::new(old_history), // We had already asserted no copying during array resize.
                        epoch: AtomicUsize::new(old_epoch),
                        arr_ver: AtomicUsize::new(old_ver),
                        _marker: PhantomData,
                    },
                );
                // Prepare the duplicated shadow partition
                // Here we assume that the partition is all zeroed out
                // Note that zero current chunk indicates shadow partition
                for i in 1..4 {
                    let dup_chunk = (old_current != 0)
                        .then_some(old_current)
                        .unwrap_or(old_history);
                    let new_dup_part = (*new_part_arr).at(ArrId(new_idx + i));
                    new_dup_part.history_chunk.store(dup_chunk, Relaxed);
                    new_dup_part.epoch.store(old_epoch, Relaxed);
                    new_dup_part.arr_ver.store(old_ver, Relaxed);
                }
                true
            };
            loop {
                let all_set = (0..new_size).step_by(4).all(|idx| {
                    let part = unsafe { (*new_part_arr).at(ArrId(idx)) };
                    let mut been_set = part.epoch() > 0;
                    if !been_set {
                        been_set = process_part(idx);
                    }
                    if !been_set {
                        backoff.spin();
                    }
                    been_set
                });
                if all_set {
                    break;
                }
            }
            // Now the new array is initialized, set its pointer to meta
            self.meta.partitions.store(new_part_arr as _, Relaxed); // Use store becasue we have the lock
                                                                    // Set the partition version to unlock array
            self.meta.array_version.store(new_arr_ver, Release);

            unsafe {
                guard.defer_unchecked(move || {
                    PartitionArray::<K, V, A, ALLOC>::gc(old_part_arr_addr);
                });
            }

            debug!("- Resizing array complete, prev version: {}", prev_arr_ver);
            return Some(new_arr_ver); // Done
        } else {
            debug!("Cannot obtain lock for split array, will retry");
            return None;
        }
    }

    fn split_migration<'a>(
        &self,
        hash: usize,
        old_chunk_ptr: ChunkPtr<'a, K, V, A, ALLOC>,
        old_epoch: usize,
        arr_ver: usize,
        part: &Partition<K, V, A, ALLOC>,
        parts: &PartitionArray<K, V, A, ALLOC>,
        guard: &crossbeam_epoch::Guard,
    ) -> ResizeResult {
        let part_arr_ver = part.arr_ver.load(Relaxed);
        let current_arr_ver = parts.version;
        let ver_diff = current_arr_ver - part_arr_ver;
        let size_diff = ver_diff;
        let hash_id = parts.id_of_hash(hash);
        let home_id = hash_id.0 >> size_diff << size_diff;
        let home_part = parts.at(ArrId(home_id));
        if self.meta.array_version.load(Acquire) != arr_ver {
            // Array split have the highest priority
            debug!("Array version is not matching");
            return ResizeResult::SwapFailed;
        }
        if let Err(_orig_history) = home_part.swap_in_history(old_chunk_ptr) {
            debug!(
                "Cannot obtain lock for split migration home at {}, hash at {}, current {}, history {}, total parts {}, hash ({}){:0>64b}, will retry",
                home_id, hash_id.0, home_part.current().ptr as usize, home_part.history().ptr as usize, parts.len, hash, hash
            );
            debug_assert!(
                !home_part.current().is_null(),
                "Home partition must be available"
            );
            return ResizeResult::SwapFailed;
        }
        debug_assert_eq!(home_part.history().base, old_chunk_ptr.base);
        debug_assert_eq!(
            part.history().base,
            old_chunk_ptr.base,
            "At this point all relevant parts should have history been set"
        );
        debug!(
            "Veryfying partition array version: {} to {}",
            arr_ver, current_arr_ver
        );
        let home_epoch = home_part.epoch();
        if old_epoch != home_epoch || arr_ver != current_arr_ver {
            home_part.set_history(ChunkPtr::null());
            debug!(
                "$ Epoch changed from {} to {}, arr_ver {} to {} after split migration lock",
                old_epoch, old_epoch, arr_ver, current_arr_ver
            );
            return ResizeResult::SwapFailed;
        }

        let ends_id = ((hash_id.0 >> size_diff) + 1) << size_diff;
        let chunk_capacity = old_chunk_ptr.capacity;
        // A presertive size which still allow new insertions to the chunks
        // Do NOT change the size, it is related to recover `pre_occupation` for slab migration
        let pre_occupation = chunk_capacity >> 1;
        debug_assert!(size_diff > 0);
        debug!(
            "Split {} from {} to {}, ver from {} to {}, ver_diff: {}, size_diff: {}, chunk base {}, part {:?}, part_arr_ver {}", 
            old_chunk_ptr.base,
            home_id, ends_id, 
            part_arr_ver, current_arr_ver, 
            ver_diff, size_diff, old_chunk_ptr.base, 
            part as *const _ as usize,
            part.arr_ver.load(Relaxed)
        );
        let sizes = Chunk::<K, V, A, ALLOC>::size_of(self.max_cap);
        let chunks_to_alloc = ends_id - home_id;
        unsafe {
            old_chunk_ptr.init_copied(chunks_to_alloc);
        }
        // Enumerate all shadow partition of this home partition
        // Must do it in reverse order such that insertions are not falling to new home part
        for id in (home_id..ends_id).rev() {
            let shadow_part = parts.at(ArrId(id));
            let ptr = parts.ptr_addr_of_part_chunk(ArrId(id));
            let chunk = ChunkPtr::new(Chunk::<K, V, A, ALLOC>::initialize_chunk(
                ptr as _,
                self.max_cap,
                &sizes,
                parts.self_ptr(),
                &self.attachment_init_meta,
            ));
            chunk.occupation.store(pre_occupation as _, Relaxed);
            shadow_part.set_epoch(old_epoch + 1);
            shadow_part.set_current(chunk);
        }
        parts.ref_cnt.fetch_add(chunks_to_alloc, AcqRel);
        // Now real migrations will be triggered by other threads such that we can have parallelism
        ResizeResult::Done
    }

    fn resize_migration<'a>(
        &self,
        hash: usize,
        old_chunk_ptr: ChunkPtr<'a, K, V, A, ALLOC>,
        chunk_epoch: usize,
        arr_ver: usize,
        part: &Partition<K, V, A, ALLOC>,
        parts: &PartitionArray<K, V, A, ALLOC>,
        guard: &crossbeam_epoch::Guard,
    ) -> ResizeResult {
        // Swap in new chunk as placeholder for the lock
        if !part.swap_in_history(old_chunk_ptr).is_ok() {
            // other thread have allocated new chunk and wins the competition, exit
            trace!("Cannot obtain lock for resize, will retry");
            return ResizeResult::SwapFailed;
        }
        let old_epoch = part.epoch();
        // After the chunk partition lock is acquired, we need to check if array epoch is changed
        // If so, or partition epoch is changed, we need to reset and try again
        if chunk_epoch != old_epoch || arr_ver != self.current_arr_ver() {
            part.set_history(ChunkPtr::null());
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
        let pre_occupation = old_cap;
        let new_chunk = Chunk::alloc_chunk(new_cap, &self.attachment_init_meta);
        unsafe {
            (*new_chunk).occupation.store(pre_occupation as _, Relaxed);
        }
        let new_chunk_ptr = ChunkPtr::new(new_chunk);
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
        part.set_current(new_chunk_ptr); // Stump becasue we have the lock already
                                         // Not going to take multithreading resize
                                         // Experiments shows there is no significant improvement in performance'
        fence(AcqRel);
        part.set_epoch(old_epoch + 1);
        let part_id = parts.id_of_hash(hash).0;
        let part_range = (part_id, part_id + 1);
        self.migrate_all_entries(old_chunk_ptr, parts, &part_range, &guard);
        let num_migrated =
            Self::update_new_chunk_counter(&old_chunk_ptr, parts, pre_occupation, &part_range);
        fence(AcqRel);
        part.set_epoch(old_epoch + 2);
        part.erase_history(guard);
        debug!(
            "!!! Resize migration for {:?} completed, new chunk is {:?}, size from {} to {}, old epoch {}, num {:?}",
            old_chunk_ptr.base,
            new_chunk_ptr.base,
            old_chunk_ptr.capacity,
            new_chunk_ptr.capacity,
            old_epoch, num_migrated
        );
        ResizeResult::Done
    }

    fn iter_migrate_slabs(
        &self,
        hash: usize,
        old_chunk: ChunkPtr<K, V, A, ALLOC>,
        part: &Partition<K, V, A, ALLOC>,
        parts: &PartitionArray<K, V, A, ALLOC>,
        guard: &crossbeam_epoch::Guard,
    ) -> bool {
        let part_arr_ver = part.arr_ver.load(Relaxed);
        let current_arr_ver = parts.version;
        let ver_diff = current_arr_ver - part_arr_ver;
        if ver_diff == 0 {
            // Wil not migrate resize with slab migration
            return false;
        }
        let size_diff = ver_diff;
        let hash_id = parts.id_of_hash(hash);
        let region = hash_id.0 >> size_diff;
        let home_id = region << size_diff;
        let ends_id = (region + 1) << size_diff;
        let part_range = (home_id, ends_id);
        let home_part = parts.at(ArrId(home_id));
        if home_part.current() == home_part.history() {
            return false;
        }
        (0..MIGRATION_TOTAL_BITS)
            .map(|i| (i + hash) & MIGRATION_TOTAL_MASK)
            .map(|i| self.migrate_slab(i, old_chunk, parts, &part_range, guard))
            .fold(false, |a, b| a || b);
        return Self::wrapup_split_migration(old_chunk, parts, &part_range, guard);
    }

    fn wrapup_split_migration(
        old_chunk: ChunkPtr<K, V, A, ALLOC>,
        parts: &PartitionArray<K, V, A, ALLOC>,
        (home_id, ends_id): &(usize, usize),
        guard: &crossbeam_epoch::Guard,
    ) -> bool {
        // Does all slab migrated?
        if !old_chunk.migrated.iter().all(|bs| bs.load(Acquire) == !0) {
            // If not, just skip this turn
            // Another thread will wrapup after they finised their job
            return false;
        }
        let pre_occupation = old_chunk.capacity >> 1;
        let home_part = parts.at(ArrId(*home_id));
        let old_epoch = home_part.epoch.load(Relaxed);
        if !Self::is_copying(old_epoch) {
            return false;
        }
        if !home_part.erase_history(guard) {
            return false;
        }
        let part_range = (*home_id, *ends_id);
        let current_arr_ver = parts.version;
        let num_migrated =
            Self::update_new_chunk_counter(&old_chunk, parts, pre_occupation, &part_range);
        let new_epoch = old_epoch + 1;
        for id in (*home_id..*ends_id).rev() {
            let shadow_part = parts.at(ArrId(id));
            shadow_part.set_epoch(new_epoch);
            shadow_part.set_arr_ver(current_arr_ver);
            shadow_part.set_history(ChunkPtr::null());
            debug!(
                "Done with split migrating to partitioned chunk {} at {}, part range {:?}, arr ver {}", 
                shadow_part.current().base, id, (home_id, ends_id), current_arr_ver
            );
        }
        debug!(
            ">>> Split migration form {} to {}, completed. Chunk {} Migrated {} paritions, entries {:?}, new epoch {}",
            home_id, ends_id,
            old_chunk.base,
            ends_id - home_id,
            num_migrated,
            new_epoch
        );
        return true;
    }

    fn migrate_all_entries(
        &self,
        old_chunk: ChunkPtr<K, V, A, ALLOC>,
        partitions: &PartitionArray<K, V, A, ALLOC>,
        (part_start, part_end): &(usize, usize),
        guard: &crossbeam_epoch::Guard,
    ) {
        let part_range = (*part_start, *part_end);
        let num_chunks = part_end - part_start;
        let max_slab = min(old_chunk.capacity, MIGRATION_TOTAL_BITS);
        unsafe {
            old_chunk.init_copied(num_chunks);
        }
        (0..max_slab).for_each(|i| {
            self.migrate_slab(i, old_chunk, partitions, &part_range, guard);
        });
    }

    fn update_new_chunk_counter(
        old_chunk: &ChunkPtr<K, V, A, ALLOC>,
        partitions: &PartitionArray<K, V, A, ALLOC>,
        pre_occupation: usize,
        (part_start, part_end): &(usize, usize),
    ) -> usize {
        old_chunk
            .copied
            .iter()
            .zip(*part_start..*part_end)
            .filter(|(_, n)| *n > 0)
            .map(|(effective_copy, part_id)| {
                let effective_copy = effective_copy.swap(0, AcqRel);
                if effective_copy > 0 {
                    let id = ArrId(part_id);
                    let part = partitions.at(id);
                    let new_chunk = part.current();
                    debug_assert!(effective_copy <= pre_occupation);
                    new_chunk
                        .occupation
                        .fetch_sub((pre_occupation - effective_copy) as _, AcqRel);
                }
                effective_copy
            })
            .sum()
    }

    fn migrate_slab(
        &self,
        slab_id: usize,
        old_chunk: ChunkPtr<K, V, A, ALLOC>,
        parts: &PartitionArray<K, V, A, ALLOC>,
        (part_start, part_end): &(usize, usize),
        _guard: &crossbeam_epoch::Guard,
    ) -> bool {
        trace!("Migrating entries from {:?}", old_chunk.base);

        // Prepare to slab migration parameters
        let slab_bits_arr_id = slab_id >> MIGRATION_BITS_SHIFT;
        let slab_bits_id = slab_id & MIGRATION_BITS_MASK;
        let slab_bit_set = 1 << slab_bits_id;

        // Set the slab bits to claim the slab
        let slab_bits_atom = &old_chunk.migrating[slab_bits_arr_id];
        let slab_bits_fin_atom = &old_chunk.migrated[slab_bits_arr_id];
        let fetched_bits = slab_bits_atom.fetch_or(slab_bit_set, Relaxed);
        if fetched_bits | slab_bit_set == fetched_bits {
            // If the bit already set
            return false;
        }

        let chunk_cap = old_chunk.capacity;
        let slab_mult_shift = chunk_cap
            .trailing_zeros()
            .saturating_sub(MIGRATION_TOTAL_BITS_SHIFT);
        let start_id = slab_id << slab_mult_shift;
        let end_id = (slab_id + 1) << slab_mult_shift;

        let mut idx = start_id;
        let mut key_addr = old_chunk.base + (start_id << ENTRY_SIZE_SHIFT);
        let mut val_addr = key_addr + KEY_SIZE;
        let bound_addr = old_chunk.base + (end_id << ENTRY_SIZE_SHIFT);

        if cfg!(debug_assertions) && slab_mult_shift > 0 {
            if slab_id == MIGRATION_TOTAL_BITS - 1 {
                assert_eq!(end_id, chunk_cap);
            }
        }

        let mut local_copied: SmallVec<[usize; COPIED_ARR_SIZE]> =
            smallvec![0usize; old_chunk.copied.len()];

        let backoff = crossbeam_utils::Backoff::new();
        while key_addr < bound_addr {
            // iterate the old chunk to extract entries that is NOT empty
            let fkey = Self::get_fast_key(key_addr);
            let fvalue = Self::get_fast_value(val_addr);
            debug_assert_eq!(key_addr, old_chunk.entry_addr(idx).0);
            // Reasoning value states
            trace!("Migrating entry have key {}", Self::get_fast_key(key_addr));
            match fvalue.val {
                EMPTY_VALUE => {
                    // Probably does not need this anymore
                    // Need to make sure that during migration, empty value always leads to new chunk
                    if Self::cas_sentinel(val_addr, fvalue.val) {
                        Self::store_key(key_addr, DISABLED_KEY);
                    } else {
                        backoff.spin();
                        continue;
                    }
                }
                TOMBSTONE_VALUE => {
                    if !Self::cas_sentinel(val_addr, fvalue.val) {
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
                }
                _ => {
                    if fkey == EMPTY_KEY {
                        backoff.spin();
                        continue;
                    }
                    let hash = if Self::WORD_KEY {
                        hash_num::<H>(fkey)
                    } else {
                        fkey
                    };
                    let part_id = parts.id_of_hash(hash);
                    let part = parts.at(part_id);
                    let new_chunk = part.current();
                    debug_assert!(
                        !new_chunk.is_null(),
                        "Cannot locate new chunk for hash {:0>64b}. Part number {}, 
                        part arr_ver: {}, part len: {}, parts mask: {:b}, masked hash: {:b},
                        arr_ver: {}, part range {:?}, old_chunk {}",
                        hash,
                        part_id.0,
                        part.arr_ver.load(Acquire),
                        parts.len,
                        parts.hash_masking,
                        parts.id_of_hash(hash).0,
                        parts.version,
                        (part_start, part_end),
                        old_chunk.base
                    );
                    debug_assert!(
                        part_id.0 >= *part_start,
                        "Got hashed {} < lower bound {}, hash {:0>64b}",
                        part_id.0,
                        part_start,
                        hash
                    );
                    debug_assert!(
                        part_id.0 < *part_end,
                        "Got hashed {} >= upper bound {}, hash {:0>64b}",
                        part_id.0,
                        part_end,
                        hash
                    );
                    let effective_copy = &mut local_copied[part_id.0 - part_start];
                    if !Self::migrate_entry(
                        fkey,
                        idx,
                        fvalue,
                        hash,
                        old_chunk,
                        new_chunk,
                        val_addr,
                        effective_copy,
                    ) {
                        backoff.spin();
                        continue;
                    }
                }
            }
            backoff.reset();
            key_addr += ENTRY_SIZE;
            val_addr = key_addr + KEY_SIZE;
            idx += 1;
        }
        for (i, n) in local_copied.into_iter().enumerate() {
            old_chunk.copied[i].fetch_add(n, Relaxed);
        }
        slab_bits_fin_atom.fetch_or(slab_bit_set, Relaxed);
        return true;
    }

    fn migrate_entry(
        fkey: FKey,
        old_idx: usize,
        fvalue: FastValue,
        hash: usize,
        old_chunk_ins: ChunkPtr<K, V, A, ALLOC>,
        new_chunk_ins: ChunkPtr<K, V, A, ALLOC>,
        old_val_addr: usize,
        effective_copy: &mut usize,
    ) -> bool {
        if fkey == EMPTY_KEY {
            return false;
        }
        // Will not migrate meta keys
        if fkey <= MAX_META_KEY || fvalue.is_primed() {
            return true;
        }
        // Insert entry into new chunk, in case of failure, skip this entry
        // Value should be locked
        let act_val = fvalue.act_val::<V>();
        let primed_orig = fvalue.prime();

        // Prime the old address to avoid modification
        if !Self::cas_value(old_val_addr, fvalue.val, primed_orig).1 {
            // here the ownership have been taken by other thread
            trace!("Entry {} has changed", fkey);
            return false;
        }

        trace!("Primed {}", fkey);

        // Since the entry is primed, it is safe to read the key
        let old_attachment = old_chunk_ins.attachment.prefetch(old_idx);
        let key = old_attachment.get_key();
        let cap_mask = new_chunk_ins.cap_mask;
        let home_idx = hash & cap_mask;
        let mut iter = SlotIter::new(home_idx, cap_mask);
        let (mut idx, _) = iter.next().unwrap();
        let backoff = Backoff::new();
        loop {
            let (key_addr, val_addr) = new_chunk_ins.entry_addr(idx);
            let k = Self::get_fast_key(key_addr);
            if k == fkey {
                let new_attachment = new_chunk_ins.attachment.prefetch(idx);
                let probe = new_attachment.probe(&key);
                if probe {
                    // New value in the new chunk, just put a sentinel and abort migration on this slot
                    Self::store_sentinel(old_val_addr);
                    return true;
                }
            } else if k == EMPTY_KEY {
                let cas_fval = act_val;
                if Self::cas_value(val_addr, EMPTY_VALUE, cas_fval).1 {
                    let new_attachment = new_chunk_ins.attachment.prefetch(idx);
                    let old_attachment = old_chunk_ins.attachment.prefetch(old_idx);
                    let value = old_attachment.get_value();
                    new_attachment.set_key(key);
                    new_attachment.set_value(value, 0);
                    Self::store_key(key_addr, fkey);
                    Self::store_sentinel(old_val_addr);
                    *effective_copy += 1;
                    return true;
                } else {
                    // Here we didn't put the fval into the new chunk due to slot conflict with
                    // other thread. Need to retry
                    trace!("Migrate {} have conflict", fkey);
                    backoff.spin();
                    continue;
                }
            }
            if let Some(next) = iter.next() {
                (idx, _) = next;
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
            (fkey, hash_num::<H>(fkey))
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
        self.partitions().key_capacity()
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

    pub(crate) fn now_epoch(&self, key: &K, fkey: usize) -> usize {
        let (_fkey, hash) = Self::hash(fkey, key);
        let ((_part, _, current_epoch), _) = self.part_of_hash(hash);
        current_epoch
    }

    pub(crate) fn part_id(&self, key: &K, fkey: usize) -> (usize, usize) {
        let (_fkey, hash) = Self::hash(fkey, key);
        let ((_, id, _), ver) = self.part_of_hash(hash);
        (id.0, ver)
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

    fn size_of(capacity: usize) -> ChunkSizes {
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
        let total_size = self_size_aligned + chunk_size_aligned + attachment_heap_size;
        ChunkSizes {
            total_size,
            self_size_aligned,
            chunk_size_aligned,
            capacity,
            page_size,
        }
    }

    fn alloc_chunk(capacity: usize, attachment_meta: &A::InitMeta) -> *mut Self {
        let sizes = Self::size_of(capacity);
        let ptr = alloc_mem::<ALLOC>(sizes.total_size) as *mut Self;
        Self::initialize_chunk(ptr, capacity, &sizes, ptr::null_mut(), attachment_meta)
    }

    fn initialize_chunk(
        ptr: *mut Chunk<K, V, A, ALLOC>,
        capacity: usize,
        sizes: &ChunkSizes,
        origin: *const PartitionArray<K, V, A, ALLOC>,
        attachment_meta: &A::InitMeta,
    ) -> *mut Self {
        let addr = ptr as usize;
        let data_base = addr + sizes.self_size_aligned;
        let attachment_base = data_base + sizes.chunk_size_aligned;
        unsafe {
            Self::fill_zeros(data_base, sizes.chunk_size_aligned, sizes.page_size);
            ptr::write(
                ptr,
                Self {
                    base: data_base,
                    capacity,
                    cap_mask: capacity - 1,
                    occupation: AtomicU32::new(0),
                    empty_entries: AtomicUsize::new(0),
                    occu_limit: occupation_limit(capacity),
                    total_size: sizes.total_size,
                    origin,
                    migrating: mem::zeroed(),
                    migrated: mem::zeroed(),
                    copied: smallvec![],
                    attachment: A::new(attachment_base, attachment_meta),
                    shadow: PhantomData,
                },
            )
        };
        ptr
    }

    unsafe fn fill_zeros(data_base: usize, data_size: usize, _page_size: usize) {
        fill_zeros(data_base, data_size);
    }

    unsafe fn gc(ptr: *mut Chunk<K, V, A, ALLOC>) {
        if ptr.is_null() {
            return;
        }
        let chunk = &*ptr;
        chunk.gc_entries();
    }

    fn gc_entries(&self) {
        let mut old_address = self.base as usize;
        let boundary = old_address + chunk_size_of(self.capacity);
        let mut idx = 0;
        while old_address < boundary {
            let old_key_addr = old_address;
            let old_val_addr = old_key_addr + KEY_SIZE;

            let fvalue = Self::get_fast_value(old_val_addr);
            let fkey = Self::get_fast_key(old_key_addr);
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
    fn get_fast_key(key_addr: usize) -> FKey {
        unsafe { intrinsics::atomic_load_relaxed(key_addr as *mut FKey) }
    }

    #[inline(always)]
    fn get_fast_value(val_addr: usize) -> FastValue {
        let val = unsafe { intrinsics::atomic_load_relaxed(val_addr as *mut FVal) };
        FastValue::new(val)
    }

    #[inline(always)]
    fn entry_addr(&self, idx: usize) -> (usize, usize) {
        let key_addr = self.base + (idx << ENTRY_SIZE_SHIFT);
        let val_addr = key_addr + KEY_SIZE;
        (key_addr, val_addr)
    }

    fn dump_dist(&self) {
        let cap = self.capacity;
        let mut res = String::new();
        for i in 0..cap {
            let (key_addr, _) = self.entry_addr(i);
            let k = Self::get_fast_key(key_addr);
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
            let (key_addr, val_addr) = self.entry_addr(i);
            let k = Self::get_fast_key(key_addr);
            let v = Self::get_fast_value(val_addr);
            res.push(format!("{}:{}", k, v.val));
        }
        error!("Chunk dump: {:?}", res);
    }

    pub fn occupation(&self) -> (u32, u32, usize) {
        (
            self.occu_limit,
            self.occupation.load(Relaxed),
            self.capacity,
        )
    }

    unsafe fn init_copied(&self, size: usize) {
        let ptr: *const _ = &self.copied;
        let vec = smallvec![0usize; size];
        ptr::replace(ptr as *mut SmallVec<[usize; COPIED_ARR_SIZE]>, vec);
        fence(Release);
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
    > Clone for Table<K, V, A, ALLOC, H, K_OFFSET, V_OFFSET>
{
    fn clone(&self) -> Self {
        let parts = self.partitions();
        let original_chunk_size = parts.chunk_size;
        let new_parts =
            PartitionArray::<K, V, A, ALLOC>::new(parts.len, parts.version, original_chunk_size);
        parts.iter().enumerate().for_each(|(i, part)| {
            let new_part = unsafe { &(*new_parts).at(ArrId(i)) };
            let current = part.current();
            let history = part.history();
            let epoch = part.epoch();
            if !current.is_null() {
                unsafe {
                    let total_size = current.total_size;
                    let new_chunk_ptr = libc::malloc(total_size);
                    libc::memcpy(new_chunk_ptr, current.ptr as _, total_size);
                    new_part.set_current(ChunkPtr::new(new_chunk_ptr as _));
                }
            }
            if !history.is_null() {
                unsafe {
                    let total_size = history.total_size;
                    let new_chunk_ptr = libc::malloc(total_size);
                    libc::memcpy(new_chunk_ptr, history.ptr as _, total_size);
                    new_part.set_history(ChunkPtr::new(new_chunk_ptr as _));
                }
            }
            new_part.set_epoch(epoch);
        });
        Self {
            meta: ChunkMeta {
                partitions: AtomicUsize::new(new_parts as _),
                array_version: AtomicUsize::new(parts.version),
            },
            count: Counter::new(),
            init_cap: self.init_cap,
            max_cap: self.max_cap,
            init_arr_len: self.init_arr_len,
            attachment_init_meta: self.attachment_init_meta.clone(),
            mark: PhantomData,
        }
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
    > Drop for Table<K, V, A, ALLOC, H, K_OFFSET, V_OFFSET>
{
    fn drop(&mut self) {
        let guard = crossbeam_epoch::pin();
        let parts_addr = self.meta.partitions.load(Acquire);
        let parts = PartitionArray::<K, V, A, ALLOC>::ref_from_addr(parts_addr);
        parts.clear(&guard);
        unsafe {
            guard.defer_unchecked(move || {
                libc::free(parts_addr as _);
            })
        }
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
    fn dispose(&self) {
        unsafe {
            Chunk::gc(self.ptr);
            if self.origin.is_null() {
                dealloc_mem::<ALLOC>(self.ptr as usize, self.total_size);
            } else {
                PartitionArray::<K, V, A, ALLOC>::gc(self.origin as _);
            }
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

impl<'a, K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> From<usize>
    for ChunkPtr<'a, K, V, A, ALLOC>
{
    fn from(value: usize) -> Self {
        Self {
            ptr: value as _,
            _marker: PhantomData,
        }
    }
}

impl<'a, K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> Into<usize>
    for ChunkPtr<'a, K, V, A, ALLOC>
{
    fn into(self) -> usize {
        self.ptr as _
    }
}

impl<'a, K, V, A: Attachment<K, V>, ALLOC: GlobalAlloc + Default> ChunkPtr<'a, K, V, A, ALLOC> {
    fn new(ptr: *mut Chunk<K, V, A, ALLOC>) -> Self {
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
    cap << ENTRY_SIZE_SHIFT
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
    cap_mask: usize,
    num_probed: usize,
}

impl Iterator for SlotIter {
    type Item = (usize, usize);

    #[inline(always)]
    fn next(&mut self) -> Option<Self::Item> {
        let probed = self.num_probed;
        if probed > self.cap_mask {
            return None;
        };
        let pos = self.pos;
        self.num_probed += 1;
        self.pos += 1;
        let rt = (self.home_idx + pos) & self.cap_mask;
        Some((rt, pos))
    }
}

impl SlotIter {
    fn new(home_idx: usize, cap_mask: usize) -> Self {
        Self {
            home_idx,
            cap_mask,
            pos: 0,
            num_probed: 0,
        }
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
        assert!(val.is_valued());
        let val = val.prime();
        let val = FastValue { val };
        assert!(val.is_primed());
        assert!(!val.is_locked());
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
        assert!(val.is_valued());
        let val = val.prime();
        let val = FastValue { val };
        assert!(val.is_primed());
        assert!(!val.is_locked());
        assert!(val.is_valued());
        assert_eq!(val.act_val::<()>(), ptr);
    }
}
