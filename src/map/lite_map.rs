use super::base::*;
use super::word_map::*;
use super::*;

#[repr(C, align(8))]
struct AlignedLiteObj<T> {
    data: T,
    _marker: PhantomData<T>,
}

pub type LiteTable<V, H, ALLOC> =
    Table<(), (), LiteAttachment<V>, H, ALLOC, RAW_KV_OFFSET, RAW_KV_OFFSET>;

pub struct LiteHashMap<
    K: Clone + Hash + Eq,
    V: Clone,
    ALLOC: GlobalAlloc + Default = System,
    H: Hasher + Default = DefaultHasher,
> {
    table: LiteTable<V, ALLOC, H>,
    shadow: PhantomData<(K, V, H)>,
}

impl<K: Clone + Hash + Eq, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default>
    LiteHashMap<K, V, ALLOC, H>
{
    const K_SIZE: usize = mem::size_of::<AlignedLiteObj<K>>();
    const V_SIZE: usize = mem::size_of::<AlignedLiteObj<V>>();

    #[inline(always)]
    pub fn insert_with_op(&self, op: InsertOp, key: K, value: V) -> Option<V> {
        let k_num = self.encode(key);
        let v_num = self.encode(value);
        self.table
            .insert(op, &(), Some(&()), k_num as FKey, v_num as FVal)
            .map(|(fv, _)| self.decode::<V>(fv as usize))
    }

    pub fn lock(&self, key: &K) -> Option<LiteMutexGuard<K, V, ALLOC, H>> {
        LiteMutexGuard::new(&self, key)
    }

    pub fn insert_locked(&self, key: &K, value: &V) -> Option<LiteMutexGuard<K, V, ALLOC, H>> {
        LiteMutexGuard::create(&self, key, value)
    }

    #[inline(always)]
    fn encode<T>(&self, d: T) -> usize {
        let mut num: u64 = 0;
        let obj_ptr = &mut num as *mut u64 as *mut T;
        unsafe {
            ptr::write(obj_ptr, d);
        }
        return num as usize as usize;
    }

    #[inline(always)]
    fn decode<T: Clone>(&self, num: usize) -> T {
        let num = num as u64;
        let ptr = &num as *const u64 as *const AlignedLiteObj<T>;
        let aligned = unsafe { &*ptr };
        let obj = aligned.data.clone();
        return obj;
    }

    #[inline(always)]
    unsafe fn decode_no_clone<T: Clone>(&self, num: usize) -> T {
        let num = num as u64;
        let ptr = &num as *const u64 as *const AlignedLiteObj<T>;
        let aligned = ptr::read(ptr);
        return aligned.data;
    }
}

impl<K: Clone + Hash + Eq, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Map<K, V>
    for LiteHashMap<K, V, ALLOC, H>
{
    fn with_capacity(cap: usize) -> Self {
        assert_eq!(Self::K_SIZE, 8);
        assert_eq!(Self::V_SIZE, 8);
        Self {
            table: Table::with_capacity(cap, ()),
            shadow: PhantomData,
        }
    }

    #[inline(always)]
    fn get(&self, key: &K) -> Option<V> {
        let k_num = self.encode(key.clone()) as FKey;
        self.table
            .get(&(), k_num, false)
            .map(|(fv, _)| self.decode::<V>(fv as usize))
    }

    #[inline(always)]
    fn insert(&self, key: K, value: V) -> Option<V> {
        self.insert_with_op(InsertOp::Insert, key, value)
    }

    #[inline(always)]
    fn try_insert(&self, key: K, value: V) -> Option<V> {
        self.insert_with_op(InsertOp::TryInsert, key, value)
    }

    #[inline(always)]
    fn remove(&self, key: &K) -> Option<V> {
        let k_num = self.encode(key.clone()) as FKey;
        self.table
            .remove(&(), k_num)
            .map(|(fv, _)| self.decode(fv as usize))
    }

    #[inline(always)]
    fn entries(&self) -> Vec<(K, V)> {
        self.table
            .entries()
            .into_iter()
            .map(|(fk, fv, _, _)| (self.decode(fk as usize), self.decode::<V>(fv as usize)))
            .collect()
    }

    #[inline(always)]
    fn contains_key(&self, key: &K) -> bool {
        let k_num = self.encode(key.clone()) as FKey;
        self.table.get(&(), k_num, false).is_some()
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.table.len()
    }

    #[inline(always)]
    fn clear(&self) {
        self.table.clear();
    }
}

pub struct LiteMutexGuard<
    'a,
    K: Clone + Hash + Eq,
    V: Clone,
    ALLOC: GlobalAlloc + Default,
    H: Hasher + Default,
> {
    map: &'a LiteHashMap<K, V, ALLOC, H>,
    fkey: usize,
    value: V,
}

impl<'a, K: Clone + Hash + Eq, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default>
    LiteMutexGuard<'a, K, V, ALLOC, H>
{
    fn new(map: &'a LiteHashMap<K, V, ALLOC, H>, key: &K) -> Option<Self> {
        let backoff = crossbeam_utils::Backoff::new();
        let guard = crossbeam_epoch::pin();
        let k_num = map.encode(key);
        let value;
        loop {
            let swap_res = map.table.swap(
                k_num,
                &(),
                move |fast_value| {
                    let locked_val = fast_value | VAL_MUTEX_BIT;
                    if fast_value == locked_val {
                        // Locked, unchanged
                        None
                    } else {
                        // Obtain lock
                        Some(locked_val)
                    }
                },
                &guard,
            );
            match swap_res {
                SwapResult::Succeed(val, _idx, _chunk) => {
                    value = map.decode(val & WORD_MUTEX_DATA_BIT_MASK);
                    break;
                }
                SwapResult::Failed | SwapResult::Aborted => {
                    backoff.spin();
                    continue;
                }
                SwapResult::NotFound => {
                    return None;
                }
            }
        }
        Some(Self {
            map,
            value,
            fkey: k_num,
        })
    }

    fn create(map: &'a LiteHashMap<K, V, ALLOC, H>, key: &K, value: &V) -> Option<Self> {
        let k_num = map.encode(key);
        let fvalue = map.encode(value);
        match map.table.insert(
            InsertOp::TryInsert,
            &(),
            Some(&()),
            k_num,
            fvalue | VAL_MUTEX_BIT,
        ) {
            None | Some((TOMBSTONE_VALUE, ())) | Some((EMPTY_VALUE, ())) => Some(Self {
                map,
                value: value.clone(),
                fkey: k_num,
            }),
            _ => None,
        }
    }

    pub fn remove(self) -> V {
        let fval = self.map.table.remove(&(), self.fkey).unwrap().0 & WORD_MUTEX_DATA_BIT_MASK;
        return self.map.decode(fval);
    }
}
impl<'a, K: Clone + Hash + Eq, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Deref
    for LiteMutexGuard<'a, K, V, ALLOC, H>
{
    type Target = V;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}

impl<'a, K: Clone + Hash + Eq, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> DerefMut
    for LiteMutexGuard<'a, K, V, ALLOC, H>
{
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}
impl<'a, K: Clone + Hash + Eq, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Drop
    for LiteMutexGuard<'a, K, V, ALLOC, H>
{
    fn drop(&mut self) {
        let fval = self.map.encode(&self.value) & WORD_MUTEX_DATA_BIT_MASK;
        self.map
            .table
            .insert(InsertOp::Insert, &(), Some(&()), self.fkey, fval);
    }
}

impl<K: Clone + Hash + Eq, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Drop
    for LiteHashMap<K, V, ALLOC, H>
{
    fn drop(&mut self) {
        for (k, v, _a, _b) in self.table.entries() {
            unsafe {
                self.decode_no_clone::<K>(k);
                self.decode_no_clone::<V>(v);
            }
        }
    }
}

pub struct LiteAttachment<V> {
    _marker: PhantomData<V>,
}

impl<V> Attachment<(), ()> for LiteAttachment<V> {
    type InitMeta = ();

    type Item = WordAttachmentItem;

    #[inline(always)]
    fn heap_entry_size() -> usize {
        0
    }

    #[inline(always)]
    fn new(_heap_ptr: usize, _meta: &()) -> Self {
        Self {
            _marker: PhantomData,
        }
    }

    #[inline(always)]
    fn prefetch(&self, _index: usize) -> Self::Item {
        WordAttachmentItem
    }

    #[inline(always)]
    fn manually_drop(&self, fval: usize) {
        let num = fval as u64;
        let ptr = &num as *const u64 as *const AlignedLiteObj<V>;
        unsafe { mem::drop(ptr::read(ptr)) }
    }
}

#[cfg(test)]
mod lite_tests {
    use crate::map::{
        base::{MAX_META_KEY, MAX_META_VAL},
        *,
    };
    use std::{alloc::System, sync::Arc};

    const START_IDX: usize = 0;

    #[test]
    fn no_resize() {
        let _ = env_logger::try_init();
        let map = LiteHashMap::<usize, usize, System>::with_capacity(4096);
        for i in START_IDX..2048 {
            let k = i;
            let v = i * 2;
            map.insert(k, v);
        }
        for i in START_IDX..2048 {
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
        for i in START_IDX..2048 {
            let k = i;
            let v = i * 2;
            let d = Arc::new(FatStruct::new(v));
            map.insert(k, d);
        }
        for i in START_IDX..2048 {
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
            if k <= MAX_META_KEY as u8 || v <= MAX_META_VAL as u8 {
                continue;
            }
            map.insert(k, v);
        }
        for i in 0..2048 {
            let k = i as u8;
            let v = (i * 2) as u8;
            if k <= MAX_META_KEY as u8 || v <= MAX_META_VAL as u8 {
                continue;
            }
            match map.get(&k) {
                Some(r) => {
                    assert_eq!(r, v);
                }
                None => panic!("{}", i),
            }
        }
    }
}
