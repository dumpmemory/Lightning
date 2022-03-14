use super::base::*;
use super::word_map::*;
use super::*;

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
    table: WordTable<ALLOC, H>,
    shadow: PhantomData<(K, V, H)>,
}

impl<K: Clone + Hash + Eq, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default>
    LiteHashMap<K, V, ALLOC, H>
{
    const K_SIZE: usize = mem::size_of::<AlignedLiteObj<K>>();
    const V_SIZE: usize = mem::size_of::<AlignedLiteObj<V>>();

    #[inline(always)]
    pub fn insert_with_op(&self, op: InsertOp, key: &K, value: &V) -> Option<V> {
        let k_num = Self::encode(key);
        let v_num = Self::encode(value);
        self.table
            .insert(op, &(), None, k_num as FKey, v_num as FVal)
            .map(|(fv, _)| Self::decode::<V>(fv as usize))
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
        return num as usize + NUM_FIX_V as usize;
    }

    #[inline(always)]
    fn decode<T: Clone>(num: usize) -> T {
        let num = (num - (NUM_FIX_V as usize)) as u64;
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
            table: Table::with_capacity(cap, ()),
            shadow: PhantomData,
        }
    }

    #[inline(always)]
    fn get(&self, key: &K) -> Option<V> {
        let k_num = Self::encode(key) as FKey;
        self.table
            .get(&(), k_num, false)
            .map(|(fv, _)| Self::decode::<V>(fv as usize))
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
        let k_num = Self::encode(key) as FKey;
        self.table
            .remove(&(), k_num)
            .map(|(fv, _)| Self::decode(fv as usize))
    }

    #[inline(always)]
    fn entries(&self) -> Vec<(K, V)> {
        self.table
            .entries()
            .into_iter()
            .map(|(fk, fv, _, _)| (Self::decode(fk as usize), Self::decode::<V>(fv as usize)))
            .collect()
    }

    #[inline(always)]
    fn contains_key(&self, key: &K) -> bool {
        let k_num = Self::encode(key) as FKey;
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

#[cfg(test)]
mod lite_tests {
    use crate::map::*;
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
