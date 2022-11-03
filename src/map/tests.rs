use chashmap::CHashMap;
use std::collections::HashMap;
use std::{
    alloc::System,
    sync::{Mutex, RwLock},
};
use test::Bencher;

use crate::map::*;

#[derive(Copy, Clone)]
pub struct Obj {
    a: usize,
    b: usize,
    c: usize,
    d: usize,
}

impl Obj {
    pub fn new(num: usize) -> Self {
        Obj {
            a: num,
            b: num + 1,
            c: num + 2,
            d: num + 3,
        }
    }
    pub fn validate(&self, num: usize) {
        assert_eq!(self.a, num);
        assert_eq!(self.b, num + 1);
        assert_eq!(self.c, num + 2);
        assert_eq!(self.d, num + 3);
    }
    pub fn get(&self) -> usize {
        self.a
    }
    pub fn set(&mut self, num: usize) {
        *self = Self::new(num)
    }
}

#[test]
#[should_panic]
fn insert_with_num_fixes() {
    let map = WordMap::<System, DefaultHasher>::with_capacity(32);
    assert_eq!(map.insert(24, 0), None);
    assert_eq!(map.insert(24, 1), Some(0));
    assert_eq!(map.insert(0, 0), None);
    assert_eq!(map.insert(0, 1), Some(0))
}

#[bench]
fn lfmap(b: &mut Bencher) {
    let _ = env_logger::try_init();
    let map = WordMap::<System, DefaultHasher>::with_capacity(8);
    let mut i = 0;
    b.iter(|| {
        map.insert(i, i);
        i += 1;
    });
}

#[bench]
fn lite_lfmap(b: &mut Bencher) {
    let _ = env_logger::try_init();
    let map = LiteHashMap::<usize, usize, System, DefaultHasher>::with_capacity(8);
    let mut i = 0;
    b.iter(|| {
        map.insert(i, i);
        i += 1;
    });
}

#[bench]
fn fat_lfmap(b: &mut Bencher) {
    let _ = env_logger::try_init();
    let map = super::LockingHashMap::<usize, usize, System, DefaultHasher>::with_capacity(8);
    let mut i = 5;
    b.iter(|| {
        map.insert(i, i);
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
