use super::base::*;
use super::*;

pub type WordTable<H, ALLOC> =
    Table<(), (), WordAttachment, H, ALLOC, RAW_KV_OFFSET, RAW_KV_OFFSET>;

impl<ALLOC: GlobalAlloc + Default, H: Hasher + Default> WordMap<ALLOC, H> {
    pub fn lock(&self, key: FKey) -> Option<WordMutexGuard<ALLOC, H>> {
        WordMutexGuard::new(&self.table, key)
    }
    pub fn try_insert_locked(&self, key: FKey) -> Option<WordMutexGuard<ALLOC, H>> {
        WordMutexGuard::create(&self.table, key)
    }
}

#[derive(Clone)]
pub struct WordMap<ALLOC: GlobalAlloc + Default = System, H: Hasher + Default = DefaultHasher> {
    table: WordTable<ALLOC, H>,
}

impl<ALLOC: GlobalAlloc + Default, H: Hasher + Default> WordMap<ALLOC, H> {
    #[inline(always)]
    fn insert_with_op(&self, op: InsertOp, key: FKey, value: FVal) -> Option<FVal> {
        self.table.insert(op, &(), None, key, value).map(|(v, _)| v)
    }

    pub fn get_from_mutex(&self, key: &FKey) -> Option<FVal> {
        self.get(key).map(|v| v & WORD_MUTEX_DATA_BIT_MASK)
    }
}

impl<ALLOC: GlobalAlloc + Default, H: Hasher + Default> Map<FKey, FVal> for WordMap<ALLOC, H> {
    fn with_capacity(cap: usize) -> Self {
        Self {
            table: Table::with_capacity(cap, ()),
        }
    }

    #[inline(always)]
    fn get(&self, key: &FKey) -> Option<FVal> {
        self.table.get(&(), *key, false).map(|v| v.0)
    }

    #[inline(always)]
    fn insert(&self, key: FKey, value: FVal) -> Option<FVal> {
        self.insert_with_op(InsertOp::Insert, key, value)
    }

    #[inline(always)]
    fn try_insert(&self, key: FKey, value: FVal) -> Option<FVal> {
        self.insert_with_op(InsertOp::TryInsert, key, value)
    }

    #[inline(always)]
    fn remove(&self, key: &FKey) -> Option<FVal> {
        self.table.remove(&(), *key).map(|(v, _)| v)
    }
    fn entries(&self) -> Vec<(FKey, FVal)> {
        self.table
            .entries()
            .into_iter()
            .map(|(k, v, _, _)| (k, v))
            .collect()
    }

    #[inline(always)]
    fn contains_key(&self, key: &FKey) -> bool {
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
    ALLOC: GlobalAlloc + Default = System,
    H: Hasher + Default = DefaultHasher,
> {
    table: &'a WordTable<ALLOC, H>,
    key: FKey,
    value: FVal,
}

impl<'a, ALLOC: GlobalAlloc + Default, H: Hasher + Default> WordMutexGuard<'a, ALLOC, H> {
    fn create(table: &'a WordTable<ALLOC, H>, key: FKey) -> Option<Self> {
        let value = 0;
        let locked_val = value | VAL_MUTEX_BIT;
        debug_assert_ne!(value, locked_val);
        match table.insert(InsertOp::TryInsert, &(), Some(&()), key, locked_val) {
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
    fn new(table: &'a WordTable<ALLOC, H>, key: FKey) -> Option<Self> {
        let backoff = crossbeam_utils::Backoff::new();
        let guard = crossbeam_epoch::pin();
        let value;
        loop {
            let swap_res = table.swap(
                key,
                &(),
                move |fast_value| {
                    trace!("The key {} have value {}", key, fast_value);
                    let locked_val = fast_value | VAL_MUTEX_BIT;
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
        Some(Self { table, key, value })
    }

    pub fn remove(self) -> FVal {
        trace!("Removing {}", self.key);
        let res = self.table.remove(&(), self.key).unwrap().0;
        mem::forget(self);
        res & WORD_MUTEX_DATA_BIT_MASK
    }
}

impl<'a, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Deref for WordMutexGuard<'a, ALLOC, H> {
    type Target = FVal;

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
        let key = self.key;
        let val = self.value;
        debug_assert_ne!(val, val | VAL_MUTEX_BIT);
        trace!(
            "Release lock for key {} with value {}",
            self.key,
            self.value
        );
        self.table.insert(InsertOp::Insert, &(), None, key, val);
    }
}

pub struct WordAttachment;
#[derive(Copy, Clone)]
pub struct WordAttachmentItem;

// this attachment basically do nothing and sized zero
impl Attachment<(), ()> for WordAttachment {
    type Item = WordAttachmentItem;
    type InitMeta = ();

    #[inline(always)]
    fn heap_entry_size() -> usize {
        0
    }

    #[inline(always)]
    fn new(_heap_ptr: usize, _meta: &()) -> Self {
        Self
    }

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
    fn set_value(self, _value: (), _old_fval: FVal) {}

    #[inline(always)]
    fn erase_value(self, _old_fval: FVal) {}

    #[inline(always)]
    fn probe(self, _value: &()) -> bool {
        true
    }

    #[inline(always)]
    fn prep_write(self) {}

    fn moveout_key(self) {}
}

#[cfg(test)]
mod test {
    use crate::{
        map::{
            base::{dump_migration_log, get_delayed_log},
            *,
        },
        tests_misc::assert_all_thread_passed,
    };
    use alloc::sync::Arc;
    use rayon::prelude::*;
    use std::{thread, time::Duration};
    use test::Bencher;

    const START_IDX: usize = 0;

    #[test]
    fn will_not_overflow() {
        let _ = env_logger::try_init();
        let table = WordMap::<System>::with_capacity(16);
        for i in 50..60 {
            assert_eq!(table.insert(i, i), None);
        }
        for i in 50..60 {
            let res = table.get(&i);
            assert_eq!(res, Some(i), "at epoch {}", table.table.now_epoch());
        }
        for i in 50..60 {
            assert_eq!(table.remove(&i), Some(i));
        }
    }

    #[test]
    fn resize() {
        let _ = env_logger::try_init();
        let map = WordMap::<System>::with_capacity(16);
        for i in START_IDX..2048 {
            map.insert(i, i * 2);
        }
        for i in START_IDX..2048 {
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
        for i in START_IDX..99 {
            map.insert(i, i * 10);
        }
        for i in 100..900 {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                for j in START_IDX..60 {
                    map.insert(i * 100 + j, i * j);
                }
            }));
        }
        for i in START_IDX..9 {
            for j in START_IDX..10 {
                map.remove(&(i * j));
            }
        }
        for thread in threads {
            thread.join().unwrap();
        }
        for i in 100..900 {
            for j in START_IDX..60 {
                let get_res = map.get(&(i * 100 + j));
                assert_eq!(get_res, Some(i * j), "at epoch {}", map.table.now_epoch())
            }
        }
        for i in START_IDX..9 {
            for j in START_IDX..10 {
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
              for j in START_IDX..test_load {
                  let key = i * 10000000 + j;
                  let value_prefix = i * j * 100;
                  for k in START_IDX..repeat_load {
                      let value = value_prefix + k;
                      if k != START_IDX {
                          assert_eq!(map.get(&key), Some(value - 1));
                      }
                      let pre_insert_epoch = map.table.now_epoch();
                      map.insert(key, value);
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
                              panic!("Unable to recover for {}, round {}, copying {}, expecting {:?}, got {:?}", key, l , map.table.map_is_copying(), right, left);
                          }
                      }
                      if j % 5 == 0 {
                        let pre_rm_epoch = map.table.now_epoch();
                        assert_eq!(
                            map.remove(&key).as_ref(),
                            Some(&value),
                            "Remove result, get {:?}, copying {}, round {}",
                            map.get(&key),
                            map.table.map_is_copying(),
                            k
                        );
                        let post_rm_epoch = map.table.now_epoch();
                        assert_eq!(map.get(&key), None, "Remove recursion, value was {:?}. Epoch pre {}, post {}, get {}, last logs {:?}", value, pre_rm_epoch, post_rm_epoch, map.table.now_epoch(), get_delayed_log(3));
                        assert!(map.lock(key).is_none(), "Remove recursion with lock, epoch {}", map.table.now_epoch());
                        assert!(map.insert(key, value).is_none());
                        assert_eq!(map.get(&key), Some(value));
                      }
                      if j % 3 == 0 {
                          let new_value = value + 7;
                          let pre_insert_epoch = map.table.now_epoch();
                          map.insert(key, new_value);
                          let post_insert_epoch = map.table.now_epoch();
                          assert_eq!(
                              map.get(&key), 
                              Some(new_value), 
                              "Checking immediate update, key {}, epoch {} to {}",
                              key, pre_insert_epoch, post_insert_epoch
                          );
                          map.insert(key, value);
                      }
                  }
              }
          }));
        }
        info!("Waiting for intensive insertion to finish");
        for thread in threads {
            thread.join().unwrap();
        }
        info!("Checking final value");
        (0..num_threads)
            .collect::<Vec<_>>()
            .par_iter()
            .for_each(|i| {
                for j in START_IDX..test_load {
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
        for i in START_IDX..128 {
            map.insert(i, i * 10);
        }
        let mut threads = vec![];
        for i in 256..265 {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                for j in START_IDX..60 {
                    map.insert(i * 10 + j, 10);
                }
            }));
        }
        for i in START_IDX..8 {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                for j in START_IDX..8 {
                    map.remove(&(i * j));
                }
            }));
        }
        for thread in threads {
            thread.join().unwrap();
        }
        for i in 256..265 {
            for j in START_IDX..60 {
                assert_eq!(map.get(&(i * 10 + j)), Some(10))
            }
        }
    }

    #[test]
    fn parallel_word_map_mutex() {
        let _ = env_logger::try_init();
        let map = Arc::new(WordMap::<System>::with_capacity(4));
        map.insert(START_IDX, START_IDX);
        let mut threads = vec![];
        let num_threads = 256;
        for _ in 0..num_threads {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                if let Some(mut guard) = map.lock(START_IDX) {
                    *guard += 1;
                } else {
                    panic!(
                        "Cannot find key at epoch {}, get {:?}",
                        map.table.now_epoch(),
                        map.get(&START_IDX)
                    )
                }
            }));
        }
        for thread in threads {
            thread.join().unwrap();
        }
        assert_eq!(map.get(&START_IDX).unwrap(), START_IDX + num_threads);
    }

    #[test]
    fn parallel_word_map_multi_mutex() {
        let _ = env_logger::try_init();
        let map = Arc::new(WordMap::<System>::with_capacity(16));
        let mut threads = vec![];
        let num_threads = num_cpus::get();
        let test_load = 4096;
        let update_load = 128;
        for thread_id in 0..num_threads {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                let target = thread_id;
                for i in START_IDX..test_load {
                    let key = target * 1000000 + i;
                    {
                        let mut mutex = map.try_insert_locked(key).unwrap();
                        *mutex = START_IDX;
                    }
                    for j in START_IDX..update_load {
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
            thread.join().unwrap();
        }
    }

    #[test]
    fn swap_single_key() {
        let _ = env_logger::try_init();
        let map = Arc::new(WordMap::<System>::with_capacity(32));
        let key = START_IDX;
        let offsetted_key = key;
        let num_threads = 256;
        let num_rounds = 40960;
        let mut threads = vec![];
        map.insert(key, START_IDX);
        for _ in 0..num_threads {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                let guard = crossbeam_epoch::pin();
                for _ in 0..num_rounds {
                    map.table.swap(offsetted_key, &(), |n| Some(n + 1), &guard);
                }
            }));
        }
        for t in threads {
            t.join().unwrap();
        }
        assert_eq!(map.get(&key), Some(START_IDX + num_threads * num_rounds));
    }

    #[test]
    fn swap_no_resize() {
        let _ = env_logger::try_init();
        let map = Arc::new(WordMap::<System>::with_capacity(32));
        let guard = crossbeam_epoch::pin();
        let base_val = 10;
        let key = 10;
        map.insert(key, base_val);
        for j in 0..40960 {
            let curr_val = base_val + j;
            let next_val = curr_val + 1;
            map.insert(key, curr_val);
            map.table.swap(
                key,
                &(),
                move |v| {
                    assert_eq!(v, curr_val);
                    Some(next_val)
                },
                &guard,
            );
        }
        let mut threads = vec![];
        for i in 0..16 {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                let guard = crossbeam_epoch::pin();
                let base_val = 10;
                let key = i + 20;
                map.insert(key, base_val);
                for j in 0..40960 {
                    let curr_val = base_val + j;
                    let next_val = curr_val + 1;
                    debug_assert_eq!(map.get(&key), Some(curr_val));
                    map.table.swap(
                        key,
                        &(),
                        move |v| {
                            assert_eq!(v, curr_val);
                            Some(next_val)
                        },
                        &guard,
                    );
                }
            }));
        }
        assert_all_thread_passed(threads);
    }

    #[test]
    fn swap_with_resize() {
        let _ = env_logger::try_init();
        let repeats: usize = 409600;
        let multiplier = 10000000;
        let map = Arc::new(WordMap::<System>::with_capacity(8));
        let mut threads = vec![];
        for i in 1..32 {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                let guard = crossbeam_epoch::pin();
                let base_val = (i + 20) * multiplier;
                let key = i + 20;
                map.insert(key, base_val);
                for j in 0..repeats {
                    let curr_val = base_val + j;
                    let next_val = curr_val + 1;
                    debug_assert_eq!(
                        map.get(&key),
                        Some(curr_val),
                        "Value checking before swap at epoch {}, expecting {}",
                        map.table.now_epoch(), curr_val
                    );
                    let epoch = map.table.now_epoch();
                    let read_val = map.get(&key);
                    map.table.swap(
                        key,
                        &(),
                        move |v| {
                            assert!(v >= base_val);
                            assert_eq!(
                                v, curr_val,
                                "Fail check {} swapping offsetted {} from {} to {}, got {}. Get val {:?}. epoch {}",
                                key, key, curr_val, next_val, v, read_val, epoch
                            );
                            Some(next_val)
                        },
                        &guard,
                    );
                    let got_value = map.get(&key);
                    let expecting_value = Some(next_val);
                    if got_value != expecting_value {
                        let error_epoch = map.table.now_epoch();
                        error!("Value checking after swap at epoch {:?}. Expecting {:?} found {:?}. Probing for final value", error_epoch, expecting_value, got_value);
                        (0..256).for_each(|i| {
                            let new_got_value = map.get(&key);
                            if new_got_value == expecting_value {
                                panic!("Value checking failed. Expecting {:?} got {:?} recovered from epoch {} at {} turn {}, cap {}", got_value, expecting_value, error_epoch, map.table.now_epoch(), i, map.table.capacity());
                            }
                        });
                        panic!("Value checking failed. Expecting {:?} got {:?} DID NOT recovered from epoch {} at {} turn {}, cap {}", got_value, expecting_value, error_epoch, map.table.now_epoch(), i, map.table.capacity());
                    }
                }
            }));
        }
        for i in 1..16 {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                for j in 0..repeats {
                    let key = i * multiplier + j;
                    assert_eq!(map.insert(key, key), None, "inserting at key {}", key);
                }
                for j in 0..repeats {
                    let key = i * multiplier + j;
                    assert_eq!(
                        map.insert(key, key),
                        Some(key),
                        "reinserting at key {}",
                        key
                    );
                }
                for j in 0..repeats {
                    let key = i * multiplier + j;
                    if key < START_IDX {
                        continue;
                    }
                    assert_eq!(map.get(&key), Some(key), "reading at key {}", key);
                }
            }));
        }
        assert_all_thread_passed(threads);
    }

    #[test]
    fn checking_inserion_with_migrations() {
        let _ = env_logger::try_init();
        let repeats: usize = 20480;
        let multplier = 100000;
        let map = Arc::new(WordMap::<System>::with_capacity(8));
        let mut threads = vec![];
        for i in 1..64 {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                for j in START_IDX..repeats {
                    let key = i * 100000 + j;
                    let prev_epoch = map.table.now_epoch();
                    assert_eq!(map.insert(key, key), None, "inserting at key {}", key);
                    let post_epoch = map.table.now_epoch();
                    assert_eq!(
                        map.get(&key),
                        Some(key),
                        "Reading after insertion at key {}, epoch {}/{}/{}, last log {:?}, i {}",
                        key,
                        map.table.now_epoch(),
                        post_epoch,
                        prev_epoch,
                        get_delayed_log(5),
                        {
                            dump_migration_log();
                            i
                        }
                    );
                    let post_insert_epoch = map.table.now_epoch();
                    assert_eq!(
                        map.insert(key, key),
                        Some(key),
                        "reinserting at key {}, get {:?}, epoch {}/{}/{}, last log {:?}, i {}",
                        key,
                        map.get(&key),
                        map.table.now_epoch(),
                        post_insert_epoch,
                        prev_epoch,
                        get_delayed_log(3),
                        {
                            dump_migration_log();
                            i
                        }
                    );
                }
                for j in START_IDX..repeats {
                    let key = i * multplier + j;
                    assert_eq!(
                        map.insert(key, key),
                        Some(key),
                        "reinserting at key {}, get {:?}, epoch {}, last log {:?}, i {}",
                        key,
                        map.get(&key),
                        map.table.now_epoch(),
                        get_delayed_log(2),
                        {
                            dump_migration_log();
                            i
                        }
                    );
                }
                for j in START_IDX..repeats {
                    let key = i * multplier + j;
                    assert_eq!(map.get(&key), Some(key), "reading at key {}", key);
                }
            }));
        }
        assert_all_thread_passed(threads);
    }

    #[bench]
    fn resizing_before(b: &mut Bencher) {
        let _ = env_logger::try_init();
        let map = Arc::new(WordMap::<System>::with_capacity(65536));
        let mut i = START_IDX;
        b.iter(|| {
            map.insert(i, i);
            i += 1;
        });
    }

    #[bench]
    fn resizing_after(b: &mut Bencher) {
        let _ = env_logger::try_init();
        let prefill = 1048000;
        let map = Arc::new(WordMap::<System>::with_capacity(16));
        for i in START_IDX..prefill {
            map.insert(i, i);
        }
        let mut i = prefill;
        b.iter(|| {
            map.insert(i, i);
            i += 1;
        });
    }

    #[bench]
    fn resizing_with(b: &mut Bencher) {
        let _ = env_logger::try_init();
        let map = Arc::new(WordMap::<System>::with_capacity(2));
        let mut i = START_IDX;
        b.iter(|| {
            map.insert(i, i);
            i += 1;
        });
    }

    #[test]
    pub fn simple_resizing() {
        let _ = env_logger::try_init();
        let map = Arc::new(WordMap::<System>::with_capacity(4));
        let num_threads = 4;
        let mut threads = vec![];
        let num_data = 6;
        for i in 0..num_threads {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                for j in START_IDX..num_data {
                    let num = i * 1000 + j;
                    map.insert(num, num);
                    debug!("Inserted {}", num);
                    assert_eq!(map.get(&num), Some(num));
                }
            }));
        }
        info!("Waiting for threads to finish");
        for t in threads {
            t.join().unwrap();
        }
        for i in 0..num_threads {
            for j in START_IDX..num_data {
                let num = i * 1000 + j;
                debug!("Get {}", num);
                let first_round = map.get(&num);
                if first_round == Some(num) {
                    continue;
                }
                for round_count in 0..99 {
                    let following_round = map.get(&num);
                    if following_round == Some(num) {
                        info!(
                            "Falling back for {}, i {}, j {}, at {}",
                            num, i, j, round_count
                        );
                        break;
                    }
                }
                error!(
                    "Cannot fall back for {}, i {}, j {}, copying {}",
                    num,
                    i,
                    j,
                    map.table.map_is_copying()
                );
            }
        }
    }
}
