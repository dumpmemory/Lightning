use super::base::*;
use super::*;

pub type WordTable<H, ALLOC> = Table<(), (), WordAttachment, H, ALLOC>;

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
        self.table
            .insert(op, &(), None, key + NUM_FIX_K, value + NUM_FIX_V)
            .map(|(v, _)| v - NUM_FIX_V)
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
        self.table
            .get(&(), key + NUM_FIX_K, false)
            .map(|v| v.0 - NUM_FIX_V)
    }

    #[inline(always)]
    fn insert(&self, key: FKey, value: FVal) -> Option<FVal> {
        self.insert_with_op(InsertOp::UpsertFast, key, value)
    }

    #[inline(always)]
    fn try_insert(&self, key: FKey, value: FVal) -> Option<FVal> {
        self.insert_with_op(InsertOp::TryInsert, key, value)
    }

    #[inline(always)]
    fn remove(&self, key: &FKey) -> Option<FVal> {
        self.table
            .remove(&(), key + NUM_FIX_K)
            .map(|(v, _)| v - NUM_FIX_V)
    }
    fn entries(&self) -> Vec<(FKey, FVal)> {
        self.table
            .entries()
            .into_iter()
            .map(|(k, v, _, _)| (k - NUM_FIX_K, v - NUM_FIX_V))
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
        let key = key + NUM_FIX_K;
        let value = 0;
        match table.insert(
            InsertOp::TryInsert,
            &(),
            Some(&()),
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
    fn new(table: &'a WordTable<ALLOC, H>, key: FKey) -> Option<Self> {
        let key = key + NUM_FIX_K;
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
        let value = value - NUM_FIX_V;
        Some(Self { table, key, value })
    }

    pub fn remove(self) -> FVal {
        trace!("Removing {}", self.key);
        let res = self.table.remove(&(), self.key).unwrap().0;
        mem::forget(self);
        res | MUTEX_BIT_MASK
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
        self.value += NUM_FIX_V;
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

pub struct WordAttachment;
#[derive(Copy, Clone)]
pub struct WordAttachmentItem;

// this attachment basically do nothing and sized zero
impl Attachment<(), ()> for WordAttachment {
    type Item = WordAttachmentItem;
    type InitMeta = ();

    #[inline(always)]
    fn heap_size_of(_cap: usize) -> usize {
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

    #[inline(always)]
    fn manually_drop(&self, _: usize) {}
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
    fn erase(self, _old_fval: FVal) {}

    #[inline(always)]
    fn probe(self, _value: &()) -> bool {
        true
    }

    #[inline(always)]
    fn prep_write(self) {}
}

#[cfg(test)]
mod test {
    use crate::map::*;
    use alloc::sync::Arc;
    use rayon::prelude::*;
    use test::Bencher;
    use std::thread;
    #[test]
    fn will_not_overflow() {
        let _ = env_logger::try_init();
        let table = WordMap::<System>::with_capacity(16);
        for i in 50..60 {
            assert_eq!(table.insert(i, i), None);
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
            map.insert(i, i * 2);
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
            map.insert(i, i * 10);
        }
        for i in 100..900 {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                for j in 5..60 {
                    map.insert(i * 100 + j, i * j);
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
                              panic!("Unable to recover for {}, round {}, copying {}", key, l , map.table.map_is_copying());
                          }
                      }
                      if j % 5 == 0 {
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
                          map.insert(key, value);
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
            map.insert(i, i * 10);
        }
        let mut threads = vec![];
        for i in 256..265 {
            let map = map.clone();
            threads.push(thread::spawn(move || {
                for j in 5..60 {
                    map.insert(i * 10 + j, 10);
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
        map.insert(1, 0);
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
        let map = Arc::new(WordMap::<System>::with_capacity(16));
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

    #[bench]
    fn resizing_before(b: &mut Bencher) {
        let _ = env_logger::try_init();
        let map = Arc::new(WordMap::<System>::with_capacity(65536));
        let mut i = 5;
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
        for i in 0..prefill {
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
        let mut i = 5;
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
                for j in 0..num_data {
                    let num = i * 1000 + j;
                    debug!("Insert {}", num);
                    map.insert(num, num);
                    assert_eq!(map.get(&num), Some(num));
                }
            }));
        }
        info!("Waiting for threads to finish");
        for t in threads {
            t.join().unwrap();
        }
        for i in 0..num_threads {
            for j in 0..num_data {
                let num = i * 1000 + j;
                debug!("Get {}", num);
                let first_round = map.get(&num);
                if first_round == Some(num) {
                    continue;
                }
                for round_count in 0..99 {
                    let following_round = map.get(&num);
                    if following_round == Some(num) {
                        info!("Falling back for {}, i {}, j {}, at {}", num, i, j, round_count);
                        break;
                    }
                }
                error!("Cannot fall back for {}, i {}, j {}, copying {}", num, i, j, map.table.map_is_copying());
            }
        }
    }
}
