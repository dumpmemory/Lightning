use super::base::*;
use super::*;

pub type HashTable<K, V, ALLOC, H> = Table<K, V, HashKVAttachment<K, V, ALLOC>, ALLOC, H>;

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
    type InitMeta = ();

    fn heap_size_of(cap: usize) -> usize {
        cap * Self::PAIR_SIZE
    }

    fn new(heap_ptr: usize, meta: &()) -> Self {
        Self {
            obj_chunk: heap_ptr,
            shadow: PhantomData,
        }
    }

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
    fn set_value(self, value: V, _old_fval: FVal) {
        let addr = self.addr;
        let val_addr = addr + Self::VAL_OFFSET;
        unsafe { ptr::write(val_addr as *mut V, value) }
    }

    #[inline(always)]
    fn erase(self, _old_fval: FVal) {
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
    table: HashTable<K, V, ALLOC, H>,
    shadow: PhantomData<H>,
}

impl<K: Clone + Hash + Eq, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default>
    HashMap<K, V, ALLOC, H>
{
    #[inline(always)]
    pub fn insert_with_op(&self, op: InsertOp, key: &K, value: &V) -> Option<V> {
        self.table
            .insert(op, key, Some(value), 0, PLACEHOLDER_VAL)
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
            table: Table::with_capacity(cap, ()),
            shadow: PhantomData,
        }
    }

    #[inline(always)]
    fn get(&self, key: &K) -> Option<V> {
        self.table.get(key, 0, true).map(|v| v.1.unwrap())
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
        self.table.remove(key, 0).map(|(_, v)| v)
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
        self.table.get(key, 0, false).is_some()
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.table.len()
    }

    fn clear(&self) {
        self.table.clear();
    }
}

pub struct HashMapReadGuard<
    'a,
    K: Clone + Eq + Hash,
    V: Clone,
    ALLOC: GlobalAlloc + Default = System,
    H: Hasher + Default = DefaultHasher,
> {
    table: &'a HashTable<K, V, ALLOC, H>,
    hash: usize,
    key: K,
    value: V,
    _mark: PhantomData<H>,
}

impl<'a, K: Clone + Eq + Hash, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default>
    HashMapReadGuard<'a, K, V, ALLOC, H>
{
    fn new(table: &'a HashTable<K, V, ALLOC, H>, key: &K) -> Option<Self> {
        let backoff = crossbeam_utils::Backoff::new();
        let guard = crossbeam_epoch::pin();
        let hash = hash_key::<K, H>(&key);
        let value: V;
        loop {
            let swap_res = table.swap(
                hash as FKey,
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
            self.hash as FKey,
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
    table: &'a HashTable<K, V, ALLOC, H>,
    hash: usize,
    key: K,
    value: V,
    _mark: PhantomData<H>,
}

impl<'a, K: Clone + Eq + Hash, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default>
    HashMapWriteGuard<'a, K, V, ALLOC, H>
{
    fn new(table: &'a HashTable<K, V, ALLOC, H>, key: &K) -> Option<Self> {
        let backoff = crossbeam_utils::Backoff::new();
        let guard = crossbeam_epoch::pin();
        let hash = hash_key::<K, H>(&key);
        let value: V;
        loop {
            let swap_res = table.swap(
                hash as FKey,
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
        let res = self.table.remove(&self.key, self.hash as FKey).unwrap().1;
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
            Some(&self.value),
            hash as FKey,
            PLACEHOLDER_VAL,
        );
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
        let map = ObjectMap::<usize, System>::with_capacity(16);
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
                              error!("Discovered mismatch key {:?}, analyzing", &key);
                              for m in 1..1024 {
                                  let mleft = map.get(&key);
                                  let mright = Some(value);
                                  if mleft == mright {
                                      panic!(
                                          "Recovered at turn {} for {:?}, copying {}, epoch {} to {}, now {}, PIE: {} to {}. Expecting {:?} got {:?}. Migration problem!!!", 
                                          m, 
                                          &key, 
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
                              panic!("Unable to recover for {:?}, round {}, copying {}. Expecting {:?} got {:?}.", &key, l , map.table.map_is_copying(), right, left);
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
mod test {
    use super::tests::*;
    use super::*;
    use std::sync::Arc;
    use std::thread;

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
}
