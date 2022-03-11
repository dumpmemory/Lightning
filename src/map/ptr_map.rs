use super::base::*;
use super::*;

pub type PtrTable<K, V, ALLOC, H> =
    Table<K, (), PtrValAttachment<K, V, ALLOC>, ALLOC, H>;

pub struct PtrHashMap<
    K: Clone + Hash + Eq,
    V: Clone,
    ALLOC: GlobalAlloc + Default = System,
    H: Hasher + Default = DefaultHasher,
> {
    table: PtrTable<K, V, ALLOC, H>,
    shadow: PhantomData<(K, V, H)>,
}

impl<K: Clone + Hash + Eq, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default>
    PtrHashMap<K, V, ALLOC, H>
{
    #[inline(always)]
    pub fn insert_with_op(&self, op: InsertOp, key: &K, value: &V) -> Option<V> {
        let guard = crossbeam_epoch::pin();
        let v_num = Self::ref_val(value);
        self.table
            .insert(op, key, Some(&()), 0 as FKey, v_num as FVal)
            .map(|(fv, _)| Self::deref_val(fv as usize))
    }

    // pub fn write(&self, key: &K) -> Option<HashMapWriteGuard<K, V, ALLOC, H>> {
    //     HashMapWriteGuard::new(&self.table, key)
    // }
    // pub fn read(&self, key: &K) -> Option<HashMapReadGuard<K, V, ALLOC, H>> {
    //     HashMapReadGuard::new(&self.table, key)
    // }

    #[inline(always)]
    fn ref_val<T: Clone>(d: &T) -> usize {
        unsafe {
            T::init(d.clone())
        }
    }

    #[inline(always)]
    fn deref_val<T: Clone>(num: usize) -> T {
        unsafe {
            T::deref(num).clone()
        }
    }
}

impl<K: Clone + Hash + Eq, V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> Map<K, V>
    for PtrHashMap<K, V, ALLOC, H>
{
    fn with_capacity(cap: usize) -> Self {
        Self {
            table: PtrTable::with_capacity(cap),
            shadow: PhantomData,
        }
    }

    fn get(&self, key: &K) -> Option<V> {
        self.table.get(key, 0, false).map(|(fv, _)| Self::deref_val(fv))
    }

    fn insert(&self, key: &K, value: &V) -> Option<V> {
        self.insert_with_op(InsertOp::Insert, key, value)
    }

    fn try_insert(&self, key: &K, value: &V) -> Option<V> {
        self.insert_with_op(InsertOp::TryInsert, key, value)
    }

    fn remove(&self, key: &K) -> Option<V> {
        self.table.remove(key, 0).map(|(fv, _)| Self::deref_val(fv))
    }

    fn entries(&self) -> Vec<(K, V)> {
        self.table
            .entries()
            .into_iter()
            .map(|(_, fv, k, _)| (k, Self::deref_val(fv)))
            .collect()
    }

    fn contains_key(&self, key: &K) -> bool {
        self.table.get(key, 0, false).is_some()
    }

    fn len(&self) -> usize {
        self.table.len()
    }

    fn clear(&self) {
        self.table.clear();
    }
}

#[derive(Clone)]
pub struct PtrValAttachment<K: Clone + Hash + Eq, V: Clone, A: GlobalAlloc + Default> {
    key_chunk: usize,
    _marker: PhantomData<(K, V, A)>,
}

#[derive(Clone)]
pub struct PtrValAttachmentItem<K, V> {
    addr: usize,
    _marker: PhantomData<(K, V)>,
}

impl<K: Clone + Hash + Eq, V: Clone, A: GlobalAlloc + Default> PtrValAttachment<K, V, A> {
    const KEY_SIZE: usize = mem::size_of::<K>();

    fn addr_by_index(&self, index: usize) -> usize {
        self.key_chunk + index * Self::KEY_SIZE
    }
}

impl<K: Clone + Hash + Eq, V: Clone, A: GlobalAlloc + Default> Attachment<K, ()>
    for PtrValAttachment<K, V, A>
{
    type Item = PtrValAttachmentItem<K, V>;

    fn heap_size_of(cap: usize) -> usize {
        cap * Self::KEY_SIZE // only keys on the heap
    }

    fn new(heap_ptr: usize) -> Self {
        Self {
            key_chunk: heap_ptr,
            _marker: PhantomData,
        }
    }

    fn prefetch(&self, index: usize) -> Self::Item {
        let addr = self.addr_by_index(index);
        unsafe {
            intrinsics::prefetch_read_data(addr as *const K, 2);
        }
        PtrValAttachmentItem {
            addr,
            _marker: PhantomData,
        }
    }
}

impl<K: Clone + Hash + Eq, V: Clone> AttachmentItem<K, ()> for PtrValAttachmentItem<K, V> {
    fn get_key(self) -> K {
        let addr = self.addr;
        unsafe { (*(addr as *mut K)).clone() }
    }

    fn get_value(self) -> () {}

    fn set_key(self, key: K) {
        let addr = self.addr;
        unsafe { ptr::write_volatile(addr as *mut K, key) }
    }

    fn set_value(self, _value: (), old_fval: FVal) {
        self.erase(old_fval)
    }

    fn erase(self, old_fval: FVal) {
        if old_fval >= NUM_FIX_V {
            unsafe {
                let guard = crossbeam_epoch::pin();
                let ptr = Shared::<V>::from_usize(old_fval as usize);
                guard.defer_destroy(ptr);
            }
        }
    }

    fn probe(self, probe_key: &K) -> bool {
        let key = unsafe { &*(self.addr as *mut K) };
        key == probe_key
    }

    fn prep_write(self) {}
}

impl<K: Clone, V: Clone> Copy for PtrValAttachmentItem<K, V> {}

#[cfg(test)]
mod ptr_map {
    use crate::map::*;
    use std::{alloc::System, sync::Arc, thread};

    #[test]
    fn no_resize() {
        let _ = env_logger::try_init();
        let map = PtrHashMap::<usize, usize, System>::with_capacity(4096);
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

    #[test]
    fn resize() {
        let _ = env_logger::try_init();
        let map = PtrHashMap::<usize, usize, System>::with_capacity(16);
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
                          // assert!(map.read(&key).is_none(), "Remove recursion with lock");
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
    
    const VAL_SIZE: usize = 2048;
    pub type Key = [u8; 128];
    pub type Value = [u8; VAL_SIZE];
    pub type FatHashMap = PtrHashMap<Key, Value, System>;

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