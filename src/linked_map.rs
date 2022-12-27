// A concurrent linked hash map, fast and lock-free on iterate
use crate::list::{LinkedRingBufferList, ListIter};
use crate::map::{Map, PtrHashMap, PtrMutexGuard};
use crate::ring_buffer::ItemPtr;
use std::hash::Hash;

#[derive(Clone, Default)]
pub struct KVPair<K: Clone + Default, V: Clone + Default>(pub K, pub V);

pub struct LinkedHashMap<K: Clone + Hash + Eq + Default, V: Clone + Default, const N: usize> {
    map: PtrHashMap<K, ItemPtr<KVPair<K, V>, N>>,
    list: LinkedRingBufferList<KVPair<K, V>, N>,
}

impl<K: Clone + Hash + Eq + Default, V: Clone + Default, const N: usize> LinkedHashMap<K, V, N> {
    pub fn with_capacity(cap: usize) -> Self {
        LinkedHashMap {
            map: PtrHashMap::with_capacity(cap),
            list: LinkedRingBufferList::new(),
        }
    }

    pub fn insert_front(&self, key: K, value: V) -> Option<V> {
        let pair = KVPair(key.clone(), value);
        let list_ref = self.list.push_front(pair);
        match self.map.locked_with_upsert(&key, list_ref) {
            Ok((_guard, old_ref)) => {
                return unsafe {
                    old_ref.remove().map(|KVPair(_, v)| v)
                };
            },
            Err(_guard) => {
                return None;
            }
        }
    }

    pub fn insert_back(&self, key: K, value: V) -> Option<V> {
        let pair = KVPair(key.clone(), value);
        let list_ref = self.list.push_back(pair);
        match self.map.locked_with_upsert(&key, list_ref) {
            Ok((_guard, old_ref)) => {
                return unsafe {
                    old_ref.remove().map(|KVPair(_, v)| v)
                };
            },
            Err(_guard) => {
                return None;
            }
        }
    }

    pub fn get(&self, key: &K) -> Option<V> {
        self.map
            .get(key)
            .and_then(|l| unsafe { l.deref() })
            .map(|pair| pair.1)
    }

    pub fn get_to_front(&self, key: &K) -> Option<V> {
        self.get_to_general(key, true)
    }

    pub fn get_to_back(&self, key: &K) -> Option<V> {
        self.get_to_general(key, false)
    }

    #[inline(always)]
    pub fn get_to_general(&self, key: &K, forwarding: bool) -> Option<V> {
        self.map.lock(key).and_then(|mut l| {
            let pair = unsafe { l.deref() }.unwrap();
            let new_ref = if forwarding {
                self.list.push_front(pair)
            } else {
                self.list.push_back(pair)
            };
            let old_ref = l.clone();
            *l = new_ref;
            unsafe { old_ref.remove().map(|KVPair(_, v)| v) }
        })
    }

    pub fn remove(&self, key: &K) -> Option<V> {
        self.map
            .lock(key)
            .and_then(|l| unsafe { 
                PtrMutexGuard::remove(l).remove().map(|KVPair(_, v)| v) 
            })
    }

    pub fn pop_front(&self) -> Option<KVPair<K, V>> {
        self.pop_general(true)
    }

    pub fn pop_back(&self) -> Option<KVPair<K, V>> {
        self.pop_general(false)
    }

    #[inline(always)]
    fn pop_general(&self, forwarding: bool) -> Option<KVPair<K, V>> {
        loop {
            let list_item = if forwarding {
                self.list.peek_front()
            } else {
                self.list.peek_back()
            };
            if let Some(pair) = list_item {
                if let Some(KVPair(k, v)) = pair.deref() {
                    if let Some(l) = self.map.lock(&k) {
                        unsafe {
                            PtrMutexGuard::remove(l).remove();
                            return Some(KVPair(k, v));
                        }
                    }
                }
            } else {
                return None;
            }
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.map.contains_key(key)
    }

    pub fn iter_front(&self) -> ListIter<KVPair<K, V>, N> {
        self.list.iter_front()
    }

    pub fn iter_back(&self) -> ListIter<KVPair<K, V>, N> {
        self.list.iter_back()
    }

    pub fn iter_front_keys(&self) -> KeyIter<K, V, N> {
        KeyIter {
            iter: self.iter_front(),
        }
    }

    pub fn iter_back_keys(&self) -> KeyIter<K, V, N> {
        KeyIter {
            iter: self.iter_back(),
        }
    }

    pub fn iter_front_values(&self) -> ValueIter<K, V, N> {
        ValueIter {
            iter: self.iter_front(),
        }
    }

    pub fn iter_back_values(&self) -> ValueIter<K, V, N> {
        ValueIter {
            iter: self.iter_back(),
        }
    }
}

pub struct KeyIter<'a, K: Clone + Hash + Default, V: Clone + Default, const N: usize> {
    iter: ListIter<'a, KVPair<K, V>, N>,
}

impl<'a, K: Clone + Hash + Default, V: Clone + Default, const N: usize> Iterator
    for KeyIter<'a, K, V, N>
{
    type Item = K;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .and_then(|i| i.deref())
            .map(|KVPair(k, _)| k)
    }
}

pub struct ValueIter<'a, K: Clone + Hash + Default, V: Clone + Default, const N: usize> {
    iter: ListIter<'a, KVPair<K, V>, N>,
}

impl<'a, K: Clone + Hash + Default, V: Clone + Default, const N: usize> Iterator
    for ValueIter<'a, K, V, N>
{
    type Item = V;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter
            .next()
            .and_then(|i| i.deref())
            .map(|KVPair(_, v)| v)
    }
}

impl<K: Clone + Default, V: Clone + Default> KVPair<K, V> {
    pub fn key(&self) -> &K {
        &self.0
    }

    pub fn value(&self) -> &V {
        &self.1
    }

    pub fn pair(&self) -> (&K, &V) {
        (&self.0, &self.1)
    }
}

unsafe impl<K: Clone + Hash + Eq + Default, V: Clone + Default, const N: usize> Send
    for LinkedHashMap<K, V, N>
{
}

#[cfg(test)]
mod test {
    use itertools::Itertools;

    use super::*;
    use crate::tests_misc::assert_all_thread_passed;
    use std::{collections::HashSet, sync::Arc, thread};
    const CAP: usize = 16;

    #[test]
    pub fn linked_map_serial() {
        let map = LinkedHashMap::<_, _, CAP>::with_capacity(16);
        for i in 0..1024 {
            map.insert_front(i, i);
        }
        for i in 1024..2048 {
            map.insert_back(i, i);
        }
    }

    #[test]
    pub fn linked_map_insertions() {
        let _ = env_logger::try_init();
        let linked_map = Arc::new(LinkedHashMap::<_, _, CAP>::with_capacity(4));
        let num_threads = 32;
        let mut threads = vec![];
        let num_data = 32;
        for i in 0..num_threads {
            let map = linked_map.clone();
            threads.push(thread::spawn(move || {
                for j in 0..num_data {
                    let num = i * 1000 + j;
                    debug!("Insert {}", num);
                    if j % 2 == 1 {
                        map.insert_back(num, Arc::new(num));
                    } else {
                        map.insert_front(num, Arc::new(num));
                    }
                    assert_eq!(map.get(&num).map(|n| *n), Some(num));
                }
                map.iter_front_keys().collect_vec();
                map.iter_front_values().collect_vec();
                map.iter_front().collect_vec();
                map.iter_back_keys().collect_vec();
                map.iter_back_values().collect_vec();
                map.iter_back().collect_vec();
            }));
        }
        info!("Waiting for threads to finish");
        for t in threads {
            t.join().unwrap();
        }
        for i in 0..num_threads {
            for j in 0..num_data {
                let num = i * 1000 + j;
                let first_round = linked_map.get(&num).map(|n| *n);
                if first_round == Some(num) {
                    continue;
                }
                for round_count in 0..99 {
                    let following_round = linked_map.get(&num).map(|n| *n);
                    if following_round == Some(num) {
                        info!("Falling back for i {}, j {}, at {}", i, j, round_count);
                        break;
                    }
                }
                linked_map.map.table.dump_dist();
                error!("Cannot fall back for i {}, j {}", i, j);
            }
        }
        let mut num_set = HashSet::new();
        for pair in linked_map.iter_front() {
            let KVPair(key, node) = pair.deref().unwrap();
            let value = node;
            assert_eq!(key, *value);
            num_set.insert(key);
        }
        assert_eq!(num_set.len(), num_threads * num_data);
    }

    macro_rules! linked_map_tests {
        (
            $name: ident,
            $insert: ident,
            $mty: ty,
            $kty: ty,
            $vty: ty,
            $minit: block,
            $kinit: block,
            $vinit: block
        ) => {
            mod $name {
                use super::*;

                pub type Key = $kty;
                pub type Value = $vty;

                fn key_from(num: usize) -> Key {
                    ($kinit)(num)
                }

                fn val_from(key: &Key, num: usize) -> Value {
                    ($vinit)(key, num)
                }

                fn map_init(cap: usize) -> $mty {
                    ($minit)(cap)
                }

                #[test]
                fn no_resize() {
                    let _ = env_logger::try_init();
                    let map = map_init(4096);
                    for i in 5..2048 {
                        let k = key_from(i);
                        let v = val_from(&k, i * 2);
                        map.$insert(k, v);
                    }
                    for i in 5..2048 {
                        let k = key_from(i);
                        let v = val_from(&k, i * 2);
                        match map.get(&k) {
                            Some(r) => assert_eq!(r, v),
                            None => panic!("{}", i),
                        }
                    }
                }

                #[test]
                fn resize() {
                    let _ = env_logger::try_init();
                    let map = map_init(16);
                    for i in 5..2048 {
                        let k = key_from(i);
                        let v = val_from(&k, i * 2);
                        map.$insert(k, v);
                    }
                    for i in 5..2048 {
                        let k = key_from(i);
                        let v = val_from(&k, i * 2);
                        match map.get(&k) {
                            Some(r) => assert_eq!(r, v),
                            None => panic!("{}", i),
                        }
                    }
                }

                #[test]
                fn parallel_no_resize() {
                    let _ = env_logger::try_init();
                    let map = Arc::new(map_init(65536));
                    let mut threads = vec![];
                    for i in 5..99 {
                        let k = key_from(i);
                        let v = val_from(&k, i * 10);
                        map.$insert(k, v);
                    }
                    for i in 100..900 {
                        let map = map.clone();
                        threads.push(thread::spawn(move || {
                            for j in 5..60 {
                                let k = key_from(i * 100 + j);
                                let v = val_from(&k, i * j);
                                map.$insert(k, v);
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
                            let v = val_from(&k, i * j);
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
                fn exaust_versions() {
                    let map = map_init(16);
                    for i in 0..255 {
                        if i == 2 {
                            println!("watch out");
                        }
                        let key = key_from(1);
                        let val = val_from(&key, i);
                        let orig_key = key.clone();
                        let orig_val = val.clone();
                        map.$insert(key, val);
                        debug_assert_eq!(map.get(&orig_key).unwrap(), orig_val);
                    }
                    for i in 255..2096 {
                        let key = key_from(1);
                        let val = val_from(&key, i);
                        let orig_key = key.clone();
                        let orig_val = val.clone();
                        map.$insert(key, val);
                        debug_assert_eq!(map.get(&orig_key).unwrap(), orig_val);
                    }
                }

                #[test]
                fn parallel_with_resize() {
                    let _ = env_logger::try_init();
                    let num_threads = num_cpus::get();
                    let test_load = 1024;
                    let repeat_load = 16;
                    let map = Arc::new(map_init(32));
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
                                      assert_eq!(map.get(&key), Some(val_from(&key, value_num - 1)));
                                  }
                                  let value = val_from(&key, value_num);
                                  let pre_insert_epoch = map.map.table.now_epoch();
                                  map.$insert(key.clone(), value.clone());
                                  let post_insert_epoch = map.map.table.now_epoch();
                                  for l in 1..128 {
                                      let pre_fail_get_epoch = map.map.table.now_epoch();
                                      let left = map.get(&key);
                                      let post_fail_get_epoch = map.map.table.now_epoch();
                                      let right = Some(&value);
                                      if left.as_ref() != right {
                                          error!("Discovered mismatch key {:?}, expecting {:?}, got {:?}, analyzing", &key, right, left);
                                          let error_checking = || {
                                            for m in 1..1024 {
                                                let mleft = map.get(&key);
                                                let mright = Some(&value);
                                                if mleft.as_ref() == mright {
                                                    panic!(
                                                        "Recovered at turn {} for {:?}, copying {}, epoch {} to {}, now {}, PIE: {} to {}. Expecting {:?} got {:?}. Migration problem!!!",
                                                        m,
                                                        &key,
                                                        map.map.table.map_is_copying(),
                                                        pre_fail_get_epoch,
                                                        post_fail_get_epoch,
                                                        map.map.table.now_epoch(),
                                                        pre_insert_epoch,
                                                        post_insert_epoch,
                                                        right, left
                                                     );
                                                    // panic!("Late value change on {:?}", key);
                                                }
                                            }
                                          };
                                          error_checking();
                                          panic!("Unable to recover for {:?}, round {}, copying {}. Expecting {:?} got {:?}.", &key, l , map.map.table.map_is_copying(), right, left);
                                          // panic!("Unrecoverable value change for {:?}", key);
                                      }
                                  }
                                  if j % 8 == 0 {
                                    let pre_rm_epoch = map.map.table.now_epoch();
                                    assert_eq!(
                                        map.remove(&key).as_ref(),
                                        Some(&value),
                                        "Remove result, get {:?}, copying {}, round {}",
                                        map.get(&key),
                                        map.map.table.map_is_copying(),
                                        k
                                    );
                                    let post_rm_epoch = map.map.table.now_epoch();
                                    assert_eq!(map.get(&key), None, "Remove recursion, value was {:?}. Epoch pre {}, post {}, get {}", value, pre_rm_epoch, post_rm_epoch, map.map.table.now_epoch());
                                    map.$insert(key.clone(), value.clone());
                                  }
                                  if j % 4 == 0 {
                                      let updating = || {
                                        let new_value = val_from(&key, value_num + 7);
                                        let pre_insert_epoch = map.map.table.now_epoch();
                                        map.$insert(key.clone(), new_value.clone());
                                        let post_insert_epoch = map.map.table.now_epoch();
                                        assert_eq!(
                                            map.get(&key).as_ref(),
                                            Some(&new_value),
                                            "Checking immediate update, key {:?}, epoch {} to {}",
                                            key, pre_insert_epoch, post_insert_epoch
                                        );
                                        map.$insert(key.clone(), value.clone());
                                      };
                                      updating();
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
                            let v = val_from(&k, v_num);
                            let get_res = map.get(&k);
                            assert_eq!(
                                Some(v),
                                get_res,
                                "Final val mismatch. k {:?}, i {}, j {}, epoch {}",
                                k,
                                i,
                                j,
                                map.map.table.now_epoch()
                            );
                        }
                    });
                }

                #[test]
                fn ptr_checking_inserion_with_migrations() {
                    let _ = env_logger::try_init();
                    let repeats: usize = 20480;
                    let map = Arc::new(map_init(8));
                    let mut threads = vec![];
                    for i in 1..64 {
                        let map = map.clone();
                        threads.push(thread::spawn(move || {
                            for j in 0..repeats {
                                let n = i * 100000 + j;
                                let key = key_from(n);
                                let value = val_from(&key, n);
                                let prev_epoch = map.map.table.now_epoch();
                                assert_eq!(map.$insert(key.clone(), value.clone()), None, "inserting at key {}", key);
                                let post_insert_epoch = map.map.table.now_epoch();
                                {
                                    let get_test_res = map.get(&key);
                                    let get_epoch = map.map.table.now_epoch();
                                    let expecting = Some(value.clone());
                                    if get_test_res != expecting {
                                        panic!(
                                            "Value mismatch {:?} expecting {:?}. Reading after insertion at key {}, epoch {}/{}/{}.",
                                            get_test_res, expecting,
                                            key, get_epoch, post_insert_epoch, prev_epoch,
                                        );
                                    }
                                }
                                let post_insert_epoch = map.map.table.now_epoch();
                                assert_eq!(
                                    map.$insert(key.clone(), value.clone()),
                                    Some(value.clone()),
                                    "reinserting at key {}, get {:?}, epoch {}/{}/{}, i {}",
                                    key,
                                    map.get(&key),
                                    map.map.table.now_epoch(),
                                    post_insert_epoch,
                                    prev_epoch, i
                                );
                            }
                            for j in 0..repeats {
                                let n = i * 100000 + j;
                                let key = key_from(n);
                                let value = val_from(&key, n);
                                assert_eq!(
                                    map.$insert(key.clone(), value.clone()),
                                    Some(value),
                                    "reinserting at key {}, get {:?}, epoch {}, i {}",
                                    key,
                                    map.get(&key),
                                    map.map.table.now_epoch(),  i
                                );
                            }
                            for j in 0..repeats {
                                let n = i * 100000 + j;
                                let key = key_from(n);
                                let value = val_from(&key, n);
                                assert_eq!(
                                    map.get(&key),
                                    Some(value),
                                    "reading at key {}, epoch {}",
                                    key,
                                    map.map.table.now_epoch()
                                );
                            }
                        }));
                    }
                    assert_all_thread_passed(threads);
                }
            }
        };
    }

    linked_map_tests!(
        usize_front_test,
        insert_front,
        LinkedHashMap<usize, usize, CAP>,
        usize, usize,
        {
            |cap| LinkedHashMap::with_capacity(cap)
        },
        {
            |k| k as usize
        }, {
            |_k, v| v as usize
        }
    );

    linked_map_tests!(
        usize_back_test,
        insert_back,
        LinkedHashMap<usize, usize, CAP>,
        usize, usize,
        {
            |cap| LinkedHashMap::with_capacity(cap)
        },
        {
            |k| k as usize
        }, {
            |_k, v| v as usize
        }
    );

    linked_map_tests!(
        string_key_back_test,
        insert_back,
        LinkedHashMap<String, usize, CAP>,
        String, usize,
        {
            |cap| LinkedHashMap::with_capacity(cap)
        },
        {
            |k| format!("{}", k)
        }, {
            |_k, v| v as usize
        }
    );

    linked_map_tests!(
        string_key_front_test,
        insert_front,
        LinkedHashMap<String, usize, CAP>,
        String, usize,
        {
            |cap| LinkedHashMap::with_capacity(cap)
        },
        {
            |k| format!("{}", k)
        }, {
            |_k, v| v as usize
        }
    );

    linked_map_tests!(
        string_kv_back_test,
        insert_back,
        LinkedHashMap<String, Vec<char>, CAP>,
        String, Vec<char>,
        {
            |cap| LinkedHashMap::with_capacity(cap)
        },
        {
            |k| format!("{}", k)
        }, {
            |k, v| {
                let str = format!("{:>32}{:>32}", k, v);
                let mut res = ['0'; 64];
                for (i, c) in str.chars().enumerate() {
                    res[i] = c;
                }
                Vec::from(res)
            }
        }
    );

    linked_map_tests!(
        string_kv_front_test,
        insert_front,
        LinkedHashMap<String, Vec<char>, CAP>,
        String, Vec<char>,
        {
            |cap| LinkedHashMap::with_capacity(cap)
        },
        {
            |k| format!("{}", k)
        }, {
            |k, v| {
                let str = format!("{:>32}{:>32}", k, v);
                let mut res = ['0'; 64];
                for (i, c) in str.chars().enumerate() {
                    res[i] = c;
                }
                Vec::from(res)
            }
        }
    );
}
