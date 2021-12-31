use std::{
    alloc::{GlobalAlloc, System},
    collections::hash_map::DefaultHasher,
    hash::Hasher,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::map::{InsertOp, ObjectTable};

pub struct TTLCache<
    V: Clone,
    ALLOC: GlobalAlloc + Default = System,
    H: Hasher + Default = DefaultHasher,
> {
    table: ObjectTable<V, ALLOC, H>,
}

impl<V: Clone, ALLOC: GlobalAlloc + Default, H: Hasher + Default> TTLCache<V, ALLOC, H> {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            table: ObjectTable::with_capacity(cap),
        }
    }
    pub fn get<F: Fn(usize) -> Option<V>>(&self, key: usize, lifetime: usize, fallback: F) -> Option<V> {
        let backoff = crossbeam_utils::Backoff::new();
        let guard = crossbeam_epoch::pin();
        let time = since_the_epoch() as usize;
        loop {
            let (ttl_val, v) = self.table.get(&(), key, true).unwrap_or((0, None));
            let mem_ttl_time = ttl_val >> 1;
            let new_ttl = time + lifetime;
            let new_ttl_val = new_ttl << 1;
            if mem_ttl_time > time {
                return v;
            }
            if v.is_none() {
                // Not existed
                return fallback(key).map(|v| {
                    self.table
                        .insert(InsertOp::TryInsert, &(), Some(v.clone()), key, new_ttl_val);
                    v
                });
            }
            // Expired
            if ttl_val & 1 == 1 {
                // First bit indicates there is a competition
                backoff.spin();
                continue; // Wait for it to be finished
            }
            match self.table.swap(
                key,
                &(),
                move |fast_value| {
                    if fast_value == ttl_val {
                        Some(new_ttl_val)
                    } else {
                        None
                    }
                },
                &guard,
            ) {
                crate::map::SwapResult::Succeed(_, _, _) => {}
                _ => {
                    backoff.spin();
                    continue
                },
            }
            if let Some(comp_v) = fallback(key) {
                self.table
                    .insert(InsertOp::Insert, &(), Some(comp_v.clone()), key, new_ttl_val);
                return Some(comp_v);
            } else {
                self.table
                    .insert(InsertOp::UpsertFast, &(), None, key, new_ttl_val);
                return None;
            }
        }
    }
}

fn since_the_epoch() -> u64 {
    let start = SystemTime::now();
    start
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs()
}

#[cfg(test)]
mod test {
    use std::{alloc::System, collections::hash_map::DefaultHasher, thread, time::Duration};

    use super::{since_the_epoch, TTLCache};

    #[test]
    fn general() {
        let cache = TTLCache::<_, System, DefaultHasher>::with_capacity(16);
        let now = since_the_epoch();
        assert_eq!(
            cache.get(42, 1, |k| {
                assert_eq!(k, 42);
                Some(now)
            }),
            Some(now)
        );
        assert_eq!(
            cache.get(42, 0, |k| {
                assert_eq!(k, 42);
                Some(24)
            }),
            Some(now)
        );
        thread::sleep(Duration::from_secs(2));
        assert_eq!(
            cache.get(42, 0, |k| {
                assert_eq!(k, 42);
                Some(48)
            }),
            Some(48)
        );
        assert_eq!(
            cache.get(24, 1, |k| {
                assert_eq!(k, 24);
                Some(96)
            }),
            Some(96)
        );
        assert_eq!(
            cache.get(42, 0, |k| {
                assert_eq!(k, 42);
                Some(48)
            }),
            Some(48)
        );
        assert_eq!(
            cache.get(24, 1, |k| {
                assert_eq!(k, 24);
                Some(1024)
            }),
            Some(96)
        );
    }
}
