#[macro_export]
macro_rules! par_list_tests {
    (
        $list_init: block,
        $num: expr
    ) => {
        use itertools::Itertools;
        use std::{collections::HashSet, sync::Arc, thread};
        #[test]
        pub fn multithread_push_front_single_thread_pop_front() {
            let num: usize = $num;
            let deque = Arc::new($list_init);
            let ths = (0..num)
                .chunks(256)
                .into_iter()
                .map(|nums| {
                    let nums = nums.collect_vec();
                    let deque = deque.clone();
                    thread::spawn(move || {
                        for i in nums {
                            deque.push_front(i);
                        }
                    })
                })
                .collect::<Vec<_>>();
            ths.into_iter().for_each(|t| {
                t.join().unwrap();
            });
            let mut all_nums = HashSet::new();
            for _ in 0..num {
                all_nums.insert(deque.pop_front().unwrap());
            }
            assert_eq!(all_nums.len(), num);
            for i in 0..num {
                assert!(all_nums.contains(&i));
            }
        }

        #[test]
        pub fn multithread_push_front_single_thread_pop_back() {
            let num: usize = $num;
            let deque = Arc::new($list_init);
            let ths = (0..num)
                .chunks(256)
                .into_iter()
                .map(|nums| {
                    let nums = nums.collect_vec();
                    let deque = deque.clone();
                    thread::spawn(move || {
                        for i in nums {
                            deque.push_front(i);
                        }
                    })
                })
                .collect::<Vec<_>>();
            ths.into_iter().for_each(|t| {
                t.join().unwrap();
            });
            let mut all_nums = HashSet::new();
            for _ in 0..num {
                all_nums.insert(deque.pop_back().unwrap());
            }
            assert_eq!(all_nums.len(), num);
            for i in 0..num {
                assert!(all_nums.contains(&i));
            }
        }

        #[test]
        pub fn multithread_push_back_single_thread_pop_front() {
            let num: usize = $num;
            let deque = Arc::new($list_init);
            let ths = (0..num)
                .chunks(256)
                .into_iter()
                .map(|nums| {
                    let nums = nums.collect_vec();
                    let deque = deque.clone();
                    thread::spawn(move || {
                        for i in nums {
                            deque.push_back(i);
                        }
                    })
                })
                .collect::<Vec<_>>();
            ths.into_iter().for_each(|t| {
                t.join().unwrap();
            });
            let mut all_nums = HashSet::new();
            for _ in 0..num {
                all_nums.insert(deque.pop_front().unwrap());
            }
            assert_eq!(all_nums.len(), num);
            for i in 0..num {
                assert!(all_nums.contains(&i));
            }
        }

        #[test]
        pub fn multithread_push_back_single_thread_pop_back() {
            let num: usize = $num;
            let deque = Arc::new($list_init);
            let ths = (0..num)
                .chunks(256)
                .into_iter()
                .map(|nums| {
                    let nums = nums.collect_vec();
                    let deque = deque.clone();
                    thread::spawn(move || {
                        for i in nums {
                            deque.push_back(i);
                        }
                    })
                })
                .collect::<Vec<_>>();
            ths.into_iter().for_each(|t| {
                t.join().unwrap();
            });
            let mut all_nums = HashSet::new();
            for _ in 0..num {
                all_nums.insert(deque.pop_back().unwrap());
            }
            assert_eq!(all_nums.len(), num);
            for i in 0..num {
                assert!(all_nums.contains(&i));
            }
        }

        #[test]
        pub fn multithread_pop_front() {
            let _ = env_logger::try_init();
            let num: usize = $num;
            let deque = Arc::new($list_init);
            for i in 0..num {
                deque.push_front(i);
            }
            let ths = (0..num)
                .chunks(256)
                .into_iter()
                .map(|nums| {
                    let deque = deque.clone();
                    let nums = nums.collect_vec();
                    thread::spawn(move || {
                        nums.into_iter()
                            .map(|_| deque.pop_front().unwrap())
                            .collect_vec()
                    })
                })
                .collect::<Vec<_>>();
            let mut all_nums = HashSet::new();
            ths.into_iter()
                .map(|t| t.join().unwrap().into_iter())
                .flatten()
                .for_each(|n| {
                    all_nums.insert(n);
                });
            assert_eq!(all_nums.len(), num);
            for i in 0..num {
                assert!(all_nums.contains(&i));
            }
            assert!(deque.pop_front().is_none());
            assert!(deque.pop_back().is_none());
        }

        #[test]
        pub fn multithread_pop_back() {
            let num: usize = $num;
            let deque = Arc::new($list_init);
            for i in 0..num {
                deque.push_front(i);
            }
            let ths = (0..num)
                .chunks(256)
                .into_iter()
                .map(|nums| {
                    let deque = deque.clone();
                    let nums = nums.collect_vec();
                    thread::spawn(move || {
                        nums.into_iter()
                            .map(|_| deque.pop_back().unwrap())
                            .collect_vec()
                    })
                })
                .collect::<Vec<_>>();
            let mut all_nums = HashSet::new();
            ths.into_iter()
                .map(|t| t.join().unwrap().into_iter())
                .flatten()
                .for_each(|n| {
                    all_nums.insert(n);
                });
            assert!(deque.pop_front().is_none());
            assert!(deque.pop_back().is_none());
            assert_eq!(all_nums.len(), num);
            for i in 0..num {
                assert!(all_nums.contains(&i));
            }
            deque.push_back(1);
            deque.push_back(2);
            deque.push_back(3);
            assert_eq!(deque.pop_back().unwrap(), 3);
            assert_eq!(deque.pop_back().unwrap(), 2);
            assert_eq!(deque.pop_back().unwrap(), 1);
            assert!(deque.pop_back().is_none());
        }

        #[test]
        pub fn multithread_push_front_and_back_single_thread_pop_front() {
            let num: usize = $num;
            let deque = Arc::new($list_init);
            let ths = (0..num)
                .chunks(256)
                .into_iter()
                .map(|nums| {
                    let nums = nums.collect_vec();
                    let deque = deque.clone();
                    thread::spawn(move || {
                        nums.into_iter().for_each(|i| {
                            if i % 2 == 0 {
                                deque.push_front(i);
                            } else {
                                deque.push_back(i);
                            }
                        });
                    })
                })
                .collect::<Vec<_>>();
            ths.into_iter().for_each(|t| {
                t.join().unwrap();
            });
            let mut all_nums = HashSet::new();
            for _ in 0..num {
                all_nums.insert(deque.pop_front().unwrap());
            }
            assert_eq!(all_nums.len(), num);
            for i in 0..num {
                assert!(all_nums.contains(&i));
            }
        }

        #[test]
        pub fn multithread_push_front_and_back_single_thread_pop_back() {
            let num: usize = $num;
            let deque = Arc::new($list_init);
            let ths = (0..num)
                .chunks(256)
                .into_iter()
                .map(|nums| {
                    let nums = nums.collect_vec();
                    let deque = deque.clone();
                    thread::spawn(move || {
                        nums.into_iter().for_each(|i| {
                            if i % 2 == 0 {
                                deque.push_front(i);
                            } else {
                                deque.push_back(i);
                            }
                        });
                    })
                })
                .collect::<Vec<_>>();
            ths.into_iter().for_each(|t| {
                t.join().unwrap();
            });
            let mut all_nums = HashSet::new();
            for _ in 0..num {
                all_nums.insert(deque.pop_back().unwrap());
            }
            assert_eq!(all_nums.len(), num);
            for i in 0..num {
                assert!(all_nums.contains(&i));
            }
        }

        #[test]
        pub fn multithread_pop_back_front() {
            let num: usize = $num;
            let deque = Arc::new($list_init);
            for i in 0..num {
                deque.push_front(i);
            }
            let ths = (0..num)
                .chunks(512)
                .into_iter()
                .map(|nums| {
                    let deque = deque.clone();
                    let nums = nums.collect_vec();
                    thread::spawn(move || {
                        nums.into_iter()
                            .map(|i| {
                                if i % 2 == 0 {
                                    deque.pop_front().unwrap()
                                } else {
                                    deque.pop_back().unwrap()
                                }
                            })
                            .collect_vec()
                    })
                })
                .collect::<Vec<_>>();
            let mut all_nums = HashSet::new();
            ths.into_iter()
                .map(|t| t.join().unwrap().into_iter())
                .flatten()
                .for_each(|n| {
                    all_nums.insert(n);
                });
            assert!(deque.pop_front().is_none());
            assert!(deque.pop_back().is_none());
            assert_eq!(all_nums.len(), num);
            for i in 0..num {
                assert!(all_nums.contains(&i));
            }
        }

        #[test]
        pub fn multithread_push_pop_front() {
            let num: usize = $num;
            let deque = Arc::new($list_init);
            let threshold = (num as f64 * 0.5) as usize;
            for i in 0..threshold {
                deque.push_front(i);
            }
            let ths = (threshold..num)
                .chunks(256)
                .into_iter()
                .map(|nums| {
                    let nums = nums.collect_vec();
                    let deque = deque.clone();
                    thread::spawn(move || {
                        nums.into_iter()
                            .map(|i| {
                                if i % 2 == 0 {
                                    deque.push_front(i);
                                    None
                                } else {
                                    Some(deque.pop_front().unwrap())
                                }
                            })
                            .collect_vec()
                    })
                })
                .collect::<Vec<_>>();
            let results = ths
                .into_iter()
                .map(|j| j.join().unwrap().into_iter())
                .flatten()
                .filter_map(|n| n)
                .collect::<Vec<_>>();
            let results_len = results.len();
            assert_eq!(results_len, (num - threshold) / 2);
            let set = results.into_iter().collect::<HashSet<_>>();
            assert_eq!(results_len, set.len());
        }

        #[test]
        pub fn multithread_push_pop_back() {
            let _ = env_logger::try_init();
            let num: usize = $num;
            let deque = Arc::new($list_init);
            let threshold = (num as f64 * 0.5) as usize;
            for i in 0..threshold {
                deque.push_back(i);
            }
            let ths = (threshold..num)
                .chunks(256)
                .into_iter()
                .map(|nums| {
                    let nums = nums.collect_vec();
                    let deque = deque.clone();
                    thread::spawn(move || {
                        nums.into_iter()
                            .map(|i| {
                                if i % 2 == 0 {
                                    deque.push_back(i);
                                    None
                                } else {
                                    Some(deque.pop_back().unwrap())
                                }
                            })
                            .collect_vec()
                    })
                })
                .collect::<Vec<_>>();
            let results = ths
                .into_iter()
                .map(|j| j.join().unwrap().into_iter())
                .flatten()
                .filter_map(|n| n)
                .collect::<Vec<_>>();
            let results_len = results.len();
            assert_eq!(results_len, (num - threshold) / 2);
            let set = results.into_iter().collect::<HashSet<_>>();
            assert_eq!(results_len, set.len());
        }
    };
}
