use itertools::Itertools;
use std::thread::JoinHandle;

pub fn assert_all_thread_passed<R>(threads: Vec<JoinHandle<R>>) {
    let all_errors = threads
        .into_iter()
        .filter_map(|r| r.join().err())
        .collect::<Vec<_>>();
    if !all_errors.is_empty() {
        panic!(
            "Have {} multithreaded test error", all_errors.len()
        );
    }
}
