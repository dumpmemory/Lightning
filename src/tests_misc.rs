use std::thread::JoinHandle;
use itertools::Itertools;

pub fn assert_all_thread_passed<R>(threads: Vec<JoinHandle<R>>) {
  let all_errors = threads
      .into_iter()
      .filter_map(|r| r.join().err())
      .collect::<Vec<_>>();
  if !all_errors.is_empty() {
      panic!(
          "Multithreaded test error with \n {}", 
          all_errors.into_iter().map(|e| format!("{:?}", e)).join("\n************\n")
      );
  }
}