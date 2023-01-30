use itertools::Itertools;
use std::sync::atomic::Ordering::*;
use std::sync::atomic::*;
use std::{panic, process, thread::JoinHandle};

lazy_static! {
    static ref HOOK_SET: AtomicBool = AtomicBool::new(false);
}

pub fn hook_panic() {
    if HOOK_SET
        .compare_exchange(false, true, AcqRel, Relaxed)
        .is_ok()
    {
        let orig_hook = std::panic::take_hook();
        panic::set_hook(Box::new(move |panic_info| {
            // invoke the default handler and exit the process
            if let Some(s) = panic_info.payload().downcast_ref::<&str>() {
                println!("panic occurred: {s:?}");
            } else {
                println!("panic occurred");
            }
            orig_hook(panic_info);
            process::exit(1);
        }));
        println!("Panic hooked");
    }
}

pub fn assert_all_thread_passed<R>(threads: Vec<JoinHandle<R>>) {
    let all_errors = threads
        .into_iter()
        .filter_map(|r| r.join().err())
        .collect::<Vec<_>>();
    if !all_errors.is_empty() {
        panic!("Have {} multithreaded test error", all_errors.len());
    }
}
