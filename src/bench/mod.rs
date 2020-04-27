use bustle::{Workload, Mix};

mod lfmap;

fn main() {
    tracing_subscriber::fmt::init();
    Workload::new(128, Mix::read_heavy()).run::<lfmap::TestTable>();
    // for n in 1..=num_cpus::get() {
    //     Workload::new(n, Mix::read_heavy()).run::<lfmap::TestTable>();
    // }
}