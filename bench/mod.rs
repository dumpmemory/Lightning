use bustle::*;
use std::fmt::Debug;
mod arc_mutex_std;
mod arc_rwlock_std;
mod chashmap;
mod lfmap;

fn main() {
    tracing_subscriber::fmt::init();
    // Workload::new(8, Mix::insert_heavy()).run::<lfmap::TestTable>();
    // Workload::new(16, Mix::uniform()).run::<lfmap::TestTable>();
    // Workload::new(126, Mix::read_heavy()).run::<lfmap::TestTable>();
    // Workload::new(1, Mix::uniform()).run::<lfmap::TestTable>();
    test_lfmap();
    test_chashmap();
    test_rwlock_std();
    test_mutex_std();
}

fn test_lfmap() {
    run_and_record::<lfmap::TestTable>("lock-free map");
}

fn test_rwlock_std() {
    run_and_record::<arc_rwlock_std::Table<u64>>("rwlock std map");
}

fn test_mutex_std() {
    run_and_record::<arc_mutex_std::Table<u64>>("mutex std map");
}

fn test_chashmap() {
    run_and_record::<chashmap::Table<u64>>("CHashmap");
}

fn run_and_record<'a, T: Collection>(name: &'a str)
where
    <T::Handle as CollectionHandle>::Key: Send + Debug,
{
    println!("Testing {}", name);
    println!("Insert heavy");
    let insert_measure = run_and_measure_mix::<T>(Mix::insert_heavy());
    println!("Read heavy");
    let read_measure = run_and_measure_mix::<T>(Mix::read_heavy());
    println!("Uniform");
    let uniform_measure = run_and_measure_mix::<T>(Mix::uniform());
}

fn run_and_measure_mix<T: Collection>(mix: Mix) -> Vec<Measurement>
where
    <T::Handle as CollectionHandle>::Key: Send + Debug,
{
    (1..=num_cpus::get())
        .map(|n| {
            let m = run_and_measure::<T>(n, mix);
            println!(
                "Completed with threads {}, ops {}, spent {:?}, throughput {}, latency {:?}",
                n, m.total_ops, m.spent, m.throughput, m.latency
            );
            m
        })
        .collect()
}

fn run_and_measure<T: Collection>(threads: usize, mix: Mix) -> Measurement
where
    <T::Handle as CollectionHandle>::Key: Send + Debug,
{
    let workload = Workload::new(threads, mix);
    workload.run_silently::<T>()
}
