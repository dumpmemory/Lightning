use bustle::{Workload, Mix};

mod lfmap;
mod arc_mutex_std;
mod arc_rwlock_std;
mod chashmap;

fn main() {
    tracing_subscriber::fmt::init();
    Workload::new(128, Mix::insert_heavy()).run::<lfmap::TestTable>();
}

fn test_lfmap() {
    println!("Testing lock-free map");
    println!("Insert heavy");
    for n in 1..=num_cpus::get() {
        Workload::new(n, Mix::insert_heavy()).run::<lfmap::TestTable>();
    }
    println!("Read heavy");
    for n in 1..=num_cpus::get() {
        Workload::new(n, Mix::read_heavy()).run::<lfmap::TestTable>();
    }
    println!("Uniform");
    for n in 1..=num_cpus::get() {
        Workload::new(n, Mix::uniform()).run::<lfmap::TestTable>();
    }
}

fn test_rwlock_std() {
    println!("Testing rwlock std map");
    println!("Insert heavy");
    for n in 1..=num_cpus::get() {
        Workload::new(n, Mix::insert_heavy()).run::<arc_rwlock_std::Table<u64>>();
    }
    println!("Read heavy");
    for n in 1..=num_cpus::get() {
        Workload::new(n, Mix::read_heavy()).run::<arc_rwlock_std::Table<u64>>();
    }
    println!("Uniform");
    for n in 1..=num_cpus::get() {
        Workload::new(n, Mix::uniform()).run::<arc_rwlock_std::Table<u64>>();
    }
}

fn test_mutex_std() {
    println!("Testing mutex std map");
    println!("Insert heavy");
    for n in 1..=num_cpus::get() {
        Workload::new(n, Mix::insert_heavy()).run::<arc_mutex_std::Table<u64>>();
    }
    println!("Read heavy");
    for n in 1..=num_cpus::get() {
        Workload::new(n, Mix::read_heavy()).run::<arc_mutex_std::Table<u64>>();
    }
    println!("Uniform");
    for n in 1..=num_cpus::get() {
        Workload::new(n, Mix::uniform()).run::<arc_mutex_std::Table<u64>>();
    }
}

fn test_chashmap() {
    println!("Testing CHashmap");
    println!("Insert heavy");
    for n in 1..=num_cpus::get() {
        Workload::new(n, Mix::insert_heavy()).run::<chashmap::Table<u64>>();
    }
    println!("Read heavy");
    for n in 1..=num_cpus::get() {
        Workload::new(n, Mix::read_heavy()).run::<chashmap::Table<u64>>();
    }
    println!("Uniform");
    for n in 1..=num_cpus::get() {
        Workload::new(n, Mix::uniform()).run::<chashmap::Table<u64>>();
    }
}