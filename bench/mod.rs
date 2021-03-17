use bustle::*;
use std::env;
use std::fmt::Debug;
use std::io::*;
use std::fs::File;
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

fn get_task_name() -> String {
    let args: Vec<String> = env::args().collect();
    args[1].clone()
}

fn test_lfmap() {
    let args: Vec<String> = env::args().collect();
    run_and_record::<lfmap::TestTable>(&get_task_name(), "lock-free-map");
}

fn test_rwlock_std() {
    let args: Vec<String> = env::args().collect();
    run_and_record::<arc_rwlock_std::Table<u64>>(&get_task_name(), "rwlock-std-map");
}

fn test_mutex_std() {
    let args: Vec<String> = env::args().collect();
    run_and_record::<arc_mutex_std::Table<u64>>(&get_task_name(), "mutex-std-map");
}

fn test_chashmap() {
    let args: Vec<String> = env::args().collect();
    run_and_record::<chashmap::Table<u64>>(&get_task_name(), "CHashmap");
}

fn run_and_record<'a, 'b, T: Collection>(task: &'b str, name: &'a str)
where
    <T::Handle as CollectionHandle>::Key: Send + Debug,
{
    println!("Testing {}", name);
    println!("Read heavy");

    let read_measure_75 = run_and_measure_mix::<T>(Mix::read_heavy(), 0.75);
    write_measures(&format!("{}_{}_75_read.csv", task, name), &read_measure_75);

    let read_measure_150 = run_and_measure_mix::<T>(Mix::read_heavy(), 21.0);
    write_measures(&format!("{}_{}_150_read.csv", task, name), &read_measure_150);


    println!("Insert heavy");
    let insert_measure_75 = run_and_measure_mix::<T>(Mix::insert_heavy(), 0.75);
    write_measures(&format!("{}_{}_75_insertion.csv", task, name), &insert_measure_75);

    let insert_measure_150 = run_and_measure_mix::<T>(Mix::insert_heavy(), 1.5);
    write_measures(&format!("{}_{}_150_insertion.csv", task, name), &insert_measure_150);


    println!("Uniform");
    let uniform_measure_75 = run_and_measure_mix::<T>(Mix::uniform(), 0.75);
    write_measures(&format!("{}_{}_75_uniform.csv", task, name), &uniform_measure_75);

    let uniform_measure_150 = run_and_measure_mix::<T>(Mix::uniform(), 6);
    write_measures(&format!("{}_{}_150_uniform.csv", task, name), &uniform_measure_150);
}

fn run_and_measure_mix<T: Collection>(mix: Mix, fill: f64) -> Vec<Measurement>
where
    <T::Handle as CollectionHandle>::Key: Send + Debug,
{
    (1..=num_cpus::get())
        .map(|n| {
            let m = run_and_measure::<T>(n, mix, fill);
            println!(
                "Completed with threads {}, ops {}, spent {:?}, throughput {}, latency {:?}",
                n, m.total_ops, m.spent, m.throughput, m.latency
            );
            m
        })
        .collect()
}

fn run_and_measure<T: Collection>(threads: usize, mix: Mix, fill: f64) -> Measurement
where
    <T::Handle as CollectionHandle>::Key: Send + Debug,
{
    let mut workload = Workload::new(threads, mix);
    workload.operations(fill);
    workload.run_silently::<T>()
}

fn write_measures<'a>(name: &'a str, measures: &[Measurement]) {
    let current_dir = env::current_dir().unwrap();
    let file = File::create(current_dir.join(name)).unwrap();
    let mut file = LineWriter::new(file);
    for (n, m) in measures.iter().enumerate() {
        let spent = m.spent.as_nanos();
        let total_ops = m.total_ops;
        let real_latency = (spent as f64) / (total_ops as f64);
        file.write_all(format!(
            "{}\t{}\t{}\t{}\t{}\t{}\n",
            n,
            total_ops,
            spent,
            m.throughput,
            real_latency,
            m.latency.as_nanos()
        ).as_bytes());
    }
    file.flush().unwrap();
    println!("Measurements logged at {}", name);
}
