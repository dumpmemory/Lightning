use bustle::*;
use chrono::prelude::*;
use perfcnt::linux::{HardwareEventType as Hardware, SoftwareEventType as Software};
use perfcnt_bench::PerfCounters;
use std::env;
use std::fmt::Debug;
use std::fs::File;
use std::io::*;
use std::time::SystemTime;
mod arc_mutex_std;
mod arc_rwlock_std;
mod chashmap;
mod lfmap;
mod cht;

fn main() {
    tracing_subscriber::fmt::init();
    // Workload::new(8, Mix::insert_heavy()).run::<lfmap::TestTable>();
    // Workload::new(16, Mix::uniform()).run::<lfmap::TestTable>();
    // Workload::new(126, Mix::read_heavy()).run::<lfmap::TestTable>();
    // Workload::new(1, Mix::uniform()).run::<lfmap::TestTable>();
    // perf_test()
    cache_behavior()
}

fn cache_behavior() {
    let args: Vec<String> = env::args().collect();
    let ds_arg = &args[1];
    let n = 128;
    let mix = Mix::uniform();
    let fill = 0.75;
    let cap = 30;
    let cont = 0.1;
    let mut workload = Workload::new(n, mix);
    let data = workload
        .operations(fill)
        .contention(cont)
        .initial_capacity_log2(cap)
        .gen_data();

    if ds_arg == "l" {
        let prefilled = workload.prefill::<lfmap::TestTable>(&data);
        run_cache_bench(workload, data, prefilled, "lock-free-cache.csv");
    } else if ds_arg == "c" {
        let prefilled = workload.prefill::<chashmap::Table>(&data);
        run_cache_bench(workload, data, prefilled, "chashmap-cache.csv");
    } else if ds_arg == "m" {
        let prefilled = workload.prefill::<arc_mutex_std::Table>(&data);
        run_cache_bench(workload, data, prefilled, "mutex-cache.csv");
    } else if ds_arg == "rw" {
        let prefilled = workload.prefill::<arc_rwlock_std::Table>(&data);
        run_cache_bench(workload, data, prefilled, "rw-lock-cache.csv");
    } else {
        panic!();
    }
}

fn run_cache_bench<'a, T: Collection>(
    workload: Workload,
    data: WorkloadData,
    prefilled: PrefilledData<T>,
    report: &'a str,
) {
    let mut pc = PerfCounters::for_this_process();
    pc.with_all_mem_cache_events()
        .with_all_branch_prediction_events()
        .with_all_tlb_cache_events()
        .with_hardware_events(vec![
            Hardware::CPUCycles,
            Hardware::Instructions,
            Hardware::CacheReferences,
            Hardware::CacheMisses,
            Hardware::BranchInstructions,
            Hardware::BranchMisses,
            Hardware::BusCycles,
            Hardware::StalledCyclesFrontend,
            Hardware::StalledCyclesBackend,
            Hardware::RefCPUCycles,
        ])
        .with_software_events(vec![
            Software::CpuClock,
            Software::TaskClock,
            Software::PageFaults,
            Software::ContextSwitches,
            Software::CpuMigrations,
            Software::PageFaultsMin,
            Software::PageFaultsMaj,
        ]);
    pc.bench(move || workload.run_against(data, prefilled));
    pc.save_result(report).unwrap();
}

fn perf_test() {
    test_lfmap();
    test_cht();
    test_chashmap();
    test_rwlock_std();
    test_mutex_std();
}

fn get_task_name() -> String {
    let args: Vec<String> = env::args().collect();
    args[1].clone()
}

fn test_lfmap() {
    //run_and_record::<lfmap::TestTable>(&get_task_name(), "lock-free-map", 0.0);
    run_and_record_contention::<lfmap::TestTable>(&get_task_name(), "lock-free-map-full", 1.0);
    run_and_record_contention::<lfmap::TestTable>(&get_task_name(), "lock-free-map-hi", 0.8);
    run_and_record_contention::<lfmap::TestTable>(&get_task_name(), "lock-free-map-mi", 0.5);
    run_and_record_contention::<lfmap::TestTable>(&get_task_name(), "lock-free-map-lo", 0.2);
}

fn test_cht() {
    //run_and_record::<lfmap::TestTable>(&get_task_name(), "lock-free-map", 0.0);
    run_and_record_contention::<cht::Table>(&get_task_name(), "cht-full", 1.0);
    run_and_record_contention::<cht::Table>(&get_task_name(), "cht-hi", 0.8);
    run_and_record_contention::<cht::Table>(&get_task_name(), "cht-mi", 0.5);
    run_and_record_contention::<cht::Table>(&get_task_name(), "cht-lo", 0.2);
}

fn test_rwlock_std() {
    //run_and_record::<arc_rwlock_std::Table>(&get_task_name(), "rwlock-std-map", 0.0);
    run_and_record_contention::<arc_rwlock_std::Table>(
        &get_task_name(),
        "rwlock-std-map-full",
        1.0,
    );
    run_and_record_contention::<arc_rwlock_std::Table>(&get_task_name(), "rwlock-std-map-hi", 0.8);
    run_and_record_contention::<arc_rwlock_std::Table>(&get_task_name(), "rwlock-std-map-mi", 0.5);
    run_and_record_contention::<arc_rwlock_std::Table>(&get_task_name(), "rwlock-std-map-lo", 0.2);
}

fn test_mutex_std() {
    //run_and_record::<arc_mutex_std::Table>(&get_task_name(), "mutex-std-map", 0.0);
    run_and_record_contention::<arc_mutex_std::Table>(&get_task_name(), "mutex-std-map-full", 1.0);
    run_and_record_contention::<arc_mutex_std::Table>(&get_task_name(), "mutex-std-map-hi", 0.8);
    run_and_record_contention::<arc_mutex_std::Table>(&get_task_name(), "mutex-std-map-mi", 0.5);
    run_and_record_contention::<arc_mutex_std::Table>(&get_task_name(), "mutex-std-map-lo", 0.2);
}

fn test_chashmap() {
    //run_and_record::<chashmap::Table>(&get_task_name(), "CHashmap", 0.0);
    run_and_record_contention::<chashmap::Table>(&get_task_name(), "CHashmap-full", 1.0);
    run_and_record_contention::<chashmap::Table>(&get_task_name(), "CHashmap-hi", 0.8);
    run_and_record_contention::<chashmap::Table>(&get_task_name(), "CHashmap-mi", 0.5);
    run_and_record_contention::<chashmap::Table>(&get_task_name(), "CHashmap-lo", 0.2);
}

fn run_and_record_contention<'a, 'b, T: Collection>(task: &'b str, name: &'a str, cont: f64) {
    println!("Testing {}", name);

    println!("Insert heavy");
    let insert_measure_75 = run_and_measure_mix::<T>(Mix::insert_heavy(), 0.75, 26, cont);
    write_measures(
        &format!("{}_{}_75_insertion.csv", task, name),
        &insert_measure_75,
    );

    // let insert_measure_150 = run_and_measure_mix::<T>(Mix::insert_heavy(), 1.5, 28, cont);
    // write_measures(&format!("{}_{}_150_insertion.csv", task, name), &insert_measure_150);

    println!("Read heavy");
    let read_measure_75 = run_and_measure_mix::<T>(Mix::read_heavy(), 0.75, 26, cont);
    write_measures(&format!("{}_{}_75_read.csv", task, name), &read_measure_75);

    // let read_measure_150 = run_and_measure_mix::<T>(Mix::read_heavy(), 55.0, 25, cont);
    // write_measures(&format!("{}_{}_150_read.csv", task, name), &read_measure_150);

    println!("Uniform");
    let uniform_measure_75 = run_and_measure_mix::<T>(Mix::uniform(), 0.75, 26, cont);
    write_measures(
        &format!("{}_{}_75_uniform.csv", task, name),
        &uniform_measure_75,
    );

    // let uniform_measure_150 = run_and_measure_mix::<T>(Mix::uniform(), 6.0, 28, cont);
    // write_measures(&format!("{}_{}_150_uniform.csv", task, name), &uniform_measure_150);
}

fn run_and_measure_mix<T: Collection>(
    mix: Mix,
    fill: f64,
    cap: u8,
    cont: f64,
) -> Vec<(usize, Measurement)> {
    let steps = 4;
    let mut threads = (steps..=num_cpus::get()).step_by(steps).collect::<Vec<_>>();
    threads.insert(0, 1);
    threads
        .into_iter()
        .map(|n| {
            let m = run_and_measure::<T>(n, mix, fill, cap, cont);
            let local: DateTime<Local> = Local::now();
            let time = local.format("%Y-%m-%d %H:%M:%S").to_string();
            println!(
                "[{}] Completed with threads {}, range {}, ops {}, spent {:?}, throughput {}, latency {:?}",
                time, n, m.key_range, m.total_ops, m.spent, m.throughput, m.latency
            );
            (n, m)
        })
        .collect()
}

fn run_and_measure<T: Collection>(
    threads: usize,
    mix: Mix,
    fill: f64,
    cap: u8,
    cont: f64,
) -> Measurement {
    let mut workload = Workload::new(threads, mix);
    workload
        .operations(fill)
        .contention(cont)
        .initial_capacity_log2(cap)
        .run_silently::<T>()
}

fn write_measures<'a>(name: &'a str, measures: &[(usize, Measurement)]) {
    let current_dir = env::current_dir().unwrap();
    let file = File::create(current_dir.join(name)).unwrap();
    let mut file = LineWriter::new(file);
    for (n, m) in measures.iter() {
        let spent = m.spent.as_nanos();
        let total_ops = m.total_ops;
        let real_latency = (spent as f64) / (total_ops as f64);
        file.write_all(
            format!(
                "{}\t{}\t{}\t{}\t{}\t{}\n",
                n,
                total_ops,
                spent,
                m.throughput,
                real_latency,
                m.latency.as_nanos()
            )
            .as_bytes(),
        );
    }
    file.flush().unwrap();
    println!("Measurements logged at {}", name);
}
