use bustle::*;
use chrono::prelude::*;
use clap::{App, Arg};
use perfcnt::linux::{HardwareEventType as Hardware, SoftwareEventType as Software};
use perfcnt_bench::PerfCounters;
use std::env;
use std::fs::File;
use std::io::*;

mod arc_mutex_std;
mod arc_rwlock_std;
mod chashmap;
mod cht;
mod lfmap;

fn main() {
    const RUNTIME: &'static str = "runtime";
    const CONTENTION: &'static str = "CONTENTION";
    const STRIDE: &'static str = "STRIDE";
    const LOAD: &'static str = "LOAD";
    const DATA_STRUCTURE: &'static str = "DATA_STRUCTURE";
    const FILE: &'static str = "FILE";
    tracing_subscriber::fmt::init();
    let matches = App::new("Lightning benches")
        .version("0.1")
        .author("Hao Shi <haoshi@umass.edu>")
        .subcommand(
            App::new("runtime")
                .about("Measure runtime of the data structures")
                .arg(
                    Arg::new(CONTENTION)
                        .short('c')
                        .long("contention")
                        .about("Sets whether to run benchmarks under different contentions")
                )
                .arg(
                    Arg::new(LOAD)
                        .short('l')
                        .long("long")
                        .value_name(LOAD)
                        .about("Sets the load factor of the benchamrk, default 28")
                        .default_value("25"),
                )
                .arg(
                    Arg::new(STRIDE)
                        .short('s')
                        .long("stride")
                        .value_name(STRIDE)
                        .about("Sets the stride of the benchamrk, default 4")
                        .default_value("4"),
                ),
        )
        .subcommand(
            App::new("perfcnt")
                .about("Run the benchmarks with performance counters")
                .arg(
                    Arg::new(DATA_STRUCTURE)
                        .short('d')
                        .long("data-structure")
                        .value_name(DATA_STRUCTURE)
                        .about("Define the data structure want to observe")
                        .required(true),
                ),
        )
        .arg(
            Arg::new(FILE)
                .short('f')
                .long("file")
                .value_name(FILE)
                .about("Sets the output file name for reports")
                .required(true),
        )
        .get_matches();
    let file_name = matches.value_of(FILE).unwrap().to_string();
    if let Some(cache_settings) = matches.subcommand_matches("perfcnt") {
        let ds = cache_settings.value_of(DATA_STRUCTURE).unwrap().to_string();
        cache_behavior(&file_name, &ds);
    } else if let Some(rt_settings) = matches.subcommand_matches("runtime") {
        let contention = rt_settings.is_present(CONTENTION);
        let load = rt_settings.value_of(LOAD).unwrap().parse().unwrap();
        let stride = rt_settings.value_of(STRIDE).unwrap().parse().unwrap();
        perf_test(&file_name, load, contention, stride);
    }
}

fn cache_behavior<'a>(file_name: &'a str, ds_arg: &'a str) {
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
        run_cache_bench(
            workload,
            data,
            prefilled,
            &format!("{}_lock-free-cache.csv", file_name),
        );
    } else if ds_arg == "c" {
        let prefilled = workload.prefill::<chashmap::Table>(&data);
        run_cache_bench(
            workload,
            data,
            prefilled,
            &format!("{}_chashmap-cache.csv", file_name),
        );
    } else if ds_arg == "m" {
        let prefilled = workload.prefill::<arc_mutex_std::Table>(&data);
        run_cache_bench(
            workload,
            data,
            prefilled,
            &format!("{}_mutex-cache.csv", file_name),
        );
    } else if ds_arg == "rw" {
        let prefilled = workload.prefill::<arc_rwlock_std::Table>(&data);
        run_cache_bench(
            workload,
            data,
            prefilled,
            &format!("{}_rw-lock-cache.csv", file_name),
        );
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
    println!("Running benchamrk for cache behaviors");
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

fn perf_test<'a>(file_name: &'a str, load: u8, contention: bool, stride: usize) {
    run_perf_test_set::<lfmap::TestTable>(file_name, "lf-map", load, contention, stride);
    run_perf_test_set::<cht::Table>(file_name, "cht", load, contention, stride);
    run_perf_test_set::<arc_rwlock_std::Table>(file_name, "rw", load, contention, stride);
    run_perf_test_set::<arc_mutex_std::Table>(file_name, "mutex", load, contention, stride);
}

fn run_perf_test_set<'a, T: Collection>(
    file_name: &'a str,
    ds_name: &'static str,
    load: u8,
    contention: bool,
    stride: usize,
) {
    println!("Testing perf with contention {}", contention);
    if contention {
        run_and_record_contention::<lfmap::TestTable>(
            file_name,
            &format!("{}_{}_full", file_name, ds_name),
            load,
            1.0,
            stride,
        );
        run_and_record_contention::<lfmap::TestTable>(
            file_name,
            &format!("{}_{}_hi", file_name, ds_name),
            load,
            0.8,
            stride,
        );
        run_and_record_contention::<lfmap::TestTable>(
            file_name,
            &format!("{}_{}_mi", file_name, ds_name),
            load,
            0.5,
            stride,
        );
        run_and_record_contention::<lfmap::TestTable>(
            file_name,
            &format!("{}_{}_lo", file_name, ds_name),
            load,
            0.2,
            stride,
        );
    } else {
        run_and_record_contention::<lfmap::TestTable>(
            file_name,
            &format!("{}_{}", file_name, ds_name),
            load,
            0.001,
            stride,
        );
    }
}

fn run_and_record_contention<'a, 'b, T: Collection>(
    task: &'b str,
    name: &'a str,
    load: u8,
    cont: f64,
    stride: usize,
) {
    println!("Testing {}", name);

    println!("Insert heavy");
    let insert_measure_75 = run_and_measure_mix::<T>(Mix::insert_heavy(), 0.75, load, cont, stride);
    write_measures(
        &format!("{}_{}_75_insertion.csv", task, name),
        &insert_measure_75,
    );

    // let insert_measure_150 = run_and_measure_mix::<T>(Mix::insert_heavy(), 1.5, 28, cont);
    // write_measures(&format!("{}_{}_150_insertion.csv", task, name), &insert_measure_150);

    println!("Read heavy");
    let read_measure_75 = run_and_measure_mix::<T>(Mix::read_heavy(), 0.75, load, cont, stride);
    write_measures(&format!("{}_{}_75_read.csv", task, name), &read_measure_75);

    // let read_measure_150 = run_and_measure_mix::<T>(Mix::read_heavy(), 55.0, 25, cont);
    // write_measures(&format!("{}_{}_150_read.csv", task, name), &read_measure_150);

    println!("Uniform");
    let uniform_measure_75 = run_and_measure_mix::<T>(Mix::uniform(), 0.75, load, cont, stride);
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
    stride: usize,
) -> Vec<(usize, Measurement)> {
    let steps = 4;
    let mut threads = (steps..=num_cpus::get())
        .step_by(stride)
        .collect::<Vec<_>>();
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
        )
        .unwrap();
    }
    file.flush().unwrap();
    println!("Measurements logged at {}", name);
}
