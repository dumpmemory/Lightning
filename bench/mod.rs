use bustle::*;
use chrono::prelude::*;
use clap::{App, Arg};
use humansize::{file_size_opts as options, FileSize};
use ipc_channel::ipc::{IpcOneShotServer, IpcSender};
use libc::{c_int, sysconf};
use perfcnt::linux::{HardwareEventType as Hardware, SoftwareEventType as Software};
use perfcnt_bench::PerfCounters;
#[cfg(target_os = "linux")]
use procinfo::pid::{stat, stat_self};
use std::fs::File;
use std::sync::mpsc::channel;
use std::time::Duration;
use std::{env, io::*, thread};

use crate::plot::draw_perf_plots;

mod arc_mutex_std;
mod arc_rwlock_std;
mod chashmap;
mod cht;
mod contrie;
mod dashmap;
mod fat_lfmap;
mod flurry;
mod lfmap;
mod lite_lfmap;
mod lite_lfmap_arc;
mod lockfree;
mod obj_lfmap;
mod ptr_lfmap;
mod scc;

mod plot;

fn main() {
    const RUNTIME: &'static str = "runtime";
    const CONTENTION: &'static str = "CONTENTION";
    const STRIDE: &'static str = "STRIDE";
    const LOAD: &'static str = "LOAD";
    const DATA_STRUCTURE: &'static str = "DATA_STRUCTURE";
    const THREADS: &'static str = "THREADS";
    const WORKLOAD: &'static str = "WORKLOAD";
    const FILE: &'static str = "FILE";
    tracing_subscriber::fmt::init();
    let matches = App::new("Lightning benches")
        .version("0.1")
        .author("Hao Shi <haoshi@umass.edu>")
        .subcommand(
            App::new("runtime")
                .about("Measure runtime of the data structures")
                .arg(
                    Arg::new(CONTENTION).short('c').long("contention"), // .about("Sets whether to run benchmarks under different contentions"),
                )
                .arg(
                    Arg::new(LOAD)
                        .short('l')
                        .long("load")
                        .value_name(LOAD)
                        // .about("Sets the load factor of the benchamrk, default 26")
                        .default_value("26"),
                )
                .arg(
                    Arg::new(STRIDE)
                        .short('s')
                        .long("stride")
                        .value_name(STRIDE)
                        // .about("Sets the stride of the benchamrk, default 4")
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
                        // .about("Define the data structure want to observe")
                        .required(true),
                ),
        )
        .subcommand(
            App::new("case") // comparison benchmark
                .about("Run the benchmark item and record the result to file")
                .arg(
                    Arg::new(DATA_STRUCTURE)
                        .short('d')
                        .long("data-structure")
                        .value_name(DATA_STRUCTURE)
                        // .about("Specify the data structure want to test")
                        .required(true),
                )
                .arg(
                    Arg::new(THREADS)
                        .short('t')
                        .long("threads")
                        .value_name(THREADS)
                        // .about("Specify the threads to run")
                        .required(true),
                )
                .arg(
                    Arg::new(LOAD)
                        .short('l')
                        .long("load")
                        .value_name(LOAD)
                        // .about("Load factor of the hashmap")
                        .required(true),
                )
                .arg(
                    Arg::new(CONTENTION)
                        .short('c')
                        .long("contention")
                        .value_name(CONTENTION)
                        // .about("Contention factor of the test")
                        .required(true),
                )
                .arg(
                    Arg::new(WORKLOAD)
                        .short('w')
                        .long("workload")
                        .value_name(WORKLOAD)
                        // .about("Worload type of the test")
                        .required(true),
                ),
        )
        .arg(
            Arg::new(FILE)
                .short('f')
                .long("file")
                .value_name(FILE)
                // .about("Sets the output file name for reports")
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
    } else if let Some(case_settings) = matches.subcommand_matches("case") {
        let data_structure = case_settings.value_of(DATA_STRUCTURE).unwrap().to_string();
        let threads = case_settings.value_of(THREADS).unwrap().to_string();
        let load = case_settings.value_of(LOAD).unwrap().to_string();
        let contention = case_settings.value_of(CONTENTION).unwrap().to_string();
        let worload = case_settings.value_of(WORKLOAD).unwrap().to_string();
        unimplemented!("Not available");
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
    } else if ds_arg == "trie" {
        let prefilled = workload.prefill::<contrie::Table>(&data);
        run_cache_bench(
            workload,
            data,
            prefilled,
            &format!("{}_trie-lock-cache.csv", file_name),
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

pub type PerfPlotData = Vec<(
    &'static str,
    Vec<(
        &'static str,
        Vec<(&'static str, Vec<(usize, Option<Measurement>, usize)>)>,
    )>,
)>;

fn perf_test<'a>(file_name: &'a str, load: u8, contention: bool, stride: usize) {
    let data = vec![
        run_perf_test_set::<ptr_lfmap::TestTable>(
            file_name,
            "lightning - ptr",
            load,
            contention,
            stride,
        ),
        run_perf_test_set::<lite_lfmap::TestTable>(
            file_name,
            "lightning - lite",
            load,
            contention,
            stride,
        ),
        run_perf_test_set::<obj_lfmap::TestTable>(
            file_name,
            "lightning - obj",
            load,
            contention,
            stride,
        ),
        run_perf_test_set::<fat_lfmap::TestTable>(
            file_name,
            "lightning - lock",
            load,
            contention,
            stride,
        ),
        run_perf_test_set::<lite_lfmap_arc::TestTable>(
            file_name,
            "lightning - arc",
            load,
            contention,
            stride,
        ),
        run_perf_test_set::<lfmap::TestTable>(
            file_name,
            "lightning - base",
            load,
            contention,
            stride,
        ),
        run_perf_test_set::<cht::Table>(file_name, "cht", load, contention, stride), // Potential OOM
        run_perf_test_set::<scc::Table>(file_name, "scc::HashMap", load, contention, stride),
        run_perf_test_set::<contrie::Table>(file_name, "contrie", load, contention, stride),
        run_perf_test_set::<dashmap::Table>(file_name, "dashmap", load, contention, stride),
        run_perf_test_set::<flurry::Table>(file_name, "flurry", load, contention, stride),
        run_perf_test_set::<chashmap::Table>(file_name, "chashmap", load, contention, stride),
        run_perf_test_set::<lockfree::Table>(file_name, "lockfree::map", load, contention, stride), // Too slow
        run_perf_test_set::<arc_rwlock_std::Table>(file_name, "rw", load, contention, stride),
        run_perf_test_set::<arc_mutex_std::Table>(file_name, "mutex", load, contention, stride),
    ];
    draw_perf_plots(data);
    println!("PERF TEST COMPLETED...");
}

fn run_perf_test_set<'a, T: Collection>(
    file_name: &'a str,
    ds_name: &'static str,
    load: u8,
    contention: bool,
    stride: usize,
) -> (
    &'static str,
    Vec<(
        &'static str,
        Vec<(&'static str, Vec<(usize, Option<Measurement>, usize)>)>,
    )>,
) {
    println!("Testing perf with contention {}", contention);
    if contention {
        // let full = run_and_record_contention::<T>(
        //     file_name,
        //     &format!("{}_full", ds_name),
        //     load,
        //     1.0,
        //     stride,
        // );
        let lo = run_and_record_contention::<T>(
            file_name,
            &format!("{}_lo", ds_name),
            load,
            0.2,
            stride,
        );
        // let hi = run_and_record_contention::<T>(
        //     file_name,
        //     &format!("{}_hi", ds_name),
        //     load,
        //     0.8,
        //     stride,
        // );
        // let mi = run_and_record_contention::<T>(
        //     file_name,
        //     &format!("{}_mi", ds_name),
        //     load,
        //     0.5,
        //     stride,
        // );
        (
            ds_name,
            vec![
                // ("full", full),
                ("lo", lo),
                // ("mi", mi),
                // ("hi", hi),
            ],
        )
    } else {
        let data = run_and_record_contention::<T>(
            file_name,
            &format!("{}_{}", file_name, ds_name),
            load,
            0.001,
            stride,
        );
        (ds_name, vec![("*", data)])
    }
}

fn run_and_record_contention<'a, 'b, T: Collection>(
    task: &'b str,
    name: &'a str,
    load: u8,
    cont: f64,
    stride: usize,
) -> Vec<(&'static str, Vec<(usize, Option<Measurement>, usize)>)> {
    println!("Testing {}", name);
    // println!("Read heavy");
    // let read_measurement = run_and_measure_mix::<T>(Mix::read_heavy(), 0.75, load, cont, stride);
    // write_measurements(&format!("{}_{}_read.csv", task, name), &read_measurement);

    // println!("Insert heavy");
    // let insert_measurement =
    //     run_and_measure_mix::<T>(Mix::insert_heavy(), 0.75, load, cont, stride);
    // write_measurements(
    //     &format!("{}_{}_insertion.csv", task, name),
    //     &insert_measurement,
    // );
    // println!("Uniform");
    // let uniform_measurement = run_and_measure_mix::<T>(Mix::uniform(), 0.75, load, cont, stride);
    // write_measurements(
    //     &format!("{}_{}_uniform.csv", task, name),
    //     &uniform_measurement,
    // );
    println!("Oversize");
    let oversize_measurement =
        run_and_measure_mix::<T>(Mix::insert_heavy(), 1.0, load, 0.0, stride);
    write_measurements(
        &format!("{}_{}_oversize.csv", task, name),
        &oversize_measurement,
    );
    vec![
        // ("insert", insert_measurement),
        // ("read", read_measurement),
        // ("uniform", uniform_measurement),
        ("oversize", oversize_measurement),
    ]
}

fn run_and_measure_mix<T: Collection>(
    mix: Mix,
    fill: f64,
    cap: u8,
    cont: f64,
    stride: usize,
) -> Vec<(usize, Option<Measurement>, usize)> {
    let page_size = unsafe { sysconf(libc::_SC_PAGESIZE) } as usize;
    let steps = 4;
    let mut threads = (steps..=num_cpus::get())
        .step_by(stride)
        .collect::<Vec<_>>();
    threads.insert(0, 1);
    threads
        .into_iter()
        .map(|n| {
            #[cfg(all(not(unsafe_bench), target_os = "linux"))]
            let (mem_sender, mem_recv) = channel();
            let mut workload = Workload::new(n, mix);
            workload
                .operations(fill)
                .contention(cont)
                .initial_capacity_log2(cap);
            let data = workload.gen_data();
            let (server, server_name) : (IpcOneShotServer<Measurement>, String) = IpcOneShotServer::new().unwrap();
            #[cfg(all(not(unsafe_bench), target_os = "linux"))]
            let self_mem = stat_self().unwrap().rss;
            let child_pid = unsafe {
                fork(|| {
                    let tx = IpcSender::connect(server_name).unwrap();
                    let prefilled = workload.prefill::<T>(&data);
                    let m = workload.run_against(data, prefilled);
                    tx.send(m).unwrap();
                })
            };
            let mut proc_stat: i32 = 0;
            #[cfg(all(not(unsafe_bench), target_os = "linux"))]
            {
                thread::spawn(move || {
                    let mut max = 0;
                    while let Ok(memstat) = stat(child_pid) {
                        let size = memstat.rss;
                        if size > max {
                            max = size;
                        }
                        thread::sleep(Duration::from_millis(200));
                    }
                    mem_sender.send(max).unwrap();
                });
            }
            let proc_res = unsafe {
                libc::wait(&mut proc_stat as *mut c_int)
            };
            let local: DateTime<Local> = Local::now();
            assert_eq!(proc_res, child_pid);
            let mut calibrated_size = 0;
            let mut size = "".to_string();
            #[cfg(all(not(unsafe_bench), target_os = "linux"))]
            {
                let max_mem = mem_recv.recv().unwrap();
                calibrated_size = if max_mem < self_mem { 0 } else { max_mem - self_mem } * page_size;
                size = calibrated_size.file_size(options::CONVENTIONAL).unwrap();
            }
            let time = local.format("%Y-%m-%d %H:%M:%S").to_string();
            if proc_stat == 0 {
                let (_, m) = server.accept().unwrap();
                println!(
                    "[{}] Completed with threads {}, range {:.4}, ops {}, spent {:?}, throughput {}, latency {:?}, mem {}",
                    time, n, m.key_range, m.total_ops, m.spent, m.throughput, m.latency, size
                );
                (n, Some(m), calibrated_size)
            } else {
                println!("[{}] Failed with threads {}, stat code {}, mem {}", time, n, proc_stat, size);
                (n, None, calibrated_size)
            }

        })
        .collect()
}

fn write_measurements<'a>(name: &'a str, measures: &[(usize, Option<Measurement>, usize)]) {
    let current_dir = env::current_dir().unwrap();
    let file = File::create(current_dir.join(name)).unwrap();
    let mut file = LineWriter::new(file);
    for (n, m_opt, mem) in measures.iter() {
        if let &Some(ref m) = m_opt {
            let spent = m.spent.as_nanos();
            let total_ops = m.total_ops;
            let real_latency = (spent as f64) / (total_ops as f64);
            file.write_all(
                format!(
                    "{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
                    n,
                    total_ops,
                    spent,
                    m.throughput,
                    real_latency,
                    m.latency.as_nanos(),
                    mem
                )
                .as_bytes(),
            )
            .unwrap();
        } else {
            let NA = "NA";
            file.write_all(format!("{}\tNA\tNA\tNA\tNA\tNA\t{}\n", n, mem).as_bytes())
                .unwrap();
        }
    }
    file.flush().unwrap();
    println!("Measurements logged at {}", name);
}

pub unsafe fn fork<F: FnOnce()>(child_func: F) -> libc::pid_t {
    match libc::fork() {
        -1 => panic!("Fork failed: {}", Error::last_os_error()),
        0 => {
            child_func();
            libc::exit(0);
        }
        pid => pid,
    }
}

#[cfg(test)]
mod tests {
    use bustle::{Mix, Workload};

    use crate::{obj_lfmap, run_and_measure_mix};

    #[test]
    fn obj_map_oversize() {
        let mix = Mix::insert_heavy();
        let mut workload = Workload::new(1, mix);
        workload
            .operations(1.5)
            .contention(0.0)
            .initial_capacity_log2(26);
        let data = workload.gen_data();
        let prefilled = workload.prefill::<obj_lfmap::TestTable>(&data);
        let m = workload.run_against(data, prefilled);
        println!(
            "Completed with range {:.4}, ops {}, spent {:?}, throughput {}, latency {:?}",
            m.key_range, m.total_ops, m.spent, m.throughput, m.latency
        )
    }
}