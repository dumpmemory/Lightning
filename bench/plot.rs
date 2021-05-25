use plotters::prelude::*;
use super::PerfPlotData;
use std::collections::HashMap;

pub fn draw_perf_plots(data: PerfPlotData) {
  let mut data_sets = HashMap::new();
  for (ds, ds_data) in &data {
    for (cnt, cnt_data) in ds_data {
      for (work_load, work_load_data) in cnt_data {
        data_sets.entry((ds, cnt)).or_insert(vec![]).push((work_load, work_load_data));
        data_sets.entry((ds, work_load)).or_insert(vec![]).push((cnt, work_load_data));
        data_sets.entry((cnt, work_load)).or_insert(vec![]).push((ds, work_load_data));
      }
    }
  }
}