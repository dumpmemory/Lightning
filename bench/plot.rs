use super::PerfPlotData;
use bustle::*;
use plotters::prelude::*;
use std::collections::HashMap;

pub fn draw_perf_plots(data: PerfPlotData) {
    let mut data_sets = HashMap::new();
    for (ds, ds_data) in &data {
        for (cnt, cnt_data) in ds_data {
            for (work_load, work_load_data) in cnt_data {
                data_sets
                    .entry((ds, cnt))
                    .or_insert(vec![])
                    .push((*work_load, work_load_data));
                data_sets
                    .entry((ds, work_load))
                    .or_insert(vec![])
                    .push((*cnt, work_load_data));
                data_sets
                    .entry((cnt, work_load))
                    .or_insert(vec![])
                    .push((*ds, work_load_data));
            }
        }
    }
    for ((s1, s2), data) in data_sets {
		let title = format!("{} - {}", s1, s2);
		plot_perf(&title, data).unwrap();
	}
}

pub fn plot_perf(
    title: &String,
    data: Vec<(&'static str, &Vec<(usize, Measurement)>)>,
) -> Result<(), Box<dyn std::error::Error>> {
    let x_scale = data
        .iter()
        .map(|(_ser_str, measures)| measures.iter().map(|(threads, _)| threads).max().unwrap())
        .max()
        .unwrap();
    let y_scale = data
        .iter()
        .map(|(_ser_str, measures)| {
            measures
                .iter()
                .map(|(_, m)| m.throughput as usize + 10)
                .max()
                .unwrap()
        })
        .max()
        .unwrap() as f64;
    let file_name = &format!("{}.svg", title);
    let root_area = SVGBackend::new(file_name, (1024, 768)).into_drawing_area();
    root_area.fill(&WHITE)?;
    let mut chart = ChartBuilder::on(&root_area)
        .margin(10)
        .caption(title, ("sans-serif", 40))
        .build_cartesian_2d(1..*x_scale, 0.0..y_scale)?;
    chart
        .configure_mesh()
        .x_desc("Threads")
        .y_desc("Throughput")
        .draw()?;
    for (i, (title, data)) in data.iter().enumerate() {
        let color = Palette99::pick(i).mix(0.9);
        chart
            .draw_series(LineSeries::new(
                data.iter().map(|(t, m)| (*t, m.throughput)),
                color.stroke_width(3),
            ))?
            .label(*title);
    }
    Ok(())
}
