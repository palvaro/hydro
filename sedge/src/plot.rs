//! SVG timeseries plots of a trial's [`Trace`], one chart per metric.
//!
//! Alongside each trial's per-second TSV the harness writes a set of SVG charts, each a single
//! timeseries with seconds on the x-axis: arrival rate, throughput, success rate, latency
//! quantiles, and one chart per program-reported gauge and counter. The trigger window is shaded
//! so every chart reads as baseline → burst → aftermath at a glance.
//!
//! The charts are written directly as SVG text — a `<polyline>` per series over a grid, a shaded
//! `<rect>` for the trigger — so the harness pulls in no graphics stack and runs offline. They
//! exist so that a run's story (does goodput return to baseline? does the backlog drain or
//! compound?) is visible without loading the TSV into other tooling.

use std::fmt::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::{fs, io};

use crate::profile::Profile;
use crate::trace::{Bucket, Trace};

const BLUE: &str = "#1f77b4";
const ORANGE: &str = "#ff7f0e";
const GREEN: &str = "#2ca02c";
const RED: &str = "#d62728";
const PURPLE: &str = "#9467bd";
/// Shade of the trigger window.
const BAND: &str = "#fde8e8";

/// One line on a chart: a value per second, `None` for seconds with nothing to plot (for
/// example a latency quantile in a second with no completions), which leave a gap in the line.
pub struct Series {
    pub label: String,
    pub color: &'static str,
    pub points: Vec<Option<f64>>,
}

/// One timeseries chart: x is seconds since the start of the trial.
pub struct Chart {
    /// Filename component, e.g. `success_rate`.
    pub name: String,
    /// Chart title, e.g. `success rate (%)`.
    pub title: String,
    pub series: Vec<Series>,
    /// The shaded x-window `[start, end)` in seconds: the trigger.
    pub band: Option<(f64, f64)>,
}

/// The standard charts for a trace: arrival rate, throughput (completions and abandonments),
/// success rate of the requests offered in each second, latency quantiles of the completions in
/// each second, then one chart per program-reported gauge and one per counter.
pub fn charts(trace: &Trace, profile: &Profile) -> Vec<Chart> {
    let band = Some((
        profile.trigger_start().as_secs_f64(),
        profile.trigger_end().as_secs_f64(),
    ));
    let per_second = |f: &dyn Fn(&Bucket) -> Option<f64>| -> Vec<Option<f64>> {
        trace.buckets.iter().map(f).collect()
    };
    let ms = |d: Option<Duration>| d.map(|d| d.as_secs_f64() * 1e3);
    let mut charts = vec![
        Chart {
            name: "arrival_rate".to_owned(),
            title: "arrival rate (req/s)".to_owned(),
            series: vec![Series {
                label: "offered".to_owned(),
                color: BLUE,
                points: per_second(&|b| Some(b.offered as f64)),
            }],
            band,
        },
        Chart {
            name: "throughput".to_owned(),
            title: "throughput (req/s)".to_owned(),
            series: vec![
                Series {
                    label: "completed".to_owned(),
                    color: GREEN,
                    points: per_second(&|b| Some(b.completed as f64)),
                },
                Series {
                    label: "abandoned".to_owned(),
                    color: RED,
                    points: per_second(&|b| Some(b.abandoned as f64)),
                },
            ],
            band,
        },
        Chart {
            name: "success_rate".to_owned(),
            title: "success rate of offered requests (%)".to_owned(),
            series: vec![Series {
                label: "completed".to_owned(),
                color: BLUE,
                points: per_second(&|b| {
                    (b.offered > 0).then(|| 100.0 * b.offered_completed as f64 / b.offered as f64)
                }),
            }],
            band,
        },
        Chart {
            name: "latency".to_owned(),
            title: "completion latency (ms)".to_owned(),
            series: vec![
                Series {
                    label: "p50".to_owned(),
                    color: BLUE,
                    points: per_second(&|b| ms(b.latency_quantile(0.5))),
                },
                Series {
                    label: "p99".to_owned(),
                    color: ORANGE,
                    points: per_second(&|b| ms(b.latency_quantile(0.99))),
                },
            ],
            band,
        },
    ];
    for name in &trace.gauge_names {
        charts.push(Chart {
            name: filename_component(name),
            title: name.clone(),
            series: vec![Series {
                label: name.clone(),
                color: PURPLE,
                points: per_second(&|b| b.gauges.get(name).copied()),
            }],
            band,
        });
    }
    for name in &trace.counter_names {
        charts.push(Chart {
            name: filename_component(name),
            title: format!("{name} (/s)"),
            series: vec![Series {
                label: name.clone(),
                color: ORANGE,
                points: per_second(&|b| b.counters.get(name).copied()),
            }],
            band,
        });
    }
    charts
}

/// Writes every chart of `trace` as `dir/{stem}.{chart}.svg` and returns the paths.
pub fn write_trial_plots(
    trace: &Trace,
    profile: &Profile,
    dir: &Path,
    stem: &str,
) -> io::Result<Vec<PathBuf>> {
    fs::create_dir_all(dir)?;
    let mut paths = Vec::new();
    for chart in charts(trace, profile) {
        let path = dir.join(format!("{stem}.{}.svg", chart.name));
        fs::write(&path, render(&chart, stem))?;
        paths.push(path);
    }
    Ok(paths)
}

/// A gauge or counter name as a filename component.
fn filename_component(name: &str) -> String {
    name.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

// Layout. A fixed size keeps the renderer simple; at 800×300 a one-minute trial gets about
// twelve pixels per second, plenty for lines read by eye, and SVG scales anyway.
const WIDTH: f64 = 800.0;
const HEIGHT: f64 = 300.0;
const LEFT: f64 = 64.0;
const RIGHT: f64 = 14.0;
const TOP: f64 = 30.0;
const BOTTOM: f64 = 40.0;

/// Renders `chart` as an SVG document. `stem` names the trial in the title line.
pub fn render(chart: &Chart, stem: &str) -> String {
    let x_max = chart
        .series
        .iter()
        .map(|s| s.points.len())
        .max()
        .unwrap_or(0)
        .max(1) as f64;
    let y_max = nice_ceiling(
        chart
            .series
            .iter()
            .flat_map(|s| s.points.iter().flatten())
            .fold(0.0f64, |a, &v| a.max(v)),
    );
    let to_x = |t: f64| LEFT + (WIDTH - LEFT - RIGHT) * t / x_max;
    let to_y = |v: f64| (HEIGHT - BOTTOM) - (HEIGHT - TOP - BOTTOM) * v / y_max;

    let mut s = String::new();
    let _ = writeln!(
        s,
        r#"<svg xmlns="http://www.w3.org/2000/svg" width="{WIDTH}" height="{HEIGHT}" viewBox="0 0 {WIDTH} {HEIGHT}" font-family="sans-serif" font-size="11">"#
    );
    let _ = writeln!(
        s,
        r#"<rect width="{WIDTH}" height="{HEIGHT}" fill="white"/>"#
    );
    let _ = writeln!(
        s,
        r#"<text x="{LEFT}" y="18" font-size="13">{} — {}</text>"#,
        escape(&chart.title),
        escape(stem),
    );

    if let Some((a, b)) = chart.band {
        let (x0, x1) = (to_x(a), to_x(b.min(x_max)));
        let _ = writeln!(
            s,
            r#"<rect x="{x0:.1}" y="{TOP}" width="{:.1}" height="{:.1}" fill="{BAND}"><title>trigger window</title></rect>"#,
            x1 - x0,
            HEIGHT - TOP - BOTTOM,
        );
    }

    // Grid and tick labels: five horizontal divisions, seconds on the x-axis at a round step.
    for i in 0..=5 {
        let v = y_max * f64::from(i) / 5.0;
        let y = to_y(v);
        let _ = writeln!(
            s,
            r##"<line x1="{LEFT}" y1="{y:.1}" x2="{:.1}" y2="{y:.1}" stroke="#e0e0e0"/>"##,
            WIDTH - RIGHT,
        );
        let _ = writeln!(
            s,
            r##"<text x="{:.1}" y="{:.1}" text-anchor="end" fill="#555">{}</text>"##,
            LEFT - 6.0,
            y + 4.0,
            format_tick(v),
        );
    }
    let step = [1, 2, 5, 10, 15, 30, 60, 120, 300]
        .into_iter()
        .find(|&s| x_max / f64::from(s) <= 12.0)
        .unwrap_or(600);
    let mut t = 0;
    while f64::from(t) <= x_max {
        let x = to_x(f64::from(t));
        let _ = writeln!(
            s,
            r##"<line x1="{x:.1}" y1="{:.1}" x2="{x:.1}" y2="{:.1}" stroke="#555"/>"##,
            HEIGHT - BOTTOM,
            HEIGHT - BOTTOM + 4.0,
        );
        let _ = writeln!(
            s,
            r##"<text x="{x:.1}" y="{:.1}" text-anchor="middle" fill="#555">{t}</text>"##,
            HEIGHT - BOTTOM + 16.0,
        );
        t += step;
    }
    let _ = writeln!(
        s,
        r##"<text x="{:.1}" y="{:.1}" text-anchor="middle" fill="#555">seconds</text>"##,
        LEFT + (WIDTH - LEFT - RIGHT) / 2.0,
        HEIGHT - 8.0,
    );
    let _ = writeln!(
        s,
        r##"<path d="M {LEFT} {TOP} V {y0} H {x1}" fill="none" stroke="#555"/>"##,
        y0 = HEIGHT - BOTTOM,
        x1 = WIDTH - RIGHT,
    );

    // The series: a polyline per gap-free stretch, each second's value at the second's midpoint.
    // An isolated point (both neighbors missing) gets a dot, which a polyline would not show.
    for series in &chart.series {
        let mut run: Vec<(f64, f64)> = Vec::new();
        let mut flush = |run: &mut Vec<(f64, f64)>| {
            match run.as_slice() {
                [] => {}
                &[(x, y)] => {
                    let _ = writeln!(
                        s,
                        r#"<circle cx="{x:.1}" cy="{y:.1}" r="2" fill="{}"/>"#,
                        series.color,
                    );
                }
                points => {
                    let _ = write!(
                        s,
                        r#"<polyline fill="none" stroke="{}" stroke-width="1.5" points=""#,
                        series.color
                    );
                    for (x, y) in points {
                        let _ = write!(s, "{x:.1},{y:.1} ");
                    }
                    let _ = writeln!(s, r#""/>"#);
                }
            }
            run.clear();
        };
        for (t, p) in series.points.iter().enumerate() {
            match p {
                Some(v) => run.push((to_x(t as f64 + 0.5), to_y(v.min(y_max)))),
                None => flush(&mut run),
            }
        }
        flush(&mut run);
    }

    // Legend, top-right inside the plot area.
    let mut entries: Vec<(&str, &str)> = chart
        .series
        .iter()
        .map(|series| (series.label.as_str(), series.color))
        .collect();
    if chart.band.is_some() {
        entries.push(("trigger", "#f0b0b0"));
    }
    let lx = WIDTH - RIGHT - 110.0;
    for (i, (label, color)) in entries.iter().enumerate() {
        let y = TOP + 8.0 + 14.0 * i as f64;
        let _ = writeln!(
            s,
            r#"<rect x="{lx:.1}" y="{:.1}" width="14" height="4" fill="{color}"/>"#,
            y - 2.0,
        );
        let _ = writeln!(
            s,
            r##"<text x="{:.1}" y="{:.1}" fill="#333">{}</text>"##,
            lx + 20.0,
            y + 4.0,
            escape(label),
        );
    }

    s.push_str("</svg>\n");
    s
}

/// The smallest 1, 2 or 5 times a power of ten that is at least `v`, so axis divisions land on
/// round numbers. All charts start at zero: rates, queues and latencies all live there, and a
/// zero baseline is what makes collapse (a line pinned to the floor) unmistakable.
fn nice_ceiling(v: f64) -> f64 {
    if v <= 0.0 {
        return 1.0;
    }
    let k = 10f64.powf(v.log10().floor());
    [1.0, 2.0, 5.0, 10.0]
        .into_iter()
        .map(|m| m * k)
        .find(|&c| c >= v * (1.0 - 1e-9))
        .unwrap_or(10.0 * k)
}

/// A tick value, without a trailing `.0`.
fn format_tick(v: f64) -> String {
    if (v - v.round()).abs() < 1e-9 {
        format!("{}", v.round() as i64)
    } else {
        format!("{v:.1}")
    }
}

/// The XML-significant characters of `text`, escaped.
fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::trace::Recorder;

    #[test]
    fn nice_ceilings_are_round() {
        assert_eq!(nice_ceiling(0.0), 1.0);
        assert_eq!(nice_ceiling(3.0), 5.0);
        assert_eq!(nice_ceiling(10.0), 10.0);
        assert_eq!(nice_ceiling(1483.0), 2000.0);
        assert_eq!(nice_ceiling(15789.0), 20000.0);
    }

    #[test]
    fn renders_a_chart_per_metric() {
        let r = Recorder::new();
        r.offered(0);
        r.completed(0);
        r.offered(1);
        r.abandoned(1);
        r.gauge("backlog", 2.0);
        r.counter("retried", 1.0);
        let trace = r.trace(Duration::from_secs(3));
        let profile = Profile {
            baseline_rps: 1.0,
            trigger_rps: 2.0,
            before: Duration::from_secs(1),
            trigger: Duration::from_secs(1),
            tail: Duration::from_secs(1),
        };
        let charts = charts(&trace, &profile);
        let names: Vec<&str> = charts.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "arrival_rate",
                "throughput",
                "success_rate",
                "latency",
                "backlog",
                "retried"
            ]
        );
        for chart in &charts {
            let svg = render(chart, "demo.trial0");
            assert!(svg.starts_with("<svg "), "{svg}");
            assert!(svg.ends_with("</svg>\n"));
            assert!(
                svg.contains("<polyline") || svg.contains("<circle"),
                "{svg}"
            );
            // The trigger band is drawn.
            assert!(svg.contains("trigger window"));
        }
    }

    #[test]
    fn gaps_split_the_line_and_isolated_points_are_dots() {
        let chart = Chart {
            name: "g".to_owned(),
            title: "g".to_owned(),
            series: vec![Series {
                label: "g".to_owned(),
                color: BLUE,
                points: vec![Some(1.0), None, Some(2.0), Some(3.0), None],
            }],
            band: None,
        };
        let svg = render(&chart, "t");
        assert_eq!(svg.matches("<circle").count(), 1);
        assert_eq!(svg.matches("<polyline").count(), 1);
    }
}
