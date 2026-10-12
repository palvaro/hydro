//! Recording what happened in a trial and bucketing it per second.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Everything a harness tells the recorder, timestamped on arrival. Shared between the generator
/// task and the tasks reading the program's outputs.
#[derive(Clone, Debug)]
pub struct Recorder {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Debug)]
struct Inner {
    start: Instant,
    /// Every request offered, by id.
    requests: BTreeMap<u64, Request>,
    /// Program-reported gauges, by name: `(time, value)`.
    gauges: BTreeMap<String, Vec<(Duration, f64)>>,
    /// Program-reported counters, by name: `(time, increment)`.
    counters: BTreeMap<String, Vec<(Duration, f64)>>,
}

#[derive(Debug, Clone, Copy)]
struct Request {
    offered: Duration,
    fate: Option<Fate>,
}

#[derive(Debug, Clone, Copy)]
enum Fate {
    Completed(Duration),
    Abandoned(Duration),
}

impl Default for Recorder {
    fn default() -> Self {
        Self::new()
    }
}

impl Recorder {
    /// A recorder whose clock starts now.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                start: Instant::now(),
                requests: BTreeMap::new(),
                gauges: BTreeMap::new(),
                counters: BTreeMap::new(),
            })),
        }
    }

    /// The instant the recorder was created: time zero of the trial.
    pub fn start(&self) -> Instant {
        self.inner.lock().unwrap().start
    }

    /// Time since the recorder was created.
    pub fn elapsed(&self) -> Duration {
        self.inner.lock().unwrap().start.elapsed()
    }

    /// The generator is about to offer request `id`.
    pub fn offered(&self, id: u64) {
        let mut g = self.inner.lock().unwrap();
        let t = g.start.elapsed();
        g.requests.insert(
            id,
            Request {
                offered: t,
                fate: None,
            },
        );
    }

    /// The program reported request `id` complete. Only the first report of a fate for an id
    /// counts; later ones (duplicate answers, a completion after an abandonment) and reports for
    /// ids never offered are ignored.
    pub fn completed(&self, id: u64) {
        self.settle(id, Fate::Completed);
    }

    /// The program reported that it gave up on request `id`.
    pub fn abandoned(&self, id: u64) {
        self.settle(id, Fate::Abandoned);
    }

    fn settle(&self, id: u64, fate: fn(Duration) -> Fate) {
        let mut g = self.inner.lock().unwrap();
        let t = g.start.elapsed();
        if let Some(r) = g.requests.get_mut(&id)
            && r.fate.is_none()
        {
            r.fate = Some(fate(t));
        }
    }

    /// A program-reported level (a queue depth, a table size) observed now.
    pub fn gauge(&self, name: &str, value: f64) {
        let mut g = self.inner.lock().unwrap();
        let t = g.start.elapsed();
        g.gauges
            .entry(name.to_owned())
            .or_default()
            .push((t, value));
    }

    /// A program-reported count of events since its last report (re-sends, requests served).
    pub fn counter(&self, name: &str, increment: f64) {
        let mut g = self.inner.lock().unwrap();
        let t = g.start.elapsed();
        g.counters
            .entry(name.to_owned())
            .or_default()
            .push((t, increment));
    }

    /// Buckets everything recorded so far into one-second rows covering `total`.
    pub fn trace(&self, total: Duration) -> Trace {
        let g = self.inner.lock().unwrap();
        let n = total.as_secs_f64().ceil() as usize;
        let idx = |t: Duration| (t.as_secs() as usize).min(n.saturating_sub(1));
        let mut buckets: Vec<Bucket> = (0..n)
            .map(|i| Bucket {
                second: i as u64,
                ..Bucket::default()
            })
            .collect();
        let mut outstanding = 0;
        for r in g.requests.values() {
            let o = &mut buckets[idx(r.offered)];
            o.offered += 1;
            match r.fate {
                Some(Fate::Completed(t)) => {
                    let latency = t.saturating_sub(r.offered);
                    o.offered_completed += 1;
                    o.offered_latencies.push(latency);
                    let c = &mut buckets[idx(t)];
                    c.completed += 1;
                    c.latencies.push(latency);
                }
                Some(Fate::Abandoned(t)) => {
                    o.offered_abandoned += 1;
                    buckets[idx(t)].abandoned += 1;
                }
                None => {
                    o.offered_outstanding += 1;
                    outstanding += 1;
                }
            }
        }
        for (name, samples) in &g.gauges {
            for &(t, v) in samples {
                // The last sample in the second wins; samples are pushed in time order.
                buckets[idx(t)].gauges.insert(name.clone(), v);
            }
        }
        for (name, samples) in &g.counters {
            for &(t, v) in samples {
                *buckets[idx(t)].counters.entry(name.clone()).or_default() += v;
            }
        }
        for b in &mut buckets {
            b.latencies.sort();
            b.offered_latencies.sort();
        }
        // Carry gauges forward through seconds with no sample so plots have no holes, and
        // give every bucket every series so the TSV is rectangular.
        let gauge_names: Vec<String> = g.gauges.keys().cloned().collect();
        let counter_names: Vec<String> = g.counters.keys().cloned().collect();
        let mut last: BTreeMap<String, f64> = BTreeMap::new();
        for b in &mut buckets {
            for name in &gauge_names {
                match b.gauges.get(name) {
                    Some(&v) => {
                        last.insert(name.clone(), v);
                    }
                    None => {
                        if let Some(&v) = last.get(name) {
                            b.gauges.insert(name.clone(), v);
                        }
                    }
                }
            }
            for name in &counter_names {
                b.counters.entry(name.clone()).or_default();
            }
        }
        Trace {
            buckets,
            outstanding,
            gauge_names,
            counter_names,
        }
    }
}

/// One second of a trial.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Bucket {
    /// Seconds since the start of the trial.
    pub second: u64,
    /// Requests the generator offered in this second.
    pub offered: u64,
    /// Of the requests offered in this second, how many completed by the end of the run.
    pub offered_completed: u64,
    /// Of the requests offered in this second, how many the program gave up on.
    pub offered_abandoned: u64,
    /// Of the requests offered in this second, how many had no answer by the end of the run.
    pub offered_outstanding: u64,
    /// End-to-end latency of each request offered in this second that completed, sorted. This is
    /// the latency a request offered at this moment experienced, which is what a verdict reads.
    pub offered_latencies: Vec<Duration>,
    /// Requests whose completion arrived in this second (throughput, for plotting).
    pub completed: u64,
    /// Requests the program gave up on in this second.
    pub abandoned: u64,
    /// End-to-end latency of each completion that arrived in this second, sorted.
    pub latencies: Vec<Duration>,
    /// Last value of each program-reported gauge in this second (carried forward if none).
    pub gauges: BTreeMap<String, f64>,
    /// Sum of each program-reported counter's increments in this second.
    pub counters: BTreeMap<String, f64>,
}

impl Bucket {
    /// The `q`-quantile of the latencies of completions that arrived in this second.
    pub fn latency_quantile(&self, q: f64) -> Option<Duration> {
        quantile(&self.latencies, q)
    }

    /// The `q`-quantile of the latencies of requests offered in this second.
    pub fn offered_latency_quantile(&self, q: f64) -> Option<Duration> {
        quantile(&self.offered_latencies, q)
    }
}

/// The `q`-quantile of a sorted slice, `q` in `[0, 1]`.
pub fn quantile(sorted: &[Duration], q: f64) -> Option<Duration> {
    if sorted.is_empty() {
        return None;
    }
    let i = ((sorted.len() - 1) as f64 * q.clamp(0.0, 1.0)).round() as usize;
    Some(sorted[i])
}

/// A bucketed trial.
#[derive(Clone, Debug, PartialEq)]
pub struct Trace {
    /// One bucket per second, in order.
    pub buckets: Vec<Bucket>,
    /// Requests offered that were neither completed nor abandoned by the end.
    pub outstanding: u64,
    /// Names of the gauges the program reported, in column order.
    pub gauge_names: Vec<String>,
    /// Names of the counters the program reported, in column order.
    pub counter_names: Vec<String>,
}

impl Trace {
    /// The buckets covering `[from, to)` seconds.
    pub fn window(&self, from: Duration, to: Duration) -> &[Bucket] {
        let a = (from.as_secs() as usize).min(self.buckets.len());
        let b = (to.as_secs_f64().ceil() as usize).min(self.buckets.len());
        &self.buckets[a..b.max(a)]
    }

    /// Tab-separated rows, one per second, with a header. Latencies are in milliseconds.
    pub fn to_tsv(&self) -> String {
        let mut out = String::new();
        let mut header = vec![
            "second",
            "offered",
            "offered_completed",
            "offered_abandoned",
            "offered_outstanding",
            "offered_p50_ms",
            "offered_p99_ms",
            "completed",
            "abandoned",
            "p50_ms",
            "p99_ms",
            "max_ms",
        ]
        .into_iter()
        .map(str::to_owned)
        .collect::<Vec<_>>();
        header.extend(self.gauge_names.iter().cloned());
        header.extend(self.counter_names.iter().cloned());
        out.push_str(&header.join("\t"));
        out.push('\n');
        for b in &self.buckets {
            let ms = |d: Option<Duration>| {
                d.map(|d| format!("{:.1}", d.as_secs_f64() * 1e3))
                    .unwrap_or_else(|| "-".to_owned())
            };
            let _ = write!(
                out,
                "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                b.second,
                b.offered,
                b.offered_completed,
                b.offered_abandoned,
                b.offered_outstanding,
                ms(b.offered_latency_quantile(0.5)),
                ms(b.offered_latency_quantile(0.99)),
                b.completed,
                b.abandoned,
                ms(b.latency_quantile(0.5)),
                ms(b.latency_quantile(0.99)),
                ms(b.latencies.last().copied()),
            );
            for name in &self.gauge_names {
                match b.gauges.get(name) {
                    Some(v) => {
                        let _ = write!(out, "\t{v}");
                    }
                    None => out.push_str("\t-"),
                }
            }
            for name in &self.counter_names {
                let _ = write!(out, "\t{}", b.counters.get(name).copied().unwrap_or(0.0));
            }
            out.push('\n');
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_count_offers_completions_and_latency() {
        let r = Recorder::new();
        r.offered(0);
        r.offered(1);
        r.completed(0);
        r.abandoned(1);
        r.completed(99); // never offered: ignored
        r.gauge("backlog", 3.0);
        r.counter("retried", 2.0);
        r.counter("retried", 1.0);
        let t = r.trace(Duration::from_secs(3));
        assert_eq!(t.buckets.len(), 3);
        assert_eq!(t.buckets[0].offered, 2);
        assert_eq!(t.buckets[0].offered_completed, 1);
        assert_eq!(t.buckets[0].offered_abandoned, 1);
        assert_eq!(t.buckets[0].offered_outstanding, 0);
        assert_eq!(t.buckets[0].completed, 1);
        assert_eq!(t.buckets[0].abandoned, 1);
        assert_eq!(t.buckets[0].latencies.len(), 1);
        assert_eq!(t.buckets[0].offered_latencies.len(), 1);
        assert_eq!(t.buckets[0].counters["retried"], 3.0);
        // Gauge carried forward.
        assert_eq!(t.buckets[2].gauges["backlog"], 3.0);
        assert_eq!(t.buckets[2].counters["retried"], 0.0);
        assert_eq!(t.outstanding, 0);
        let tsv = t.to_tsv();
        assert!(tsv.starts_with("second\toffered\toffered_completed\toffered_abandoned\toffered_outstanding\toffered_p50_ms\toffered_p99_ms\tcompleted\tabandoned\tp50_ms\tp99_ms\tmax_ms\tbacklog\tretried\n"), "{tsv}");
        assert_eq!(tsv.lines().count(), 4);
    }
}
