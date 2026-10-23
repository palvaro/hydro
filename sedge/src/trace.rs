//! Recording what happened in a trial and bucketing it per second.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// What the harness records about one record of an output stream: see
/// `sedge::program::Observation`, of which this is the dependency-free twin so that the trace can
/// be built and tested without the deployment machinery.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct Sample {
    /// The record's numeric value, when it is a plain number.
    pub value: Option<f64>,
    /// The record's id, when it carries one.
    pub id: Option<u64>,
    /// The cluster member that produced the record, for streams on clusters. A level from a
    /// cluster is kept per member and reported as the sum over members.
    pub member: Option<u32>,
}

/// Everything a harness tells the recorder, timestamped on arrival. Shared between the generator
/// task and the tasks reading the program's outputs.
#[derive(Clone, Debug)]
pub struct Recorder {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Debug)]
struct Inner {
    start: Instant,
    /// Every record offered, by sequence number.
    requests: BTreeMap<u64, Request>,
    /// Fates reported for ids not offered yet: a reply can reach the reader task before the
    /// generator task has recorded the round that carried the request.
    early: BTreeMap<u64, Fate>,
    /// When each round was offered.
    rounds: Vec<Duration>,
    /// Every record of every observed output stream, by stream name: `(time, sample)`.
    streams: BTreeMap<String, Vec<(Duration, Sample)>>,
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
                early: BTreeMap::new(),
                rounds: Vec::new(),
                streams: BTreeMap::new(),
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

    /// The generator offered one round at `at`, carrying records `first..first + n` of the data
    /// stream (numbered from zero over the trial).
    pub fn offered_round(&self, at: Instant, first: u64, n: u64) {
        let mut g = self.inner.lock().unwrap();
        let t = at.saturating_duration_since(g.start);
        g.rounds.push(t);
        for id in first..first + n {
            let fate = g.early.remove(&id);
            g.requests.insert(id, Request { offered: t, fate });
        }
    }

    /// Output stream `name` produced one record, observed now.
    pub fn record(&self, name: &str, sample: Sample) {
        let mut g = self.inner.lock().unwrap();
        let t = g.start.elapsed();
        g.streams
            .entry(name.to_owned())
            .or_default()
            .push((t, sample));
    }

    /// The names of every stream that has produced a record so far.
    pub fn stream_names(&self) -> Vec<String> {
        self.inner.lock().unwrap().streams.keys().cloned().collect()
    }

    /// Whether some record of `name` so far carried an id.
    pub fn has_ids(&self, name: &str) -> bool {
        let g = self.inner.lock().unwrap();
        g.streams
            .get(name)
            .is_some_and(|v| v.iter().any(|(_, s)| s.id.is_some()))
    }

    /// Whether every record of `name` so far carried a numeric value.
    pub fn all_numeric(&self, name: &str) -> bool {
        let g = self.inner.lock().unwrap();
        g.streams
            .get(name)
            .is_some_and(|v| !v.is_empty() && v.iter().all(|(_, s)| s.value.is_some()))
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
        match g.requests.get_mut(&id) {
            Some(r) => {
                if r.fate.is_none() {
                    r.fate = Some(fate(t));
                }
            }
            None => {
                g.early.entry(id).or_insert(fate(t));
            }
        }
    }

    /// A program-reported level (a queue depth, a table size) observed now. Recorded as a record
    /// of stream `name` carrying `value`.
    pub fn gauge(&self, name: &str, value: f64) {
        self.record(
            name,
            Sample {
                value: Some(value),
                id: None,
                member: None,
            },
        );
    }

    /// A program-reported count of events since its last report (re-sends, requests served).
    /// Recorded as `increment` records of stream `name` (rounded).
    pub fn counter(&self, name: &str, increment: f64) {
        for _ in 0..increment.round().max(0.0) as u64 {
            self.record(name, Sample::default());
        }
    }

    /// Buckets everything recorded so far into one-second rows covering `total`. When
    /// `completion_stream` names an observed stream, every record of it that carries an id is
    /// read as the completion of that request (in addition to what [`Recorder::completed`] was
    /// told), so a conserved output stream carrying the input's sequence numbers gives goodput
    /// and latency without a hand-written mapping.
    pub fn trace(&self, total: Duration, completion_stream: Option<&str>) -> Trace {
        let mut g = self.inner.lock().unwrap();
        if let Some(name) = completion_stream
            && let Some(samples) = g.streams.get(name).cloned()
        {
            for (t, s) in samples {
                if let Some(id) = s.id
                    && let Some(r) = g.requests.get_mut(&id)
                    && r.fate.is_none()
                {
                    r.fate = Some(Fate::Completed(t));
                }
            }
        }
        let n = total.as_secs_f64().ceil() as usize;
        let idx = |t: Duration| (t.as_secs() as usize).min(n.saturating_sub(1));
        let mut buckets: Vec<Bucket> = (0..n)
            .map(|i| Bucket {
                second: i as u64,
                ..Bucket::default()
            })
            .collect();
        for &t in &g.rounds {
            buckets[idx(t)].rounds += 1;
        }
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
        for (name, samples) in &g.streams {
            // Records arriving after `total` (in the grace period) would all land in the last
            // bucket and inflate its rate, so they are left out of the per-second rows; fates
            // read from them above still count.
            //
            // A value is the latest sample, summed over cluster members (a process stream has one
            // "member", `None`): each member's depth is its own, and the program's backlog is
            // their total.
            let mut latest_by_member: BTreeMap<Option<u32>, f64> = BTreeMap::new();
            for &(t, s) in samples.iter().filter(|(t, _)| *t < total) {
                let b = &mut buckets[idx(t)];
                *b.stream_records.entry(name.clone()).or_default() += 1;
                if let Some(v) = s.value {
                    // The last sample in the second wins; samples are pushed in time order.
                    latest_by_member.insert(s.member, v);
                    b.stream_values
                        .insert(name.clone(), latest_by_member.values().sum());
                }
            }
        }
        for b in &mut buckets {
            b.latencies.sort();
            b.offered_latencies.sort();
        }
        // Carry values forward through seconds with no sample so plots have no holes, and give
        // every bucket every series so the TSV is rectangular.
        let stream_names: Vec<String> = g.streams.keys().cloned().collect();
        let valued_names: Vec<String> = stream_names
            .iter()
            .filter(|n| g.streams[*n].iter().any(|(_, s)| s.value.is_some()))
            .cloned()
            .collect();
        let mut last: BTreeMap<String, f64> = BTreeMap::new();
        for b in &mut buckets {
            for name in &valued_names {
                match b.stream_values.get(name) {
                    Some(&v) => {
                        last.insert(name.clone(), v);
                    }
                    None => {
                        if let Some(&v) = last.get(name) {
                            b.stream_values.insert(name.clone(), v);
                        }
                    }
                }
            }
            for name in &stream_names {
                b.stream_records.entry(name.clone()).or_default();
            }
        }
        Trace {
            buckets,
            outstanding,
            stream_names,
            valued_names,
            extra: Vec::new(),
        }
    }
}

/// One second of a trial.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Bucket {
    /// Seconds since the start of the trial.
    pub second: u64,
    /// Rounds the generator offered in this second.
    pub rounds: u64,
    /// Records (requests) the generator offered in this second.
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
    /// Records each observed output stream produced in this second.
    pub stream_records: BTreeMap<String, u64>,
    /// Last numeric value each observed output stream produced in this second (carried forward
    /// if none), for streams whose records are numbers.
    pub stream_values: BTreeMap<String, f64>,
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
    /// Names of every observed output stream, in column order.
    pub stream_names: Vec<String>,
    /// Of those, the streams that produced numeric values.
    pub valued_names: Vec<String>,
    /// Columns the harness measured itself rather than read from the program, one value per
    /// bucket (missing values print as `-`): per-node CPU usage and throttling, read from each
    /// node's cgroup. The verdict never reads them; they are the run's ground truth about
    /// capacity.
    pub extra: Vec<(String, Vec<f64>)>,
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
            "rounds",
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
        header.extend(self.stream_names.iter().map(|n| format!("{n}_n")));
        header.extend(self.valued_names.iter().map(|n| format!("{n}_v")));
        header.extend(self.extra.iter().map(|(n, _)| n.clone()));
        out.push_str(&header.join("\t"));
        out.push('\n');
        for (i, b) in self.buckets.iter().enumerate() {
            let ms = |d: Option<Duration>| {
                d.map(|d| format!("{:.1}", d.as_secs_f64() * 1e3))
                    .unwrap_or_else(|| "-".to_owned())
            };
            let _ = write!(
                out,
                "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
                b.second,
                b.rounds,
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
            for name in &self.stream_names {
                let _ = write!(out, "\t{}", b.stream_records.get(name).copied().unwrap_or(0));
            }
            for name in &self.valued_names {
                match b.stream_values.get(name) {
                    Some(v) => {
                        let _ = write!(out, "\t{v}");
                    }
                    None => out.push_str("\t-"),
                }
            }
            for (_, values) in &self.extra {
                match values.get(i) {
                    Some(v) => {
                        let _ = write!(out, "\t{v:.3}");
                    }
                    None => out.push_str("\t-"),
                }
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
        let t = r.trace(Duration::from_secs(3), None);
        assert_eq!(t.buckets.len(), 3);
        assert_eq!(t.buckets[0].offered, 2);
        assert_eq!(t.buckets[0].offered_completed, 1);
        assert_eq!(t.buckets[0].offered_abandoned, 1);
        assert_eq!(t.buckets[0].offered_outstanding, 0);
        assert_eq!(t.buckets[0].completed, 1);
        assert_eq!(t.buckets[0].abandoned, 1);
        assert_eq!(t.buckets[0].latencies.len(), 1);
        assert_eq!(t.buckets[0].offered_latencies.len(), 1);
        assert_eq!(t.buckets[0].stream_records["retried"], 3);
        // Gauge carried forward.
        assert_eq!(t.buckets[2].stream_values["backlog"], 3.0);
        assert_eq!(t.buckets[2].stream_records["retried"], 0);
        assert_eq!(t.outstanding, 0);
        let tsv = t.to_tsv();
        assert!(tsv.starts_with("second\trounds\toffered\toffered_completed\toffered_abandoned\toffered_outstanding\toffered_p50_ms\toffered_p99_ms\tcompleted\tabandoned\tp50_ms\tp99_ms\tmax_ms\tbacklog_n\tretried_n\tbacklog_v\n"), "{tsv}");
        assert_eq!(tsv.lines().count(), 4);
    }

    #[test]
    fn cluster_levels_are_summed_over_members() {
        let r = Recorder::new();
        let depth = |m: u32, v: f64| Sample {
            value: Some(v),
            id: None,
            member: Some(m),
        };
        r.record("inbox", depth(0, 3.0));
        r.record("inbox", depth(1, 4.0));
        r.record("inbox", depth(0, 1.0));
        let t = r.trace(Duration::from_secs(2), None);
        assert_eq!(t.buckets[0].stream_records["inbox"], 3);
        // Member 0's latest is 1, member 1's is 4.
        assert_eq!(t.buckets[0].stream_values["inbox"], 5.0);
        assert_eq!(t.buckets[1].stream_values["inbox"], 5.0);
    }
}
