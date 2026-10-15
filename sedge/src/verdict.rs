//! Reading a verdict from a bucketed trace.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

use crate::profile::Profile;
use crate::trace::{Bucket, Trace, quantile};

/// How the verdict reads an observed output stream. The kinds come from Cress's conservation
/// table (`conserved`, `amplified`, `level`), from an explicit registration (`gauge` is a level,
/// `counter` is work), or, failing both, from the item type (a numeric stream is a level,
/// anything else is work).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamKind {
    /// The count is fixed by the input: its rate is goodput, and a tail rate below the
    /// baseline's is lost work.
    Conserved,
    /// The count rises under delay: its rate is the program's cost, and a tail rate above the
    /// baseline's is amplification.
    Amplified,
    /// One sample per tick: its value is a backlog, read for rising and falling.
    Level,
    /// Not classified: read like an amplified stream (its tail rate must return to baseline),
    /// but never counted as goodput.
    Work,
}

impl StreamKind {
    /// Parses Cress's word.
    pub fn parse(word: &str) -> Option<Self> {
        match word {
            "conserved" => Some(StreamKind::Conserved),
            "amplified" => Some(StreamKind::Amplified),
            "level" => Some(StreamKind::Level),
            "shed" | "silent" | "work" => Some(StreamKind::Work),
            _ => None,
        }
    }

    pub fn word(&self) -> &'static str {
        match self {
            StreamKind::Conserved => "conserved",
            StreamKind::Amplified => "amplified",
            StreamKind::Level => "level",
            StreamKind::Work => "work",
        }
    }
}

/// The kind of every observed stream, by name.
pub type StreamKinds = BTreeMap<String, StreamKind>;

/// One observed stream over a window.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamWindow {
    pub name: String,
    pub kind: StreamKind,
    /// Mean records per second over the window.
    pub rate: f64,
    /// For a level: its mean value over the first and the second half of the window.
    pub halves: Option<(f64, f64)>,
}

/// What the tail of a trial looked like against its pre-trigger baseline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// The tail completes requests at the baseline rate, abandons none, and no program-reported
    /// level is still rising.
    Recovered,
    /// The tail completes at most half of what the baseline did, or a program-reported level is
    /// still rising through the tail while goodput or latency is degraded.
    Collapsed,
    /// Neither: a level is falling, or goodput is below baseline without the marks of collapse.
    /// A longer tail would decide it.
    Draining,
}

impl fmt::Display for Outcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Outcome::Recovered => "RECOVERED",
            Outcome::Collapsed => "COLLAPSED",
            Outcome::Draining => "DRAINING",
        })
    }
}

/// Summary statistics of a window of buckets, read by the time requests were *offered*: a
/// request offered in the window is counted here whatever happened to it later, so a window's
/// completion fraction and latency describe what a request offered then experienced.
#[derive(Debug, Clone, PartialEq)]
pub struct WindowStats {
    /// The window, in seconds since the start of the trial.
    pub from: u64,
    pub to: u64,
    pub offered: u64,
    /// Of `offered`, how many completed by the end of the run.
    pub completed: u64,
    /// Of `offered`, how many the program gave up on.
    pub abandoned: u64,
    /// Of `offered`, how many had no answer by the end of the run.
    pub outstanding: u64,
    /// `completed / offered`, or 1 when nothing was offered.
    pub completion_fraction: f64,
    /// Median end-to-end latency of the window's completed requests.
    pub p50: Option<Duration>,
    /// 99th percentile of the same.
    pub p99: Option<Duration>,
    /// Every observed stream over the window.
    pub streams: Vec<StreamWindow>,
}

impl WindowStats {
    /// For each level, `(name, first-half mean, second-half mean)`.
    pub fn gauge_halves(&self) -> Vec<(String, f64, f64)> {
        self.streams
            .iter()
            .filter_map(|s| s.halves.map(|(a, b)| (s.name.clone(), a, b)))
            .collect()
    }

    fn stream(&self, name: &str) -> Option<&StreamWindow> {
        self.streams.iter().find(|s| s.name == name)
    }
}

fn stats(buckets: &[Bucket], kinds: &StreamKinds) -> WindowStats {
    let offered = buckets.iter().map(|b| b.offered).sum();
    let completed = buckets.iter().map(|b| b.offered_completed).sum();
    let abandoned = buckets.iter().map(|b| b.offered_abandoned).sum();
    let outstanding = buckets.iter().map(|b| b.offered_outstanding).sum();
    let mut latencies: Vec<Duration> = buckets
        .iter()
        .flat_map(|b| b.offered_latencies.iter().copied())
        .collect();
    latencies.sort();
    let mean = |xs: &[f64]| {
        if xs.is_empty() {
            None
        } else {
            Some(xs.iter().sum::<f64>() / xs.len() as f64)
        }
    };
    let (first, second) = buckets.split_at(buckets.len() / 2);
    let streams = kinds
        .iter()
        .map(|(name, kind)| {
            let records: u64 = buckets
                .iter()
                .map(|b| b.stream_records.get(name).copied().unwrap_or(0))
                .sum();
            let rate = if buckets.is_empty() {
                0.0
            } else {
                records as f64 / buckets.len() as f64
            };
            let values = |bs: &[Bucket]| -> Vec<f64> {
                bs.iter()
                    .filter_map(|b| b.stream_values.get(name).copied())
                    .collect()
            };
            let halves = if *kind == StreamKind::Level {
                mean(&values(first)).zip(mean(&values(second)))
            } else {
                None
            };
            StreamWindow {
                name: name.clone(),
                kind: *kind,
                rate,
                halves,
            }
        })
        .collect();
    WindowStats {
        from: buckets.first().map(|b| b.second).unwrap_or(0),
        to: buckets.last().map(|b| b.second + 1).unwrap_or(0),
        offered,
        completed,
        abandoned,
        outstanding,
        completion_fraction: if offered == 0 {
            1.0
        } else {
            completed as f64 / offered as f64
        },
        p50: quantile(&latencies, 0.5),
        p99: quantile(&latencies, 0.99),
        streams,
    }
}

/// The verdict and the numbers it was read from.
#[derive(Debug, Clone, PartialEq)]
pub struct Verdict {
    pub outcome: Outcome,
    /// Why, in one line.
    pub reason: String,
    pub baseline: WindowStats,
    pub tail: WindowStats,
    /// Requests offered at any time and never answered by the end of the run.
    pub outstanding: u64,
    /// Whether goodput and latency were read from ids (a completion stream), or only from
    /// stream rates.
    pub by_id: bool,
    /// Tail goodput as a fraction of the baseline's: completion fractions when `by_id`, else the
    /// smallest tail/baseline rate ratio over the conserved streams (1 when there are none).
    pub relative_goodput: f64,
    /// The largest tail/baseline rate ratio over the amplified and unclassified streams, with the
    /// stream's name; 1 when there are none.
    pub amplification: (String, f64),
}

impl fmt::Display for Verdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ms = |d: Option<Duration>| {
            d.map(|d| format!("{:.1}ms", d.as_secs_f64() * 1e3))
                .unwrap_or_else(|| "-".to_owned())
        };
        writeln!(f, "{}: {}", self.outcome, self.reason)?;
        for (label, w) in [("baseline", &self.baseline), ("tail", &self.tail)] {
            if self.by_id {
                writeln!(
                    f,
                    "  {label} [{}s, {}s): offered {} completed {} ({:.1}%) abandoned {} outstanding {} p50 {} p99 {}",
                    w.from,
                    w.to,
                    w.offered,
                    w.completed,
                    w.completion_fraction * 100.0,
                    w.abandoned,
                    w.outstanding,
                    ms(w.p50),
                    ms(w.p99)
                )?;
            } else {
                writeln!(f, "  {label} [{}s, {}s): offered {}", w.from, w.to, w.offered)?;
            }
            for s in &w.streams {
                match s.halves {
                    Some((first, second)) => writeln!(
                        f,
                        "    {} ({}): mean {first:.0} in the first half, {second:.0} in the second",
                        s.name,
                        s.kind.word()
                    )?,
                    None => writeln!(f, "    {} ({}): {:.1}/s", s.name, s.kind.word(), s.rate)?,
                }
            }
        }
        write!(f, "  outstanding at end: {}", self.outstanding)
    }
}

/// How much slower than the baseline the tail's median latency may be and still read as
/// recovered, as a multiple plus a floor for jitter.
const LATENCY_SLACK_FACTOR: f64 = 5.0;
const LATENCY_SLACK_FLOOR: Duration = Duration::from_millis(20);

/// Reads the tail of `trace` against its pre-trigger baseline. The baseline window is the second
/// half of the pre-trigger period (the first half is warm-up); the tail window is the second half
/// of the post-trigger period, so that a backlog the trigger built has had the first half to
/// drain. Both windows are read by offer time (see [`WindowStats`]).
///
/// The rule, in order:
///
/// - **Recovered** if the tail completes at least 95% of the baseline's fraction, abandons
///   nothing, has a median latency within [`LATENCY_SLACK_FACTOR`]× the baseline's (plus
///   [`LATENCY_SLACK_FLOOR`]), and no program-reported level is rising.
/// - **Draining** if some level is falling and none is rising, whatever the goodput: the program
///   is working off what the trigger left and the tail was too short to see it finish. The
///   reason gives the rate and an estimate of the time to empty.
/// - **Collapsed** if the tail completes at most half the fraction the baseline did, or if a
///   level is still rising while goodput or latency is degraded.
/// - **Draining** otherwise.
///
/// Levels are judged by comparing the mean over the second half of the window with the mean
/// over the first: rising if up by more than 5% and one unit, falling if down by as much.
pub fn judge(trace: &Trace, profile: &Profile) -> Verdict {
    let kinds: StreamKinds = trace
        .stream_names
        .iter()
        .map(|n| {
            let kind = if trace.valued_names.contains(n) {
                StreamKind::Level
            } else {
                StreamKind::Work
            };
            (n.clone(), kind)
        })
        .collect();
    judge_with_kinds(trace, profile, &kinds, true)
}

/// An amplified stream's tail rate is read as the ratio `tail / (baseline + floor)`, the floor
/// in records per second so that a stream quiet in both windows is not read as infinite; it has
/// returned to baseline when the ratio is within the slack factor, and collapsed from the
/// collapse factor on.
const AMPLIFICATION_SLACK_FACTOR: f64 = 1.5;
const AMPLIFICATION_SLACK_FLOOR: f64 = 2.0;
const AMPLIFICATION_COLLAPSE_FACTOR: f64 = 3.0;

/// [`judge`] with every observed stream's kind given, and whether ids were recorded (so that
/// completion fractions and latencies mean something). Without ids, goodput is the smallest
/// tail/baseline rate ratio over the conserved streams, and latency is not read.
///
/// The generalized rule, in order:
///
/// - **Recovered** if goodput is at least 95% of the baseline's, nothing was abandoned, latency
///   is within slack (when read), no level is rising, and every amplified or unclassified stream's
///   tail rate is within [`AMPLIFICATION_SLACK_FACTOR`]× its baseline rate.
/// - **Draining** if some level is falling and none is rising.
/// - **Collapsed** if goodput is at most half the baseline's, or some amplified stream's tail rate
///   is at least [`AMPLIFICATION_COLLAPSE_FACTOR`]× its baseline rate, or a level is rising while
///   goodput, latency or amplification is degraded.
/// - **Draining** otherwise.
pub fn judge_with_kinds(
    trace: &Trace,
    profile: &Profile,
    kinds: &StreamKinds,
    by_id: bool,
) -> Verdict {
    let baseline_from = profile.before / 2;
    let baseline = stats(trace.window(baseline_from, profile.trigger_start()), kinds);
    let tail_from = profile.trigger_end() + profile.tail / 2;
    let tail = stats(trace.window(tail_from, profile.total()), kinds);

    let rate_ratio = |name: &str| -> Option<f64> {
        let b = baseline.stream(name)?.rate;
        let t = tail.stream(name)?.rate;
        Some(if b > 0.0 { t / b } else if t > 0.0 { f64::INFINITY } else { 1.0 })
    };
    let relative_goodput = if by_id {
        if baseline.completion_fraction > 0.0 {
            tail.completion_fraction / baseline.completion_fraction
        } else {
            tail.completion_fraction
        }
    } else {
        kinds
            .iter()
            .filter(|(_, k)| **k == StreamKind::Conserved)
            .filter_map(|(n, _)| rate_ratio(n))
            .fold(1.0, f64::min)
    };
    let latency_ok = if by_id {
        match (baseline.p50, tail.p50) {
            (Some(b), Some(t)) => t <= b.mul_f64(LATENCY_SLACK_FACTOR) + LATENCY_SLACK_FLOOR,
            (None, Some(t)) => t <= LATENCY_SLACK_FLOOR,
            (_, None) => false,
        }
    } else {
        true
    };
    // Amplified (and unclassified) streams: the tail rate against the baseline rate, with a
    // floor so that a stream quiet in both windows is not read as infinite.
    let amplification = kinds
        .iter()
        .filter(|(_, k)| matches!(k, StreamKind::Amplified | StreamKind::Work))
        .filter_map(|(n, _)| {
            let b = baseline.stream(n)?.rate;
            let t = tail.stream(n)?.rate;
            let ratio = t / (b + AMPLIFICATION_SLACK_FLOOR);
            Some((n.clone(), ratio, b, t))
        })
        .max_by(|x, y| x.1.partial_cmp(&y.1).unwrap_or(std::cmp::Ordering::Equal));
    let amplification_ok = amplification
        .as_ref()
        .is_none_or(|(_, r, _, _)| *r <= AMPLIFICATION_SLACK_FACTOR);
    let amplification_collapsed = amplification
        .as_ref()
        .is_some_and(|(_, r, _, _)| *r >= AMPLIFICATION_COLLAPSE_FACTOR);
    let amplification_text = amplification
        .as_ref()
        .map(|(n, _, b, t)| format!("{n} at {t:.1}/s against {b:.1}/s"))
        .unwrap_or_else(|| "no amplified stream".to_owned());
    // A level is rising if its second-half mean exceeds its first-half mean by more than 5% and
    // at least one unit, so jitter around a small steady value is not read as growth.
    let halves = tail.gauge_halves();
    let rising: Vec<&str> = halves
        .iter()
        .filter(|(_, first, second)| *second > first * 1.05 + 1.0)
        .map(|(n, _, _)| n.as_str())
        .collect();
    let falling: Vec<&str> = halves
        .iter()
        .filter(|(_, first, second)| *second < first * 0.95 - 1.0)
        .map(|(n, _, _)| n.as_str())
        .collect();
    let ms = |d: Option<Duration>| {
        d.map(|d| format!("{:.0}ms", d.as_secs_f64() * 1e3))
            .unwrap_or_else(|| "-".to_owned())
    };
    let latency_text = if by_id {
        format!(", median latency {} against {}", ms(tail.p50), ms(baseline.p50))
    } else {
        String::new()
    };

    let healthy = relative_goodput >= 0.95
        && tail.abandoned == 0
        && latency_ok
        && rising.is_empty()
        && amplification_ok;
    let degraded = relative_goodput < 0.9 || !latency_ok || !amplification_ok;
    let (outcome, reason) = if healthy {
        (
            Outcome::Recovered,
            format!(
                "tail goodput is {:.0}% of baseline{latency_text}, no abandonments, {amplification_text}",
                relative_goodput * 100.0,
            ),
        )
    } else if rising.is_empty() && !falling.is_empty() {
        // Whatever the goodput, a level that is steadily falling means the program is working
        // off what the trigger left behind; the run was too short to see it finish.
        let eta: Vec<String> = halves
            .iter()
            .filter(|(n, _, _)| falling.contains(&n.as_str()))
            .map(|(n, first, second)| {
                let half = (tail.to - tail.from) as f64 / 2.0;
                let rate = (first - second) / half;
                format!(
                    "{n} falling at {rate:.0}/s, about {:.0}s from empty",
                    second / rate
                )
            })
            .collect();
        (
            Outcome::Draining,
            format!(
                "goodput at {:.0}% of baseline; {}; {amplification_text}",
                relative_goodput * 100.0,
                eta.join(", ")
            ),
        )
    } else if amplification_collapsed {
        (
            Outcome::Collapsed,
            format!(
                "{amplification_text} through the tail, goodput at {:.0}% of baseline{latency_text}{}",
                relative_goodput * 100.0,
                if rising.is_empty() {
                    String::new()
                } else {
                    format!(", {} still rising", rising.join(", "))
                }
            ),
        )
    } else if relative_goodput <= 0.5 {
        (
            Outcome::Collapsed,
            format!(
                "requests offered in the tail completed at {:.0}% of the baseline rate{}",
                relative_goodput * 100.0,
                if rising.is_empty() {
                    String::new()
                } else {
                    format!(", {} still rising", rising.join(", "))
                }
            ),
        )
    } else if !rising.is_empty() && degraded {
        (
            Outcome::Collapsed,
            format!(
                "{} still rising through the tail with goodput at {:.0}% of baseline{latency_text}, {amplification_text}",
                rising.join(", "),
                relative_goodput * 100.0,
            ),
        )
    } else {
        let mut why = vec![format!(
            "goodput at {:.0}% of baseline",
            relative_goodput * 100.0
        )];
        if !latency_ok {
            why.push(format!(
                "median latency {} against {}",
                ms(tail.p50),
                ms(baseline.p50)
            ));
        }
        if !amplification_ok {
            why.push(amplification_text.clone());
        }
        if tail.abandoned > 0 {
            why.push(format!("{} abandoned", tail.abandoned));
        }
        if !rising.is_empty() {
            why.push(format!("{} rising", rising.join(", ")));
        }
        (Outcome::Draining, why.join("; "))
    };

    Verdict {
        outcome,
        reason,
        baseline,
        tail,
        outstanding: trace.outstanding,
        by_id,
        relative_goodput,
        amplification: amplification
            .map(|(n, r, _, _)| (n, r))
            .unwrap_or_else(|| (String::new(), 1.0)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile() -> Profile {
        Profile {
            baseline_rps: 10.0,
            trigger_rps: 100.0,
            before: Duration::from_secs(4),
            trigger: Duration::from_secs(2),
            tail: Duration::from_secs(8),
        }
    }

    fn trace(
        tail_completed: u64,
        tail_abandoned: u64,
        tail_latency: Duration,
        backlog: impl Fn(u64) -> f64,
    ) -> Trace {
        let p = profile();
        let n = p.total().as_secs();
        let buckets = (0..n)
            .map(|s| {
                let in_trigger = s >= p.trigger_start().as_secs() && s < p.trigger_end().as_secs();
                let in_tail = s >= p.trigger_end().as_secs();
                let offered = if in_trigger { 100 } else { 10 };
                let completed = if in_tail {
                    tail_completed
                } else if in_trigger {
                    20
                } else {
                    10
                };
                let abandoned = if in_tail { tail_abandoned } else { 0 };
                let latency = if in_tail {
                    tail_latency
                } else {
                    Duration::from_millis(1)
                };
                let mut gauges = std::collections::BTreeMap::new();
                gauges.insert("backlog".to_owned(), backlog(s));
                let mut records = std::collections::BTreeMap::new();
                records.insert("backlog".to_owned(), 1);
                Bucket {
                    second: s,
                    rounds: offered,
                    offered,
                    offered_completed: completed,
                    offered_abandoned: abandoned,
                    offered_outstanding: offered.saturating_sub(completed + abandoned),
                    offered_latencies: vec![latency; completed as usize],
                    completed,
                    abandoned,
                    latencies: vec![latency; completed as usize],
                    stream_records: records,
                    stream_values: gauges,
                }
            })
            .collect();
        Trace {
            buckets,
            outstanding: 0,
            stream_names: vec!["backlog".to_owned()],
            valued_names: vec!["backlog".to_owned()],
        }
    }

    #[test]
    fn recovered_when_tail_matches_baseline() {
        let v = judge(&trace(10, 0, Duration::from_millis(2), |_| 0.0), &profile());
        assert_eq!(v.outcome, Outcome::Recovered, "{v}");
    }

    #[test]
    fn collapsed_when_goodput_halves() {
        let v = judge(
            &trace(3, 5, Duration::from_secs(4), |s| s as f64 * 10.0),
            &profile(),
        );
        assert_eq!(v.outcome, Outcome::Collapsed, "{v}");
    }

    #[test]
    fn collapsed_when_backlog_rises_with_degraded_goodput() {
        let v = judge(
            &trace(7, 1, Duration::from_secs(4), |s| s as f64 * 10.0),
            &profile(),
        );
        assert_eq!(v.outcome, Outcome::Collapsed, "{v}");
    }

    #[test]
    fn collapsed_when_everything_completes_late_and_backlog_rises() {
        // Goodput looks fine by count, but every tail request waited seconds and the queue is
        // still growing: the test that caught the first version of this rule.
        let v = judge(
            &trace(10, 0, Duration::from_secs(4), |s| s as f64 * 10.0),
            &profile(),
        );
        assert_eq!(v.outcome, Outcome::Collapsed, "{v}");
    }

    #[test]
    fn draining_when_backlog_falls() {
        let v = judge(
            &trace(8, 0, Duration::from_millis(500), |s| {
                (200 - s as i64 * 10) as f64
            }),
            &profile(),
        );
        assert_eq!(v.outcome, Outcome::Draining, "{v}");
    }
}
