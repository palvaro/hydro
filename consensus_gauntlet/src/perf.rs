//! Host-independent performance measurements for the consensus gauntlet.
//!
//! Benchmark deployments emit one JSON record per measurement window on
//! stdout. Keeping parsing and summarization independent of Hydro's deployment
//! types lets localhost and ECS runs feed the same report pipeline.

use std::error::Error;
use std::fmt;

use serde::{Deserialize, Serialize};

/// Prefix used to distinguish machine-readable metrics from application logs.
pub const METRIC_PREFIX: &str = "CONSENSUS_GAUNTLET_METRIC ";

/// Number of initial windows discarded while the deployment warms up.
pub const DEFAULT_WARMUP_WINDOWS: usize = 3;
/// Number of windows included in steady-state statistics.
pub const DEFAULT_STEADY_WINDOWS: usize = 12;

/// One complete benchmark measurement window.
///
/// Latency values are milliseconds. A single record intentionally contains
/// both throughput and latency so interleaved process output cannot pair values
/// from different windows.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WindowMetrics {
    pub sequence: u64,
    pub throughput_rps: f64,
    pub p50_ms: f64,
    pub p99_ms: f64,
    pub p999_ms: f64,
    pub samples: u64,
}

impl WindowMetrics {
    fn validate(&self) -> Result<(), PerfError> {
        for (field, value) in [
            ("throughput_rps", self.throughput_rps),
            ("p50_ms", self.p50_ms),
            ("p99_ms", self.p99_ms),
            ("p999_ms", self.p999_ms),
        ] {
            if !value.is_finite() || value < 0.0 {
                return Err(PerfError::InvalidValue {
                    sequence: self.sequence,
                    field,
                    value,
                });
            }
        }
        Ok(())
    }
}

/// Measurement-window policy shared by every backend and execution target.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PerfConfig {
    pub warmup_windows: usize,
    pub steady_windows: usize,
}

impl PerfConfig {
    pub const fn total_windows(self) -> usize {
        self.warmup_windows + self.steady_windows
    }
}

impl Default for PerfConfig {
    fn default() -> Self {
        Self {
            warmup_windows: DEFAULT_WARMUP_WINDOWS,
            steady_windows: DEFAULT_STEADY_WINDOWS,
        }
    }
}

/// Where benchmark processes execute (or, for `EcsExport`, will execute).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionTarget {
    Localhost,
    /// An ECS manifest was generated, but tasks were not launched by Hydro.
    EcsExport,
    /// A completed ECS run whose task logs were collected externally.
    Ecs,
}

impl fmt::Display for ExecutionTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Localhost => "localhost",
            Self::EcsExport => "ecs-export",
            Self::Ecs => "ecs",
        })
    }
}

/// Deployment metadata recorded alongside results to prevent unlike targets
/// or ECS task configurations from being compared as if they were identical.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionMetadata {
    pub target: ExecutionTarget,
    pub host: Option<String>,
    pub region: Option<String>,
    pub ecs_cluster: Option<String>,
    pub task_cpu: Option<u32>,
    pub task_memory_mib: Option<u32>,
    pub image_digest: Option<String>,
}

impl ExecutionMetadata {
    pub fn localhost(host: impl Into<String>) -> Self {
        Self {
            target: ExecutionTarget::Localhost,
            host: Some(host.into()),
            region: None,
            ecs_cluster: None,
            task_cpu: None,
            task_memory_mib: None,
            image_digest: None,
        }
    }
}

/// Descriptive statistics over the steady-state throughput windows.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct ThroughputStats {
    pub min: f64,
    pub median: f64,
    pub mean: f64,
    pub max: f64,
}

/// A complete performance run. `windows` retains the latency growth curve,
/// including warmup, while `throughput` is computed only from steady state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PerfSummary {
    pub config: PerfConfig,
    pub windows: Vec<WindowMetrics>,
    pub throughput: ThroughputStats,
}

impl PerfSummary {
    /// Validate and summarize a complete run.
    ///
    /// Exactly `warmup_windows + steady_windows` records are required. This
    /// prevents a truncated run or an accidental extra window from silently
    /// changing the comparison basis.
    pub fn new(windows: Vec<WindowMetrics>, config: PerfConfig) -> Result<Self, PerfError> {
        let expected = config.total_windows();
        if windows.len() != expected {
            return Err(PerfError::WrongWindowCount {
                expected,
                actual: windows.len(),
            });
        }
        if config.steady_windows == 0 {
            return Err(PerfError::NoSteadyWindows);
        }
        for window in &windows {
            window.validate()?;
        }

        let steady = &windows[config.warmup_windows..];
        debug_assert_eq!(steady.len(), config.steady_windows);
        let mut sorted: Vec<f64> = steady.iter().map(|w| w.throughput_rps).collect();
        sorted.sort_by(f64::total_cmp);

        let middle = sorted.len() / 2;
        let median = if sorted.len() % 2 == 0 {
            (sorted[middle - 1] + sorted[middle]) / 2.0
        } else {
            sorted[middle]
        };
        let mean = sorted.iter().sum::<f64>() / sorted.len() as f64;
        let throughput = ThroughputStats {
            min: sorted[0],
            median,
            mean,
            max: sorted[sorted.len() - 1],
        };

        Ok(Self {
            config,
            windows,
            throughput,
        })
    }

    pub fn steady_windows(&self) -> &[WindowMetrics] {
        &self.windows[self.config.warmup_windows..]
    }
}

/// Policy for a classic closed-loop concurrency saturation sweep.
///
/// Each point is a fresh deployment. `concurrency` is the total number of
/// virtual clients (and therefore the exact maximum number of in-flight
/// requests) across all client nodes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SweepConfig {
    pub concurrency: Vec<usize>,
    pub repetitions: usize,
    pub client_nodes: usize,
    pub windows: PerfConfig,
}

impl Default for SweepConfig {
    fn default() -> Self {
        Self {
            concurrency: vec![1, 2, 4, 8, 16, 32, 64, 128, 256, 512],
            repetitions: 3,
            client_nodes: 1,
            windows: PerfConfig::default(),
        }
    }
}

impl SweepConfig {
    pub fn validate(&self) -> Result<(), PerfError> {
        if self.concurrency.is_empty() {
            return Err(PerfError::InvalidSweep(
                "at least one concurrency point is required",
            ));
        }
        if self.repetitions == 0 {
            return Err(PerfError::InvalidSweep(
                "at least one repetition is required",
            ));
        }
        if self.client_nodes == 0 {
            return Err(PerfError::InvalidSweep(
                "at least one client node is required",
            ));
        }
        let mut previous = 0;
        for &concurrency in &self.concurrency {
            if concurrency == 0 {
                return Err(PerfError::InvalidSweep("concurrency must be non-zero"));
            }
            if concurrency <= previous {
                return Err(PerfError::InvalidSweep(
                    "concurrency points must be strictly increasing",
                ));
            }
            if concurrency % self.client_nodes != 0 {
                return Err(PerfError::InvalidSweep(
                    "each concurrency point must be divisible by client_nodes",
                ));
            }
            previous = concurrency;
        }
        if self.windows.steady_windows == 0 {
            return Err(PerfError::NoSteadyWindows);
        }
        Ok(())
    }
}

/// Distribution of one per-repetition statistic at a saturation point.
///
/// Each input is first collapsed within its repetition (median over the twelve
/// steady windows). These fields then summarize those repetition medians; raw
/// windows remain available through [`SaturationPoint::repetitions`].
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub struct DistributionStats {
    pub min: f64,
    pub median: f64,
    pub mean: f64,
    pub max: f64,
}

impl DistributionStats {
    fn from_values(mut values: Vec<f64>) -> Result<Self, PerfError> {
        if values.is_empty() {
            return Err(PerfError::NoRepetitions);
        }
        if values
            .iter()
            .any(|value| !value.is_finite() || *value < 0.0)
        {
            return Err(PerfError::InvalidSweep(
                "repetition summaries must be finite and non-negative",
            ));
        }
        values.sort_by(f64::total_cmp);
        Ok(Self {
            min: values[0],
            median: median_sorted(&values),
            mean: values.iter().sum::<f64>() / values.len() as f64,
            max: values[values.len() - 1],
        })
    }
}

/// One requested-concurrency point and all of its independently deployed runs.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SaturationPoint {
    pub requested_concurrency: usize,
    pub client_nodes: usize,
    pub concurrency_per_node: usize,
    pub repetitions: Vec<PerfSummary>,
    pub throughput_rps: DistributionStats,
    pub p50_ms: DistributionStats,
    pub p99_ms: DistributionStats,
    pub p999_ms: DistributionStats,
}

impl SaturationPoint {
    pub fn new(
        requested_concurrency: usize,
        client_nodes: usize,
        repetitions: Vec<PerfSummary>,
        expected_repetitions: usize,
    ) -> Result<Self, PerfError> {
        if requested_concurrency == 0 || client_nodes == 0 {
            return Err(PerfError::InvalidSweep(
                "requested concurrency and client_nodes must be non-zero",
            ));
        }
        if requested_concurrency % client_nodes != 0 {
            return Err(PerfError::InvalidSweep(
                "requested concurrency must be divisible by client_nodes",
            ));
        }
        if repetitions.len() != expected_repetitions {
            return Err(PerfError::WrongRepetitionCount {
                expected: expected_repetitions,
                actual: repetitions.len(),
            });
        }
        if repetitions.is_empty() {
            return Err(PerfError::NoRepetitions);
        }

        let throughput = repetitions
            .iter()
            .map(|run| run.throughput.median)
            .collect();
        let p50 = repetitions
            .iter()
            .map(|run| steady_median(run, |window| window.p50_ms))
            .collect();
        let p99 = repetitions
            .iter()
            .map(|run| steady_median(run, |window| window.p99_ms))
            .collect();
        let p999 = repetitions
            .iter()
            .map(|run| steady_median(run, |window| window.p999_ms))
            .collect();

        Ok(Self {
            requested_concurrency,
            client_nodes,
            concurrency_per_node: requested_concurrency / client_nodes,
            throughput_rps: DistributionStats::from_values(throughput)?,
            p50_ms: DistributionStats::from_values(p50)?,
            p99_ms: DistributionStats::from_values(p99)?,
            p999_ms: DistributionStats::from_values(p999)?,
            repetitions,
        })
    }
}

/// An optional, descriptive saturation-knee annotation (not a scalar score).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct KneeAnnotation {
    pub point_index: usize,
    pub requested_concurrency: usize,
    pub detail: String,
}

/// A full backend curve over requested closed-loop concurrency.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SaturationCurve {
    pub backend: String,
    pub execution: ExecutionMetadata,
    pub config: SweepConfig,
    pub points: Vec<SaturationPoint>,
    pub knee: Option<KneeAnnotation>,
}

impl SaturationCurve {
    pub fn new(
        backend: impl Into<String>,
        execution: ExecutionMetadata,
        config: SweepConfig,
        points: Vec<SaturationPoint>,
    ) -> Result<Self, PerfError> {
        config.validate()?;
        if points.len() != config.concurrency.len() {
            return Err(PerfError::WrongSweepPointCount {
                expected: config.concurrency.len(),
                actual: points.len(),
            });
        }
        for (point, expected_concurrency) in points.iter().zip(&config.concurrency) {
            if point.requested_concurrency != *expected_concurrency {
                return Err(PerfError::InvalidSweep(
                    "point concurrency does not match sweep configuration",
                ));
            }
            if point.client_nodes != config.client_nodes {
                return Err(PerfError::InvalidSweep(
                    "point client_nodes does not match sweep configuration",
                ));
            }
            if point.repetitions.len() != config.repetitions {
                return Err(PerfError::WrongRepetitionCount {
                    expected: config.repetitions,
                    actual: point.repetitions.len(),
                });
            }
        }
        let knee = detect_knee(&points);
        Ok(Self {
            backend: backend.into(),
            execution,
            config,
            points,
            knee,
        })
    }
}

fn steady_median(run: &PerfSummary, value: impl Fn(&WindowMetrics) -> f64) -> f64 {
    let mut values: Vec<_> = run.steady_windows().iter().map(value).collect();
    values.sort_by(f64::total_cmp);
    median_sorted(&values)
}

fn median_sorted(values: &[f64]) -> f64 {
    let middle = values.len() / 2;
    if values.len() % 2 == 0 {
        (values[middle - 1] + values[middle]) / 2.0
    } else {
        values[middle]
    }
}

fn detect_knee(points: &[SaturationPoint]) -> Option<KneeAnnotation> {
    let transition_saturates = |before: &SaturationPoint, after: &SaturationPoint| {
        let throughput_before = before.throughput_rps.median;
        let p50_before = before.p50_ms.median;
        if throughput_before <= 0.0 || p50_before <= 0.0 {
            return false;
        }
        let throughput_gain = (after.throughput_rps.median - throughput_before) / throughput_before;
        let p50_growth = (after.p50_ms.median - p50_before) / p50_before;
        throughput_gain < 0.10 && p50_growth > 0.25
    };

    for index in 1..points.len().saturating_sub(1) {
        if transition_saturates(&points[index - 1], &points[index])
            && transition_saturates(&points[index], &points[index + 1])
        {
            return Some(KneeAnnotation {
                point_index: index,
                requested_concurrency: points[index].requested_concurrency,
                detail: "two consecutive points add <10% throughput while p50 rises >25%"
                    .to_owned(),
            });
        }
    }
    None
}

/// Errors produced while decoding or summarizing benchmark metrics.
#[derive(Debug)]
pub enum PerfError {
    MalformedMetric(serde_json::Error),
    WrongWindowCount {
        expected: usize,
        actual: usize,
    },
    WrongRepetitionCount {
        expected: usize,
        actual: usize,
    },
    WrongSweepPointCount {
        expected: usize,
        actual: usize,
    },
    NoSteadyWindows,
    NoRepetitions,
    InvalidSweep(&'static str),
    InvalidValue {
        sequence: u64,
        field: &'static str,
        value: f64,
    },
}

impl fmt::Display for PerfError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MalformedMetric(error) => write!(f, "malformed gauntlet metric: {error}"),
            Self::WrongWindowCount { expected, actual } => {
                write!(
                    f,
                    "expected exactly {expected} metric windows, received {actual}"
                )
            }
            Self::WrongRepetitionCount { expected, actual } => write!(
                f,
                "expected exactly {expected} repetitions, received {actual}"
            ),
            Self::WrongSweepPointCount { expected, actual } => write!(
                f,
                "expected exactly {expected} sweep points, received {actual}"
            ),
            Self::NoSteadyWindows => f.write_str("at least one steady-state window is required"),
            Self::NoRepetitions => f.write_str("at least one repetition is required"),
            Self::InvalidSweep(reason) => write!(f, "invalid saturation sweep: {reason}"),
            Self::InvalidValue {
                sequence,
                field,
                value,
            } => write!(
                f,
                "metric window {sequence} has invalid {field} value {value}"
            ),
        }
    }
}

impl Error for PerfError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::MalformedMetric(error) => Some(error),
            _ => None,
        }
    }
}

/// Encode one metric as the canonical prefixed stdout record.
pub fn format_metric_line(metric: &WindowMetrics) -> Result<String, PerfError> {
    metric.validate()?;
    serde_json::to_string(metric)
        .map(|json| format!("{METRIC_PREFIX}{json}"))
        .map_err(PerfError::MalformedMetric)
}

/// Parse one stdout line. Unrelated log lines return `Ok(None)`; a line that
/// claims to be a gauntlet metric but contains invalid JSON is an error.
pub fn parse_metric_line(line: &str) -> Result<Option<WindowMetrics>, PerfError> {
    let Some(json) = line.trim().strip_prefix(METRIC_PREFIX) else {
        return Ok(None);
    };
    let metric: WindowMetrics = serde_json::from_str(json).map_err(PerfError::MalformedMetric)?;
    metric.validate()?;
    Ok(Some(metric))
}

/// Extract metrics from noisy, interleaved deployment logs.
pub fn parse_metric_lines<'a>(
    lines: impl IntoIterator<Item = &'a str>,
) -> Result<Vec<WindowMetrics>, PerfError> {
    lines
        .into_iter()
        .filter_map(|line| parse_metric_line(line).transpose())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn metric(sequence: u64, throughput_rps: f64) -> WindowMetrics {
        WindowMetrics {
            sequence,
            throughput_rps,
            p50_ms: sequence as f64,
            p99_ms: sequence as f64 + 1.0,
            p999_ms: sequence as f64 + 2.0,
            samples: 100,
        }
    }

    #[test]
    fn default_run_discards_three_and_summarizes_exactly_twelve() {
        let windows = (0..15)
            .map(|sequence| metric(sequence, sequence as f64))
            .collect();
        let summary = PerfSummary::new(windows, PerfConfig::default()).unwrap();

        assert_eq!(summary.windows.len(), 15);
        assert_eq!(summary.steady_windows().len(), 12);
        assert_eq!(summary.steady_windows()[0].sequence, 3);
        assert_eq!(summary.throughput.min, 3.0);
        assert_eq!(summary.throughput.median, 8.5);
        assert_eq!(summary.throughput.mean, 8.5);
        assert_eq!(summary.throughput.max, 14.0);
    }

    #[test]
    fn preserves_every_latency_growth_point() {
        let windows: Vec<_> = (0..15).map(|sequence| metric(sequence, 10_000.0)).collect();
        let expected_p50: Vec<_> = windows.iter().map(|window| window.p50_ms).collect();
        let summary = PerfSummary::new(windows, PerfConfig::default()).unwrap();

        assert_eq!(
            summary
                .windows
                .iter()
                .map(|window| window.p50_ms)
                .collect::<Vec<_>>(),
            expected_p50
        );
    }

    #[test]
    fn parser_ignores_noise_and_rejects_malformed_prefixed_lines() {
        let encoded = format_metric_line(&metric(7, 42.0)).unwrap();
        let parsed = parse_metric_lines(["ordinary process log", &encoded]).unwrap();
        assert_eq!(parsed, vec![metric(7, 42.0)]);

        assert!(matches!(
            parse_metric_line("CONSENSUS_GAUNTLET_METRIC {not json}"),
            Err(PerfError::MalformedMetric(_))
        ));
    }

    #[test]
    fn even_sample_median_averages_the_middle_pair() {
        let config = PerfConfig {
            warmup_windows: 0,
            steady_windows: 4,
        };
        let windows = [10.0, 40.0, 20.0, 30.0]
            .into_iter()
            .enumerate()
            .map(|(sequence, throughput)| metric(sequence as u64, throughput))
            .collect();
        let summary = PerfSummary::new(windows, config).unwrap();
        assert_eq!(summary.throughput.median, 25.0);
    }

    fn summary(steady_throughput: f64, p50: f64, p99: f64, p999: f64) -> PerfSummary {
        let windows = (0..15)
            .map(|sequence| WindowMetrics {
                sequence,
                throughput_rps: if sequence < 3 { 0.0 } else { steady_throughput },
                p50_ms: if sequence < 3 { 0.0 } else { p50 },
                p99_ms: if sequence < 3 { 0.0 } else { p99 },
                p999_ms: if sequence < 3 { 0.0 } else { p999 },
                samples: if sequence < 3 {
                    0
                } else {
                    steady_throughput as u64
                },
            })
            .collect();
        PerfSummary::new(windows, PerfConfig::default()).unwrap()
    }

    #[test]
    fn default_sweep_is_geometric_and_valid() {
        let sweep = SweepConfig::default();
        assert_eq!(sweep.concurrency, [1, 2, 4, 8, 16, 32, 64, 128, 256, 512]);
        assert_eq!(sweep.repetitions, 3);
        assert_eq!(sweep.client_nodes, 1);
        sweep.validate().unwrap();
    }

    #[test]
    fn saturation_point_collapses_within_runs_then_across_repetitions() {
        let runs = vec![
            summary(100.0, 1.0, 2.0, 3.0),
            summary(300.0, 3.0, 6.0, 9.0),
            summary(200.0, 2.0, 4.0, 6.0),
        ];
        let point = SaturationPoint::new(32, 2, runs, 3).unwrap();
        assert_eq!(point.concurrency_per_node, 16);
        assert_eq!(point.throughput_rps.min, 100.0);
        assert_eq!(point.throughput_rps.median, 200.0);
        assert_eq!(point.throughput_rps.mean, 200.0);
        assert_eq!(point.throughput_rps.max, 300.0);
        assert_eq!(point.p50_ms.median, 2.0);
        assert_eq!(point.p99_ms.median, 4.0);
        assert_eq!(point.p999_ms.median, 6.0);
        assert_eq!(point.repetitions.len(), 3);
    }

    #[test]
    fn sweep_validation_rejects_ambiguous_or_incomplete_points() {
        assert!(matches!(
            SweepConfig {
                concurrency: vec![1, 1],
                ..SweepConfig::default()
            }
            .validate(),
            Err(PerfError::InvalidSweep(_))
        ));
        assert!(matches!(
            SaturationPoint::new(3, 2, vec![summary(1.0, 1.0, 1.0, 1.0)], 1),
            Err(PerfError::InvalidSweep(_))
        ));
        assert!(matches!(
            SaturationPoint::new(2, 1, vec![summary(1.0, 1.0, 1.0, 1.0)], 3),
            Err(PerfError::WrongRepetitionCount {
                expected: 3,
                actual: 1
            })
        ));
    }

    #[test]
    fn curve_detects_sustained_saturation_knee() {
        let mk = |concurrency, throughput, p50| {
            SaturationPoint::new(
                concurrency,
                1,
                vec![summary(throughput, p50, p50 * 2.0, p50 * 3.0)],
                1,
            )
            .unwrap()
        };
        let config = SweepConfig {
            concurrency: vec![1, 2, 4, 8],
            repetitions: 1,
            client_nodes: 1,
            windows: PerfConfig::default(),
        };
        let curve = SaturationCurve::new(
            "backend",
            ExecutionMetadata::localhost("host"),
            config,
            vec![
                mk(1, 100.0, 1.0),
                mk(2, 150.0, 1.1),
                mk(4, 158.0, 1.5),
                mk(8, 162.0, 2.1),
            ],
        )
        .unwrap();
        assert_eq!(
            curve.knee.as_ref().map(|knee| knee.requested_concurrency),
            Some(4)
        );
    }

    #[test]
    fn truncated_and_non_finite_runs_are_errors() {
        assert!(matches!(
            PerfSummary::new(vec![metric(0, 1.0)], PerfConfig::default()),
            Err(PerfError::WrongWindowCount {
                expected: 15,
                actual: 1
            })
        ));

        let mut windows: Vec<_> = (0..15)
            .map(|sequence| metric(sequence, sequence as f64))
            .collect();
        windows[4].p50_ms = f64::NAN;
        assert!(matches!(
            PerfSummary::new(windows, PerfConfig::default()),
            Err(PerfError::InvalidValue {
                sequence: 4,
                field: "p50_ms",
                ..
            })
        ));
    }
}
