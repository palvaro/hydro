//! Runtime support for the harness that [`overload_check`](crate::overload_check) generates:
//! the run configuration the command passes in, the deployment of one trial, the registry the
//! command reads, and the files a run leaves behind. Nothing here is specific to any program.
//!
//! # Where things end up
//!
//! Under `$SEDGE_RESULTS_DIR` (the command sets it to `target/sedge/`): `results.tsv` has one
//! row per trial, and `traces/` has one per-second TSV per trial, named by crate, function,
//! configuration, factor and trial number.

use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use hydro_deploy::Deployment;
use hydro_lang::compile::deploy::DeployResult;
use hydro_lang::deploy::{HydroDeploy, TrybuildHost};
use hydro_lang::location::external_process::{ExternalBincodeSink, NotMany};

use crate::generator::drive;
use crate::profile::Profile;
use crate::program::{LoadItem, Observer};
use crate::trace::Recorder;
use crate::verdict::{Outcome, Verdict, judge};

/// What the command asks for: which checks, at which overprovisioning factors, how many times,
/// and the shape of the load profile.
#[derive(Debug, Clone)]
pub struct RunConfig {
    /// Substring of the function name to match; empty matches all.
    pub function_filter: String,
    /// Exact configuration name to match; empty matches all.
    pub configuration_filter: String,
    /// Run checks marked `ignore = "..."` too.
    pub include_ignored: bool,
    /// Overprovisioning factors `μ / λ` to run. The baseline offered load is `μ / factor`.
    pub factors: Vec<f64>,
    /// Trials per factor.
    pub trials: usize,
    /// Overrides the check's `mu` (requests per second the service can sustain).
    pub mu: Option<f64>,
    /// Trigger offered load as a multiple of `μ`.
    pub trigger_multiple: f64,
    /// Baseline before the trigger.
    pub before: Duration,
    /// Length of the trigger.
    pub trigger: Duration,
    /// Baseline after the trigger.
    pub tail: Duration,
    /// Wait after the last request before reading the trace.
    pub grace: Duration,
    /// Seed of the first trial's arrival process; trial `n` uses `seed + n`.
    pub seed: u64,
    /// Where to write results.
    pub results_dir: PathBuf,
    /// Extra Cargo features to build the deployed program with (the crate's `sedge` feature is
    /// always on).
    pub features: Vec<String>,
}

impl Default for RunConfig {
    fn default() -> Self {
        Self {
            function_filter: String::new(),
            configuration_filter: String::new(),
            include_ignored: false,
            factors: vec![2.0],
            trials: 1,
            mu: None,
            trigger_multiple: 2.0,
            before: Duration::from_secs(10),
            trigger: Duration::from_secs(5),
            tail: Duration::from_secs(45),
            grace: Duration::from_secs(2),
            seed: 1,
            results_dir: results_dir(),
            features: Vec::new(),
        }
    }
}

impl RunConfig {
    /// The `(factor, trial index)` pairs a check runs, factors in the order given.
    pub fn schedule(&self) -> Vec<(f64, usize)> {
        self.factors
            .iter()
            .flat_map(|&f| (0..self.trials).map(move |t| (f, t)))
            .collect()
    }

    /// The load profile for `factor` at capacity `mu`.
    pub fn profile(&self, mu: f64, factor: f64) -> Profile {
        Profile {
            baseline_rps: mu / factor,
            trigger_rps: mu * self.trigger_multiple,
            before: self.before,
            trigger: self.trigger,
            tail: self.tail,
        }
    }

    /// Parses the command's arguments: `[--option value]... [function-substring]`. See
    /// [`USAGE`].
    pub fn from_args(args: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let mut cfg = Self::default();
        let mut args = args.into_iter();
        let secs = |v: &str, what: &str| -> Result<Duration, String> {
            v.parse::<f64>()
                .ok()
                .filter(|s| *s >= 0.0)
                .map(Duration::from_secs_f64)
                .ok_or_else(|| format!("{what} wants a number of seconds, got `{v}`"))
        };
        while let Some(arg) = args.next() {
            let mut value = |name: &str| -> Result<String, String> {
                args.next().ok_or_else(|| format!("{name} needs a value"))
            };
            match arg.as_str() {
                "--config" => cfg.configuration_filter = value("--config")?,
                "--include-ignored" => cfg.include_ignored = true,
                "--factors" => {
                    let v = value("--factors")?;
                    cfg.factors = v
                        .split(',')
                        .map(|s| {
                            s.trim()
                                .parse::<f64>()
                                .ok()
                                .filter(|f| *f > 0.0)
                                .ok_or_else(|| {
                                    format!("--factors wants positive numbers, got `{s}`")
                                })
                        })
                        .collect::<Result<_, _>>()?;
                }
                "--trials" => {
                    let v = value("--trials")?;
                    cfg.trials =
                        v.parse().ok().filter(|t| *t > 0).ok_or_else(|| {
                            format!("--trials wants a positive integer, got `{v}`")
                        })?;
                }
                "--mu" => {
                    let v = value("--mu")?;
                    cfg.mu = Some(
                        v.parse::<f64>()
                            .ok()
                            .filter(|m| *m > 0.0)
                            .ok_or_else(|| format!("--mu wants a positive number, got `{v}`"))?,
                    );
                }
                "--trigger-multiple" => {
                    let v = value("--trigger-multiple")?;
                    cfg.trigger_multiple =
                        v.parse::<f64>().ok().filter(|m| *m > 0.0).ok_or_else(|| {
                            format!("--trigger-multiple wants a positive number, got `{v}`")
                        })?;
                }
                "--before" => cfg.before = secs(&value("--before")?, "--before")?,
                "--trigger" => cfg.trigger = secs(&value("--trigger")?, "--trigger")?,
                "--tail" => cfg.tail = secs(&value("--tail")?, "--tail")?,
                "--grace" => cfg.grace = secs(&value("--grace")?, "--grace")?,
                "--seed" => {
                    let v = value("--seed")?;
                    cfg.seed = v
                        .parse()
                        .map_err(|_| format!("--seed wants an integer, got `{v}`"))?;
                }
                "--features" => {
                    cfg.features = value("--features")?
                        .split(',')
                        .map(|s| s.trim().to_owned())
                        .filter(|s| !s.is_empty())
                        .collect();
                }
                s if s.starts_with("--") => return Err(format!("unknown option `{s}`\n{USAGE}")),
                s => {
                    if !cfg.function_filter.is_empty() {
                        return Err(format!("unexpected argument `{s}`\n{USAGE}"));
                    }
                    cfg.function_filter = s.to_owned();
                }
            }
        }
        Ok(cfg)
    }
}

/// The command's options.
pub const USAGE: &str = "\
usage: sedge [options] <crate> [function-substring]
  --config NAME            only the configuration named NAME
  --include-ignored        also run checks marked ignore = \"...\"
  --factors F1,F2,...      overprovisioning factors μ/λ to run (default 2)
  --trials N               trials per factor (default 1)
  --mu RPS                 service capacity, overriding the check's `mu`
  --trigger-multiple M     trigger load as a multiple of μ (default 2)
  --before S               seconds of baseline before the trigger (default 10)
  --trigger S              seconds the trigger lasts (default 5)
  --tail S                 seconds of baseline after the trigger (default 45)
  --grace S                seconds to wait for stragglers after the last request (default 2)
  --seed N                 seed of the first trial's arrivals (default 1)
  --features A,B           extra Cargo features for the deployed program";

/// The directory results are written to: `$SEDGE_RESULTS_DIR` if set, otherwise `target/sedge/`
/// under the current directory.
pub fn results_dir() -> PathBuf {
    match std::env::var_os("SEDGE_RESULTS_DIR") {
        Some(dir) => PathBuf::from(dir),
        None => Path::new("target").join("sedge"),
    }
}

/// What the generated code knows about the check it is running.
#[derive(Debug, Clone)]
pub struct Meta {
    pub crate_name: &'static str,
    pub module: &'static str,
    pub function: &'static str,
    pub configuration: &'static str,
    /// The check's `mu`, if the attribute gave one.
    pub mu: Option<f64>,
}

/// The result of one trial.
#[derive(Debug, Clone)]
pub struct TrialResult {
    pub function: &'static str,
    pub configuration: &'static str,
    pub factor: f64,
    pub trial: usize,
    pub seed: u64,
    pub mu: f64,
    pub lambda: f64,
    pub verdict: Verdict,
    /// Arrivals the generator fell behind on; nonzero means the offered load was below the
    /// profile.
    pub late: u64,
    pub trace_path: Option<PathBuf>,
}

/// A generated overload check that the command can discover.
#[doc(hidden)]
#[derive(Clone, Copy)]
pub struct RegisteredCheck {
    pub function: &'static str,
    pub configuration: &'static str,
    pub ignore: Option<&'static str>,
    pub run: fn(&RunConfig) -> Vec<TrialResult>,
}

static REGISTERED_CHECKS: OnceLock<Mutex<Vec<RegisteredCheck>>> = OnceLock::new();

/// Adds a generated check to the command's process-local registry.
#[doc(hidden)]
pub fn register_check(check: RegisteredCheck) {
    REGISTERED_CHECKS
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .expect("overload check registry lock poisoned")
        .push(check);
}

/// Runs the registered checks `run` selects. An error means no check matched.
pub fn run_registered_checks(run: &RunConfig) -> Result<Vec<TrialResult>, String> {
    let mut checks = REGISTERED_CHECKS
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .map_err(|_| "overload check registry lock poisoned".to_owned())?
        .clone();
    checks.retain(|c| c.function.contains(&run.function_filter));
    checks.retain(|c| {
        run.configuration_filter.is_empty() || c.configuration == run.configuration_filter
    });
    checks.retain(|c| run.include_ignored || c.ignore.is_none());
    checks.sort_by_key(|c| (c.function, c.configuration));
    if checks.is_empty() {
        let mut msg = "no overload checks found".to_owned();
        if !run.function_filter.is_empty() {
            msg += &format!(" matching function `{}`", run.function_filter);
        }
        if !run.configuration_filter.is_empty() {
            msg += &format!(" with configuration `{}`", run.configuration_filter);
        }
        return Err(msg);
    }
    fs::create_dir_all(run.results_dir.join("traces")).map_err(|e| {
        format!(
            "could not create {}: {e}",
            run.results_dir.join("traces").display()
        )
    })?;
    let mut results = Vec::new();
    for check in checks {
        eprintln!(
            "Running {} ({}) at factors {:?}, {} trial(s) each…",
            check.function, check.configuration, run.factors, run.trials
        );
        results.extend((check.run)(run));
    }
    Ok(results)
}

/// Runs a future to completion on a fresh multi-threaded Tokio runtime. The generated check
/// calls this around its trials; the deployment machinery needs a runtime.
pub fn block_on<F: Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("could not start a Tokio runtime")
        .block_on(future)
}

/// A localhost host for one deployed process, built optimized and with the crate's `sedge`
/// feature on (so that the program's `Observed` impl and its quotes are part of the build) plus
/// whatever `--features` asked for.
pub fn host(deployment: &mut Deployment, run: &RunConfig) -> TrybuildHost {
    let mut host = TrybuildHost::new(deployment.Localhost())
        .rustflags("-C opt-level=3 -C codegen-units=1 -C strip=none -C debuginfo=2 -C lto=off")
        .feature("sedge");
    for f in &run.features {
        host = host.feature(f.clone());
    }
    host
}

/// Deploys, runs and judges one trial. `nodes` is the deployed flow; `load` is the generator's
/// port; `observer` holds the program's registered outputs.
#[expect(clippy::too_many_arguments, reason = "called only by generated code")]
pub async fn run_trial<T>(
    deployment: &mut Deployment,
    nodes: &DeployResult<'_, HydroDeploy>,
    load: ExternalBincodeSink<T, NotMany>,
    observer: Observer<'_>,
    meta: &Meta,
    run: &RunConfig,
    factor: f64,
    trial: usize,
) -> TrialResult
where
    T: LoadItem,
{
    let mu = run.mu.or(meta.mu).unwrap_or_else(|| {
        panic!(
            "{} ({}) gives no `mu`; add `mu = <requests per second>` to the attribute or pass --mu",
            meta.function, meta.configuration
        )
    });
    let profile = run.profile(mu, factor);
    let seed = run.seed + trial as u64;
    eprintln!(
        "  factor {factor}, trial {trial}: μ = {mu:.0} rps, λ = {:.1} rps, trigger {:.0} rps for {:.0}s, tail {:.0}s",
        profile.baseline_rps,
        profile.trigger_rps,
        run.trigger.as_secs_f64(),
        run.tail.as_secs_f64()
    );

    deployment.deploy().await.expect("deployment failed");
    let mut load = nodes.connect(load).await;
    let mut completions = match observer.completions {
        Some(p) => Some(nodes.connect(p).await),
        None => None,
    };
    let mut abandoned = match observer.abandoned {
        Some(p) => Some(nodes.connect(p).await),
        None => None,
    };
    let mut gauges = Vec::new();
    for (name, p) in observer.gauges {
        gauges.push((name, nodes.connect(p).await));
    }
    let mut counters = Vec::new();
    for (name, p) in observer.counters {
        counters.push((name, nodes.connect(p).await));
    }
    deployment.start().await.expect("deployment did not start");
    // Let the processes settle and their timers start before time zero.
    tokio::time::sleep(Duration::from_secs(1)).await;

    let recorder = Recorder::new();
    let readers = {
        let r = recorder.clone();
        async move {
            let read_completions = async {
                if let Some(s) = &mut completions {
                    while let Some(id) = s.next().await {
                        r.completed(id);
                    }
                }
            };
            let read_abandoned = async {
                if let Some(s) = &mut abandoned {
                    while let Some(id) = s.next().await {
                        r.abandoned(id);
                    }
                }
            };
            let read_gauges = futures::future::join_all(gauges.iter_mut().map(|(name, s)| {
                let r = r.clone();
                async move {
                    while let Some(v) = s.next().await {
                        r.gauge(name, v);
                    }
                }
            }));
            let read_counters = futures::future::join_all(counters.iter_mut().map(|(name, s)| {
                let r = r.clone();
                async move {
                    while let Some(v) = s.next().await {
                        r.counter(name, v);
                    }
                }
            }));
            futures::join!(read_completions, read_abandoned, read_gauges, read_counters);
        }
    };
    let mut readers = std::pin::pin!(readers);

    let driven = tokio::select! {
        d = drive(profile, seed, &recorder, async |id| load.send(T::from_id(id)).await) => d,
        () = &mut readers => panic!("the program's output streams ended while the trial was running"),
    };
    let driven = match driven {
        Ok(d) => d,
        Err((e, d)) => {
            eprintln!(
                "  generator stopped early after {} requests: {e}",
                d.offered
            );
            d
        }
    };
    tokio::select! {
        () = tokio::time::sleep(run.grace) => {}
        () = &mut readers => panic!("the program's output streams ended while the trial was running"),
    }

    let trace = recorder.trace(profile.total());
    let verdict = judge(&trace, &profile);
    if let Err(e) = deployment.stop().await {
        eprintln!("  (deployment did not stop cleanly: {e})");
    }

    let trace_name = format!(
        "{}.{}.{}.factor{}.trial{}.tsv",
        meta.crate_name, meta.function, meta.configuration, factor, trial
    );
    let trace_path = run.results_dir.join("traces").join(trace_name);
    let trace_path = match fs::write(&trace_path, trace.to_tsv()) {
        Ok(()) => Some(trace_path),
        Err(e) => {
            eprintln!("  could not write {}: {e}", trace_path.display());
            None
        }
    };
    let result = TrialResult {
        function: meta.function,
        configuration: meta.configuration,
        factor,
        trial,
        seed,
        mu,
        lambda: profile.baseline_rps,
        verdict,
        late: driven.late,
        trace_path,
    };
    eprintln!("  {}", result.verdict.outcome);
    record(run, &result);
    result
}

/// Appends one row for `result` to `results.tsv`.
fn record(run: &RunConfig, r: &TrialResult) {
    let ms = |d: Option<Duration>| {
        d.map(|d| format!("{:.1}", d.as_secs_f64() * 1e3))
            .unwrap_or_else(|| "-".to_owned())
    };
    let path = run.results_dir.join("results.tsv");
    let write = || -> std::io::Result<()> {
        let new = !path.exists();
        let mut f = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        if new {
            writeln!(
                f,
                "function\tconfiguration\tfactor\ttrial\tseed\tmu\tlambda\toutcome\ttail_goodput\ttail_p50_ms\ttail_p99_ms\ttail_abandoned\toutstanding\tlate\treason\ttrace"
            )?;
        }
        writeln!(
            f,
            "{}\t{}\t{}\t{}\t{}\t{:.1}\t{:.2}\t{}\t{:.3}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            r.function,
            r.configuration,
            r.factor,
            r.trial,
            r.seed,
            r.mu,
            r.lambda,
            r.verdict.outcome,
            r.verdict.tail.completion_fraction,
            ms(r.verdict.tail.p50),
            ms(r.verdict.tail.p99),
            r.verdict.tail.abandoned,
            r.verdict.outstanding,
            r.late,
            r.verdict.reason,
            r.trace_path
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_default()
        )
    };
    if let Err(e) = write() {
        eprintln!(
            "sedge: could not record results under {}: {e}",
            path.display()
        );
    }
}

/// Prints every trial's verdict, then one summary line per check and factor with the count of
/// each outcome.
pub fn print_results(results: &[TrialResult]) {
    for r in results {
        println!(
            "{} ({}) factor {} trial {}: {}",
            r.function, r.configuration, r.factor, r.trial, r.verdict
        );
        if r.late > 0 {
            println!(
                "  WARNING: the generator fell behind on {} arrivals; offered load was below the profile",
                r.late
            );
        }
    }
    println!();
    println!("{}", Summary::new(results));
}

/// Outcome counts per check and factor.
pub struct Summary<'r> {
    rows: BTreeMap<(&'static str, &'static str, u64), Vec<&'r TrialResult>>,
}

impl<'r> Summary<'r> {
    pub fn new(results: &'r [TrialResult]) -> Self {
        let mut rows: BTreeMap<_, Vec<&TrialResult>> = BTreeMap::new();
        for r in results {
            rows.entry((r.function, r.configuration, r.factor.to_bits()))
                .or_default()
                .push(r);
        }
        Self { rows }
    }
}

impl fmt::Display for Summary<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "{:<24} {:<16} {:>6} {:>8} {:>8} {:>9} {:>8} {:>9}",
            "function", "configuration", "factor", "μ", "λ", "recovered", "draining", "collapsed"
        )?;
        for ((function, configuration, _), trials) in &self.rows {
            let count = |o: Outcome| trials.iter().filter(|t| t.verdict.outcome == o).count();
            let first = trials[0];
            writeln!(
                f,
                "{:<24} {:<16} {:>6} {:>8.0} {:>8.1} {:>9} {:>8} {:>9}",
                function,
                configuration,
                first.factor,
                first.mu,
                first.lambda,
                count(Outcome::Recovered),
                count(Outcome::Draining),
                count(Outcome::Collapsed)
            )?;
        }
        Ok(())
    }
}
