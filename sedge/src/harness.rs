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
use std::pin::Pin;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use futures::{Sink, SinkExt, StreamExt};
use hydro_deploy::Deployment;
use hydro_lang::compile::deploy::DeployResult;
use hydro_lang::deploy::{HydroDeploy, TrybuildHost};

use crate::generator::drive;
use crate::profile::Profile;
use crate::program::Observer;
use crate::trace::{Recorder, Sample};
use crate::verdict::{Outcome, StreamKind, StreamKinds, Verdict, judge_with_kinds};

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
    /// Overrides the check's `mu` (rounds of workload per second the service can sustain).
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
    /// Capacity dips: during the trigger window, the named timer's period is multiplied by the
    /// factor. A dip with `--trigger-multiple 1` is a trigger that removes capacity instead of
    /// adding load.
    pub dips: Vec<(String, f64)>,
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
            dips: Vec::new(),
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
                "--dip" => {
                    let v = value("--dip")?;
                    let (name, k) = v
                        .split_once('=')
                        .ok_or_else(|| format!("--dip wants `timer=factor`, got `{v}`"))?;
                    let k = k
                        .trim()
                        .parse::<f64>()
                        .ok()
                        .filter(|k| *k > 0.0)
                        .ok_or_else(|| format!("--dip wants a positive factor, got `{k}`"))?;
                    cfg.dips.push((name.trim().to_owned(), k));
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
  --dip TIMER=K            stretch TIMER's period by K during the trigger (a capacity dip; repeatable)
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

/// One of the program's timers, driven by the harness over an external port so that its period
/// can be changed during the trigger (see [`RunConfig::dips`]). A timer on a cluster of `members`
/// fires at every member once per period, the members staggered by `period / members`: the
/// deployment's deterministic form of the per-member phase a real deployment's independent clocks
/// (or Raft's randomized election timeouts) have, and the shape Cress's staggered timer closures
/// take. A process is a cluster of one.
pub struct TimerDrive {
    pub name: String,
    pub period: Duration,
    /// `(member raw id, ())`; for a process the member is always 0.
    pub sink: Pin<Box<dyn Sink<(u32, ()), Error = std::io::Error>>>,
    pub members: u32,
}

/// A process timer's sink, which takes `()`, as the `(member, ())` sink [`TimerDrive`] wants.
pub fn process_timer_sink(
    sink: Pin<Box<dyn Sink<(), Error = std::io::Error>>>,
) -> Pin<Box<dyn Sink<(u32, ()), Error = std::io::Error>>> {
    Box::pin(sink.with(|(_, ()): (u32, ())| futures::future::ready(Ok::<(), std::io::Error>(()))))
}

/// Drives one timer from `start`: at every member, an element every `period`, the members
/// staggered by `period / members`, the period stretched by the dip factor while the profile is in
/// its trigger window. Runs until the sink closes.
async fn drive_timer(mut timer: TimerDrive, start: Instant, profile: Profile, dip: Option<f64>) {
    let members = timer.members.max(1);
    let mut next: Vec<Instant> = (0..members)
        .map(|m| start + timer.period.mul_f64(m as f64 / members as f64))
        .collect();
    loop {
        let (m, due) = next
            .iter()
            .copied()
            .enumerate()
            .min_by_key(|(_, t)| *t)
            .unwrap();
        tokio::time::sleep_until(due.into()).await;
        if timer.sink.send((m as u32, ())).await.is_err() {
            return;
        }
        let t = due.saturating_duration_since(start);
        let stretched = dip.is_some_and(|_| t >= profile.trigger_start() && t < profile.trigger_end());
        let period = if stretched {
            timer.period.mul_f64(dip.unwrap())
        } else {
            timer.period
        };
        next[m] = due + period;
    }
}

/// Reads Cress's conservation table for `meta`'s function and configuration, if one has been
/// written: `$CRESS_RESULTS_DIR/conservation.tsv`, or `conservation.tsv` under the `cress/`
/// sibling of the results directory (`target/cress/` beside `target/sedge/`). Later rows for the
/// same stream win. Returns the kind of every stream named there.
pub fn cress_kinds(run: &RunConfig, meta: &Meta) -> StreamKinds {
    let path = match std::env::var_os("CRESS_RESULTS_DIR") {
        Some(dir) => PathBuf::from(dir).join("conservation.tsv"),
        None => run
            .results_dir
            .parent()
            .map(|p| p.join("cress").join("conservation.tsv"))
            .unwrap_or_else(|| PathBuf::from("target/cress/conservation.tsv")),
    };
    let mut kinds = StreamKinds::new();
    let Ok(text) = fs::read_to_string(&path) else {
        return kinds;
    };
    for line in text.lines() {
        let f: Vec<&str> = line.split('\t').collect();
        if f.len() < 5 || f[1] != meta.function || f[2] != meta.configuration {
            continue;
        }
        if let Some(kind) = StreamKind::parse(f[4]) {
            kinds.insert(f[3].to_owned(), kind);
        }
    }
    if !kinds.is_empty() {
        eprintln!(
            "  stream kinds from {}: {}",
            path.display(),
            kinds
                .iter()
                .map(|(n, k)| format!("{n}={}", k.word()))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    kinds
}

/// Deploys, runs and judges one trial. The deployment must already be deployed (not started):
/// the generated code deploys it, connects the generator's and the timers' ports, and builds
/// `send_round`, which writes one round of the workload and returns how many records it wrote.
/// `observer` holds the program's registered outputs.
#[expect(clippy::too_many_arguments, reason = "called only by generated code")]
pub async fn run_trial<'a, F, E>(
    deployment: &mut Deployment,
    nodes: &DeployResult<'a, HydroDeploy>,
    send_round: F,
    timers: Vec<TimerDrive>,
    observer: Observer<'a>,
    meta: &Meta,
    run: &RunConfig,
    factor: f64,
    trial: usize,
) -> TrialResult
where
    F: AsyncFnMut(u64) -> Result<u64, E>,
    E: fmt::Display,
{
    let mu = run.mu.or(meta.mu).unwrap_or_else(|| {
        panic!(
            "{} ({}) gives no `mu`; add `mu = <rounds per second>` to the attribute or pass --mu",
            meta.function, meta.configuration
        )
    });
    let profile = run.profile(mu, factor);
    let seed = run.seed + trial as u64;
    let dips: Vec<String> = run
        .dips
        .iter()
        .map(|(n, k)| format!("{n} period ×{k}"))
        .collect();
    for (n, _) in &run.dips {
        assert!(
            timers.iter().any(|t| t.name == *n),
            "--dip names timer `{n}`, which {} has no timer parameter of (timers: {})",
            meta.function,
            timers.iter().map(|t| t.name.as_str()).collect::<Vec<_>>().join(", ")
        );
    }
    eprintln!(
        "  factor {factor}, trial {trial}: μ = {mu:.0} rounds/s, ρ = {:.1} rounds/s, trigger {:.0} rounds/s{} for {:.0}s, tail {:.0}s",
        profile.baseline_rps,
        profile.trigger_rps,
        if dips.is_empty() {
            String::new()
        } else {
            format!(" with {}", dips.join(", "))
        },
        run.trigger.as_secs_f64(),
        run.tail.as_secs_f64()
    );

    // Stream kinds: explicit registrations first, Cress's table for the rest, the item type
    // when neither says.
    let mut kinds = StreamKinds::new();
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
        kinds.insert(name.clone(), StreamKind::Level);
        gauges.push((name, nodes.connect(p).await));
    }
    let mut counters = Vec::new();
    for (name, p) in observer.counters {
        kinds.insert(name.clone(), StreamKind::Work);
        counters.push((name, nodes.connect(p).await));
    }
    let from_cress = cress_kinds(run, meta);
    let mut streams = Vec::new();
    for s in observer.streams {
        let kind = from_cress.get(&s.name).copied().unwrap_or(if s.numeric {
            StreamKind::Level
        } else {
            StreamKind::Work
        });
        kinds.insert(s.name.clone(), kind);
        streams.push((s.name, (s.connect)(nodes).await));
    }
    let explicit_ids = completions.is_some();
    deployment.start().await.expect("deployment did not start");
    // Let the processes settle before time zero; the timers start with the trial.
    tokio::time::sleep(Duration::from_secs(1)).await;

    let recorder = Recorder::new();
    let start = recorder.start();
    let timers_running = futures::future::join_all(timers.into_iter().map(|t| {
        let dip = run.dips.iter().find(|(n, _)| *n == t.name).map(|(_, k)| *k);
        drive_timer(t, start, profile, dip)
    }));
    let mut timers_running = std::pin::pin!(timers_running);
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
            let read_streams = futures::future::join_all(streams.iter_mut().map(|(name, s)| {
                let r = r.clone();
                async move {
                    while let Some(o) = s.next().await {
                        r.record(
                            name,
                            Sample {
                                value: o.value,
                                id: o.id,
                                member: o.member,
                            },
                        );
                    }
                }
            }));
            futures::join!(
                read_completions,
                read_abandoned,
                read_gauges,
                read_counters,
                read_streams
            );
        }
    };
    let mut readers = std::pin::pin!(readers);

    let driven = tokio::select! {
        d = drive(profile, seed, &recorder, send_round) => d,
        () = &mut readers => panic!("the program's output streams ended while the trial was running"),
        _ = &mut timers_running => panic!("a timer port closed while the trial was running"),
    };
    let driven = match driven {
        Ok(d) => d,
        Err((e, d)) => {
            eprintln!(
                "  generator stopped early after {} rounds ({} records): {e}",
                d.rounds, d.offered
            );
            d
        }
    };
    tokio::select! {
        () = tokio::time::sleep(run.grace) => {}
        () = &mut readers => panic!("the program's output streams ended while the trial was running"),
        _ = &mut timers_running => panic!("a timer port closed while the trial was running"),
    }

    // The id overlay: explicit completions, or else the one conserved stream whose records carry
    // ids (the item is a `u64` or has an `id` field). Streams whose kind came from the item type
    // alone do not qualify, since a numeric stream could as well be a level.
    let completion_stream = if explicit_ids {
        None
    } else {
        let candidates: Vec<&String> = kinds
            .iter()
            .filter(|(n, k)| **k == StreamKind::Conserved && from_cress.contains_key(*n))
            .map(|(n, _)| n)
            .filter(|n| recorder.has_ids(n))
            .collect();
        match candidates.as_slice() {
            [one] => Some((*one).clone()),
            _ => None,
        }
    };
    let by_id = explicit_ids || completion_stream.is_some();
    if let Some(name) = &completion_stream {
        eprintln!("  reading goodput and latency from the ids in `{name}`");
    }
    let trace = recorder.trace(profile.total(), completion_stream.as_deref());
    let verdict = judge_with_kinds(&trace, &profile, &kinds, by_id);
    if let Err(e) = deployment.stop().await {
        eprintln!("  (deployment did not stop cleanly: {e})");
    }

    // A dip run at the same factor is a different experiment and must not overwrite the burst
    // run's trace, so the dips are part of the name.
    let dip_tag: String = run
        .dips
        .iter()
        .map(|(n, k)| format!(".dip-{n}x{k}"))
        .collect();
    let trace_name = format!(
        "{}.{}.{}.factor{}.trial{}{}.tsv",
        meta.crate_name, meta.function, meta.configuration, factor, trial, dip_tag
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
    eprintln!("  {}", result.verdict);
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
                "function\tconfiguration\tfactor\ttrial\tseed\tmu\tlambda\toutcome\ttail_goodput\ttail_p50_ms\ttail_p99_ms\ttail_abandoned\toutstanding\tlate\tby_id\trelative_goodput\tamplified_stream\tamplification\treason\ttrace"
            )?;
        }
        writeln!(
            f,
            "{}\t{}\t{}\t{}\t{}\t{:.1}\t{:.2}\t{}\t{:.3}\t{}\t{}\t{}\t{}\t{}\t{}\t{:.3}\t{}\t{:.2}\t{}\t{}",
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
            r.verdict.by_id,
            r.verdict.relative_goodput,
            r.verdict.amplification.0,
            r.verdict.amplification.1,
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
