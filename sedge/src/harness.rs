//! Runtime support for the harness that [`overload_check`](crate::overload_check) generates:
//! the run configuration the command passes in, the capacity each deployed node gets, the
//! measurement of the program's capacity μ, the deployment of one trial, the registry the
//! command reads, and the files a run leaves behind. Nothing here is specific to any program.
//!
//! # What the harness controls
//!
//! Only what is *outside* the program: the load it is offered (an open-loop Poisson generator on
//! its external ports), the resources each of its nodes gets (a CPU quota per node, see
//! [`crate::capacity`]), and, for a capacity-dip trigger, a cut in that quota for a while. Its
//! timers are its own: the generated code binds every timer parameter to an in-process
//! `source_interval`, so CPU starvation and queueing delay them as they would in production. The
//! old harness-driven timer survives only as a *timing fault* (`--dip TIMER=K`), reported apart
//! from overload.
//!
//! # μ is measured
//!
//! The sweep's factors `f = μ/λ` are relative to the capacity μ, in rounds of workload per
//! second. Unless `--mu` gives it, μ is measured before the sweep, per configuration and per set
//! of limits, by [`calibrate`]: deploy at a constant offered rate, check whether goodput keeps
//! up and the program's levels stay flat, and search for the highest rate that passes. The result
//! is cached in `calibration.tsv` per (program, configuration, limits, host).
//!
//! # Where things end up
//!
//! Under `$SEDGE_RESULTS_DIR` (the command sets it to `target/sedge/`, or `sedge/` under
//! `$CARGO_TARGET_DIR`): `results.tsv` has one row per trial, `probes.tsv` one row per capacity
//! probe, `calibration.tsv` the cached μ's, and `traces/` one per-second TSV per trial, named by
//! crate, function, configuration, factor, trial number and trigger.

use std::collections::BTreeMap;
use std::fmt;
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use futures::{Sink, SinkExt, StreamExt};
use hydro_deploy::Deployment;
use hydro_lang::compile::deploy::DeployResult;
use hydro_lang::deploy::{HydroDeploy, TrybuildHost};

use crate::capacity::{
    CapacityBackend, CpuQuota, CpuStat, DockerBackend, Handle, Limits, NodeSpec, Nodes, ProcIo,
    ProcSampler, host_fingerprint,
};
use crate::generator::drive;
use hydro_deploy_integration::{SEND_BUFFER_ENV, SEND_STATS_ENV};
use crate::profile::Profile;
use crate::program::{Observer, StreamMode};
use crate::trace::{Recorder, Sample, Trace};
use crate::verdict::{
    Outcome, StreamKind, StreamKinds, Verdict, judge_with_kinds, level_rising, window_stats,
};

// ---------------------------------------------------------------------------------------------
// Configuration

/// How each deployed node is run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendChoice {
    /// One Docker container per node, with a CFS quota (the default when Docker answers).
    Docker,
    /// Plain localhost processes, uncapped: the program's capacity is whatever the host gives
    /// it, and nothing about it is measured. Only for hosts without Docker.
    None,
}

/// What resources each deployed node of the program gets.
#[derive(Debug, Clone)]
pub struct CapacityConfig {
    pub backend: BackendChoice,
    /// Cores' worth of CPU per node, overriding the check's `cpus`. `Some(0.0)` is `none`: no
    /// quota.
    pub cpus: Option<f64>,
    /// Per-role overrides of `cpus`, by the location's marker type name (case-insensitive); `0.0`
    /// is `none`.
    pub role_cpus: Vec<(String, f64)>,
    /// The CFS period. Short, so that a small quota is a slower node rather than one that stops
    /// for longer than a tick.
    pub period_us: Option<u64>,
    /// The cores the nodes run on.
    pub node_cpuset: Option<String>,
    /// The cores the harness itself (generator, observer, relay) is pinned to.
    pub harness_cpuset: Option<String>,
    /// The container image the nodes run in.
    pub image: String,
    /// A memory limit per node, in bytes.
    pub memory: Option<u64>,
    /// Per-role memory limits (`--memory-mb ROLE=M`), overriding `memory`; `None` removes the
    /// limit for that role.
    pub role_memory: Vec<(String, Option<u64>)>,
}

/// Parses a CPU cap: a positive number of cores, or `none` for no quota (stored as `0.0`).
fn parse_cpus(v: &str) -> Result<f64, String> {
    let v = v.trim();
    if v.eq_ignore_ascii_case("none") {
        return Ok(0.0);
    }
    v.parse::<f64>()
        .ok()
        .filter(|c| *c > 0.0)
        .ok_or_else(|| format!("--cpus wants a positive number of cores or `none`, got `{v}`"))
}

/// The CFS period when neither the command nor the check says: short, so that a quota slows a
/// node down rather than stopping it for longer than a tick. The kernel's smallest quota is
/// 1 ms, so at this period no cap is below 0.1 cores; a tighter cap needs a longer period.
pub const DEFAULT_PERIOD_US: u64 = 10_000;

impl Default for CapacityConfig {
    fn default() -> Self {
        // Split the host: the upper half of the cores for the nodes, the lower half for the
        // harness, so that the generator never competes with the program.
        let n = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1);
        let (node_cpuset, harness_cpuset) = if n >= 4 {
            (
                Some(format!("{}-{}", n / 2, n - 1)),
                Some(format!("0-{}", n / 2 - 1)),
            )
        } else {
            (None, None)
        };
        Self {
            backend: if DockerBackend::available() {
                BackendChoice::Docker
            } else {
                BackendChoice::None
            },
            cpus: None,
            role_cpus: Vec::new(),
            period_us: None,
            node_cpuset,
            harness_cpuset,
            image: "public.ecr.aws/amazonlinux/amazonlinux:latest".to_owned(),
            memory: None,
            role_memory: Vec::new(),
        }
    }
}

impl CapacityConfig {
    /// The limits of a node with role `role` under check `meta`.
    /// A node is capped only where a cap was asked for: the command's per-role cap, its cap for
    /// every node, the check's per-role cap, the check's cap for every node, in that order. A
    /// role no one caps (a client, a load-generating role) runs with no quota: only the resource
    /// under test gets one. `none` (0.0) at any level means no quota.
    pub fn limits(&self, meta: &Meta, role: &str) -> Limits {
        let cpus = self
            .role_cpus
            .iter()
            .find(|(r, _)| r.eq_ignore_ascii_case(role))
            .map(|(_, c)| *c)
            .or(self.cpus)
            .or_else(|| {
                meta.role_cpus
                    .iter()
                    .find(|(r, _)| r.eq_ignore_ascii_case(role))
                    .map(|(_, c)| *c)
            })
            .or(meta.cpus)
            .unwrap_or(0.0);
        Limits {
            cpu: (cpus > 0.0).then(|| CpuQuota::from_cores(cpus, self.period(meta))),
            cpuset: self.node_cpuset.clone(),
            memory: self
                .role_memory
                .iter()
                .find(|(r, _)| r.eq_ignore_ascii_case(role))
                .map(|(_, m)| *m)
                .unwrap_or(self.memory),
        }
    }

    /// The CFS period: the command's, the check's `cpu_period_us`, or [`DEFAULT_PERIOD_US`].
    pub fn period(&self, meta: &Meta) -> u64 {
        self.period_us
            .or(meta.cpu_period_us)
            .unwrap_or(DEFAULT_PERIOD_US)
    }

    /// A key for the calibration cache: everything about the limits that changes μ.
    pub fn key(&self, meta: &Meta, roles: &[String]) -> String {
        match self.backend {
            BackendChoice::None => "uncapped".to_owned(),
            BackendChoice::Docker => {
                let mut parts: Vec<String> = roles
                    .iter()
                    .map(|r| {
                        let l = self.limits(meta, r);
                        format!(
                            "{r}={}",
                            l.cpu
                                .map(|q| format!("{}/{}", q.quota_us, q.period_us))
                                .unwrap_or_else(|| "-".to_owned())
                        )
                    })
                    .collect();
                parts.sort();
                parts.dedup();
                format!(
                    "docker;{};cpuset={};mem={}",
                    parts.join(","),
                    self.node_cpuset.as_deref().unwrap_or("-"),
                    self.memory.map(|m| m.to_string()).unwrap_or_else(|| "-".to_owned())
                )
            }
        }
    }
}

/// Which nodes a capacity dip applies to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeSelector {
    All,
    Role(String),
    Member(String, u32),
}

impl NodeSelector {
    fn parse(s: &str) -> Result<Self, String> {
        let s = s.trim();
        if s.eq_ignore_ascii_case("all") {
            return Ok(NodeSelector::All);
        }
        match s.split_once(':') {
            Some((role, m)) => m
                .trim()
                .parse()
                .map(|m| NodeSelector::Member(role.trim().to_owned(), m))
                .map_err(|_| format!("--cpu-dip-nodes wants ROLE or ROLE:INDEX, got `{s}`")),
            None => Ok(NodeSelector::Role(s.to_owned())),
        }
    }

    fn matches(&self, node: &NodeSpec) -> bool {
        match self {
            NodeSelector::All => true,
            NodeSelector::Role(r) => r.eq_ignore_ascii_case(&node.role),
            NodeSelector::Member(r, m) => {
                r.eq_ignore_ascii_case(&node.role) && node.member.unwrap_or(0) == *m
            }
        }
    }
}

impl fmt::Display for NodeSelector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NodeSelector::All => f.write_str("all"),
            NodeSelector::Role(r) => f.write_str(r),
            NodeSelector::Member(r, m) => write!(f, "{r}:{m}"),
        }
    }
}

/// A capacity-dip trigger: during the trigger window, the CPU quota of the selected nodes is
/// divided by `k`, then restored. With `--trigger-multiple 1` the offered load stays at the
/// baseline and the dip alone is the trigger.
#[derive(Debug, Clone, PartialEq)]
pub struct CpuDip {
    pub k: f64,
    pub nodes: Vec<NodeSelector>,
}

/// When to measure μ.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Calibrate {
    /// Use the cached μ for these limits if there is one, otherwise measure it.
    Auto,
    /// Measure it even if it is cached.
    Force,
    /// Never: use the check's `mu` as declared (the old behavior).
    Off,
}

/// What the command asks for: which checks, at which overprovisioning factors, how many times,
/// the shape of the load profile, the capacity of each node and the trigger.
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
    /// Gives μ (rounds of workload per second the program sustains) instead of measuring it.
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
    /// Timing faults: during the trigger window, the named timer's period is multiplied by the
    /// factor. A timer named here is driven by the harness over an external port instead of
    /// running in-process, which is a change to the program's environment that no real
    /// overload makes; its runs are reported as `TIMING_FAULT`, apart from overload.
    pub dips: Vec<(String, f64)>,
    /// What each node gets.
    pub capacity: CapacityConfig,
    /// A capacity-dip trigger, only with `--cpu-dip`. Without it (the default) every node's
    /// quota is set when its container starts and never changed during the run.
    pub cpu_dip: Option<CpuDip>,
    /// When to measure μ.
    pub calibrate: Calibrate,
    /// How long the probe offers each rate (after a warm-up of a third of it).
    pub probe_step: Duration,
    /// The rate the probe starts its search at, overriding the check's `mu` hint.
    pub probe_start: Option<f64>,
    /// Only measure μ (or read it from the cache); run no trials.
    pub calibrate_only: bool,
    /// Give the program's nodes unbounded send queues (`HYDRO_DEPLOY_SEND_BUFFER=unbounded`), so
    /// that a slow peer never stalls a node: the default, since without it backpressure from the
    /// first slow link reaches the generator and the workload is no longer open-loop.
    pub unbounded_sends: bool,
    /// Roles whose nodes get bounded send queues even when `unbounded_sends` is set
    /// (`--send-buffer bounded:ROLE,...`): their sends push back on their own dataflow, while
    /// every other node (the load's entry point in particular) keeps unbounded queues and the
    /// feed stays open-loop.
    pub bounded_send_roles: Vec<String>,
    /// Roles whose nodes get *lossy* bounded send queues (`--send-buffer lossy:N:ROLE,...`):
    /// each outgoing connection queues at most `lossy_capacity` messages and drops a send that
    /// finds its queue full, so a slow peer neither stalls the node (as `bounded` does) nor
    /// grows its memory without limit (as `unbounded` does). A dropped message is a message
    /// omission, the fault a retrying protocol is built to tolerate.
    pub lossy_send_roles: Vec<String>,
    pub lossy_capacity: usize,
    /// The Cargo profile the deployed nodes are built with: [`DEPLOY_PROFILE`] by default (an
    /// optimized build), or `dev` for a debug build (`--deploy-profile dev`).
    pub deploy_profile: String,
}

/// The Cargo profile Sedge builds deployed nodes with unless `--deploy-profile` says otherwise:
/// the workspace's `profile` profile (release with debug info, no LTO, symbols kept). μ and every
/// verdict are about the program's real capacity, so its nodes must not carry a debug build's
/// costs (debug assertions, overflow checks), even with `-C opt-level=3` in the rustflags.
pub const DEPLOY_PROFILE: &str = "profile";

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
            capacity: CapacityConfig::default(),
            cpu_dip: None,
            calibrate: Calibrate::Auto,
            probe_step: Duration::from_secs(10),
            probe_start: None,
            calibrate_only: false,
            unbounded_sends: true,
            bounded_send_roles: Vec::new(),
            lossy_send_roles: Vec::new(),
            lossy_capacity: 0,
            deploy_profile: DEPLOY_PROFILE.to_owned(),
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

    /// What kind of trigger this run's trials use.
    pub fn trigger_kind(&self) -> TriggerKind {
        if !self.dips.is_empty() {
            TriggerKind::TimingFault
        } else if self.cpu_dip.is_some() {
            TriggerKind::CpuDip
        } else {
            TriggerKind::Burst
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
        let positive = |v: &str, what: &str| -> Result<f64, String> {
            v.trim()
                .parse::<f64>()
                .ok()
                .filter(|m| *m > 0.0)
                .ok_or_else(|| format!("{what} wants a positive number, got `{v}`"))
        };
        let mut dip_k: Option<f64> = None;
        let mut dip_nodes: Vec<NodeSelector> = Vec::new();
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
                        .map(|s| positive(s, "--factors"))
                        .collect::<Result<_, _>>()?;
                }
                "--trials" => {
                    let v = value("--trials")?;
                    cfg.trials =
                        v.parse().ok().filter(|t| *t > 0).ok_or_else(|| {
                            format!("--trials wants a positive integer, got `{v}`")
                        })?;
                }
                "--mu" => cfg.mu = Some(positive(&value("--mu")?, "--mu")?),
                "--trigger-multiple" => {
                    cfg.trigger_multiple =
                        positive(&value("--trigger-multiple")?, "--trigger-multiple")?
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
                    cfg.dips.push((name.trim().to_owned(), positive(k, "--dip")?));
                }
                "--features" => {
                    cfg.features = value("--features")?
                        .split(',')
                        .map(|s| s.trim().to_owned())
                        .filter(|s| !s.is_empty())
                        .collect();
                }
                "--backend" => {
                    cfg.capacity.backend = match value("--backend")?.as_str() {
                        "docker" => BackendChoice::Docker,
                        "none" => BackendChoice::None,
                        v => return Err(format!("--backend wants docker or none, got `{v}`")),
                    }
                }
                "--cpus" => {
                    let v = value("--cpus")?;
                    match v.split_once('=') {
                        Some((role, c)) => cfg
                            .capacity
                            .role_cpus
                            .push((role.trim().to_owned(), parse_cpus(c)?)),
                        None => cfg.capacity.cpus = Some(parse_cpus(&v)?),
                    }
                }
                "--cpu-period-us" => {
                    let v = value("--cpu-period-us")?;
                    cfg.capacity.period_us = Some(v
                        .parse()
                        .ok()
                        .filter(|p| (1000..=1_000_000).contains(p))
                        .ok_or_else(|| {
                            format!("--cpu-period-us wants 1000..=1000000, got `{v}`")
                        })?);
                }
                "--node-cpus" => {
                    let v = value("--node-cpus")?;
                    cfg.capacity.node_cpuset = (v != "none").then_some(v);
                }
                "--harness-cpus" => {
                    let v = value("--harness-cpus")?;
                    cfg.capacity.harness_cpuset = (v != "none").then_some(v);
                }
                "--image" => cfg.capacity.image = value("--image")?,
                "--memory-mb" => {
                    let v = value("--memory-mb")?;
                    match v.split_once('=') {
                        Some((role, m)) => {
                            let m = if m.trim() == "none" {
                                None
                            } else {
                                Some((positive(m, "--memory-mb")? * 1024.0 * 1024.0) as u64)
                            };
                            cfg.capacity.role_memory.push((role.trim().to_owned(), m));
                        }
                        None => {
                            cfg.capacity.memory =
                                Some((positive(&v, "--memory-mb")? * 1024.0 * 1024.0) as u64)
                        }
                    }
                }
                "--cpu-dip" => dip_k = Some(positive(&value("--cpu-dip")?, "--cpu-dip")?),
                "--cpu-dip-nodes" => {
                    for s in value("--cpu-dip-nodes")?.split(',') {
                        dip_nodes.push(NodeSelector::parse(s)?);
                    }
                }
                "--recalibrate" => cfg.calibrate = Calibrate::Force,
                "--no-calibrate" => cfg.calibrate = Calibrate::Off,
                "--calibrate-only" => cfg.calibrate_only = true,
                "--send-buffer" => {
                    let v = value("--send-buffer")?;
                    match v.as_str() {
                        "unbounded" => cfg.unbounded_sends = true,
                        "bounded" => cfg.unbounded_sends = false,
                        v if v.starts_with("bounded:") => {
                            cfg.unbounded_sends = true;
                            cfg.bounded_send_roles = v["bounded:".len()..]
                                .split(',')
                                .map(|r| r.trim().to_owned())
                                .filter(|r| !r.is_empty())
                                .collect();
                        }
                        v if v.starts_with("lossy:") => {
                            let bad = || {
                                format!("--send-buffer lossy wants lossy:N:ROLE,..., got `{v}`")
                            };
                            let (n, roles) =
                                v["lossy:".len()..].split_once(':').ok_or_else(bad)?;
                            cfg.unbounded_sends = true;
                            cfg.lossy_capacity =
                                n.trim().parse().ok().filter(|n| *n > 0).ok_or_else(bad)?;
                            cfg.lossy_send_roles = roles
                                .split(',')
                                .map(|r| r.trim().to_owned())
                                .filter(|r| !r.is_empty())
                                .collect();
                            if cfg.lossy_send_roles.is_empty() {
                                return Err(bad());
                            }
                        }
                        v => {
                            return Err(format!(
                                "--send-buffer wants unbounded, bounded, bounded:ROLE,... or lossy:N:ROLE,..., got `{v}`"
                            ));
                        }
                    }
                }
                "--deploy-profile" => cfg.deploy_profile = value("--deploy-profile")?,
                "--probe-step" => cfg.probe_step = secs(&value("--probe-step")?, "--probe-step")?,
                "--probe-start" => {
                    cfg.probe_start = Some(positive(&value("--probe-start")?, "--probe-start")?)
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
        match dip_k {
            Some(k) => {
                if dip_nodes.is_empty() {
                    dip_nodes.push(NodeSelector::All);
                }
                if cfg.capacity.backend == BackendChoice::None {
                    return Err("--cpu-dip needs a capacity backend (--backend docker)".to_owned());
                }
                cfg.cpu_dip = Some(CpuDip {
                    k,
                    nodes: dip_nodes,
                });
            }
            None if !dip_nodes.is_empty() => {
                return Err("--cpu-dip-nodes needs --cpu-dip K".to_owned());
            }
            None => {}
        }
        if cfg.cpu_dip.is_some() && !cfg.dips.is_empty() {
            return Err("--cpu-dip and --dip are different experiments; give one".to_owned());
        }
        Ok(cfg)
    }
}

/// The command's options.
pub const USAGE: &str = "\
usage: sedge [options] <crate> [function-substring]
 selection and sweep:
  --config NAME            only the configuration named NAME
  --include-ignored        also run checks marked ignore = \"...\"
  --factors F1,F2,...      overprovisioning factors μ/λ to run (default 2)
  --trials N               trials per factor (default 1)
  --trigger-multiple M     trigger load as a multiple of μ (default 2; 1 for a dip alone)
  --before S               seconds of baseline before the trigger (default 10)
  --trigger S              seconds the trigger lasts (default 5)
  --tail S                 seconds of baseline after the trigger (default 45)
  --grace S                seconds to wait for stragglers after the last request (default 2)
  --seed N                 seed of the first trial's arrivals (default 1)
  --features A,B           extra Cargo features for the deployed program
 capacity (from outside the program; each node's quota is set once, when its container starts,
 and stays fixed for the whole run unless --cpu-dip asks otherwise):
  --backend docker|none    run each node in a CPU-capped container (default docker if available)
  --cpus C|none            cores' worth of CPU for every node (default: the check's `cpus`; a
                           node no one caps runs with no quota)
  --cpus ROLE=C|none       per-role override, by location marker name (repeatable)
  --cpu-period-us P        CFS period in microseconds (default: the check's, else 10000; the
                           smallest quota is 1 ms, so caps below 0.1 cpu need a longer period)
  --node-cpus LIST         cores the nodes run on (default: upper half; `none` for any)
  --harness-cpus LIST      cores the harness is pinned to (default: lower half; `none`)
  --image IMAGE            container image (default amazonlinux:latest)
  --memory-mb M|ROLE=M     memory limit per node, or per role (ROLE=none: no limit)
  --send-buffer unbounded|bounded|bounded:ROLE,...|lossy:N:ROLE,...
                           nodes' outgoing connections queue sends in memory (default unbounded,
                           so a slow peer never stalls a node and the workload stays open-loop),
                           or push back on the sending dataflow (Hydro's own default); with
                           `bounded:ROLE,...` only those roles push back; with
                           `lossy:N:ROLE,...` those roles queue at most N messages per
                           connection and drop a send that finds the queue full
  --deploy-profile NAME    Cargo profile the nodes are built with (default `profile`: release
                           with debug info and no LTO; `dev` for a debug build)
 μ:
  --mu RPS                 take μ as given instead of measuring it
  --recalibrate            measure μ even if it is cached for these limits
  --no-calibrate           use the check's declared `mu` (no measurement)
  --calibrate-only         measure (or look up) μ and stop; run no trials
  --probe-step S           seconds per probe rate (default 10)
  --probe-start RPS        rate the probe starts at (default: the check's `mu` hint, else 50)
 triggers (default: a load burst only):
  --cpu-dip K              opt-in capacity dip: divide the selected nodes' CPU quota by K during
                           the trigger window, then restore it (the only mid-run quota change)
  --cpu-dip-nodes SEL,...  which nodes: all, ROLE, or ROLE:INDEX (default all)
  --dip TIMER=K            TIMING FAULT: drive TIMER from the harness and stretch its period by K
                           during the trigger (repeatable; reported apart from overload)";

/// The directory results are written to: `$SEDGE_RESULTS_DIR` if set, otherwise `target/sedge/`
/// under the current directory.
pub fn results_dir() -> PathBuf {
    match std::env::var_os("SEDGE_RESULTS_DIR") {
        Some(dir) => PathBuf::from(dir),
        None => Path::new("target").join("sedge"),
    }
}

// ---------------------------------------------------------------------------------------------
// Checks and their results

/// What the generated code knows about the check it is running.
#[derive(Debug, Clone)]
pub struct Meta {
    pub crate_name: &'static str,
    pub module: &'static str,
    pub function: &'static str,
    pub configuration: &'static str,
    /// The check's `mu`: with calibration, only the rate the probe starts at.
    pub mu: Option<f64>,
    /// The check's `cpus = C`: the cap each node gets unless the command says otherwise.
    pub cpus: Option<f64>,
    /// The check's `cpus(Role = C, ...)`: per-role caps, by location marker name.
    pub role_cpus: Vec<(&'static str, f64)>,
    /// The check's `cpu_period_us`: the CFS period its caps are enforced over.
    pub cpu_period_us: Option<u64>,
    /// The role (location marker name) of every location the check deploys, for the
    /// calibration key.
    pub roles: &'static [&'static str],
}

/// What triggered a trial's overload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TriggerKind {
    /// Offered load above μ for the trigger window.
    Burst,
    /// The nodes' CPU quota cut for the trigger window (from outside).
    CpuDip,
    /// A harness-driven timer stretched for the trigger window: a change to the program's
    /// environment, not overload.
    TimingFault,
}

impl fmt::Display for TriggerKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            TriggerKind::Burst => "burst",
            TriggerKind::CpuDip => "cpu_dip",
            TriggerKind::TimingFault => "TIMING_FAULT",
        })
    }
}

/// Where a trial's μ came from.
#[derive(Debug, Clone, PartialEq)]
pub enum MuSource {
    /// `--mu`.
    Given,
    /// The check's `mu`, unmeasured (`--no-calibrate`, or no capacity backend).
    Declared,
    /// Measured by the probe now.
    Measured,
    /// Measured by an earlier probe under the same limits on this host.
    Cached,
}

impl fmt::Display for MuSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            MuSource::Given => "given",
            MuSource::Declared => "declared",
            MuSource::Measured => "measured",
            MuSource::Cached => "cached",
        })
    }
}

/// The CPU one node used over a run, from its cgroup.
#[derive(Debug, Clone)]
pub struct NodeUsage {
    pub node: NodeSpec,
    pub limits: Limits,
    /// Per second: cores used, CFS periods throttled, milliseconds throttled.
    pub per_second: Vec<(f64, u64, f64)>,
    /// Totals over the sampled seconds.
    pub total: CpuStat,
    /// Seconds sampled.
    pub seconds: usize,
}

impl NodeUsage {
    /// Mean cores used.
    pub fn mean_cores(&self) -> f64 {
        if self.seconds == 0 {
            0.0
        } else {
            self.total.usage_ns as f64 / 1e9 / self.seconds as f64
        }
    }

    /// Fraction of runnable CFS periods in which the node was throttled.
    pub fn throttled_fraction(&self) -> f64 {
        if self.total.nr_periods == 0 {
            0.0
        } else {
            self.total.nr_throttled as f64 / self.total.nr_periods as f64
        }
    }
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
    pub mu_source: MuSource,
    pub lambda: f64,
    pub trigger: TriggerKind,
    /// The trigger, in words (`burst 2×μ`, `cpu quota ÷4 on Server`).
    pub trigger_text: String,
    pub verdict: Verdict,
    /// Arrivals the generator fell behind on; nonzero means the offered load was below the
    /// profile.
    pub late: u64,
    /// Records that entered the program more than [`FEED_LATE`] after their scheduled arrival.
    pub feed_late: u64,
    /// The worst lag between a record's scheduled arrival and its write into the program.
    pub feed_max_lag: Duration,
    /// The largest value every level stream reached over the run.
    pub max_levels: Vec<(String, f64)>,
    /// Per-node CPU usage, empty without a capacity backend.
    pub usage: Vec<NodeUsage>,
    /// The limits, in words.
    pub limits: String,
    pub host: String,
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

/// Pins this process (every thread, and so every child it spawns later: the relay, the docker
/// CLI) to `cpus`.
fn pin_harness(cpus: &str) {
    let pid = std::process::id().to_string();
    match std::process::Command::new("taskset")
        .args(["-a", "-p", "-c", cpus, &pid])
        .stdout(std::process::Stdio::null())
        .status()
    {
        Ok(s) if s.success() => eprintln!("  harness pinned to cpus {cpus}"),
        _ => eprintln!("  (could not pin the harness to cpus {cpus}; is taskset installed?)"),
    }
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
    match run.capacity.backend {
        BackendChoice::Docker => {
            if let Some(cpus) = &run.capacity.harness_cpuset {
                pin_harness(cpus);
            }
            // Containers a killed earlier run of this process id left behind.
            DockerBackend::new(&run.capacity.image, run.results_dir.join("shims"))
                .remove_prefix(&format!("sedge-{}-", std::process::id()));
        }
        BackendChoice::None => eprintln!(
            "  WARNING: no capacity backend; nodes run uncapped and μ is the check's declared value"
        ),
    }
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

// ---------------------------------------------------------------------------------------------
// One deployment

/// What one deployment of a check is for: a trial of the sweep, or a capacity probe at a
/// constant rate.
#[derive(Debug, Clone)]
pub struct Spec {
    pub profile: Profile,
    pub seed: u64,
    /// For names: `factor2.trial0` or `probe3`.
    pub label: String,
    pub probe: bool,
    /// The capacity dip to apply in the trigger window.
    pub cpu_dip: Option<CpuDip>,
    /// The timers to drive from the harness, stretched by the factor in the trigger window.
    pub timing_dips: Vec<(String, f64)>,
}

impl Spec {
    /// Whether timer `name` is driven by the harness (a timing fault) rather than in-process.
    pub fn drives_timer(&self, name: &str) -> bool {
        self.timing_dips.iter().any(|(n, _)| n == name)
    }
}

static NODE_SEQ: AtomicU64 = AtomicU64::new(0);

/// The capacity side of one deployment: the hosts its nodes run on and, with a backend, the
/// handles to their containers. Dropping it removes the containers.
pub struct TrialCapacity {
    pub nodes: Option<Nodes>,
    pub prefix: String,
}

impl TrialCapacity {
    pub fn new(run: &RunConfig) -> Self {
        let seq = NODE_SEQ.fetch_add(1, Ordering::Relaxed);
        let prefix = format!("sedge-{}-{seq}", std::process::id());
        let nodes = match run.capacity.backend {
            BackendChoice::Docker => {
                let backend: Arc<dyn CapacityBackend> = Arc::new(DockerBackend::new(
                    &run.capacity.image,
                    run.results_dir.join("shims"),
                ));
                Some(Nodes::new(backend))
            }
            BackendChoice::None => None,
        };
        Self { nodes, prefix }
    }

    /// The host for one node of the program: role `role` (the location's marker name), member
    /// `member` of a cluster. Built optimized, with the crate's `sedge` feature and `--features`.
    pub fn host(
        &self,
        deployment: &mut Deployment,
        run: &RunConfig,
        meta: &Meta,
        role: &str,
        member: Option<u32>,
    ) -> TrybuildHost {
        let host: Arc<dyn hydro_deploy::Host> = match &self.nodes {
            Some(nodes) => {
                let name = match member {
                    Some(m) => format!("{}-{}-{m}", self.prefix, sanitize(role)),
                    None => format!("{}-{}", self.prefix, sanitize(role)),
                };
                nodes.host(
                    deployment.Localhost(),
                    name,
                    NodeSpec {
                        role: role.to_owned(),
                        member,
                    },
                    run.capacity.limits(meta, role),
                )
            }
            None => deployment.Localhost(),
        };
        let mut h = TrybuildHost::new(host)
            .rustflags("-C opt-level=3 -C codegen-units=1 -C strip=none -C debuginfo=2 -C lto=off")
            .profile(run.deploy_profile.clone())
            .feature("sedge");
        if run
            .lossy_send_roles
            .iter()
            .any(|r| r.eq_ignore_ascii_case(role))
        {
            h = h.env(SEND_BUFFER_ENV, format!("lossy:{}", run.lossy_capacity));
            if self.nodes.is_some() {
                let name = match member {
                    Some(m) => format!("{}-{}-{m}", self.prefix, sanitize(role)),
                    None => format!("{}-{}", self.prefix, sanitize(role)),
                };
                let path = send_stats_path(&name);
                if let Some(dir) = path.parent() {
                    let _ = fs::create_dir_all(dir);
                }
                let _ = fs::remove_file(&path);
                h = h.env(SEND_STATS_ENV, path.to_string_lossy().into_owned());
            }
        } else if run.unbounded_sends
            && !run
                .bounded_send_roles
                .iter()
                .any(|r| r.eq_ignore_ascii_case(role))
        {
            h = h.env(SEND_BUFFER_ENV, "unbounded");
        }
        for f in &run.features {
            h = h.feature(f.clone());
        }
        h
    }
}

/// Where the node in container `name` publishes its lossy send queues' counters
/// ([`SEND_STATS_ENV`]): under the temporary directory, which every node container mounts.
pub fn send_stats_path(name: &str) -> PathBuf {
    std::env::temp_dir()
        .join("sedge-sendstats")
        .join(format!("{name}.stats"))
}

/// The counters a node's lossy send queues published: `[attempted, attempted bytes, dropped,
/// dropped bytes]`, or `None` if it publishes none.
fn read_send_stats(name: &str) -> Option<[u64; 4]> {
    let text = fs::read_to_string(send_stats_path(name)).ok()?;
    let v: Vec<u64> = text.split_whitespace().filter_map(|x| x.parse().ok()).collect();
    (v.len() == 4).then(|| [v[0], v[1], v[2], v[3]])
}

fn sanitize(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c.to_ascii_lowercase() } else { '_' })
        .collect()
}

/// A localhost host for the harness's own relay process: uncapped, and (being a child of the
/// pinned harness) on the harness's cores. Built like the program's nodes.
pub fn host(deployment: &mut Deployment, run: &RunConfig) -> TrybuildHost {
    let mut host = TrybuildHost::new(deployment.Localhost())
        .rustflags("-C opt-level=3 -C codegen-units=1 -C strip=none -C debuginfo=2 -C lto=off")
        .profile(run.deploy_profile.clone())
        .feature("sedge");
    for f in &run.features {
        host = host.feature(f.clone());
    }
    host
}

/// How far behind schedule the harness wrote the workload into the program's input sockets.
/// Shared by the [`feed`] writers of one deployment; [`execute`] sets the time origin.
#[derive(Clone, Default)]
pub struct FeedStats {
    inner: Arc<Mutex<FeedInner>>,
}

#[derive(Default)]
struct FeedInner {
    start: Option<Instant>,
    records: u64,
    late: u64,
    max_lag: Duration,
    /// Worst lag of the records scheduled in each second.
    per_second: Vec<Duration>,
}

/// A record written more than this long after its scheduled arrival counts as late.
pub const FEED_LATE: Duration = Duration::from_millis(50);

impl FeedStats {
    fn set_start(&self, start: Instant) {
        self.inner.lock().unwrap().start = Some(start);
    }

    fn record(&self, due: Instant, lag: Duration) {
        let mut g = self.inner.lock().unwrap();
        g.records += 1;
        if lag > FEED_LATE {
            g.late += 1;
        }
        g.max_lag = g.max_lag.max(lag);
        if let Some(start) = g.start {
            let s = due.saturating_duration_since(start).as_secs() as usize;
            if g.per_second.len() <= s {
                g.per_second.resize(s + 1, Duration::ZERO);
            }
            g.per_second[s] = g.per_second[s].max(lag);
        }
    }

    /// `(records written, of which late, worst lag, worst lag per second)`.
    pub fn summary(&self) -> (u64, u64, Duration, Vec<Duration>) {
        let g = self.inner.lock().unwrap();
        (g.records, g.late, g.max_lag, g.per_second.clone())
    }
}

/// The sending half of a [`feed`]: queueing a record never waits.
pub struct FeedSender<T> {
    tx: futures::channel::mpsc::UnboundedSender<(Instant, T)>,
}

impl<T> FeedSender<T> {
    /// Queues `item`, scheduled to arrive at `due`, for the writer. Never blocks; an error means
    /// the writer has stopped (the program closed the connection).
    pub fn send(&self, due: Instant, item: T) -> Result<(), String> {
        self.tx
            .unbounded_send((due, item))
            .map_err(|_| "the program closed its input connection".to_owned())
    }
}

/// Decouples the generator from one of the program's input sockets. The generator queues each
/// record on an unbounded channel at its scheduled arrival and never waits; the returned writer
/// drains the channel into `sink`, flushing whenever it has caught up, and records in `stats`
/// how long after its scheduled arrival each record was written. If the program keeps reading
/// its socket (with `HYDRO_DEPLOY_SEND_BUFFER=unbounded`, a node never stops reading because a
/// peer is slow), the lag stays near zero and every request enters the program on schedule:
/// open loop. A lag that grows says the program's input is not being read, and the run is
/// reported as not open-loop.
pub fn feed<T: 'static>(
    sink: Pin<Box<dyn Sink<T, Error = std::io::Error>>>,
    stats: FeedStats,
) -> (FeedSender<T>, Pin<Box<dyn Future<Output = ()>>>) {
    let (tx, mut rx) = futures::channel::mpsc::unbounded::<(Instant, T)>();
    let writer = async move {
        let mut sink = sink;
        let mut dues: Vec<Instant> = Vec::new();
        while let Some((due, item)) = rx.next().await {
            dues.push(due);
            if sink.feed(item).await.is_err() {
                return;
            }
            while let Ok(Some((due, item))) = rx.try_next() {
                dues.push(due);
                if sink.feed(item).await.is_err() {
                    return;
                }
            }
            if sink.flush().await.is_err() {
                return;
            }
            let now = Instant::now();
            for d in dues.drain(..) {
                stats.record(d, now.saturating_duration_since(d));
            }
        }
    };
    (FeedSender { tx }, Box::pin(writer))
}

/// One of the program's timers, driven by the harness over an external port: only for a timing
/// fault (`--dip`), since every other timer runs in-process. A timer on a cluster of `members`
/// fires at every member once per period, the members staggered by `period / members`, as the
/// in-process timers are. A process is a cluster of one.
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
        let stretched =
            dip.is_some_and(|_| t >= profile.trigger_start() && t < profile.trigger_end());
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
pub fn cress_kinds(run: &RunConfig, meta: &Meta, quiet: bool) -> StreamKinds {
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
    if !kinds.is_empty() && !quiet {
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

/// What one deployment produced, before it is judged.
pub struct Executed {
    pub trace: Trace,
    pub kinds: StreamKinds,
    /// Whether goodput and latency come from request ids.
    pub by_id: bool,
    /// Rounds the generator offered.
    pub rounds: u64,
    /// Arrivals the generator itself reached more than 50 ms late (its own scheduling).
    pub late: u64,
    /// Records the feed writers wrote into the program's sockets.
    pub feed_records: u64,
    /// Of those, records written more than [`FEED_LATE`] after their scheduled arrival.
    pub feed_late: u64,
    /// The worst such lag.
    pub feed_max_lag: Duration,
    pub usage: Vec<NodeUsage>,
    pub limits: String,
    /// Nodes that were no longer running when the run ended, in words (`Member:3 was OOM-killed
    /// at its memory limit`). Any makes the trial INVALID: a dead node is not an overloaded one.
    pub node_failures: Vec<String>,
}

/// The largest fraction of records that may enter the program late for a run to count as open
/// loop.
pub const OPEN_LOOP_LATE: f64 = 0.001;

impl Executed {
    /// Whether every request entered the program on schedule: the generator kept its schedule
    /// and the program kept reading its input (all but [`OPEN_LOOP_LATE`] of the records were
    /// written within [`FEED_LATE`] of their scheduled arrival).
    pub fn open_loop(&self) -> bool {
        self.late == 0
            && (self.feed_late as f64) <= OPEN_LOOP_LATE * self.feed_records.max(1) as f64
    }

    /// Whether the harness, not the program, limited the run: the generator fell behind while no
    /// node was ever throttled. Only meaningful with a capacity backend.
    pub fn harness_bound(&self) -> bool {
        !self.usage.is_empty()
            && (self.late > 0 || self.feed_late > 0)
            && self.usage.iter().all(|u| u.total.nr_throttled == 0)
    }
}

/// Samples every node's `cpu.stat` once a second from `start` until the returned flag is set,
/// and applies `dip` in the profile's trigger window.
/// With a `procs` sampler, also reads every node's `/proc` accounting each second.
type CapacitySamples = (
    Vec<Vec<Option<CpuStat>>>,
    Vec<Vec<Option<ProcIo>>>,
    Vec<Vec<Option<[u64; 4]>>>,
);

fn spawn_capacity_tasks(
    nodes: &Nodes,
    start: Instant,
    profile: Profile,
    dip: Option<CpuDip>,
    procs: Option<Arc<ProcSampler>>,
) -> (
    Arc<std::sync::atomic::AtomicBool>,
    tokio::task::JoinHandle<CapacitySamples>,
    Option<tokio::task::JoinHandle<()>>,
) {
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let handles = nodes.handles();
    let backend = nodes.backend.clone();
    let sampler = {
        let stop = stop.clone();
        let handles = handles.clone();
        let backend = backend.clone();
        tokio::spawn(async move {
            // samples[s][node]: cumulative stat at second s.
            let mut samples: Vec<Vec<Option<CpuStat>>> = Vec::new();
            let mut io: Vec<Vec<Option<ProcIo>>> = Vec::new();
            let mut sends: Vec<Vec<Option<[u64; 4]>>> = Vec::new();
            let mut s = 0u32;
            loop {
                tokio::time::sleep_until((start + Duration::from_secs(s as u64)).into()).await;
                let hs = handles.clone();
                let b = backend.clone();
                let p = procs.clone();
                let (row, io_row, send_row) = tokio::task::spawn_blocking(move || {
                    let row = hs.iter().map(|h| b.cpu_stat(h)).collect::<Vec<_>>();
                    let io_row = p.map(|p| {
                        let pids: Vec<Vec<u32>> = hs.iter().map(|h| b.pids(h)).collect();
                        p.set_pids(&pids);
                        p.read(hs.len())
                    });
                    let send_row = hs.iter().map(|h| read_send_stats(&h.name)).collect();
                    (row, io_row.unwrap_or_default(), send_row)
                })
                .await
                .unwrap_or_default();
                samples.push(row);
                io.push(io_row);
                sends.push(send_row);
                if stop.load(Ordering::Relaxed) {
                    return (samples, io, sends);
                }
                s += 1;
            }
        })
    };
    let dipper = dip.map(|dip| {
        let targets: Vec<Arc<Handle>> = handles
            .iter()
            .filter(|h| dip.nodes.iter().any(|sel| sel.matches(&h.node)))
            .cloned()
            .collect();
        assert!(
            !targets.is_empty(),
            "--cpu-dip-nodes {} selects none of the nodes ({})",
            dip.nodes
                .iter()
                .map(|n| n.to_string())
                .collect::<Vec<_>>()
                .join(","),
            handles
                .iter()
                .map(|h| h.node.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
        tokio::spawn(async move {
            let set = |to_dip: bool| {
                let targets = targets.clone();
                let backend = backend.clone();
                let k = dip.k;
                tokio::task::spawn_blocking(move || {
                    for h in &targets {
                        let Some(q) = h.limits.cpu else {
                            eprintln!("  (cannot dip {}: it has no CPU quota)", h.node);
                            continue;
                        };
                        let to = if to_dip { q.divided(k) } else { q };
                        if let Err(e) = backend.set_cpu(h, to) {
                            eprintln!("  (dip of {} failed: {e})", h.node);
                        }
                    }
                })
            };
            tokio::time::sleep_until((start + profile.trigger_start()).into()).await;
            let _ = set(true).await;
            tokio::time::sleep_until((start + profile.trigger_end()).into()).await;
            let _ = set(false).await;
        })
    });
    (stop, sampler, dipper)
}

/// Turns cumulative per-second samples into per-node usage.
fn usage_from_samples(handles: &[Arc<Handle>], samples: &[Vec<Option<CpuStat>>]) -> Vec<NodeUsage> {
    handles
        .iter()
        .enumerate()
        .map(|(i, h)| {
            let series: Vec<Option<CpuStat>> =
                samples.iter().map(|row| row.get(i).copied().flatten()).collect();
            let mut per_second = Vec::new();
            for w in series.windows(2) {
                per_second.push(match (w[0], w[1]) {
                    (Some(a), Some(b)) => (
                        b.usage_ns.saturating_sub(a.usage_ns) as f64 / 1e9,
                        b.nr_throttled.saturating_sub(a.nr_throttled),
                        b.throttled_ns.saturating_sub(a.throttled_ns) as f64 / 1e6,
                    ),
                    _ => (f64::NAN, 0, f64::NAN),
                });
            }
            let first = series.iter().flatten().next().copied().unwrap_or_default();
            let last = series.iter().flatten().last().copied().unwrap_or_default();
            NodeUsage {
                node: h.node.clone(),
                limits: h.limits.clone(),
                seconds: per_second.len(),
                per_second,
                total: CpuStat {
                    usage_ns: last.usage_ns.saturating_sub(first.usage_ns),
                    nr_periods: last.nr_periods.saturating_sub(first.nr_periods),
                    nr_throttled: last.nr_throttled.saturating_sub(first.nr_throttled),
                    throttled_ns: last.throttled_ns.saturating_sub(first.throttled_ns),
                },
            }
        })
        .collect()
}

/// Runs one deployment. The deployment must already be deployed (not started): the generated
/// code deploys it, connects the generator's and the driven timers' ports, and builds
/// `send_round`, which writes one round of the workload and returns how many records it wrote.
/// `observer` holds the program's registered outputs; `capacity` the nodes' containers.
#[expect(clippy::too_many_arguments, reason = "called only by generated code")]
pub async fn execute<'a, F, E>(
    deployment: &mut Deployment,
    nodes: &DeployResult<'a, HydroDeploy>,
    send_round: F,
    writers: Vec<Pin<Box<dyn Future<Output = ()>>>>,
    feed_stats: FeedStats,
    timers: Vec<TimerDrive>,
    observer: Observer<'a>,
    meta: &Meta,
    run: &RunConfig,
    spec: &Spec,
    capacity: TrialCapacity,
) -> Executed
where
    F: AsyncFnMut(u64, Instant) -> Result<u64, E>,
    E: fmt::Display,
{
    let profile = spec.profile;
    for (n, _) in &spec.timing_dips {
        assert!(
            timers.iter().any(|t| t.name == *n),
            "--dip names timer `{n}`, which {} has no timer parameter of (timers: {})",
            meta.function,
            timers.iter().map(|t| t.name.as_str()).collect::<Vec<_>>().join(", ")
        );
    }

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
    let from_cress = cress_kinds(run, meta, spec.probe);
    let mut streams = Vec::new();
    for s in observer.streams {
        let kind = match s.mode {
            StreamMode::Counter => StreamKind::Work,
            StreamMode::Gauge => StreamKind::Level,
            StreamMode::Whole => from_cress.get(&s.name).copied().unwrap_or(if s.numeric {
                StreamKind::Level
            } else {
                StreamKind::Work
            }),
        };
        kinds.insert(s.name.clone(), kind);
        streams.push((s.name, s.mode, (s.connect)(nodes).await));
    }
    let explicit_ids = completions.is_some();
    // The nodes' `/proc` accounting, read from outside them: what each node really wrote to its
    // sockets, even when it is too starved to report.
    let proc_sampler = match &capacity.nodes {
        Some(n) => {
            let backend = n.backend.clone();
            let name = format!("{}-procstat", capacity.prefix);
            let dir = std::env::temp_dir().join("sedge-procstat").join(&name);
            let cpuset = run.capacity.harness_cpuset.clone();
            tokio::task::spawn_blocking(move || {
                backend.proc_sampler(&name, &dir, cpuset.as_deref())
            })
            .await
            .ok()
            .flatten()
            .map(Arc::new)
        }
        None => None,
    };
    deployment.start().await.expect("deployment did not start");
    // Let the processes settle before time zero; the timers start with the trial.
    tokio::time::sleep(Duration::from_secs(1)).await;

    let recorder = Recorder::new();
    let start = recorder.start();
    feed_stats.set_start(start);
    // The writers finish once the generator is done and they have drained; the run goes on
    // through the grace period regardless.
    let writers = async move {
        futures::future::join_all(writers).await;
        futures::future::pending::<()>().await;
    };
    let mut writers = std::pin::pin!(writers);
    let capacity_tasks = capacity.nodes.as_ref().map(|n| {
        spawn_capacity_tasks(n, start, profile, spec.cpu_dip.clone(), proc_sampler.clone())
    });
    // Only a timing fault's timers are driven from here; with none, this never completes.
    let driven_timers = !timers.is_empty();
    let timers_running = futures::future::join_all(timers.into_iter().map(|t| {
        let dip = spec
            .timing_dips
            .iter()
            .find(|(n, _)| *n == t.name)
            .map(|(_, k)| *k);
        drive_timer(t, start, profile, dip)
    }));
    let timers_running = async move {
        timers_running.await;
        if !driven_timers {
            futures::future::pending::<()>().await;
        }
    };
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
            let read_streams = futures::future::join_all(streams.iter_mut().map(|(name, mode, s)| {
                let r = r.clone();
                let mode = *mode;
                async move {
                    while let Some(o) = s.next().await {
                        if mode == StreamMode::Counter {
                            r.counter(name, o.value.unwrap_or(0.0));
                            continue;
                        }
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
        d = drive(profile, spec.seed, &recorder, send_round) => d,
        () = &mut readers => panic!("the program's output streams ended while the trial was running"),
        _ = &mut timers_running => panic!("a timer port closed while the trial was running"),
        () = &mut writers => unreachable!(),
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
        () = &mut writers => unreachable!(),
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
    if let Some(name) = &completion_stream
        && !spec.probe
    {
        eprintln!("  reading goodput and latency from the ids in `{name}`");
    }
    let mut trace = recorder.trace(profile.total(), completion_stream.as_deref());
    let (feed_records, feed_late, feed_max_lag, feed_per_second) = feed_stats.summary();
    trace.extra.push((
        "feed_lag_ms".to_owned(),
        (0..trace.buckets.len())
            .map(|i| {
                feed_per_second
                    .get(i)
                    .map(|d| d.as_secs_f64() * 1e3)
                    .unwrap_or(0.0)
            })
            .collect(),
    ));

    // Stop sampling (the last sample is the end of the grace period) and read the nodes' usage.
    let mut usage = Vec::new();
    if let (Some((stop, sampler, dipper)), Some(n)) = (capacity_tasks, &capacity.nodes) {
        stop.store(true, Ordering::Relaxed);
        if let Some(d) = dipper {
            d.abort();
        }
        let (samples, io, sends) = sampler.await.unwrap_or_default();
        usage = usage_from_samples(&n.handles(), &samples);
        let len = trace.buckets.len();
        if proc_sampler.is_some() {
            for (i, h) in n.handles().iter().enumerate() {
                let tag = h.node.to_string().replace(':', "_");
                let at = |s: usize| io.get(s).and_then(|row| row.get(i).copied().flatten());
                // Per second s: the change from sample s to s+1, as for the CPU columns.
                let delta = |f: &dyn Fn(&ProcIo) -> u64, scale: f64| -> Vec<f64> {
                    (0..len)
                        .map(|s| match (at(s), at(s + 1)) {
                            (Some(a), Some(b)) => f(&b).saturating_sub(f(&a)) as f64 / scale,
                            _ => f64::NAN,
                        })
                        .collect()
                };
                trace.extra.push((format!("wchar_kb_{tag}"), delta(&|p| p.wchar, 1024.0)));
                trace.extra.push((format!("rchar_kb_{tag}"), delta(&|p| p.rchar, 1024.0)));
                trace.extra.push((format!("syscw_{tag}"), delta(&|p| p.syscw, 1.0)));
                trace.extra.push((
                    format!("rss_mb_{tag}"),
                    (0..len)
                        .map(|s| at(s + 1).map(|p| p.rss_kb as f64 / 1024.0).unwrap_or(f64::NAN))
                        .collect(),
                ));
            }
        }
        // The lossy send queues' own counters (`--send-buffer lossy:...`), per second: what the
        // dataflow tried to send and what was dropped at a full queue, counted in the transport.
        for (i, h) in n.handles().iter().enumerate() {
            let at = |s: usize| sends.get(s).and_then(|row| row.get(i).copied().flatten());
            if (0..=len).all(|s| at(s).is_none()) {
                continue;
            }
            let tag = h.node.to_string().replace(':', "_");
            let delta = |k: usize, scale: f64| -> Vec<f64> {
                (0..len)
                    .map(|s| match (at(s), at(s + 1)) {
                        (Some(a), Some(b)) => b[k].saturating_sub(a[k]) as f64 / scale,
                        _ => f64::NAN,
                    })
                    .collect()
            };
            trace.extra.push((format!("sendq_attempted_{tag}"), delta(0, 1.0)));
            trace.extra.push((format!("sendq_attempted_kb_{tag}"), delta(1, 1024.0)));
            trace.extra.push((format!("sendq_dropped_{tag}"), delta(2, 1.0)));
            trace.extra.push((format!("sendq_dropped_kb_{tag}"), delta(3, 1024.0)));
        }
        for u in &usage {
            let col = |f: &dyn Fn(&(f64, u64, f64)) -> f64| -> Vec<f64> {
                (0..len)
                    .map(|i| u.per_second.get(i).map(f).unwrap_or(f64::NAN))
                    .collect()
            };
            let tag = u.node.to_string().replace(':', "_");
            trace.extra.push((format!("cpu_{tag}"), col(&|s| s.0)));
            trace
                .extra
                .push((format!("nr_throttled_{tag}"), col(&|s| s.1 as f64)));
            trace.extra.push((format!("throttled_ms_{tag}"), col(&|s| s.2)));
        }
    }
    // Before the deployment is stopped: which nodes had already died, and how.
    let node_failures = match &capacity.nodes {
        Some(n) => {
            let handles = n.handles();
            let backend = n.backend.clone();
            tokio::task::spawn_blocking(move || {
                handles
                    .iter()
                    .filter_map(|h| {
                        backend.exit(h).map(|e| {
                            if e.oom_killed {
                                format!("{} was OOM-killed at its memory limit", h.node)
                            } else {
                                format!("{} exited with code {}", h.node, e.exit_code)
                            }
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .await
            .unwrap_or_default()
        }
        None => Vec::new(),
    };
    for f in &node_failures {
        eprintln!("  node failure: {f}");
    }
    if let Err(e) = deployment.stop().await {
        eprintln!("  (deployment did not stop cleanly: {e})");
    }
    let limits = match &capacity.nodes {
        Some(n) => {
            let mut by_role: BTreeMap<String, String> = BTreeMap::new();
            for h in n.handles() {
                by_role
                    .entry(h.node.role.clone())
                    .or_insert_with(|| h.limits.to_string());
            }
            n.remove_all();
            // A node that was dying (an OOM kill) when it was removed can survive `rm -f`; sweep
            // everything of this trial by name.
            n.backend.remove_prefix(&format!("{}-", capacity.prefix));
            by_role
                .into_iter()
                .map(|(r, l)| format!("{r}: {l}"))
                .collect::<Vec<_>>()
                .join("; ")
        }
        None => "uncapped (no backend)".to_owned(),
    };
    drop(proc_sampler);
    drop(capacity);
    Executed {
        trace,
        kinds,
        by_id,
        rounds: driven.rounds,
        late: driven.late,
        feed_records,
        feed_late,
        feed_max_lag,
        usage,
        limits,
        node_failures,
    }
}

// ---------------------------------------------------------------------------------------------
// A check: μ, then the sweep

/// Runs one check: resolves μ (measuring it if need be), then deploys `once` for every trial of
/// the sweep, judges it, and records it. `once` builds, deploys and [`execute`]s the program for
/// one [`Spec`]; the generated code supplies it.
pub fn run_check<F>(meta: &Meta, run: &RunConfig, mut once: F) -> Vec<TrialResult>
where
    F: AsyncFnMut(&Spec) -> Executed,
{
    block_on(async {
        let host = host_fingerprint();
        let (mu, mu_source) = resolve_mu(meta, run, &host, &mut once).await;
        let mut results = Vec::new();
        if run.calibrate_only {
            println!(
                "{} ({}): μ = {mu:.1} rounds/s ({mu_source}) under {}",
                meta.function,
                meta.configuration,
                run.capacity
                    .key(meta, &meta.roles.iter().map(|r| r.to_string()).collect::<Vec<_>>())
            );
            return results;
        }
        for (factor, trial) in run.schedule() {
            let profile = run.profile(mu, factor);
            let seed = run.seed + trial as u64;
            let trigger = run.trigger_kind();
            let trigger_text = match (&run.cpu_dip, run.dips.is_empty()) {
                (Some(d), _) => format!(
                    "cpu quota ÷{} on {} at {}×μ",
                    d.k,
                    d.nodes
                        .iter()
                        .map(|n| n.to_string())
                        .collect::<Vec<_>>()
                        .join(","),
                    run.trigger_multiple
                ),
                (None, false) => format!(
                    "timing fault: {} at {}×μ",
                    run.dips
                        .iter()
                        .map(|(n, k)| format!("{n} period ×{k}"))
                        .collect::<Vec<_>>()
                        .join(", "),
                    run.trigger_multiple
                ),
                (None, true) => format!("burst {}×μ", run.trigger_multiple),
            };
            eprintln!(
                "  factor {factor}, trial {trial}: μ = {mu:.1} rounds/s ({mu_source}), λ = {:.1} rounds/s, trigger {:.0}s of {trigger_text}, tail {:.0}s",
                profile.baseline_rps,
                run.trigger.as_secs_f64(),
                run.tail.as_secs_f64()
            );
            let spec = Spec {
                profile,
                seed,
                label: format!("factor{factor}.trial{trial}"),
                probe: false,
                cpu_dip: run.cpu_dip.clone(),
                timing_dips: run.dips.clone(),
            };
            let ex = once(&spec).await;
            let mut verdict = judge_with_kinds(&ex.trace, &profile, &ex.kinds, ex.by_id);
            if !ex.open_loop() && !ex.harness_bound() {
                verdict.reason = format!(
                    "not open-loop: {} of {} records entered the program more than {} ms after their scheduled arrival (worst {:.0} ms), so the offered load was paced by the program (the program's own reading was {}: {})",
                    ex.feed_late,
                    ex.feed_records,
                    FEED_LATE.as_millis(),
                    ex.feed_max_lag.as_secs_f64() * 1e3,
                    verdict.outcome,
                    verdict.reason
                );
                verdict.outcome = Outcome::Invalid;
            }
            if ex.harness_bound() {
                verdict.reason = format!(
                    "the generator fell behind on {} arrivals while no node was ever throttled: the harness, not the program, was the bottleneck (the program's own reading was {}: {})",
                    ex.late, verdict.outcome, verdict.reason
                );
                verdict.outcome = Outcome::Invalid;
            }
            let tail = ex.trace.window(profile.trigger_end(), profile.total());
            let silent = silent_seconds(tail, &ex.kinds);
            if ex.node_failures.is_empty()
                && verdict.outcome != Outcome::Invalid
                && (silent as f64) > SILENT_MAX * tail.len().max(1) as f64
            {
                // The program's levels cannot be read; its capped nodes' kernel writes, measured
                // from outside, still can. Far below their baseline while the load is still
                // offered is a collapse; anything else cannot be judged.
                match outside_reading(&ex, &profile) {
                    Some((writes, cpu)) if writes <= OUTSIDE_COLLAPSE_WRITES => {
                        verdict.reason = format!(
                            "the program went silent in band ({silent} of the tail's {} seconds without a report); read from outside, its capped nodes wrote {:.0}% of their baseline bytes per second through the tail, at {:.0}% of their CPU quota (the program's own reading was {}: {})",
                            tail.len(),
                            writes * 100.0,
                            cpu * 100.0,
                            verdict.outcome,
                            verdict.reason
                        );
                        verdict.outcome = Outcome::Collapsed;
                    }
                    reading => {
                        verdict.reason = format!(
                            "the program went silent: no in-band report reached the harness in {silent} of the tail's {} seconds, so its levels cannot be read{} (the program's own reading was {}: {})",
                            tail.len(),
                            match reading {
                                Some((w, c)) => format!(
                                    "; from outside, its capped nodes wrote {:.0}% of their baseline bytes at {:.0}% of their CPU quota",
                                    w * 100.0,
                                    c * 100.0
                                ),
                                None => String::new(),
                            },
                            verdict.outcome,
                            verdict.reason
                        );
                        verdict.outcome = Outcome::Invalid;
                    }
                }
            }
            if !ex.node_failures.is_empty() {
                verdict.reason = format!(
                    "a node died during the run ({}), so this is a crash, not an overload (the program's own reading was {}: {})",
                    ex.node_failures.join("; "),
                    verdict.outcome,
                    verdict.reason
                );
                verdict.outcome = Outcome::Invalid;
            }
            let tag = match trigger {
                TriggerKind::Burst => String::new(),
                TriggerKind::CpuDip => {
                    let d = run.cpu_dip.as_ref().unwrap();
                    format!(
                        ".cpudip-{}x{}",
                        d.nodes
                            .iter()
                            .map(|n| sanitize(&n.to_string()))
                            .collect::<Vec<_>>()
                            .join("+"),
                        d.k
                    )
                }
                TriggerKind::TimingFault => run
                    .dips
                    .iter()
                    .map(|(n, k)| format!(".dip-{n}x{k}"))
                    .collect(),
            };
            let trace_name = format!(
                "{}.{}.{}.factor{}.trial{}{}.tsv",
                meta.crate_name, meta.function, meta.configuration, factor, trial, tag
            );
            let trace_path = run.results_dir.join("traces").join(trace_name);
            let trace_path = match fs::write(&trace_path, ex.trace.to_tsv()) {
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
                mu_source: mu_source.clone(),
                lambda: profile.baseline_rps,
                trigger,
                trigger_text,
                verdict,
                late: ex.late,
                feed_late: ex.feed_late,
                feed_max_lag: ex.feed_max_lag,
                max_levels: ex
                    .kinds
                    .iter()
                    .filter(|(_, k)| **k == StreamKind::Level)
                    .map(|(n, _)| {
                        let max = ex
                            .trace
                            .buckets
                            .iter()
                            .filter_map(|b| b.stream_values.get(n).copied())
                            .fold(0.0, f64::max);
                        (n.clone(), max)
                    })
                    .collect(),
                usage: ex.usage,
                limits: ex.limits,
                host: host.clone(),
                trace_path,
            };
            eprintln!("  [{}] {}", result.trigger, result.verdict);
            eprintln!(
                "    open loop: {}; {} records entered late, worst lag {:.1} ms; max levels: {}",
                if result.feed_late == 0 && result.late == 0 { "yes" } else { "NO" },
                result.feed_late,
                result.feed_max_lag.as_secs_f64() * 1e3,
                result
                    .max_levels
                    .iter()
                    .map(|(n, v)| format!("{n}={v:.0}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
            for u in &result.usage {
                eprintln!(
                    "    {} ({}): {:.3} cores mean, throttled in {:.0}% of periods ({:.0} ms)",
                    u.node,
                    u.limits,
                    u.mean_cores(),
                    u.throttled_fraction() * 100.0,
                    u.total.throttled_ns as f64 / 1e6
                );
            }
            record(run, &result);
            results.push(result);
        }
        results
    })
}

/// μ for `meta` under `run`: given, cached, measured, or declared, in that order of preference.
async fn resolve_mu<F>(meta: &Meta, run: &RunConfig, host: &str, once: &mut F) -> (f64, MuSource)
where
    F: AsyncFnMut(&Spec) -> Executed,
{
    if let Some(mu) = run.mu {
        return (mu, MuSource::Given);
    }
    let declared = || {
        meta.mu.unwrap_or_else(|| {
            panic!(
                "{} ({}) gives no `mu` and μ is not being measured; pass --mu, or measure it with a capacity backend",
                meta.function, meta.configuration
            )
        })
    };
    if run.calibrate == Calibrate::Off || run.capacity.backend == BackendChoice::None {
        return (declared(), MuSource::Declared);
    }
    let roles: Vec<String> = meta.roles.iter().map(|r| r.to_string()).collect();
    // μ depends on how the nodes were built as much as on their limits.
    let mut key = format!("{};build={}", run.capacity.key(meta, &roles), run.deploy_profile);
    // The send buffers change μ too; the default (unbounded everywhere) keeps the old key.
    if !run.unbounded_sends {
        key += ";send=bounded";
    } else if !run.bounded_send_roles.is_empty() {
        key += &format!(";send=bounded:{}", run.bounded_send_roles.join(","));
    } else if !run.lossy_send_roles.is_empty() {
        key += &format!(";send=lossy:{}:{}", run.lossy_capacity, run.lossy_send_roles.join(","));
    }
    for (r, m) in &run.capacity.role_memory {
        key += &format!(";mem[{r}]={}", m.map(|m| m.to_string()).unwrap_or_else(|| "-".to_owned()));
    }
    if run.calibrate == Calibrate::Auto
        && let Some(mu) = cached_mu(run, meta, &key, host)
    {
        eprintln!("  μ = {mu:.1} rounds/s, cached for {key} on {host}");
        return (mu, MuSource::Cached);
    }
    let mu = calibrate(meta, run, once).await;
    store_mu(run, meta, &key, host, mu);
    (mu, MuSource::Measured)
}

// ---------------------------------------------------------------------------------------------
// Calibration

/// One probe: the program at a constant offered rate.
#[derive(Debug, Clone)]
pub struct ProbePoint {
    pub rate: f64,
    /// Rounds per second actually offered in the measured window.
    pub offered_rate: f64,
    /// Goodput relative to what was offered (by id), or the conserved streams' records per
    /// round relative to the reference point's (without ids).
    pub goodput: f64,
    /// Levels whose second-half mean rose past the first-half mean.
    pub rising: Vec<String>,
    pub abandoned: u64,
    pub harness_bound: bool,
    pub mean_cores: Vec<(String, f64)>,
    pub throttled: f64,
    /// Seconds of the measured window in which no observed stream produced a record: the
    /// program's in-band reports stopped, so its levels cannot show a backlog.
    pub silent_seconds: usize,
    pub window_seconds: usize,
    /// A node died (OOM-killed or exited) during the probe.
    pub node_failed: bool,
    pub good: bool,
}

/// The largest fraction of a window's seconds that may have no in-band record at all for the
/// program's own streams to be read. A program whose reports stop cannot show a rising backlog
/// (its levels carry their last value forward), so a probe or tail that goes silent is not
/// evidence of keeping up.
pub const SILENT_MAX: f64 = 0.2;

/// Seconds of `window` in which none of the observed streams produced a record. Zero when the
/// program observes no streams.
pub fn silent_seconds(window: &[crate::trace::Bucket], kinds: &StreamKinds) -> usize {
    if kinds.is_empty() {
        return 0;
    }
    window
        .iter()
        .filter(|b| {
            kinds
                .keys()
                .all(|n| b.stream_records.get(n).copied().unwrap_or(0) == 0)
        })
        .count()
}

/// A silent tail reads as collapsed when the capped nodes' kernel writes, measured from outside,
/// are at most this fraction of their pre-trigger rate.
pub const OUTSIDE_COLLAPSE_WRITES: f64 = 0.5;

/// The capped nodes' outside-measured kernel writes (`wchar`) through the tail as a fraction of
/// their rate in the second half of the pre-trigger baseline, and their CPU use through the tail
/// as a fraction of their quota. `None` without a capacity backend's `/proc` columns.
pub fn outside_reading(ex: &Executed, profile: &Profile) -> Option<(f64, f64)> {
    let capped: Vec<(String, f64)> = ex
        .usage
        .iter()
        .filter_map(|u| {
            let q = u.limits.cpu?;
            Some((
                u.node.to_string().replace(':', "_"),
                q.quota_us as f64 / q.period_us as f64,
            ))
        })
        .collect();
    if capped.is_empty() {
        return None;
    }
    let col = |name: String| ex.trace.extra.iter().find(|(n, _)| *n == name).map(|(_, v)| v);
    let mean_sum = |prefix: &str, from: usize, to: usize| -> Option<f64> {
        let cols: Vec<&Vec<f64>> = capped
            .iter()
            .map(|(tag, _)| col(format!("{prefix}{tag}")))
            .collect::<Option<_>>()?;
        let per_second: Vec<f64> = (from..to)
            .map(|s| cols.iter().map(|c| c.get(s).copied().unwrap_or(f64::NAN)).sum::<f64>())
            .filter(|v| v.is_finite())
            .collect();
        (!per_second.is_empty()).then(|| per_second.iter().sum::<f64>() / per_second.len() as f64)
    };
    let start = profile.trigger_start().as_secs() as usize;
    let end = profile.trigger_end().as_secs() as usize;
    let total = profile.total().as_secs() as usize;
    let base = mean_sum("wchar_kb_", start / 2, start)?;
    let tail = mean_sum("wchar_kb_", end, total)?;
    let cpu = mean_sum("cpu_", end, total)?;
    let quota: f64 = capped.iter().map(|(_, q)| q).sum();
    (base > 0.0).then(|| (tail / base, cpu / quota))
}

/// The fraction of offered work a probe must complete to count as keeping up.
pub const PROBE_GOODPUT: f64 = 0.95;

/// The largest fraction of a probe's arrivals the generator may write late (more than 50 ms after
/// their scheduled time) for the probe to count as keeping up.
pub const PROBE_LATE: f64 = 0.01;

/// Measures μ: the highest constant offered rate (rounds per second) at which goodput is at least
/// [`PROBE_GOODPUT`] of what was offered, no level rises, and the generator could write all but
/// [`PROBE_LATE`] of the arrivals on time. Each rate is a fresh deployment,
/// offered for `probe_step` after a warm-up of a third of that, so an overloaded probe cannot
/// leave a backlog for the next. The search doubles (or halves) from the starting rate until it
/// brackets μ, then bisects geometrically until the bracket is within 10 %.
pub async fn calibrate<F>(meta: &Meta, run: &RunConfig, once: &mut F) -> f64
where
    F: AsyncFnMut(&Spec) -> Executed,
{
    let start = run.probe_start.or(meta.mu).unwrap_or(50.0);
    eprintln!(
        "  measuring μ: constant-rate probes of {:.0}s from {start:.1} rounds/s",
        run.probe_step.as_secs_f64()
    );
    let mut index = 0usize;
    let mut reference: Option<f64> = None;
    let mut probe = async |rate: f64, reference: &mut Option<f64>| -> ProbePoint {
        // Whole seconds, so that the measured window is exactly its per-second buckets.
        let step = Duration::from_secs(run.probe_step.as_secs_f64().ceil().max(1.0) as u64);
        let warm = Duration::from_secs((step.as_secs_f64() / 3.0).ceil().max(2.0) as u64);
        let profile = Profile {
            baseline_rps: rate,
            trigger_rps: rate,
            before: warm,
            trigger: step,
            tail: Duration::ZERO,
        };
        let spec = Spec {
            profile,
            seed: run.seed + 1000 + index as u64,
            label: format!("probe{index}"),
            probe: true,
            cpu_dip: None,
            timing_dips: Vec::new(),
        };
        index += 1;
        let ex = once(&spec).await;
        let p = judge_probe(&ex, &profile, rate, reference);
        eprintln!(
            "    probe {rate:>9.1}/s: offered {:.1}/s ({} late, {} entered late, worst {:.0} ms), goodput {:.1}%, {}{}{}{}{} → {}",
            p.offered_rate,
            ex.late,
            ex.feed_late,
            ex.feed_max_lag.as_secs_f64() * 1e3,
            p.goodput * 100.0,
            if p.rising.is_empty() {
                "levels flat".to_owned()
            } else {
                format!("{} rising", p.rising.join(", "))
            },
            if p.abandoned > 0 {
                format!(", {} abandoned", p.abandoned)
            } else {
                String::new()
            },
            if p.harness_bound {
                ", generator fell behind with no throttling (harness-bound)".to_owned()
            } else {
                format!(", throttled {:.0}%", p.throttled * 100.0)
            },
            if p.silent_seconds > 0 {
                format!(
                    ", silent {}/{} s (no in-band reports)",
                    p.silent_seconds, p.window_seconds
                )
            } else {
                String::new()
            },
            if p.node_failed { ", a node died" } else { "" },
            if p.good { "keeps up" } else { "falls behind" }
        );
        record_probe(run, meta, &ex, &p);
        p
    };
    let mut lo: Option<f64> = None;
    let mut hi: Option<f64> = None;
    let mut r = start;
    for _ in 0..12 {
        let p = probe(r, &mut reference).await;
        if p.good {
            lo = Some(r);
            if hi.is_some() {
                break;
            }
            r *= 2.0;
        } else {
            hi = Some(r);
            if lo.is_some() {
                break;
            }
            r /= 2.0;
            if r < 0.5 {
                break;
            }
        }
    }
    let (Some(mut lo), Some(mut hi)) = (lo, hi) else {
        let mu = lo.unwrap_or(r);
        eprintln!(
            "  WARNING: the probe never bracketed μ (it {} at every rate tried); taking μ = {mu:.1}",
            if lo.is_some() { "kept up" } else { "fell behind" }
        );
        return mu;
    };
    while hi / lo > 1.1 {
        let m = (lo * hi).sqrt();
        if probe(m, &mut reference).await.good {
            lo = m;
        } else {
            hi = m;
        }
    }
    eprintln!("  μ = {lo:.1} rounds/s (falls behind at {hi:.1})");
    lo
}

/// Reads one probe. The measured window is the part after the warm-up.
fn judge_probe(ex: &Executed, profile: &Profile, rate: f64, reference: &mut Option<f64>) -> ProbePoint {
    let window = ex.trace.window(profile.trigger_start(), profile.total());
    let w = window_stats(window, &ex.kinds);
    let secs = window.len().max(1) as f64;
    let rounds: u64 = window.iter().map(|b| b.rounds).sum();
    let offered_rate = rounds as f64 / secs;
    let goodput = if ex.by_id {
        w.completion_fraction
    } else {
        // Without ids, goodput is the conserved streams' records per round, against the same
        // ratio at the first probe that kept its levels flat.
        let per_round = w
            .streams
            .iter()
            .filter(|s| s.kind == StreamKind::Conserved)
            .filter_map(|s| s.per_round)
            .fold(f64::INFINITY, f64::min);
        if !per_round.is_finite() {
            1.0
        } else {
            match *reference {
                Some(r) if r > 0.0 => per_round / r,
                _ => 1.0,
            }
        }
    };
    // A backlog that the program cannot keep up with grows steadily, by the excess of the
    // offered rate over μ: a sustained trend across the window, larger than the level's own
    // size and than 5 % of what the window offered (and 10 items). See `level_rising`.
    let floor = (0.05 * rate * window.len() as f64).max(10.0);
    let rising: Vec<String> = ex
        .kinds
        .iter()
        .filter(|(_, k)| **k == StreamKind::Level)
        .filter(|(n, _)| {
            let values: Vec<f64> = window
                .iter()
                .filter_map(|b| b.stream_values.get(*n).copied())
                .collect();
            level_rising(&values, floor)
        })
        .map(|(n, _)| n.clone())
        .collect();
    let harness_bound = ex.harness_bound();
    // Arrivals the generator could not write on time: either the program pushed back (a full
    // socket, so it is not keeping up) or the harness could not keep up; either way the rate was
    // not offered open-loop.
    let late_fraction = ex.late as f64 / ex.rounds.max(1) as f64
        + ex.feed_late as f64 / ex.feed_records.max(1) as f64;
    let throttled = if ex.usage.is_empty() {
        0.0
    } else {
        ex.usage.iter().map(|u| u.throttled_fraction()).fold(0.0, f64::max)
    };
    let silent = silent_seconds(window, &ex.kinds);
    let node_failed = !ex.node_failures.is_empty();
    let good = goodput >= PROBE_GOODPUT
        && rising.is_empty()
        && (silent as f64) <= SILENT_MAX * window.len().max(1) as f64
        && !node_failed
        && w.abandoned == 0
        && offered_rate >= 0.8 * rate
        && offered_rate <= 1.25 * rate
        && late_fraction <= PROBE_LATE
        && !harness_bound;
    if good && reference.is_none() && !ex.by_id {
        *reference = w
            .streams
            .iter()
            .filter(|s| s.kind == StreamKind::Conserved)
            .filter_map(|s| s.per_round)
            .reduce(f64::min);
    }
    ProbePoint {
        rate,
        offered_rate,
        goodput,
        rising,
        abandoned: w.abandoned,
        harness_bound,
        mean_cores: ex
            .usage
            .iter()
            .map(|u| (u.node.to_string(), u.mean_cores()))
            .collect(),
        throttled,
        silent_seconds: silent,
        window_seconds: window.len(),
        node_failed,
        good,
    }
}

fn calibration_path(run: &RunConfig) -> PathBuf {
    run.results_dir.join("calibration.tsv")
}

/// The latest cached μ for this check, limits and host.
fn cached_mu(run: &RunConfig, meta: &Meta, key: &str, host: &str) -> Option<f64> {
    let text = fs::read_to_string(calibration_path(run)).ok()?;
    text.lines()
        .filter_map(|l| {
            let f: Vec<&str> = l.split('\t').collect();
            (f.len() >= 6
                && f[0] == meta.crate_name
                && f[1] == meta.function
                && f[2] == meta.configuration
                && f[3] == key
                && f[4] == host)
                .then(|| f[5].parse().ok())
                .flatten()
        })
        .last()
}

fn store_mu(run: &RunConfig, meta: &Meta, key: &str, host: &str, mu: f64) {
    let path = calibration_path(run);
    let write = || -> std::io::Result<()> {
        let new = !path.exists();
        let mut f = fs::OpenOptions::new().create(true).append(true).open(&path)?;
        if new {
            writeln!(f, "crate\tfunction\tconfiguration\tlimits\thost\tmu\tunix_time")?;
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        writeln!(
            f,
            "{}\t{}\t{}\t{key}\t{host}\t{mu:.2}\t{now}",
            meta.crate_name, meta.function, meta.configuration
        )
    };
    if let Err(e) = write() {
        eprintln!("sedge: could not cache μ in {}: {e}", path.display());
    }
}

fn record_probe(run: &RunConfig, meta: &Meta, ex: &Executed, p: &ProbePoint) {
    let path = run.results_dir.join("probes.tsv");
    let write = || -> std::io::Result<()> {
        let new = !path.exists();
        let mut f = fs::OpenOptions::new().create(true).append(true).open(&path)?;
        if new {
            writeln!(
                f,
                "function\tconfiguration\tlimits\trate\toffered_rate\tgoodput\trising\tabandoned\tharness_bound\tmax_throttled_fraction\tmean_cores\tgood"
            )?;
        }
        writeln!(
            f,
            "{}\t{}\t{}\t{:.2}\t{:.2}\t{:.4}\t{}\t{}\t{}\t{:.3}\t{}\t{}",
            meta.function,
            meta.configuration,
            ex.limits,
            p.rate,
            p.offered_rate,
            p.goodput,
            p.rising.join(","),
            p.abandoned,
            p.harness_bound,
            p.throttled,
            p.mean_cores
                .iter()
                .map(|(n, c)| format!("{n}={c:.3}"))
                .collect::<Vec<_>>()
                .join(","),
            p.good
        )
    };
    if let Err(e) = write() {
        eprintln!("sedge: could not record probe in {}: {e}", path.display());
    }
}

// ---------------------------------------------------------------------------------------------
// Reporting

const RESULTS_HEADER: &str = "function\tconfiguration\ttrigger\tfactor\ttrial\tseed\tmu\tmu_source\tlambda\toutcome\ttail_goodput\ttail_p50_ms\ttail_p99_ms\ttail_abandoned\toutstanding\tlate\tfeed_late\tfeed_max_lag_ms\tmax_levels\tby_id\trelative_goodput\tamplified_stream\tamplification\tnode_cores\tmax_throttled_fraction\tlimits\thost\treason\ttrace";

/// Appends one row for `result` to `results.tsv`. A table with another header (from an older
/// Sedge) is moved aside first.
fn record(run: &RunConfig, r: &TrialResult) {
    let ms = |d: Option<Duration>| {
        d.map(|d| format!("{:.1}", d.as_secs_f64() * 1e3))
            .unwrap_or_else(|| "-".to_owned())
    };
    let path = run.results_dir.join("results.tsv");
    let write = || -> std::io::Result<()> {
        if let Ok(text) = fs::read_to_string(&path)
            && text.lines().next() != Some(RESULTS_HEADER)
        {
            fs::rename(&path, run.results_dir.join("results.old.tsv"))?;
        }
        let new = !path.exists();
        let mut f = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)?;
        if new {
            writeln!(f, "{RESULTS_HEADER}")?;
        }
        writeln!(
            f,
            "{}\t{}\t{}\t{}\t{}\t{}\t{:.1}\t{}\t{:.2}\t{}\t{:.3}\t{}\t{}\t{}\t{}\t{}\t{}\t{:.1}\t{}\t{}\t{:.3}\t{}\t{:.2}\t{}\t{:.3}\t{}\t{}\t{}\t{}",
            r.function,
            r.configuration,
            r.trigger,
            r.factor,
            r.trial,
            r.seed,
            r.mu,
            r.mu_source,
            r.lambda,
            r.verdict.outcome,
            r.verdict.tail.completion_fraction,
            ms(r.verdict.tail.p50),
            ms(r.verdict.tail.p99),
            r.verdict.tail.abandoned,
            r.verdict.outstanding,
            r.late,
            r.feed_late,
            r.feed_max_lag.as_secs_f64() * 1e3,
            r.max_levels
                .iter()
                .map(|(n, v)| format!("{n}={v:.0}"))
                .collect::<Vec<_>>()
                .join(","),
            r.verdict.by_id,
            r.verdict.relative_goodput,
            r.verdict.amplification.0,
            r.verdict.amplification.1,
            r.usage
                .iter()
                .map(|u| format!("{}={:.3}", u.node, u.mean_cores()))
                .collect::<Vec<_>>()
                .join(","),
            r.usage
                .iter()
                .map(|u| u.throttled_fraction())
                .fold(0.0, f64::max),
            r.limits,
            r.host,
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

/// Prints every trial's verdict, then one summary line per check, trigger and factor with the
/// count of each outcome.
pub fn print_results(results: &[TrialResult]) {
    for r in results {
        println!(
            "{} ({}) [{}] factor {} trial {}: {}",
            r.function, r.configuration, r.trigger, r.factor, r.trial, r.verdict
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

/// Outcome counts per check, trigger and factor.
pub struct Summary<'r> {
    rows: BTreeMap<(&'static str, &'static str, TriggerKind, u64), Vec<&'r TrialResult>>,
}

impl<'r> Summary<'r> {
    pub fn new(results: &'r [TrialResult]) -> Self {
        let mut rows: BTreeMap<_, Vec<&TrialResult>> = BTreeMap::new();
        for r in results {
            rows.entry((r.function, r.configuration, r.trigger, r.factor.to_bits()))
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
            "{:<24} {:<16} {:<12} {:>6} {:>8} {:>9} {:>8} {:>9} {:>8} {:>9} {:>7}",
            "function",
            "configuration",
            "trigger",
            "factor",
            "μ",
            "μ from",
            "λ",
            "recovered",
            "draining",
            "collapsed",
            "invalid"
        )?;
        for ((function, configuration, trigger, _), trials) in &self.rows {
            let count = |o: Outcome| trials.iter().filter(|t| t.verdict.outcome == o).count();
            let first = trials[0];
            writeln!(
                f,
                "{:<24} {:<16} {:<12} {:>6} {:>8.1} {:>9} {:>8.1} {:>9} {:>8} {:>9} {:>7}",
                function,
                configuration,
                trigger.to_string(),
                first.factor,
                first.mu,
                first.mu_source.to_string(),
                first.lambda,
                count(Outcome::Recovered),
                count(Outcome::Draining),
                count(Outcome::Collapsed),
                count(Outcome::Invalid)
            )?;
        }
        if let Some(r) = self.rows.values().next().map(|t| t[0]) {
            write!(f, "limits: {}\nhost: {}", r.limits, r.host)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<RunConfig, String> {
        RunConfig::from_args(args.iter().map(|s| s.to_string()))
    }

    #[test]
    fn silent_seconds_counts_seconds_with_no_in_band_record() {
        let kinds: StreamKinds = [("unacked".to_owned(), StreamKind::Level)].into();
        let bucket = |records: u64| crate::trace::Bucket {
            stream_records: [("unacked".to_owned(), records)].into(),
            ..Default::default()
        };
        // A level carries its value forward, so only the record counts show the silence.
        let window = [bucket(9), bucket(0), bucket(9), bucket(0), bucket(0)];
        assert_eq!(silent_seconds(&window, &kinds), 3);
        assert_eq!(silent_seconds(&window, &StreamKinds::new()), 0);
        assert_eq!(silent_seconds(&[crate::trace::Bucket::default()], &kinds), 1);
    }

    fn meta() -> Meta {
        Meta {
            crate_name: "c",
            module: "m",
            function: "f",
            configuration: "x",
            mu: None,
            cpus: Some(0.3),
            role_cpus: vec![("Server", 0.2)],
            cpu_period_us: None,
            roles: &["Client", "Server"],
        }
    }

    #[test]
    fn caps_resolve_command_first_then_check_then_default() {
        let m = meta();
        let run = parse(&["--backend", "docker"]).unwrap();
        assert_eq!(run.capacity.limits(&m, "Server").cpu.unwrap().quota_us, 2000);
        assert_eq!(run.capacity.limits(&m, "client").cpu.unwrap().quota_us, 3000);
        let run = parse(&["--backend", "docker", "--cpus", "0.5"]).unwrap();
        assert_eq!(run.capacity.limits(&m, "Server").cpu.unwrap().quota_us, 5000);
        let run = parse(&["--backend", "docker", "--cpus", "0.5", "--cpus", "server=0.25"]).unwrap();
        assert_eq!(run.capacity.limits(&m, "Server").cpu.unwrap().quota_us, 2500);
        assert_eq!(run.capacity.limits(&m, "Client").cpu.unwrap().quota_us, 5000);
        // Nobody caps the client: no quota, but it still gets the nodes' cpuset.
        let m2 = Meta { cpus: None, ..meta() };
        let run = parse(&["--backend", "docker"]).unwrap();
        assert_eq!(run.capacity.limits(&m2, "Client").cpu, None);
        assert!(run.capacity.limits(&m2, "Client").cpuset.is_some() || run.capacity.node_cpuset.is_none());
        assert_eq!(run.capacity.limits(&m2, "Server").cpu.unwrap().quota_us, 2000);
        // `none` at any level means no quota.
        let run = parse(&["--backend", "docker", "--cpus", "Server=none"]).unwrap();
        assert_eq!(run.capacity.limits(&m, "Server").cpu, None);
        let m3 = Meta { role_cpus: vec![("Client", 0.0)], ..meta() };
        let run = parse(&["--backend", "docker"]).unwrap();
        assert_eq!(run.capacity.limits(&m3, "Client").cpu, None);
        assert!(parse(&["--cpus", "Client=0"]).is_err());
    }

    #[test]
    fn cpu_dip_options() {
        let run = parse(&[
            "--backend",
            "docker",
            "--cpu-dip",
            "4",
            "--cpu-dip-nodes",
            "Server,Replica:0",
        ])
        .unwrap();
        let dip = run.cpu_dip.clone().unwrap();
        assert_eq!(dip.k, 4.0);
        assert_eq!(
            dip.nodes,
            vec![
                NodeSelector::Role("Server".to_owned()),
                NodeSelector::Member("Replica".to_owned(), 0)
            ]
        );
        assert_eq!(run.trigger_kind(), TriggerKind::CpuDip);
        let replica0 = NodeSpec {
            role: "replica".to_owned(),
            member: Some(0),
        };
        assert!(dip.nodes[1].matches(&replica0));
        assert!(!dip.nodes[0].matches(&replica0));
        assert!(parse(&["--cpu-dip-nodes", "all"]).is_err());
        assert!(parse(&["--backend", "none", "--cpu-dip", "2"]).is_err());
        assert!(parse(&["--backend", "docker", "--cpu-dip", "2", "--dip", "t=2"]).is_err());
        let run = parse(&["--dip", "heartbeat=8"]).unwrap();
        assert_eq!(run.trigger_kind(), TriggerKind::TimingFault);
        assert_eq!(parse(&[]).unwrap().trigger_kind(), TriggerKind::Burst);
    }

    #[test]
    fn by_default_the_quota_never_changes_mid_run() {
        // Only `--cpu-dip` makes the harness touch a running node's quota.
        for args in [&[][..], &["--backend", "docker"][..], &["--dip", "t=8"][..], &["--recalibrate"][..]] {
            let run = parse(args).unwrap();
            assert!(run.cpu_dip.is_none(), "{args:?}");
        }
    }

    #[test]
    fn quotas() {
        let q = CpuQuota::from_cores(0.25, 10_000);
        assert_eq!(q.quota_us, 2500);
        assert_eq!(q.divided(4.0).quota_us, 1000);
        // Never below the kernel's 1 ms minimum.
        assert_eq!(q.divided(10.0).quota_us, 1000);
        assert_eq!(CpuQuota::from_cores(0.01, 10_000).quota_us, 1000);
    }

    #[test]
    fn nodes_are_built_optimized_unless_asked() {
        assert_eq!(parse(&[]).unwrap().deploy_profile, DEPLOY_PROFILE);
        assert_eq!(
            parse(&["--deploy-profile", "dev"]).unwrap().deploy_profile,
            "dev"
        );
    }

    #[test]
    fn calibration_key_covers_every_role() {
        let m = meta();
        let run = parse(&["--backend", "docker", "--node-cpus", "4-7"]).unwrap();
        let key = run.capacity.key(&m, &["Client".to_owned(), "Server".to_owned()]);
        assert_eq!(key, "docker;Client=3000/10000,Server=2000/10000;cpuset=4-7;mem=-");
        let run = parse(&["--backend", "none"]).unwrap();
        assert_eq!(run.capacity.key(&m, &[]), "uncapped");
    }
}
