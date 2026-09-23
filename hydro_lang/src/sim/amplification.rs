//! The amplification checker: does the schedule alone make a Hydro program do work its input
//! did not require, and if so, where?
//!
//! [`check`] takes a simulation and a fixed workload, compiles the simulation once, and runs
//! it many times. The first run follows the deterministic prompt schedule and discovers every
//! `use::batch` hook that ever has something buffered. Each later run holds one of those hooks
//! (see [`super::hold_one_hook`]) from a chosen round for a chosen number of rounds, with every
//! other decision still following the prompt schedule, so a delay of any length on one edge is a
//! single decision. Every run is measured with the counts the simulator collects itself (see
//! [`super::work_counts`]); nothing is read from the program's outputs.
//!
//! # The verdict rule
//!
//! A hold delays records and cannot create them, so any count that rises under a hold is the
//! program's reaction to delay. The rule reads two counts against the unheld run on the same
//! input: each cluster member's (or process's) outgoing network messages, and the program's
//! total records admitted into ticks by batch hooks. Messages are read per sender because a
//! cluster total can fall while one member's traffic rises (a leader that stops sending
//! heartbeats while followers start sending vote requests). Admitted records are read in total
//! because within one location a reaction can replace one record with another (a completion
//! becomes an abandonment) without adding work. The verdict is [`Verdict::Hazardous`] if some
//! hold that the simulator honoured raises either count above the unheld run at any hold length,
//! and [`Verdict::Benign`] otherwise. The held hook whose gain is largest is reported as the
//! location, together with the hold length at which its gain first appeared and the hooks that
//! admitted more records under it.
//!
//! # What the caller supplies
//!
//! The program, wired with `sim_input` streams for its timers and inputs, and a closure that
//! sends one round of the fixed workload. The checker calls the closure once per round and
//! awaits [`super::quiesce`] after each call, so the closure need not; if it wants to drain
//! outputs each round it may await `quiesce()` itself first. The workload should be the
//! program's ordinary steady state, with no burst: the checker's job is to find schedules under
//! which that same input costs more.
//!
//! # What this does not see
//!
//! Records are counted where they enter a tick and where they cross the network. Work that a
//! program represents as a number inside a record, or as records flowing between operators
//! within one tick, is invisible. `use::snapshot` hooks are neither held nor counted.

use std::collections::BTreeMap;
use std::fmt;
use std::panic::RefUnwindSafe;
use std::time::Instant;

use super::compiled::{CompiledSim, quiesce};
use super::flow::SimFlow;
use super::hold_one_hook::{HoldHandle, HoldOneHookDriver};
use super::work_counts::{self, WorkCounts};

/// The hold lengths, in rounds, the corpus experiments used.
pub const DEFAULT_GRID: &[usize] = &[0, 5, 10, 20, 40, 60, 80, 100];

/// How long to run and where to hold.
#[derive(Debug, Clone)]
pub struct CheckConfig {
    /// Rounds of workload per run.
    pub rounds: usize,
    /// The round at which a hold begins; everything before it is warm-up.
    pub hold_start: usize,
    /// Hold lengths to try for every hook. Zero is the unheld run and is implied.
    pub grid: Vec<usize>,
}

impl CheckConfig {
    /// 240 rounds, holds from round 20, the [`DEFAULT_GRID`].
    pub fn new(rounds: usize) -> Self {
        Self {
            rounds,
            hold_start: 20,
            grid: DEFAULT_GRID.to_vec(),
        }
    }

    /// Replaces the hold grid.
    pub fn with_grid(mut self, grid: &[usize]) -> Self {
        self.grid = grid.to_vec();
        self
    }

    /// Replaces the round at which holds begin.
    pub fn with_hold_start(mut self, hold_start: usize) -> Self {
        self.hold_start = hold_start;
        self
    }
}

/// The checker's answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Some hold raised a member's outgoing messages or the program's admitted records.
    Hazardous,
    /// No hold added any work.
    Benign,
}

impl fmt::Display for Verdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Verdict::Hazardous => "hazardous",
            Verdict::Benign => "benign",
        })
    }
}

/// What one held hook did to the counts, across the grid.
#[derive(Debug, Clone)]
pub struct HookCurve {
    /// The hook's identity, `location#index [item type]`.
    pub hook: String,
    /// `(k, admitted records minus the unheld run's)` for each grid point, including `k = 0`.
    pub extra_admitted: Vec<(usize, i64)>,
    /// `(k, per-sender messages minus the unheld run's)` for each grid point.
    pub extra_sends_by_member: Vec<(usize, BTreeMap<String, i64>)>,
    /// The full counts at each grid point.
    pub counts: Vec<(usize, WorkCounts)>,
    /// Hold lengths at which the simulator forced the held hook to release. A curve with any
    /// refusal is not read for the verdict.
    pub refused_at: Vec<usize>,
    /// The largest rise, over the unheld run, of admitted records or of any one sender's
    /// messages, at any grid point.
    pub gain: i64,
    /// What rose to give `gain`: `"admitted records"` or a `sends from ...` place.
    pub rose: Option<String>,
    /// The smallest hold length at which any positive rise appeared.
    pub first_gain_at: Option<usize>,
}

impl HookCurve {
    /// The `use::batch` source location, without the index and item type.
    pub fn source_location(&self) -> &str {
        self.hook.split('#').next().unwrap_or(&self.hook)
    }

    /// Whether the simulator honoured the hold at every grid point.
    pub fn holdable(&self) -> bool {
        self.refused_at.is_empty()
    }
}

/// Where the hazard was found.
#[derive(Debug, Clone)]
pub struct Location {
    /// The held hook whose hold produced the largest gain.
    pub hook: String,
    /// That hook's `use::batch` source location.
    pub source_location: String,
    /// What rose: `"admitted records"` or a `sends from ...` place.
    pub rose: String,
    /// The largest rise over the unheld run.
    pub gain: i64,
    /// The smallest hold length at which a rise appeared.
    pub first_gain_at: usize,
    /// Every hook that admitted more records under the winning hold than in the unheld run,
    /// at the grid point where the winning hook's admitted total peaked, with the rise. These
    /// are the edges the extra records crossed.
    pub hooks_that_rose: BTreeMap<String, i64>,
}

/// Everything [`check`] measured.
#[derive(Debug, Clone)]
pub struct Report {
    pub verdict: Verdict,
    /// Counts from the unheld run.
    pub baseline: WorkCounts,
    /// One curve per hook the discovery run found with buffered input.
    pub curves: Vec<HookCurve>,
    /// Present when the verdict is hazardous.
    pub location: Option<Location>,
    /// Wall clock for the whole check, including compilation.
    pub seconds: f64,
}

impl fmt::Display for Report {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "baseline: admitted {} network {} over {} hooks with buffered input",
            self.baseline.admitted(),
            self.baseline.network(),
            self.curves.len()
        )?;
        for c in &self.curves {
            let extra: Vec<i64> = c.extra_admitted.iter().map(|(_, e)| *e).collect();
            write!(f, "hold {}: extra admitted {:?} gain {}", c.hook, extra, c.gain)?;
            if let Some(r) = &c.rose {
                write!(f, " in {r}")?;
            }
            if !c.holdable() {
                write!(f, " (refused at k = {:?})", c.refused_at)?;
            }
            writeln!(f)?;
        }
        write!(f, "verdict {}", self.verdict)?;
        if let Some(l) = &self.location {
            write!(
                f,
                ": largest gain {} in {} when holding {} (first at k = {})",
                l.gain, l.rose, l.hook, l.first_gain_at
            )?;
            if !l.hooks_that_rose.is_empty() {
                let rises: Vec<String> = l
                    .hooks_that_rose
                    .iter()
                    .map(|(h, d)| format!("{h} +{d}"))
                    .collect();
                write!(f, "; hooks that admitted more: {}", rises.join("; "))?;
            }
        }
        writeln!(f, " in {:.1} s", self.seconds)
    }
}

/// Runs the checker. `round(i)` sends round `i` of the fixed workload; the checker awaits
/// `quiesce()` after it. See the [module docs](self).
pub fn check(
    flow: SimFlow,
    config: &CheckConfig,
    mut round: impl AsyncFnMut(usize) + RefUnwindSafe,
) -> Report {
    let started = Instant::now();
    let compiled = flow.compiled();
    let (baseline, hooks, _) = run_once(&compiled, config, &mut round, None, 0);
    let base_sends = baseline.sends_by_member();
    let base_admitted = baseline.admitted() as i64;

    let mut curves = Vec::with_capacity(hooks.len());
    for hook in &hooks {
        let mut extra_admitted = vec![(0usize, 0i64)];
        let mut extra_sends = vec![(0usize, BTreeMap::new())];
        let mut counts = vec![(0usize, baseline.clone())];
        let mut refused_at = Vec::new();
        let mut gain = 0i64;
        let mut rose = None;
        let mut first_gain_at = None;
        for &k in config.grid.iter().filter(|k| **k > 0) {
            let (c, _, forced) = run_once(&compiled, config, &mut round, Some(hook), k);
            if forced > 0 {
                refused_at.push(k);
            }
            let d_admitted = c.admitted() as i64 - base_admitted;
            let mut d_sends = BTreeMap::new();
            let mut rise_here = false;
            if d_admitted > 0 {
                rise_here = true;
            }
            if d_admitted > gain {
                gain = d_admitted;
                rose = Some("admitted records".to_owned());
            }
            for (place, n) in c.sends_by_member() {
                let d = n as i64 - base_sends.get(&place).copied().unwrap_or(0) as i64;
                if d > 0 {
                    rise_here = true;
                }
                if d > gain {
                    gain = d;
                    rose = Some(place.clone());
                }
                d_sends.insert(place, d);
            }
            if rise_here && first_gain_at.is_none() {
                first_gain_at = Some(k);
            }
            extra_admitted.push((k, d_admitted));
            extra_sends.push((k, d_sends));
            counts.push((k, c));
        }
        curves.push(HookCurve {
            hook: hook.clone(),
            extra_admitted,
            extra_sends_by_member: extra_sends,
            counts,
            refused_at,
            gain,
            rose,
            first_gain_at,
        });
    }

    // On a tie the first hook in discovery order (sorted by identity) wins.
    let mut best: Option<&HookCurve> = None;
    for c in curves.iter().filter(|c| c.holdable() && c.gain > 0) {
        if best.is_none_or(|b| c.gain > b.gain) {
            best = Some(c);
        }
    }
    let location = best.map(|c| Location {
        hook: c.hook.clone(),
        source_location: c.source_location().to_owned(),
        rose: c.rose.clone().unwrap_or_default(),
        gain: c.gain,
        first_gain_at: c.first_gain_at.unwrap_or(0),
        hooks_that_rose: hooks_that_rose(&baseline, c),
    });
    Report {
        verdict: if location.is_some() {
            Verdict::Hazardous
        } else {
            Verdict::Benign
        },
        baseline,
        curves,
        location,
        seconds: started.elapsed().as_secs_f64(),
    }
}

/// One execution: `config.rounds` rounds of workload, holding `target` from `config.hold_start`
/// for `k` rounds. Returns the counts, the hooks the driver saw with buffered input, and how
/// many times the held hook was forced to release.
fn run_once(
    compiled: &CompiledSim,
    config: &CheckConfig,
    round: &mut (impl AsyncFnMut(usize) + RefUnwindSafe),
    target: Option<&str>,
    k: usize,
) -> (WorkCounts, Vec<String>, u64) {
    let (driver, handle) = HoldOneHookDriver::new();
    work_counts::enable();
    let handle_ref = &handle;
    compiled.run_with_driver(driver, async |instance| {
        instance
            .run_with_scheduler(async {
                for r in 0..config.rounds {
                    apply_hold(handle_ref, target, k, r, config.hold_start);
                    round(r).await;
                    quiesce().await;
                }
            })
            .await
    });
    handle.end_hold();
    let counts = work_counts::take();
    (counts, handle.hooks_seen(), handle.forced_releases())
}

fn apply_hold(handle: &HoldHandle, target: Option<&str>, k: usize, round: usize, start: usize) {
    if let Some(t) = target {
        if round == start && k > 0 {
            handle.begin_hold(t);
        }
        if round == start + k {
            handle.end_hold();
        }
    }
}

/// Hooks that admitted more records than in the unheld run, at the grid point where the
/// curve's admitted total peaked.
fn hooks_that_rose(baseline: &WorkCounts, curve: &HookCurve) -> BTreeMap<String, i64> {
    let Some((_, peak)) = curve
        .counts
        .iter()
        .max_by_key(|(_, c)| c.admitted())
    else {
        return BTreeMap::new();
    };
    let mut rises = BTreeMap::new();
    for (hook, n) in &peak.hook_releases {
        let d = *n as i64 - baseline.hook_releases.get(hook).copied().unwrap_or(0) as i64;
        if d > 0 {
            rises.insert(hook.clone(), d);
        }
    }
    rises
}
