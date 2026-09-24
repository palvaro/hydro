//! The amplification checker: does the schedule alone make a Hydro program do work its input
//! did not require, and if so, where?
//!
//! [`check`] takes a simulation and a fixed workload, compiles the simulation once, and runs
//! it many times. The first run follows the deterministic prompt schedule and discovers every
//! `use::batch` and `use::snapshot` hook that ever has something buffered. Each later run holds
//! one of those hooks (see [`super::hold_one_hook`]) for a chosen number of rounds, with every
//! other decision still following the prompt schedule, so a delay of any length on one edge is
//! a single decision. A held batch delays the records arriving at it; a held snapshot keeps its
//! tick observing a stale version of a singleton. Every run is measured with the counts the
//! simulator collects itself (see [`super::work_counts`]); nothing is read from the program's
//! outputs.
//!
//! # How long a hold lasts
//!
//! The checker does not ask the caller which delays to try, because a caller who knew the
//! program's timescales would not need the checker, and a list of delays that stops short of the
//! program's timeout would read benign with nothing to say so. Instead, for each hook, it holds
//! for 1 round, then 2, then 4, doubling, up to and including a hold that lasts to the end of the
//! run, which is the schedule in which that hook's records are never delivered. Every length in
//! the sequence is run for every hook, so each report carries the complete curve and a reader
//! can see whether the extra work grows with the length of the delay or steps once and stays
//! flat. The only quantity the caller sets is the run length in rounds, the *horizon*, and a
//! benign verdict is a statement about that horizon: no reaction to a delay of up to the horizon
//! was found at any decision point. The horizon is printed with every report.
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
//! hold that the simulator honoured raises either count above the unheld run at any hold length
//! tried, and [`Verdict::Benign`] otherwise. The rule uses only the existence of a gain. The
//! shape of the curve, whether the gain grows with the hold length or not, is reported for the
//! reader and is not a criterion the checker applies.
//!
//! # The location
//!
//! Among the hooks whose hold added work, the one that reacted to the shortest delay is
//! reported as the location: hooks are ranked by the smallest hold length at which extra work
//! first appeared, then, among hooks tied on that length, by the larger extra work at that
//! length, and only then by the largest extra work anywhere on the curve. The point that reacts
//! to the least delay is where the program's response to lateness begins, which is the fact a
//! reader wants; the largest gain over the whole curve is not a good primary criterion because
//! the last hold in the sequence lasts to the end of the run, and under a permanent hold on any
//! edge of a resend loop the sender exhausts its resends on every request, so several edges of
//! the same loop reach one ceiling and the largest gain no longer distinguishes them. The report
//! carries the winning hook's first-reaction length, its gain at that length, its largest gain
//! over the curve, and the hooks that admitted more records under it.
//!
//! # What the caller supplies
//!
//! The program, wired with `sim_input` streams for its timers and inputs, a closure that sends
//! one round of the fixed workload, and the horizon. The checker calls the closure once per
//! round and awaits [`super::quiesce`] after each call, so the closure need not; if it wants to
//! drain outputs each round it may await `quiesce()` itself first. The workload should be the
//! program's ordinary steady state, with no burst: the checker's job is to find schedules under
//! which that same input costs more. As each hook's escalation completes the checker prints one
//! line to standard error with that hook's curve, so a long run shows its findings as it goes.
//!
//! # What this does not see
//!
//! Records are counted where they enter a tick and where they cross the network. Work that a
//! program represents as a number inside a record, or as records flowing between operators
//! within one tick, is invisible. Snapshot hooks are held but their releases are not counted,
//! because a snapshot is one value per tick execution and not a record the program produced.
//! Hooks are held one at a time.

use std::collections::BTreeMap;
use std::fmt;
use std::panic::RefUnwindSafe;
use std::time::Instant;

use super::compiled::{CompiledSim, quiesce};
use super::flow::SimFlow;
use super::hold_one_hook::{HoldHandle, HoldOneHookDriver};
use super::work_counts::{self, WorkCounts};

/// The horizon [`CheckConfig::default`] uses, in rounds. See [`CheckConfig::rounds`] for how it
/// was chosen.
pub const DEFAULT_HORIZON: usize = 1000;

/// How long to run. The hold lengths are not configurable; see the [module docs](self).
#[derive(Debug, Clone)]
pub struct CheckConfig {
    /// Rounds of workload per run: the horizon. A benign verdict means no reaction was found to
    /// a delay of up to `rounds - hold_start` rounds. The default, [`DEFAULT_HORIZON`], is
    /// large because the horizon bounds the longest delay the checker can impose and a program
    /// whose reaction begins beyond it reads benign; the cost is roughly linear in the horizon
    /// and is printed with every report, so a caller who finds the default too slow can lower it
    /// knowingly. On the corpus a 240-round run costs about two seconds, so one hook costs
    /// about `2 s × (log2(rounds) + 1) × rounds / 240`, which at the default is roughly 90
    /// seconds per hook and, for a program with seven hooks, about ten minutes.
    pub rounds: usize,
    /// The round at which every hold begins. This is not a tuning knob: a hold delays whatever
    /// arrives at the held hook after it begins, and the program's reaction depends on how long
    /// the delay lasts, not on when it starts, so the checker starts every hold at round 1, after
    /// one round of ordinary operation has created the hooks and their state. It is kept as a
    /// field so that a report can say exactly which rounds were held.
    pub hold_start: usize,
    /// Whether to print one line to standard error as each hook's escalation completes.
    pub progress: bool,
}

impl Default for CheckConfig {
    fn default() -> Self {
        Self::new(DEFAULT_HORIZON)
    }
}

impl CheckConfig {
    /// A run of `rounds` rounds with every hold starting at round 1 and progress printed.
    pub fn new(rounds: usize) -> Self {
        Self {
            rounds,
            hold_start: 1,
            progress: true,
        }
    }

    /// Turns progress printing off.
    pub fn quiet(mut self) -> Self {
        self.progress = false;
        self
    }

    /// The longest delay the checker will impose: a hold from `hold_start` to the end of the run.
    pub fn horizon(&self) -> usize {
        self.rounds.saturating_sub(self.hold_start)
    }

    /// The hold lengths tried for every hook: 1, 2, 4, ... while below the horizon, then the
    /// horizon itself, which is a hold that lasts to the end of the run.
    pub fn hold_lengths(&self) -> Vec<usize> {
        let max = self.horizon();
        let mut ks = Vec::new();
        let mut k = 1usize;
        while k < max {
            ks.push(k);
            k *= 2;
        }
        if max > 0 {
            ks.push(max);
        }
        ks
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

/// What one held hook did to the counts, across the hold lengths tried.
#[derive(Debug, Clone)]
pub struct HookCurve {
    /// The hook's identity, `location#index [item type]`.
    pub hook: String,
    /// `(k, admitted records minus the unheld run's)` for each hold length, including `k = 0`.
    pub extra_admitted: Vec<(usize, i64)>,
    /// `(k, per-sender messages minus the unheld run's)` for each hold length.
    pub extra_sends_by_member: Vec<(usize, BTreeMap<String, i64>)>,
    /// The full counts at each hold length.
    pub counts: Vec<(usize, WorkCounts)>,
    /// Hold lengths at which the simulator forced the held hook to release. A curve with any
    /// refusal is not read for the verdict.
    pub refused_at: Vec<usize>,
    /// `(k, how many of the held hook's questions were answered "do not move")` for each hold
    /// length with `k > 0`. Zero at some `k` does not mean the hold had no effect: a tick whose
    /// only releasable hook is held is not runnable, so it parks and the held hook is never
    /// asked. That is how a hold on a single-hook tick (a server whose only input is its
    /// arrivals) takes effect. A nonzero count shows the hook was asked while held and answered
    /// "do not move" each time.
    pub holds_applied: Vec<(usize, u64)>,
    /// The largest rise, over the unheld run, of admitted records or of any one sender's
    /// messages, at any hold length.
    pub gain: i64,
    /// What rose to give `gain`: `"admitted records"` or a `sends from ...` place.
    pub rose: Option<String>,
    /// The smallest hold length at which any positive rise appeared.
    pub first_gain_at: Option<usize>,
    /// The largest rise at `first_gain_at` (admitted records or one sender's messages), used to
    /// rank hooks that first reacted at the same length.
    pub gain_at_first: i64,
}

impl HookCurve {
    /// The one line the checker prints for this hook: identity, both curves, and the gain.
    fn summary(&self) -> String {
        let extra: Vec<i64> = self.extra_admitted.iter().map(|(_, e)| *e).collect();
        let sends: Vec<i64> = self
            .extra_sends_by_member
            .iter()
            .map(|(_, m)| m.values().copied().max().unwrap_or(0))
            .collect();
        let mut s = format!(
            "hold {}: extra admitted {:?} extra sends (largest sender) {:?} gain {}",
            self.hook, extra, sends, self.gain
        );
        if let Some(r) = &self.rose {
            s.push_str(&format!(" in {r}"));
        }
        if let Some(k) = self.first_gain_at {
            s.push_str(&format!(" (first at k = {k}, gain there {})", self.gain_at_first));
        }
        if !self.holdable() {
            s.push_str(&format!(" (refused at k = {:?})", self.refused_at));
        }
        let unasked = self.unasked_while_held_at();
        if !unasked.is_empty() {
            s.push_str(&format!(" (tick parked, hook never asked, at k = {unasked:?})"));
        }
        s
    }

    /// The `use::batch` or `use::snapshot` source location, without the index and item type.
    pub fn source_location(&self) -> &str {
        self.hook.split('#').next().unwrap_or(&self.hook)
    }

    /// Whether the held hook was a `use::snapshot` (its identity carries the word `snapshot`).
    pub fn is_snapshot(&self) -> bool {
        self.hook.contains("[snapshot ")
    }

    /// Whether the simulator honoured the hold at every hold length.
    pub fn holdable(&self) -> bool {
        self.refused_at.is_empty()
    }

    /// Hold lengths at which the held hook was never asked a question while held. Its tick
    /// parked for the whole hold (see [`HookCurve::holds_applied`]).
    pub fn unasked_while_held_at(&self) -> Vec<usize> {
        self.holds_applied
            .iter()
            .filter(|(_, n)| *n == 0)
            .map(|(k, _)| *k)
            .collect()
    }
}

/// Where the hazard was found.
#[derive(Debug, Clone)]
pub struct Location {
    /// The held hook that reacted to the shortest delay (see the [module docs](self)).
    pub hook: String,
    /// That hook's `use::batch` or `use::snapshot` source location.
    pub source_location: String,
    /// What rose to give `gain`: `"admitted records"` or a `sends from ...` place.
    pub rose: String,
    /// The largest rise over the unheld run at any hold length.
    pub gain: i64,
    /// The smallest hold length at which a rise appeared.
    pub first_gain_at: usize,
    /// The rise at `first_gain_at`.
    pub gain_at_first: i64,
    /// Every hook that admitted more records under the winning hold than in the unheld run,
    /// at the hold length where the winning hook's admitted total peaked, with the rise. These
    /// are the edges the extra records crossed.
    pub hooks_that_rose: BTreeMap<String, i64>,
}

/// Everything [`check`] measured.
#[derive(Debug, Clone)]
pub struct Report {
    /// Hazardous if some honoured hold raised a count; benign otherwise.
    pub verdict: Verdict,
    /// Rounds of workload in every run.
    pub rounds: usize,
    /// The round at which every hold began.
    pub hold_start: usize,
    /// The longest delay imposed, in rounds: a hold from `hold_start` to the end of the run. A
    /// benign verdict is a statement about delays up to this length.
    pub horizon: usize,
    /// The hold lengths tried for every hook, in order, not including the unheld run.
    pub hold_lengths: Vec<usize>,
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
            "{} rounds, holds from round {} of {:?} rounds (horizon {}); baseline: admitted {} network {} over {} hooks with buffered input",
            self.rounds,
            self.hold_start,
            self.hold_lengths,
            self.horizon,
            self.baseline.admitted(),
            self.baseline.network(),
            self.curves.len()
        )?;
        for c in &self.curves {
            writeln!(f, "{}", c.summary())?;
        }
        match &self.location {
            Some(l) => {
                write!(
                    f,
                    "verdict hazardous: holding {} first added work at a delay of {} rounds ({} in {} there; largest gain {} over the curve)",
                    l.hook, l.first_gain_at, l.gain_at_first, l.rose, l.gain
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
            None => {
                write!(
                    f,
                    "verdict benign: no reaction to a delay of up to {} rounds was found at any decision point",
                    self.horizon
                )?;
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
    let hold_lengths = config.hold_lengths();
    let (baseline, hooks, _, _) = run_once(&compiled, config, &mut round, None, 0);
    let base_sends = baseline.sends_by_member();
    let base_admitted = baseline.admitted() as i64;

    let mut curves = Vec::with_capacity(hooks.len());
    for hook in &hooks {
        let mut extra_admitted = vec![(0usize, 0i64)];
        let mut extra_sends = vec![(0usize, BTreeMap::new())];
        let mut counts = vec![(0usize, baseline.clone())];
        let mut refused_at = Vec::new();
        let mut holds_applied = Vec::new();
        let mut gain = 0i64;
        let mut rose = None;
        let mut first_gain_at = None;
        let mut gain_at_first = 0i64;
        for &k in &hold_lengths {
            let (c, _, forced, applied) = run_once(&compiled, config, &mut round, Some(hook), k);
            if forced > 0 {
                refused_at.push(k);
            }
            holds_applied.push((k, applied));
            let d_admitted = c.admitted() as i64 - base_admitted;
            let mut d_sends = BTreeMap::new();
            let mut rise_here = d_admitted.max(0);
            if d_admitted > gain {
                gain = d_admitted;
                rose = Some("admitted records".to_owned());
            }
            for (place, n) in c.sends_by_member() {
                let d = n as i64 - base_sends.get(&place).copied().unwrap_or(0) as i64;
                rise_here = rise_here.max(d);
                if d > gain {
                    gain = d;
                    rose = Some(place.clone());
                }
                d_sends.insert(place, d);
            }
            if rise_here > 0 && first_gain_at.is_none() {
                first_gain_at = Some(k);
                gain_at_first = rise_here;
            }
            extra_admitted.push((k, d_admitted));
            extra_sends.push((k, d_sends));
            counts.push((k, c));
        }
        let curve = HookCurve {
            hook: hook.clone(),
            extra_admitted,
            extra_sends_by_member: extra_sends,
            counts,
            refused_at,
            holds_applied,
            gain,
            rose,
            first_gain_at,
            gain_at_first,
        };
        if config.progress {
            eprintln!(
                "[amplification {}/{} at {:.0} s] {}",
                curves.len() + 1,
                hooks.len(),
                started.elapsed().as_secs_f64(),
                curve.summary()
            );
        }
        curves.push(curve);
    }

    // Rank: shortest first-reaction length, then larger gain at that length, then larger gain
    // over the curve. A remaining tie goes to the first hook in identity order, and the report's
    // curves show it.
    let mut best: Option<&HookCurve> = None;
    for c in curves.iter().filter(|c| c.holdable() && c.gain > 0) {
        let key = |c: &HookCurve| {
            (
                std::cmp::Reverse(c.first_gain_at.unwrap_or(usize::MAX)),
                c.gain_at_first,
                c.gain,
            )
        };
        if best.is_none_or(|b| key(c) > key(b)) {
            best = Some(c);
        }
    }
    let location = best.map(|c| Location {
        hook: c.hook.clone(),
        source_location: c.source_location().to_owned(),
        rose: c.rose.clone().unwrap_or_default(),
        gain: c.gain,
        first_gain_at: c.first_gain_at.unwrap_or(0),
        gain_at_first: c.gain_at_first,
        hooks_that_rose: hooks_that_rose(&baseline, c),
    });
    Report {
        verdict: if location.is_some() {
            Verdict::Hazardous
        } else {
            Verdict::Benign
        },
        rounds: config.rounds,
        hold_start: config.hold_start,
        horizon: config.horizon(),
        hold_lengths,
        baseline,
        curves,
        location,
        seconds: started.elapsed().as_secs_f64(),
    }
}

/// One execution: `config.rounds` rounds of workload, holding `target` from `config.hold_start`
/// for `k` rounds (a `k` reaching the end of the run is a hold that never ends). Returns the
/// counts, the hooks the driver saw with buffered input, how many times the held hook was forced
/// to release, and how many times the hold was applied.
fn run_once(
    compiled: &CompiledSim,
    config: &CheckConfig,
    round: &mut (impl AsyncFnMut(usize) + RefUnwindSafe),
    target: Option<&str>,
    k: usize,
) -> (WorkCounts, Vec<String>, u64, u64) {
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
    (
        counts,
        handle.hooks_seen(),
        handle.forced_releases(),
        handle.holds_applied(),
    )
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

/// Hooks that admitted more records than in the unheld run, at the hold length where the
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
