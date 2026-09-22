//! The sweep: the amplification checker, as a tool rather than a hand-written test
//! (`design_docs/2026-09_amplification_as_adversarial_scheduling.md`, "Next: the sweep").
//!
//! Every result before this module was found by a person reading the program and picking the
//! edge to hold. The sweep is given a program with fixed inputs, the edges the driver meters as
//! clocks, and a goal extractor (which payload field of a record names the goal it was derived
//! toward); it discovers every other tick-boundary hook edge from a prompt run and, for each of
//! them, for each move shape ([`Shape::Constant`], a delay of `d` decisions on every record, and
//! [`Shape::Bursty`], a gap of `d` decisions followed by a burst) and each `d` of a grid, runs the
//! same inputs and compares derivations per goal with the prompt run. The verdict is the design's
//! first principle: same inputs, two schedules, derivations per goal ([`Lineage::per_goal`],
//! payload attribution at the hooks; no lineage pass). It reports, per (edge, shape):
//!
//! - the threshold: the smallest `d` at which the program does more work toward some goal than
//!   under prompt delivery, or its own progress signals change (refined by bisection between the
//!   grid points once the grid has bracketed it);
//! - the gain at every `d`: derivations toward goals in total, the histogram of derivations per
//!   goal, the largest per-goal ratio against prompt, the records no goal accounts for;
//! - the shape of the gain in logical time: the excess over prompt in four equal windows of the
//!   clock, and whether it is transient (confined to the early windows) or sustained;
//! - the program's own progress signals (terms, campaigns, commits), as the harness reports them.
//!
//! Logical time is the metered clock ([`Program::time`]): a record's time is the clock count at
//! the decision in which it *arrived* at its hook, which a hold cannot move (its release can).
//!
//! Every run of the sweep happens under [`super::edge_counts::with_empty_ticks_allowed`], so an
//! edge alone in its tick can be held like any other; the end-of-run tail therefore drains held
//! records `d` decisions late rather than flushing them when the clock runs dry.
//!
//! What the sweep does not do, to be stated in every report: its moves cover one of the design's
//! five hook dimensions, release timing. Release *size* produced the sim collapse with no hold at
//! all; order, crash and membership moves do not exist yet. Keyed `batch` edges are counted but
//! not logged by the lineage side log and cannot be held item by item; the sweep tries the bursty
//! shape on them (all keys or none) and says so.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Debug;
use std::rc::Rc;

use super::edge_counts::{EdgeCounts, with_empty_ticks_allowed};
use super::hold_schedule::{EdgePolicy, Fork, HoldScheduleDriver};
use super::lineage::{Lineage, Record};

/// The two move shapes the sweep tries on every edge; both bound the delay of any record by `d`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Shape {
    /// [`EdgePolicy::Hold`]`(d)`: every record is released `d` decisions after it arrived.
    Constant,
    /// [`EdgePolicy::Periodic`]` { period: d, count: None }`: nothing for `d - 1` decisions,
    /// then everything buffered.
    Bursty,
}

impl Shape {
    /// The policy of this shape at delay `d`.
    pub fn policy(self, d: u64) -> EdgePolicy {
        match self {
            Shape::Constant => EdgePolicy::Hold(d),
            Shape::Bursty => EdgePolicy::Periodic { period: d, count: None },
        }
    }
}

/// How the harness feeds the program its fixed inputs; the report says which was used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputShape {
    /// Everything is sent before the run, and the driver meters it: time is decisions.
    UpFront,
    /// A round of inputs, then `quiesce()`, repeatedly: a hold lasts `d` empty rounds inside the
    /// quiesce, so time is rounds.
    RoundByRound,
    /// The harness reacts to outputs (the collapse harness): the reaction is part of the workload.
    ClosedLoop,
}

/// What one run of the program under a driver produced, as the harness returns it.
pub struct Run {
    /// Per-edge record counts (every hook edge, keyed batches included).
    pub counts: EdgeCounts,
    /// The boundary log (every release of every non-keyed hook edge, with payloads).
    pub lineage: Lineage,
    /// The program's own progress signals, named by the harness: terms, campaigns, commits,
    /// decrees per command. Compared against the prompt run's.
    pub progress: BTreeMap<String, i64>,
}

/// A program under the sweep: the fixed inputs, the clocks, and the one declared line of program
/// knowledge, the goal extractor.
pub struct Program<G> {
    /// For the report.
    pub name: String,
    /// See [`InputShape`].
    pub inputs: InputShape,
    /// Runs the fixed inputs under `driver`, which already carries the clock policies and the
    /// move, and returns what happened. Called once per cell of the sweep.
    pub run: Box<dyn Fn(HoldScheduleDriver) -> Run>,
    /// The clocks: edges the driver meters or paces in every run (a metered input stream, a
    /// periodic timer). Never moved.
    pub clocks: Vec<(Rc<dyn Fn(&str) -> bool>, EdgePolicy)>,
    /// The clock whose non-empty releases are logical time (one of the clocks), if the program
    /// has one. Without it the gain has no shape: a single-shot program.
    pub time: Option<Box<dyn Fn(&str) -> bool>>,
    /// The goal extractor: which goals a record on an edge was derived toward (none, one, or
    /// several). Records for which it returns none are counted as unattributed.
    pub goals: Box<dyn Fn(&str, &Record) -> Vec<G>>,
    /// Which goals a record on an edge *reaches* (a completion, a commit), if the goal edge is a
    /// hook edge; `None` if the harness reports goals reached through `progress` instead. Records
    /// it names are neither derivations nor unattributed.
    pub reached: Option<Box<dyn Fn(&str, &Record) -> Vec<G>>>,
}

/// A clock entry for [`Program::clocks`].
pub fn clock(pred: impl Fn(&str) -> bool + 'static, policy: EdgePolicy) -> (Rc<dyn Fn(&str) -> bool>, EdgePolicy) {
    (Rc::new(pred), policy)
}

/// Number of equal windows of logical time the gain is broken into.
pub const WINDOWS: usize = 4;

/// The measurements of one run, reduced for comparison.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Measure<G: Ord> {
    /// Derivations toward each goal, over every non-clock edge.
    pub per_goal: BTreeMap<G, u64>,
    /// Derivations toward each goal on each edge.
    pub per_edge_per_goal: BTreeMap<String, BTreeMap<G, u64>>,
    /// Records on non-clock edges attributed to no goal (and not reaching one).
    pub unattributed: u64,
    /// Records reaching each goal (see [`Program::reached`]).
    pub reached: BTreeMap<G, u64>,
    /// Derivations toward goals whose producing decision fell in each window of the clock.
    pub by_window: [u64; WINDOWS],
    /// Records per edge (every edge, clocks included), from the counter.
    pub records: BTreeMap<String, u64>,
    /// The harness's progress signals.
    pub progress: BTreeMap<String, i64>,
    /// Non-empty releases of the time clock: the length of the run in logical time.
    pub clock_ticks: u64,
}

impl<G: Ord> Measure<G> {
    /// Derivations toward goals, total.
    pub fn derivations(&self) -> u64 {
        self.per_goal.values().sum()
    }

    /// Histogram of derivations per goal: how many goals were derived toward `n` times.
    pub fn histogram(&self) -> BTreeMap<u64, usize> {
        let mut h = BTreeMap::new();
        for n in self.per_goal.values() {
            *h.entry(*n).or_default() += 1;
        }
        h
    }
}

/// Whether the extra work over prompt keeps coming as the run goes on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Gain {
    /// No goal was derived toward more often than under prompt, and no progress signal changed.
    None,
    /// Extra derivations, but at least three quarters of them in the first half of the run.
    Transient,
    /// Extra derivations, at least a quarter of them in the second half of the run.
    Sustained,
    /// No extra derivations, but a progress signal changed (the program is doing different
    /// work, e.g. elections instead of heartbeats).
    ProgressOnly,
    /// Extra derivations in a program with no clock ([`Program::time`] is `None`), so no shape.
    Extra,
}

/// One cell of the sweep: one edge, one shape, one `d`.
#[derive(Debug, Clone)]
pub struct Cell<G: Ord> {
    /// The delay.
    pub d: u64,
    /// What was measured.
    pub measure: Measure<G>,
    /// Excess derivations over prompt per window (this run's minus prompt's).
    pub excess_by_window: [i64; WINDOWS],
    /// Largest `this / prompt` over goals prompt derived toward at least once.
    pub max_ratio: f64,
    /// Progress signals whose value differs from prompt: `(name, prompt, this)`.
    pub progress_changes: Vec<(String, i64, i64)>,
    /// The verdict for this cell.
    pub gain: Gain,
}

impl<G: Ord> Cell<G> {
    /// Extra derivations over prompt, total (may be negative).
    pub fn excess(&self) -> i64 {
        self.excess_by_window.iter().sum()
    }

    /// Whether this cell found anything.
    pub fn found(&self) -> bool {
        self.gain != Gain::None
    }
}

/// The sweep over one edge under one shape.
#[derive(Debug, Clone)]
pub struct EdgeSweep<G: Ord> {
    /// The edge key.
    pub edge: String,
    /// The shape.
    pub shape: Shape,
    /// The cells, in increasing `d`.
    pub cells: Vec<Cell<G>>,
    /// The smallest `d` at which the cell found something, after bisection.
    pub threshold: Option<u64>,
    /// The smallest `d` at which the gain is sustained, after bisection (the knee between a
    /// bounded transient and a loop), if any cell is.
    pub sustained_from: Option<u64>,
    /// The smallest `d` at which a progress signal differs from prompt, after bisection, if any.
    pub progress_from: Option<u64>,
    /// The verdict at the largest `d` tried.
    pub verdict: Gain,
    /// Why the edge was not (fully) swept, if it was not.
    pub note: Option<String>,
}

/// The whole report.
#[derive(Debug, Clone)]
pub struct Report<G: Ord> {
    /// The program's name.
    pub program: String,
    /// See [`InputShape`].
    pub inputs: InputShape,
    /// The prompt run.
    pub prompt: Measure<G>,
    /// The clock edges (matched by the harness's predicates in the prompt run).
    pub clocks: Vec<String>,
    /// Every edge sweep, in edge key order then shape order.
    pub sweeps: Vec<EdgeSweep<G>>,
    /// Edges with no records under prompt, which cannot be held.
    pub idle_edges: Vec<String>,
    /// Runs performed, including the prompt run.
    pub runs: usize,
    /// See [`Options::empty_ticks`].
    pub empty_ticks: bool,
}

/// A geometric-ish grid of delays up to `max` (inclusive if it falls on a point): 1, 2, 3, 4,
/// 6, 8, 12, 16, 24, 32, ...
pub fn grid(max: u64) -> Vec<u64> {
    let mut ds = vec![];
    let mut a = 1;
    while a <= max {
        ds.push(a);
        if a >= 2 {
            let b = a + a / 2;
            if b <= max {
                ds.push(b);
            }
        }
        a *= 2;
    }
    ds.sort_unstable();
    ds.dedup();
    ds
}

/// Options of a sweep.
pub struct Options {
    /// The delays to try on every edge, in increasing order.
    pub ds: Vec<u64>,
    /// Whether to bisect between the last `d` without a finding and the first with one.
    pub refine: bool,
    /// Whether to print every cell as it is measured.
    pub verbose: bool,
    /// Whether every run happens under [`with_empty_ticks_allowed`] (the default: an edge alone
    /// in its tick can then be held). Under the forcing rule instead, such an edge is silently
    /// prompt, the end of the run flushes what is held when the clock runs dry, and a tick with
    /// a held-back interrupt does not run empty, which can shift the phase of one member's timers
    /// against another's.
    pub empty_ticks: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            ds: grid(64),
            refine: true,
            verbose: true,
            empty_ticks: true,
        }
    }
}

/// Runs `program` once under its clocks, `moves` and `forks`, with empty ticks allowed if
/// `empty_ticks`. A move on a clock edge is not possible (the clock rules come first).
pub(crate) fn run_with<G>(program: &Program<G>, moves: &[(String, EdgePolicy)], forks: &[Fork], empty_ticks: bool) -> Run {
    let mut driver = HoldScheduleDriver::new();
    for (pred, policy) in &program.clocks {
        let pred = pred.clone();
        driver = driver.with_policy(move |key: &str| pred(key), *policy);
    }
    for (edge, policy) in moves {
        let edge = edge.clone();
        driver = driver.with_policy(move |key: &str| key == edge, *policy);
    }
    for fork in forks {
        driver = driver.with_fork(fork.clone());
    }
    if empty_ticks {
        with_empty_ticks_allowed(|| (program.run)(driver))
    } else {
        (program.run)(driver)
    }
}

impl<G: Ord + Clone + Debug> Program<G> {
    /// Reduces a run to its measurements.
    fn measure(&self, run: &Run, clocks: &BTreeSet<String>) -> Measure<G> {
        // Logical time per (edge, member, decision index): the number of non-empty time-clock
        // releases logged before that decision.
        let mut clock = 0u64;
        let mut time_at: HashMap<(&str, Option<u32>, u64), u64> = HashMap::new();
        for release in &run.lineage.releases {
            time_at.insert((release.edge.as_str(), release.member, release.tick), clock);
            if let Some(time) = &self.time
                && time(&release.edge)
                && !release.released.is_empty()
            {
                clock += 1;
            }
        }
        let clock_ticks = clock.max(1);
        let window = |t: u64| ((t * WINDOWS as u64 / clock_ticks) as usize).min(WINDOWS - 1);

        let mut per_goal: BTreeMap<G, u64> = BTreeMap::new();
        let mut per_edge_per_goal: BTreeMap<String, BTreeMap<G, u64>> = BTreeMap::new();
        let mut reached: BTreeMap<G, u64> = BTreeMap::new();
        let mut unattributed = 0;
        let mut by_window = [0u64; WINDOWS];
        for release in &run.lineage.releases {
            if clocks.contains(&release.edge) {
                continue;
            }
            for record in &release.released {
                let t = time_at[&(release.edge.as_str(), release.member, record.arrived)];
                if let Some(reached_by) = &self.reached {
                    let goals = reached_by(&release.edge, record);
                    if !goals.is_empty() {
                        for g in goals {
                            *reached.entry(g).or_default() += 1;
                        }
                        continue;
                    }
                }
                let goals = (self.goals)(&release.edge, record);
                if goals.is_empty() {
                    unattributed += 1;
                    continue;
                }
                let on_edge = per_edge_per_goal.entry(release.edge.clone()).or_default();
                for g in goals {
                    *per_goal.entry(g.clone()).or_default() += 1;
                    *on_edge.entry(g).or_default() += 1;
                    by_window[window(t)] += 1;
                }
            }
        }
        Measure {
            per_goal,
            per_edge_per_goal,
            unattributed,
            reached,
            by_window,
            records: run.counts.0.iter().map(|(k, e)| (k.clone(), e.records)).collect(),
            progress: run.progress.clone(),
            clock_ticks: clock,
        }
    }
}

/// Compares a cell's measurements with prompt's; `shaped` is whether the program has a clock.
fn judge<G: Ord + Clone + Debug>(d: u64, prompt: &Measure<G>, measure: Measure<G>, shaped: bool) -> Cell<G> {
    let mut excess_by_window = [0i64; WINDOWS];
    for (w, excess) in excess_by_window.iter_mut().enumerate() {
        *excess = measure.by_window[w] as i64 - prompt.by_window[w] as i64;
    }
    let mut more = false;
    let mut max_ratio = 0.0f64;
    for (goal, n) in &measure.per_goal {
        let base = prompt.per_goal.get(goal).copied().unwrap_or(0);
        if *n > base {
            more = true;
        }
        if base > 0 {
            max_ratio = max_ratio.max(*n as f64 / base as f64);
        }
    }
    let progress_changes: Vec<(String, i64, i64)> = measure
        .progress
        .iter()
        .filter_map(|(k, v)| {
            let base = prompt.progress.get(k).copied().unwrap_or(0);
            (*v != base).then(|| (k.clone(), base, *v))
        })
        .collect();
    let excess: i64 = excess_by_window.iter().sum();
    let late: i64 = excess_by_window[WINDOWS / 2..].iter().sum();
    let gain = if more && excess > 0 {
        if !shaped {
            Gain::Extra
        } else if late * 4 >= excess {
            Gain::Sustained
        } else {
            Gain::Transient
        }
    } else if more || !progress_changes.is_empty() {
        // `more` without positive total excess: some goal gained while others lost (the work
        // moved between goals); reported like a progress change.
        Gain::ProgressOnly
    } else {
        Gain::None
    };
    Cell {
        d,
        measure,
        excess_by_window,
        max_ratio,
        progress_changes,
        gain,
    }
}

impl<G: Ord + Clone + Debug> Program<G> {
    /// Runs the sweep.
    pub fn sweep(self, options: &Options) -> Report<G> {
        let prompt_run = run_with(&self, &[], &[], options.empty_ticks);
        let mut runs = 1;
        // Clock keys: every counted edge a clock predicate matches.
        let clocks: BTreeSet<String> = prompt_run
            .counts
            .0
            .keys()
            .filter(|key| self.clocks.iter().any(|(pred, _)| pred(key)))
            .cloned()
            .collect();
        let prompt = self.measure(&prompt_run, &clocks);
        if options.verbose {
            println!(
                "== sweep: {} ({:?} inputs, {}), prompt: {} derivations toward {} goals {:?}, {} unattributed, reached {:?}, progress {:?}, {} clock ticks",
                self.name,
                self.inputs,
                if options.empty_ticks { "ticks may run empty" } else { "forcing rule" },
                prompt.derivations(),
                prompt.per_goal.len(),
                prompt.histogram(),
                prompt.unattributed,
                histogram(prompt.reached.values().copied()),
                prompt.progress,
                prompt.clock_ticks
            );
            for (key, n) in &prompt.records {
                let tag = if clocks.contains(key) { " (clock)" } else { "" };
                println!("   [{key}] records={n}{tag}");
            }
        }

        // Candidate edges: every counted edge that is not a clock and carried a record.
        let logged: BTreeSet<&str> = prompt_run.lineage.edges().into_iter().collect();
        let mut idle_edges = vec![];
        let mut sweeps = vec![];
        for (key, count) in &prompt_run.counts.0 {
            if clocks.contains(key) {
                continue;
            }
            if count.records == 0 {
                idle_edges.push(key.clone());
                continue;
            }
            let keyed = !logged.contains(key.as_str());
            for shape in [Shape::Constant, Shape::Bursty] {
                if keyed && shape == Shape::Constant {
                    sweeps.push(EdgeSweep {
                        edge: key.clone(),
                        shape,
                        cells: vec![],
                        threshold: None,
                        sustained_from: None,
                        progress_from: None,
                        verdict: Gain::None,
                        note: Some("keyed batch: not in the lineage log, a constant hold is not expressible item by item; bursty only".to_owned()),
                    });
                    continue;
                }
                let mut cells: Vec<Cell<G>> = vec![];
                let mut measure_d = |d: u64, cells: &mut Vec<Cell<G>>| {
                    let run = run_with(&self, &[(key.clone(), shape.policy(d))], &[], options.empty_ticks);
                    runs += 1;
                    let cell = judge(d, &prompt, self.measure(&run, &clocks), self.time.is_some());
                    if options.verbose {
                        print_cell(&self.name, key, shape, &prompt, &cell);
                    }
                    cells.push(cell);
                };
                for &d in &options.ds {
                    measure_d(d, &mut cells);
                }
                cells.sort_by_key(|c| c.d);
                let bisect = |cells: &mut Vec<Cell<G>>, measure_d: &mut dyn FnMut(u64, &mut Vec<Cell<G>>), pred: &dyn Fn(&Cell<G>) -> bool| {
                    // Bisect between the last d failing `pred` and the first satisfying it, so
                    // the transition is located exactly (assuming it is monotone in d).
                    while let Some(i) = cells.iter().position(pred) {
                        if i == 0 {
                            break;
                        }
                        let (lo, hi) = (cells[i - 1].d, cells[i].d);
                        if hi - lo <= 1 {
                            break;
                        }
                        measure_d(lo + (hi - lo) / 2, cells);
                        cells.sort_by_key(|c| c.d);
                    }
                };
                if options.refine {
                    bisect(&mut cells, &mut measure_d, &|c| c.found());
                    bisect(&mut cells, &mut measure_d, &|c| c.gain == Gain::Sustained);
                    bisect(&mut cells, &mut measure_d, &|c| !c.progress_changes.is_empty());
                }
                let threshold = cells.iter().find(|c| c.found()).map(|c| c.d);
                let sustained_from = cells.iter().find(|c| c.gain == Gain::Sustained).map(|c| c.d);
                let progress_from = cells.iter().find(|c| !c.progress_changes.is_empty()).map(|c| c.d);
                let verdict = cells.last().map_or(Gain::None, |c| c.gain);
                sweeps.push(EdgeSweep {
                    edge: key.clone(),
                    shape,
                    cells,
                    threshold,
                    sustained_from,
                    progress_from,
                    verdict,
                    note: keyed.then(|| "keyed batch: all keys or none per release".to_owned()),
                });
            }
        }
        let report = Report {
            program: self.name.clone(),
            inputs: self.inputs,
            prompt,
            clocks: clocks.into_iter().collect(),
            sweeps,
            idle_edges,
            runs,
            empty_ticks: options.empty_ticks,
        };
        if options.verbose {
            report.print();
        }
        report
    }
}

fn histogram(values: impl Iterator<Item = u64>) -> BTreeMap<u64, usize> {
    let mut h = BTreeMap::new();
    for v in values {
        *h.entry(v).or_default() += 1;
    }
    h
}

fn print_cell<G: Ord + Clone + Debug>(program: &str, edge: &str, shape: Shape, prompt: &Measure<G>, cell: &Cell<G>) {
    println!(
        "   {program} [{edge}] {shape:?} d = {:>3}: derivations {} (prompt {}), per goal {:?}, max ratio {:.2}, excess by window {:?}, unattributed {} (prompt {}), reached {:?}, progress {:?} -> {:?}",
        cell.d,
        cell.measure.derivations(),
        prompt.derivations(),
        cell.measure.histogram(),
        cell.max_ratio,
        cell.excess_by_window,
        cell.measure.unattributed,
        prompt.unattributed,
        histogram(cell.measure.reached.values().copied()),
        cell.progress_changes,
        cell.gain
    );
}

impl<G: Ord + Clone + Debug> Report<G> {
    /// The sweeps that found something, in order of threshold.
    pub fn findings(&self) -> Vec<&EdgeSweep<G>> {
        let mut found: Vec<&EdgeSweep<G>> = self.sweeps.iter().filter(|s| s.threshold.is_some()).collect();
        found.sort_by_key(|s| s.threshold);
        found
    }

    /// Whether the unique edge matching `pred` was idle under prompt (no records, so not swept).
    pub fn is_idle(&self, pred: impl Fn(&str) -> bool) -> bool {
        self.idle_edges.iter().any(|e| pred(e))
    }

    /// The sweep of `edge` (matched by `pred`, unique) under `shape`.
    pub fn sweep_of(&self, pred: impl Fn(&str) -> bool, shape: Shape) -> &EdgeSweep<G> {
        let mut matching = self.sweeps.iter().filter(|s| pred(&s.edge) && s.shape == shape);
        let found = matching.next().expect("no swept edge matches the predicate");
        assert!(matching.next().is_none(), "more than one swept edge matches the predicate");
        found
    }

    /// Prints the summary table.
    pub fn print(&self) {
        println!(
            "== sweep summary: {} ({:?} inputs, {}), {} runs; prompt {} derivations toward {} goals, {} unattributed; clocks {:?}; idle edges {:?}",
            self.program,
            self.inputs,
            if self.empty_ticks { "ticks may run empty" } else { "forcing rule" },
            self.runs,
            self.prompt.derivations(),
            self.prompt.per_goal.len(),
            self.prompt.unattributed,
            self.clocks,
            self.idle_edges
        );
        println!("   edge / shape: first gain, knee (sustained from), first progress change, verdict at the largest d; then d: derivations (max per-goal ratio) [excess by window] progress");
        for s in &self.sweeps {
            let cells: Vec<String> = s
                .cells
                .iter()
                .map(|c| {
                    let progress = if c.progress_changes.is_empty() {
                        String::new()
                    } else {
                        format!(
                            " {}",
                            c.progress_changes
                                .iter()
                                .map(|(k, a, b)| format!("{k} {a}->{b}"))
                                .collect::<Vec<_>>()
                                .join(", ")
                        )
                    };
                    format!(
                        "{}: {} ({:.2}) {:?}{progress}",
                        c.d,
                        c.measure.derivations(),
                        c.max_ratio,
                        c.excess_by_window
                    )
                })
                .collect();
            println!(
                "   [{}] {:?}: threshold {:?}, sustained from {:?}, progress changes from {:?}, {:?}{}",
                s.edge,
                s.shape,
                s.threshold,
                s.sustained_from,
                s.progress_from,
                s.verdict,
                s.note.as_ref().map(|n| format!(" ({n})")).unwrap_or_default()
            );
            for c in cells {
                println!("      d = {c}");
            }
        }
        println!(
            "   incompleteness: the moves cover one of the design's five hook dimensions (release timing); release size, order, crash and membership moves do not exist yet."
        );
    }
}
