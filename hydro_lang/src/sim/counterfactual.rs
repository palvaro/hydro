//! Per-decision counterfactual forks: the "one untested path" of
//! `design_docs/2026-09_perffuzz_spike.md`, and the executable form of E3.3's negative leaves.
//!
//! A run under a `Hold(d)` policy holds every record on the edge `d` decisions. A **fork** replays
//! that run with one edit: at one decision of one hook, the records that arrived there are
//! released at once ([`super::hold_schedule::Fork`]); everything else — the clocks, the policy on
//! every other decision, the inputs — is the same. Both branches run the same clock, so whatever
//! differs between them is the consequence of that one delivery not being held. The diff is taken
//! three ways, and the point of the experiment is to compare them:
//!
//! - **per edge** (records released, from the counter): no program knowledge;
//! - **per payload shape** (records per edge per `Debug` rendering with digits and string
//!   contents removed, so an election message and a heartbeat on one edge are told apart): no
//!   program knowledge, a heuristic;
//! - **per goal** (the harness's goal extractor, as in the sweep): one declared line of program
//!   knowledge.
//!
//! For each fork the report gives the records the held run has and the fork does not (the
//! positive part of held − fork: the work the hold caused) and the reverse (the work the hold
//! displaced), per edge, per shape and per goal, and sums them over the forks tried. It also
//! lists the **records themselves** ([`ForkOutcome::caused_records`]): every record, with its
//! contents, that crossed a hook in the held run and has no counterpart (same edge, same member,
//! same payload) in the fork. That list is the provenance the lineage rule could not compute
//! inside a closure: for one held delivery, the concrete records that exist because it was held.
//! Boundary records only; the lineage pass's derivation ids are allocated in execution order and
//! do not match across runs. A totally
//! ordered hook can only release a prefix, so on such an edge a fork releases the held record and
//! everything older with it; the report says which kind each fork was.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;

use super::hold_schedule::{EdgePolicy, Fork};
use super::lineage::Record;
use super::sweep::{Program, Run, run_with};

/// Which held deliveries to fork.
#[derive(Debug, Clone)]
pub enum Sample {
    /// The first `n` arrival groups (an arrival group is the records one hook of one member first
    /// saw at one decision), in arrival order.
    First(usize),
    /// Every `k`-th arrival group, up to `n` of them.
    Stride {
        /// The stride.
        k: usize,
        /// The most groups to fork.
        n: usize,
    },
}

/// A count under the held run and under the fork.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Pair {
    /// Under the held run.
    pub held: u64,
    /// Under the fork.
    pub fork: u64,
}

impl Pair {
    /// Records the hold caused: what the held run has and the fork does not.
    pub fn caused(&self) -> u64 {
        self.held.saturating_sub(self.fork)
    }
    /// Records the hold displaced: what the fork has and the held run does not.
    pub fn displaced(&self) -> u64 {
        self.fork.saturating_sub(self.held)
    }
}

/// The counts one branch is reduced to.
#[derive(Debug, Clone, Default)]
pub struct Counts<G: Ord> {
    /// Records per edge (clocks excluded).
    pub per_edge: BTreeMap<String, u64>,
    /// Records per (edge, payload shape).
    pub per_shape: BTreeMap<(String, String), u64>,
    /// Derivations toward each goal.
    pub per_goal: BTreeMap<G, u64>,
    /// The harness's progress signals.
    pub progress: BTreeMap<String, i64>,
    /// Decisions (ticks run) in total, clocks included: the framework cost the spike found
    /// confounding whole-run diffs.
    pub decisions: u64,
}

/// One fork and its diff.
#[derive(Debug, Clone)]
pub struct ForkOutcome<G: Ord> {
    /// The edit.
    pub fork: Fork,
    /// How many records the forked group had.
    pub group_size: usize,
    /// The payload shapes of the forked group's records (see [`shape`]).
    pub group_shapes: Vec<String>,
    /// Whether the forked decision released older records with the group (a totally ordered
    /// hook releases a prefix), read off the fork run's log.
    pub prefix: bool,
    /// Per edge.
    pub per_edge: BTreeMap<String, Pair>,
    /// Per (edge, shape).
    pub per_shape: BTreeMap<(String, String), Pair>,
    /// Per goal.
    pub per_goal: BTreeMap<G, Pair>,
    /// Progress signals that differ: `(name, held, fork)`.
    pub progress_changes: Vec<(String, i64, i64)>,
    /// Decisions, held and fork.
    pub decisions: Pair,
    /// The records the hold caused: on a non-clock edge in the held run, with no counterpart in
    /// the fork (matched by edge, member and payload; a record present twice in the held run and
    /// once in the fork appears once here). `(edge, member, record)`.
    pub caused_records: Vec<(String, Option<u32>, Record)>,
    /// The records the hold displaced: in the fork, with no counterpart in the held run.
    pub displaced_records: Vec<(String, Option<u32>, Record)>,
}

impl<G: Ord> ForkOutcome<G> {
    /// Records the hold caused, summed over edges.
    pub fn caused(&self) -> u64 {
        self.per_edge.values().map(Pair::caused).sum()
    }
    /// Records the hold displaced, summed over edges.
    pub fn displaced(&self) -> u64 {
        self.per_edge.values().map(Pair::displaced).sum()
    }
    /// Derivations toward goals the hold caused, summed over goals.
    pub fn caused_toward_goals(&self) -> u64 {
        self.per_goal.values().map(Pair::caused).sum()
    }
}

/// The report of one experiment: a held run and its forks.
#[derive(Debug, Clone)]
pub struct ForkReport<G: Ord> {
    /// The program's name.
    pub program: String,
    /// The held edge and its policy.
    pub held: (String, EdgePolicy),
    /// The held run's counts.
    pub baseline: Counts<G>,
    /// Every fork, in arrival order.
    pub forks: Vec<ForkOutcome<G>>,
    /// Arrival groups on the held edge in the held run (the population the sample came from).
    pub groups: usize,
}

/// The records of a run on non-clock edges, as a multiset keyed by (edge, member, payload — or the
/// `Debug` rendering when the type is not `Serialize`), with one representative record per key.
fn record_multiset(run: &Run, clocks: &BTreeSet<String>) -> BTreeMap<(String, Option<u32>, Vec<u8>), (usize, Record)> {
    let mut out: BTreeMap<(String, Option<u32>, Vec<u8>), (usize, Record)> = BTreeMap::new();
    for release in &run.lineage.releases {
        if clocks.contains(&release.edge) {
            continue;
        }
        for record in &release.released {
            let contents = record
                .payload
                .clone()
                .or_else(|| record.debug.as_ref().map(|d| d.as_bytes().to_vec()))
                .unwrap_or_default();
            out.entry((release.edge.clone(), release.member, contents))
                .or_insert_with(|| (0, record.clone()))
                .0 += 1;
        }
    }
    out
}

/// Records in `a` with no counterpart in `b`, one entry per excess copy.
fn multiset_minus(
    a: &BTreeMap<(String, Option<u32>, Vec<u8>), (usize, Record)>,
    b: &BTreeMap<(String, Option<u32>, Vec<u8>), (usize, Record)>,
) -> Vec<(String, Option<u32>, Record)> {
    let mut out = vec![];
    for (key, (n, record)) in a {
        let m = b.get(key).map_or(0, |(m, _)| *m);
        for _ in m..*n {
            out.push((key.0.clone(), key.1, record.clone()));
        }
    }
    out
}

/// The payload shape of a record: its `Debug` rendering with digits and the contents of string
/// literals removed and whitespace collapsed, cut to 72 characters; `"?"` without a rendering.
pub fn shape(record: &Record) -> String {
    let Some(debug) = &record.debug else {
        return "?".to_owned();
    };
    let mut out = String::new();
    let mut in_string = false;
    let mut last_space = false;
    for c in debug.chars() {
        if in_string {
            if c == '"' {
                in_string = false;
                out.push('"');
            }
            continue;
        }
        match c {
            '"' => {
                in_string = true;
                out.push('"');
            }
            '0'..='9' => {}
            c if c.is_whitespace() => {
                if !last_space {
                    out.push(' ');
                }
            }
            c => out.push(c),
        }
        last_space = c.is_whitespace();
        if out.chars().count() >= 72 {
            break;
        }
    }
    out
}

impl<G: Ord + Clone + Debug> Program<G> {
    fn counts(&self, run: &Run, clocks: &BTreeSet<String>) -> Counts<G> {
        let mut counts = Counts {
            per_edge: BTreeMap::new(),
            per_shape: BTreeMap::new(),
            per_goal: BTreeMap::new(),
            progress: run.progress.clone(),
            decisions: run.counts.0.values().map(|e| e.decisions).sum(),
        };
        for (key, e) in &run.counts.0 {
            if !clocks.contains(key) {
                counts.per_edge.insert(key.clone(), e.records);
            }
        }
        for release in &run.lineage.releases {
            if clocks.contains(&release.edge) {
                continue;
            }
            for record in &release.released {
                *counts.per_shape.entry((release.edge.clone(), shape(record))).or_default() += 1;
                for g in (self.goals)(&release.edge, record) {
                    *counts.per_goal.entry(g).or_default() += 1;
                }
            }
        }
        counts
    }

    /// Runs the program under its clocks with `policy` on the edge matching `held`, then once
    /// more per sampled arrival group on that edge with that group released at its arrival, and
    /// diffs each fork against the held run.
    pub fn forks(&self, held: impl Fn(&str) -> bool, policy: EdgePolicy, sample: Sample, empty_ticks: bool, verbose: bool) -> ForkReport<G> {
        let base = run_with(self, &[], &[], empty_ticks);
        let clocks: BTreeSet<String> = base
            .counts
            .0
            .keys()
            .filter(|key| self.clocks.iter().any(|(pred, _)| pred(key)))
            .cloned()
            .collect();
        let edges: Vec<String> = base.counts.0.keys().filter(|k| held(k) && !clocks.contains(*k)).cloned().collect();
        assert_eq!(edges.len(), 1, "the held predicate must name one non-clock edge, matched {edges:?}");
        let edge = edges.into_iter().next().unwrap();
        let moves = [(edge.clone(), policy)];

        let held_run = run_with(self, &moves, &[], empty_ticks);
        let baseline = self.counts(&held_run, &clocks);
        let held_records = record_multiset(&held_run, &clocks);
        // Arrival groups on the held edge: (arrival decision, member) -> records, in arrival order.
        let mut groups: BTreeMap<(u64, Option<u32>), Vec<String>> = BTreeMap::new();
        for release in held_run.lineage.releases_on(|k| k == edge) {
            for record in &release.released {
                groups.entry((record.arrived, release.member)).or_default().push(shape(record));
            }
        }
        let chosen: Vec<((u64, Option<u32>), Vec<String>)> = match sample {
            Sample::First(n) => groups.iter().take(n).map(|(k, v)| (*k, v.clone())).collect(),
            Sample::Stride { k, n } => groups.iter().step_by(k.max(1)).take(n).map(|(k, v)| (*k, v.clone())).collect(),
        };
        if verbose {
            println!(
                "== forks: {} holding [{edge}] under {policy:?} ({} arrival groups, {} forked): held run {} records on non-clock edges, {} derivations toward {} goals, progress {:?}, {} decisions",
                self.name,
                groups.len(),
                chosen.len(),
                baseline.per_edge.values().sum::<u64>(),
                baseline.per_goal.values().sum::<u64>(),
                baseline.per_goal.len(),
                baseline.progress,
                baseline.decisions
            );
        }

        let mut forks = vec![];
        for ((arrived, member), group_shapes) in chosen {
            let group_size = group_shapes.len();
            let fork = Fork { edge: edge.clone(), member, decision: arrived, arrived };
            let run = run_with(self, &moves, std::slice::from_ref(&fork), empty_ticks);
            let counts = self.counts(&run, &clocks);
            // Prefix or exact: whether the forked decision released, besides the group and
            // whatever the policy had due anyway, older records the policy would still have held.
            let due = |rec_arrived: u64| match policy {
                EdgePolicy::Hold(d) => rec_arrived + d <= arrived,
                _ => true,
            };
            let prefix = run
                .lineage
                .releases_on(|k| k == edge)
                .find(|r| r.member == member && r.tick == arrived)
                .is_some_and(|r| r.released.iter().any(|rec| rec.arrived < arrived && !due(rec.arrived)));
            let fork_records = record_multiset(&run, &clocks);
            let outcome = ForkOutcome {
                caused_records: multiset_minus(&held_records, &fork_records),
                displaced_records: multiset_minus(&fork_records, &held_records),
                fork,
                group_size,
                group_shapes,
                prefix,
                per_edge: pair_up(&baseline.per_edge, &counts.per_edge),
                per_shape: pair_up(&baseline.per_shape, &counts.per_shape),
                per_goal: pair_up(&baseline.per_goal, &counts.per_goal),
                progress_changes: counts
                    .progress
                    .iter()
                    .filter_map(|(k, v)| {
                        let base = baseline.progress.get(k).copied().unwrap_or(0);
                        (*v != base).then(|| (k.clone(), base, *v))
                    })
                    .collect(),
                decisions: Pair { held: baseline.decisions, fork: counts.decisions },
            };
            if verbose {
                print_fork(&outcome);
            }
            forks.push(outcome);
        }
        let report = ForkReport {
            program: self.name.clone(),
            held: (edge, policy),
            baseline,
            forks,
            groups: groups.len(),
        };
        if verbose {
            report.print();
        }
        report
    }
}

fn pair_up<K: Ord + Clone>(held: &BTreeMap<K, u64>, fork: &BTreeMap<K, u64>) -> BTreeMap<K, Pair> {
    let mut out: BTreeMap<K, Pair> = BTreeMap::new();
    for (k, v) in held {
        out.entry(k.clone()).or_default().held = *v;
    }
    for (k, v) in fork {
        out.entry(k.clone()).or_default().fork = *v;
    }
    out
}

fn short_edge(key: &str) -> String {
    let (loc, ty) = key.split_once(' ').unwrap_or((key, ""));
    let loc = loc.rsplit('/').next().unwrap_or(loc);
    let ty = ty.rsplit("::").next().unwrap_or(ty);
    format!("{loc} {ty}")
}

fn print_fork<G: Ord + Debug>(o: &ForkOutcome<G>) {
    let edges: Vec<String> = o
        .per_edge
        .iter()
        .filter(|(_, p)| p.held != p.fork)
        .map(|(k, p)| format!("{}: {}→{}", short_edge(k), p.held, p.fork))
        .collect();
    let shapes: Vec<String> = o
        .per_shape
        .iter()
        .filter(|(_, p)| p.held != p.fork)
        .map(|((k, s), p)| format!("{} {s}: {}→{}", short_edge(k), p.held, p.fork))
        .collect();
    let goals: Vec<String> = o
        .per_goal
        .iter()
        .filter(|(_, p)| p.held != p.fork)
        .map(|(g, p)| format!("{g:?}: {}→{}", p.held, p.fork))
        .collect();
    println!(
        "   fork member {:?} decision {} ({} record(s){}: {}): caused {} displaced {} | goals caused {} | decisions {}→{} | progress {:?}",
        o.fork.member,
        o.fork.decision,
        o.group_size,
        if o.prefix { ", prefix" } else { "" },
        o.group_shapes.iter().map(|s| s.chars().take(40).collect::<String>()).collect::<Vec<_>>().join(" | "),
        o.caused(),
        o.displaced(),
        o.caused_toward_goals(),
        o.decisions.held,
        o.decisions.fork,
        o.progress_changes
    );
    if !edges.is_empty() {
        println!("      per edge:  {}", edges.join("; "));
    }
    if !shapes.is_empty() {
        println!("      per shape: {}", shapes.join("; "));
    }
    if !goals.is_empty() {
        println!("      per goal:  {}", goals.join("; "));
    }
    let list = |records: &[(String, Option<u32>, Record)]| -> String {
        let mut shown: Vec<String> = records
            .iter()
            .take(8)
            .map(|(edge, member, r)| format!("[{} @{}] {}", short_edge(edge), member.map_or("-".to_owned(), |m| m.to_string()), r.debug.as_deref().unwrap_or("?").chars().take(90).collect::<String>()))
            .collect();
        if records.len() > 8 {
            shown.push(format!("… {} more", records.len() - 8));
        }
        shown.join("\n         ")
    };
    if !o.caused_records.is_empty() {
        println!("      caused records ({}):\n         {}", o.caused_records.len(), list(&o.caused_records));
    }
    if !o.displaced_records.is_empty() {
        println!("      displaced records ({}):\n         {}", o.displaced_records.len(), list(&o.displaced_records));
    }
}

impl<G: Ord + Clone + Debug> ForkReport<G> {
    /// Sum over forks of the records the hold caused, per edge.
    pub fn caused_per_edge(&self) -> BTreeMap<String, u64> {
        let mut out = BTreeMap::new();
        for f in &self.forks {
            for (k, p) in &f.per_edge {
                *out.entry(k.clone()).or_default() += p.caused();
            }
        }
        out
    }
    /// Sum over forks of the records the hold caused, per (edge, shape).
    pub fn caused_per_shape(&self) -> BTreeMap<(String, String), u64> {
        let mut out = BTreeMap::new();
        for f in &self.forks {
            for (k, p) in &f.per_shape {
                *out.entry(k.clone()).or_default() += p.caused();
            }
        }
        out
    }
    /// Sum over forks of the derivations toward goals the hold caused.
    pub fn caused_per_goal(&self) -> BTreeMap<G, u64> {
        let mut out = BTreeMap::new();
        for f in &self.forks {
            for (g, p) in &f.per_goal {
                *out.entry(g.clone()).or_default() += p.caused();
            }
        }
        out
    }
    /// Total caused, total displaced, total caused toward goals, over the forks.
    pub fn totals(&self) -> (u64, u64, u64) {
        (
            self.forks.iter().map(|f| f.caused()).sum(),
            self.forks.iter().map(|f| f.displaced()).sum(),
            self.forks.iter().map(|f| f.caused_toward_goals()).sum(),
        )
    }

    /// Prints the summary.
    pub fn print(&self) {
        let (caused, displaced, toward) = self.totals();
        println!(
            "== forks summary: {} holding [{}] under {:?}: {} forks of {} groups; caused {} records, displaced {}, caused toward goals {}; forks with any change {}",
            self.program,
            short_edge(&self.held.0),
            self.held.1,
            self.forks.len(),
            self.groups,
            caused,
            displaced,
            toward,
            self.forks.iter().filter(|f| f.caused() + f.displaced() > 0 || !f.progress_changes.is_empty()).count()
        );
        for (k, v) in self.caused_per_edge() {
            if v > 0 {
                println!("   caused per edge:  {} {v}", short_edge(&k));
            }
        }
        for ((k, s), v) in self.caused_per_shape() {
            if v > 0 {
                println!("   caused per shape: {} {s} {v}", short_edge(&k));
            }
        }
        let goals = self.caused_per_goal();
        if !goals.is_empty() {
            let mut h: BTreeMap<u64, usize> = BTreeMap::new();
            for v in goals.values() {
                *h.entry(*v).or_default() += 1;
            }
            println!("   caused per goal (histogram count -> goals): {h:?}");
        }
    }
}
