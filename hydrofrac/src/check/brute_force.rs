//! The brute-force cross-check of the search (docs/design/hydrofrac-lite.md §7, "Honesty
//! check"): every admissible fault set up to a size, run without any lineage pruning, against
//! what [`EcCheck::run`] concluded.
//!
//! The enumerator learns its universe from the runs themselves. The failure-free probe names
//! the first messages; every set it runs may reveal more (a crash can change what the others
//! send), and those join the universe, until a pass over every set up to the size reveals
//! nothing new. The candidates are those of the fault model:
//! - `omit(m)` for every message `m` sent before the heal on a lossy channel;
//! - `crash(n)` for every crashable node, and `crash(n after m)` for every message `m` it sent
//!   before the heal.
//!
//! A set is admissible for a label under the same bounds the search uses: at most one crash per
//! node, never every node, never all or all but one of the checked cluster, and the
//! [`EcCheck::max_crashes`] cap. Each set runs over the same schedules, with the same two
//! phases (inputs under faults, heal, recovery) and quiesces, as the search's, and the runs are read by the same evaluation of the
//! specification. Everything else is the oracle's own: its execution loop ([`Engine::execute`]
//! here) reads the outputs straight from the event log, with no lineage, no provenance and no
//! pruning. So agreement tests the lineage, the literals and the hitting-set program.
//!
//! What agreement means, for the fault sets the enumeration covers:
//! - a certificate for `k` crashes ([`Tolerance::UpTo`]) means no set with at most `k` crashes
//!   breaks the label, and [`Tolerance::All`] that no admissible set does;
//! - a counterexample after tolerating `t` crashes means no set with at most `t` crashes breaks
//!   the label, the counterexample has exactly `t + 1` crashes (none when `t` is `None`), and
//!   rerunning its fault set breaks the label again (under exhaustive exploration).
//!
//! So whenever the enumeration finds a breaking set with `c` crashes, the search must report a
//! counterexample with at most `c` crashes (it tolerates fewer than `c`); and when the
//! enumeration finds none, the search must not report one within the enumerated sizes.
//!
//! A sampled search is never certified ([`Verdict::Exhausted`]); the same comparison applies
//! to it, as a check of what its sample found.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Display};
use std::panic::AssertUnwindSafe;
use std::rc::Rc;
use std::time::{Duration, Instant};

use hydro_lang::sim::compiled::CompiledSim;

use super::oracle2::{self, Oracle2};
use super::{
    EcCheck, Engine, Failure, Target, correct_members, drive, program_panic, record_domain,
};
use crate::check::results::{LabelResult, Results, Verdict, label_name};
use crate::fault::{Event, Fate, FaultHook, FaultPlan, InstanceRecord, MessageId, NodeId};
use crate::lineage::Tables;
use crate::observe::{Fact, Labels, decode_output};
use crate::report::ReportNames;
use crate::search::{CrashBounds, Fault, FaultSet, Tolerance, crash_count};
use crate::spec::{MemberView, Observation, evaluate};

/// The outcome of [`EcCheck::cross_check`]: the search's results, the enumeration's, and every
/// way they disagree.
#[doc(hidden)]
pub struct CrossCheck {
    /// What [`EcCheck::run`] concluded.
    pub results: Results,
    /// What the enumeration found.
    pub brute: BruteForce,
    /// What the second oracle found (`oracle2.rs`).
    pub oracle2: Oracle2,
    /// Every disagreement, in words (oracle 2's prefixed `oracle 2:`). Empty when they agree.
    pub disagreements: Vec<String>,
    /// For each counterexample whose fault set was rerun, whether the rerun broke its label
    /// again (by label index).
    pub confirmed: BTreeMap<usize, bool>,
}

impl CrossCheck {
    /// Recomputes [`Self::disagreements`] from [`Self::results`] and [`Self::brute`] (after a
    /// test edits a verdict, to check that the comparison catches it).
    pub fn recheck(&mut self) {
        self.disagreements = disagreements(&self.results, &self.brute, &self.confirmed);
        self.disagreements
            .extend(oracle2::disagreements(&self.results, &self.oracle2));
    }

    /// Panics with both reports if the search and the enumeration disagree.
    pub fn assert_agrees(&self) {
        assert!(
            self.disagreements.is_empty(),
            "hydrofrac: the search and its oracles disagree:\n- {}\n\nsearch:\n{}\nenumeration:\n{}\n{}",
            self.disagreements.join("\n- "),
            self.results,
            self.brute.rendered(Some(&self.results.report_names())),
            self.oracle2,
        );
    }
}

impl Display for CrossCheck {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "{}",
            self.brute.rendered(Some(&self.results.report_names()))
        )?;
        writeln!(f, "{}", self.oracle2)?;
        if self.disagreements.is_empty() {
            writeln!(f, "the search agrees with both oracles on every label")
        } else {
            for d in &self.disagreements {
                writeln!(f, "DISAGREEMENT: {d}")?;
            }
            Ok(())
        }
    }
}

/// What the enumeration found, per searched label.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct BruteForce {
    /// The largest set size enumerated.
    pub max_faults: usize,
    /// One entry per label the search searched (labels decided without a search, such as
    /// vacuous or single-member ones, have none).
    pub labels: Vec<BruteLabel>,
    /// The size of the final universe of candidate faults.
    pub universe: usize,
    /// The fault sets run, counting the failure-free probe.
    pub sets: usize,
    /// The schedules explored over those sets.
    pub schedules: usize,
    /// How many passes the universe took to reach its fixpoint.
    pub passes: usize,
    /// Wall time of the enumeration.
    pub wall: Duration,
    /// Every set run, counting the failure-free probe.
    pub enumerated: BTreeSet<FaultSet<MessageId>>,
}

impl BruteForce {
    /// The entry for the label with this index.
    pub fn label(&self, index: usize) -> Option<&BruteLabel> {
        self.labels.iter().find(|l| l.index == index)
    }
}

/// One label's enumeration.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct BruteLabel {
    /// The label's index ([`crate::observe::Labels::plans`]).
    pub index: usize,
    /// Its name, as the report prints it.
    pub name: String,
    /// The admissible sets run for it, counting the failure-free probe.
    pub admissible: usize,
    /// Every admissible set that breaks it, fewest crashes first, then fewest faults. The
    /// empty set if the failure-free run already breaks it.
    pub breaking: Vec<FaultSet<MessageId>>,
}

impl BruteLabel {
    /// The fewest crashes of any breaking set (`Some(0)` if omissions alone break the label).
    pub fn min_crashes(&self) -> Option<usize> {
        self.breaking.first().map(crash_count)
    }

    /// A breaking set with at most `crashes` crashes, if any.
    pub fn breaking_within(&self, crashes: usize) -> Option<&FaultSet<MessageId>> {
        self.breaking.iter().find(|s| crash_count(s) <= crashes)
    }
}

/// `{f1, f2}` in the report's names (source names for channels and nodes, decoded payloads),
/// or in the faults' own rendering when no names are at hand.
fn show(names: Option<&ReportNames<'_>>, set: &FaultSet<MessageId>) -> String {
    if let Some(names) = names {
        names.fault_set(set)
    } else {
        let faults: Vec<String> = set.iter().map(|f| f.to_string()).collect();
        format!("{{{}}}", faults.join(", "))
    }
}

impl Display for BruteForce {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.render(f, None)
    }
}

impl BruteForce {
    /// The enumeration's report, naming faults the way `names` does (raw without it).
    pub fn rendered(&self, names: Option<&ReportNames<'_>>) -> String {
        struct Rendered<'a, 'n>(&'a BruteForce, Option<&'a ReportNames<'n>>);
        impl Display for Rendered<'_, '_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.render(f, self.1)
            }
        }
        Rendered(self, names).to_string()
    }

    fn render(&self, f: &mut fmt::Formatter<'_>, names: Option<&ReportNames<'_>>) -> fmt::Result {
        writeln!(
            f,
            "brute force up to {} fault(s): {} candidate faults, {} sets, {} schedules, {} pass(es), {:.2?}",
            self.max_faults, self.universe, self.sets, self.schedules, self.passes, self.wall
        )?;
        for l in &self.labels {
            write!(f, "  {}: ", l.name)?;
            match l.breaking.first() {
                None => write!(f, "no admissible set breaks it")?,
                Some(set) if set.is_empty() => write!(f, "the failure-free run breaks it")?,
                Some(set) => match crash_count(set) {
                    0 => write!(f, "omissions alone break it: {}", show(names, set))?,
                    c => write!(f, "breaks with {c} crash(es): {}", show(names, set))?,
                },
            }
            writeln!(
                f,
                " ({} breaking of {} admissible sets)",
                l.breaking.len(),
                l.admissible
            )?;
        }
        Ok(())
    }
}

impl EcCheck {
    /// Runs [`Self::run`], then the brute-force enumeration of every admissible fault set of at
    /// most `max_faults` faults (see the [module docs](self)), and compares them. Test-only:
    /// the enumeration is exponential in `max_faults`.
    ///
    /// # Panics
    /// As [`Self::run`].
    #[doc(hidden)]
    pub fn cross_check(
        &self,
        sim: &CompiledSim,
        inputs: impl AsyncFnMut(),
        max_faults: usize,
    ) -> CrossCheck {
        self.cross_check_then_recover(sim, inputs, async || {}, max_faults)
    }

    /// [`Self::cross_check`] for [`Self::run_then_recover`]: both sides run the same two
    /// phases.
    ///
    /// # Panics
    /// As [`Self::run_then_recover`].
    #[doc(hidden)]
    pub fn cross_check_then_recover(
        &self,
        sim: &CompiledSim,
        mut inputs: impl AsyncFnMut(),
        mut recover: impl AsyncFnMut(),
        max_faults: usize,
    ) -> CrossCheck {
        let results = self.run_then_recover(sim, &mut inputs, &mut recover);
        let labels = self
            .taps
            .as_ref()
            .expect("`EcCheck::cross_check` needs a simulation compiled from `EcCheck::instrument`")
            .labels();
        let (brute, confirmed) = Engine::new(self, sim, &labels, &mut inputs, &mut recover)
            .brute_force(max_faults, &results);
        let mut disagreements = disagreements(&results, &brute, &confirmed);
        // The second oracle runs the same harness on its own program.
        let oracle2 = self.oracle2(&mut inputs, &mut recover, max_faults, &results);
        disagreements.extend(oracle2::disagreements(&results, &oracle2));
        CrossCheck {
            results,
            brute,
            oracle2,
            disagreements,
            confirmed,
        }
    }
}

/// Adds the candidate faults `record` reveals to `universe`.
fn harvest(record: &InstanceRecord, tables: &Tables, universe: &mut BTreeSet<Fault<MessageId>>) {
    let names = tables.names();
    let crashable: BTreeSet<&NodeId> = names.crashable().iter().collect();
    for node in &crashable {
        universe.insert(Fault::crash_after(names.name(node), None));
    }
    for event in record.log.events() {
        match event {
            // Neither omissions nor new crashes fire after the heal.
            Event::Heal => break,
            Event::Sent { msg, fate, .. } if *fate != Fate::SenderDead => {
                if crashable.contains(&msg.from) {
                    universe.insert(Fault::crash_after(names.name(&msg.from), Some(msg.clone())));
                }
                if tables.is_faulty(msg.channel) {
                    universe.insert(Fault::Omit(msg.clone()));
                }
            }
            _ => {}
        }
    }
}

/// Whether `set` is admissible under `bounds`: at most one crash per node, and within the
/// crash cap, the vacuity bound and the checked cluster's cap.
fn admissible(set: &FaultSet<MessageId>, bounds: &CrashBounds) -> bool {
    let crashed: Vec<&String> = set.iter().filter_map(Fault::crashed_node).collect();
    let distinct: BTreeSet<&String> = crashed.iter().copied().collect();
    if distinct.len() != crashed.len() {
        return false;
    }
    if bounds.max_crashes().is_some_and(|max| crashed.len() > max) {
        return false;
    }
    match &bounds.checked {
        Some(checked) => {
            crashed
                .iter()
                .filter(|n| checked.nodes.contains(**n))
                .count()
                <= checked.cap
        }
        None => true,
    }
}

/// Every subset of `universe` of exactly `size` faults, in order.
fn subsets(universe: &[Fault<MessageId>], size: usize) -> Vec<FaultSet<MessageId>> {
    fn go(
        universe: &[Fault<MessageId>],
        start: usize,
        size: usize,
        current: &mut Vec<Fault<MessageId>>,
        out: &mut Vec<FaultSet<MessageId>>,
    ) {
        if current.len() == size {
            out.push(current.iter().cloned().collect());
            return;
        }
        for i in start..universe.len() {
            current.push(universe[i].clone());
            go(universe, i + 1, size, current, out);
            current.pop();
        }
    }
    let mut out = vec![];
    go(universe, 0, size, &mut vec![], &mut out);
    out
}

/// What one run (a fault set over all its schedules) says about one label: its first failure.
#[derive(Default)]
struct LabelRun {
    failure: Option<Rc<Failure>>,
}

/// One fault set, run over its schedules.
struct Run {
    /// By label index.
    labels: BTreeMap<usize, LabelRun>,
    schedules: usize,
    /// The probe only: each target's fact domain, by label index.
    domains: BTreeMap<usize, BTreeSet<Fact>>,
}

/// Whether one schedule's record breaks `target`, against `domain`: the specification's
/// evaluation, over the outputs the log shows (no lineage).
fn failure_of(
    labels: &Labels,
    target: &Target,
    domain: &BTreeSet<Fact>,
    record: &Rc<InstanceRecord>,
) -> Option<Failure> {
    let observations: Vec<Observation<()>> = record
        .log
        .events()
        .iter()
        .filter_map(|e| match e {
            Event::Output {
                port, node, bytes, ..
            } if labels.label_of_port(*port) == Some(target.label) => Some(Observation {
                label: target.label,
                member: node.member.unwrap_or(0),
                fact: decode_output(bytes),
                support: (),
            }),
            _ => None,
        })
        .collect();
    let view = MemberView::of(target.label, &observations);
    let correct = correct_members(target, record);
    let evaluation = evaluate(&view, domain, &correct);
    let divergent: Vec<Fact> = evaluation
        .outside_domain
        .iter()
        .filter(|fact| !correct.iter().all(|m| view.holds(*m, fact)))
        .cloned()
        .collect();
    (!evaluation.holds() || !divergent.is_empty()).then(|| Failure {
        missing: evaluation.missing,
        divergent,
        panic: None,
        record: record.clone(),
    })
}

/// A run being read one schedule at a time.
struct RunFold<'t> {
    targets: &'t [Target],
    /// Whether this is the failure-free probe, which learns each target's domain as it reads.
    probe: bool,
    labels: BTreeMap<usize, LabelRun>,
    domains: BTreeMap<usize, BTreeSet<Fact>>,
    schedules: usize,
    /// The latest record, read when the next one arrives or at [`Self::finish`]: when a
    /// schedule panics, the run's last record is the panicking one.
    pending: Option<InstanceRecord>,
}

impl<'t> RunFold<'t> {
    fn new(targets: &'t [Target], probe: bool) -> Self {
        RunFold {
            targets,
            probe,
            labels: targets
                .iter()
                .map(|t| (t.label, LabelRun::default()))
                .collect(),
            domains: BTreeMap::new(),
            schedules: 0,
            pending: None,
        }
    }

    /// Takes the next schedule's record (reading the one before it).
    fn push(&mut self, labels: &Labels, record: InstanceRecord) {
        if let Some(previous) = self.pending.replace(record) {
            self.read(labels, previous, None);
        }
    }

    /// Reads the last record (the panicking schedule's, if `panic`) and ends the run.
    fn finish(mut self, labels: &Labels, panic: Option<String>) -> Run {
        if let Some(last) = self.pending.take() {
            self.read(labels, last, panic.as_deref());
        }
        for lr in self.labels.values_mut() {
            // The panicking schedule left no record: still a counterexample.
            if let Some(panic) = &panic {
                lr.failure
                    .get_or_insert_with(|| Rc::new(Failure::panic(panic, Rc::default())));
            }
        }
        Run {
            labels: self.labels,
            schedules: self.schedules,
            domains: self.domains,
        }
    }

    /// Reads one schedule's record for every target.
    fn read(&mut self, labels: &Labels, record: InstanceRecord, panicked: Option<&str>) {
        self.schedules += 1;
        let record = Rc::new(record);
        for t in self.targets {
            let lr = self.labels.get_mut(&t.label).unwrap();
            if let Some(panic) = panicked {
                // A program that panics under injected faults breaks every label.
                lr.failure
                    .get_or_insert_with(|| Rc::new(Failure::panic(panic, record.clone())));
                continue;
            }
            // The probe reads each schedule against the facts its own correct members hold: a
            // schedule's facts are a subset of the domain (the union of every schedule's), so
            // that is the same reading as against the whole domain.
            let own;
            let domain = if self.probe {
                own = record_domain(labels, t, &record);
                self.domains
                    .entry(t.label)
                    .or_default()
                    .extend(own.iter().cloned());
                &own
            } else {
                &t.domain
            };
            if lr.failure.is_none() {
                lr.failure = failure_of(labels, t, domain, &record).map(Rc::new);
            }
        }
    }
}

impl<B: AsyncFnMut(), R: AsyncFnMut()> Engine<'_, B, R> {
    /// Runs `faults` over every schedule (or the sample), folding each record into a [`Run`] as
    /// it finishes. `seen` gets every record first.
    fn execute(
        &mut self,
        faults: &FaultSet<MessageId>,
        targets: &[Target],
        probe: bool,
        seen: impl FnMut(&Tables, &InstanceRecord),
    ) -> Run {
        let hook = Rc::new(FaultHook::new(
            &FaultPlan {
                faults: faults.clone(),
            },
            self.sim,
        ));
        self.sim.set_network_hook(hook.clone());
        let fold = RefCell::new(RunFold::new(targets, probe));
        let seen = RefCell::new(seen);
        let labels = self.labels;
        let tables = &self.tables;
        let absorb = |records: Vec<InstanceRecord>| {
            for record in records {
                (seen.borrow_mut())(tables, &record);
                fold.borrow_mut().push(labels, record);
            }
        };
        let outcome = {
            let healer = AssertUnwindSafe(hook.clone());
            let mut inputs = AssertUnwindSafe(&mut *self.inputs);
            let mut recover = AssertUnwindSafe(&mut *self.recover);
            let thunk = async || drive(&mut *inputs, &mut *recover, &healer).await;
            let after = || absorb(hook.drain_instances());
            let sim = self.sim;
            let sample = self.sample_schedules;
            if sample.is_none() {
                super::races_engine::refuse_truncated_exhaustive();
            }
            std::panic::catch_unwind(AssertUnwindSafe(move || match sample {
                Some(n) => sim.fuzz_bounded_then(n, thunk, after),
                None => {
                    sim.exhaustive_then(thunk, after);
                }
            }))
        };
        self.sim.clear_network_hook();
        // The schedule that panicked (and any replay of it) never reached `after`.
        absorb(hook.drain_instances());
        let panic = outcome.err().map(|p| program_panic(&*p));
        fold.into_inner().finish(labels, panic)
    }

    /// The labels to check, with their domains from the failure-free probe, and the probe's run.
    /// `seen` gets each of the probe's records first.
    fn prepare(&mut self, seen: impl FnMut(&Tables, &InstanceRecord)) -> (Vec<Target>, Rc<Run>) {
        let (_, mut targets) = self.label_targets();
        let mut probe = self.execute(&FaultSet::new(), &targets, true, seen);
        for t in &mut targets {
            t.domain = probe.domains.remove(&t.label).unwrap_or_default();
        }
        (targets, Rc::new(probe))
    }

    /// The enumeration (see the [module docs](self)), and for every counterexample in
    /// `results` whose set was rerun, whether the rerun broke its label again (by label index).
    fn brute_force(
        &mut self,
        max_faults: usize,
        results: &Results,
    ) -> (BruteForce, BTreeMap<usize, bool>) {
        let start = Instant::now();
        let mut universe: BTreeSet<Fault<MessageId>> = BTreeSet::new();
        let (targets, probe) =
            self.prepare(|tables, record| harvest(record, tables, &mut universe));
        let bounds: Vec<CrashBounds> = targets.iter().map(|t| self.bounds(t)).collect();
        let mut done: BTreeSet<FaultSet<MessageId>> = BTreeSet::from([FaultSet::new()]);
        let mut runs: Vec<(FaultSet<MessageId>, Rc<Run>)> = vec![(FaultSet::new(), probe.clone())];
        let mut schedules = probe.schedules;
        let mut passes = 0;
        loop {
            passes += 1;
            let candidates: Vec<Fault<MessageId>> = universe.iter().cloned().collect();
            let fresh: Vec<FaultSet<MessageId>> = (1..=max_faults)
                .flat_map(|size| subsets(&candidates, size))
                .filter(|set| !done.contains(set))
                .filter(|set| bounds.iter().any(|b| admissible(set, b)))
                .collect();
            if fresh.is_empty() {
                break;
            }
            for set in fresh {
                done.insert(set.clone());
                let run = Rc::new(self.execute(&set, &targets, false, |tables, record| {
                    harvest(record, tables, &mut universe)
                }));
                schedules += run.schedules;
                runs.push((set, run));
            }
        }

        let labels = targets
            .iter()
            .zip(&bounds)
            .map(|(t, b)| {
                let mut admissible_sets = 0;
                let mut breaking = vec![];
                for (set, run) in &runs {
                    if !admissible(set, b) {
                        continue;
                    }
                    admissible_sets += 1;
                    if run.labels[&t.label].failure.is_some() {
                        breaking.push(set.clone());
                    }
                }
                breaking.sort_by_key(|s| (crash_count(s), s.len()));
                BruteLabel {
                    index: t.label,
                    name: label_name(&self.labels.plans[t.label]),
                    admissible: admissible_sets,
                    breaking,
                }
            })
            .collect();

        // Rerun every counterexample's set (outside the enumeration if it is larger), unless
        // schedules are sampled: a sample may miss the failing schedule.
        let mut confirmed = BTreeMap::new();
        if self.sample_schedules.is_none() {
            for label in results.results() {
                let Some(cex) = label.verdict.counterexample() else {
                    continue;
                };
                if !targets.iter().any(|t| t.label == label.index) {
                    continue;
                }
                let failed = if let Some((_, run)) = runs.iter().find(|(set, _)| *set == cex.faults)
                {
                    run.labels[&label.index].failure.is_some()
                } else {
                    let run = self.execute(&cex.faults, &targets, false, |_, _| {});
                    run.labels[&label.index].failure.is_some()
                };
                confirmed.insert(label.index, failed);
            }
        }

        let brute = BruteForce {
            max_faults,
            labels,
            universe: universe.len(),
            sets: runs.len(),
            schedules,
            passes,
            wall: start.elapsed(),
            enumerated: done,
        };
        (brute, confirmed)
    }
}

/// Every way `results` and `brute` disagree (see the [module docs](self)).
fn disagreements(
    results: &Results,
    brute: &BruteForce,
    confirmed: &BTreeMap<usize, bool>,
) -> Vec<String> {
    let names = results.report_names();
    let mut out = vec![];
    for label in results.results() {
        let Some(b) = brute.label(label.index) else {
            continue;
        };
        if let Some(problem) = disagreement(
            label,
            b,
            brute,
            confirmed.get(&label.index).copied(),
            &names,
        ) {
            out.push(format!("{}: {problem}", b.name));
        }
    }
    out
}

/// How `label`'s verdict disagrees with the enumeration `b`, if it does.
fn disagreement(
    label: &LabelResult,
    b: &BruteLabel,
    brute: &BruteForce,
    confirmed: Option<bool>,
    names: &ReportNames<'_>,
) -> Option<String> {
    let show = |set: &FaultSet<MessageId>| show(Some(names), set);
    let refuted = |k: usize, claim: &str| {
        b.breaking_within(k).map(|set| {
            format!(
                "the search says it {claim}, but {} ({} crash(es)) breaks it",
                show(set),
                crash_count(set)
            )
        })
    };
    // The same, for a search that sampled schedules, which proves nothing.
    let missed = |k: usize, claim: &str| {
        b.breaking_within(k).map(|set| {
            format!(
                "the search {claim}, but {} ({} crash(es)) breaks it (schedules were \
                 sampled: a miss of the sample, not a false certificate)",
                show(set),
                crash_count(set)
            )
        })
    };
    match &label.verdict {
        Verdict::Certified {
            tolerates: Tolerance::All,
        } => refuted(usize::MAX, "holds under every admissible fault set"),
        Verdict::Certified {
            tolerates: Tolerance::UpTo(k),
        } => refuted(*k, &format!("holds with up to {k} crash(es)")),
        Verdict::Budget { tolerates: Some(t) } => {
            refuted(*t, &format!("holds with up to {t} crash(es) (budget)"))
        }
        Verdict::Vacuous(_) => refuted(usize::MAX, "is vacuous"),
        Verdict::Exhausted {
            tolerates: Tolerance::All,
            ..
        } => missed(usize::MAX, "found no counterexample"),
        Verdict::Exhausted {
            tolerates: Tolerance::UpTo(k),
            ..
        } => missed(
            *k,
            &format!("found no counterexample with up to {k} crash(es)"),
        ),
        Verdict::Counterexample(cex) => {
            if let Some(t) = cex.tolerates {
                let problem = if cex.tolerance_certified {
                    refuted(t, &format!("tolerates {t} crash(es)"))
                } else {
                    missed(
                        t,
                        &format!("found no counterexample with up to {t} crash(es)"),
                    )
                };
                if let Some(problem) = problem {
                    return Some(problem);
                }
            }
            let expected = cex.tolerates.map_or(0, |t| t + 1);
            if cex.crashes() != expected {
                return Some(format!(
                    "the counterexample {} has {} crash(es), but the sweep tolerated {:?} and so \
                     should break at exactly {expected}",
                    show(&cex.faults),
                    cex.crashes(),
                    cex.tolerates,
                ));
            }
            if confirmed == Some(false) {
                return Some(format!(
                    "rerunning the counterexample {} does not break the label",
                    show(&cex.faults)
                ));
            }
            // A counterexample within the enumerated sizes must be one the enumeration ran
            // and saw break.
            if cex.faults.len() <= brute.max_faults {
                if !brute.enumerated.contains(&cex.faults) {
                    return Some(format!(
                        "the search's counterexample {} is within the enumerated sizes, but the \
                         enumeration never ran it (its universe missed a fault, or the set is \
                         inadmissible)",
                        show(&cex.faults)
                    ));
                }
                if !b.breaking.contains(&cex.faults) {
                    return Some(format!(
                        "the enumeration ran the search's counterexample {} and it held",
                        show(&cex.faults)
                    ));
                }
            }
            None
        }
        _ => None,
    }
}
