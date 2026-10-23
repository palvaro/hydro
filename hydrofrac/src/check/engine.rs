//! The loop behind [`EcCheck::run`] (see the [module docs](super)).

use std::cell::RefCell;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::panic::AssertUnwindSafe;
use std::rc::Rc;
use std::time::{Duration, Instant};

use hydro_lang::location::dynamic::LocationId;
use hydro_lang::sim::compiled::{CompiledSim, quiesce};
use hydro_lang::sim::flow::SimFlow;
use hydro_lang::sim::network_hook::IrPassStage;

use super::results::{
    Counterexample, LabelResult, NondetSite, Results, Stats, VacuousReason, Verdict,
    counterexample_facts, label_name, verdict_line,
};
use crate::fault::{Event, Fate, FaultHook, FaultPlan, InstanceRecord, MessageId, NodeId};
use crate::lineage::{Lineage, Tables};
use crate::observe::{Fact, Labels, TapPass, decode_output, tap_pass};
use crate::report::ReportNames;
use crate::search::{
    CheckedCluster, Clause, CrashBounds, Derivation, Fault, FaultSet, Next, Sweep,
};
use crate::spec::{MemberView, Observation, evaluate, learn_domain};

// A child of this module (it drives the engine), kept in its own file beside it.
#[path = "brute_force.rs"]
mod brute;
#[doc(hidden)]
pub use brute::{BruteForce, BruteLabel, CrossCheck};

/// Checks every `EventualConsistency` label of a program (see the [module docs](super)).
///
/// Two knobs; everything else is a convention (docs/design/hydrofrac-lite.md §2):
/// - [`Self::max_crashes`] caps the crash sweep;
/// - [`Self::sample_schedules`] explores a random sample of each hypothesis's schedules instead
///   of all of them.
#[derive(Default)]
pub struct EcCheck {
    max_crashes: Option<usize>,
    sample_schedules: Option<usize>,
    taps: Option<TapPass>,
    /// The operators the provenance rewrite left as opaque islands (filled at compile time).
    islands: Rc<RefCell<Vec<String>>>,
}

impl EcCheck {
    /// A check with no cap on crashes, exploring every schedule.
    pub fn new() -> Self {
        EcCheck::default()
    }

    /// Caps the crash sweep at `n` crashes: a label that holds at `n` is reported as holding
    /// with up to `n` crashes ([`crate::search::Tolerance::UpTo`]).
    pub fn max_crashes(mut self, n: usize) -> Self {
        self.max_crashes = Some(n);
        self
    }

    /// Runs each hypothesis over `n` randomly scheduled executions instead of every schedule,
    /// for programs whose schedule space is too large to explore. A certificate then covers
    /// the sampled schedules only.
    pub fn sample_schedules(mut self, n: usize) -> Self {
        self.sample_schedules = Some(n);
        self
    }

    /// Instruments a simulation for the check: taps every checked label, emits network hook
    /// points, attaches record provenance, skips the program's own consistency assertions
    /// (the check is what checks them) and acknowledges lossy channels (the check injects their
    /// omissions itself). Compile the result and pass it to [`Self::run`].
    pub fn instrument<'a>(&mut self, flow: SimFlow<'a>) -> SimFlow<'a> {
        let (taps, pass) = tap_pass();
        self.taps = Some(taps);
        // The tap pass runs before networks are compiled, so the provenance rewrite (a
        // `PostNetwork` pass) traces the taps like any other operator.
        let flow = flow
            .skip_consistency_assertions()
            .test_safety_only()
            .with_network_hook_points()
            .with_ir_pass(IrPassStage::PreNetwork, pass);
        // Record provenance (docs/design/hydrofrac-lite.md §5): a `PostNetwork` pass, so it
        // sees the taps' outputs and attaches each observed fact's lineage to them. Operators
        // it cannot see into are recorded as opaque islands; their records fall back to the
        // execution rule, which is sound.
        crate::provenance::install(flow, Some(self.islands.clone()))
    }

    /// Runs the check on `sim` (compiled from a flow passed through [`Self::instrument`]).
    /// `body` drives the program's inputs; after it returns, the network heals and the run
    /// quiesces.
    ///
    /// # Panics
    /// If `sim` was not instrumented by this check.
    pub fn run(&self, sim: &CompiledSim, mut body: impl AsyncFnMut()) -> Results {
        let labels = self
            .taps
            .as_ref()
            .expect("`EcCheck::run` needs a simulation compiled from `EcCheck::instrument`")
            .labels();
        let mut engine = Engine::new(self, sim, &labels, &mut body);
        engine.check()
    }
}

/// One label's search inputs.
struct Target {
    /// The label's index.
    label: usize,
    /// The members of its cluster.
    members: BTreeSet<u32>,
    /// Its location.
    cluster: LocationId,
    /// Its fact domain (from the probe).
    domain: BTreeSet<Fact>,
    /// What makes it schedule-sensitive (empty if nothing does): then its search is a heuristic
    /// and cannot certify it (docs/design/hydrofrac-lite.md §4.9).
    sites: Vec<NondetSite>,
}

impl Target {
    fn sensitive(&self) -> bool {
        !self.sites.is_empty()
    }
}

/// What decides how a clause hits faults: its derivation, and for each sender of one of its
/// deliveries, that sender's sends up to the last one the clause depends on (cut at the heal),
/// which give its positional crash points their meaning. Two clauses with the same key are hit
/// by the same faults and propose the same candidates.
type ClauseKey = (Derivation<MessageId>, Vec<(String, Vec<MessageId>)>);

fn clause_key(clause: &Clause<MessageId>) -> ClauseKey {
    let mut last: BTreeMap<&String, u64> = BTreeMap::new();
    for dep in &clause.deps {
        if let Some((sender, ordinal)) = &dep.sender {
            let e = last.entry(sender).or_default();
            *e = (*e).max(*ordinal);
        }
    }
    let prefixes = last
        .into_iter()
        .map(|(sender, ordinal)| {
            let cut = clause
                .pre_heal_sends
                .get(sender)
                .copied()
                .unwrap_or(0)
                .min(ordinal as usize);
            let prefix = clause
                .sends
                .get(sender)
                .map(|seq| seq.iter().take(cut).cloned().collect())
                .unwrap_or_default();
            (sender.clone(), prefix)
        })
        .collect();
    (clause.deps.clone(), prefixes)
}

/// What one schedule of a run says about one label.
enum ScheduleOutcome {
    /// `POST` held; what any breaking hypothesis must kill.
    Holds(Derivation<MessageId>),
    /// `POST` held because no correct member holds any domain fact: Molly's vacuous case.
    Vacuous,
    /// `POST` failed.
    Fails {
        missing: Vec<(u32, Fact)>,
        divergent: Vec<Fact>,
    },
}

/// What a schedule's failure looks like for a counterexample.
struct Failure {
    missing: Vec<(u32, Fact)>,
    divergent: Vec<Fact>,
    panic: Option<String>,
    record: Rc<InstanceRecord>,
}

/// What one run (a fault set over all its schedules) says about one label.
#[derive(Default)]
struct LabelRun {
    clauses: Vec<Clause<MessageId>>,
    failure: Option<Rc<Failure>>,
    /// Every schedule was vacuous.
    all_vacuous: bool,
}

/// One fault set, run over its schedules.
struct Run {
    /// By label index (searched labels only).
    labels: BTreeMap<usize, LabelRun>,
    schedules: usize,
    wall: Duration,
    opaque_fallbacks: usize,
    /// The probe only: each target's fact domain, by label index.
    domains: BTreeMap<usize, BTreeSet<Fact>>,
    /// The message of the panic that ended the run, if a schedule panicked.
    panic: Option<String>,
}

/// A run being read one schedule at a time, as each finishes (see [`Engine::execute`]). What it
/// keeps is bounded by what the run taught (distinct clauses, the first failure per label), not
/// by its number of schedules: each record is dropped once read, unless it is a failure's.
struct RunFold<'t> {
    targets: &'t [Target],
    /// The run's crashes: the premises of its clauses.
    premises: FaultSet<MessageId>,
    /// Whether this is the failure-free probe, which learns each target's domain as it reads.
    probe: bool,
    labels: BTreeMap<usize, LabelRun>,
    /// Per label, its clauses' indices by the hash of their derivation, to drop the duplicate
    /// derivations most schedules repeat without a linear scan.
    seen: BTreeMap<usize, HashMap<u64, Vec<usize>>>,
    domains: BTreeMap<usize, BTreeSet<Fact>>,
    schedules: usize,
    opaque_fallbacks: usize,
    /// The latest record, read when the next one arrives or at [`Self::finish`]: when a
    /// schedule panics, the run's last record is the panicking one.
    pending: Option<InstanceRecord>,
}

impl<'t> RunFold<'t> {
    fn new(faults: &FaultSet<MessageId>, targets: &'t [Target], probe: bool) -> Self {
        RunFold {
            targets,
            premises: faults
                .iter()
                .filter(|f| matches!(f, Fault::Crash { .. }))
                .cloned()
                .collect(),
            probe,
            labels: targets
                .iter()
                .map(|t| {
                    (
                        t.label,
                        LabelRun {
                            all_vacuous: true,
                            ..LabelRun::default()
                        },
                    )
                })
                .collect(),
            seen: BTreeMap::new(),
            domains: BTreeMap::new(),
            schedules: 0,
            opaque_fallbacks: 0,
            pending: None,
        }
    }

    /// Takes the next schedule's record (reading the one before it).
    fn push(&mut self, labels: &Labels, tables: &Tables, record: InstanceRecord) {
        if let Some(previous) = self.pending.replace(record) {
            self.read(labels, tables, previous, None);
        }
    }

    /// Reads the last record (the panicking schedule's, if `panic`) and ends the run.
    fn finish(
        mut self,
        labels: &Labels,
        tables: &Tables,
        panic: Option<String>,
        wall: Duration,
    ) -> Run {
        if let Some(last) = self.pending.take() {
            self.read(labels, tables, last, panic.as_deref());
        }
        let schedules = self.schedules;
        for lr in self.labels.values_mut() {
            lr.all_vacuous &= schedules > 0;
        }
        Run {
            labels: self.labels,
            schedules,
            wall,
            opaque_fallbacks: self.opaque_fallbacks,
            domains: self.domains,
            panic,
        }
    }

    /// Reads one schedule's record for every target.
    fn read(
        &mut self,
        labels: &Labels,
        tables: &Tables,
        record: InstanceRecord,
        panicked: Option<&str>,
    ) {
        self.schedules += 1;
        let record = Rc::new(record);
        if self.probe {
            for t in self.targets {
                let domain = record_domain(labels, t, &record);
                self.domains.entry(t.label).or_default().extend(domain);
            }
        }
        if let Some(panic) = panicked {
            // A program that panics under injected faults breaks every label.
            for t in self.targets {
                let lr = self.labels.get_mut(&t.label).unwrap();
                lr.all_vacuous = false;
                lr.failure.get_or_insert_with(|| {
                    Rc::new(Failure {
                        missing: vec![],
                        divergent: vec![],
                        panic: Some(panic.to_owned()),
                        record: record.clone(),
                    })
                });
            }
            return;
        }
        let lineage = Lineage::new(&record.log, tables);
        for t in self.targets {
            // The probe reads each schedule against the facts its own correct members hold.
            // That is the same reading as against the whole domain, which is only known once
            // every schedule is in: a schedule's facts are a subset of the domain (the union of
            // every schedule's), so in both readings what is required is exactly what the
            // schedule's correct members hold, and nothing is outside the domain.
            let probe_domain;
            let domain = if self.probe {
                probe_domain = record_domain(labels, t, &record);
                &probe_domain
            } else {
                &t.domain
            };
            let outcome = outcome(labels, t, domain, &record, &lineage);
            let lr = self.labels.get_mut(&t.label).unwrap();
            match outcome {
                ScheduleOutcome::Holds(deps) => {
                    lr.all_vacuous = false;
                    let clause = Clause {
                        deps,
                        sends: lineage.send_sequences().clone(),
                        pre_heal_sends: lineage.pre_heal_sends().clone(),
                        premises: self.premises.clone(),
                    };
                    // Schedules repeat derivations; keep one clause per kill relation (the send
                    // order matters to positional crashes, so it is part of the identity).
                    let key = clause_key(&clause);
                    let mut hasher = DefaultHasher::new();
                    key.hash(&mut hasher);
                    let same = self
                        .seen
                        .entry(t.label)
                        .or_default()
                        .entry(hasher.finish())
                        .or_default();
                    if !same.iter().any(|&i| clause_key(&lr.clauses[i]) == key) {
                        same.push(lr.clauses.len());
                        lr.clauses.push(clause);
                    }
                }
                ScheduleOutcome::Vacuous => {}
                ScheduleOutcome::Fails { missing, divergent } => {
                    lr.all_vacuous = false;
                    lr.failure.get_or_insert_with(|| {
                        Rc::new(Failure {
                            missing,
                            divergent,
                            panic: None,
                            record: record.clone(),
                        })
                    });
                }
            }
        }
        self.opaque_fallbacks += lineage.opaque_fallbacks();
    }
}

/// The facts `target`'s correct members hold in `record`: what the probe learns as its domain.
fn record_domain(labels: &Labels, target: &Target, record: &InstanceRecord) -> BTreeSet<Fact> {
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
    learn_domain(&view, &correct_members(target, record))
}

/// What one schedule says about `target`, against `domain`.
fn outcome(
    labels: &Labels,
    target: &Target,
    domain: &BTreeSet<Fact>,
    record: &InstanceRecord,
    lineage: &Lineage<'_>,
) -> ScheduleOutcome {
    let observations: Vec<Observation<usize>> = lineage
        .observations()
        .filter(|o| labels.label_of_port(o.port) == Some(target.label))
        .map(|o| Observation {
            label: target.label,
            member: o.node.member.unwrap_or(0),
            fact: decode_output(o.bytes),
            support: o.event,
        })
        .collect();
    let view = MemberView::of(target.label, &observations);
    let correct = correct_members(target, record);
    let evaluation = evaluate(&view, domain, &correct);
    // Facts outside the domain that only some correct members hold: a fault made members
    // derive something new (e.g. through an `anti_join`), and they disagree on it.
    let divergent: Vec<Fact> = evaluation
        .outside_domain
        .iter()
        .filter(|fact| !correct.iter().all(|m| view.holds(*m, fact)))
        .cloned()
        .collect();
    if !evaluation.holds() || !divergent.is_empty() {
        return ScheduleOutcome::Fails {
            missing: evaluation.missing,
            divergent,
        };
    }
    if evaluation.required.is_empty() {
        return ScheduleOutcome::Vacuous;
    }
    // `POST` needs every required `(member, fact)` pair. A hypothesis breaks it only by
    // removing every observation of some pair, and so in particular that pair's first
    // observation: it must kill a delivery of that observation's derivation. The union of
    // the first observations' derivations is therefore a sound clause, and tighter than the
    // union of every observation's.
    let mut deps = Derivation::new();
    for pair in &evaluation.held {
        deps.extend(lineage.derivation(pair.supports[0]).iter().cloned());
    }
    ScheduleOutcome::Holds(deps)
}

/// What one label's search has learned from its runs.
struct Learned {
    /// Whether the label is schedule-sensitive (see [`Engine::search`]).
    sensitive: bool,
    /// Not schedule-sensitive: every clause must be hit.
    clauses: Vec<Clause<MessageId>>,
    known: HashSet<(ClauseKey, FaultSet<MessageId>)>,
    /// Schedule-sensitive: some clause of every group (one group per run).
    groups: Vec<Vec<Clause<MessageId>>>,
    known_groups: HashSet<Vec<ClauseKey>>,
}

impl Learned {
    fn new(sensitive: bool) -> Self {
        Learned {
            sensitive,
            clauses: vec![],
            known: HashSet::new(),
            groups: vec![],
            known_groups: HashSet::new(),
        }
    }

    /// Learns the clauses of one run (its schedules' distinct derivations).
    fn learn(&mut self, run_clauses: &[Clause<MessageId>]) {
        if self.sensitive {
            if run_clauses.is_empty() {
                return;
            }
            let mut keys: Vec<ClauseKey> = run_clauses.iter().map(clause_key).collect();
            keys.sort();
            if self.known_groups.insert(keys) {
                self.groups.push(run_clauses.to_vec());
            }
        } else {
            for clause in run_clauses {
                if self.known.insert((clause_key(clause), clause.premises.clone())) {
                    self.clauses.push(clause.clone());
                }
            }
        }
    }

    /// The sweep's next step given what was learned.
    fn next(&self, sweep: &mut Sweep<MessageId>) -> Next<MessageId> {
        if self.sensitive {
            let groups: Vec<&[Clause<MessageId>]> =
                self.groups.iter().map(Vec::as_slice).collect();
            sweep.next_any(&groups)
        } else {
            sweep.next(&self.clauses)
        }
    }
}

struct Engine<'c, B> {
    sim: &'c CompiledSim,
    /// The provenance rewrite's opaque islands.
    islands: Rc<RefCell<Vec<String>>>,
    labels: &'c Labels,
    body: &'c mut B,
    tables: Tables,
    max_crashes: Option<usize>,
    sample_schedules: Option<usize>,
    /// Every run so far, by fault set; labels share them.
    runs: BTreeMap<FaultSet<MessageId>, Rc<Run>>,
}

/// The message of a panic payload.
fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_owned()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "(non-string panic payload)".to_owned()
    }
}

impl<'c, B: AsyncFnMut()> Engine<'c, B> {
    fn new(check: &EcCheck, sim: &'c CompiledSim, labels: &'c Labels, body: &'c mut B) -> Self {
        Engine {
            sim,
            islands: check.islands.clone(),
            labels,
            body,
            tables: Tables::new(sim.channels(), sim.locations()),
            max_crashes: check.max_crashes,
            sample_schedules: check.sample_schedules,
            runs: BTreeMap::new(),
        }
    }

    /// What makes the label `plan` schedule-sensitive: the static walk's sites, and, to be
    /// conservative, every operator the provenance rewrite could not see into (an opaque
    /// island's records fall back to the execution rule, and the walk cannot tell what flows
    /// through it).
    fn sites_of(&self, plan: &crate::observe::LabelPlan) -> Vec<NondetSite> {
        let mut sites = plan.sites.clone();
        for island in self.islands.borrow().iter() {
            let site = NondetSite {
                site: island.clone(),
                op: "provenance_island",
                kind: "-",
                location: plan.cluster().root().clone(),
                local: true,
                opaque: true,
                send_order: false,
            };
            if !sites.contains(&site) {
                sites.push(site);
            }
        }
        sites
    }

    /// The members of `cluster` (`None` for a process).
    fn members_of(&self, cluster: &LocationId) -> Option<BTreeSet<u32>> {
        self.sim
            .locations()
            .iter()
            .find(|l| l.id.root() == cluster.root())
            .and_then(|l| l.members.as_ref())
            .map(|m| m.iter().copied().collect())
    }

    /// Runs `faults` over every schedule (or the sample), folding each schedule's record into a
    /// [`Run`] as soon as it finishes, so a run's memory is bounded by what it learned (distinct
    /// clauses, the first failure per label) rather than by its number of schedules. `seen` gets
    /// every record before it is folded. The probe (`probe`) also learns each target's domain.
    fn execute(
        &mut self,
        faults: &FaultSet<MessageId>,
        targets: &[Target],
        probe: bool,
        seen: impl FnMut(&Tables, &InstanceRecord),
    ) -> Run {
        let start = Instant::now();
        let hook = Rc::new(FaultHook::new(
            &FaultPlan {
                faults: faults.clone(),
            },
            self.sim,
        ));
        self.sim.set_network_hook(hook.clone());
        let fold = RefCell::new(RunFold::new(faults, targets, probe));
        let seen = RefCell::new(seen);
        let labels = self.labels;
        let tables = &self.tables;
        let absorb = |records: Vec<InstanceRecord>| {
            for record in records {
                (seen.borrow_mut())(tables, &record);
                fold.borrow_mut().push(labels, tables, record);
            }
        };
        let outcome = {
            let healer = AssertUnwindSafe(hook.clone());
            let mut body = AssertUnwindSafe(&mut *self.body);
            let thunk = async || {
                (*body)().await;
                // Faults are admitted while the body drives inputs and while the work it caused
                // settles; then the network heals (docs/design/hydrofrac-lite.md §4.6). The
                // simulator only runs the program while the body is blocked, so healing as soon
                // as a body that does not quiesce returns would admit no fault at all.
                quiesce().await;
                healer.heal();
                quiesce().await;
            };
            let after = || absorb(hook.drain_instances());
            let sim = self.sim;
            let sample = self.sample_schedules;
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
        let panic = outcome.err().map(|p| panic_message(&*p));
        fold.into_inner()
            .finish(labels, tables, panic, start.elapsed())
    }

    /// Runs `faults` (once; later calls reuse the run) and reads every target's outcome.
    fn run(&mut self, faults: &FaultSet<MessageId>, targets: &[Target]) -> Rc<Run> {
        if let Some(run) = self.runs.get(faults) {
            return run.clone();
        }
        let run = Rc::new(self.execute(faults, targets, false, |_, _| {}));
        self.runs.insert(faults.clone(), run.clone());
        run
    }

    /// The crash bounds of `target`'s search.
    fn bounds(&self, target: &Target) -> CrashBounds {
        let names = self.tables.names();
        CrashBounds {
            cap: self.max_crashes,
            nodes: self.sim.node_count(),
            checked: Some(CheckedCluster::new(
                target
                    .members
                    .iter()
                    .map(|m| names.name(&NodeId::new(&target.cluster, Some(*m)))),
            )),
        }
    }

    /// One label's sweep, from the probe's run.
    ///
    /// For a label that is not schedule-sensitive each fault set has one execution, and a
    /// hypothesis must kill every derivation seen so far (Molly's rule): running out of
    /// hypotheses certifies the label. For a schedule-sensitive one that rule is unsound (a
    /// derivation seen in one schedule need not recur in another), so the search is a heuristic
    /// instead: each earlier run is one disjunctive group, and a hypothesis need only kill the
    /// derivations of one of its schedules ([`Sweep::next_any`]). Running out of hypotheses then
    /// ends [`Verdict::Exhausted`], not a certificate (docs/design/hydrofrac-lite.md §4.9).
    fn search(&mut self, target: &Target, probe: &Run, targets: &[Target]) -> (Verdict, Stats) {
        let start = Instant::now();
        let sensitive = target.sensitive();
        let mut stats = Stats {
            hypotheses: 1,
            schedules: probe.schedules,
            wall: probe.wall,
            opaque_fallbacks: probe.opaque_fallbacks,
        };
        let probe_run = &probe.labels[&target.label];
        if let Some(failure) = &probe_run.failure {
            return (
                counterexample(FaultSet::new(), None, !sensitive, 0, failure),
                stats,
            );
        }
        let mut sweep: Sweep<MessageId> = Sweep::new(self.bounds(target));
        let mut learned = Learned::new(sensitive);
        learned.learn(&probe_run.clauses);
        let verdict = loop {
            let next = learned.next(&mut sweep);
            match next {
                Next::Try(set) => {
                    let cached = self.runs.contains_key(&set);
                    let run = self.run(&set, targets);
                    stats.schedules += run.schedules;
                    if !cached {
                        stats.opaque_fallbacks += run.opaque_fallbacks;
                    }
                    let lr = &run.labels[&target.label];
                    if let Some(failure) = &lr.failure {
                        break counterexample(
                            set,
                            sweep.tolerated(),
                            !sensitive,
                            sweep.hypotheses() - 1,
                            failure,
                        );
                    }
                    learned.learn(&lr.clauses);
                    if lr.all_vacuous {
                        sweep.mark_vacuous(set);
                    }
                }
                Next::Certified(tolerates) if sensitive => {
                    break Verdict::Exhausted {
                        tolerates,
                        sites: target.sites.clone(),
                    };
                }
                Next::Certified(tolerates) => break Verdict::Certified { tolerates },
                Next::Budget => {
                    break Verdict::Budget {
                        tolerates: sweep.tolerated(),
                    };
                }
            }
        };
        stats.hypotheses = sweep.hypotheses();
        stats.wall += start.elapsed();
        (verdict, stats)
    }

    fn check(&mut self) -> Results {
        let (mut verdicts, targets, probe) = self.prepare(|_, _| {});
        for t in &targets {
            let result = self.search(t, &probe, &targets);
            verdicts[t.label] = Some(result);
        }
        self.write_diagrams(&mut verdicts);

        let plans = &self.labels.plans;
        Results {
            labels: plans
                .iter()
                .enumerate()
                .map(|(index, plan)| {
                    let (verdict, stats) = verdicts[index].take().expect("every label decided");
                    LabelResult {
                        index,
                        plan: plan.clone(),
                        verdict,
                        stats,
                    }
                })
                .collect(),
            names: self.tables.names().clone(),
            islands: self.islands.borrow().clone(),
            channels: self.sim.channels().to_vec(),
            taps: self.labels.clone(),
            test: crate::lamport::test_name(),
        }
    }

    /// Decides the labels that need no search, and runs the failure-free probe: the verdicts
    /// decided so far (by label index), the labels left to search (with their fact domains),
    /// and the probe's run. `seen` gets each of the probe's records before it is read.
    fn prepare(
        &mut self,
        seen: impl FnMut(&Tables, &InstanceRecord),
    ) -> (Vec<Option<(Verdict, Stats)>>, Vec<Target>, Rc<Run>) {
        let plans = &self.labels.plans;
        let mut verdicts: Vec<Option<(Verdict, Stats)>> = vec![None; plans.len()];
        // The searched labels with more than one member; the others are decided up front.
        let mut targets = vec![];
        for (i, plan) in plans.iter().enumerate() {
            if let Some(reason) = &plan.not_checked {
                verdicts[i] = Some((Verdict::NotChecked(reason.clone()), Stats::default()));
            } else if let Some(by) = plan.covered_by {
                verdicts[i] = Some((Verdict::CoveredBy(by), Stats::default()));
            } else {
                match self.members_of(plan.cluster()) {
                    Some(members) if members.len() > 1 => targets.push(Target {
                        label: i,
                        members,
                        cluster: plan.cluster().clone(),
                        domain: BTreeSet::new(),
                        sites: self.sites_of(plan),
                    }),
                    _ => {
                        verdicts[i] = Some((
                            Verdict::Vacuous(VacuousReason::SingleMember),
                            Stats::default(),
                        ));
                    }
                }
            }
        }

        // Hypothesis 0: the failure-free probe learns every label's fact domain as it reads
        // each schedule (see `RunFold::read`).
        let mut probe = self.execute(&FaultSet::new(), &targets, true, seen);
        for t in &mut targets {
            t.domain = probe.domains.remove(&t.label).unwrap_or_default();
        }
        let (empty, targets): (Vec<Target>, Vec<Target>) =
            targets.into_iter().partition(|t| t.domain.is_empty());
        for t in empty {
            let lr = probe.labels.remove(&t.label).unwrap_or_default();
            if probe.panic.is_some() {
                // The probe panicked before the label saw anything: still a counterexample.
                targets_panicked(&mut verdicts, &t, lr.failure.as_deref(), &probe);
            } else {
                verdicts[t.label] = Some((
                    Verdict::Vacuous(VacuousReason::EmptyDomain),
                    Stats {
                        hypotheses: 1,
                        schedules: probe.schedules,
                        wall: probe.wall,
                        opaque_fallbacks: 0,
                    },
                ));
            }
        }
        let probe = Rc::new(probe);
        self.runs.insert(FaultSet::new(), probe.clone());
        (verdicts, targets, probe)
    }

    /// Writes each counterexample's Lamport diagram to `target/hydrofrac/<test>/<label>/`
    /// (docs/design/hydrofrac-lite.md §4.8) and records where it went.
    fn write_diagrams(&self, verdicts: &mut [Option<(Verdict, Stats)>]) {
        let labels = self.labels;
        let lamport_labels = crate::lamport::Labels::default()
            .with_channels(self.sim.channels())
            .outputs(move |port, bytes| match labels.label_of_port(port) {
                Some(_) => decode_output(bytes).to_string(),
                None => crate::lamport::short_hex(bytes),
            });
        let test = crate::lamport::test_name();
        for (index, slot) in verdicts.iter_mut().enumerate() {
            let Some((Verdict::Counterexample(cex), _)) = slot else {
                continue;
            };
            let name = label_name(&labels.plans[index]);
            let trace = crate::lamport::trace_with(
                &cex.record.log,
                self.tables.names(),
                &cex.faults,
                &lamport_labels,
            );
            let opts = crate::lamport::Options {
                title: Some({
                    let facts = counterexample_facts(cex, labels);
                    let names = ReportNames::new(self.tables.names(), self.sim.channels())
                        .with_facts(facts.iter());
                    let line = verdict_line(&Verdict::Counterexample(cex.clone()), &names, |j| {
                        labels
                            .plans
                            .get(j)
                            .map_or_else(|| format!("label {j}"), label_name)
                    });
                    format!("{name}: {line}")
                }),
                highlight: self.missing_support(index, cex),
                ..crate::lamport::Options::default()
            };
            let dir = crate::lamport::claim(crate::lamport::default_dir(&test, &name));
            cex.diagram = crate::lamport::write(&dir, "counterexample", &trace, &opts)
                .ok()
                .map(|w| w.path().to_path_buf());
        }
    }

    /// The deliveries to emphasize in a counterexample's diagram: how the members that do
    /// hold a missing fact came to hold it (the support the run denied the others).
    fn missing_support(&self, label: usize, cex: &Counterexample) -> BTreeSet<u64> {
        let missing: BTreeSet<&Fact> = cex.missing.iter().map(|(_, fact)| fact).collect();
        if missing.is_empty() {
            return BTreeSet::new();
        }
        let lineage = Lineage::new(&cex.record.log, &self.tables);
        let mut messages: BTreeSet<MessageId> = BTreeSet::new();
        for o in lineage.observations() {
            if self.labels.label_of_port(o.port) == Some(label)
                && missing.contains(&decode_output(o.bytes))
            {
                messages.extend(
                    lineage
                        .derivation(o.event)
                        .iter()
                        .map(|d| d.message.clone()),
                );
            }
        }
        cex.record
            .log
            .events()
            .iter()
            .filter_map(|e| match e {
                Event::Sent {
                    id,
                    msg,
                    fate: Fate::Delivered,
                    ..
                } if messages.contains(msg) => Some(*id),
                _ => None,
            })
            .collect()
    }
}

/// The members of `target`'s cluster whose crash did not fire in `record`.
fn correct_members(target: &Target, record: &InstanceRecord) -> BTreeSet<u32> {
    target
        .members
        .iter()
        .copied()
        .filter(|m| {
            !record
                .crashed
                .contains(&NodeId::new(&target.cluster, Some(*m)))
        })
        .collect()
}

fn counterexample(
    faults: FaultSet<MessageId>,
    tolerates: Option<usize>,
    tolerance_certified: bool,
    hypothesis: usize,
    failure: &Failure,
) -> Verdict {
    Verdict::Counterexample(Box::new(Counterexample {
        faults,
        tolerates,
        tolerance_certified,
        hypothesis,
        missing: failure.missing.clone(),
        divergent: failure.divergent.clone(),
        panic: failure.panic.clone(),
        record: failure.record.clone(),
        diagram: None,
    }))
}

/// A label whose probe panicked before it observed anything; `failure` is the panic the probe's
/// read recorded for it, if the panicking schedule left a record.
fn targets_panicked(
    verdicts: &mut [Option<(Verdict, Stats)>],
    target: &Target,
    failure: Option<&Failure>,
    probe: &Run,
) {
    let record = failure.map_or_else(|| Rc::new(InstanceRecord::default()), |f| f.record.clone());
    verdicts[target.label] = Some((
        counterexample(
            FaultSet::new(),
            None,
            target.sites.is_empty(),
            0,
            &Failure {
                missing: vec![],
                divergent: vec![],
                panic: probe.panic.clone(),
                record,
            },
        ),
        Stats {
            hypotheses: 1,
            schedules: probe.schedules,
            wall: probe.wall,
            opaque_fallbacks: 0,
        },
    ));
}
