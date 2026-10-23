//! The loop behind [`EcCheck::run`] (see the [module docs](super)).

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::panic::AssertUnwindSafe;
use std::rc::Rc;
use std::time::{Duration, Instant};

use hydro_lang::location::dynamic::LocationId;
use hydro_lang::sim::compiled::{CompiledSim, quiesce};
use hydro_lang::sim::flow::SimFlow;
use hydro_lang::sim::network_hook::IrPassStage;

use super::results::{Counterexample, LabelResult, Results, Stats, VacuousReason, Verdict};
use crate::fault::{FaultHook, FaultPlan, InstanceRecord, MessageId, NodeId};
use crate::lineage::{Lineage, Tables};
use crate::observe::{Fact, Labels, TapPass, decode_output, tap_pass};
use crate::search::{
    CheckedCluster, Clause, CrashBounds, Derivation, Fault, FaultSet, Next, Sweep,
};
use crate::spec::{MemberView, Observation, evaluate, learn_domain};

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
        // TODO(lead): attach record provenance here once `crate::provenance` is merged:
        // `let flow = crate::provenance::install(flow, None);`. Until then every record is
        // opaque and lineage uses the execution rule, which is sound.
        flow
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
}

struct Engine<'c, B> {
    sim: &'c CompiledSim,
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
            labels,
            body,
            tables: Tables::new(sim.channels(), sim.locations()),
            max_crashes: check.max_crashes,
            sample_schedules: check.sample_schedules,
            runs: BTreeMap::new(),
        }
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

    /// Runs `faults` over every schedule (or the sample): each schedule's record, the panic
    /// message if a schedule panicked (its record is the last), and the wall time.
    fn execute(
        &mut self,
        faults: &FaultSet<MessageId>,
    ) -> (Vec<InstanceRecord>, Option<String>, Duration) {
        let start = Instant::now();
        let hook = Rc::new(FaultHook::new(
            &FaultPlan {
                faults: faults.clone(),
            },
            self.sim,
        ));
        self.sim.set_network_hook(hook.clone());
        let records: RefCell<Vec<InstanceRecord>> = RefCell::new(vec![]);
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
            let after = || records.borrow_mut().extend(hook.drain_instances());
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
        let mut records = records.into_inner();
        // The schedule that panicked (and any replay of it) never reached `after`.
        records.extend(hook.drain_instances());
        let panic = outcome.err().map(|p| panic_message(&*p));
        (records, panic, start.elapsed())
    }

    /// What one schedule says about `target`.
    fn outcome(
        &self,
        target: &Target,
        record: &InstanceRecord,
        lineage: &Lineage<'_>,
    ) -> ScheduleOutcome {
        let observations: Vec<Observation<usize>> = lineage
            .observations()
            .filter(|o| self.labels.label_of_port(o.port) == Some(target.label))
            .map(|o| Observation {
                label: target.label,
                member: o.node.member.unwrap_or(0),
                fact: decode_output(o.bytes),
                support: o.event,
            })
            .collect();
        let view = MemberView::of(target.label, &observations);
        let correct = correct_members(target, record);
        let evaluation = evaluate(&view, &target.domain, &correct);
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

    /// Runs `faults` (once; later calls reuse the run) and reads every target's outcome.
    fn run(&mut self, faults: &FaultSet<MessageId>, targets: &[Target]) -> Rc<Run> {
        if let Some(run) = self.runs.get(faults) {
            return run.clone();
        }
        let (records, panic, wall) = self.execute(faults);
        let run = Rc::new(self.read(faults, records, panic, wall, targets));
        self.runs.insert(faults.clone(), run.clone());
        run
    }

    /// Reads a run's records for every target.
    fn read(
        &self,
        faults: &FaultSet<MessageId>,
        records: Vec<InstanceRecord>,
        panic: Option<String>,
        wall: Duration,
        targets: &[Target],
    ) -> Run {
        let premises: FaultSet<MessageId> = faults
            .iter()
            .filter(|f| matches!(f, Fault::Crash { .. }))
            .cloned()
            .collect();
        let schedules = records.len();
        let mut labels: BTreeMap<usize, LabelRun> = targets
            .iter()
            .map(|t| {
                (
                    t.label,
                    LabelRun {
                        all_vacuous: schedules > 0,
                        ..LabelRun::default()
                    },
                )
            })
            .collect();
        let mut opaque_fallbacks = 0;
        let panicked = panic.is_some().then(|| records.len().saturating_sub(1));
        for (i, record) in records.into_iter().enumerate() {
            let record = Rc::new(record);
            if panicked == Some(i) {
                // A program that panics under injected faults breaks every label.
                for t in targets {
                    let lr = labels.get_mut(&t.label).unwrap();
                    lr.all_vacuous = false;
                    lr.failure.get_or_insert_with(|| {
                        Rc::new(Failure {
                            missing: vec![],
                            divergent: vec![],
                            panic: panic.clone(),
                            record: record.clone(),
                        })
                    });
                }
                continue;
            }
            let lineage = Lineage::new(&record.log, &self.tables);
            for t in targets {
                let outcome = self.outcome(t, &record, &lineage);
                let lr = labels.get_mut(&t.label).unwrap();
                match outcome {
                    ScheduleOutcome::Holds(deps) => {
                        lr.all_vacuous = false;
                        let duplicate = lr.clauses.iter().any(|c| c.deps == deps);
                        if !duplicate {
                            lr.clauses.push(Clause {
                                deps,
                                sends: lineage.send_sequences().clone(),
                                pre_heal_sends: lineage.pre_heal_sends().clone(),
                                premises: premises.clone(),
                            });
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
            opaque_fallbacks += lineage.opaque_fallbacks();
        }
        Run {
            labels,
            schedules,
            wall,
            opaque_fallbacks,
        }
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
    fn search(&mut self, target: &Target, probe: &Run, targets: &[Target]) -> (Verdict, Stats) {
        let start = Instant::now();
        let mut stats = Stats {
            hypotheses: 1,
            schedules: probe.schedules,
            wall: probe.wall,
            opaque_fallbacks: probe.opaque_fallbacks,
        };
        let probe_run = &probe.labels[&target.label];
        if let Some(failure) = &probe_run.failure {
            return (counterexample(FaultSet::new(), None, 0, failure), stats);
        }
        let mut sweep: Sweep<MessageId> = Sweep::new(self.bounds(target));
        let mut clauses: Vec<Clause<MessageId>> = probe_run.clauses.clone();
        let verdict = loop {
            match sweep.next(&clauses) {
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
                            sweep.hypotheses() - 1,
                            failure,
                        );
                    }
                    for clause in &lr.clauses {
                        let known = clauses
                            .iter()
                            .any(|c| c.deps == clause.deps && c.premises == clause.premises);
                        if !known {
                            clauses.push(clause.clone());
                        }
                    }
                    if lr.all_vacuous {
                        sweep.mark_vacuous(set);
                    }
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

        // Hypothesis 0: the failure-free probe learns every label's fact domain, then is read
        // like any other run.
        let (records, panic, wall) = self.execute(&FaultSet::new());
        for t in &mut targets {
            let mut domain = BTreeSet::new();
            for record in &records {
                let observations: Vec<Observation<()>> = record
                    .log
                    .events()
                    .iter()
                    .filter_map(|e| match e {
                        crate::fault::Event::Output {
                            port, node, bytes, ..
                        } if self.labels.label_of_port(*port) == Some(t.label) => {
                            Some(Observation {
                                label: t.label,
                                member: node.member.unwrap_or(0),
                                fact: decode_output(bytes),
                                support: (),
                            })
                        }
                        _ => None,
                    })
                    .collect();
                let view = MemberView::of(t.label, &observations);
                domain.extend(learn_domain(&view, &correct_members(t, record)));
            }
            t.domain = domain;
        }
        let (empty, targets): (Vec<Target>, Vec<Target>) =
            targets.into_iter().partition(|t| t.domain.is_empty());
        for t in empty {
            if panic.is_some() {
                // The probe panicked before the label saw anything: still a counterexample.
                targets_panicked(&mut verdicts, &t, &records, &panic, wall);
            } else {
                verdicts[t.label] = Some((
                    Verdict::Vacuous(VacuousReason::EmptyDomain),
                    Stats {
                        hypotheses: 1,
                        schedules: records.len(),
                        wall,
                        opaque_fallbacks: 0,
                    },
                ));
            }
        }
        let probe = Rc::new(self.read(&FaultSet::new(), records, panic, wall, &targets));
        self.runs.insert(FaultSet::new(), probe.clone());

        for t in &targets {
            let result = self.search(t, &probe, &targets);
            verdicts[t.label] = Some(result);
        }
        // TODO(lead): write each counterexample's Lamport diagram (`crate::lamport`) to
        // `target/hydrofrac/<test>/<label>/` once that module is merged.

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
        }
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
    hypothesis: usize,
    failure: &Failure,
) -> Verdict {
    Verdict::Counterexample(Box::new(Counterexample {
        faults,
        tolerates,
        hypothesis,
        missing: failure.missing.clone(),
        divergent: failure.divergent.clone(),
        panic: failure.panic.clone(),
        record: failure.record.clone(),
    }))
}

/// A label whose probe panicked before it observed anything.
fn targets_panicked(
    verdicts: &mut [Option<(Verdict, Stats)>],
    target: &Target,
    records: &[InstanceRecord],
    panic: &Option<String>,
    wall: Duration,
) {
    let record = Rc::new(records.last().cloned().unwrap_or_default());
    verdicts[target.label] = Some((
        counterexample(
            FaultSet::new(),
            None,
            0,
            &Failure {
                missing: vec![],
                divergent: vec![],
                panic: panic.clone(),
                record,
            },
        ),
        Stats {
            hypotheses: 1,
            schedules: records.len(),
            wall,
            opaque_fallbacks: 0,
        },
    ));
}
