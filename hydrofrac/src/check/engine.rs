//! The loop behind [`EcCheck::run`] (see the [module docs](super)): the races search
//! (`races_engine.rs`, docs/design/hydrofrac-races.md), and what it shares with the test-only
//! brute-force oracle (`brute_force.rs`).

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;

use hydro_lang::location::dynamic::LocationId;
use hydro_lang::sim::compiled::{CompiledSim, forbid_barriers, quiesce};
use hydro_lang::sim::flow::SimFlow;
use hydro_lang::sim::network_hook::IrPassStage;

use super::results::{
    ChoiceSite, Counterexample, LabelResult, Results, Stats, VacuousReason, Verdict,
    counterexample_facts, label_name, verdict_line,
};
use crate::fault::{Event, FaultHook, FaultPlan, InstanceRecord, MessageId, NodeId};
use crate::lineage::{Lineage, Tables};
use crate::observe::{Fact, Labels, TapPass, decode_output, tap_pass};
use crate::report::ReportNames;
use crate::search::{CheckedCluster, CrashBounds, Fault, FaultSet};
use crate::spec::{MemberView, Observation, evaluate, learn_domain};

// The test-only honesty check (docs/design/hydrofrac-lite.md §7): a child of this module, as it
// drives the engine. It has its own execution path, independent of the search.
#[cfg(any(test, feature = "cross-check"))]
#[path = "brute_force.rs"]
mod brute;
#[cfg(any(test, feature = "cross-check"))]
pub use brute::{BruteForce, BruteLabel, CrossCheck};

// The search (docs/design/hydrofrac-races.md §4.4, §12): a child of this module, as it drives
// the engine.
#[path = "races_engine.rs"]
mod races_engine;

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
    /// The program's fragile steps (filled at compile time).
    fragile: Option<Rc<RefCell<Vec<crate::provenance::FragileSite>>>>,
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
    /// for programs whose schedule space is too large to explore. Then nothing is certified: a
    /// label that holds on the sample is [`Verdict::Exhausted`].
    pub fn sample_schedules(mut self, n: usize) -> Self {
        self.sample_schedules = Some(n);
        self
    }

    /// Instruments a simulation for the check, and returns it to compile and pass to
    /// [`Self::run`]:
    /// - taps every checked label and emits network hook points;
    /// - marks the closed-broadcast channels, whose fan-out order is proved
    ///   (`crate::provenance::fanout`; the mark runs before the rewrite, on the program's own IR);
    /// - attaches record provenance in fragile mode (docs/design/hydrofrac-races.md §4.1): each
    ///   record names the deliveries it came from and the fragile steps it passed through.
    ///   Operators the rewrite cannot see into are opaque islands; their records fall back to
    ///   the execution rule, which is sound, and count as fragile;
    /// - skips the program's own consistency assertions (the check is what checks them) and
    ///   acknowledges lossy channels (the check injects their omissions itself).
    ///
    /// Once compiled, [`Self::fragile_sites`] lists the program's fragile steps.
    pub fn instrument<'a>(&mut self, flow: SimFlow<'a>) -> SimFlow<'a> {
        let (taps, pass) = tap_pass();
        self.taps = Some(taps);
        // The tap pass runs before networks are compiled, so the provenance rewrite (a
        // `PostNetwork` pass) traces the taps like any other operator.
        let flow = flow
            .refuse_sim_faults("hydrofrac")
            .skip_consistency_assertions()
            .test_safety_only()
            .with_network_hook_points()
            .with_ir_pass(IrPassStage::PreNetwork, pass)
            .with_ir_pass(IrPassStage::PostNetwork, |ir| {
                crate::provenance::fanout::mark(ir);
            });
        let sites = Rc::new(RefCell::new(vec![]));
        self.fragile = Some(sites.clone());
        crate::provenance::install_fragile(flow, Some(self.islands.clone()), sites)
    }

    /// The program's fragile steps, indexed by site: `Some` once a flow passed through
    /// [`Self::instrument`] was compiled, else `None`.
    pub fn fragile_sites(&self) -> Option<Vec<crate::provenance::FragileSite>> {
        self.fragile.as_ref().map(|sites| sites.borrow().clone())
    }

    /// Runs the check on `sim` (compiled from a flow passed through [`Self::instrument`]).
    /// `inputs` drives the program's inputs while faults strike; then the run quiesces, the
    /// network heals and the run quiesces again. The same as [`Self::run_then_recover`] with
    /// nothing to recover.
    ///
    /// # Panics
    /// If `sim` was not instrumented by this check, or if `inputs` places a phase barrier
    /// (see [`Self::run_then_recover`]).
    pub fn run(&self, sim: &CompiledSim, inputs: impl AsyncFnMut()) -> Results {
        self.run_then_recover(sim, inputs, async || {})
    }

    /// Runs the check on `sim` (compiled from a flow passed through [`Self::instrument`]), in
    /// two phases:
    /// 1. the **fault phase**: `inputs` drives the program's inputs while omissions and crashes
    ///    strike. Every interleaving of the inputs with the program's work is explored, so the
    ///    phase must not place a barrier: [`quiesce`] here (or an observation forcing
    ///    quiescence, such as `try_next` with work pending) panics. Awaiting the program's
    ///    outputs is allowed;
    /// 2. the run quiesces and the network **heals**: no more omissions, no new crashes, and
    ///    crashed nodes stay crashed. Then `recover` runs, with no faults: the place for timeout
    ///    ticks and retry rounds, with [`quiesce`] between them if they model time passing.
    ///
    /// The run quiesces again and the specification is checked on what the members hold.
    ///
    /// # Panics
    /// If `sim` was not instrumented by this check, or if `inputs` places a phase barrier.
    pub fn run_then_recover(
        &self,
        sim: &CompiledSim,
        mut inputs: impl AsyncFnMut(),
        mut recover: impl AsyncFnMut(),
    ) -> Results {
        let labels = self
            .taps
            .as_ref()
            .expect("`EcCheck::run` needs a simulation compiled from `EcCheck::instrument`")
            .labels();
        Engine::new(self, sim, &labels, &mut inputs, &mut recover).check()
    }
}

/// The panic of a phase barrier in the fault phase.
const BARRIER_IN_FAULT_PHASE: &str = "hydrofrac: a phase barrier (`quiesce()`, or input after an observation that forced quiescence) inside the inputs of `EcCheck::run`. The fault phase explores every interleaving of the inputs with the program's work; a barrier there hides the interleavings it cuts. Move the rounds that come after the faults (timeouts, retries) into the recovery closure of `EcCheck::run_then_recover`, or drop the barrier.";

/// One instance of a harness: the fault phase (`inputs`, with barriers forbidden), quiesce,
/// heal, the recovery phase (`recover`, with no faults), quiesce.
async fn drive(inputs: &mut impl AsyncFnMut(), recover: &mut impl AsyncFnMut(), hook: &FaultHook) {
    forbid_barriers(BARRIER_IN_FAULT_PHASE, inputs()).await;
    quiesce().await;
    hook.heal();
    quiesce().await;
    recover().await;
    quiesce().await;
}

/// The message of a run's panic, re-raised if it is the harness's mistake rather than the
/// program's failure: a barrier in the fault phase, or a labelled type with no fact identity.
fn program_panic(payload: &(dyn std::any::Any + Send)) -> String {
    let message = panic_message(payload);
    if message == BARRIER_IN_FAULT_PHASE || message.starts_with(crate::observe::NO_IDENTITY) {
        panic!("{message}");
    }
    message
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
    /// The program's schedule choices (empty if it has none), reported with a sampled verdict.
    sites: Vec<ChoiceSite>,
}

/// What a schedule's failure looks like for a counterexample.
struct Failure {
    missing: Vec<(u32, Fact)>,
    divergent: Vec<Fact>,
    panic: Option<String>,
    record: Rc<InstanceRecord>,
}

impl Failure {
    /// A program that panics under injected faults breaks every label.
    fn panic(message: &str, record: Rc<InstanceRecord>) -> Self {
        Failure {
            missing: vec![],
            divergent: vec![],
            panic: Some(message.to_owned()),
            record,
        }
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

struct Engine<'c, B, R> {
    sim: &'c CompiledSim,
    /// The provenance rewrite's opaque islands.
    islands: Rc<RefCell<Vec<String>>>,
    labels: &'c Labels,
    /// The harness's fault phase and recovery phase.
    inputs: &'c mut B,
    recover: &'c mut R,
    tables: Tables,
    max_crashes: Option<usize>,
    sample_schedules: Option<usize>,
    /// The program's fragile steps.
    fragile: Option<Vec<crate::provenance::FragileSite>>,
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

impl<'c, B: AsyncFnMut(), R: AsyncFnMut()> Engine<'c, B, R> {
    fn new(
        check: &EcCheck,
        sim: &'c CompiledSim,
        labels: &'c Labels,
        inputs: &'c mut B,
        recover: &'c mut R,
    ) -> Self {
        Engine {
            sim,
            islands: check.islands.clone(),
            labels,
            inputs,
            recover,
            tables: Tables::new(sim.channels(), sim.locations()),
            max_crashes: check.max_crashes,
            sample_schedules: check.sample_schedules,
            fragile: check.fragile_sites(),
        }
    }

    /// The program's schedule choices: the scan's, and, to be conservative, every operator the
    /// provenance rewrite could not see into (an opaque island's records fall back to the
    /// execution rule, and the scan cannot tell what flows through it).
    fn choices(&self) -> Vec<ChoiceSite> {
        let mut choices = self.labels.choices.clone();
        for island in self.islands.borrow().iter() {
            let choice = ChoiceSite {
                op: "provenance_island".to_owned(),
                site: island.clone(),
            };
            if !choices.contains(&choice) {
                choices.push(choice);
            }
        }
        choices
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

    /// The crash bounds of `target`'s search.
    fn bounds(&self, target: &Target) -> CrashBounds {
        let names = self.tables.names();
        CrashBounds {
            cap: self.max_crashes,
            nodes: names.crashable().len(),
            checked: Some(CheckedCluster::new(
                target
                    .members
                    .iter()
                    .map(|m| names.name(&NodeId::new(&target.cluster, Some(*m)))),
            )),
        }
    }

    /// The results, from every label's verdict (by label index).
    fn results(&self, mut verdicts: Vec<Option<(Verdict, Stats)>>) -> Results {
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
            choices: self.choices(),
            channels: self.sim.channels().to_vec(),
            taps: self.labels.clone(),
        }
    }

    /// The labels that need no search, decided (by label index), and the labels left to search
    /// (with empty fact domains).
    fn label_targets(&self) -> (Vec<Option<(Verdict, Stats)>>, Vec<Target>) {
        let plans = &self.labels.plans;
        let mut verdicts: Vec<Option<(Verdict, Stats)>> = vec![None; plans.len()];
        // The searched labels with more than one member; the others are decided up front.
        let mut targets = vec![];
        let choices = self.choices();
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
                        sites: choices.clone(),
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
        (verdicts, targets)
    }

    /// Writes each counterexample's Lamport diagram to `target/hydrofrac/<test>/<label>/`
    /// (docs/design/hydrofrac-lite.md §4.7) and records where it went.
    fn write_diagrams(&self, verdicts: &mut [Option<(Verdict, Stats)>]) {
        let labels = self.labels;
        let test = crate::lamport::test_name();
        for (index, slot) in verdicts.iter_mut().enumerate() {
            let Some((Verdict::Counterexample(cex), _)) = slot else {
                continue;
            };
            let facts = counterexample_facts(cex, labels);
            let names = ReportNames::new(self.tables.names(), self.sim.channels())
                .with_facts(facts.iter())
                .with_log(&cex.record.log);
            let lamport_labels = crate::lamport::Labels::default()
                .with_channels(self.sim.channels())
                .payloads(|m| names.payload_full(&m.bytes))
                .outputs(|port, bytes| match labels.label_of_port(port) {
                    Some(_) => decode_output(bytes).to_string(),
                    None => crate::lamport::short_hex(bytes),
                });
            let trace = crate::lamport::trace_with(
                &cex.record.log,
                self.tables.names(),
                &cex.faults,
                &lamport_labels,
            );
            let name = label_name(&labels.plans[index]);
            let line = verdict_line(&Verdict::Counterexample(cex.clone()), &names, |j| {
                label_name(&labels.plans[j])
            });
            let title = format!("{name}: {line}");
            let dir = crate::lamport::claim(crate::lamport::default_dir(&test, &name));
            cex.diagram = crate::lamport::write(&dir, "counterexample", &trace, &title)
                .ok()
                .map(|w| w.path().to_path_buf());
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
