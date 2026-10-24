//! The races mode of the check (docs/design/hydrofrac-races.md §4.4, §5): [`EcCheck::races`].
//!
//! Each label keeps one clause per derivation of each `(member, fact)` pair, robust if no record
//! of it passed a fragile step ([`crate::races`]). A fault set is skipped only when, for every
//! pair, a robust derivation survives it (Lemma R). Every set the search does run is explored
//! over its schedules, and stops early as soon as the derivations learned cover every pair:
//! - one schedule, if every run of the set has the same facts: the program is fragile-free (or
//!   has no schedule choices) and every crash of the set is a start crash;
//! - otherwise every schedule (or the sample), stopping early.
//!
//! When the sweep cannot finish (a pair with no robust derivation, or a set that was run and is
//! still admissible for some pair), the label falls back to enumerating every admissible set of
//! at most [`FALLBACK_FAULTS`] faults over the faults the runs revealed, fewest crashes first.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::panic::AssertUnwindSafe;
use std::rc::Rc;
use std::time::Instant;

use hydro_lang::sim::compiled::quiesce;

use super::*;
use crate::check::RaceNotes;
use crate::fault::{Fate, NodeNames};
use crate::races::{self, PairClause, Pairs, SendOrder};
use crate::search::{CrashAt, HYPOTHESIS_BUDGET, Node, Tolerance, crash_count};

/// The largest fault set the fallback enumeration runs.
pub(super) const FALLBACK_FAULTS: usize = 2;

/// A pair: the node name of a member of the checked cluster, and a fact.
type Pair = (Node, Fact);

/// The payload of the panic that ends an exploration early (never a program panic).
struct EarlyStop;

/// How a fault set is explored.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Policy {
    /// Its first schedule only: every run of it has the same facts.
    One,
    /// Every schedule (or the sample), with early stopping.
    All,
}

/// What an exploration did.
struct Explored {
    schedules: usize,
    /// The callback stopped it.
    stopped: bool,
    /// The program panicked: the message and the last record.
    panic: Option<(String, Rc<InstanceRecord>)>,
}

/// What one schedule says about one label.
enum Reading {
    /// `POST` failed.
    Fails(Failure),
    /// `POST` held: a clause for every derivation of every held pair, and the domain facts some
    /// correct member holds.
    Holds {
        clauses: Vec<(Pair, PairClause<MessageId>)>,
        held: BTreeSet<Fact>,
    },
}

/// The faults and sends the runs revealed, shared by every label.
#[derive(Default)]
struct World {
    /// The brute-force enumerator's candidates (`check/brute_force.rs` `harvest`).
    universe: BTreeSet<Fault<MessageId>>,
    /// Every message each node sent before the heal.
    sends: BTreeMap<Node, BTreeSet<MessageId>>,
}

impl World {
    /// Adds what `record` reveals: `omit(m)` for every lossy message sent before the heal,
    /// `crash(n)` for every crashable node and `crash(n after m)` for every message it sent
    /// before the heal. Returns the sends it saw, by node name.
    fn harvest(&mut self, record: &InstanceRecord, tables: &Tables) -> Vec<(Node, MessageId)> {
        let names = tables.names();
        let crashable: BTreeSet<&NodeId> = names.crashable().iter().collect();
        for node in &crashable {
            self.universe
                .insert(Fault::crash_after(names.name(node), None));
        }
        let mut sent = vec![];
        for event in record.log.events() {
            match event {
                // Neither omissions nor new crashes fire after the heal.
                Event::Heal => break,
                Event::Sent { msg, fate, .. } if *fate != Fate::SenderDead => {
                    let from = names.name(&msg.from);
                    if crashable.contains(&msg.from) {
                        self.universe
                            .insert(Fault::crash_after(from.clone(), Some(msg.clone())));
                    }
                    if tables.is_faulty(msg.channel) {
                        self.universe.insert(Fault::Omit(msg.clone()));
                    }
                    self.sends
                        .entry(from.clone())
                        .or_default()
                        .insert(msg.clone());
                    sent.push((from, msg.clone()));
                }
                _ => {}
            }
        }
        sent
    }
}

/// One label's pairs and their clauses, without duplicates.
struct LabelPairs {
    pairs: Pairs<Pair, MessageId>,
    seen: BTreeSet<(Pair, PairClause<MessageId>)>,
}

impl LabelPairs {
    /// A pair, with no clause yet, for every member of `target` and every fact of `domain`.
    fn new(target: &Target, domain: &BTreeSet<Fact>, names: &NodeNames) -> Self {
        let mut pairs = Pairs::new();
        for &m in &target.members {
            let node = names.name(&NodeId::new(&target.cluster, Some(m)));
            for fact in domain {
                pairs.insert((node.clone(), fact.clone()), vec![]);
            }
        }
        LabelPairs {
            pairs,
            seen: BTreeSet::new(),
        }
    }

    fn learn(&mut self, clauses: impl IntoIterator<Item = (Pair, PairClause<MessageId>)>) {
        for (pair, clause) in clauses {
            if self.seen.insert((pair.clone(), clause.clone())) {
                self.pairs.entry(pair).or_default().push(clause);
            }
        }
    }
}

/// What one schedule's record says about `target`, against `domain`: lite's `outcome`, with
/// one clause per support of each held pair. `all_robust`: the program has no schedule choices,
/// so every derivation holds in its one schedule.
fn read_schedule(
    labels: &Labels,
    tables: &Tables,
    target: &Target,
    domain: &BTreeSet<Fact>,
    record: &Rc<InstanceRecord>,
    lineage: &Lineage<'_>,
    all_robust: bool,
) -> Reading {
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
    let divergent: Vec<Fact> = evaluation
        .outside_domain
        .iter()
        .filter(|fact| !correct.iter().all(|m| view.holds(*m, fact)))
        .cloned()
        .collect();
    if !evaluation.holds() || !divergent.is_empty() {
        return Reading::Fails(Failure {
            missing: evaluation.missing,
            divergent,
            panic: None,
            record: record.clone(),
        });
    }
    let names = tables.names();
    let mut clauses = vec![];
    for pair in &evaluation.held {
        let key = (
            names.name(&NodeId::new(&target.cluster, Some(pair.member))),
            pair.fact.clone(),
        );
        for &event in &pair.supports {
            let clause = PairClause {
                deps: (*lineage.derivation(event)).clone(),
                robust: all_robust || lineage.fragile_sites(event).is_empty(),
            };
            clauses.push((key.clone(), clause));
        }
    }
    Reading::Holds {
        clauses,
        held: evaluation.required,
    }
}

/// Whether every crash of `set` is a start crash (`crash(n)`, before any send), or a crash
/// point `allowed` accepts.
fn crashes_all(set: &FaultSet<MessageId>, allowed: impl Fn(&Node, &MessageId) -> bool) -> bool {
    set.iter().all(|f| match f {
        Fault::Crash {
            node,
            at: CrashAt::After(m),
        } => allowed(node, m),
        _ => true,
    })
}

/// The order of closed-broadcast fan-outs (`crate::provenance::fanout`): on a marked channel,
/// a node sends each payload to the destination cluster's members in increasing id order, so
/// `(v → j)` precedes `(v → k)` for `j < k`. Each `(channel, sender, payload)` is one chain.
#[derive(Debug, Default)]
pub(super) struct FanOutOrder {
    channels: BTreeSet<usize>,
}

impl FanOutOrder {
    /// The marked channels of `sim`.
    fn of(sim: &CompiledSim) -> Self {
        FanOutOrder {
            channels: sim
                .channels()
                .iter()
                .filter(|c| c.op.id == Some(crate::provenance::fanout::FANOUT_MARK))
                .map(|c| c.id)
                .collect(),
        }
    }
}

impl SendOrder<MessageId> for FanOutOrder {
    fn precedes(&self, earlier: &MessageId, later: &MessageId) -> bool {
        self.channels.contains(&later.channel)
            && earlier.channel == later.channel
            && earlier.from == later.from
            && earlier.bytes == later.bytes
            && earlier.to.location == later.to.location
            && matches!((earlier.to.member, later.to.member), (Some(j), Some(k)) if j < k)
    }
}

/// What the program is, for the races policy.
#[derive(Clone, Copy)]
struct Shape {
    /// No fragile step and no opaque island: every derivation is robust.
    fragile_free: bool,
    /// No schedule choice: one schedule per fault set.
    choice_free: bool,
    /// Some channel admits omissions.
    lossy: bool,
}

impl Shape {
    /// Every run of `set` has the same facts (Lemma R both ways), so explore one schedule, if
    /// the program has no choices, or is fragile-free and every crash of `set` is a start crash
    /// or `exact` (`races::Sweep::is_exact`): then the crash rule is exact for `set`, so a
    /// derivation one run shows survives the rule, and holds in every run.
    fn policy(
        self,
        set: &FaultSet<MessageId>,
        exact: impl Fn(&Node, &MessageId) -> bool,
    ) -> Policy {
        let one = self.choice_free && crashes_all(set, |_, _| false)
            || self.fragile_free && crashes_all(set, exact);
        if one { Policy::One } else { Policy::All }
    }
}

/// One label's search state, besides its sweep.
struct Search<'t> {
    target: &'t Target,
    pairs: LabelPairs,
    stats: Stats,
    notes: RaceNotes,
    /// A multi-schedule exploration ran on a sample to its end: no certificate.
    sampled: bool,
}

/// What exploring one set found.
struct SetOutcome {
    failure: Option<Failure>,
}

impl<B: AsyncFnMut()> Engine<'_, B> {
    /// Runs `faults` under `policy`, giving each schedule's record to `on_record` as it
    /// finishes; `on_record` returns `true` to stop the exploration (by a panic with an
    /// [`EarlyStop`] payload from the simulator's `after` callback). Records after the stop,
    /// including bolero's replay of the stopping schedule, are dropped.
    fn explore_races(
        &mut self,
        faults: &FaultSet<MessageId>,
        policy: Policy,
        on_record: impl FnMut(&Tables, Rc<InstanceRecord>) -> bool,
    ) -> Explored {
        let hook = Rc::new(FaultHook::new(
            &FaultPlan {
                faults: faults.clone(),
            },
            self.sim,
        ));
        self.sim.set_network_hook(hook.clone());
        let stopped = Cell::new(false);
        let schedules = Cell::new(0usize);
        let on_record = RefCell::new(on_record);
        let tables = &self.tables;
        let absorb = |records: Vec<InstanceRecord>| -> bool {
            for record in records {
                if stopped.get() {
                    break;
                }
                schedules.set(schedules.get() + 1);
                if (on_record.borrow_mut())(tables, Rc::new(record)) {
                    stopped.set(true);
                }
            }
            stopped.get()
        };
        let outcome = {
            let healer = AssertUnwindSafe(hook.clone());
            let mut body = AssertUnwindSafe(&mut *self.body);
            let thunk = async || {
                (*body)().await;
                // As lite's `execute`: faults are admitted while the body drives inputs and
                // while the work it caused settles; then the network heals.
                quiesce().await;
                healer.heal();
                quiesce().await;
            };
            let after = || {
                if absorb(hook.drain_instances()) {
                    std::panic::resume_unwind(Box::new(EarlyStop));
                }
            };
            let sim = self.sim;
            let sample = self.sample_schedules;
            std::panic::catch_unwind(AssertUnwindSafe(move || match (policy, sample) {
                (Policy::One, _) => sim.first_schedule(thunk),
                (Policy::All, Some(n)) => sim.fuzz_bounded_then(n, thunk, after),
                (Policy::All, None) => {
                    sim.exhaustive_then(thunk, after);
                }
            }))
        };
        self.sim.clear_network_hook();
        let rest = hook.drain_instances();
        let panic = match outcome {
            Ok(()) => {
                absorb(rest);
                None
            }
            Err(_) if stopped.get() => None,
            // The panicking schedule (and any replay of it) never reached `after`.
            Err(payload) => Some((
                panic_message(&*payload),
                Rc::new(rest.into_iter().last().unwrap_or_default()),
            )),
        };
        Explored {
            schedules: schedules.get(),
            stopped: stopped.get(),
            panic,
        }
    }

    /// Whether the program is fragile-free, choice-free, lossy.
    fn shape(&self) -> Shape {
        Shape {
            fragile_free: self.fragile.as_ref().is_some_and(Vec::is_empty)
                && self.islands.borrow().is_empty(),
            choice_free: self.choices().is_empty(),
            lossy: self
                .sim
                .channels()
                .iter()
                .any(|c| self.tables.is_faulty(c.id)),
        }
    }

    /// The races loop over every label (see the [module docs](self)).
    pub(super) fn check_races(&mut self) -> Results {
        let start = Instant::now();
        let (mut verdicts, targets) = self.label_targets();
        let shape = self.shape();
        let labels = self.labels;
        let mut world = World::default();

        // Hypothesis 0: the failure-free probe, one schedule if every run has the same facts.
        let policy = shape.policy(&FaultSet::new(), |_, _| false);
        let mut domains: BTreeMap<usize, BTreeSet<Fact>> = BTreeMap::new();
        let mut learned: BTreeMap<usize, Vec<(Pair, PairClause<MessageId>)>> = BTreeMap::new();
        let mut failures: BTreeMap<usize, Failure> = BTreeMap::new();
        let probe = self.explore_races(&FaultSet::new(), policy, |tables, record| {
            world.harvest(&record, tables);
            let lineage = Lineage::new(&record.log, tables);
            for t in &targets {
                // As lite's probe: each schedule is read against its own correct members' facts.
                let own = record_domain(labels, t, &record);
                domains
                    .entry(t.label)
                    .or_default()
                    .extend(own.iter().cloned());
                let reading = read_schedule(
                    labels,
                    tables,
                    t,
                    &own,
                    &record,
                    &lineage,
                    shape.choice_free,
                );
                match reading {
                    Reading::Fails(failure) => {
                        failures.entry(t.label).or_insert(failure);
                    }
                    Reading::Holds { clauses, .. } => {
                        learned.entry(t.label).or_default().extend(clauses);
                    }
                }
            }
            false
        });
        let probe_sampled = policy == Policy::All && self.sample_schedules.is_some();
        let probe_stats = Stats {
            hypotheses: 1,
            schedules: probe.schedules,
            wall: start.elapsed(),
            races: Some(RaceNotes {
                single_schedule_sets: usize::from(policy == Policy::One),
                // Only certificates certify Claim B (see `search_races`).
                claim_b: false,
                ..RaceNotes::default()
            }),
        };
        if let Some((message, record)) = &probe.panic {
            for t in &targets {
                failures
                    .entry(t.label)
                    .or_insert_with(|| Failure::panic(message, record.clone()));
            }
        }

        for mut target in targets {
            target.domain = domains.remove(&target.label).unwrap_or_default();
            let failure = failures.remove(&target.label);
            let result = if let Some(failure) = failure {
                let verdict = counterexample(FaultSet::new(), None, !probe_sampled, 0, &failure);
                (verdict, probe_stats)
            } else if target.domain.is_empty() {
                (Verdict::Vacuous(VacuousReason::EmptyDomain), probe_stats)
            } else {
                let clauses = learned.remove(&target.label).unwrap_or_default();
                self.search_races(
                    &target,
                    clauses,
                    probe_stats,
                    probe_sampled,
                    shape,
                    &mut world,
                )
            };
            verdicts[target.label] = Some(result);
        }
        self.write_diagrams(&mut verdicts);
        self.results(verdicts)
    }

    /// One label's search, from the probe's clauses.
    fn search_races(
        &mut self,
        target: &Target,
        probe_clauses: Vec<(Pair, PairClause<MessageId>)>,
        probe_stats: Stats,
        probe_sampled: bool,
        shape: Shape,
        world: &mut World,
    ) -> (Verdict, Stats) {
        let start = Instant::now();
        let mut search = Search {
            target,
            pairs: LabelPairs::new(target, &target.domain, self.tables.names()),
            stats: probe_stats,
            notes: probe_stats.races.unwrap_or_default(),
            sampled: probe_sampled,
        };
        search.pairs.learn(probe_clauses);
        let mut sweep: races::Sweep<MessageId, Fact> =
            races::Sweep::new(self.bounds(target)).with_order(Rc::new(FanOutOrder::of(self.sim)));
        for (node, sent) in &world.sends {
            sweep.saw_sends(node, sent.iter().cloned());
        }
        if shape.fragile_free {
            // The probe showed every send (`races::Sweep::sends_complete`).
            sweep.sends_complete();
        }
        let mut fell_back = None;
        let verdict = loop {
            match sweep.next(&search.pairs.pairs) {
                races::Next::Try(set) => {
                    let outcome = self.explore_set(&mut search, &mut sweep, world, &set, shape);
                    if let Some(failure) = outcome.failure {
                        break counterexample(
                            set,
                            sweep.tolerated(),
                            !search.sampled,
                            search.stats.hypotheses - 1,
                            &failure,
                        );
                    }
                }
                races::Next::Certified(tolerates) if search.sampled => {
                    break Verdict::Exhausted {
                        tolerates,
                        sites: target.sites.clone(),
                    };
                }
                races::Next::Certified(tolerates) => break Verdict::Certified { tolerates },
                races::Next::Budget => {
                    break Verdict::Budget {
                        tolerates: sweep.tolerated(),
                    };
                }
                races::Next::Unconstrained(_) => {
                    let (verdict, skipped) = self.fallback(&mut search, &mut sweep, world, shape);
                    fell_back = Some(skipped);
                    break verdict;
                }
            }
        };
        let enumerated_everything = fell_back
            .is_some_and(|skipped| skipped == 0 && search.notes.early_stops == 0)
            && !search.sampled;
        search.notes.claim_b = matches!(verdict, Verdict::Certified { .. })
            && (shape.fragile_free || shape.choice_free || enumerated_everything);
        search.stats.races = Some(search.notes);
        search.stats.wall += start.elapsed();
        (verdict, search.stats)
    }

    /// Explores `set` for the label of `search` (see the [module docs](self)), learning its
    /// clauses, and marks its lost facts dead where that is sound.
    fn explore_set(
        &mut self,
        search: &mut Search<'_>,
        sweep: &mut races::Sweep<MessageId, Fact>,
        world: &mut World,
        set: &FaultSet<MessageId>,
        shape: Shape,
    ) -> SetOutcome {
        let policy = shape.policy(set, |n, m| sweep.is_exact(n, m));
        let labels = self.labels;
        let target = search.target;
        let names = self.tables.names();
        let members: BTreeSet<Node> = target
            .members
            .iter()
            .map(|m| names.name(&NodeId::new(&target.cluster, Some(*m))))
            .collect();
        let pairs = &mut search.pairs;
        let mut failure = None;
        let mut pruned = false;
        let mut held_somewhere: BTreeSet<Fact> = BTreeSet::new();
        let explored = self.explore_races(set, policy, |tables, record| {
            for (node, msg) in world.harvest(&record, tables) {
                sweep.saw_sends(&node, [msg]);
            }
            let lineage = Lineage::new(&record.log, tables);
            match read_schedule(
                labels,
                tables,
                target,
                &target.domain,
                &record,
                &lineage,
                shape.choice_free,
            ) {
                Reading::Fails(f) => {
                    failure = Some(f);
                    true
                }
                Reading::Holds { clauses, held } => {
                    pairs.learn(clauses);
                    held_somewhere.extend(held);
                    if policy == Policy::All
                        && races::pruned(set, &pairs.pairs, sweep.dead(), sweep.order())
                    {
                        pruned = true;
                    }
                    pruned
                }
            }
        });
        search.stats.hypotheses += 1;
        search.stats.schedules += explored.schedules;
        if let Some((message, record)) = &explored.panic {
            failure.get_or_insert_with(|| Failure::panic(message, record.clone()));
        }
        match policy {
            Policy::One => search.notes.single_schedule_sets += 1,
            Policy::All if pruned => search.notes.early_stops += 1,
            Policy::All => {}
        }
        if policy == Policy::All
            && self.sample_schedules.is_some()
            && !explored.stopped
            && failure.is_none()
        {
            search.sampled = true;
        }
        // A fragile-free program, a set of omissions, start crashes and exact crash points of
        // nodes outside the cluster, its one schedule read in full: a domain fact no correct
        // member holds is held by none under any superset either (`races::Sweep::mark_dead`).
        let deadly = crashes_all(set, |n, m| !members.contains(n) && sweep.is_exact(n, m));
        if failure.is_none() && policy == Policy::One && shape.fragile_free && deadly {
            for fact in &target.domain {
                if !held_somewhere.contains(fact) {
                    sweep.mark_dead(set.clone(), fact.clone());
                }
            }
        }
        SetOutcome { failure }
    }

    /// The fallback enumeration (see the [module docs](self)): every admissible set of at most
    /// [`FALLBACK_FAULTS`] faults over the harvested universe, fewest crashes first, skipping
    /// those tried or pruned. Returns the verdict, and how many sets pruning skipped.
    fn fallback(
        &mut self,
        search: &mut Search<'_>,
        sweep: &mut races::Sweep<MessageId, Fact>,
        world: &mut World,
        shape: Shape,
    ) -> (Verdict, usize) {
        let bounds = self.bounds(search.target);
        let collect = |universe: &BTreeSet<Fault<MessageId>>| -> Vec<FaultSet<MessageId>> {
            let faults: Vec<&Fault<MessageId>> = universe.iter().collect();
            let mut sets: Vec<FaultSet<MessageId>> = vec![];
            for (i, a) in faults.iter().enumerate() {
                sets.push(FaultSet::from([(*a).clone()]));
                if FALLBACK_FAULTS >= 2 {
                    for b in &faults[i + 1..] {
                        sets.push(FaultSet::from([(*a).clone(), (*b).clone()]));
                    }
                }
            }
            sets.retain(|s| races::within_bounds(s, &bounds));
            sets.sort_by_cached_key(|s| (crash_count(s), s.len(), s.clone()));
            sets
        };
        let mut queue = collect(&world.universe);
        let mut known = world.universe.len();
        let mut next = 0;
        let mut skipped = 0;
        // The breaking set with the fewest crashes so far, its failure and hypothesis index.
        let mut best: Option<(FaultSet<MessageId>, Failure, usize)> = None;
        loop {
            if world.universe.len() != known {
                queue = collect(&world.universe);
                known = world.universe.len();
                next = 0;
            }
            let Some(set) = queue.get(next).cloned() else {
                break;
            };
            next += 1;
            if best
                .as_ref()
                .is_some_and(|(b, _, _)| crash_count(&set) >= crash_count(b))
            {
                break;
            }
            if sweep.tried().contains(&set) {
                continue;
            }
            if races::pruned(&set, &search.pairs.pairs, sweep.dead(), sweep.order()) {
                skipped += 1;
                continue;
            }
            if search.stats.hypotheses >= HYPOTHESIS_BUDGET {
                return (
                    Verdict::Budget {
                        tolerates: sweep.tolerated(),
                    },
                    skipped,
                );
            }
            sweep.mark_tried(set.clone());
            search.notes.enumerated_sets += 1;
            let outcome = self.explore_set(search, sweep, world, &set, shape);
            if let Some(failure) = outcome.failure {
                best = Some((set, failure, search.stats.hypotheses - 1));
            }
        }
        let cap = self
            .max_crashes
            .unwrap_or(FALLBACK_FAULTS)
            .min(FALLBACK_FAULTS);
        let verdict = if let Some((set, failure, hypothesis)) = best {
            let tolerates = crash_count(&set).checked_sub(1);
            // Sets with fewer crashes but more than `FALLBACK_FAULTS` faults (omissions) were
            // not enumerated, unless the sweep had already cleared their bound.
            let certified = !search.sampled
                && (tolerates.is_none() || !shape.lossy || sweep.tolerated() >= tolerates);
            counterexample(set, tolerates, certified, hypothesis, &failure)
        } else {
            // With no lossy channel every fault is a crash. If no admissible set can crash more
            // than `FALLBACK_FAULTS` nodes, the enumeration covered every admissible set.
            let names = self.tables.names();
            let crashable = names.crashable().len();
            let most = match &bounds.checked {
                Some(checked) => {
                    let inside = names
                        .crashable()
                        .iter()
                        .filter(|n| checked.nodes.contains(&names.name(n)))
                        .count();
                    crashable - inside + inside.min(checked.cap)
                }
                None => crashable,
            };
            // Never every node (the vacuity bound); the `max_crashes` cap is not counted, so
            // that `All` means what it means for the sweep.
            let most = most.min(bounds.nodes.saturating_sub(1));
            let tolerates = if !shape.lossy && most <= FALLBACK_FAULTS {
                Tolerance::All
            } else {
                Tolerance::UpTo(sweep.tolerated().map_or(cap, |t| t.max(cap)))
            };
            if shape.lossy || search.sampled {
                Verdict::Exhausted {
                    tolerates,
                    sites: search.target.sites.clone(),
                }
            } else {
                Verdict::Certified { tolerates }
            }
        };
        (verdict, skipped)
    }
}
