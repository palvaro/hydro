//! The search behind [`EcCheck::run`] (docs/design/hydrofrac-races.md §4.4, §5, §12).
//!
//! Each label keeps one clause per derivation of each `(member, fact)` pair, with the premises of
//! the fragile steps it passed ([`crate::races`]). A fault set is skipped only when, for every
//! pair, a robust derivation survives it (Lemma R). Every set the search does run is explored
//! over its schedules, and stops early as soon as the derivations learned cover every pair:
//! - one schedule, if every run of the set has the same facts: the program is fragile-free (or
//!   has no schedule choices) and every crash literal the set touches is proved;
//! - otherwise every schedule (or the sample), stopping early.
//!
//! Claim A (POST) is certified when the hitting-set program has no untried solution over the
//! faults the runs revealed ([`races::Sweep`]). There is no fallback and no size bound beyond the
//! crash bound: a pair no robust derivation protects makes the program propose every set. Claim B
//! (no divergence outside the domain) is then proved for a fragile-free or choice-free program,
//! and otherwise checked by a second sweep that runs every set within the bounds in full
//! ([`races::Sweep::check_claim_b`]); only then is the label certified.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet};
use std::panic::AssertUnwindSafe;
use std::rc::Rc;
use std::time::Instant;

use super::*;
use crate::check::RaceNotes;
use crate::fault::{Fate, NodeNames};
use crate::races::{self, PairClause, Pairs};
use crate::search::{CrashAt, Node};

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
    /// It ran past the schedule budget (`explore_schedules`), so its schedules were sampled.
    sampled: bool,
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

/// Panics if bolero would cut an exhaustive exploration short without saying so. With
/// `BOLERO_RANDOM_TEST_TIME_MS` set, bolero's `run_exhaustive` stops at the time limit and
/// returns normally, so an exploration would look complete when it is not (and a certificate
/// built on it would be wrong). Exhaustive runs ignore bolero's other limits.
pub(super) fn refuse_truncated_exhaustive() {
    if let Ok(limit) = std::env::var("BOLERO_RANDOM_TEST_TIME_MS") {
        panic!(
            "hydrofrac: BOLERO_RANDOM_TEST_TIME_MS={limit} would silently cut the exhaustive \
             exploration short; unset it"
        );
    }
}

/// What one run revealed: the faults it makes possible (`check/brute_force.rs` `harvest`), and
/// each crashable node's sends before the heal, with their positions among that node's sends.
#[derive(Default)]
struct Revealed {
    faults: Vec<Fault<MessageId>>,
    sends: Vec<(Node, MessageId, u64)>,
}

impl Revealed {
    /// `omit(m)` for every lossy message sent before the heal, `crash(n)` for every crashable
    /// node and `crash(n after m)` for every message it sent before the heal.
    fn of(record: &InstanceRecord, tables: &Tables) -> Self {
        let names = tables.names();
        let crashable: BTreeSet<&NodeId> = names.crashable().iter().collect();
        let mut out = Revealed::default();
        for node in &crashable {
            out.faults.push(Fault::crash_after(names.name(node), None));
        }
        let mut positions: BTreeMap<Node, u64> = BTreeMap::new();
        for event in record.log.events() {
            match event {
                // Neither omissions nor new crashes fire after the heal.
                Event::Heal => break,
                Event::Sent { msg, fate, .. } if *fate != Fate::SenderDead => {
                    if tables.is_faulty(msg.channel) {
                        out.faults.push(Fault::Omit(msg.clone()));
                    }
                    if crashable.contains(&msg.from) {
                        let from = names.name(&msg.from);
                        out.faults
                            .push(Fault::crash_after(from.clone(), Some(msg.clone())));
                        let at = positions.entry(from.clone()).or_default();
                        out.sends.push((from, msg.clone(), *at));
                        *at += 1;
                    }
                }
                _ => {}
            }
        }
        out
    }

    /// Tells `sweep` what this run revealed.
    fn tell(&self, sweep: &mut races::Sweep<MessageId, Fact>) {
        for f in &self.faults {
            sweep.saw_fault(f.clone());
        }
        for (node, m, at) in &self.sends {
            sweep.saw_send(node, m.clone(), *at);
        }
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

/// What one schedule's record says about `target`, against `domain`, with one clause per
/// support of each held pair. `all_robust`: the program has no schedule choices,
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
            // In a choice-free program each fault set has one schedule, so a derivation holds
            // in it and needs no premise, unless it counts copies: the kill rule reads contents,
            // which serves a set-preserving step, but not a fold, scan or `enumerate` that saw a
            // later copy (docs/design/hydrofrac-soundness.md §3.1).
            let deps = (*lineage.derivation(event)).clone();
            let premises = if all_robust && deps.iter().all(|d| d.message.copy == 0) {
                BTreeSet::new()
            } else {
                (*lineage.fragile_sites(event)).clone()
            };
            let clause = PairClause { deps, premises };
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

/// The proved send order of `sim`: its marked closed-broadcast channels, and which of them are
/// sequenced ([`races::FanOutOrder`]); the ranks are learned from the probe.
fn fan_out_order(sim: &CompiledSim) -> races::FanOutOrder {
    use crate::provenance::fanout::{FANOUT_MARK, SEQUENCED_MARK};
    let marked = |mark| {
        sim.channels()
            .iter()
            .filter(move |c| c.op.id == Some(mark))
            .map(|c| c.id)
    };
    races::FanOutOrder::new(marked(FANOUT_MARK), marked(SEQUENCED_MARK))
}

/// What the program is, for the races policy.
#[derive(Clone, Copy)]
struct Shape {
    /// No fragile step and no opaque island: every derivation is robust.
    fragile_free: bool,
    /// No schedule choice: one schedule per fault set.
    choice_free: bool,
}

impl Shape {
    /// Every run of `set` has the same facts (Lemma R both ways), so explore one schedule, if
    /// the program has no choices, or is fragile-free and every fault of `set` is `exact`
    /// (`races::Sweep::is_exact_set`: an omission of a content's only copy or of a later copy,
    /// a start crash, or a crash point whose every literal is proved). Then the kill rule is
    /// exact for `set`, so a derivation one run shows survives the rule, and holds in every run.
    fn policy(
        self,
        set: &FaultSet<MessageId>,
        exact: impl Fn(&FaultSet<MessageId>) -> bool,
    ) -> Policy {
        let one =
            self.choice_free && crashes_all(set, |_, _| false) || self.fragile_free && exact(set);
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

impl<B: AsyncFnMut(), R: AsyncFnMut()> Engine<'_, B, R> {
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
        let sampled = Cell::new(false);
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
            let mut inputs = AssertUnwindSafe(&mut *self.inputs);
            let mut recover = AssertUnwindSafe(&mut *self.recover);
            let thunk = async || drive(&mut *inputs, &mut *recover, &healer).await;
            let after = || {
                let records = hook.drain_instances();
                let sends = records.iter().map(|r| r.log.sends().count()).sum();
                if absorb(records) {
                    std::panic::resume_unwind(Box::new(EarlyStop));
                }
                sends
            };
            let (sim, over) = (self.sim, &self.over_budget);
            let discard = || drop(hook.drain_instances());
            std::panic::catch_unwind(AssertUnwindSafe(|| match policy {
                Policy::One => sim.first_schedule(thunk),
                Policy::All => {
                    sampled.set(explore_schedules(sim, over, thunk, after, discard));
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
                program_panic(&*payload),
                Rc::new(rest.into_iter().last().unwrap_or_default()),
            )),
        };
        Explored {
            schedules: schedules.get(),
            stopped: stopped.get(),
            sampled: sampled.get(),
            panic,
        }
    }

    /// Whether the program is fragile-free, choice-free.
    fn shape(&self) -> Shape {
        Shape {
            fragile_free: self.fragile.as_ref().is_some_and(Vec::is_empty)
                && self.islands.borrow().is_empty(),
            choice_free: self.choices().is_empty(),
        }
    }

    /// The races loop over every label (see the [module docs](self)).
    pub(super) fn check(&mut self) -> Results {
        let start = Instant::now();
        let (mut verdicts, targets) = self.label_targets();
        let shape = self.shape();
        let labels = self.labels;
        // What every run revealed, for every label's sweep.
        let mut world: Vec<Revealed> = vec![];

        // Hypothesis 0: the failure-free probe, one schedule if every run has the same facts.
        let policy = shape.policy(&FaultSet::new(), |set| set.is_empty());
        let mut domains: BTreeMap<usize, BTreeSet<Fact>> = BTreeMap::new();
        let mut learned: BTreeMap<usize, Vec<(Pair, PairClause<MessageId>)>> = BTreeMap::new();
        let mut failures: BTreeMap<usize, Failure> = BTreeMap::new();
        let mut order = fan_out_order(self.sim);
        let probe = self.explore_races(&FaultSet::new(), policy, |tables, record| {
            order.learn(record.log.sends().map(|(_, m, _, _)| m));
            world.push(Revealed::of(&record, tables));
            let lineage = Lineage::new(&record.log, tables);
            for t in &targets {
                // Each schedule is read against its own correct members' facts (as the oracle reads it).
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
        let probe_sampled = probe.sampled;
        // A sampled probe counts as run in full too (a sampled search certifies nothing).
        let probe_in_full = policy == Policy::All || shape.choice_free;
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
            // Each target's, below.
            domain: 0,
        };
        if let Some((message, record)) = &probe.panic {
            for t in &targets {
                failures
                    .entry(t.label)
                    .or_insert_with(|| Failure::panic(message, record.clone()));
            }
        }

        self.order = Rc::new(order);
        for mut target in targets {
            target.domain = domains.remove(&target.label).unwrap_or_default();
            let probe_stats = Stats {
                domain: target.domain.len(),
                ..probe_stats
            };
            let failure = failures.remove(&target.label);
            let result = if let Some(failure) = failure {
                let verdict = counterexample(FaultSet::new(), None, !probe_sampled, 0, &failure);
                (verdict, probe_stats)
            } else if target.domain.is_empty()
                && (shape.fragile_free || shape.choice_free)
                && !probe_sampled
            {
                // No fault creates a fact outside the domain (Claim B, §13 of
                // docs/design/hydrofrac-soundness.md), so no run holds a fact at all. Otherwise
                // faults may create facts (a failover's output), and the search below runs every
                // set within the bounds to check them (`races::Sweep::next`, no pairs).
                (Verdict::Vacuous(VacuousReason::EmptyDomain), probe_stats)
            } else {
                // Each label's search is sampled from its first set past the budget on (or from
                // the start, if the probe was).
                self.over_budget.set(probe_sampled);
                let clauses = learned.remove(&target.label).unwrap_or_default();
                self.search_races(
                    &target,
                    clauses,
                    probe_stats,
                    (probe_sampled, probe_in_full),
                    shape,
                    &mut world,
                )
            };
            verdicts[target.label] = Some(result);
        }
        self.write_diagrams(&mut verdicts);
        self.results(verdicts)
    }

    /// One label's search, from the probe's clauses. `probe`: whether the probe was sampled, and
    /// whether it ran every schedule.
    fn search_races(
        &mut self,
        target: &Target,
        probe_clauses: Vec<(Pair, PairClause<MessageId>)>,
        probe_stats: Stats,
        (probe_sampled, probe_in_full): (bool, bool),
        shape: Shape,
        world: &mut Vec<Revealed>,
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
            races::Sweep::new(self.bounds(target)).with_order(self.order.clone());
        for revealed in world.iter() {
            revealed.tell(&mut sweep);
        }
        if shape.fragile_free {
            // The probe showed every send (`races::Sweep::sends_complete`).
            sweep.sends_complete();
        }
        if probe_in_full {
            sweep.explored_in_full(FaultSet::new());
        }
        let verdict = loop {
            match sweep.next(&search.pairs.pairs) {
                races::Next::Try { set, in_full } => {
                    let failure =
                        self.explore_set(&mut search, &mut sweep, world, &set, shape, in_full);
                    if let Some(failure) = failure {
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
                // Claim A holds. Claim B (no divergence outside the domain) holds too in a
                // fragile-free or choice-free program, where faults create no fact outside the
                // domain (§13 of docs/design/hydrofrac-soundness.md). Otherwise check it: run
                // every set within the bounds in full.
                races::Next::Certified(_)
                    if !(shape.fragile_free
                        || shape.choice_free
                        || sweep.checks_claim_b(&search.pairs.pairs)) =>
                {
                    sweep.check_claim_b();
                }
                races::Next::Certified(tolerates) => break Verdict::Certified { tolerates },
                races::Next::Budget => {
                    break Verdict::Budget {
                        tolerates: sweep.tolerated(),
                    };
                }
            }
        };
        // A certificate covers Claim B too: the loop above checks it before certifying.
        search.notes.claim_b = matches!(verdict, Verdict::Certified { .. });
        search.stats.races = Some(search.notes);
        search.stats.wall += start.elapsed();
        (verdict, search.stats)
    }

    /// Explores `set` for the label of `search` (see the [module docs](self)), learning its
    /// clauses and what its runs reveal. With `in_full`, every schedule, without stopping early.
    /// Returns the failure, if a schedule failed.
    fn explore_set(
        &mut self,
        search: &mut Search<'_>,
        sweep: &mut races::Sweep<MessageId, Fact>,
        world: &mut Vec<Revealed>,
        set: &FaultSet<MessageId>,
        shape: Shape,
        in_full: bool,
    ) -> Option<Failure> {
        let policy = if in_full {
            Policy::All
        } else {
            shape.policy(set, |set| sweep.is_exact_set(set, |_, _| true))
        };
        let labels = self.labels;
        let target = search.target;
        let names = self.tables.names();
        let members: BTreeSet<Node> = target
            .members
            .iter()
            .map(|m| names.name(&NodeId::new(&target.cluster, Some(*m))))
            .collect();
        let order = self.order.clone();
        let pairs = &mut search.pairs;
        let mut failure = None;
        let mut pruned = false;
        let mut held_somewhere: BTreeSet<Fact> = BTreeSet::new();
        let explored = self.explore_races(set, policy, |tables, record| {
            order.check(record.log.sends().map(|(_, m, _, _)| m));
            let revealed = Revealed::of(&record, tables);
            revealed.tell(sweep);
            world.push(revealed);
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
                        && !in_full
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
        if explored.sampled && !explored.stopped && failure.is_none() {
            search.sampled = true;
        }
        if failure.is_none() && !explored.stopped && (policy == Policy::All || shape.choice_free) {
            // Every schedule was run, or a sample of them: a sampled search certifies nothing
            // (`Verdict::Exhausted`), so counting the sample as the set's runs only spares
            // re-running it for each unseen crash.
            sweep.explored_in_full(set.clone());
        }
        // A fragile-free program, a set of omissions, start crashes and exact crash points of
        // nodes outside the cluster, its one schedule read in full: a domain fact no correct
        // member holds is held by none under any superset either (`races::Sweep::mark_dead`).
        let deadly = sweep.is_exact_set(set, |n, _| !members.contains(n));
        if failure.is_none() && policy == Policy::One && shape.fragile_free && deadly {
            for fact in &target.domain {
                if !held_somewhere.contains(fact) {
                    sweep.mark_dead(set.clone(), fact.clone());
                }
            }
        }
        failure
    }
}
