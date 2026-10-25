//! Two-phase commit under hydrofrac's `EcCheck` (docs/design/hydrofrac-lite.md §7): every
//! `EventualConsistency` label of [`two_pc`](super::two_pc_ctp::two_pc), with or without
//! cooperative termination, every node crashable, crash tolerance swept from zero upwards.
//!
//! The labels are the `prepare`, `commit` and `peer` broadcasts (labelled by their transport)
//! and the participants' `committed` transactions (labelled as a claim: that is agreement).
//!
//! # The harness
//!
//! The fault phase sends one transaction. With *racing* timers it also fires every
//! participant's timer, with no barrier: a timer may fire before, during or after the
//! transaction reaches its participant, and crashes may strike anywhere in the exchange.
//! The recovery phase ([`hydrofrac::EcCheck::run_then_recover`]) models the ticks after the
//! faults stop: every participant's timer fires `ticks` more times, each once the previous one
//! settled. A participant's timer is recurring: while it is uncertain, every tick re-sends its
//! query (`two_pc_ctp`'s `queries`). With racing timers and no recovery tick the timer fires
//! once, and a query that reached a peer before the peer decided is never repeated.
//!
//! Racing timers explode the schedule space (one racing timer over two participants passed 10
//! minutes of exhaustive exploration without finishing), so the check samples them. Timers that
//! fire only in the recovery phase are exhaustive, but certify a narrower claim: no crash strikes
//! the peer exchange.

use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
use hydro_lang::prelude::*;
#[cfg(stageleft_runtime)]
use hydro_lang::sim::compiled::CompiledSim;
#[cfg(stageleft_runtime)]
use hydro_lang::sim::{SimClusterSender, SimSender};

use super::two_pc_ctp::{Coordinator, Participant, Txn, two_pc};

/// The timer ticks of the recovery phase. One is enough: after the faults stop and the run
/// settles, a query reaches every correct peer, and one that has decided answers it.
#[cfg(stageleft_runtime)]
const RECOVERY_TICKS: usize = 1;

/// Schedules per fault set the oracles sample in a cross-check with racing timers (the 64 they
/// sampled when the check took a sample size). Racing CTP's schedule space is past what the
/// oracles can enumerate: with two participants the failure-free run alone passed 31,957
/// schedules in 10 minutes without finishing, and with three and a recovery tick it has more
/// than 223,000. So the cross-checks that use this are weaker: a sampled oracle misses what
/// its sample misses.
#[cfg(stageleft_runtime)]
pub(super) const RACING_ORACLE_SAMPLES: usize = 64;

/// 2PC (`ctp`: with cooperative termination) compiled for a check, with its inputs.
#[cfg(stageleft_runtime)]
pub(super) struct TwoPc {
    check: hydrofrac::EcCheck,
    sim: CompiledSim,
    txns: SimSender<Txn, TotalOrder, ExactlyOnce>,
    timeouts: SimClusterSender<(), TotalOrder, ExactlyOnce>,
    participants: u32,
}

#[cfg(stageleft_runtime)]
impl TwoPc {
    fn new(ctp: bool, participants: u32) -> Self {
        TwoPc::with(hydrofrac::EcCheck::new(), ctp, participants)
    }

    /// The program, for `check`.
    pub(super) fn with(mut check: hydrofrac::EcCheck, ctp: bool, participants: u32) -> Self {
        let mut flow = FlowBuilder::new();
        let coordinator = flow.process::<Coordinator>();
        let cluster = flow.cluster::<Participant>();
        let (txns, txn_stream) = coordinator.sim_input::<Txn, TotalOrder, ExactlyOnce>();
        let (timeouts, timeout_stream) = cluster.sim_input::<(), TotalOrder, ExactlyOnce>();
        let _committed = two_pc(
            &coordinator,
            &cluster,
            participants as usize,
            txn_stream,
            timeout_stream,
            ctp,
            nondet!(/** explored by the check */),
            nondet!(/** explored by the check */),
            nondet!(/** explored by the check */),
            nondet!(/** explored by the check */),
            nondet!(/** explored by the check */),
        )
        .sim_cluster_output();
        let sim = check
            .instrument(
                flow.sim()
                    .with_cluster_size(&cluster, participants as usize),
            )
            .compiled();
        TwoPc {
            check,
            sim,
            txns,
            timeouts,
            participants,
        }
    }

    /// The harness (see the [module docs](self)): `racing` timers, and `ticks` recovery ticks.
    fn phases(
        &self,
        racing: bool,
        ticks: usize,
    ) -> (impl AsyncFnMut() + '_, impl AsyncFnMut() + '_) {
        let all = move || (0..self.participants).map(|p| (p, ()));
        let inputs = async move || {
            self.txns.send(Txn(1));
            if racing {
                self.timeouts.send_many(all());
            }
        };
        let recover = async move || {
            for _ in 0..ticks {
                self.timeouts.send_many(all());
                hydro_lang::sim::compiled::quiesce().await;
            }
        };
        (inputs, recover)
    }

    /// The check, with `racing` timers and `ticks` recovery ticks.
    fn run(&self, racing: bool, ticks: usize) -> hydrofrac::Results {
        let (inputs, recover) = self.phases(racing, ticks);
        let start = std::time::Instant::now();
        let results = self.check.run_then_recover(&self.sim, inputs, recover);
        eprintln!(
            "2PC ({} participants, racing timers: {racing}, {ticks} recovery ticks): {:.1}s\n{results}",
            self.participants,
            start.elapsed().as_secs_f64()
        );
        results
    }

    /// The check cross-checked by brute force over every set of at most `max_faults` faults,
    /// with `racing` timers and `ticks` recovery ticks. The oracles run `oracle_samples`
    /// seeded schedules per set if set (see [`RACING_ORACLE_SAMPLES`]), else every schedule.
    pub(super) fn cross_check(
        &self,
        racing: bool,
        ticks: usize,
        max_faults: usize,
        oracle_samples: Option<usize>,
    ) -> hydrofrac::check::CrossCheck {
        let (inputs, recover) = self.phases(racing, ticks);
        let start = std::time::Instant::now();
        let cross = self.check.cross_check_sampling_oracles(
            &self.sim,
            inputs,
            recover,
            max_faults,
            oracle_samples,
        );
        eprintln!(
            "2PC ({} participants, racing timers: {racing}, {ticks} recovery ticks, cross-checked \
             up to {max_faults} faults): {:.1}s\nsearch:\n{}\n{cross}",
            self.participants,
            start.elapsed().as_secs_f64(),
            cross.results
        );
        cross
    }
}

/// The label of the participants' committed transactions (the agreement claim). It is the
/// function's return value, `committed.assert_has_consistency_of(..)`, so the report names it
/// by its receiver, `committed`.
#[cfg(stageleft_runtime)]
fn committed(results: &hydrofrac::Results) -> &hydrofrac::check::LabelResult {
    let mut found = results.results().iter().filter(|l| {
        l.plan.collection.display_name() == "committed"
            && l.plan.collection.op == "AssertIsConsistent"
    });
    let label = found
        .next()
        .unwrap_or_else(|| panic!("no committed label in\n{results}"));
    assert!(found.next().is_none(), "{results}");
    label
}

/// `label` breaks with exactly one crash, of the coordinator mid fan-out, after a certified
/// zero-crash bound.
#[cfg(stageleft_runtime)]
fn breaks_at_one_coordinator_crash(
    results: &hydrofrac::Results,
    label: &hydrofrac::check::LabelResult,
) {
    use hydrofrac::search::{CrashAt, Fault};
    let hydrofrac::Verdict::Counterexample(cex) = &label.verdict else {
        panic!("{}: expected a counterexample\n{results}", label.name());
    };
    assert_eq!(cex.tolerates, Some(0), "{}\n{results}", label.name());
    let [Fault::Crash { node, at }] = cex.faults.iter().collect::<Vec<_>>()[..] else {
        panic!("{}: one crash\n{results}", label.name());
    };
    assert!(
        node.starts_with("Coordinator"),
        "{}\n{results}",
        label.name()
    );
    assert!(
        matches!(at, CrashAt::After(_)),
        "{}\n{results}",
        label.name()
    );
    assert!(!cex.missing.is_empty(), "{}\n{results}", label.name());
}

/// Plain 2PC blocks: the coordinator dies after its `commit` to some participants, and the
/// others never commit, however often their timers fire. The `prepare` and `commit` hops
/// break the same way.
#[test]
fn plain_two_pc_breaks_at_one_coordinator_crash() {
    let results = TwoPc::new(false, 2).run(true, RECOVERY_TICKS);
    breaks_at_one_coordinator_crash(&results, results.label("prepares"));
    breaks_at_one_coordinator_crash(&results, results.label("commits"));
    breaks_at_one_coordinator_crash(&results, committed(&results));
}

/// Direct injection, bypassing the search: the coordinator dies right after its `commit` to
/// participant 0, and participant 0 dies right after its first `Decided` to participant 1.
/// Counts the sampled schedules in which participant 1 decides and participant 2 does not.
#[cfg(stageleft_runtime)]
fn inject_two_crashes(samples: usize) -> (usize, usize) {
    use std::collections::BTreeSet;
    use std::rc::Rc;

    use hydrofrac::fault::{Event, FaultHook, FaultPlan, MessageId, NodeNames};
    use hydrofrac::search::Fault;

    const N: u32 = 3;
    let mut flow = FlowBuilder::new();
    let coordinator = flow.process::<Coordinator>();
    let participants = flow.cluster::<Participant>();
    let (txn_send, txns) = coordinator.sim_input::<Txn, TotalOrder, ExactlyOnce>();
    let (timeout_send, timeouts) = participants.sim_input::<(), TotalOrder, ExactlyOnce>();
    let _committed = two_pc(
        &coordinator,
        &participants,
        N as usize,
        txns,
        timeouts,
        true,
        nondet!(/** test */),
        nondet!(/** test */),
        nondet!(/** test */),
        nondet!(/** test */),
        nondet!(/** test */),
    )
    .sim_cluster_output();
    let sim = flow
        .sim()
        .skip_consistency_assertions()
        .with_network_hook_points()
        .with_cluster_size(&participants, N as usize)
        .compiled();
    let names = NodeNames::new(sim.locations());
    let channel = |name: &str| {
        sim.channels()
            .iter()
            .find(|c| c.name.as_deref() == Some(name))
            .unwrap_or_else(|| panic!("channel {name}"))
            .id
    };
    let (commit, peer) = (channel("commit"), channel("peer"));
    let run = |faults: BTreeSet<Fault<MessageId>>, n: usize| {
        let hook = Rc::new(FaultHook::new(&FaultPlan { faults }, &sim));
        sim.set_network_hook(hook.clone());
        sim.fuzz_bounded(n, async || {
            txn_send.send(Txn(1));
            hydro_lang::sim::compiled::quiesce().await;
            timeout_send.send_many((0..N).map(|p| (p, ())));
            hydro_lang::sim::compiled::quiesce().await;
        });
        sim.clear_network_hook();
        hook.drain_instances()
    };
    let find = |records: &[hydrofrac::fault::InstanceRecord], pick: &dyn Fn(&MessageId) -> bool| {
        records
            .iter()
            .flat_map(|r| r.log.events())
            .find_map(|e| match e {
                Event::Sent { msg, .. } if pick(msg) => Some(msg.clone()),
                _ => None,
            })
            .expect("message")
    };
    let free = run(BTreeSet::new(), 1);
    let commit_to_0 = find(&free, &|m| m.channel == commit && m.to.member == Some(0));
    let crash_coord = Fault::crash_after(names.name(&commit_to_0.from), Some(commit_to_0));
    let one = run([crash_coord.clone()].into(), 64);
    let decided_0_to_1 = find(&one, &|m| {
        m.channel == peer && m.from.member == Some(0) && m.to.member == Some(1) && m.bytes[0] == 1
    });
    let crash_p0 = Fault::crash_after(names.name(&decided_0_to_1.from), Some(decided_0_to_1));
    let two = run([crash_coord, crash_p0].into(), samples);
    let mut split = 0;
    for record in &two {
        let mut sent = std::collections::HashMap::new();
        let mut decided = BTreeSet::new();
        for e in record.log.events() {
            match e {
                Event::Sent { id, msg, .. } => {
                    sent.insert(*id, msg.clone());
                }
                Event::Recv { id, channel: c, to } => {
                    let m = &sent[id];
                    if *c == commit || (*c == peer && m.bytes[0] == 1) {
                        decided.insert(to.member.unwrap());
                    }
                }
                _ => {}
            }
        }
        if decided.contains(&1) != decided.contains(&2) {
            split += 1;
        }
    }
    (split, two.len())
}

/// The two-crash violation of cooperative termination with three participants is reachable:
/// some sampled schedules leave participant 1 committed and participant 2 (correct) not.
#[test]
fn two_crashes_split_three_participant_cooperative_termination() {
    // About 1600 of 4096 sampled schedules split them; 512 is plenty.
    let (split, total) = inject_two_crashes(512);
    eprintln!("two crashes: {split} of {total} schedules split participants 1 and 2");
    assert!(
        split > 0,
        "no schedule split participants 1 and 2 (of {total})"
    );
}

/// Decisions for one seeded schedule: the simulator's fuzz driver reads its choices from these
/// bytes (an xorshift stream, as `hydro_lang::sim::feedback_campaign` seeds its schedules), so
/// a seed names one schedule reproducibly, unlike `fuzz_bounded`'s OS-seeded sample.
#[cfg(stageleft_runtime)]
fn schedule_bytes(seed: u64) -> Vec<u8> {
    let mut state = seed.max(1);
    let mut bytes = vec![0; 1 << 16];
    for byte in &mut bytes {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = state as u8;
    }
    bytes
}

/// Direct injection into cooperative termination with two participants whose timers race the
/// transaction: the coordinator crashes right after its `commit` to participant 0. For each
/// seed in `seeds`, whether participant 0 decides and participant 1 (correct) does not.
#[cfg(stageleft_runtime)]
fn ctp2_racing_split(seeds: impl IntoIterator<Item = u64>) -> Vec<(u64, bool)> {
    use std::collections::BTreeSet;
    use std::rc::Rc;

    use hydrofrac::fault::{Event, FaultHook, FaultPlan, InstanceRecord, MessageId, NodeNames};
    use hydrofrac::search::Fault;

    const N: u32 = 2;
    let mut flow = FlowBuilder::new();
    let coordinator = flow.process::<Coordinator>();
    let participants = flow.cluster::<Participant>();
    let (txn_send, txns) = coordinator.sim_input::<Txn, TotalOrder, ExactlyOnce>();
    let (timeout_send, timeouts) = participants.sim_input::<(), TotalOrder, ExactlyOnce>();
    let _committed = two_pc(
        &coordinator,
        &participants,
        N as usize,
        txns,
        timeouts,
        true,
        nondet!(/** test */),
        nondet!(/** test */),
        nondet!(/** test */),
        nondet!(/** test */),
        nondet!(/** test */),
    )
    .sim_cluster_output();
    let sim = flow
        .sim()
        .skip_consistency_assertions()
        .with_network_hook_points()
        .with_cluster_size(&participants, N as usize)
        .compiled();
    let names = NodeNames::new(sim.locations());
    let channel = |name: &str| {
        sim.channels()
            .iter()
            .find(|c| c.name.as_deref() == Some(name))
            .unwrap_or_else(|| panic!("channel {name}"))
            .id
    };
    let (commit, peer) = (channel("commit"), channel("peer"));
    let run = |faults: BTreeSet<Fault<MessageId>>, seed: u64| -> InstanceRecord {
        let hook = Rc::new(FaultHook::new(&FaultPlan { faults }, &sim));
        sim.set_network_hook(hook.clone());
        sim.fuzz_repro(schedule_bytes(seed), async |instance| {
            instance
                .run_with_scheduler_and_logger(std::io::sink(), async {
                    txn_send.send(Txn(1));
                    timeout_send.send_many((0..N).map(|p| (p, ())));
                    hydro_lang::sim::compiled::quiesce().await;
                })
                .await;
        });
        sim.clear_network_hook();
        let mut records = hook.drain_instances();
        assert_eq!(records.len(), 1);
        records.pop().unwrap()
    };
    let free = run(BTreeSet::new(), 1);
    let commit_to_0 = free
        .log
        .events()
        .iter()
        .find_map(|e| match e {
            Event::Sent { msg, .. } if msg.channel == commit && msg.to.member == Some(0) => {
                Some(msg.clone())
            }
            _ => None,
        })
        .expect("the commit to participant 0");
    let crash = Fault::crash_after(names.name(&commit_to_0.from), Some(commit_to_0));
    seeds
        .into_iter()
        .map(|seed| {
            let record = run([crash.clone()].into(), seed);
            let mut sent = std::collections::HashMap::new();
            let mut decided = BTreeSet::new();
            for e in record.log.events() {
                match e {
                    Event::Sent { id, msg, .. } => {
                        sent.insert(*id, msg.clone());
                    }
                    Event::Recv { id, channel: c, to } => {
                        let m: &MessageId = &sent[id];
                        // A `commit`, or a peer's `Decided` (tag 1).
                        if *c == commit || (*c == peer && m.bytes[0] == 1) {
                            decided.insert(to.member.unwrap());
                        }
                    }
                    _ => {}
                }
            }
            (seed, decided.contains(&0) && !decided.contains(&1))
        })
        .collect()
}

/// Cooperative termination with two participants breaks under one crash when the timers race
/// the transaction ([`check`] fires them only once the transaction settles, where the label
/// holds): the coordinator dies right after its `commit` to
/// participant 0, and participant 1, whose single timeout fired before it learned anything (or
/// whose query reached participant 0 before the `commit` did), never decides. Reproduced by
/// direct injection on seeded schedules: 208 of seeds 1..=256 split them, seed 1 among them.
/// About 3 s.
#[cfg(stageleft_runtime)]
#[test]
fn ctp2_racing_timers_split_by_direct_injection() {
    let results = ctp2_racing_split(1..=8);
    assert!(
        results.iter().any(|(_, split)| *split),
        "no seed in 1..=8 split the participants: {results:?}"
    );
}

/// The regression for barrier-free harnesses: with a one-shot timer (no recovery ticks), the
/// check finds unaided that a single crash of the coordinator, right after its `commit` to
/// participant 0, leaves participant 1 never committing (its one query reached participant 0
/// before the `commit` did, or it fired before participant 1 had voted). Sampled (the schedule
/// space is past the check's budget), cross-checked by brute force over every single fault.
/// Racing CTP is past what the oracles can enumerate too: they sample, so this cross-check is
/// weaker.
#[test]
fn ctp2_one_shot_timer_breaks_at_one_coordinator_crash() {
    let cross = TwoPc::new(true, 2).cross_check(true, 0, 1, Some(RACING_ORACLE_SAMPLES));
    cross.assert_agrees();
    breaks_at_one_coordinator_crash(&cross.results, committed(&cross.results));
}

/// The fix: the timer recurs, and the recovery tick re-sends the query once the run settles,
/// so participant 1 learns the decision from participant 0. With two participants that is
/// every admissible fault set for agreement (a participant's crash leaves one correct
/// participant). Sampled, so not a certificate; cross-checked by brute force over every set of
/// at most two faults. Racing CTP is past what the oracles can enumerate too: they sample, so
/// this cross-check is weaker.
#[test]
fn ctp2_with_a_recurring_timer_survives_the_coordinators_crash() {
    let cross =
        TwoPc::new(true, 2).cross_check(true, RECOVERY_TICKS, 2, Some(RACING_ORACLE_SAMPLES));
    cross.assert_agrees();
    let results = &cross.results;
    breaks_at_one_coordinator_crash(results, results.label("prepares"));
    breaks_at_one_coordinator_crash(results, results.label("commits"));
    assert!(
        matches!(
            committed(results).verdict,
            hydrofrac::Verdict::Exhausted {
                tolerates: hydrofrac::search::Tolerance::All,
                ..
            }
        ),
        "{results}"
    );
}

/// With timers that fire only after the faults stop, every schedule is explored: the fixed
/// protocol's `committed` is certified for every admissible fault set (none breaks it), and
/// the enumeration of every set of at most two faults agrees. The claim is narrower than the
/// racing one above: no crash strikes the peer exchange.
#[test]
fn ctp2_with_timers_after_the_faults_is_certified() {
    let cross = TwoPc::new(true, 2).cross_check(false, RECOVERY_TICKS, 2, None);
    cross.assert_agrees();
    assert!(
        matches!(
            committed(&cross.results).verdict,
            hydrofrac::Verdict::Certified {
                tolerates: hydrofrac::search::Tolerance::All
            }
        ),
        "{cross}"
    );
}

/// Three participants, racing timers, recurring: no admissible fault set breaks `committed`
/// (with a one-shot timer one crash does: `ec_bruteforce`'s
/// `brute_force_agrees_on_two_pc_cooperative_termination`). The split of the barrier
/// harness, where participant 0 dies after answering participant 1 but before answering
/// participant 2, is repaired: participant 2 asks again in the recovery phase, and participant
/// 1 answers. Sampled (the schedule space is past the check's budget), so not a certificate;
/// cross-checked by brute force over every set of at most two faults. Racing CTP-3 (more than
/// 223,000 schedules) is past what the oracles can enumerate too: they sample 64 schedules per
/// set (200 sets, 12,800 schedules), so this cross-check is weaker. About 3 minutes.
#[test]
fn ctp3_with_a_recurring_timer_survives_two_crashes() {
    let cross =
        TwoPc::new(true, 3).cross_check(true, RECOVERY_TICKS, 2, Some(RACING_ORACLE_SAMPLES));
    cross.assert_agrees();
    assert!(
        matches!(
            committed(&cross.results).verdict,
            hydrofrac::Verdict::Exhausted {
                tolerates: hydrofrac::search::Tolerance::All,
                ..
            }
        ),
        "{cross}"
    );
}
