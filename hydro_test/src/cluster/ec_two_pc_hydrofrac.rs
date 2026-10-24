//! Two-phase commit under hydrofrac's `EcCheck` (docs/design/hydrofrac-lite.md §7): every
//! `EventualConsistency` label of [`two_pc`](super::two_pc_ctp::two_pc), with or without
//! cooperative termination, every node crashable, crash tolerance swept from zero upwards.
//!
//! The labels are the `prepare`, `commit` and `peer` broadcasts (labelled by their transport)
//! and the participants' `committed` transactions (labelled as a claim: that is agreement).

use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
use hydro_lang::prelude::*;

use super::two_pc_ctp::{Coordinator, Participant, Txn, two_pc};

/// Every label of 2PC (`ctp`: with cooperative termination), searched over `samples` sampled
/// schedules per hypothesis: exhaustive exploration of 2PC's batching and timer interleavings
/// does not finish.
#[cfg(stageleft_runtime)]
fn check(ctp: bool, participants_count: u32, samples: usize) -> hydrofrac::Results {
    let mut flow = FlowBuilder::new();
    let coordinator = flow.process::<Coordinator>();
    let participants = flow.cluster::<Participant>();
    let (txn_send, txns) = coordinator.sim_input::<Txn, TotalOrder, ExactlyOnce>();
    let (timeout_send, timeouts) = participants.sim_input::<(), TotalOrder, ExactlyOnce>();
    let _committed = two_pc(
        &coordinator,
        &participants,
        participants_count as usize,
        txns,
        timeouts,
        ctp,
        nondet!(/** explored by the check */),
        nondet!(/** explored by the check */),
        nondet!(/** explored by the check */),
        nondet!(/** explored by the check */),
        nondet!(/** external input, consumed as it arrives */),
    )
    .sim_cluster_output();

    let mut check = hydrofrac::EcCheck::new().sample_schedules(samples);
    let sim = check
        .instrument(
            flow.sim()
                .with_cluster_size(&participants, participants_count as usize),
        )
        .compiled();
    let start = std::time::Instant::now();
    let results = check.run(&sim, async || {
        // One transaction; once it settles, every participant's uncertainty timer fires.
        txn_send.send(Txn(1));
        hydro_lang::sim::compiled::quiesce().await;
        timeout_send.send_many((0..participants_count).map(|p| (p, ())));
    });
    eprintln!(
        "2PC (ctp: {ctp}, {participants_count} participants): {:.1}s\n{results}",
        start.elapsed().as_secs_f64()
    );
    results
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

/// `peer` and its receiving side carry nothing without faults (no timer fires before the
/// decision arrives), so they are vacuous.
#[cfg(stageleft_runtime)]
fn peers_are_vacuous(results: &hydrofrac::Results) {
    for name in ["peer", "peer_in"] {
        let label = results.label(name);
        assert!(
            matches!(
                label.verdict,
                hydrofrac::Verdict::Vacuous(hydrofrac::check::VacuousReason::EmptyDomain)
            ),
            "{name}\n{results}"
        );
    }
}

/// Plain 2PC blocks: the coordinator dies after its `commit` to some participants, and the
/// others never commit. The `prepare` and `commit` hops break the same way.
#[test]
fn plain_two_pc_breaks_at_one_coordinator_crash() {
    let results = check(false, 2, 8);
    breaks_at_one_coordinator_crash(&results, results.label("prepares"));
    breaks_at_one_coordinator_crash(&results, results.label("commits"));
    breaks_at_one_coordinator_crash(&results, committed(&results));
    peers_are_vacuous(&results);
}

/// Cooperative termination survives the coordinator's crash: a participant that got the
/// `commit` answers its peers' queries. With two participants that is the only admissible
/// crash for agreement: crashing a participant too leaves one correct participant, which
/// agrees with itself (docs/design/hydrofrac-lite.md §4.5). The coordinator's own hops still
/// break at its crash.
///
/// The program has schedule choices (the participants batch their inputs, and whether a peer
/// can answer a query depends on what it has decided when the query lands), so the search
/// cannot certify `committed`: it finds no counterexample, which is all it can say
/// (docs/design/hydrofrac-lite.md §4.9).
#[test]
fn two_pc_with_cooperative_termination_survives_the_coordinators_crash() {
    let results = check(true, 2, 8);
    breaks_at_one_coordinator_crash(&results, results.label("prepares"));
    breaks_at_one_coordinator_crash(&results, results.label("commits"));
    peers_are_vacuous(&results);
    let label = committed(&results);
    assert!(results.choices.iter().any(|s| s.op == "batch"), "{results}");
    assert!(
        matches!(
            label.verdict,
            hydrofrac::Verdict::Exhausted {
                tolerates: hydrofrac::search::Tolerance::All,
                ..
            }
        ),
        "{results}"
    );
}

/// With three participants, cooperative termination breaks at two crashes: the coordinator
/// dies after its `commit` to participant 0, and participant 0 dies after answering
/// participant 1 but before answering participant 2, whose query reached participant 1
/// before participant 1 had decided. Participant 1 commits; participant 2 never does.
/// `two_crashes_split_three_participant_cooperative_termination` shows the violation is
/// reachable.
///
/// Molly's pruning rule certified this label: it requires a hypothesis to kill every
/// derivation of every schedule of an earlier run, but which derivation a schedule takes
/// (here, whether participant 1 relays the decision) depends on the schedule, so the breaking
/// set was pruned by a derivation that does not occur in the schedules it breaks. The program
/// has schedule choices, so the search now uses its heuristic (a hypothesis need only kill one
/// schedule's derivations per earlier run), which finds the two crashes; the one crash it did
/// not break at is searched, not certified (docs/design/hydrofrac-lite.md §4.9).
#[test]
fn two_pc_with_cooperative_termination_breaks_at_two_crashes_with_three_participants() {
    // About 20 s at 256 samples per hypothesis.
    let results = check(true, 3, 256);
    let label = committed(&results);
    assert!(!results.choices.is_empty(), "{results}");
    let hydrofrac::Verdict::Counterexample(cex) = &label.verdict else {
        panic!("expected a counterexample\n{results}");
    };
    assert_eq!(cex.tolerates, Some(1), "{results}");
    assert!(!cex.tolerance_certified, "{results}");
    assert_eq!(cex.crashes(), 2, "{results}");
    assert!(
        cex.faults
            .iter()
            .filter_map(hydrofrac::search::Fault::crashed_node)
            .any(|n| n.starts_with("Coordinator")),
        "{results}"
    );
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

// Cooperative termination with racing timers (docs/design/hydrofrac-lite.md §4.9).

/// Two-phase commit with cooperative termination over two participants and one transaction,
/// with every participant's timer fired together with the transaction instead of after it
/// settles (as [`check`] does), so a participant's query can race the coordinator's `commit`:
/// the same race as `ec_bruteforce`'s `ask_once`. Searched, and cross-checked by brute force
/// over every admissible set of at most `faults` faults, on `samples` schedules per set.
#[cfg(stageleft_runtime)]
fn racing_timers(samples: usize, faults: usize) -> hydrofrac::check::CrossCheck {
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
        nondet!(/** explored */),
        nondet!(/** explored */),
        nondet!(/** explored */),
        nondet!(/** explored */),
        nondet!(/** explored */),
    )
    .sim_cluster_output();
    let mut check = hydrofrac::EcCheck::new().sample_schedules(samples);
    let sim = check
        .instrument(flow.sim().with_cluster_size(&participants, N as usize))
        .compiled();
    check.cross_check(
        &sim,
        async || {
            txn_send.send(Txn(1));
            timeout_send.send_many((0..N).map(|p| (p, ())));
        },
        faults,
    )
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

/// The same race through the check: the search's heuristic finds the coordinator's crash (at
/// its second hypothesis) on sampled schedules. About 5 s.
#[cfg(stageleft_runtime)]
#[test]
fn ctp2_racing_timers_search_finds_the_crash() {
    let cross = racing_timers(64, 1);
    let committed = cross
        .results
        .results()
        .iter()
        .find(|l| l.plan.collection.display_name() == "committed")
        .expect("the committed label");
    let cex = committed
        .verdict
        .counterexample()
        .unwrap_or_else(|| panic!("expected a counterexample:\n{}", cross.results));
    assert_eq!(cex.crashes(), 1, "{}", cross.results);
    assert!(
        cex.faults
            .iter()
            .filter_map(hydrofrac::search::Fault::crashed_node)
            .all(|n| n.starts_with("Coordinator")),
        "{}",
        cross.results
    );
    cross.assert_agrees();
}
