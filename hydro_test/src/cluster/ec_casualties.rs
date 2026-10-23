//! The research branch's certificates on schedule-sensitive labels, re-checked by brute force
//! (docs/design/hydrofrac-lite.md §4.9: "The research branch has the same hole").
//!
//! The research branch (`hydrofrac-input-coverage` @ `0d801bbafc`) certified labels whose cone
//! passes a `nondet!` site with Molly's pruning rule, which is unsound there: those certificates
//! are unjustified. This sweep asks whether they are also *wrong*: for each such label it runs
//! [`EcCheck::cross_check`], which enumerates every admissible fault set of at most
//! [`max_faults`] faults with no lineage pruning, over every schedule (`samples = None`) or a
//! random sample of schedules per set, and reports every set that breaks a label.
//!
//! Every sweep is `#[ignore]`d (minutes each). Run one with
//! `cargo test -p hydro_test --lib ec_casualties::<name> -- --ignored --nocapture`;
//! `CASUALTY_SAMPLES=n` overrides the schedules sampled per fault set and `CASUALTY_FAULTS=k`
//! the largest set size. Each prints the search's report, the enumeration's table (with every
//! breaking set), and the comparison.
//!
//! Result (docs/design/hydrofrac-lite.md §4.9, "How many of them are wrong"): no swept label
//! breaks under any admissible set in the research configurations. The racing-timers variant of
//! cooperative termination, which is not one, does break; its regression tests are at the end.

use std::time::Instant;

use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
use hydro_lang::prelude::*;
use hydrofrac::EcCheck;
use hydrofrac::check::CrossCheck;

use super::reliable_broadcast::{Broadcaster, Member, Payload, broadcast};
use super::two_pc_ctp::{Coordinator, Participant, Txn, two_pc};

pub struct Source;
pub struct Proposer;
pub struct Acceptor;
pub struct Learner;

const MEMBERS: usize = 3;

/// The schedules sampled per fault set: `CASUALTY_SAMPLES`, else `default` (`0` for every
/// schedule).
fn samples(default: usize) -> Option<usize> {
    let n = std::env::var("CASUALTY_SAMPLES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default);
    (n > 0).then_some(n)
}

/// The largest fault set enumerated: `CASUALTY_FAULTS`, else `default`.
fn max_faults(default: usize) -> usize {
    std::env::var("CASUALTY_FAULTS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn check_with(samples: Option<usize>, cap: Option<usize>) -> EcCheck {
    let check = match samples {
        Some(n) => EcCheck::new().sample_schedules(n),
        None => EcCheck::new(),
    };
    match cap {
        Some(k) => check.max_crashes(k),
        None => check,
    }
}

/// Prints the sweep and returns how many labels some enumerated set breaks.
fn report(name: &str, start: Instant, samples: Option<usize>, cross: &CrossCheck) -> usize {
    eprintln!(
        "=== {name} (samples per set: {samples:?}, {:.1}s)\nsearch:\n{}\n{cross}",
        start.elapsed().as_secs_f64(),
        cross.results
    );
    let names = cross.results.report_names();
    let mut broken = 0;
    for b in &cross.brute.labels {
        if let Some(set) = b.breaking.first() {
            broken += 1;
            eprintln!(
                "CASUALTY {name}: {} breaks under {} ({} breaking sets of {}):",
                b.name,
                names.fault_set(set),
                b.breaking.len(),
                b.admissible
            );
            for s in &b.breaking {
                eprintln!("    {}", names.fault_set(s));
            }
        }
    }
    eprintln!(
        "SUMMARY {name}: {} labels, {} broken; {} sets, {} schedules, {:.1}s",
        cross.brute.labels.len(),
        broken,
        cross.brute.sets,
        cross.brute.schedules,
        start.elapsed().as_secs_f64()
    );
    broken
}

/// `hydro_test`'s echoing reliable broadcast (`reliable_broadcast::broadcast`, echo on) to
/// three members, one payload: `deliveries`.
fn echo_broadcast(samples: Option<usize>, faults: usize) -> (CrossCheck, Instant) {
    let mut flow = FlowBuilder::new();
    let broadcaster = flow.process::<Broadcaster>();
    let members = flow.cluster::<Member>();
    let (send, source) = broadcaster.sim_input::<Payload, TotalOrder, ExactlyOnce>();
    let _out = broadcast(
        &members,
        source,
        true,
        nondet!(/** explored */),
        nondet!(/** explored */),
    )
    .sim_cluster_output();
    let mut check = check_with(samples, None);
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, MEMBERS))
        .compiled();
    let start = Instant::now();
    let cross = check.cross_check(
        &sim,
        async || {
            send.send(Payload(42));
        },
        faults,
    );
    (cross, start)
}

#[test]
#[ignore = "sweep: minutes"]
fn casualty_echo_broadcast() {
    let s = samples(64);
    let (cross, start) = echo_broadcast(s, max_faults(2));
    report("echo_broadcast", start, s, &cross);
}

/// `hydro_std`'s uniform reliable broadcast with `threshold`, a process source, three members,
/// `items` items.
fn uniform(
    threshold: usize,
    items: u32,
    samples: Option<usize>,
    faults: usize,
) -> (CrossCheck, Instant) {
    use hydro_std::ec_inference_demos::uniform_broadcast::uniform_reliable_broadcast_closed;

    let mut flow = FlowBuilder::new();
    let source = flow.process::<Source>();
    let members = flow.cluster::<Member>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let _out = uniform_reliable_broadcast_closed(input, &members, threshold)
        .weaken_consistency()
        .sim_cluster_output();
    let mut check = check_with(samples, None);
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, MEMBERS))
        .compiled();
    let start = Instant::now();
    let cross = check.cross_check(
        &sim,
        async || {
            for i in 1..=items {
                send.send(i);
            }
        },
        faults,
    );
    (cross, start)
}

#[test]
#[ignore = "sweep: minutes"]
fn casualty_uniform_broadcast() {
    let s = samples(32);
    let items = std::env::var("CASUALTY_ITEMS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    for threshold in [1, 2] {
        let (cross, start) = uniform(threshold, items, s, max_faults(2));
        report(
            &format!("uniform({threshold}, {items} items)"),
            start,
            s,
            &cross,
        );
    }
}

/// G-Set gossip (closed or live) among three members, one element each, `rounds` gossip ticks
/// at every member (the harness of `ec_demos_hydrofrac`).
fn gossip(live: bool, rounds: u32, samples: Option<usize>, faults: usize) -> (CrossCheck, Instant) {
    use hydro_std::ec_inference_demos::crdt_gossip::{g_set_gossip, g_set_gossip_live};

    let mut flow = FlowBuilder::new();
    let members = flow.cluster::<Member>();
    let (update, updates) = members.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let (tick, ticks) = members.sim_input::<(), TotalOrder, ExactlyOnce>();
    let updates = updates.weaken_ordering::<NoOrder>();
    let _set = if live {
        g_set_gossip_live(&members, updates, ticks)
    } else {
        g_set_gossip(&members, updates, ticks)
    };
    let mut check = check_with(samples, None);
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, MEMBERS))
        .compiled();
    let start = Instant::now();
    let cross = check.cross_check(
        &sim,
        async || {
            for m in 0..MEMBERS as u32 {
                update.send(m, 10 + m);
            }
            hydro_lang::sim::compiled::quiesce().await;
            for _ in 0..rounds {
                tick.send_many((0..MEMBERS as u32).map(|m| (m, ())));
                hydro_lang::sim::compiled::quiesce().await;
            }
        },
        faults,
    );
    (cross, start)
}

#[test]
#[ignore = "sweep: minutes"]
fn casualty_gossip() {
    let s = samples(256);
    let rounds = std::env::var("CASUALTY_ROUNDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3);
    for live in [false, true] {
        let (cross, start) = gossip(live, rounds, s, max_faults(2));
        report(
            &format!("gossip (live: {live}, {rounds} rounds)"),
            start,
            s,
            &cross,
        );
    }
}

/// Multi-decree Paxos: one proposer leading round 1, two commands, three acceptors, three
/// learners, at most one crash (the harness of `ec_demos_hydrofrac`).
#[test]
#[ignore = "sweep: minutes"]
fn casualty_multi_paxos() {
    use hydro_std::ec_inference_demos::multi_paxos::multi_paxos;

    let s = samples(16);
    let mut flow = FlowBuilder::new();
    let proposers = flow.cluster::<Proposer>();
    let acceptors = flow.cluster::<Acceptor>();
    let learners = flow.cluster::<Learner>();
    let (lead, leads) = proposers.sim_input::<u64, TotalOrder, ExactlyOnce>();
    let (command, commands) = proposers.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let outputs = multi_paxos(&acceptors, &learners, 2, leads, commands);
    let _learned = outputs.learned.weaken_consistency().sim_cluster_output();
    let mut check = check_with(s, Some(1));
    let sim = check
        .instrument(
            flow.sim()
                .with_cluster_size(&proposers, 1)
                .with_cluster_size(&acceptors, 3)
                .with_cluster_size(&learners, MEMBERS),
        )
        .compiled();
    let start = Instant::now();
    let cross = check.cross_check(
        &sim,
        async || {
            lead.send(0, 1);
            hydro_lang::sim::compiled::quiesce().await;
            command.send(0, 7);
            command.send(0, 8);
        },
        max_faults(1),
    );
    report("multi_paxos (cap 1)", start, s, &cross);
}

// ABD (reliable phase 2) is swept in `hydro_std`'s `ec_abd_hydrofrac`
// (`reliable_phase_two_brute_force_sweep`): its message types are private to that crate, so a
// simulation staged from here cannot name them.

/// Two-phase commit with cooperative termination over `n` participants, `txns` transactions,
/// every participant's timer fired once the transactions settle.
///
/// With `racing_timers`, the timers fire together with the transactions instead of after they
/// settle, so a participant's query can race the coordinator's `commit` (not the research
/// harness: an extra probe of the same race as `ask_once`).
fn two_pc_ctp(
    n: u32,
    txns: u32,
    racing_timers: bool,
    samples: Option<usize>,
    faults: usize,
) -> (CrossCheck, Instant) {
    let mut flow = FlowBuilder::new();
    let coordinator = flow.process::<Coordinator>();
    let participants = flow.cluster::<Participant>();
    let (txn_send, txn_in) = coordinator.sim_input::<Txn, TotalOrder, ExactlyOnce>();
    let (timeout_send, timeouts) = participants.sim_input::<(), TotalOrder, ExactlyOnce>();
    let _committed = two_pc(
        &coordinator,
        &participants,
        n as usize,
        txn_in,
        timeouts,
        true,
        nondet!(/** explored */),
        nondet!(/** explored */),
        nondet!(/** explored */),
        nondet!(/** explored */),
        nondet!(/** explored */),
    )
    .sim_cluster_output();
    let mut check = check_with(samples, None);
    let sim = check
        .instrument(flow.sim().with_cluster_size(&participants, n as usize))
        .compiled();
    let start = Instant::now();
    let cross = check.cross_check(
        &sim,
        async || {
            txn_send.send_many((1..=txns).map(Txn));
            if !racing_timers {
                hydro_lang::sim::compiled::quiesce().await;
            }
            timeout_send.send_many((0..n).map(|p| (p, ())));
        },
        faults,
    );
    (cross, start)
}

/// Cooperative termination with two participants: only the coordinator may crash (the checked
/// cluster keeps two correct members), so every admissible set is one crash. One transaction,
/// every schedule; then the research harness's four transactions, sampled.
#[test]
#[ignore = "sweep: minutes"]
fn casualty_two_pc_ctp_two_participants() {
    let (cross, start) = two_pc_ctp(2, 1, false, None, max_faults(2));
    report(
        "2pc ctp (2 participants, 1 txn, exhaustive)",
        start,
        None,
        &cross,
    );
    let s = samples(64);
    let (cross, start) = two_pc_ctp(2, 4, false, s, max_faults(2));
    report("2pc ctp (2 participants, 4 txns)", start, s, &cross);
}

/// Cooperative termination with two participants whose timers race the transaction (not the
/// research harness; see [`two_pc_ctp`]). One transaction, sampled: exhaustively, a single
/// fault set passed 100,000 schedules without finishing.
#[test]
#[ignore = "sweep: minutes"]
fn casualty_two_pc_ctp_two_participants_racing_timers() {
    let s = samples(256);
    let (cross, start) = two_pc_ctp(2, 1, true, s, max_faults(2));
    report(
        "2pc ctp (2 participants, 1 txn, racing timers)",
        start,
        s,
        &cross,
    );
}

/// Decisions for one seeded schedule: the simulator's fuzz driver reads its choices from these
/// bytes (an xorshift stream, as `hydro_lang::sim::feedback_campaign` seeds its schedules), so
/// a seed names one schedule reproducibly, unlike `fuzz_bounded`'s OS-seeded sample.
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

/// How often the racing-timers split happens, and which seeds show it (run by hand).
#[test]
#[ignore = "survey"]
fn ctp2_racing_split_survey() {
    let results = ctp2_racing_split(1..=256);
    let split: Vec<u64> = results
        .iter()
        .filter(|(_, s)| *s)
        .map(|(s, _)| *s)
        .collect();
    eprintln!(
        "{} of {} seeds split: {split:?}",
        split.len(),
        results.len()
    );
}

/// Cooperative termination with two participants breaks under one crash when the timers race
/// the transaction (the research harness, and `ec_two_pc_hydrofrac`'s, fire them only once the
/// transaction settles, where the label holds): the coordinator dies right after its `commit` to
/// participant 0, and participant 1, whose single timeout fired before it learned anything (or
/// whose query reached participant 0 before the `commit` did), never decides. Reproduced by
/// direct injection on seeded schedules: 208 of seeds 1..=256 split them, seed 1 among them.
/// About 3 s.
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
#[test]
fn ctp2_racing_timers_search_finds_the_crash() {
    let (cross, _) = two_pc_ctp(2, 1, true, Some(64), 1);
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
