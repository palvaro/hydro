//! hydrofrac-lite on the EC-inference demonstration protocols of
//! `hydro_std::ec_inference_demos` (docs/design/hydrofrac-lite.md §7): the research harness
//! `ec_demos_ldfi.rs` (`hydrofrac-input-coverage` @ `0d801bbafc`), ported to [`EcCheck`].
//!
//! The demos were written to show EC *inferred* by the type system from a few trusted premises:
//! `fan_out`'s single `manual_proof!` rests on membership completeness, holder persistence and
//! channel eventual delivery. None names the *data holder crashing*. The check turns each such
//! premise into a search, with every process and cluster member crashable and the transport
//! types as the fault model (every channel here is `fail_stop`, so the faults are crashes):
//!
//! - `broadcast_live_from_process`: one hop over live membership; the sender crashing
//!   mid-fan-out breaks it.
//! - `reliable_broadcast_closed` / `_live`: the echo cycle is the retransmission the premises
//!   assume; it holds under every admissible crash.
//! - `uniform_reliable_broadcast_closed`: delivery after a threshold of echoes.
//! - `g_set_gossip` / `_live`: state-based gossip; holds.
//! - `leader_merge_broadcast`: EC rests on the leader; the leader crashing mid-broadcast breaks
//!   it, which is the demo's own claim.
//! - `multi_paxos`: `learned` holds under one crash (the protocol's `f = 1`).
//!
//! What changed from the research harness, by the lite conventions (design doc §2, §4.5, §4.6):
//! - no crashable lists: every node is crashable;
//! - no hand-written `Spec`: the single-stream searches take the verdict of the label the demo
//!   returns, and the all-labels searches are the same check read in full;
//! - the verdict is a crash sweep: "tolerates k crashes, breaks with k + 1";
//! - no `end_of_finite_failures`: the network heals after the fault phase (the gossip search
//!   used to end failures after the updates; it now admits crashes during a first gossip round
//!   racing the updates, and runs the later rounds in the recovery phase);
//! - `leader_merge` is compiled with provenance (the rewrite now tracks its keyed
//!   `entries_partially_ordered` precisely);
//! - the static order check and the one-schedule-per-hypothesis search are phase 2 and are not
//!   ported.

#[cfg(stageleft_runtime)]
use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
#[cfg(stageleft_runtime)]
use hydro_lang::prelude::*;
#[cfg(stageleft_runtime)]
use hydro_std::ec_inference_demos::broadcast_live::broadcast_live_from_process;
#[cfg(stageleft_runtime)]
use hydro_std::ec_inference_demos::crdt_gossip::{g_set_gossip, g_set_gossip_live};
#[cfg(stageleft_runtime)]
use hydro_std::ec_inference_demos::leader_merge::leader_merge_broadcast;
#[cfg(stageleft_runtime)]
use hydro_std::ec_inference_demos::multi_paxos::multi_paxos;
#[cfg(stageleft_runtime)]
use hydro_std::ec_inference_demos::reliable_broadcast::{
    reliable_broadcast_closed, reliable_broadcast_live,
};
#[cfg(stageleft_runtime)]
use hydro_std::ec_inference_demos::uniform_broadcast::uniform_reliable_broadcast_closed;
#[cfg(stageleft_runtime)]
use hydrofrac::EcCheck;
#[cfg(stageleft_runtime)]
use hydrofrac::check::{LabelResult, Results, Verdict};
#[cfg(stageleft_runtime)]
use hydrofrac::search::{Fault, Tolerance};
#[cfg(stageleft_runtime)]
use std::time::Instant;

pub struct Source;
pub struct Member;
pub struct Writer;
pub struct Proposer;
pub struct Acceptor;
pub struct Learner;

#[cfg(stageleft_runtime)]
const MEMBERS: usize = 3;

/// Which broadcast demo a process → cluster check runs.
#[cfg(stageleft_runtime)]
#[derive(Clone, Copy, Debug)]
enum Broadcast {
    Live,
    ReliableClosed,
    ReliableLive,
    /// Uniform reliable broadcast with this echo threshold.
    Uniform(usize),
}

/// Prints the report and the wall time of a check (`--nocapture`).
#[cfg(stageleft_runtime)]
fn report(what: &str, start: Instant, results: &Results) {
    eprintln!(
        "== {what} ({:.1}s)\n{results}",
        start.elapsed().as_secs_f64()
    );
}

/// A process source broadcasts two items to a cluster of three through the demo. The returned
/// stream is what the demo labels `EventualConsistency`; `delivered` names it in the results.
#[cfg(stageleft_runtime)]
fn broadcast_check(which: Broadcast) -> Results {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<Source>();
    let members = flow.cluster::<Member>();
    let (send, items) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();

    let delivered = match which {
        Broadcast::Live => {
            broadcast_live_from_process(items, &members, TCP.fail_stop().name("bcast").bincode())
                .weaken_consistency()
        }
        Broadcast::ReliableClosed => {
            reliable_broadcast_closed(items, &members).weaken_consistency()
        }
        Broadcast::ReliableLive => reliable_broadcast_live(items, &members).weaken_consistency(),
        Broadcast::Uniform(threshold) => {
            uniform_reliable_broadcast_closed(items, &members, threshold).weaken_consistency()
        }
    };
    let _out = delivered.sim_cluster_output();

    // The echo cycles of the reliable variants are small enough to explore exhaustively;
    // uniform broadcast's threshold logic over the echoes is not (the research probe ran past
    // 240,000 interleavings), so the check samples it.
    let mut check = EcCheck::new();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, MEMBERS))
        .compiled();
    let start = Instant::now();
    let results = check.run(&sim, async || {
        send.send(1);
        send.send(2);
    });
    report(&format!("{which:?}"), start, &results);
    results
}

/// G-Set gossip (closed or live fan-out) among three members, each contributing one element.
/// In the fault phase every member's gossip tick fires once, racing the updates, so crashes
/// strike during gossip; the recovery phase fires three more rounds at every member, each once
/// the previous one settled. The G-Set is a labelled `Singleton`, checked at the labelled
/// stream it folds.
#[cfg(stageleft_runtime)]
fn gossip_check(live: bool) -> Results {
    const ROUNDS: u32 = 3;
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

    // Gossip rounds among three members multiply interleavings: the check samples them.
    let mut check = EcCheck::new();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, MEMBERS))
        .compiled();
    let start = Instant::now();
    let every = || (0..MEMBERS as u32).map(|m| (m, ()));
    let results = check.run_then_recover(
        &sim,
        async || {
            for m in 0..MEMBERS as u32 {
                update.send(m, 10 + m);
            }
            tick.send_many(every());
        },
        async || {
            for _ in 0..ROUNDS {
                tick.send_many(every());
                hydro_lang::sim::compiled::quiesce().await;
            }
        },
    );
    report(&format!("g_set_gossip (live: {live})"), start, &results);
    results
}

/// Two writers send keyed values to one leader process, which merges them into one order and
/// broadcasts it to three replicas (`leader_merge_broadcast`).
#[cfg(stageleft_runtime)]
fn leader_merge_check() -> Results {
    let mut flow = FlowBuilder::new();
    let writers = flow.cluster::<Writer>();
    let leader = flow.process::<Source>();
    let replicas = flow.cluster::<Member>();
    let (write, writes) = writers.sim_input::<u32, TotalOrder, ExactlyOnce>();

    let at_leader = writes.send(&leader, TCP.fail_stop().name("to_leader").bincode());
    let merged = leader_merge_broadcast(
        at_leader,
        &replicas,
        TCP.fail_stop().name("merged").bincode(),
        nondet!(/** the leader's interleaving of the writers */),
    )
    .weaken_consistency();
    let _out = merged.sim_cluster_output();

    let mut check = EcCheck::new();
    let sim = check
        .instrument(
            flow.sim()
                .with_cluster_size(&writers, 2)
                .with_cluster_size(&replicas, MEMBERS),
        )
        .compiled();
    let start = Instant::now();
    let results = check.run(&sim, async || {
        write.send(0, 100);
        write.send(1, 200);
    });
    report("leader_merge_broadcast", start, &results);
    results
}

/// Multi-decree Paxos (`multi_paxos`): one proposer told to lead round 1 and given two commands
/// together, three acceptors (majority two), three learners. `learned`, the chosen decrees at each learner, is
/// disseminated by the leader's broadcast plus a learner-side echo cycle. One crash at most:
/// the protocol's own fault model (`f = 1` of three acceptors); unbounded, the research search
/// did not finish within 2,000 hypotheses.
#[cfg(stageleft_runtime)]
fn multi_paxos_check() -> Results {
    let mut flow = FlowBuilder::new();
    let proposers = flow.cluster::<Proposer>();
    let acceptors = flow.cluster::<Acceptor>();
    let learners = flow.cluster::<Learner>();
    let (lead, leads) = proposers.sim_input::<u64, TotalOrder, ExactlyOnce>();
    let (command, commands) = proposers.sim_input::<u32, TotalOrder, ExactlyOnce>();

    let outputs = multi_paxos(&acceptors, &learners, 2, leads, commands);
    let _learned = outputs.learned.weaken_consistency().sim_cluster_output();

    let mut check = EcCheck::new().max_crashes(1);
    let sim = check
        .instrument(
            flow.sim()
                .with_cluster_size(&proposers, 1)
                .with_cluster_size(&acceptors, 3)
                .with_cluster_size(&learners, MEMBERS),
        )
        .compiled();
    let start = Instant::now();
    let results = check.run(&sim, async || {
        lead.send(0, 1);
        command.send(0, 7);
        command.send(0, 8);
    });
    report("multi_paxos", start, &results);
    results
}

/// The labels whose source file is `file` (a suffix of the label's file).
#[cfg(stageleft_runtime)]
fn labels_in<'r>(results: &'r Results, file: &'r str) -> impl Iterator<Item = &'r LabelResult> {
    results.results().iter().filter(move |l| {
        l.plan
            .collection
            .file
            .as_deref()
            .is_some_and(|f| f.ends_with(file))
    })
}

/// The verdict a label is judged by here: its own, or for a scalar folding a labelled stream
/// (which hydrofrac does not check: agreeing streams can still fold to different values) the
/// verdict of that stream. For an idempotent fold such as a G-Set's union, agreeing streams do
/// fold to the same value, by reading. That reading is this test's argument, not hydrofrac's:
/// hydrofrac reports the scalar `NotChecked`, which [`Results::holds`] rejects, cover or not.
#[cfg(stageleft_runtime)]
fn effective<'r>(results: &'r Results, label: &'r LabelResult) -> &'r Verdict {
    match (&label.verdict, label.plan.collection.covered_by) {
        (Verdict::NotChecked(_), Some(index)) => &results.results()[index].verdict,
        _ => &label.verdict,
    }
}

#[cfg(stageleft_runtime)]
fn certified_all(verdict: &Verdict) -> bool {
    matches!(
        verdict,
        Verdict::Certified {
            tolerates: Tolerance::All
        }
    )
}

/// A label of a program with schedule choices whose search found no counterexample under
/// `tolerates`: all the check can say for it, since only labels of a program whose fault sets
/// have one execution each can be certified (docs/design/hydrofrac-lite.md §4.9).
#[cfg(stageleft_runtime)]
fn exhausted(verdict: &Verdict, tolerates: Tolerance) -> bool {
    matches!(verdict, Verdict::Exhausted { tolerates: t, .. } if *t == tolerates)
}

#[cfg(stageleft_runtime)]
fn crashes_node(verdict: &Verdict, prefix: &str) -> bool {
    verdict.counterexample().is_some_and(|c| {
        c.faults
            .iter()
            .any(|f| matches!(f, Fault::Crash { node, .. } if node.starts_with(prefix)))
    })
}

/// `fan_out`'s premises do not include the data holder surviving: one hop over live
/// membership tolerates no crash, and breaks under the sender's crash between two fan-out
/// sends.
#[test]
fn ec_demo_broadcast_live_breaks_under_a_sender_crash() {
    let results = broadcast_check(Broadcast::Live);
    // The demo's one label is `fan_out`'s (the hop `broadcast_live_from_process` delegates to).
    let hops: Vec<_> = labels_in(&results, "fan_out.rs").collect();
    assert!(!hops.is_empty(), "{results}");
    for hop in hops {
        let Verdict::Counterexample(cex) = &hop.verdict else {
            panic!("{}: expected a counterexample\n{results}", hop.name());
        };
        assert_eq!(cex.tolerates, Some(0), "{results}");
        assert_eq!(cex.crashes(), 1, "{results}");
        assert!(crashes_node(&hop.verdict, "Source"), "{results}");
    }
}

/// The echo cycle is the retransmission the premises assume: reliable broadcast, closed and
/// live, holds under every admissible crash set at its delivered set. The single hops into it
/// (the source's broadcast, the echo) are labels of their own and break on their own.
#[test]
fn ec_demo_reliable_broadcast_certifies() {
    for which in [Broadcast::ReliableClosed, Broadcast::ReliableLive] {
        let results = broadcast_check(which);
        let delivered: Vec<_> = labels_in(&results, "reliable_broadcast.rs")
            .filter(|l| certified_all(effective(&results, l)))
            .collect();
        assert!(
            !delivered.is_empty(),
            "{which:?}: the delivered set should certify\n{results}"
        );
        assert!(
            results
                .results()
                .iter()
                .all(|l| !matches!(l.verdict, Verdict::Budget { .. })),
            "{results}"
        );
    }
}

/// Every label of `reliable_broadcast_closed`: the delivered set certifies; the initial
/// single-hop broadcast the label is inferred from breaks on its own under the source's crash
/// mid-fan-out (it reaches the delivered set only merged with the echo).
#[test]
fn ec_demo_reliable_broadcast_all_labels() {
    let results = broadcast_check(Broadcast::ReliableClosed);
    let labels: Vec<_> = labels_in(&results, "reliable_broadcast.rs").collect();
    assert!(
        labels.iter().any(|l| certified_all(effective(&results, l))),
        "the delivered set certifies\n{results}"
    );
    assert!(
        labels.iter().any(|l| crashes_node(&l.verdict, "Source")
            && l.verdict.counterexample().unwrap().tolerates == Some(0)),
        "the initial hop breaks under the source's crash\n{results}"
    );
    // Nothing exhausted the budget, and every label got a verdict.
    assert!(
        results
            .results()
            .iter()
            .all(|l| !matches!(l.verdict, Verdict::Budget { .. })),
        "{results}"
    );
}

/// Uniform reliable broadcast delivers after `threshold` echoes. Its delivered set (the `map`
/// out of the quorum, and the quorum's certificates before it) has no counterexample under
/// any admissible crash set for thresholds 1 and 2; the single hops into the echo cycle break
/// on their own. The quorum batches its acks, so the program has schedule choices: the
/// delivered set is searched, not certified.
#[test]
fn ec_demo_uniform_broadcast() {
    for threshold in [1, 2] {
        let results = broadcast_check(Broadcast::Uniform(threshold));
        let delivered: Vec<_> = labels_in(&results, "uniform_broadcast.rs").collect();
        assert_eq!(delivered.len(), 1, "uniform({threshold})\n{results}");
        assert!(
            exhausted(effective(&results, delivered[0]), Tolerance::All),
            "uniform({threshold})\n{results}"
        );
        for l in results.results() {
            assert!(
                !matches!(l.verdict, Verdict::Budget { .. } | Verdict::Vacuous(_)),
                "uniform({threshold}) {}\n{results}",
                l.name()
            );
        }
    }
}

/// State-based G-Set gossip, closed and live: the stream the G-Set folds (the G-Set itself is
/// not checked, see [`effective`]) has no counterexample under any admissible crash set. Gossip rounds are batched, so the program
/// has schedule choices: searched, not certified.
#[test]
fn ec_demo_g_set_gossip_certifies() {
    for live in [false, true] {
        let results = gossip_check(live);
        let set: Vec<_> = labels_in(&results, "crdt_gossip.rs")
            .filter(|l| l.plan.collection.covered_by.is_some())
            .collect();
        assert_eq!(set.len(), 1, "live: {live}\n{results}");
        assert!(
            matches!(set[0].verdict, Verdict::NotChecked(_)) && set[0].verdict.fails(),
            "hydrofrac itself certifies nothing about the G-Set (live: {live})\n{results}"
        );
        assert!(
            exhausted(effective(&results, set[0]), Tolerance::All),
            "live: {live}\n{results}"
        );
    }
}

/// Every label of G-Set gossip, closed and live: the G-Set has no counterexample (above); no
/// label runs out of budget, and the report names every single hop into the merge.
#[test]
fn ec_demo_g_set_gossip_all_labels() {
    for live in [false, true] {
        let results = gossip_check(live);
        assert!(
            results
                .results()
                .iter()
                .all(|l| !matches!(l.verdict, Verdict::Budget { .. })),
            "live: {live}\n{results}"
        );
        assert!(
            labels_in(&results, "crdt_gossip.rs")
                .filter(|l| l.plan.collection.covered_by.is_some())
                .all(|l| exhausted(effective(&results, l), Tolerance::All)),
            "live: {live}\n{results}"
        );
    }
}

/// `leader_merge`'s EC label "silently assumes the leader's survival" (its docs): it tolerates
/// no crash, and the leader crashing between two of its broadcast sends is the counterexample.
#[test]
fn ec_demo_leader_merge_breaks_under_a_leader_crash() {
    let results = leader_merge_check();
    let merged: Vec<_> = labels_in(&results, "leader_merge.rs").collect();
    assert!(!merged.is_empty(), "{results}");
    assert!(
        merged.iter().any(|l| crashes_node(&l.verdict, "Source")
            && l.verdict.counterexample().unwrap().tolerates == Some(0)),
        "{results}"
    );
    assert!(results.islands.is_empty(), "{results}");
}

/// `multi_paxos.learned` is labelled EC with the argument that the learner-side echo makes
/// dissemination all-or-nothing among live learners even if the proposer crashes
/// mid-broadcast. Under one crash anywhere the check finds no counterexample; Paxos batches
/// throughout, so that is a search, not a certificate.
#[test]
fn ec_demo_multi_paxos_learned_under_one_crash() {
    let results = multi_paxos_check();
    let learned: Vec<_> = labels_in(&results, "multi_paxos.rs")
        .filter(|l| l.plan.collection.display_name() == "learned")
        .collect();
    assert!(!learned.is_empty(), "{results}");
    assert!(
        learned.iter().any(|l| {
            let v = effective(&results, l);
            exhausted(v, Tolerance::UpTo(1)) || exhausted(v, Tolerance::All)
        }),
        "{results}"
    );
}

/// Every label of `multi_paxos`, under one crash: nothing runs out of budget, `learned` holds
/// (above), and the spliced log, a labelled scalar, is checked at the stream it folds.
#[test]
fn ec_demo_multi_paxos_all_labels() {
    let results = multi_paxos_check();
    assert!(
        results
            .results()
            .iter()
            .all(|l| !matches!(l.verdict, Verdict::Budget { .. })),
        "{results}"
    );
}
