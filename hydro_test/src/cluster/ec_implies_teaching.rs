//! Teaching examples for hydrofrac's `implies` rules: a rule between two collections, checked
//! like a label.
//!
//! A label (`EventualConsistency`) says "every correct member ends up with what any correct
//! member has". A rule says more precisely what is owed, and by whom:
//!
//! ```ignore
//! use hydrofrac::{ASide, BSide, Implies};
//! a.implies("name", &b, q!(|r| key), q!(|r| key), ASide::Correct, BSide::AllCorrect);
//! ```
//!
//! reads **"if a record is in `a` at a member the A-side chooses, a record with the same key is
//! in `b` at the members the B-side chooses"**, at the end of every run. The A-side is `Correct`
//! (members that never crash) or `Any` (any member, counting what a crashed one held before it
//! crashed). The B-side is `AllCorrect` (every member that never crashes), `AtLeast(k)` (at
//! least `k` members that never crash), or `Some` (at least one member, counting what a crashed
//! one held before it crashed).
//! `results.label("name")` is the rule's verdict.
//!
//! | test | nodes | outcome |
//! |---|---|---|
//! | [`a_member_that_delivers_then_crashes_breaks_uniformity`] | sender, 2 members | regular holds; uniform broken by 2 crashes |
//! | [`uniform_reliable_broadcast_is_uniform`] | sender, 2 members | both hold |
//! | [`acking_after_one_store_is_not_durable`] | client, 2 replicas | broken by 1 lost message |
//! | [`acking_after_every_store_is_durable`] | client, 2 replicas | holds |
//! | [`two_copies_survive_one_crash_but_not_two`] | client, 3 replicas | `AtLeast(1)` holds under 1 crash, broken by 2 |
//!
//! The exact faults, and a brute-force cross-check of every verdict, are in
//! `ec_implies_checks.rs`.

use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
use hydro_lang::prelude::*;
use hydro_std::ec_inference_demos::quorum::quorum;
use hydro_std::ec_inference_demos::reliable_broadcast::reliable_broadcast_closed;
use hydro_std::ec_inference_demos::uniform_broadcast::uniform_reliable_broadcast_closed;
use hydrofrac::{ASide, BSide, EcCheck, Implies};

/// The node that broadcasts, or the client that writes.
pub struct Source;

/// The members of the broadcast, or the replicas.
pub struct Member;

/// **Delivering before forwarding is not uniform.** The echo broadcast: each member delivers the
/// item the first time it sees it, and forwards it to every member. *Regular* reliable
/// broadcast asks that whatever a correct member delivers, every correct member delivers; it
/// holds, because a correct member forwards. *Uniform* reliable broadcast asks the same of what
/// any member delivers, even one that crashes later. The sender crashes after reaching member
/// 0, and member 0 delivers and crashes before its forward reaches member 1: member 1 never
/// delivers what member 0 did.
#[test]
fn a_member_that_delivers_then_crashes_breaks_uniformity() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<Source>();
    let members = flow.cluster::<Member>();
    let (send, item) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let delivered = reliable_broadcast_closed(item, &members);
    let id = || q!(|m| m);
    delivered.implies(
        "regular",
        &delivered,
        id(),
        id(),
        ASide::Correct,
        BSide::AllCorrect,
    );
    delivered.implies(
        "uniform",
        &delivered,
        id(),
        id(),
        ASide::Any,
        BSide::AllCorrect,
    );
    let _out = delivered.sim_cluster_output();

    let mut check = EcCheck::new();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .compiled();
    let results = check.run(&sim, async || {
        send.send(1);
    });

    println!("{results}");
    results.label("regular").assert_holds();
    let cex = results
        .label("uniform")
        .counterexample()
        .expect("a member that delivers and then crashes breaks uniformity");
    assert_eq!(cex.crashes(), 2);
}

/// **Waiting for echoes is uniform.** The repo's uniform reliable broadcast: a member delivers
/// only once it has received the item's echo from a quorum of members (here one, out of two).
/// A member echoes before it can deliver, so whatever any member delivers, every correct member
/// receives an echo of, and delivers.
#[test]
fn uniform_reliable_broadcast_is_uniform() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<Source>();
    let members = flow.cluster::<Member>();
    let (send, item) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let delivered = uniform_reliable_broadcast_closed(item, &members, 1);
    let id = || q!(|m| m);
    delivered.implies(
        "regular",
        &delivered,
        id(),
        id(),
        ASide::Correct,
        BSide::AllCorrect,
    );
    delivered.implies(
        "uniform",
        &delivered,
        id(),
        id(),
        ASide::Any,
        BSide::AllCorrect,
    );
    let _out = delivered.sim_cluster_output();

    let mut check = EcCheck::new();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .compiled();
    let results = check.run(&sim, async || {
        send.send(1);
    });

    println!("{results}");
    results.label("regular").assert_holds();
    results.label("uniform").assert_holds();
}

/// The client writes to every replica over a link that may lose messages; each replica stores
/// the write (`stored`) and acknowledges it; the client acknowledges the write to its user
/// (`acked`) once `acks` replicas have. The rule `durable`: **a write the client acked is
/// stored at the replicas `b_side` names.**
fn replicated_store<'a>(
    flow: &mut FlowBuilder<'a>,
    acks: usize,
    b_side: BSide,
) -> (
    Process<'a, Source>,
    Cluster<'a, Member>,
    hydro_lang::sim::SimSender<u32, TotalOrder, ExactlyOnce>,
) {
    let client = flow.process::<Source>();
    let replicas = flow.cluster::<Member>();
    let (send, writes) = client.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let stored = writes.broadcast_closed(
        &replicas,
        TCP.lossy_delayed_forever().name("write").bincode(),
    );
    let replies = stored
        .clone()
        .send(&client, TCP.fail_stop().name("ack").bincode())
        .entries()
        .map(q!(|(replica, write)| (write, replica)));
    let acked = quorum(acks, replies).map(q!(|durable| durable.into_fact()));
    acked.implies(
        "durable",
        &stored,
        q!(|w| w),
        q!(|w| w),
        ASide::Correct,
        b_side,
    );
    (client, replicas, send)
}

/// **Acking after one store is not durable.** The client acks as soon as one replica has stored
/// the write. The write to the other replica is lost, so a replica that never crashes never
/// stores a write the client acked.
#[test]
fn acking_after_one_store_is_not_durable() {
    let mut flow = FlowBuilder::new();
    let (_client, replicas, send) = replicated_store(&mut flow, 1, BSide::AllCorrect);

    let mut check = EcCheck::new();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&replicas, 2))
        .compiled();
    let results = check.run(&sim, async || {
        send.send(1);
    });

    println!("{results}");
    let cex = results
        .label("durable")
        .counterexample()
        .expect("a lost write leaves a correct replica without an acked write");
    assert_eq!(cex.crashes(), 0);
}

/// **Acking after every store is durable.** The client acks only once both replicas stored the
/// write: whatever it acked, every replica has (a crashed one had it before crashing).
#[test]
fn acking_after_every_store_is_durable() {
    let mut flow = FlowBuilder::new();
    let (_client, replicas, send) = replicated_store(&mut flow, 2, BSide::AllCorrect);

    let mut check = EcCheck::new();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&replicas, 2))
        .compiled();
    let results = check.run(&sim, async || {
        send.send(1);
    });

    println!("{results}");
    results.label("durable").assert_holds();
}

/// **Two copies survive one crash, not two.** Three replicas; the client acks once two have
/// stored the write. The rule asks for one replica that never crashes to hold every acked
/// write (`AtLeast(1)`). If one replica crashes, the other replica that acked still holds it.
/// If both replicas that acked crash, right after their acks, nobody correct holds the write.
#[test]
fn two_copies_survive_one_crash_but_not_two() {
    let mut flow = FlowBuilder::new();
    let (_client, replicas, send) = replicated_store(&mut flow, 2, BSide::AtLeast(1));

    let mut check = EcCheck::new().max_crashes(1);
    let sim = check
        .instrument(flow.sim().with_cluster_size(&replicas, 3))
        .compiled();
    let results = check.run(&sim, async || {
        send.send(1);
    });
    println!("{results}");
    results.label("durable").assert_holds();

    let check = check.max_crashes(2);
    let results = check.run(&sim, async || {
        send.send(1);
    });
    println!("{results}");
    let cex = results
        .label("durable")
        .counterexample()
        .expect("two crashes leave no correct replica with the write");
    assert_eq!(cex.crashes(), 2);
}
