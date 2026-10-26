//! Teaching examples for hydrofrac: the smallest programs that show the classic ways a
//! broadcast loses agreement, and two that keep it.
//!
//! Each test is an ordinary Hydro simulation test plus a few lines of hydrofrac:
//!
//! ```ignore
//! let mut check = EcCheck::new();
//! let sim = check.instrument(flow.sim().with_cluster_size(&receivers, 2)).compiled();
//! let results = check.run(&sim, async || { send.send(1); });
//! ```
//!
//! `run` injects every admissible fault (crashes, and lost messages on links that may lose
//! them), explores the schedules, and checks every collection the program types as
//! `EventualConsistency`: **at the end of the run, every correct member holds every item some
//! correct member holds.** `broadcast_closed` types what it delivers that way, so every
//! broadcast is a claim to check, and so is everything computed from it (here, `delivered` and
//! `items`). `results.counterexample()` is the first counterexample found, if any, and
//! `results.label(name)` the verdict on the collection bound to `name`:
//! `results.label("delivered").assert_holds()` fails the test, with the label's report, unless
//! no admissible fault breaks it.
//!
//! Printing the results shows the verdict on each collection. A counterexample is also drawn
//! as a Lamport diagram under `target/hydrofrac/<test>/<label>/counterexample.svg`, and the
//! report's `diagram:` line names it.
//!
//! | test | nodes | outcome |
//! |---|---|---|
//! | [`a_sender_crash_mid_broadcast_leaves_a_receiver_without_the_item`] | sender, 2 receivers | broken by 1 crash |
//! | [`one_lost_message_leaves_a_receiver_without_the_item`] | sender, 2 receivers | broken by 1 lost message |
//! | [`a_cluster_sender_crash_mid_broadcast_leaves_a_receiver_without_the_item`] | 1 cluster sender, 2 receivers | broken by 1 crash |
//! | [`receivers_that_echo_survive_a_sender_crash`] | sender, 2 receivers | `delivered` holds under any crashes |
//! | [`two_redundant_senders_survive_one_crash_but_not_two`] | 2 cluster senders, 2 receivers | `items` hold under 1 crash, broken by 2 |
//!
//! The exact faults behind each verdict, and a brute-force cross-check of every verdict, are
//! hydrofrac's regression tests on the same programs, in `ec_teaching_checks.rs`.

use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
use hydro_lang::prelude::*;
use hydrofrac::EcCheck;

/// The node that sends the item.
pub struct Sender;

/// The nodes that should all end up with it.
pub struct Receiver;

/// **The sender crashes in the middle of its broadcast.** It sends the item to receiver 0, then
/// to receiver 1. Links never lose messages, but the sender can crash. If it crashes after its
/// first send, receiver 0 has the item and receiver 1 never gets it.
#[test]
fn a_sender_crash_mid_broadcast_leaves_a_receiver_without_the_item() {
    let mut flow = FlowBuilder::new();
    let sender = flow.process::<Sender>();
    let receivers = flow.cluster::<Receiver>();
    let (send, item) = sender.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let received = item.broadcast_closed(&receivers, TCP.fail_stop().name("bcast").bincode());
    let _out = received.sim_cluster_output();

    let mut check = EcCheck::new();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&receivers, 2))
        .compiled();
    let results = check.run(&sim, async || {
        send.send(1);
    });

    println!("{results}");
    let cex = results
        .counterexample()
        .expect("a crash mid-broadcast leaves a receiver without the item");
    assert_eq!(cex.crashes(), 1);
}

/// **One message is lost.** The same broadcast, over a link that may lose messages. Nothing
/// resends, so if the message to one receiver is lost, that receiver never gets the item while
/// the other has it. No crash is needed.
#[test]
fn one_lost_message_leaves_a_receiver_without_the_item() {
    let mut flow = FlowBuilder::new();
    let sender = flow.process::<Sender>();
    let receivers = flow.cluster::<Receiver>();
    let (send, item) = sender.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let received = item.broadcast_closed(
        &receivers,
        TCP.lossy_delayed_forever().name("bcast").bincode(),
    );
    let _out = received.sim_cluster_output();

    let mut check = EcCheck::new();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&receivers, 2))
        .compiled();
    let results = check.run(&sim, async || {
        send.send(1);
    });

    println!("{results}");
    let cex = results
        .counterexample()
        .expect("a lost message leaves a receiver without the item");
    assert_eq!(cex.crashes(), 0);
}

/// **A cluster sender crashes in the middle of its broadcast.** The same story as
/// [`a_sender_crash_mid_broadcast_leaves_a_receiver_without_the_item`], but the sender is the
/// one member of a cluster. What the receivers get is keyed by who sent it.
#[test]
fn a_cluster_sender_crash_mid_broadcast_leaves_a_receiver_without_the_item() {
    let mut flow = FlowBuilder::new();
    let senders = flow.cluster::<Sender>();
    let receivers = flow.cluster::<Receiver>();
    let (send, item) = senders.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let received = item.broadcast_closed(&receivers, TCP.fail_stop().name("bcast").bincode());
    let _out = received.entries().sim_cluster_output();

    let mut check = EcCheck::new();
    let sim = check
        .instrument(
            flow.sim()
                .with_cluster_size(&senders, 1)
                .with_cluster_size(&receivers, 2),
        )
        .compiled();
    let results = check.run(&sim, async || {
        send.send(0, 1);
    });

    println!("{results}");
    let cex = results
        .counterexample()
        .expect("a crash mid-broadcast leaves a receiver without the item");
    assert_eq!(cex.crashes(), 1);
}

/// **The fix: receivers echo.** Each receiver, the first time it sees the item, sends it on to
/// every receiver, and delivers it. If the sender crashes after reaching receiver 0, receiver 0
/// forwards the item to receiver 1. If a receiver crashes, the receiver left agrees with itself.
/// So `delivered` holds under every crash.
///
/// What a receiver gets straight from the sender (`from_sender`) is still the broken broadcast
/// of [`a_sender_crash_mid_broadcast_leaves_a_receiver_without_the_item`]. Its type still says
/// `EventualConsistency`, so hydrofrac still checks it and still breaks it with the same crash.
#[test]
fn receivers_that_echo_survive_a_sender_crash() {
    let mut flow = FlowBuilder::new();
    let sender = flow.process::<Sender>();
    let receivers = flow.cluster::<Receiver>();
    let (send, item) = sender.sim_input::<u32, TotalOrder, ExactlyOnce>();

    let from_sender = item.broadcast_closed(&receivers, TCP.fail_stop().name("bcast").bincode());
    let (echo_handle, from_receivers) = from_sender
        .location()
        .forward_ref::<Stream<u32, _, Unbounded, NoOrder>>();
    let delivered = from_sender.merge_unordered(from_receivers).unique();
    echo_handle.complete(
        delivered
            .clone()
            .broadcast_closed(&receivers, TCP.fail_stop().name("echo").bincode())
            .values(),
    );
    let _out = delivered.sim_cluster_output();

    let mut check = EcCheck::new();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&receivers, 2))
        .compiled();
    let results = check.run(&sim, async || {
        send.send(1);
    });

    println!("{results}");
    results.label("delivered").assert_holds();
    assert!(results.label("from_sender").counterexample().is_some());
}

/// **Plain redundancy masks one crash, but not two.** Two senders both hold the item and both
/// broadcast it to two receivers. This is redundancy, not a reliable broadcast: the receivers do
/// not echo. If one sender crashes mid-broadcast, the other still reaches both receivers, so the
/// receivers' `items` hold under one crash. Two crashes, each sender right after its send to
/// receiver 0, leave receiver 1 without the item.
///
/// Who sent each copy (`received`, keyed by sender) is not redundant: one crash breaks it, since
/// receiver 0 got a copy from sender 0 and receiver 1 did not.
#[test]
fn two_redundant_senders_survive_one_crash_but_not_two() {
    let mut flow = FlowBuilder::new();
    let senders = flow.cluster::<Sender>();
    let receivers = flow.cluster::<Receiver>();
    let (send, item) = senders.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let received = item.broadcast_closed(&receivers, TCP.fail_stop().name("bcast").bincode());
    let items = received.values();
    let _out = items.sim_cluster_output();

    let mut check = EcCheck::new().max_crashes(1);
    let sim = check
        .instrument(
            flow.sim()
                .with_cluster_size(&senders, 2)
                .with_cluster_size(&receivers, 2),
        )
        .compiled();
    let results = check.run(&sim, async || {
        send.send(0, 1);
        send.send(1, 1);
    });
    println!("{results}");
    results.label("items").assert_holds();
    assert!(results.label("received").counterexample().is_some());

    let check = check.max_crashes(2);
    let results = check.run(&sim, async || {
        send.send(0, 1);
        send.send(1, 1);
    });
    println!("{results}");
    let cex = results
        .label("items")
        .counterexample()
        .expect("two crashes leave a receiver without the item");
    assert_eq!(cex.crashes(), 2);
}
