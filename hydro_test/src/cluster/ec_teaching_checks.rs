//! hydrofrac's regression tests on the teaching programs of [`super::ec_teaching`]: the same
//! five programs, with the exact verdicts asserted.
//!
//! The teaching file reads like a user's tests and asserts only what a user would. This file
//! pins down what hydrofrac must report for each program: the faults of each counterexample
//! (which node crashes, and where, or which message is lost), the crash tolerance, and the
//! certificate on the echo. Every test also compares hydrofrac's verdicts with a brute-force
//! enumeration of every fault set of up to [`MAX_FAULTS`] faults (`EcCheck::cross_check`).
//!
//! The programs are duplicated from the teaching file rather than shared, so each file stays
//! self-contained. The tests keep the teaching tests' names; their diagrams land in the same
//! `target/hydrofrac/<test>/` directories, under labels suffixed by this file's name
//! (`received_ec_teaching_checks.rs/`), so the two do not overwrite each other.
//!
//! | test | nodes | faults that break it |
//! |---|---|---|
//! | [`a_sender_crash_mid_broadcast_leaves_a_receiver_without_the_item`] | sender, 2 receivers | 1 crash |
//! | [`one_lost_message_leaves_a_receiver_without_the_item`] | sender, 2 receivers | 1 lost message |
//! | [`a_cluster_sender_crash_mid_broadcast_leaves_a_receiver_without_the_item`] | 1 cluster sender, 2 receivers | 1 crash |
//! | [`receivers_that_echo_survive_a_sender_crash`] | sender, 2 receivers | none (certified) |
//! | [`two_redundant_senders_survive_one_crash_but_not_two`] | 2 cluster senders, 2 receivers | 2 crashes |
//!
//! The property is the one `EventualConsistency` stands for: **at the end of the run, every
//! correct member holds every item some correct member holds.** The first three programs have
//! no output (their streams are dropped once built): the broadcast's own label is the one that
//! breaks. The last two output the collection whose agreement is the point (`delivered`,
//! `items`).

use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
use hydro_lang::location::cluster::EventualConsistency;
use hydro_lang::prelude::*;
use hydrofrac::EcCheck;
use hydrofrac::check::{CrossCheck, LabelResult, Verdict};
use hydrofrac::search::{CrashAt, Fault, Tolerance};

/// The node that sends the item.
pub struct Sender;

/// The nodes that should all end up with it.
pub struct Receiver;

/// The largest fault set the brute-force cross-check enumerates.
const MAX_FAULTS: usize = 2;

/// The item every program broadcasts.
const ITEM: u32 = 1;

/// Prints both reports, and fails the test if the search and the enumeration disagree.
fn agree(cross: &CrossCheck) {
    eprintln!("search:\n{}\nenumeration:\n{cross}", cross.results);
    cross.assert_agrees();
}

/// The verdict on the label bound to `name`.
fn verdict<'c>(cross: &'c CrossCheck, name: &str) -> &'c LabelResult {
    cross.results.label(name)
}

/// The faults of the counterexample on `label`, which must have one.
fn faults<'l>(
    cross: &CrossCheck,
    label: &'l LabelResult,
) -> Vec<&'l Fault<hydrofrac::fault::MessageId>> {
    label
        .verdict
        .counterexample()
        .unwrap_or_else(|| {
            panic!(
                "{}: expected a counterexample:\n{}",
                label.name(),
                cross.results
            )
        })
        .faults
        .iter()
        .collect()
}

/// Which transport the broadcast declares.
#[derive(Clone, Copy)]
enum Transport {
    /// Messages are never lost, but a node may crash (and stay down).
    FailStop,
    /// As [`Self::FailStop`], and messages may also be lost (delayed forever).
    LossyDelayedForever,
}

/// A process sends [`ITEM`] once to each of two receivers. The labelled collection is
/// `received`.
fn one_item_to_two_receivers(transport: Transport) -> CrossCheck {
    let mut flow = FlowBuilder::new();
    let sender = flow.process::<Sender>();
    let receivers = flow.cluster::<Receiver>();
    let (send, item) = sender.sim_input::<u32, TotalOrder, ExactlyOnce>();
    match transport {
        Transport::FailStop => {
            let received =
                item.broadcast_closed(&receivers, TCP.fail_stop().name("bcast").bincode());
            drop(received);
        }
        Transport::LossyDelayedForever => {
            let received = item.broadcast_closed(
                &receivers,
                TCP.lossy_delayed_forever().name("bcast").bincode(),
            );
            drop(received);
        }
    }

    let mut check = EcCheck::new();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&receivers, 2))
        .compiled();
    check.cross_check(
        &sim,
        async || {
            send.send(ITEM);
        },
        MAX_FAULTS,
    )
}

/// **A crash in the middle of a broadcast.** The sender sends the item to receiver 0, then to
/// receiver 1. Links never lose messages, but the sender can crash, and if it crashes between
/// its two sends, receiver 0 has the item and receiver 1 never will. One crash breaks it.
#[test]
fn a_sender_crash_mid_broadcast_leaves_a_receiver_without_the_item() {
    let cross = one_item_to_two_receivers(Transport::FailStop);
    agree(&cross);
    let received = verdict(&cross, "received");
    let [Fault::Crash { node, at }] = faults(&cross, received)[..] else {
        panic!("expected one crash:\n{}", cross.results);
    };
    assert!(
        node.starts_with("Sender"),
        "the sender crashes:\n{}",
        cross.results
    );
    assert!(
        matches!(at, CrashAt::After(_)),
        "after its first send:\n{}",
        cross.results
    );
}

/// **One lost message.** The same broadcast over a link that may lose messages. Nothing
/// resends, so losing the one message to one receiver leaves it without the item while the
/// other has it. No crash is needed: hydrofrac reports that omissions alone break it.
#[test]
fn one_lost_message_leaves_a_receiver_without_the_item() {
    let cross = one_item_to_two_receivers(Transport::LossyDelayedForever);
    agree(&cross);
    let received = verdict(&cross, "received");
    let [Fault::Omit(_)] = faults(&cross, received)[..] else {
        panic!("expected one lost message:\n{}", cross.results);
    };
    let cex = received.verdict.counterexample().expect("a counterexample");
    assert_eq!(
        cex.tolerates, None,
        "no crash is needed:\n{}",
        cross.results
    );
}

/// **A cluster sender's crash.** The sender is now a member of a cluster (of one), sending to a
/// cluster of two receivers. Its crash between its two sends breaks agreement exactly as in
/// [`a_sender_crash_mid_broadcast_leaves_a_receiver_without_the_item`]: what the receivers get
/// is keyed by who sent it, but the story is the same.
#[test]
fn a_cluster_sender_crash_mid_broadcast_leaves_a_receiver_without_the_item() {
    let mut flow = FlowBuilder::new();
    let senders = flow.cluster::<Sender>();
    let receivers = flow.cluster::<Receiver>();
    let (send, item) = senders.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let received = item.broadcast_closed(&receivers, TCP.fail_stop().name("bcast").bincode());
    drop(received);

    let mut check = EcCheck::new();
    let sim = check
        .instrument(
            flow.sim()
                .with_cluster_size(&senders, 1)
                .with_cluster_size(&receivers, 2),
        )
        .compiled();
    let cross = check.cross_check(
        &sim,
        async || {
            send.send(0, ITEM);
        },
        MAX_FAULTS,
    );
    agree(&cross);
    let received = verdict(&cross, "received");
    let [Fault::Crash { node, at }] = faults(&cross, received)[..] else {
        panic!("expected one crash:\n{}", cross.results);
    };
    assert!(
        node.starts_with("Sender@"),
        "the sender crashes:\n{}",
        cross.results
    );
    assert!(matches!(at, CrashAt::After(_)), "{}", cross.results);
}

/// **A fix that works: receivers echo.** Each receiver, the first time it sees the item,
/// sends it on to every receiver before delivering it. If the sender crashes after reaching
/// receiver 0, receiver 0 forwards the item to receiver 1. And if a receiver crashes, the only
/// correct receiver left agrees with itself. hydrofrac certifies the deliveries under every
/// admissible crash set. Two receivers are the fewest that have anything to agree on.
///
/// What the receivers got *straight from the sender* (`from_sender`) is still the broken
/// broadcast of [`a_sender_crash_mid_broadcast_leaves_a_receiver_without_the_item`], and
/// hydrofrac still reports it, with the same crash. Only `delivered`, which also counts the
/// echoes, is fixed.
#[test]
fn receivers_that_echo_survive_a_sender_crash() {
    let mut flow = FlowBuilder::new();
    let sender = flow.process::<Sender>();
    let receivers = flow.cluster::<Receiver>();
    let (send, item) = sender.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let _out = echo_broadcast(item, &receivers).sim_cluster_output();

    let mut check = EcCheck::new();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&receivers, 2))
        .compiled();
    let cross = check.cross_check(
        &sim,
        async || {
            send.send(ITEM);
        },
        MAX_FAULTS,
    );
    agree(&cross);
    let delivered = verdict(&cross, "delivered");
    assert!(
        matches!(
            delivered.verdict,
            Verdict::Certified {
                tolerates: Tolerance::All
            }
        ),
        "the echo is certified under every admissible crash set:\n{}",
        cross.results
    );
    let from_sender = verdict(&cross, "from_sender");
    let [Fault::Crash { node, .. }] = faults(&cross, from_sender)[..] else {
        panic!(
            "the plain broadcast underneath still breaks:\n{}",
            cross.results
        );
    };
    assert!(node.starts_with("Sender"), "{}", cross.results);
}

/// The echo broadcast of [`receivers_that_echo_survive_a_sender_crash`]: deliver what arrives
/// from the sender or from another receiver, once, and forward it to every receiver.
fn echo_broadcast<'a>(
    item: Stream<u32, Process<'a, Sender>, Unbounded, TotalOrder>,
    receivers: &Cluster<'a, Receiver>,
) -> Stream<u32, Cluster<'a, Receiver, EventualConsistency>, Unbounded, NoOrder> {
    let from_sender = item.broadcast_closed(receivers, TCP.fail_stop().name("bcast").bincode());
    let (echo_handle, from_receivers) = from_sender
        .location()
        .forward_ref::<Stream<u32, _, Unbounded, NoOrder>>();
    let delivered = from_sender.merge_unordered(from_receivers).unique();
    echo_handle.complete(
        delivered
            .clone()
            .broadcast_closed(receivers, TCP.fail_stop().name("echo").bincode())
            .values(),
    );
    delivered
}

/// **Plain redundancy masks one crash, but not two.** Not a reliable broadcast: the receivers
/// do not echo. Two senders (a cluster of two) both hold the item and both broadcast it to two
/// receivers. If one sender crashes mid-broadcast, the other still reaches both receivers. It
/// takes two crashes, one per sender, each right after its send to receiver 0, to leave
/// receiver 1 without the item. hydrofrac certifies that no single crash breaks the receivers'
/// items, and finds the two crashes.
///
/// The receivers also see who sent each copy (`received`, keyed by sender). That view loses
/// agreement under one crash: receiver 0 got a copy *from sender 0*, receiver 1 did not. Only
/// the copies' contents (`items`) are redundant.
#[test]
fn two_redundant_senders_survive_one_crash_but_not_two() {
    let mut flow = FlowBuilder::new();
    let senders = flow.cluster::<Sender>();
    let receivers = flow.cluster::<Receiver>();
    let (send, item) = senders.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let received = item.broadcast_closed(&receivers, TCP.fail_stop().name("bcast").bincode());
    let items = received.values();
    let _out = items.sim_cluster_output();

    let mut check = EcCheck::new();
    let sim = check
        .instrument(
            flow.sim()
                .with_cluster_size(&senders, 2)
                .with_cluster_size(&receivers, 2),
        )
        .compiled();
    let cross = check.cross_check(
        &sim,
        async || {
            send.send(0, ITEM);
            send.send(1, ITEM);
        },
        MAX_FAULTS,
    );
    agree(&cross);

    let items = verdict(&cross, "items");
    let cex = items
        .verdict
        .counterexample()
        .unwrap_or_else(|| panic!("two crashes break `items`:\n{}", cross.results));
    assert_eq!(
        cex.tolerates,
        Some(1),
        "one crash is tolerated:\n{}",
        cross.results
    );
    assert!(cex.tolerance_certified, "{}", cross.results);
    let crashed: Vec<_> = faults(&cross, items)
        .into_iter()
        .map(|f| match f {
            Fault::Crash { node, at } => {
                assert!(matches!(at, CrashAt::After(_)), "{}", cross.results);
                node.as_str()
            }
            Fault::Omit(_) => panic!("no message is lost on `fail_stop`:\n{}", cross.results),
        })
        .collect();
    assert_eq!(crashed.len(), 2, "{}", cross.results);
    assert!(
        crashed.iter().all(|n| n.starts_with("Sender@")),
        "both senders crash:\n{}",
        cross.results
    );

    let received = verdict(&cross, "received");
    let cex = received
        .verdict
        .counterexample()
        .unwrap_or_else(|| panic!("one crash breaks `received`:\n{}", cross.results));
    assert_eq!(cex.crashes(), 1, "{}", cross.results);
}
