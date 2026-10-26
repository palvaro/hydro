//! hydrofrac's regression tests on `implies` rules: the programs of
//! [`super::ec_implies_teaching`], and the other instances of the rule the design names
//! (docs/design/hydrofrac-implies.md), each with its verdict pinned down and compared with
//! both oracles over every fault set of up to [`MAX_FAULTS`] faults (`EcCheck::cross_check`).
//!
//! | test | rule | verdict |
//! |---|---|---|
//! | [`echo_broadcast_is_regular_not_uniform`] | deliveries, `Correct`/`Any` → `AllCorrect` | regular holds, uniform broken by 2 crashes |
//! | [`urb_is_regular_and_uniform`] | the same, on the uniform broadcast | both hold |
//! | [`validity_holds_on_reliable_links_and_breaks_on_lossy_ones`] | sends at a correct sender → deliveries, `AllCorrect` | holds; broken by a lost message |
//! | [`no_creation_holds_and_catches_a_made_up_delivery`] | deliveries → sends, `Some` | holds; broken without faults |
//! | [`acked_writes_are_durable_only_after_enough_stores`] | acks → stores | the teaching verdicts |

use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
use hydro_lang::prelude::*;
use hydro_std::ec_inference_demos::quorum::quorum;
use hydro_std::ec_inference_demos::reliable_broadcast::reliable_broadcast_closed;
use hydro_std::ec_inference_demos::uniform_broadcast::uniform_reliable_broadcast_closed;
use hydrofrac::check::{CrossCheck, LabelResult, Verdict};
use hydrofrac::search::Tolerance;
use hydrofrac::{ASide, BSide, EcCheck, Implies};

/// The node that broadcasts, or the client that writes.
pub struct Source;

/// The members of the broadcast, or the replicas.
pub struct Member;

/// The largest fault set the cross-check enumerates.
const MAX_FAULTS: usize = 2;

/// Prints the reports, and fails the test if the search and the oracles disagree.
fn agree(cross: &CrossCheck) {
    eprintln!("search:\n{}\nenumeration:\n{cross}", cross.results);
    cross.assert_agrees();
}

fn rule<'c>(cross: &'c CrossCheck, name: &str) -> &'c LabelResult {
    cross.results.label(name)
}

/// Fails unless `name` is certified under every number of crashes.
fn certified(cross: &CrossCheck, name: &str) {
    let r = rule(cross, name);
    assert!(
        matches!(
            r.verdict,
            Verdict::Certified {
                tolerates: Tolerance::All
            }
        ),
        "{name}: expected a certificate:\n{}",
        cross.results
    );
}

/// The crashes and the faults of `name`'s counterexample, which must have one.
fn broken(cross: &CrossCheck, name: &str) -> (usize, usize) {
    let r = rule(cross, name);
    let cex = r
        .counterexample()
        .unwrap_or_else(|| panic!("{name}: expected a counterexample:\n{}", cross.results));
    (cex.crashes(), cex.faults.len())
}

/// A broadcast from one process to two members, with both rules on its deliveries.
fn broadcast(uniform: bool) -> CrossCheck {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<Source>();
    let members = flow.cluster::<Member>();
    let (send, item) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let delivered = if uniform {
        uniform_reliable_broadcast_closed(item, &members, 1)
    } else {
        reliable_broadcast_closed(item, &members)
    };
    delivered.implies(
        "regular",
        &delivered,
        q!(|m| m),
        q!(|m| m),
        ASide::Correct,
        BSide::AllCorrect,
    );
    delivered.implies(
        "uniform",
        &delivered,
        q!(|m| m),
        q!(|m| m),
        ASide::Any,
        BSide::AllCorrect,
    );
    drop(delivered);
    let mut check = EcCheck::new();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .compiled();
    check.cross_check(
        &sim,
        async || {
            send.send(1);
        },
        MAX_FAULTS,
    )
}

/// The echo broadcast delivers before it forwards: the source crashing after reaching member 0,
/// and member 0 crashing right after its first echo (to itself), break uniformity, and nothing
/// less does. Regular broadcast holds.
#[test]
fn echo_broadcast_is_regular_not_uniform() {
    let cross = broadcast(false);
    agree(&cross);
    certified(&cross, "regular");
    assert_eq!(broken(&cross, "uniform"), (2, 2), "{}", cross.results);
}

/// The uniform broadcast echoes before it delivers: both rules hold.
#[test]
fn urb_is_regular_and_uniform() {
    let cross = broadcast(true);
    agree(&cross);
    certified(&cross, "regular");
    certified(&cross, "uniform");
}

/// Validity: what a correct source sends, every correct member receives. True over reliable
/// links; one lost message breaks it over lossy ones.
#[test]
fn validity_holds_on_reliable_links_and_breaks_on_lossy_ones() {
    for lossy in [false, true] {
        let mut flow = FlowBuilder::new();
        let source = flow.process::<Source>();
        let members = flow.cluster::<Member>();
        let (send, item) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
        if lossy {
            let received = item.clone().broadcast_closed(
                &members,
                TCP.lossy_delayed_forever().name("bcast").bincode(),
            );
            item.implies(
                "validity",
                &received,
                q!(|m| m),
                q!(|m| m),
                ASide::Correct,
                BSide::AllCorrect,
            );
        } else {
            let received = item
                .clone()
                .broadcast_closed(&members, TCP.fail_stop().name("bcast").bincode());
            item.implies(
                "validity",
                &received,
                q!(|m| m),
                q!(|m| m),
                ASide::Correct,
                BSide::AllCorrect,
            );
        }
        drop(item);
        let mut check = EcCheck::new();
        let sim = check
            .instrument(flow.sim().with_cluster_size(&members, 2))
            .compiled();
        let cross = check.cross_check(
            &sim,
            async || {
                send.send(1);
            },
            MAX_FAULTS,
        );
        agree(&cross);
        if lossy {
            assert_eq!(broken(&cross, "validity"), (0, 1), "{}", cross.results);
        } else {
            certified(&cross, "validity");
        }
    }
}

/// No creation: whatever a member delivers, some source sent, even one that crashed after
/// sending. A member that delivers a value no source sent breaks it with no fault at all.
#[test]
fn no_creation_holds_and_catches_a_made_up_delivery() {
    for made_up in [false, true] {
        let mut flow = FlowBuilder::new();
        let source = flow.process::<Source>();
        let members = flow.cluster::<Member>();
        let (send, item) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
        let received = item
            .clone()
            .broadcast_closed(&members, TCP.fail_stop().name("bcast").bincode());
        // The made-up delivery: each member delivers one more than it received.
        let delivered = if made_up {
            received.map(q!(|m| m + 1))
        } else {
            received.map(q!(|m| m))
        };
        delivered.implies(
            "no_creation",
            &item,
            q!(|m| m),
            q!(|m| m),
            ASide::Any,
            BSide::Some,
        );
        drop((item, delivered));
        let mut check = EcCheck::new();
        let sim = check
            .instrument(flow.sim().with_cluster_size(&members, 2))
            .compiled();
        let cross = check.cross_check(
            &sim,
            async || {
                send.send(1);
            },
            MAX_FAULTS,
        );
        agree(&cross);
        if made_up {
            assert_eq!(broken(&cross, "no_creation"), (0, 0), "{}", cross.results);
        } else {
            certified(&cross, "no_creation");
        }
    }
}

/// The teaching file's replicated store ([`super::ec_implies_teaching`]), duplicated.
fn durable(replicas: usize, acks: usize, b_side: BSide, max_crashes: Option<usize>) -> CrossCheck {
    let mut flow = FlowBuilder::new();
    let client = flow.process::<Source>();
    let cluster = flow.cluster::<Member>();
    let (send, writes) = client.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let stored = writes.broadcast_closed(
        &cluster,
        TCP.lossy_delayed_forever().name("write").bincode(),
    );
    let replies = stored
        .clone()
        .send(&client, TCP.fail_stop().name("ack").bincode())
        .entries()
        .map(q!(|(replica, write)| (write, replica)));
    let acked = quorum(acks, replies).map(q!(|durable| durable.into_fact()));
    acked.implies("durable", &stored, q!(|w| w), q!(|w| w), ASide::Correct, b_side);
    drop((acked, stored));
    let mut check = EcCheck::new();
    if let Some(k) = max_crashes {
        check = check.max_crashes(k);
    }
    let sim = check
        .instrument(flow.sim().with_cluster_size(&cluster, replicas))
        .compiled();
    check.cross_check(
        &sim,
        async || {
            send.send(1);
        },
        MAX_FAULTS,
    )
}

/// Acked ⇒ durable: acking after one store breaks by one lost write; acking after both holds;
/// with three replicas and two acks, one correct copy survives one crash but not two.
#[test]
fn acked_writes_are_durable_only_after_enough_stores() {
    let cross = durable(2, 1, BSide::AllCorrect, None);
    agree(&cross);
    assert_eq!(broken(&cross, "durable"), (0, 1), "{}", cross.results);

    let cross = durable(2, 2, BSide::AllCorrect, None);
    agree(&cross);
    certified(&cross, "durable");

    let cross = durable(3, 2, BSide::AtLeast(1), Some(1));
    agree(&cross);
    assert!(
        matches!(
            rule(&cross, "durable").verdict,
            Verdict::Certified {
                tolerates: Tolerance::UpTo(1)
            }
        ),
        "{}",
        cross.results
    );

    let cross = durable(3, 2, BSide::AtLeast(1), Some(2));
    agree(&cross);
    assert_eq!(broken(&cross, "durable").0, 2, "{}", cross.results);
}
