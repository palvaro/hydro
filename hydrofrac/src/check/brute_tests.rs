//! The brute-force cross-check (docs/design/hydrofrac-lite.md §7, "Honesty check"): on small
//! inline programs, every admissible fault set of at most two faults is run without lineage
//! pruning, and the search's verdicts must agree with what the enumeration finds.
//!
//! Every item is `cfg(stageleft_runtime)`: the module must exist in the staged copy of the crate
//! (its staged closures resolve through it), while what the tests use is host-side only.

#[cfg(stageleft_runtime)]
use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
#[cfg(stageleft_runtime)]
use hydro_lang::location::cluster::EventualConsistency;
#[cfg(stageleft_runtime)]
use hydro_lang::prelude::*;

#[cfg(stageleft_runtime)]
use super::Counterexample;
#[cfg(stageleft_runtime)]
use super::{CrossCheck, EcCheck, Verdict};
#[cfg(stageleft_runtime)]
use crate::search::{Fault, FaultSet, Tolerance};

/// The largest fault set the enumeration covers.
#[cfg(stageleft_runtime)]
const MAX_FAULTS: usize = 2;

/// Prints the cross-check (run with `--nocapture` to see the tables) and asserts agreement.
#[cfg(stageleft_runtime)]
fn agree(name: &str, cross: &CrossCheck) {
    eprintln!("=== {name}\n{cross}");
    cross.assert_agrees();
}

/// A process broadcasts one input to three members (label `delivered`), over `fail_stop` or a
/// lossy channel.
#[cfg(stageleft_runtime)]
fn plain_broadcast(lossy: bool) -> CrossCheck {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    if lossy {
        let delivered = input.broadcast_closed(&members, TCP.lossy_delayed_forever().bincode());
        let _out = delivered.sim_cluster_output();
    } else {
        let delivered = input.broadcast_closed(&members, TCP.fail_stop().bincode());
        let _out = delivered.sim_cluster_output();
    }

    let mut check = EcCheck::new();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 3))
        .compiled();
    check.cross_check(
        &sim,
        async || {
            send.send(1);
        },
        MAX_FAULTS,
    )
}

/// A process broadcasts to `n` members over `initial`, and every member echoes what it got to
/// every member over `echo`; the label `delivered` is the union. `initial_lossy` makes the
/// first hop lossy (the echo stays `fail_stop`).
#[cfg(stageleft_runtime)]
fn echo_broadcast(n: usize, initial_lossy: bool) -> CrossCheck {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    // The two first hops have different types, so the rest of the program is spelled out for
    // each.
    macro_rules! echo_from {
        ($initial:expr) => {{
            let initial = $initial.weaken_consistency();
            let echoed = initial
                .clone()
                .broadcast_closed(&members, TCP.fail_stop().name("echo").bincode())
                .values()
                .weaken_consistency();
            let delivered = initial
                .weaken_ordering::<NoOrder>()
                .merge_unordered(echoed)
                .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
                    /** claimed: one correct recipient of the broadcast echoes it to everyone */
                ));
            let _out = delivered.sim_cluster_output();
        }};
    }
    if initial_lossy {
        echo_from!(input.broadcast_closed(
            &members,
            TCP.lossy_delayed_forever().name("initial").bincode(),
        ));
    } else {
        echo_from!(input.broadcast_closed(&members, TCP.fail_stop().name("initial").bincode()));
    }

    let mut check = EcCheck::new();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, n))
        .compiled();
    check.cross_check(
        &sim,
        async || {
            send.send(7);
        },
        MAX_FAULTS,
    )
}

#[cfg(stageleft_runtime)]
#[test]
fn brute_force_agrees_on_a_plain_broadcast() {
    let cross = plain_broadcast(false);
    agree("plain broadcast (fail_stop)", &cross);
    // The enumeration itself finds the mid fan-out crash of the source.
    let b = &cross.brute.labels[0];
    assert_eq!(b.min_crashes(), Some(1), "{cross}");
}

#[cfg(stageleft_runtime)]
#[test]
fn brute_force_agrees_on_a_lossy_broadcast() {
    let cross = plain_broadcast(true);
    agree("plain broadcast (lossy)", &cross);
    let b = &cross.brute.labels[0];
    assert_eq!(b.min_crashes(), Some(0), "omissions alone: {cross}");
    assert!(
        b.breaking[0].iter().all(|f| matches!(f, Fault::Omit(_))),
        "{cross}"
    );
}

#[cfg(stageleft_runtime)]
#[test]
fn brute_force_agrees_on_an_echo_broadcast() {
    let cross = echo_broadcast(3, false);
    agree("echo broadcast (3 members)", &cross);
    // The merged label survives one crash and two crashes break it, as the search says.
    let label = cross.results.label("delivered");
    let b = cross.brute.label(label.index).expect("searched");
    assert_eq!(b.min_crashes(), Some(2), "{cross}");
}

#[cfg(stageleft_runtime)]
#[test]
fn brute_force_agrees_on_a_two_member_exchange() {
    let cross = echo_broadcast(2, false);
    agree("echo broadcast (2 members)", &cross);
    let label = cross.results.label("delivered");
    assert!(
        matches!(
            label.verdict,
            Verdict::Certified {
                tolerates: Tolerance::All
            }
        ),
        "{cross}"
    );
    let b = cross.brute.label(label.index).expect("searched");
    assert!(b.breaking.is_empty(), "{cross}");
}

/// The echo masks omissions on a lossy first hop: a member the source's message never reached
/// still gets it from a recipient's echo.
#[cfg(stageleft_runtime)]
#[test]
fn brute_force_agrees_on_an_echo_over_a_lossy_first_hop() {
    let cross = echo_broadcast(3, true);
    agree("echo broadcast (lossy first hop)", &cross);
}

/// [`echo_broadcast`], but each member batches what it collected before it reports it: the
/// label `delivered` is schedule-sensitive (its batch is a choice of the simulator's), so its
/// search is a heuristic, and its certificate, if any, comes from enumerating its fault sets
/// (docs/design/hydrofrac-lite.md §4.9).
#[cfg(stageleft_runtime)]
fn batched_echo(n: usize, initial_lossy: bool) -> CrossCheck {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let tick = members.tick();
    macro_rules! echo_from {
        ($initial:expr) => {{
            let initial = $initial.weaken_consistency();
            let echoed = initial
                .clone()
                .broadcast_closed(&members, TCP.fail_stop().name("echo").bincode())
                .values()
                .weaken_consistency();
            let delivered = initial
                .weaken_ordering::<NoOrder>()
                .merge_unordered(echoed)
                .batch(&tick, nondet!(/** which tick a delivery lands in */))
                .all_ticks()
                .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
                    /** claimed: one correct recipient of the broadcast echoes it to everyone */
                ));
            let _out = delivered.sim_cluster_output();
        }};
    }
    if initial_lossy {
        echo_from!(input.broadcast_closed(
            &members,
            TCP.lossy_delayed_forever().name("initial").bincode(),
        ));
    } else {
        echo_from!(input.broadcast_closed(&members, TCP.fail_stop().name("initial").bincode()));
    }

    let mut check = EcCheck::new();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, n))
        .compiled();
    check.cross_check(
        &sim,
        async || {
            send.send(7);
        },
        MAX_FAULTS,
    )
}

/// The batched two-member exchange holds under every admissible fault set: the search's
/// enumeration certifies it, and the brute force agrees.
#[cfg(stageleft_runtime)]
#[test]
fn brute_force_agrees_with_a_certificate_by_enumeration() {
    let cross = batched_echo(2, false);
    agree("batched echo (2 members)", &cross);
    let label = cross.results.label("delivered");
    assert!(
        matches!(
            label.verdict,
            Verdict::CertifiedByEnumeration {
                tolerates: Tolerance::All,
                max_faults: None,
                ..
            }
        ),
        "{cross}"
    );
    let b = cross.brute.label(label.index).expect("searched");
    assert!(b.breaking.is_empty(), "{cross}");
}

/// Two members and a lossy first hop: the omission of either member's copy is masked by the
/// other's echo, and omitting both leaves no correct member holding anything (vacuous). The
/// enumeration runs every admissible set, omissions included, and the brute force agrees.
#[cfg(stageleft_runtime)]
#[test]
fn brute_force_agrees_on_a_batched_echo_over_a_lossy_first_hop() {
    let cross = batched_echo(2, true);
    agree("batched echo (lossy first hop)", &cross);
    let label = cross.results.label("delivered");
    assert!(
        matches!(
            label.verdict,
            Verdict::CertifiedByEnumeration {
                tolerates: Tolerance::All,
                max_faults: None,
                ..
            }
        ),
        "{cross}"
    );
}

/// A relay chain with no retries: a source sends two inputs over a lossy channel to a relay
/// process, which broadcasts them to three members over `fail_stop` (label `relayed`).
#[cfg(stageleft_runtime)]
#[test]
fn brute_force_agrees_on_a_lossy_relay_chain() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let relay = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let relayed = input
        .send(
            &relay,
            TCP.lossy_delayed_forever().name("to_relay").bincode(),
        )
        .broadcast_closed(&members, TCP.fail_stop().name("relayed").bincode());
    let _out = relayed.sim_cluster_output();

    let mut check = EcCheck::new();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 3))
        .compiled();
    let cross = check.cross_check(
        &sim,
        async || {
            send.send(1);
            send.send(2);
        },
        MAX_FAULTS,
    );
    agree("lossy relay chain", &cross);
}

/// A gossip-like exchange over `fail_stop`: the source broadcasts two inputs, each member keeps
/// the ones it owns (by id) and forwards them to every member; the label `known` is what a
/// member keeps or hears.
#[cfg(stageleft_runtime)]
#[test]
fn brute_force_agrees_on_an_ownership_exchange() {
    use hydro_lang::location::cluster::CLUSTER_SELF_ID;

    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let owned = input
        .broadcast_closed(&members, TCP.fail_stop().name("assign").bincode())
        .weaken_consistency()
        .filter(q!(move |x| *x % 3 == CLUSTER_SELF_ID.get_raw_id()));
    let heard = owned
        .clone()
        .broadcast_closed(&members, TCP.fail_stop().name("gossip").bincode())
        .values()
        .weaken_consistency();
    let known = owned
        .weaken_ordering::<NoOrder>()
        .merge_unordered(heard)
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** claimed: an owner tells everyone */
        ));
    let _out = known.sim_cluster_output();

    let mut check = EcCheck::new();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 3))
        .compiled();
    let cross = check.cross_check(
        &sim,
        async || {
            send.send(0);
            send.send(1);
        },
        MAX_FAULTS,
    );
    agree("ownership exchange", &cross);
}

/// Two independent inputs reach the same members: `a` over a lossy channel (the label), and
/// `b` over `fail_stop`, consumed at the same members but never part of `a`'s facts. Record
/// provenance blames only `a`'s deliveries; the enumeration checks that this precision costs
/// no soundness.
#[cfg(stageleft_runtime)]
#[test]
fn brute_force_agrees_where_provenance_precision_matters() {
    let mut flow = FlowBuilder::new();
    let source_a = flow.process::<()>();
    let source_b = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send_a, input_a) = source_a.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let (send_b, input_b) = source_b.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let a = input_a
        .broadcast_closed(&members, TCP.lossy_delayed_forever().name("a").bincode())
        .weaken_consistency();
    let b = input_b.broadcast_closed(&members, TCP.fail_stop().name("b").bincode());
    let echoed = a
        .clone()
        .broadcast_closed(&members, TCP.fail_stop().name("echo_a").bincode())
        .values()
        .weaken_consistency();
    let facts = a
        .weaken_ordering::<NoOrder>()
        .merge_unordered(echoed)
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** claimed: a recipient of `a` echoes it */
        ));
    let _facts = facts.sim_cluster_output();
    b.for_each(q!(|_| {}));

    let mut check = EcCheck::new();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 3))
        .compiled();
    let cross = check.cross_check(
        &sim,
        async || {
            send_a.send(1);
            send_b.send(2);
        },
        MAX_FAULTS,
    );
    agree("two independent inputs", &cross);
}

/// The comparison is not vacuous: wrong verdicts planted in the search's results are caught.
#[cfg(stageleft_runtime)]
#[test]
fn the_cross_check_catches_wrong_verdicts() {
    let mut cross = plain_broadcast(false);
    cross.assert_agrees();
    let index = cross.results.label("delivered").index;
    let label = cross
        .results
        .labels
        .iter_mut()
        .find(|l| l.index == index)
        .unwrap();
    let Verdict::Counterexample(original) = label.verdict.clone() else {
        panic!("expected a counterexample:\n{cross}");
    };

    // A certificate where one crash breaks the label.
    label.verdict = Verdict::Certified {
        tolerates: Tolerance::All,
    };
    cross.recheck();
    assert!(
        !cross.disagreements.is_empty(),
        "missed a wrong certificate"
    );

    // A certificate for one crash, likewise.
    let label = cross
        .results
        .labels
        .iter_mut()
        .find(|l| l.index == index)
        .unwrap();
    label.verdict = Verdict::Certified {
        tolerates: Tolerance::UpTo(1),
    };
    cross.recheck();
    assert!(
        !cross.disagreements.is_empty(),
        "missed a wrong bounded certificate"
    );

    // A counterexample claiming to tolerate the crash that breaks it.
    let label = cross
        .results
        .labels
        .iter_mut()
        .find(|l| l.index == index)
        .unwrap();
    label.verdict = Verdict::Counterexample(Box::new(Counterexample {
        tolerates: Some(1),
        ..(*original).clone()
    }));
    cross.recheck();
    assert!(
        !cross.disagreements.is_empty(),
        "missed an overstated tolerance"
    );

    // A counterexample whose fault set does not break the label.
    let label = cross
        .results
        .labels
        .iter_mut()
        .find(|l| l.index == index)
        .unwrap();
    label.verdict = Verdict::Counterexample(Box::new(Counterexample {
        faults: FaultSet::new(),
        tolerates: None,
        ..(*original).clone()
    }));
    cross.recheck();
    assert!(
        !cross.disagreements.is_empty(),
        "missed a bogus counterexample"
    );

    // The original verdict agrees again.
    let label = cross
        .results
        .labels
        .iter_mut()
        .find(|l| l.index == index)
        .unwrap();
    label.verdict = Verdict::Counterexample(original);
    cross.recheck();
    cross.assert_agrees();
}
