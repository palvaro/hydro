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
    let mut check = EcCheck::new();
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
    let mut check = EcCheck::new();
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
    let cex = cross
        .results
        .label("delivered")
        .verdict
        .counterexample()
        .expect("a mid fan-out crash");
    assert_eq!(cex.crashes(), 1, "{cross}");
    assert!(cex.tolerance_certified, "{cross}");
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
    let cex = cross
        .results
        .label("delivered")
        .verdict
        .counterexample()
        .expect("an omission");
    assert_eq!(cex.crashes(), 0, "{cross}");
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
    let cex = label
        .verdict
        .counterexample()
        .expect("two crashes break it");
    assert_eq!(cex.crashes(), 2, "{cross}");
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

/// A relay chain with no retries: a source sends two inputs over a lossy channel to a relay
/// process, which broadcasts them to three members over `fail_stop` (label `relayed`).
#[cfg(stageleft_runtime)]
#[test]
fn brute_force_agrees_on_a_lossy_relay_chain() {
    agree("lossy relay chain", &lossy_relay_chain());
}

/// The relay chain of [`brute_force_agrees_on_a_lossy_relay_chain`].
#[cfg(stageleft_runtime)]
fn lossy_relay_chain() -> CrossCheck {
    let mut check = EcCheck::new();
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

    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 3))
        .compiled();
    check.cross_check(
        &sim,
        async || {
            send.send(1);
            send.send(2);
        },
        MAX_FAULTS,
    )
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

/// Suspected soundness bug S1: a clause learned in a run where a fact vanished everywhere says
/// nothing about that fact, yet the search treats it as a constraint on every hypothesis.
///
/// Two members (`m0`, `m1`), two facts in the label `known`:
/// - `f = 0`: the source `s` broadcasts it over the lossy `assign`; only `m0` keeps it (it owns
///   the even values), and `m0` gossips what it keeps to every member over the lossy `gossip`;
/// - `g = 101`: a second process `t` broadcasts it to both members over `fail_stop` (`steady`).
///
/// `omit(gossip m0→m1)` alone breaks the label (`m0` holds `f`, `m1` never does). But the
/// omissions the probe's clause proposes are tried in channel order, so the search first tries
/// `omit(assign s→m0)`: `f` is then lost at *every* member, `POST` holds on `g` alone, and the
/// run's clause is `g`'s derivation (two `fail_stop` deliveries from `t`). No omission kills it,
/// so the zero-crash bound is declared exhausted and `omit(gossip m0→m1)` is never tried.
///
/// Returns the cross-check, with the search capped at `max_crashes` crashes if given.
#[cfg(stageleft_runtime)]
fn total_loss_masks_a_partial_loss(max_crashes: Option<usize>) -> CrossCheck {
    use hydro_lang::location::cluster::CLUSTER_SELF_ID;

    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let steady = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send_f, input_f) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let (send_g, input_g) = steady.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let owned = input_f
        .broadcast_closed(
            &members,
            TCP.lossy_delayed_forever().name("assign").bincode(),
        )
        .weaken_consistency()
        .filter(q!(move |x| *x % 2 == CLUSTER_SELF_ID.get_raw_id()));
    let heard = owned
        .clone()
        .broadcast_closed(
            &members,
            TCP.lossy_delayed_forever().name("gossip").bincode(),
        )
        .values()
        .weaken_consistency();
    let g = input_g
        .broadcast_closed(&members, TCP.fail_stop().name("steady").bincode())
        .weaken_consistency()
        .weaken_ordering::<NoOrder>();
    let known = owned
        .weaken_ordering::<NoOrder>()
        .merge_unordered(heard)
        .merge_unordered(g)
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** claimed (wrongly): the owner tells everyone */
        ));
    let _out = known.sim_cluster_output();

    let mut check = match max_crashes {
        Some(n) => EcCheck::new().max_crashes(n),
        None => EcCheck::new(),
    };
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .compiled();
    check.cross_check(
        &sim,
        async || {
            send_f.send(0);
            send_g.send(101);
        },
        MAX_FAULTS,
    )
}

/// S1 with the search capped at zero crashes. Lite's union clauses certified it falsely; the
/// per-pair clauses keep (m1, f)'s derivation from the probe, and `omit(gossip m0→m1)` alone
/// breaks the label, as the enumeration finds.
#[cfg(stageleft_runtime)]
#[test]
fn suspected_total_loss_masks_a_partial_loss_certificate() {
    let cross = total_loss_masks_a_partial_loss(Some(0));
    eprintln!("=== S1 (max_crashes 0)\n{cross}\n{}", cross.results);
    assert!(
        cross.results.choices.is_empty(),
        "not choice-free: {:?}",
        cross.results.choices
    );
    assert_breaks_by_the_gossip_omission(&cross);
}

/// S1 uncapped: the counterexample is the zero-crash omission (lite claimed to tolerate every
/// zero-crash fault set).
#[cfg(stageleft_runtime)]
#[test]
fn suspected_total_loss_masks_a_partial_loss_tolerance() {
    let cross = total_loss_masks_a_partial_loss(None);
    eprintln!("=== S1 (uncapped)\n{cross}\n{}", cross.results);
    assert_breaks_by_the_gossip_omission(&cross);
}

/// The search agrees with the enumeration on S1, and breaks `known` with no crash.
#[cfg(stageleft_runtime)]
fn assert_breaks_by_the_gossip_omission(cross: &CrossCheck) {
    cross.assert_agrees();
    let label = cross.results.label("known");
    let cex = label
        .verdict
        .counterexample()
        .unwrap_or_else(|| panic!("omissions alone break it: {cross}"));
    assert_eq!(cex.crashes(), 0, "{cross}");
    assert!(cex.tolerance_certified, "{cross}");
    let b = cross.brute.label(label.index).expect("searched");
    assert_eq!(b.min_crashes(), Some(0), "{cross}");
}

/// The verdict on `label`, and its races notes.
#[cfg(stageleft_runtime)]
fn races_label<'c>(cross: &'c CrossCheck, label: &str) -> (&'c Verdict, super::RaceNotes) {
    let l = cross.results.label(label);
    let notes = l
        .stats
        .races
        .unwrap_or_else(|| panic!("no races notes: {cross}"));
    (&l.verdict, notes)
}

/// An echo broadcast whose members batch what they receive (a `nondet!` choice) and only map
/// it before echoing: the program has schedule choices but no fragile step, so every
/// derivation is robust, every run of a set has the same facts, and the label is certified.
#[cfg(stageleft_runtime)]
#[test]
fn races_certify_a_batched_echo() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let initial = input
        .broadcast_closed(&members, TCP.fail_stop().name("initial").bincode())
        .weaken_consistency();
    let tick = members.tick();
    let echoed = initial
        .clone()
        .batch(&tick, nondet!(/** explored by the check */))
        .map(q!(|x| x))
        .all_ticks()
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

    let mut check = EcCheck::new().max_crashes(1);
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 3))
        .compiled();
    assert_eq!(check.fragile_sites(), Some(vec![]));
    let cross = check.cross_check(
        &sim,
        async || {
            send.send(7);
        },
        MAX_FAULTS,
    );
    agree("batched echo", &cross);
    assert!(!cross.results.choices.is_empty(), "{cross}");
    let label = cross.results.label("delivered");
    assert!(
        matches!(
            label.verdict,
            Verdict::Certified {
                tolerates: Tolerance::UpTo(1)
            }
        ),
        "{cross}"
    );
    let notes = label.stats.races.expect("races notes");
    assert!(notes.claim_b, "{cross}");
    // One schedule per set: start crashes run once, the others stop at their first schedule.
    assert_eq!(label.stats.schedules, label.stats.hypotheses, "{cross}");
}

/// A planted bug: an echo broadcast whose members re-forward nothing (the echo filters every
/// value out). A crash of the source mid fan-out must still be found by the races mode, now
/// that the fan-out order spares the members before the crash point.
#[cfg(stageleft_runtime)]
#[test]
fn races_find_a_broadcast_whose_members_do_not_re_forward() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let initial = input
        .broadcast_closed(&members, TCP.fail_stop().name("initial").bincode())
        .weaken_consistency();
    let echoed = initial
        .clone()
        .filter(q!(|_| false))
        .broadcast_closed(&members, TCP.fail_stop().name("echo").bincode())
        .values()
        .weaken_consistency();
    let delivered = initial
        .weaken_ordering::<NoOrder>()
        .merge_unordered(echoed)
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** claimed (wrongly): recipients echo what they get */
        ));
    let _out = delivered.sim_cluster_output();

    let mut check = EcCheck::new();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 3))
        .compiled();
    let cross = check.cross_check(
        &sim,
        async || {
            send.send(7);
        },
        MAX_FAULTS,
    );
    agree("broadcast without re-forwarding", &cross);
    let (verdict, _) = races_label(&cross, "delivered");
    let cex = verdict
        .counterexample()
        .unwrap_or_else(|| panic!("a mid fan-out crash of the source breaks it: {cross}"));
    assert_eq!(cex.crashes(), 1, "{cross}");
    assert!(cex.faults.iter().all(|f| matches!(
        f,
        Fault::Crash {
            at: crate::search::CrashAt::After(_),
            ..
        }
    )));
}

// Suspected soundness bug S2: a crash point's meaning moves with the sender's send order, which a
// fault elsewhere can change (docs/design/hydrofrac-lite.md §4.5).

/// The S2 relay: a fact `y` reaches the relay `n` twice. `direct` (lossy, one hop from `s`) asks
/// `n` to forward `y` to `m0` only; the slower copy (two `fail_stop` hops, through `r1` and,
/// redundantly, `r2`) asks it to forward `y` to `m1`, then to `m0`. `m1` echoes what it gets to
/// `m0`. Returns the cross-check, enumerating sets of at most `max_faults` faults, with the
/// search capped at `max_crashes` crashes if given.
#[cfg(stageleft_runtime)]
fn positional_crash_order_relay(max_faults: usize, max_crashes: Option<usize>) -> CrossCheck {
    let mut check = EcCheck::new();
    use hydro_lang::location::cluster::CLUSTER_SELF_ID;

    let mut flow = FlowBuilder::new();
    let fast_source = flow.process::<()>();
    let s1 = flow.process::<()>();
    let s2 = flow.process::<()>();
    let r1 = flow.process::<()>();
    let r2 = flow.process::<()>();
    let relay = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send_fast, fast) = fast_source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let (send_1, in_1) = s1.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let (send_2, in_2) = s2.sim_input::<u32, TotalOrder, ExactlyOnce>();
    // Two independent slow paths, so one crash never leaves only the fast copy.
    let via_r1 = in_1
        .send(&r1, TCP.fail_stop().name("to_r1").bincode())
        .send(&relay, TCP.fail_stop().name("r1_n").bincode());
    let via_r2 = in_2
        .send(&r2, TCP.fail_stop().name("to_r2").bincode())
        .send(&relay, TCP.fail_stop().name("r2_n").bincode());
    let direct = fast.send(&relay, TCP.lossy_delayed_forever().name("direct").bincode());
    let slow = via_r1
        .weaken_ordering::<NoOrder>()
        .merge_unordered(via_r2.weaken_ordering::<NoOrder>())
        .map(q!(|y| (y, true)));
    let fanout = direct
        .map(q!(|y| (y, false)))
        .merge_unordered(slow)
        .flat_map_ordered(q!(|(y, slow)| if slow {
            vec![(1u32, y), (0u32, y)]
        } else {
            vec![(0u32, y)]
        }))
        .map(q!(|(to, y)| (
            hydro_lang::location::MemberId::<()>::from_raw_id(to),
            y
        )))
        .demux(&members, TCP.fail_stop().name("fanout").bincode())
        .weaken_consistency();
    let echoed = fanout
        .clone()
        .filter(q!(move |_| CLUSTER_SELF_ID.get_raw_id() == 1))
        .map(q!(|y| (
            hydro_lang::location::MemberId::<()>::from_raw_id(0),
            y
        )))
        .demux(&members, TCP.fail_stop().name("echo").bincode())
        .values()
        .weaken_consistency();
    let known = fanout
        .weaken_ordering::<NoOrder>()
        .merge_unordered(echoed.weaken_ordering::<NoOrder>())
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** claimed (wrongly): `m0` hears from `n` or from `m1`'s echo */
        ));
    let _out = known.sim_cluster_output();

    if let Some(n) = max_crashes {
        check = check.max_crashes(n);
    }
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .compiled();
    check.cross_check(
        &sim,
        async || {
            send_fast.send(7);
            send_1.send(7);
            send_2.send(7);
        },
        max_faults,
    )
}

/// The fault set that breaks the relay with one crash (up to its payload's rendering): `n`
/// crashes right after its first send to `m0`.
#[cfg(stageleft_runtime)]
const RELAY_BREAK: &str = "crash(process#5 after fanout[process#5→cluster@0]: ";

/// Suspected soundness bug S2 (demonstrated in lite, whose positional crash rule is deleted):
/// lite reported a counterexample with two crashes and claimed (as certified) that the label
/// tolerates one, while one crash breaks it. The order-free crash rule finds the one crash.
///
/// The program ([`positional_crash_order_relay`]): two members `m0`, `m1`, label `known`, one
/// fact `7`. The relay `n` (`process#5`) gets `7` twice: first over the lossy one-hop `direct`,
/// which asks it to forward to `m0` only; then over two redundant `fail_stop` two-hop paths
/// (through `r1`, `r2`), which ask it to forward to `m1`, then `m0`. `m1` echoes to `m0`. There
/// is no `nondet!`: one execution per fault set.
///
/// - **Probe:** `n` sends `→m0, →m1, →m0, …`. The clause names `→m0` (position 0) and `→m1`
///   (position 1).
/// - **`{omit(direct)}`** (tried at zero crashes): only the slow copies arrive, so `n` sends
///   `→m1, →m0, …`. The label holds; the clause names `→m1` at position 0 and `→m0` at
///   position 1.
/// - **`H = {crash(n after →m0)}`:** without the omission, `n` sends `→m0` first, as in the
///   probe, and dies before `→m1`. `m0` holds `7`, `m1` never does: the label breaks with one
///   crash. But in `{omit(direct)}`'s run `→m0` came *after* `→m1`, so the kill relation
///   (lite's `Dependency::killed_by`, read in the clause's own run) said `H` kills nothing there:
///   `m1`'s old derivation "survived" `H`, and `H` was never proposed. The order-free rule says
///   `crash(n after →m0)` kills `→m1` (their order is not proved), so `H` is proposed.
/// - Lite: every other one-crash set either holds (the echo covers `{omit(direct), crash(n after
///   →m1)}`; the redundant paths cover `r1`, `r2` and the sources) or hits no clause. So bound 1
///   was declared exhausted, and lite broke at 2 with `{crash(s1), crash(s2)}`.
///
/// The breaking pair `(m1, 7)` is held in the run whose clause excludes `H`, with a derivation
/// through `n`'s `→m1`: this is the send-order flip (S2), not a fact lost everywhere (S1).
#[cfg(stageleft_runtime)]
#[test]
fn suspected_positional_crash_order_tolerance() {
    for max_faults in [2, 3, 4] {
        let cross = positional_crash_order_relay(max_faults, None);
        eprintln!(
            "=== S2 relay (uncapped, {max_faults} faults)\n{cross}\nsearch:\n{}",
            cross.results
        );
        assert!(
            cross.results.choices.is_empty(),
            "not choice-free: {:?}",
            cross.results.choices
        );
        assert_breaks_at_the_relay(&cross);
    }
}

/// S2 with the search capped at one crash: lite certified "holds with up to 1 crash"; the search
/// finds `{crash(n after →m0)}`. See [`suspected_positional_crash_order_tolerance`].
#[cfg(stageleft_runtime)]
#[test]
fn suspected_positional_crash_order_certificate() {
    let cross = positional_crash_order_relay(3, Some(1));
    eprintln!(
        "=== S2 relay (max_crashes 1, 3 faults)\n{cross}\nsearch:\n{}",
        cross.results
    );
    assert_breaks_at_the_relay(&cross);
}

/// The search agrees with the enumeration on the S2 relay, and breaks `known` with the one crash
/// `{crash(n after →m0)}`, its tolerance of none certified.
#[cfg(stageleft_runtime)]
fn assert_breaks_at_the_relay(cross: &CrossCheck) {
    cross.assert_agrees();
    let label = cross.results.label("known");
    let cex = label
        .verdict
        .counterexample()
        .unwrap_or_else(|| panic!("expected a counterexample: {cross}"));
    assert_eq!(cex.crashes(), 1, "{cross}");
    assert_eq!(cex.tolerates, Some(0), "{cross}");
    assert!(cex.tolerance_certified, "{cross}");
    let b = cross.brute.label(label.index).expect("searched");
    assert_eq!(b.min_crashes(), Some(1), "{cross}");
    assert!(
        cross
            .results
            .report_names()
            .fault_set(&b.breaking[0])
            .starts_with(&format!("{{{RELAY_BREAK}")),
        "{cross}"
    );
}

/// A top-level `scan` over an order the schedule picks (`assume_ordering` at top level is a
/// simulator hook): two sources each send one value to two members, and each member keeps the
/// first value it sees. The members can disagree with no fault at all. Before `fragile.rs` became
/// an allowlist, folds and scans were fragile only inside a tick, so this program counted as
/// fragile-free, ran on one schedule, and was certified; brute force broke it with the empty set.
#[cfg(stageleft_runtime)]
#[test]
fn a_top_level_scan_over_a_chosen_order_is_fragile() {
    let mut flow = FlowBuilder::new();
    let a = flow.process::<()>();
    let b = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send_a, in_a) = a.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let (send_b, in_b) = b.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let from_a = in_a
        .broadcast_closed(&members, TCP.fail_stop().name("a").bincode())
        .weaken_ordering::<NoOrder>();
    let from_b = in_b
        .broadcast_closed(&members, TCP.fail_stop().name("b").bincode())
        .weaken_ordering::<NoOrder>();
    let firsts = from_a
        .merge_unordered(from_b)
        .assume_ordering::<TotalOrder>(nondet!(/** the order a member sees */))
        .scan(
            q!(|| false),
            q!(|seen, x| if *seen {
                None
            } else {
                *seen = true;
                Some(x)
            }),
        )
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** claimed (wrongly): every member sees the same first value */
        ));
    let _first = firsts.sim_cluster_output();
    let mut check = EcCheck::new().max_crashes(0);
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .compiled();
    let sites = check.fragile_sites().expect("instrumented");
    assert!(sites.iter().any(|s| s.op == "scan"), "{sites:?}");
    let cross = check.cross_check(
        &sim,
        async || {
            send_a.send(1);
            send_b.send(2);
        },
        MAX_FAULTS,
    );
    agree("top-level scan over a chosen order", &cross);
    let label = cross
        .results
        .label("a_top_level_scan_over_a_chosen_order_is_fragile");
    let cex = label
        .verdict
        .counterexample()
        .unwrap_or_else(|| panic!("the members disagree with no fault: {cross}"));
    assert!(cex.faults.is_empty(), "{cross}");
}

/// A record that reaches the output only through `defer_tick` is emitted only if a later tick
/// runs at that member, and the simulator runs a tick only when a hook releases something. Two
/// values to two members, each batched, deferred a tick and emitted: a member that batched them
/// apart emits the first (the second tick runs), one that batched them together emits nothing.
/// Before `defer_tick` was fragile, this program counted as fragile-free and was certified;
/// brute force broke it with the empty set.
#[cfg(stageleft_runtime)]
#[test]
fn deferred_records_are_fragile() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let tick = members.tick();
    let delayed = input
        .broadcast_closed(&members, TCP.fail_stop().name("initial").bincode())
        .batch(&tick, nondet!(/** which tick a delivery lands in */))
        .defer_tick()
        .all_ticks()
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** claimed (wrongly): every member emits what it received, a tick later */
        ));
    let _out = delayed.sim_cluster_output();
    let mut check = EcCheck::new().max_crashes(0);
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .compiled();
    let sites = check.fragile_sites().expect("instrumented");
    assert!(sites.iter().any(|s| s.op == "defer_tick"), "{sites:?}");
    let cross = check.cross_check(
        &sim,
        async || {
            send.send(1);
            send.send(2);
        },
        MAX_FAULTS,
    );
    agree("deferred records", &cross);
    let broken = cross
        .results
        .results()
        .iter()
        .filter_map(|l| l.verdict.counterexample())
        .any(|c| c.faults.is_empty());
    assert!(broken, "the members disagree with no fault: {cross}");
}

/// Whether some label of `cross` breaks under a fault set satisfying `faults`.
#[cfg(stageleft_runtime)]
fn breaks_under(cross: &CrossCheck, faults: impl Fn(&FaultSet<crate::fault::MessageId>) -> bool) -> bool {
    cross
        .results
        .results()
        .iter()
        .filter_map(|l| l.verdict.counterexample())
        .any(|c| faults(&c.faults))
}

/// A constant inside a tick (`tick.singleton`) is emitted in every tick that runs, and only if
/// one runs: here the tick runs only on a lossy delivery, so omitting it leaves the member
/// without the constant. The label also merges a `fail_stop` stream at top level, so it is fed
/// by the network and checked. Before the fix the constant had no lineage (no fault could kill
/// it) and the search certified the label.
#[cfg(stageleft_runtime)]
#[test]
fn tick_constants_are_fragile() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send_x, in_x) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let (send_y, in_y) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let tick = members.tick();
    in_x.broadcast_closed(&members, TCP.lossy_delayed_forever().name("x").bincode())
        .batch(&tick, nondet!(/** the tick runs only when `x` arrives */))
        .all_ticks()
        .for_each(q!(|_| {}, commutative = manual_proof!(/** a no-op */)));
    let y = in_y.broadcast_closed(&members, TCP.fail_stop().name("y").bincode());
    let nine = tick
        .singleton(q!(9u32))
        .into_stream()
        .all_ticks()
        .weaken_ordering::<NoOrder>()
        .merge_unordered(y.weaken_consistency().weaken_ordering::<NoOrder>())
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** claimed (wrongly): every member emits 9 */
        ));
    let _out = nine.sim_cluster_output();
    let mut check = EcCheck::new().max_crashes(0);
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .compiled();
    let sites = check.fragile_sites().expect("instrumented");
    assert!(sites.iter().any(|s| s.op == "tick_constant"), "{sites:?}");
    let cross = check.cross_check(
        &sim,
        async || {
            send_x.send(1);
            send_y.send(5);
        },
        MAX_FAULTS,
    );
    agree("tick constants", &cross);
    assert!(
        breaks_under(&cross, |f| !f.is_empty()),
        "omitting `x` leaves a member without 9: {cross}"
    );
}

/// The dedup idiom's state edge is robust only if nothing but the negation and the cycle read
/// it. Here `seen` also leaves the tick (`all_ticks`): a record carried to the next tick is
/// emitted only if a next tick runs, so the members disagree with no fault.
#[cfg(stageleft_runtime)]
#[test]
fn a_dedup_state_read_elsewhere_is_fragile() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let tick = members.tick();
    let arrived = input
        .broadcast_closed(&members, TCP.fail_stop().name("initial").bincode())
        .batch(&tick, nondet!(/** which tick a delivery lands in */))
        .weaken_ordering::<NoOrder>();
    let (handle, prev) = tick.cycle::<Stream<u32, _, Bounded, NoOrder>, _>();
    let seen = prev.chain(tick.singleton(q!(0u32)).into_stream().weaken_ordering::<NoOrder>());
    let fresh = arrived.filter_not_in(seen.clone());
    handle.complete_next_tick(seen.clone().chain(fresh.clone()));
    fresh
        .all_ticks()
        .for_each(q!(|_| {}, commutative = manual_proof!(/** a no-op */)));
    let leaked = seen
        .all_ticks()
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** claimed (wrongly): every member's history is the same */
        ));
    let _out = leaked.sim_cluster_output();
    let mut check = EcCheck::new().max_crashes(0);
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .compiled();
    let sites = check.fragile_sites().expect("instrumented");
    assert!(sites.iter().any(|s| s.op == "defer_tick"), "{sites:?}");
    let cross = check.cross_check(
        &sim,
        async || {
            send.send(1);
            send.send(2);
        },
        MAX_FAULTS,
    );
    agree("dedup state read elsewhere", &cross);
    assert!(
        breaks_under(&cross, |f| f.is_empty()),
        "the members disagree with no fault: {cross}"
    );
}

/// Reviewer #1's program: a batch in a tick that also snapshots a singleton (`s`) that has a
/// value only once its producer tick ran. Omitting `x` to a member leaves its snapshot without
/// a value, so the simulator never runs its consumer tick, and that member never emits 7. Before
/// the fix the batch was robust, the search certified `facts`, and the simulator aborted the
/// process on the stuck tick.
#[cfg(stageleft_runtime)]
#[test]
fn review_batch_beside_an_unready_snapshot() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send_x, in_x) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let (send_y, in_y) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let y = in_y
        .broadcast_closed(&members, TCP.fail_stop().name("y").bincode())
        .weaken_consistency();
    let consumer = members.tick();
    let facts = y
        .batch(&consumer, nondet!(/** */))
        .all_ticks()
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(/** */));
    let _out = facts.sim_cluster_output();
    let x = in_x
        .broadcast_closed(&members, TCP.lossy_delayed_forever().name("x").bincode())
        .weaken_consistency();
    let producer = members.tick();
    let s = x.batch(&producer, nondet!(/** */)).count().latest();
    s.snapshot(&consumer, nondet!(/** */))
        .into_stream()
        .all_ticks()
        .for_each(q!(|_| {}));
    let mut check = EcCheck::new().max_crashes(0);
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .compiled();
    let sites = check.fragile_sites().expect("instrumented");
    assert!(sites.iter().any(|s| s.op == "batch_beside_snapshot"), "{sites:?}");
    let cross = check.cross_check(
        &sim,
        async || {
            send_x.send(1);
            send_y.send(7);
        },
        MAX_FAULTS,
    );
    cross.assert_agrees();
    let facts = cross
        .results
        .results()
        .iter()
        .find(|l| l.plan.collection.display_name() == "facts")
        .expect("the `facts` label");
    assert!(
        facts.verdict.counterexample().is_some(),
        "omitting `x` leaves a member without 7: {cross}"
    );
}

/// Two collections labelled by one line (a helper called twice) are two labels. Merged, the
/// `fail_stop` collection's 1 masked the lossy one's missing 1, and both engines certified the
/// union.
#[cfg(stageleft_runtime)]
#[test]
fn one_labelling_line_twice_is_two_labels() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send_a, in_a) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let (send_b, in_b) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let a = in_a.broadcast_closed(&members, TCP.fail_stop().name("a").bincode());
    let b = in_b.broadcast_closed(&members, TCP.lossy_delayed_forever().name("b").bincode());
    fn claim<'a>(
        s: Stream<u32, Cluster<'a, ()>, Unbounded, NoOrder>,
    ) -> Stream<u32, Cluster<'a, (), EventualConsistency>, Unbounded, NoOrder> {
        s.assert_has_consistency_of::<Cluster<'a, (), EventualConsistency>>(manual_proof!(/** */))
    }
    let _a = claim(a.weaken_consistency().weaken_ordering()).sim_cluster_output();
    let _b = claim(b.weaken_consistency().weaken_ordering()).sim_cluster_output();
    let mut check = EcCheck::new().max_crashes(0);
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .compiled();
    let cross = check.cross_check(
        &sim,
        async || {
            send_a.send(1);
            send_b.send(1);
        },
        MAX_FAULTS,
    );
    agree("one labelling line twice", &cross);
    // The two broadcasts, and the two collections `claim` labels.
    let searched: Vec<_> = cross.results.results().iter().filter(|l| l.plan.is_searched()).collect();
    assert_eq!(searched.len(), 4, "{cross}");
    let claimed: Vec<_> = searched
        .iter()
        .filter(|l| l.plan.collection.label == searched[searched.len() - 1].plan.collection.label)
        .collect();
    assert_eq!(claimed.len(), 2, "{cross}");
    // `b`'s broadcast and `claim(b)` break under omissions; `a`'s and `claim(a)` hold.
    let breaking = searched.iter().filter(|l| l.verdict.counterexample().is_some()).count();
    assert_eq!(breaking, 2, "{cross}");
    assert!(
        claimed.iter().any(|l| l.verdict.counterexample().is_some()),
        "omitting `b` leaves a member without claim(b)'s 1: {cross}"
    );
}

/// A closure that names outside state is fragile, and a choice (its one schedule would not
/// stand for the others).
#[cfg(stageleft_runtime)]
#[test]
fn impure_closures_are_fragile_choices() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (_send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let out = input
        .broadcast_closed(&members, TCP.fail_stop().name("initial").bincode())
        .map(q!(|x: u32| x + std::process::id() * 0))
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(/** */));
    let _out = out.sim_cluster_output();
    let mut check = EcCheck::new().max_crashes(0);
    let _sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .compiled();
    let sites = check.fragile_sites().expect("instrumented");
    assert!(sites.iter().any(|s| s.op == "impure_closure"), "{sites:?}");
    let impure = |text: &str| crate::provenance::fragile::names_impure(text.parse().unwrap());
    assert!(impure("|x| { static N: u32 = 0; x + N }"));
    assert!(impure("|x| x + COUNT.fetch_add(1, Ordering::Relaxed) as AtomicU32"));
    assert!(impure("|x| (x, std::time::SystemTime::now())"));
    assert!(impure("|x| x.borrow_mut().replace(RefCell::new(0))"));
    assert!(!impure("|x: &'static str| x.len()"));
    assert!(!impure("|(k, v)| (k, v + 1)"));
}

/// The simulator's own crash injection and dynamic membership are refused: hydrofrac would
/// count the members the simulator crashed as correct.
#[cfg(stageleft_runtime)]
#[test]
#[should_panic(expected = "does not support the simulator's own crash injection")]
fn the_simulators_own_crashes_are_refused() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (_send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let out = input
        .broadcast_closed(&members, TCP.fail_stop().name("initial").bincode())
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(/** */));
    let _out = out.sim_cluster_output();
    let mut check = EcCheck::new().max_crashes(0);
    let _sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .with_crashable_cluster(&members, 1)
        .compiled();
}

/// With `BOLERO_RANDOM_TEST_TIME_MS` set, bolero ends an exhaustive exploration at the time limit
/// without saying so: the check refuses to run (in a child process, so the variable does not
/// leak into other tests).
#[cfg(stageleft_runtime)]
#[test]
fn a_time_bounded_exhaustive_run_is_refused() {
    const CHILD: &str = "HYDROFRAC_TIME_BOUND_CHILD";
    if std::env::var(CHILD).is_ok() {
        let cross = plain_broadcast(true);
        eprintln!("{cross}");
        return;
    }
    let out = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "check::brute_tests::a_time_bounded_exhaustive_run_is_refused",
            "--nocapture",
            "--test-threads=1",
        ])
        .env(CHILD, "1")
        .env("BOLERO_RANDOM_TEST_TIME_MS", "1")
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(!out.status.success(), "the child ran: {stdout}\n{stderr}");
    assert!(
        stderr.contains("would silently cut the exhaustive exploration short")
            || stdout.contains("would silently cut the exhaustive exploration short"),
        "{stdout}\n{stderr}"
    );
}

/// A member reports `x` as missing (label `missing`) when a probe for `x` reaches it in the
/// recovery phase and it never heard `x`. Without faults every member hears `x` before the heal,
/// so the label is empty in every failure-free schedule. The program batches (a choice), so no
/// proof says faults create no fact.
#[cfg(stageleft_runtime)]
pub(super) fn missing_alarm(mut check: EcCheck) -> CrossCheck {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let prober = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let (send_probe, probe) = prober.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let tick = members.tick();
    let arrived = input
        .broadcast_closed(&members, TCP.fail_stop().name("data").bincode())
        .batch(&tick, nondet!(/** which tick a delivery lands in */))
        .weaken_ordering::<NoOrder>();
    let (handle, prev) = tick.cycle::<Stream<u32, _, Bounded, NoOrder>, _>();
    let heard = prev.chain(arrived);
    handle.complete_next_tick(heard.clone());
    let probes = probe.broadcast_closed(&members, TCP.fail_stop().name("probe").bincode());
    let missing = probes
        .batch(&tick, nondet!(/** which tick a probe lands in */))
        .weaken_ordering::<NoOrder>()
        .filter_not_in(heard)
        .all_ticks()
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** claimed (wrongly): every member misses the same inputs */
        ));
    let _out = missing.sim_cluster_output();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 3))
        .compiled();
    check.cross_check_then_recover(
        &sim,
        async || send.send(1),
        async || send_probe.send(1),
        MAX_FAULTS,
    )
}

/// Review R3-F5: a label with an empty domain was reported `Vacuous` without a search, and the
/// enumeration dropped it too, so both passed it. In [`missing_alarm`], a single crash of the
/// source, right after it sends `x` to one member, makes the other two report `x` missing and
/// the first not: a fact only faults create, held by some correct members and not others. Such a
/// label is now searched over every set within the bounds (Claim B). With no crash allowed, no
/// set breaks it, and that search certifies Claim B.
#[cfg(stageleft_runtime)]
#[test]
fn a_fact_only_faults_create_is_checked() {
    let cross = missing_alarm(EcCheck::new());
    agree("a fact only faults create", &cross);
    let cex = cross.results.label("missing").verdict.counterexample();
    let cex = cex.unwrap_or_else(|| panic!("a single crash of the source breaks it: {cross}"));
    assert_eq!(cex.crashes(), 1, "{cross}");
    assert!(cex.missing.is_empty() && !cex.divergent.is_empty(), "{cross}");

    let cross = missing_alarm(EcCheck::new().max_crashes(0));
    agree("a fact only faults create, no crash", &cross);
    let label = cross.results.label("missing");
    assert!(
        matches!(label.verdict, Verdict::Certified { tolerates: Tolerance::UpTo(0) }),
        "{cross}"
    );
    assert!(label.stats.races.is_some_and(|r| r.claim_b), "{cross}");
}

/// A choice-free program that is not fragile-free: two sources each broadcast the same input
/// over `fail_stop`, and each member passes each source's stream through a `scan` (a fragile
/// step) and merges the two (label `either`). Either source alone delivers every fact.
#[cfg(stageleft_runtime)]
fn redundant_scans() -> CrossCheck {
    let mut flow = FlowBuilder::new();
    let first = flow.process::<()>();
    let second = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send_first, input_first) = first.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let (send_second, input_second) = second.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let from_first = input_first
        .broadcast_closed(&members, TCP.fail_stop().name("first").bincode())
        .scan(q!(|| 0u32), q!(|count, x| {
            *count += 1;
            Some(x)
        }))
        .weaken_consistency()
        .weaken_ordering::<NoOrder>();
    let from_second = input_second
        .broadcast_closed(&members, TCP.fail_stop().name("second").bincode())
        .scan(q!(|| 0u32), q!(|count, x| {
            *count += 1;
            Some(x)
        }))
        .weaken_consistency()
        .weaken_ordering::<NoOrder>();
    let either = from_first
        .merge_unordered(from_second)
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** claimed: either source delivers every fact */
        ));
    let _out = either.sim_cluster_output();
    let mut check = EcCheck::new().max_crashes(1);
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 3))
        .compiled();
    assert!(check.fragile_sites().is_some_and(|s| !s.is_empty()), "the scans are fragile");
    check.cross_check(
        &sim,
        async || {
            send_first.send(1);
            send_second.send(1);
        },
        MAX_FAULTS,
    )
}

/// The choice-free shortcut (`provenance/choices.rs`; soundness §3.1, §14). [`redundant_scans`]
/// has no schedule choice, so each set runs one schedule, and every derivation counts as robust
/// although `scan` is fragile. One source's crash then leaves every pair a derivation through
/// the other source, which protects it, so few sets are run. Without the shortcut every
/// derivation is fragile, no pair is protected, and the search runs every one-crash set.
#[cfg(stageleft_runtime)]
#[test]
fn the_choice_free_shortcut_runs_one_schedule_and_aims_the_search() {
    let cross = redundant_scans();
    agree("redundant scans", &cross);
    let label = cross.results.label("either");
    assert!(
        matches!(label.verdict, Verdict::Certified { tolerates: Tolerance::UpTo(1) }),
        "{cross}"
    );
    // Only the probe runs: its robust clauses leave no admissible one-crash set. With the
    // shortcut forced off, this search ran 12 sets, none on one schedule (measured).
    let notes = label.stats.races.expect("a races search");
    assert_eq!(
        (label.stats.hypotheses, notes.single_schedule_sets, notes.claim_b),
        (1, 1, true),
        "{cross}"
    );
}

/// A process broadcasts two harness inputs to two members, which batch what they got (a schedule
/// choice) and echo it to every member; the label `delivered` is the union. The first broadcast
/// is sequenced (docs/design/hydrofrac-soundness.md §6.4), so each crash point of the process is
/// exact: every literal `after(·, m)` of its sends is proved, by fan-out within a payload and by
/// the harness order between payloads. Its crash sets then run one schedule each, where without
/// the sequence proof (`after(2 → m0, 1 → m1)` free) they ran every schedule. Brute force agrees.
#[cfg(stageleft_runtime)]
#[test]
fn a_sequenced_broadcast_runs_each_crash_point_of_its_sender_once() {
    let mut check = EcCheck::new();
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let initial = input
        .broadcast_closed(&members, TCP.fail_stop().name("initial").bincode())
        .weaken_consistency();
    let tick = members.tick();
    let echoed = initial
        .clone()
        .batch(&tick, nondet!(/** explored */))
        .all_ticks()
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
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .compiled();
    let cross = check.cross_check(
        &sim,
        async || {
            send.send(1);
            send.send(2);
        },
        1,
    );
    agree("sequenced echo", &cross);
    let label = cross.results.label("delivered");
    let notes = label.stats.races.expect("a races search");
    eprintln!(
        "sets {} schedules {} single-schedule sets {}",
        label.stats.hypotheses, label.stats.schedules, notes.single_schedule_sets
    );
    assert!(
        matches!(label.verdict, Verdict::Certified { .. }),
        "{cross}"
    );
    // Without the sequence proof: 6 sets, 24 schedules, 2 of the sets on one schedule. With it:
    // 4 sets, every one on one schedule.
    assert_eq!(label.stats.schedules, label.stats.hypotheses, "{cross}");
    assert_eq!(notes.single_schedule_sets, label.stats.hypotheses, "{cross}");
}
