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
    plain_broadcast_in(EcCheck::new(), lossy)
}

/// [`plain_broadcast`], checked by `check`.
#[cfg(stageleft_runtime)]
fn plain_broadcast_in(mut check: EcCheck, lossy: bool) -> CrossCheck {
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
    echo_broadcast_in(EcCheck::new(), n, initial_lossy)
}

/// [`echo_broadcast`], checked by `check`.
#[cfg(stageleft_runtime)]
fn echo_broadcast_in(mut check: EcCheck, n: usize, initial_lossy: bool) -> CrossCheck {
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

/// A relay chain with no retries: a source sends two inputs over a lossy channel to a relay
/// process, which broadcasts them to three members over `fail_stop` (label `relayed`).
#[cfg(stageleft_runtime)]
#[test]
fn brute_force_agrees_on_a_lossy_relay_chain() {
    agree("lossy relay chain", &lossy_relay_chain(EcCheck::new()));
}

/// The relay chain of [`brute_force_agrees_on_a_lossy_relay_chain`], checked by `check`.
#[cfg(stageleft_runtime)]
fn lossy_relay_chain(mut check: EcCheck) -> CrossCheck {
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
    total_loss_masks_a_partial_loss_in(EcCheck::new(), max_crashes)
}

/// [`total_loss_masks_a_partial_loss`], checked by `check`.
#[cfg(stageleft_runtime)]
fn total_loss_masks_a_partial_loss_in(check: EcCheck, max_crashes: Option<usize>) -> CrossCheck {
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
        Some(n) => check.max_crashes(n),
        None => check,
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

/// S1 with the search capped at zero crashes: a false certificate.
#[cfg(stageleft_runtime)]
#[test]
fn suspected_total_loss_masks_a_partial_loss_certificate() {
    let start = std::time::Instant::now();
    let cross = total_loss_masks_a_partial_loss(Some(0));
    eprintln!(
        "=== S1 (max_crashes 0), {:?}\n{cross}\n{}",
        start.elapsed(),
        cross.results
    );
    assert!(
        cross.results.choices.is_empty(),
        "not choice-free: {:?}",
        cross.results.choices
    );
    // The enumeration finds a zero-crash breaking set; the search certifies zero crashes.
    assert!(
        matches!(
            cross.results.label("known").verdict,
            Verdict::Certified {
                tolerates: Tolerance::UpTo(0)
            }
        ),
        "{cross}"
    );
    let label = cross.results.label("known");
    let b = cross.brute.label(label.index).expect("searched");
    assert_eq!(b.min_crashes(), Some(0), "{cross}");
    assert!(
        cross
            .disagreements
            .iter()
            .any(|d| d.starts_with("known") && d.contains("omit(gossip[cluster@0→cluster@1]")),
        "expected the search to disagree with the enumeration on `known`:\n{cross}"
    );
}

/// S1 uncapped: the search reports a counterexample, but claims the label tolerates every
/// zero-crash fault set, while `omit(gossip m0→m1)` alone breaks it.
#[cfg(stageleft_runtime)]
#[test]
fn suspected_total_loss_masks_a_partial_loss_tolerance() {
    let start = std::time::Instant::now();
    let cross = total_loss_masks_a_partial_loss(None);
    eprintln!(
        "=== S1 (uncapped), {:?}\n{cross}\n{}",
        start.elapsed(),
        cross.results
    );
    let label = cross.results.label("known");
    let b = cross.brute.label(label.index).expect("searched");
    assert_eq!(b.min_crashes(), Some(0), "{cross}");
    assert!(
        cross
            .disagreements
            .iter()
            .any(|d| d.starts_with("known") && d.contains("omit(gossip[cluster@0→cluster@1]")),
        "expected the search to disagree with the enumeration on `known`:\n{cross}"
    );
}

// The races mode (docs/design/hydrofrac-races.md §4.4), cross-checked on the same programs.

/// Prints the races search's report and the cross-check, and asserts agreement.
#[cfg(stageleft_runtime)]
fn races_agree(name: &str, cross: &CrossCheck) {
    eprintln!("=== races: {name}\nsearch:\n{}\n{cross}", cross.results);
    cross.assert_agrees();
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

#[cfg(stageleft_runtime)]
#[test]
fn races_agree_on_a_plain_broadcast() {
    let cross = plain_broadcast_in(EcCheck::races(), false);
    races_agree("plain broadcast (fail_stop)", &cross);
    let (verdict, _) = races_label(&cross, "delivered");
    let cex = verdict.counterexample().expect("a mid fan-out crash");
    assert_eq!(cex.crashes(), 1, "{cross}");
    assert!(cex.tolerance_certified, "{cross}");
}

#[cfg(stageleft_runtime)]
#[test]
fn races_agree_on_a_lossy_broadcast() {
    let cross = plain_broadcast_in(EcCheck::races(), true);
    races_agree("plain broadcast (lossy)", &cross);
    let (verdict, _) = races_label(&cross, "delivered");
    let cex = verdict.counterexample().expect("an omission");
    assert_eq!(cex.crashes(), 0, "{cross}");
}

#[cfg(stageleft_runtime)]
#[test]
fn races_agree_on_an_echo_broadcast() {
    let cross = echo_broadcast_in(EcCheck::races(), 3, false);
    races_agree("echo broadcast (3 members)", &cross);
    let (verdict, _) = races_label(&cross, "delivered");
    let cex = verdict.counterexample().expect("two crashes break it");
    assert_eq!(cex.crashes(), 2, "{cross}");
}

#[cfg(stageleft_runtime)]
#[test]
fn races_agree_on_an_echo_over_a_lossy_first_hop() {
    let cross = echo_broadcast_in(EcCheck::races(), 3, true);
    races_agree("echo broadcast (lossy first hop)", &cross);
}

#[cfg(stageleft_runtime)]
#[test]
fn races_agree_on_a_lossy_relay_chain() {
    races_agree("lossy relay chain", &lossy_relay_chain(EcCheck::races()));
}

/// S1: where lite certifies zero crashes (or claims to tolerate them) and `omit(gossip m0→m1)`
/// alone breaks the label, the races mode keeps (m1, f)'s clause from the probe and finds it.
#[cfg(stageleft_runtime)]
#[test]
fn races_agree_where_a_total_loss_masks_a_partial_loss() {
    for cap in [Some(0), None] {
        let cross = total_loss_masks_a_partial_loss_in(EcCheck::races(), cap);
        races_agree(&format!("S1 (max_crashes {cap:?})"), &cross);
        let (verdict, _) = races_label(&cross, "known");
        let cex = verdict
            .counterexample()
            .unwrap_or_else(|| panic!("omissions alone break it: {cross}"));
        assert_eq!(cex.crashes(), 0, "{cross}");
    }
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

    let mut check = EcCheck::races().max_crashes(1);
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
    races_agree("batched echo", &cross);
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

    let mut check = EcCheck::races();
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
    races_agree("broadcast without re-forwarding", &cross);
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
/// `m0`. Returns the cross-check, enumerating sets of at most `max_faults` faults.
#[cfg(stageleft_runtime)]
fn positional_crash_order_relay(check: EcCheck, max_faults: usize) -> CrossCheck {
    positional_crash_order_relay_in(check, max_faults, None)
}

/// [`positional_crash_order_relay`], with the search capped at `max_crashes` crashes if given.
#[cfg(stageleft_runtime)]
fn positional_crash_order_relay_in(
    mut check: EcCheck,
    max_faults: usize,
    max_crashes: Option<usize>,
) -> CrossCheck {
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

/// Suspected soundness bug S2, uncapped: the search reports a counterexample with two crashes and
/// claims (as certified) that the label tolerates one, while one crash breaks it.
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
///   (`Dependency::killed_by`, read in the clause's own run) says `H` kills nothing there: `m1`'s
///   old derivation "survives" `H`, and `H` is never proposed.
/// - Every other one-crash set either holds (the echo covers `{omit(direct), crash(n after
///   →m1)}`; the redundant paths cover `r1`, `r2` and the sources) or hits no clause. So bound 1
///   is declared exhausted (the completion adds only `crash(n)`, which is vacuous), and the search
///   breaks at 2 with `{crash(s1), crash(s2)}`.
///
/// The breaking pair `(m1, 7)` is held in the run whose clause excludes `H`, with a derivation
/// through `n`'s `→m1`: this is the send-order flip (S2), not a fact lost everywhere (S1).
#[cfg(stageleft_runtime)]
#[test]
fn suspected_positional_crash_order_tolerance() {
    for max_faults in [2, 3, 4] {
        let start = std::time::Instant::now();
        let cross = positional_crash_order_relay(EcCheck::new(), max_faults);
        eprintln!(
            "=== S2 relay (uncapped, {max_faults} faults), {:?}\n{cross}\nsearch:\n{}",
            start.elapsed(),
            cross.results
        );
        assert!(
            cross.results.choices.is_empty(),
            "not choice-free: {:?}",
            cross.results.choices
        );
        let label = cross.results.label("known");
        let cex = label
            .verdict
            .counterexample()
            .unwrap_or_else(|| panic!("expected a counterexample: {cross}"));
        assert_eq!(cex.tolerates, Some(1), "{cross}");
        assert!(cex.tolerance_certified, "{cross}");
        assert_eq!(cex.crashes(), 2, "{cross}");
        let b = cross.brute.label(label.index).expect("searched");
        assert_eq!(b.min_crashes(), Some(1), "{cross}");
        assert_eq!(b.breaking[0].len(), 1, "{cross}");
        assert!(
            cross
                .results
                .report_names()
                .fault_set(&b.breaking[0])
                .starts_with(&format!("{{{RELAY_BREAK}")),
            "{cross}"
        );
        assert!(
            cross
                .disagreements
                .iter()
                .any(|d| d.starts_with("known") && d.contains("tolerates 1 crash")),
            "expected the search to disagree with the enumeration on `known`:\n{cross}"
        );
    }
}

/// S2 with the search capped at one crash: a false certificate ("holds with up to 1 crash")
/// where `{crash(n after →m0)}` breaks the label. See
/// [`suspected_positional_crash_order_tolerance`].
#[cfg(stageleft_runtime)]
#[test]
fn suspected_positional_crash_order_certificate() {
    let start = std::time::Instant::now();
    let cross = positional_crash_order_relay_in(EcCheck::new(), 3, Some(1));
    eprintln!(
        "=== S2 relay (max_crashes 1, 3 faults), {:?}\n{cross}\nsearch:\n{}",
        start.elapsed(),
        cross.results
    );
    let label = cross.results.label("known");
    assert!(
        matches!(
            label.verdict,
            Verdict::Certified {
                tolerates: Tolerance::UpTo(1)
            }
        ),
        "{cross}"
    );
    assert!(
        cross
            .disagreements
            .iter()
            .any(|d| d.starts_with("known") && d.contains(RELAY_BREAK)),
        "expected the search to disagree with the enumeration on `known`:\n{cross}"
    );
}

/// The races mode on the S2 relay agrees with the enumeration (up to 4 faults): it finds
/// `{crash(n after →m0)}` at one crash.
#[cfg(stageleft_runtime)]
#[test]
fn races_on_the_positional_crash_order_relay() {
    for (cap, max_faults) in [(None, 3), (Some(1), 3), (None, 4)] {
        let cross = positional_crash_order_relay_in(EcCheck::races(), max_faults, cap);
        eprintln!(
            "=== races: S2 relay (max_crashes {cap:?}, {max_faults} faults)\nsearch:\n{}\n{cross}",
            cross.results
        );
        cross.assert_agrees();
    }
}
