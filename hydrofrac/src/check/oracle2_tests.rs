//! Oracle 2 (`oracle2.rs`) catches wrong verdicts on its own: with the brute-force oracle's
//! findings erased, the verdicts planted in the search's results still disagree with it.
//!
//! Every item is `cfg(stageleft_runtime)`, as in `brute_tests.rs`.

#[cfg(stageleft_runtime)]
use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
#[cfg(stageleft_runtime)]
use hydro_lang::prelude::*;

#[cfg(stageleft_runtime)]
use super::{Counterexample, CrossCheck, EcCheck, VacuousReason, Verdict};
#[cfg(stageleft_runtime)]
use crate::search::{FaultSet, Tolerance};

/// A process broadcasts one input to three members over `fail_stop` (label `delivered`): a
/// crash of the source mid fan-out breaks it.
#[cfg(stageleft_runtime)]
fn broadcast() -> CrossCheck {
    let mut check = EcCheck::new();
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let delivered = input.broadcast_closed(&members, TCP.fail_stop().bincode());
    let _out = delivered.sim_cluster_output();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 3))
        .compiled();
    check.cross_check(
        &sim,
        async || {
            send.send(1);
        },
        2,
    )
}

/// Sets the verdict of the label at `index`, and recomputes the disagreements.
#[cfg(stageleft_runtime)]
fn plant(cross: &mut CrossCheck, index: usize, verdict: Verdict) {
    let label = cross
        .results
        .labels
        .iter_mut()
        .find(|l| l.index == index)
        .unwrap();
    label.verdict = verdict;
    cross.recheck();
}

/// Only oracle 2 is left to disagree, and it does.
#[cfg(stageleft_runtime)]
fn caught_by_oracle2(cross: &CrossCheck, what: &str) {
    assert!(
        !cross.disagreements.is_empty(),
        "oracle 2 missed {what}:\n{cross}"
    );
    assert!(
        cross
            .disagreements
            .iter()
            .all(|d| d.starts_with("oracle 2: ")),
        "only oracle 2 should disagree ({what}):\n{cross}"
    );
}

#[cfg(stageleft_runtime)]
#[test]
fn oracle2_catches_wrong_verdicts_on_its_own() {
    let mut cross = broadcast();
    eprintln!("{cross}");
    cross.assert_agrees();
    // Oracle 2 found the mid fan-out crash with its own program, taps and faults.
    let index = cross.results.label("delivered").index;
    let collection = cross
        .oracle2
        .collections
        .iter()
        .find(|c| c.labels == [index])
        .expect("oracle 2 taps the label");
    assert_eq!(
        collection.breaking.first().map(|b| b.crashes),
        Some(1),
        "{cross}"
    );

    // Erase the brute force's findings: whatever disagrees now is oracle 2's.
    cross.brute.labels.clear();
    cross.recheck();
    cross.assert_agrees();
    let Verdict::Counterexample(original) = cross.results.label("delivered").verdict.clone() else {
        panic!("expected a counterexample:\n{cross}");
    };

    plant(
        &mut cross,
        index,
        Verdict::Certified {
            tolerates: Tolerance::All,
        },
    );
    caught_by_oracle2(&cross, "a wrong certificate");

    plant(
        &mut cross,
        index,
        Verdict::Certified {
            tolerates: Tolerance::UpTo(1),
        },
    );
    caught_by_oracle2(&cross, "a wrong bounded certificate");

    plant(
        &mut cross,
        index,
        Verdict::Counterexample(Box::new(Counterexample {
            tolerates: Some(1),
            ..(*original).clone()
        })),
    );
    caught_by_oracle2(&cross, "an overstated tolerance");

    // The failure-free run does not break the label.
    plant(
        &mut cross,
        index,
        Verdict::Counterexample(Box::new(Counterexample {
            faults: FaultSet::new(),
            tolerates: None,
            ..(*original).clone()
        })),
    );
    caught_by_oracle2(&cross, "a bogus counterexample");

    // An empty domain left unsearched passes `assert_holds`: oracle 2 refutes it too.
    plant(
        &mut cross,
        index,
        Verdict::Vacuous(VacuousReason::EmptyDomain),
    );
    caught_by_oracle2(&cross, "a wrong vacuity");

    plant(&mut cross, index, Verdict::Counterexample(original));
    cross.assert_agrees();
}

/// Two collections labelled by one line (a helper called twice, as in
/// `brute_tests::one_labelling_line_twice_is_two_labels`) share a key, so oracle 2 matches them
/// to the search's two labels as a group. A certificate planted on the lossy one is still
/// caught: whichever way the group is matched, a certified label gets the breaking collection.
#[cfg(stageleft_runtime)]
#[test]
fn oracle2_matches_same_key_collections_as_a_group() {
    use hydro_lang::live_collections::stream::NoOrder;
    use hydro_lang::location::cluster::EventualConsistency;

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
    let mut cross = check.cross_check(
        &sim,
        async || {
            send_a.send(1);
            send_b.send(1);
        },
        2,
    );
    eprintln!("{cross}");
    cross.assert_agrees();
    // The two `claim` collections: one group of two on oracle 2's side.
    let claim_label = |cross: &CrossCheck, breaking: bool| {
        cross
            .results
            .results()
            .iter()
            .filter(|l| l.plan.is_searched())
            .filter(|l| {
                cross
                    .oracle2
                    .collections
                    .iter()
                    .any(|c| c.labels.len() == 2 && c.labels.contains(&l.index))
            })
            .find(|l| l.verdict.counterexample().is_some() == breaking)
            .map(|l| l.index)
            .unwrap_or_else(|| panic!("a `claim` label (breaking: {breaking}):\n{cross}"))
    };
    let (holds, breaks) = (claim_label(&cross, false), claim_label(&cross, true));
    assert_ne!(holds, breaks);

    cross.brute.labels.clear();
    cross.recheck();
    cross.assert_agrees();
    plant(
        &mut cross,
        breaks,
        Verdict::Certified {
            tolerates: Tolerance::All,
        },
    );
    caught_by_oracle2(&cross, "a wrong certificate within a same-key group");
}

/// R3-F5 on oracle 2's side: a label whose failure-free runs hold no fact, reported `Vacuous`
/// without a search (the search's behaviour before the fix), is refuted by oracle 2 alone. In
/// `brute_tests::missing_alarm`, a single crash of the source right after it sends `x` to one
/// member makes the other two members report `x` missing and the first not.
#[cfg(stageleft_runtime)]
#[test]
fn oracle2_refutes_a_vacuous_label_that_faults_break() {
    let mut cross = super::brute_tests::missing_alarm(EcCheck::new());
    cross.assert_agrees();
    let index = cross.results.label("missing").index;
    let collection = cross
        .oracle2
        .collections
        .iter()
        .find(|c| c.labels == [index])
        .expect("oracle 2 taps `missing`");
    assert!(!collection.failure_free_facts, "{cross}");
    assert_eq!(
        collection.breaking.first().map(|b| b.crashes),
        Some(1),
        "{cross}"
    );

    cross.brute.labels.clear();
    cross.recheck();
    cross.assert_agrees();
    plant(
        &mut cross,
        index,
        Verdict::Vacuous(VacuousReason::EmptyDomain),
    );
    caught_by_oracle2(&cross, "an unsearched empty domain that a crash breaks");
}
