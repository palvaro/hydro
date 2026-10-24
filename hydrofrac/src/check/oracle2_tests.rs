//! Oracle 2 (`oracle2.rs`) catches wrong verdicts on its own: with the brute-force oracle's
//! findings erased, the verdicts planted in the search's results still disagree with it.
//!
//! Every item is `cfg(stageleft_runtime)`, as in `brute_tests.rs`.

#[cfg(stageleft_runtime)]
use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
#[cfg(stageleft_runtime)]
use hydro_lang::prelude::*;

#[cfg(stageleft_runtime)]
use super::{Counterexample, CrossCheck, EcCheck, Verdict};
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
    assert!(!cross.disagreements.is_empty(), "oracle 2 missed {what}:\n{cross}");
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
        .find(|c| c.label == Some(index))
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
    let Verdict::Counterexample(original) = cross.results.label("delivered").verdict.clone()
    else {
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

    plant(&mut cross, index, Verdict::Counterexample(original));
    cross.assert_agrees();
}
