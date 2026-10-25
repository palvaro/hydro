//! REVIEWER-A (review 2): adversarial counterexamples to the soundness argument
//! (docs/design/hydrofrac-soundness.md). Each test prints the cross-check and asserts what was
//! observed, so a test that passes documents the finding as it stands at `664c60e2f1`.
//!
//! Every item is `cfg(stageleft_runtime)`, as in `brute_tests.rs`.

#[cfg(stageleft_runtime)]
use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
#[cfg(stageleft_runtime)]
use hydro_lang::location::cluster::EventualConsistency;
#[cfg(stageleft_runtime)]
use hydro_lang::prelude::*;

#[cfg(stageleft_runtime)]
use super::{CrossCheck, EcCheck, Verdict};

#[cfg(stageleft_runtime)]
const MAX_FAULTS: usize = 2;

/// A source `a` broadcasts one value to three members. Each member numbers what reaches it, its
/// delivery from `a` and a local constant, with a counter that the `map` closure *owns*: a block
/// expression whose closure keeps its count in a `std::sync::mpsc` channel it created. The
/// closure names nothing the textual guard looks for (`static`, `Cell`, `Atomic*`, `std::sync`
/// is not an impure module), so it is robust and the program choice-free. Each member also holds
/// the constant `1` directly.
///
/// Failure-free each member numbers `a`'s value 1 and the local constant 2, so every member
/// holds {1, 2}. A crash of `a` right after it sends to member 0 leaves member 0 with {1, 2}
/// and members 1 and 2 with {1} (the local constant is now numbered 1): fact `2` is missing at
/// two correct members. Lineage gives fact `2` only the local constant (a `map` is per-record),
/// and fact `1` has the direct local derivation, so no fault is admissible for any pair, and
/// the search certifies.
#[cfg(stageleft_runtime)]
fn owned_counter() -> CrossCheck {
    let mut flow = FlowBuilder::new();
    let a = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send_a, in_a) = a.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let from_a = in_a
        .broadcast_closed(&members, TCP.fail_stop().name("a").bincode())
        .weaken_consistency()
        .weaken_ordering::<NoOrder>();
    let local = members
        .source_iter(q!([7u32]))
        .weaken_consistency()
        .weaken_ordering::<NoOrder>();
    let counted = from_a.merge_unordered(local.into()).map(q!({
        let (tx, rx) = std::sync::mpsc::channel::<u32>();
        move |_x: u32| {
            let n = rx.try_recv().unwrap_or(0) + 1;
            tx.send(n).unwrap();
            n
        }
    }));
    let one = members
        .source_iter(q!([1u32]))
        .weaken_consistency()
        .weaken_ordering::<NoOrder>();
    let numbered = counted
        .merge_unordered(one.into())
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** claimed (wrongly): every member numbers the same count of arrivals */
        ));
    let _out = numbered.sim_cluster_output();
    let mut check = EcCheck::new().max_crashes(1);
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 3))
        .compiled();
    eprintln!("fragile sites: {:?}", check.fragile_sites());
    check.cross_check(
        &sim,
        async || {
            send_a.send(10);
        },
        MAX_FAULTS,
    )
}

#[cfg(stageleft_runtime)]
#[test]
fn review2a_a_closure_owning_its_state_escapes_the_impurity_guard() {
    let cross = owned_counter();
    eprintln!("=== owned counter\n{cross}");
    eprintln!("disagreements: {:#?}", cross.disagreements);
    let label = cross.results.label("numbered");
    eprintln!("numbered: certified = {}", matches!(label.verdict, Verdict::Certified { .. }));
    assert!(
        matches!(label.verdict, Verdict::Certified { .. }),
        "the search certifies `numbered`: {cross}"
    );
    assert!(
        cross.disagreements.iter().any(|d| d.contains("numbered")),
        "and the oracles break it: {cross}"
    );
}
