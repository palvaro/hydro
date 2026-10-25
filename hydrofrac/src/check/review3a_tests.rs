//! REVIEWER-3A (review 3): adversarial counterexamples to the soundness argument
//! (docs/design/hydrofrac-soundness.md) at `56effda043`. Each test asserts what was observed:
//! that `assert_holds` passes (a certificate) while the brute force and/or oracle 2 break the
//! label. A test that passes documents an open false certificate.
//!
//! Every item is `cfg(stageleft_runtime)`, as in `brute_tests.rs`.

#[cfg(stageleft_runtime)]
use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
#[cfg(stageleft_runtime)]
use hydro_lang::location::cluster::EventualConsistency;
#[cfg(stageleft_runtime)]
use hydro_lang::prelude::*;

#[cfg(stageleft_runtime)]
use super::CrossCheck;

#[cfg(stageleft_runtime)]
const MAX_FAULTS: usize = 2;

/// Asserts the false certificate: the search says `label` holds (and `assert_holds` passes),
/// while the brute force breaks it with at most `crashes` crashes and oracle 2 breaks it too.
#[cfg(stageleft_runtime)]
fn assert_false_certificate(cross: &CrossCheck, label: &str, crashes: usize) {
    let found = cross.results.label(label);
    assert!(
        !found.verdict.fails(),
        "expected a (false) certificate for `{label}`: {cross}"
    );
    cross.results.assert_holds();
    let brute = cross.brute.label(found.index).expect("searched");
    assert!(
        brute.breaking_within(crashes).is_some(),
        "brute force breaks `{label}` within {crashes} crashes: {cross}"
    );
    let o2 = cross
        .oracle2
        .collections
        .iter()
        .find(|c| c.labels.contains(&found.index))
        .expect("oracle 2 maps the label");
    assert!(
        o2.breaking.iter().any(|b| b.crashes <= crashes),
        "oracle 2 breaks `{label}`: {cross}"
    );
    assert!(!cross.disagreements.is_empty(), "{cross}");
}

/// **Finding 1: interior mutability through allow-listed names.** Each member makes one
/// `Rc<Cell<u32>>` from a source expression (`Rc::default()`: `Rc` is on `STD_PATHS`), pairs
/// every arriving record with it (`cross_product`, a top-level half join: robust), and a `map`
/// numbers the records with `c.get()` and `c.replace(..)`, both on `METHODS`. So the map is a
/// hidden counter, yet every name in it is allowed: known pure, robust, and the program is
/// fragile-free and choice-free.
///
/// Records numbered: one value from source `a` (sent to each member in turn, by `demux`, which
/// carries no consistency label of its own) and a local constant `7`. Each member also holds the
/// constant `1`. Failure-free each member holds {1, 2}. A single crash of `a` right after it
/// sends to member 0 leaves members 1 and 2 numbering only the
/// constant (1): they miss fact `2`. Lineage gives fact `2` whichever record came second, here
/// the local constant, so no fault kills it.
#[cfg(stageleft_runtime)]
fn shared_cell_counter() -> CrossCheck {
    let mut flow = FlowBuilder::new();
    let a = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send_a, in_a) = a.sim_input::<(u32, u32), TotalOrder, ExactlyOnce>();
    // Point-to-point sends to each member: no consistency label on the delivery itself.
    let from_a = in_a
        .map(q!(|(d, v)| (
            hydro_lang::location::MemberId::<()>::from_raw_id(d),
            v
        )))
        .demux(&members, TCP.fail_stop().name("a").bincode())
        .weaken_ordering::<NoOrder>();
    let local = members
        .source_iter(q!([7u32]))
        .weaken_consistency()
        .weaken_ordering::<NoOrder>();
    let cell = members
        .source_iter(q!([std::rc::Rc::<std::cell::Cell<u32>>::default()]))
        .weaken_consistency();
    let counted = from_a
        .merge_unordered(local.into())
        .cross_product(cell)
        .map(q!(|(_x, c): (u32, std::rc::Rc<std::cell::Cell<u32>>)| {
            let n = c.get() + 1;
            c.replace(n);
            n
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
    let mut check = super::EcCheck::new().max_crashes(1);
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 3))
        .compiled();
    eprintln!("fragile sites: {:?}", check.fragile_sites());
    check.cross_check(
        &sim,
        async || {
            for d in 0..3 {
                send_a.send((d, 10));
            }
        },
        MAX_FAULTS,
    )
}

#[cfg(stageleft_runtime)]
#[test]
fn review3a_a_shared_cell_counter_is_known_pure() {
    let cross = shared_cell_counter();
    eprintln!("=== shared cell counter\n{cross}");
    assert_false_certificate(&cross, "numbered", 1);
}
