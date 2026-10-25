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

/// A per-member sequence number generator, the kind of helper the purity allowlist is meant to
/// refuse ("a call of any other function ... which may hide a `static`, is not known pure").
/// It keeps one counter per member in a `static`.
pub fn next_seq(member: u32) -> u32 {
    static SEQ: std::sync::Mutex<std::collections::BTreeMap<u32, u32>> =
        std::sync::Mutex::new(std::collections::BTreeMap::new());
    let mut seq = SEQ.lock().unwrap();
    let n = seq.entry(member).or_insert(0);
    *n += 1;
    *n
}

/// **Finding 2: a function passed as a value is not a call.** `names_pure` checks a name only
/// when a `(...)` follows it, so `Some(m).map(next_seq)` hands `next_seq` (above, which keeps a
/// `static` counter per member) to the allow-listed `Option::map` and the closure is known
/// pure. The program is the shape of finding 1, with the hidden counter in `next_seq`: each
/// member numbers its delivery from `a` (point to point, `demux`) and a local record, and also
/// holds the constant `1`.
///
/// Failure-free each member numbers two records, so every member holds the same two numbers
/// (the counters persist across runs, but every member's advances by two per run, so they stay
/// equal). A single crash of `a` right after it sends to member 0 makes member 0 number two
/// records and members 1 and 2 only one: member 0 holds a number the others miss. Lineage gives
/// that number whichever record was numbered second, here the local one, so no fault kills it,
/// and the search certifies from the probe alone.
#[cfg(stageleft_runtime)]
fn helper_passed_as_a_value() -> CrossCheck {
    use hydro_lang::location::cluster::CLUSTER_SELF_ID;
    let mut flow = FlowBuilder::new();
    let a = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send_a, in_a) = a.sim_input::<(u32, u32), TotalOrder, ExactlyOnce>();
    let from_a = in_a
        .map(q!(|(d, v)| (
            hydro_lang::location::MemberId::<()>::from_raw_id(d),
            v
        )))
        .demux(&members, TCP.fail_stop().name("a").bincode())
        .weaken_ordering::<NoOrder>();
    let local = members
        .source_iter(q!([CLUSTER_SELF_ID.get_raw_id()]))
        .weaken_consistency()
        .weaken_ordering::<NoOrder>();
    let counted = from_a
        .merge_unordered(local.into())
        .map(q!(|m: u32| Some(m).map(crate::check::review3a_tests::next_seq).unwrap()));
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
            // Each member is sent its own id, which keys its counter.
            for d in 0..3 {
                send_a.send((d, d));
            }
        },
        MAX_FAULTS,
    )
}

#[cfg(stageleft_runtime)]
#[test]
fn review3a_a_helper_passed_as_a_value_is_known_pure() {
    let cross = helper_passed_as_a_value();
    eprintln!("=== helper passed as a value\n{cross}");
    assert_false_certificate(&cross, "numbered", 1);
}

/// **Finding 2 (the allowlist's other doors).** Closures the purity allowlist accepts as known
/// pure although each can call a function it does not know, or change state shared across calls.
/// Each line is a way in; finding 1 and the test above run two of them end to end.
#[test]
fn review3a_the_purity_allowlist_accepts_hidden_state() {
    let pure = |text: &str| crate::provenance::purity::known_pure(text.parse().unwrap(), true);
    // A function passed as a value: only a name followed by `(...)` is checked as a call.
    assert!(pure("|x| Some(x).map(helper).unwrap()"));
    assert!(pure("|x: Vec<u32>| x.into_iter().map(helper).count()"));
    // A capitalised name is taken for a constructor, whatever it is: a user function, or a local
    // bound to one.
    assert!(pure("|x| Bump(x)"));
    assert!(pure("|x| { let F = helper; F(x) }"));
    // A call through a tuple field: only a call after a group is refused as a call through a value.
    assert!(pure("|x| (helper,).0(x)"));
    // A module path whose last module is named like a `std` one (`iter`, `mem`, `cmp`, ...).
    assert!(pure("|x| crate::util::iter::bump(x)"));
    // Listed method names that mutate shared state through `&`: an atomic `static`'s `swap`,
    // a `thread_local!` cell's `replace`, `take` and `get`.
    assert!(pure(
        "|x: u32| x + COUNT.swap(x, std::sync::atomic::Ordering::Relaxed)"
    ));
    assert!(pure("|x: u32| x + LAST.replace(x) + SEEN.take()"));
    // A shared `Rc<Cell<_>>` or `Rc<RefCell<_>>` record, as in finding 1.
    assert!(pure("|(x, c): (u32, Rc<Cell<u32>>)| c.replace(c.get() + x)"));
    assert!(pure("|(x, v): (u32, Rc<RefCell<Vec<u32>>>)| { v.take().len() as u32 + x }"));
}
