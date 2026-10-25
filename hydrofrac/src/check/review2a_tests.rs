//! REVIEWER-A (review 2): adversarial counterexamples to the soundness argument
//! (docs/design/hydrofrac-soundness.md). Each test prints the cross-check and asserts what was
//! observed, so a test that passes documents the finding as it stands at `664c60e2f1`. The two
//! owned-counter tests are fixed (the purity allowlist, §3) and now assert the counterexample;
//! the program descriptions below say what the textual guard missed.
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
    // Fixed: a closure is pure only if the allowlist (`provenance::purity`) knows it is, and a
    // block that creates state before returning the closure is not a closure literal. The
    // `map` is an `impure_closure` site, so `numbered`'s derivations through it are fragile, the
    // program has a schedule choice, and the search finds the crash.
    let cross = owned_counter();
    eprintln!("=== owned counter\n{cross}");
    let label = cross.results.label("numbered");
    assert!(
        label.verdict.counterexample().is_some(),
        "the search breaks `numbered`: {cross}"
    );
    cross.assert_agrees();
}

/// The same owned counter, aimed at §13 (Claim B and `Vacuous`). Each member numbers its
/// delivery from `a` and a local constant `7`, and keeps the constant only if it came first.
/// Failure-free `a`'s value always comes first, so no member holds anything: the domain is
/// empty, and since the program is fragile-free (and choice-free), §13 says faults create no
/// fact, and the label is `Vacuous` with no search. A crash of `a` right after it sends to
/// member 0 makes members 1 and 2 number the constant first and hold `7`, while member 0 holds
/// nothing: a divergence that only a fault creates.
#[cfg(stageleft_runtime)]
fn owned_counter_alarm() -> CrossCheck {
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
    let alarm = from_a
        .merge_unordered(local.into())
        .map(q!({
            let (tx, rx) = std::sync::mpsc::channel::<u32>();
            move |x: u32| {
                let n = rx.try_recv().unwrap_or(0) + 1;
                tx.send(n).unwrap();
                (n, x)
            }
        }))
        .filter_map(q!(|(n, x): (u32, u32)| (n == 1 && x == 7).then_some(x)))
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** claimed (wrongly): no member ever numbers the constant first */
        ));
    let _out = alarm.sim_cluster_output();
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
fn review2a_an_owned_counter_makes_a_false_vacuous_verdict() {
    // Fixed: the counting closure is not known pure, so the program is neither fragile-free nor
    // choice-free, §13 does not apply, and the empty-domain label is searched for Claim B.
    let cross = owned_counter_alarm();
    eprintln!("=== owned counter alarm\n{cross}");
    let label = cross.results.label("owned_counter_alarm");
    assert!(
        label.verdict.counterexample().is_some(),
        "the search breaks `alarm`: {cross}"
    );
    cross.assert_agrees();
}

/// A source broadcasts the same value `10` twice (the harness sends it twice) to three members
/// over `fail_stop`, and each member numbers its deliveries with `enumerate`; each member also
/// holds `(0, 10)` locally. Failure-free every member holds {(0, 10), (1, 10)}.
///
/// hydrofrac's crash point `crash(n after m)` fires after the *first* copy of `m` (§0), so no
/// crash it can express falls inside the second round of sends: a crash in the first round
/// leaves no member a second delivery, so every member holds just (0, 10), and the label holds.
/// A real crash of the source right after its second send to member 0 (oracle 2's "crash after
/// send #3") leaves member 0 with {(0, 10), (1, 10)} and members 1 and 2 with {(0, 10)}: fact
/// `(1, 10)` is missing at two correct members. The search certifies.
#[cfg(stageleft_runtime)]
fn numbered_resend() -> CrossCheck {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let numbered = input
        .broadcast_closed(&members, TCP.fail_stop().name("twice").bincode())
        .weaken_consistency()
        .enumerate()
        .weaken_ordering::<NoOrder>();
    let local = members
        .source_iter(q!([(0usize, 10u32)]))
        .weaken_consistency()
        .weaken_ordering::<NoOrder>();
    let numbered_resend = numbered
        .merge_unordered(local.into())
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** claimed (wrongly): every member numbers the same deliveries */
        ));
    let _out = numbered_resend.sim_cluster_output();
    let mut check = EcCheck::new().max_crashes(1);
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 3))
        .compiled();
    eprintln!("fragile sites: {:?}", check.fragile_sites());
    check.cross_check(
        &sim,
        async || {
            send.send(10);
            send.send(10);
        },
        MAX_FAULTS,
    )
}

#[cfg(stageleft_runtime)]
#[test]
fn review2a_a_crash_inside_a_resend_is_outside_the_fault_model() {
    let cross = numbered_resend();
    eprintln!("=== numbered resend\n{cross}");
    eprintln!("disagreements: {:#?}", cross.disagreements);
    let label = cross.results.label("numbered_resend");
    eprintln!(
        "numbered_resend: certified = {}",
        matches!(label.verdict, Verdict::Certified { .. })
    );
    assert!(
        matches!(label.verdict, Verdict::Certified { .. }),
        "the search certifies `numbered_resend`: {cross}"
    );
    assert!(
        cross
            .disagreements
            .iter()
            .any(|d| d.starts_with("oracle 2") && d.contains("numbered_resend")),
        "and oracle 2 breaks it: {cross}"
    );
}

/// Lemma R's "Duplicates" paragraph (§2) says a dependency on a later copy of `m` is served by
/// the first copy. That holds for set-preserving steps, not for `enumerate`, which the
/// choice-free shortcut (§3.1) treats as robust. Within hydrofrac's own fault model:
///
/// A source broadcasts `10` twice to three members over `fail_stop`; each member numbers its
/// deliveries (`enumerate`) and holds `(0, 10)` locally. Member 2, on its first delivery,
/// broadcasts a token to the cluster, and member 0 turns the token into `(1, 10)`.
/// Failure-free every member holds {(0, 10), (1, 10)}.
///
/// Under `crash(source after (10 → member 2))`, the first round completes and the whole second
/// round is cut: members 1 and 2 hold only (0, 10), while member 0 also holds (1, 10) from the
/// token. The clauses of `(member 1, (1, 10))` and `(member 0, (1, 10))` are second copies,
/// whose ids are the first copies', which the fan-out literal proves precede `(10 → member 2)`;
/// that of `(member 2, (1, 10))` is `m` itself. So the kill rule spares every pair, the set is
/// pruned unrun, and the search certifies. Every crash earlier in the first round leaves
/// member 2 without a delivery, so no token and no `(1, 10)` anywhere: the label holds there.
#[cfg(stageleft_runtime)]
fn later_copy() -> CrossCheck {
    use hydro_lang::location::cluster::CLUSTER_SELF_ID;
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let delivered = input
        .broadcast_closed(&members, TCP.fail_stop().name("twice").bincode())
        .weaken_consistency();
    let token = delivered
        .clone()
        .filter(q!(move |_| CLUSTER_SELF_ID.get_raw_id() == 2))
        .broadcast_closed(&members, TCP.fail_stop().name("token").bincode())
        .values()
        .weaken_consistency()
        .filter(q!(move |_| CLUSTER_SELF_ID.get_raw_id() == 0))
        .map(q!(|x: u32| (1usize, x)))
        .weaken_ordering::<NoOrder>();
    let numbered = delivered.enumerate().weaken_ordering::<NoOrder>();
    let local = members
        .source_iter(q!([(0usize, 10u32)]))
        .weaken_consistency()
        .weaken_ordering::<NoOrder>();
    let later_copy = numbered
        .merge_unordered(local.into())
        .merge_unordered(token)
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** claimed (wrongly): every member numbers the same deliveries */
        ));
    let _out = later_copy.sim_cluster_output();
    let mut check = EcCheck::new().max_crashes(1);
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 3))
        .compiled();
    eprintln!("fragile sites: {:?}", check.fragile_sites());
    check.cross_check(
        &sim,
        async || {
            send.send(10);
            send.send(10);
        },
        MAX_FAULTS,
    )
}

#[cfg(stageleft_runtime)]
#[test]
fn review2a_a_crash_cuts_a_later_copy_the_kill_rule_spares() {
    let cross = later_copy();
    eprintln!("=== later copy\n{cross}");
    eprintln!("disagreements: {:#?}", cross.disagreements);
    let label = cross.results.label("later_copy");
    eprintln!(
        "later_copy: certified = {}",
        matches!(label.verdict, Verdict::Certified { .. })
    );
    assert!(
        matches!(label.verdict, Verdict::Certified { .. }),
        "the search certifies `later_copy`: {cross}"
    );
    assert!(
        cross
            .disagreements
            .iter()
            .any(|d| !d.starts_with("oracle 2") && d.contains("later_copy")),
        "and brute force breaks it: {cross}"
    );
}
