//! REVIEWER-B (review 2): adversarial cross-checks of the races search
//! (`races/`, `check/races_engine.rs`). Each test asserts that the search agrees with the
//! brute-force enumeration and oracle 2; a failing test is a disagreement.
//!
//! Every item is `cfg(stageleft_runtime)`, as in `brute_tests.rs`.

#[cfg(stageleft_runtime)]
use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
#[cfg(stageleft_runtime)]
use hydro_lang::location::cluster::EventualConsistency;
#[cfg(stageleft_runtime)]
use hydro_lang::prelude::*;

#[cfg(stageleft_runtime)]
use super::{CrossCheck, EcCheck};

#[cfg(stageleft_runtime)]
const MAX_FAULTS: usize = 2;

#[cfg(stageleft_runtime)]
fn agree(name: &str, cross: &CrossCheck) {
    eprintln!("=== {name}\n{cross}");
    cross.assert_agrees();
}

/// One label, two parts. `plain`: every member holds the constant 10 (a top-level iterator, no
/// delivery: robust, the whole domain). `missing`: the `missing_alarm` of `brute_tests.rs`
/// (process `source` broadcasts 1; in the recovery phase a probe reaches every member, and a
/// member that never heard 1 reports 101). Without faults no member reports 101, so the domain
/// is `{10}`. A crash of `source` right after its send to member 0 kills no derivation of 10, so
/// the search skips it, yet members 1 and 2 then report 101 and member 0 does not.
#[cfg(stageleft_runtime)]
fn divergence_beside_a_robust_fact(mut check: EcCheck) -> CrossCheck {
    let mut flow = FlowBuilder::new();
    let vsrc = flow.process::<()>();
    let source = flow.process::<()>();
    let prober = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send_v, in_v) = vsrc.sim_input::<u32, TotalOrder, ExactlyOnce>();
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
        .map(q!(|x| x + 100))
        .weaken_consistency();
    let _ = in_v;
    let plain = members
        .source_iter(q!(vec![10u32]))
        .weaken_boundedness::<Unbounded>()
        .weaken_consistency()
        .weaken_ordering::<NoOrder>();
    let facts = plain
        .merge_unordered(missing)
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** claimed (wrongly): every member misses the same inputs */
        ));
    let _out = facts.sim_cluster_output();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 3))
        .compiled();
    check.cross_check_then_recover(
        &sim,
        async || {
            let _ = &send_v;
            send.send(1);
        },
        async || send_probe.send(1),
        MAX_FAULTS,
    )
}

#[cfg(stageleft_runtime)]
#[test]
fn review2b_fault_created_divergence_beside_a_robust_fact() {
    let cross = divergence_beside_a_robust_fact(EcCheck::new().max_crashes(1));
    agree("fault-created divergence beside a robust fact", &cross);
}

/// The same shape, but the alarm panics instead of reporting: a member that never heard 1 when
/// the recovery probe arrives panics. The label is only `plain` (10 from `vsrc`). A crash of
/// `source` kills no derivation of 10, so the search skips it, yet the run panics, which the
/// engine counts as a failure wherever it does run it.
#[cfg(stageleft_runtime)]
#[test]
fn review2b_fault_created_panic_beside_a_robust_fact() {
    let mut flow = FlowBuilder::new();
    let vsrc = flow.process::<()>();
    let source = flow.process::<()>();
    let prober = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send_v, in_v) = vsrc.sim_input::<u32, TotalOrder, ExactlyOnce>();
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
    probes
        .batch(&tick, nondet!(/** which tick a probe lands in */))
        .weaken_ordering::<NoOrder>()
        .filter_not_in(heard)
        .count()
        .into_stream()
        .all_ticks()
        .for_each(q!(|n| if n > 0 {
            panic!("{n} input(s) never arrived")
        }));
    let plain = in_v
        .broadcast_closed(&members, TCP.fail_stop().name("plain").bincode())
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** a fail_stop broadcast from a node nobody crashes */
        ));
    let _out = plain.sim_cluster_output();
    let mut check = EcCheck::new().max_crashes(1);
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 3))
        .compiled();
    let cross = check.cross_check_then_recover(
        &sim,
        async || {
            send_v.send(10);
            send.send(1);
        },
        async || send_probe.send(1),
        MAX_FAULTS,
    );
    agree("fault-created panic beside a robust fact", &cross);
    // `arrived` breaks only by the panic, which names no broken fact: its diagram is the whole
    // run, less the other labels' outputs and the raw bytes of `sim_cluster_output`.
    let label = cross.results.label("arrived");
    let cex = label.verdict.counterexample().expect("the panic breaks it");
    let diagram = cex.diagram.as_ref().expect("the diagram was written");
    let dot = std::fs::read_to_string(diagram.with_extension("dot")).unwrap();
    assert!(dot.contains("⇒ 1\""), "{dot}");
    assert!(!dot.contains("⇒ 10") && !dot.contains("⇒ 0a000000"), "{dot}");
    assert!(dot.contains("unrelated outputs not shown"), "{dot}");
}

/// A race with no fault: `source` broadcasts `1..=items`, and each member reports `probed` as
/// missing if the probe for it lands in a tick before the data does (the `missing_alarm` shape,
/// with the probe sent in the fault phase). With `probed` 1 the members can disagree in
/// failure-free schedules; with a probe for a value never sent, every member reports it, in
/// every schedule. More items, more schedules.
#[cfg(stageleft_runtime)]
fn racy_alarm(
    check: &mut EcCheck,
    items: u32,
    probed: u32,
) -> (
    hydro_lang::sim::compiled::CompiledSim,
    impl FnMut() + use<>,
) {
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
            /** claimed (wrongly): the probe never overtakes the data */
        ));
    let _out = missing.sim_cluster_output();
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 3))
        .compiled();
    (sim, move || {
        for item in 1..=items {
            send.send(item);
        }
        send_probe.send(probed);
    })
}

/// The parent's question: does a sampled verdict that missed the breaking schedule pass as a
/// certificate? With one item, `racy_alarm` is within the check's schedule budget, and the race
/// breaks `missing` with no fault, a certified counterexample. With `ITEMS` items it is past the
/// budget, so the probe, and with it every label's search, is sampled. Then:
/// - with the racing probe, whatever the sample shows is no certificate: a counterexample's
///   tolerance is not certified, and a miss is `Exhausted`, which `assert_holds` refuses;
/// - with a probe that cannot race, every label holds in every schedule. That is what a sampled
///   miss looks like to the search, which cannot tell a label that holds from one whose
///   breaking schedule fell outside the sample: every verdict must be `Exhausted`, and
///   `assert_holds` must refuse it.
#[cfg(stageleft_runtime)]
#[test]
fn review2b_a_sampled_miss_is_not_accepted_by_assert_holds() {
    use super::Verdict;
    const ITEMS: u32 = 3;
    let refuses = |results: &super::Results| {
        assert!(!results.holds(), "{results}");
        let refused =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| results.assert_holds()));
        assert!(refused.is_err(), "assert_holds accepted a sampled search: {results}");
    };
    let run = |items, probed| {
        let mut check = EcCheck::new().max_crashes(0);
        let (sim, mut inputs) = racy_alarm(&mut check, items, probed);
        check.run(&sim, async || inputs())
    };

    let full = run(1, 1);
    let cex = full.label("missing").verdict.counterexample();
    assert!(
        cex.is_some_and(|c| c.faults.is_empty() && c.tolerance_certified),
        "exhaustively, the race breaks it with no fault: {full}"
    );

    let racing = run(ITEMS, 1);
    for label in racing.results() {
        match &label.verdict {
            Verdict::Counterexample(cex) => assert!(!cex.tolerance_certified, "{racing}"),
            Verdict::Exhausted { .. } => {}
            _ => panic!("a sampled search must not certify `{}`: {racing}", label.name()),
        }
    }
    refuses(&racing);

    let holding = run(ITEMS, 0);
    for label in holding.results() {
        assert!(
            matches!(label.verdict, Verdict::Exhausted { .. }),
            "a sampled search must not certify `{}`: {holding}",
            label.name()
        );
    }
    holding.assert_no_counterexample();
    refuses(&holding);
    let message = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| holding.assert_holds()))
        .expect_err("refused above");
    let message = message.downcast_ref::<String>().expect("a formatted message");
    assert!(message.contains("schedules were sampled"), "{message}");
}

/// A fragile-free program with schedule choices (batches that snapshot nothing) and duplicate
/// messages. `source` broadcasts 1 over a lossy channel. Each member batches what it got and
/// forwards it to `relay` over `fail_stop`. `relay` re-broadcasts every copy it gets (up to
/// three identical fan-outs, so identical `MessageId`s). Label `facts`: direct ∪ relayed.
/// This exercises the fragile-free shortcuts: one schedule for exact crash points, dead facts,
/// and `sends_complete`.
#[cfg(stageleft_runtime)]
fn relay_with_batches(crashes: usize) -> CrossCheck {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let relay = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let direct = input
        .broadcast_closed(
            &members,
            TCP.lossy_delayed_forever().name("direct").bincode(),
        )
        .weaken_consistency();
    let tick = members.tick();
    let up = direct
        .clone()
        .batch(&tick, nondet!(/** which tick a delivery lands in */))
        .all_ticks()
        .send(&relay, TCP.fail_stop().name("up").bincode())
        .values();
    let relayed = up
        .broadcast_closed(&members, TCP.fail_stop().name("down").bincode())
        .weaken_consistency();
    let facts = direct
        .merge_unordered(relayed)
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** claimed: the relay repairs lost direct deliveries */
        ));
    let _out = facts.sim_cluster_output();
    let mut check = EcCheck::new().max_crashes(crashes);
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 3))
        .compiled();
    check.cross_check(&sim, async || send.send(1), MAX_FAULTS)
}

#[cfg(stageleft_runtime)]
#[test]
fn review2b_relay_with_batches_and_duplicates() {
    agree("relay with batches, one crash", &relay_with_batches(1));
    agree("relay with batches, two crashes", &relay_with_batches(2));
}

/// Two sources broadcast the same 1 over `fail_stop`, and every member echoes every copy it
/// hears to every member: each member sends one payload, twice, so its crash points are exact
/// (one fan-out chain). Label `known`: heard ∪ echoed.
#[cfg(stageleft_runtime)]
#[test]
fn review2b_double_echo_of_one_payload() {
    let mut flow = FlowBuilder::new();
    let first = flow.process::<()>();
    let second = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send_a, in_a) = first.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let (send_b, in_b) = second.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let heard = in_a
        .broadcast_closed(&members, TCP.fail_stop().name("a").bincode())
        .weaken_consistency()
        .weaken_ordering::<NoOrder>()
        .merge_unordered(
            in_b.broadcast_closed(&members, TCP.fail_stop().name("b").bincode())
                .weaken_consistency(),
        );
    let echoed = heard
        .clone()
        .broadcast_closed(&members, TCP.fail_stop().name("echo").bincode())
        .values()
        .weaken_consistency();
    let known = heard
        .merge_unordered(echoed)
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** claimed: an echo spreads what anyone heard */
        ));
    let _out = known.sim_cluster_output();
    let mut check = EcCheck::new().max_crashes(2);
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 3))
        .compiled();
    let cross = check.cross_check(
        &sim,
        async || {
            send_a.send(1);
            send_b.send(1);
        },
        MAX_FAULTS,
    );
    agree("double echo, two crashes", &cross);
}
