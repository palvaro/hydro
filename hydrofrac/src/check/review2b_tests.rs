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
}
