//! TEMPORARY: reviewer #2 probes (not committed).

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
fn report(name: &str, check: &EcCheck, cross: &CrossCheck) {
    eprintln!(
        "=== PROBE {name}\nfragile sites: {:?}\n{cross}\nPROBE {name} DISAGREEMENTS ({}): {:#?}",
        check.fragile_sites(),
        cross.disagreements.len(),
        cross.disagreements
    );
    eprintln!("PROBE {name} SEARCH:\n{}", cross.results);
}

/// C1: a tick constant, in a tick that runs only on a lossy delivery.
#[cfg(stageleft_runtime)]
#[test]
#[cfg(stageleft_runtime)]
fn c1_tick_constant() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let tick = members.tick();
    input
        .broadcast_closed(&members, TCP.lossy_delayed_forever().name("initial").bincode())
        .batch(&tick, nondet!(/** probe */))
        .all_ticks()
        .for_each(q!(|_| {}, commutative = manual_proof!(/** no-op */)));
    let nine = tick
        .singleton(q!(9u32))
        .into_stream()
        .all_ticks()
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** probe */
        ));
    let _out = nine.sim_cluster_output();
    let mut check = EcCheck::new().max_crashes(0);
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .compiled();
    let cross = check.cross_check(
        &sim,
        async || {
            send.send(1);
        },
        MAX_FAULTS,
    );
    report("C1", &check, &cross);
}

/// C1b: a tick constant beside a lossy batch, chained at top level with a fail_stop stream.
#[cfg(stageleft_runtime)]
#[test]
fn c1b_tick_constant_fed() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let (send_y, in_y) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let tick = members.tick();
    input
        .broadcast_closed(&members, TCP.lossy_delayed_forever().name("x").bincode())
        .batch(&tick, nondet!(/** probe */))
        .all_ticks()
        .for_each(q!(|_| {}, commutative = manual_proof!(/** no-op */)));
    let y = in_y.broadcast_closed(&members, TCP.fail_stop().name("y").bincode());
    let nine = tick
        .singleton(q!(9u32))
        .into_stream()
        .all_ticks()
        .weaken_ordering::<NoOrder>()
        .merge_unordered(y.weaken_consistency().weaken_ordering::<NoOrder>())
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** probe */
        ));
    let _out = nine.sim_cluster_output();
    let mut check = EcCheck::new().max_crashes(0);
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .compiled();
    let cross = check.cross_check(
        &sim,
        async || {
            send.send(1);
            send_y.send(5);
        },
        MAX_FAULTS,
    );
    report("C1b", &check, &cross);
}

/// C2: `optional_first_tick(1).unwrap_or(singleton(2))`, zero faults.
#[cfg(stageleft_runtime)]
#[test]
#[cfg(stageleft_runtime)]
fn c2_first_tick_unwrap_or() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let tick = members.tick();
    input
        .broadcast_closed(&members, TCP.fail_stop().name("initial").bincode())
        .batch(&tick, nondet!(/** probe */))
        .all_ticks()
        .for_each(q!(|_| {}, commutative = manual_proof!(/** no-op */)));
    let firsts = tick
        .optional_first_tick(q!(1u32))
        .unwrap_or(tick.singleton(q!(2u32)))
        .into_stream()
        .all_ticks()
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** probe */
        ));
    let _out = firsts.sim_cluster_output();
    let mut check = EcCheck::new().max_crashes(0);
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .compiled();
    let cross = check.cross_check(
        &sim,
        async || {
            send.send(1);
            send.send(2);
        },
        MAX_FAULTS,
    );
    report("C2", &check, &cross);
}

/// C3: `tick.singleton(7).defer_tick()`.
#[cfg(stageleft_runtime)]
#[test]
#[cfg(stageleft_runtime)]
fn c3_deferred_constant() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let tick = members.tick();
    input
        .broadcast_closed(&members, TCP.fail_stop().name("initial").bincode())
        .batch(&tick, nondet!(/** probe */))
        .all_ticks()
        .for_each(q!(|_| {}, commutative = manual_proof!(/** no-op */)));
    let sevens = tick
        .singleton(q!(7u32))
        .into_stream()
        .defer_tick()
        .all_ticks()
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** probe */
        ));
    let _out = sevens.sim_cluster_output();
    let mut check = EcCheck::new().max_crashes(0);
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .compiled();
    let cross = check.cross_check(
        &sim,
        async || {
            send.send(1);
            send.send(2);
        },
        MAX_FAULTS,
    );
    report("C3", &check, &cross);
}

/// C4: a batch in a tick that also snapshots a singleton fed by a lossy channel.
#[cfg(stageleft_runtime)]
#[test]
#[cfg(stageleft_runtime)]
fn c4_snapshot_blocks_tick() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let (send2, input2) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let tick = members.tick();
    let count = input2
        .broadcast_closed(&members, TCP.lossy_delayed_forever().name("second").bincode())
        .count();
    count
        .snapshot(&tick, nondet!(/** probe */))
        .into_stream()
        .all_ticks()
        .for_each(q!(|_| {}, commutative = manual_proof!(/** no-op */)));
    let batched = input
        .broadcast_closed(&members, TCP.fail_stop().name("initial").bincode())
        .batch(&tick, nondet!(/** probe */))
        .all_ticks()
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** probe */
        ));
    let _out = batched.sim_cluster_output();
    let mut check = EcCheck::new().max_crashes(0);
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .compiled();
    let cross = check.cross_check(
        &sim,
        async || {
            send.send(1);
            send2.send(5);
        },
        MAX_FAULTS,
    );
    report("C4", &check, &cross);
}

/// C5: the dedup idiom's state, also read by `all_ticks`.
#[cfg(stageleft_runtime)]
#[test]
#[cfg(stageleft_runtime)]
fn c5_dedup_state_leaks() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let tick = members.tick();
    let arrived = input
        .broadcast_closed(&members, TCP.fail_stop().name("initial").bincode())
        .batch(&tick, nondet!(/** probe */))
        .weaken_ordering::<NoOrder>();
    let (handle, prev) = tick.cycle::<Stream<u32, _, Bounded, NoOrder>, _>();
    let state = prev.chain(tick.singleton(q!(0u32)).into_stream().weaken_ordering::<NoOrder>());
    let fresh = arrived.filter_not_in(state.clone());
    handle.complete_next_tick(state.clone().chain(fresh.clone()));
    fresh.all_ticks().for_each(q!(|_| {}, commutative = manual_proof!(/** no-op */)));
    let leaked = state
        .all_ticks()
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** probe */
        ));
    let _out = leaked.sim_cluster_output();
    let mut check = EcCheck::new().max_crashes(0);
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .compiled();
    let cross = check.cross_check(
        &sim,
        async || {
            send.send(1);
            send.send(2);
        },
        MAX_FAULTS,
    );
    report("C5", &check, &cross);
}

/// F4: `atomic()` of a keyed singleton (a snapshot in the simulator).
#[cfg(stageleft_runtime)]
#[test]
#[cfg(stageleft_runtime)]
fn f4_atomic_keyed_singleton() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<(u32, u32), TotalOrder, ExactlyOnce>();
    let firsts = input
        .broadcast_closed(&members, TCP.fail_stop().name("initial").bincode())
        .into_keyed()
        .first()
        .atomic()
        .entries()
        .end_atomic()
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** probe */
        ));
    let _out = firsts.sim_cluster_output();
    let mut check = EcCheck::new().max_crashes(0);
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .compiled();
    let cross = check.cross_check(
        &sim,
        async || {
            send.send((1, 10));
            send.send((2, 20));
        },
        MAX_FAULTS,
    );
    report("F4", &check, &cross);
}

/// F5: an impure closure (a file-backed counter) that misbehaves on its fourth call.
#[cfg(stageleft_runtime)]
#[test]
fn f5_impure_closure() {
    let _ = std::fs::remove_file(std::env::temp_dir().join("hydrofrac_f5_calls"));
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let (send_x, in_x) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    in_x.broadcast_closed(&members, TCP.lossy_delayed_forever().name("x").bincode())
        .for_each(q!(|_| {}, commutative = manual_proof!(/** no-op */)));
    let out = input
        .broadcast_closed(&members, TCP.fail_stop().name("initial").bincode())
        .map(q!(|x: u32| {
            let p = std::env::temp_dir().join("hydrofrac_f5_calls");
            let n = std::fs::read_to_string(&p).map(|s| s.len()).unwrap_or(0);
            std::fs::write(&p, "x".repeat(n + 1)).unwrap();
            if n >= 2 && n % 2 == 0 { x + 1000 } else { x }
        }))
        .assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(manual_proof!(
            /** probe */
        ));
    let _out = out.sim_cluster_output();
    let mut check = EcCheck::new().max_crashes(0);
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .compiled();
    let cross = check.cross_check(
        &sim,
        async || {
            send_x.send(1);
            send.send(1);
        },
        MAX_FAULTS,
    );
    report("F5", &check, &cross);
}

/// Reviewer #1's repro (F2): a batch beside a snapshot that never becomes ready.
#[cfg(stageleft_runtime)]
#[test]
fn zz_review_batch_beside_an_unready_snapshot() {
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
        .for_each(q!(|_| {}, commutative = manual_proof!(/** no-op */)));
    let mut check = EcCheck::new().max_crashes(0);
    let sim = check
        .instrument(flow.sim().with_cluster_size(&members, 2))
        .compiled();
    let cross = check.cross_check(
        &sim,
        async || {
            send_x.send(1);
            send_y.send(7);
        },
        MAX_FAULTS,
    );
    report("R1", &check, &cross);
}
