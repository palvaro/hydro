//! Closed-broadcast fan-out order (docs/design/hydrofrac-races.md §4.2): the recognised shape,
//! and, under every schedule, the order of the sends it makes.
//!
//! Every item is `cfg(stageleft_runtime)`: the module must exist in the staged copy of the crate
//! (its staged closures resolve through it), while what the tests use is host-side only.

#[cfg(stageleft_runtime)]
use std::collections::BTreeMap;
#[cfg(stageleft_runtime)]
use std::rc::Rc;

#[cfg(stageleft_runtime)]
use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
#[cfg(stageleft_runtime)]
use hydro_lang::prelude::*;
#[cfg(stageleft_runtime)]
use hydro_lang::sim::compiled::{CompiledSim, quiesce};
#[cfg(stageleft_runtime)]
use hydro_lang::sim::network_hook::IrPassStage;

#[cfg(stageleft_runtime)]
use crate::fault::{FaultHook, FaultPlan, MessageId, NodeId};
#[cfg(stageleft_runtime)]
use crate::provenance::fanout::{self, FANOUT_MARK};

/// The channels `sim` marked as closed broadcasts, by id.
#[cfg(stageleft_runtime)]
fn marked(sim: &CompiledSim) -> Vec<usize> {
    sim.channels()
        .iter()
        .filter(|c| c.op.id == Some(FANOUT_MARK))
        .map(|c| c.id)
        .collect()
}

/// Runs every failure-free schedule of `sim` and checks, in each, that on every marked channel
/// each `(sender, payload)` is sent to members `0, 1, …, n−1` in that order (repeated if the
/// payload is sent again). Returns the number of schedules.
#[cfg(stageleft_runtime)]
fn assert_fan_out_order(sim: &CompiledSim, members: u32, mut body: impl AsyncFnMut()) -> usize {
    let channels = marked(sim);
    let hook = Rc::new(FaultHook::new(&FaultPlan::default(), sim));
    sim.set_network_hook(hook.clone());
    let mut body = std::panic::AssertUnwindSafe(&mut body);
    sim.exhaustive(async || {
        (*body)().await;
        quiesce().await;
    });
    sim.clear_network_hook();
    let records = hook.drain_instances();
    let mut fan_outs = 0;
    for record in &records {
        let mut order: BTreeMap<(usize, NodeId, Rc<[u8]>), Vec<u32>> = BTreeMap::new();
        for (_, msg, _, _) in record.log.sends() {
            let MessageId {
                channel,
                from,
                to,
                bytes,
            } = msg;
            if channels.contains(channel) {
                order
                    .entry((*channel, from.clone(), bytes.clone()))
                    .or_default()
                    .push(to.member.expect("a cluster destination"));
            }
        }
        for (key, destinations) in &order {
            let expected: Vec<u32> = (0..members).cycle().take(destinations.len()).collect();
            assert_eq!(
                destinations, &expected,
                "fan-out order of {key:?} in {:?}",
                record.log
            );
            assert_eq!(destinations.len() % members as usize, 0, "{key:?}");
            fan_outs += 1;
        }
    }
    assert!(fan_outs > 0, "no fan-out observed");
    records.len()
}

/// A process broadcasts its inputs to three members, which batch what they get and echo every
/// value to every member: a process broadcast and a cluster broadcast. Several values arrive at
/// once, and more after the first ones settle.
#[cfg(stageleft_runtime)]
#[test]
fn closed_broadcasts_fan_out_in_member_order() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let first = input.broadcast_closed(&members, TCP.fail_stop().name("first").bincode());
    let tick = members.tick();
    let echoed = first
        .batch(&tick, nondet!(/** explored */))
        .all_ticks()
        .broadcast_closed(&members, TCP.fail_stop().name("echo").bincode())
        .values();
    let _out = echoed.sim_cluster_output();
    let sim = flow
        .sim()
        .with_cluster_size(&members, 3)
        .with_network_hook_points()
        .with_ir_pass(IrPassStage::PostNetwork, |ir| {
            assert_eq!(fanout::mark(ir), 2, "both broadcasts are recognised");
        })
        .compiled();
    assert_eq!(marked(&sim).len(), 2);
    let schedules = assert_fan_out_order(&sim, 3, async || {
        send.send(1);
        send.send(2);
        quiesce().await;
        send.send(3);
    });
    eprintln!("{schedules} schedules");
    assert!(schedules > 1, "the program has schedule choices to explore");
}

/// A broadcast whose payloads go through a batch first: the probe side arrives in arbitrary
/// tick splits, and the order still holds.
#[cfg(stageleft_runtime)]
#[test]
fn closed_broadcasts_after_a_batch_fan_out_in_member_order() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let tick = source.tick();
    let batched = input
        .batch(&tick, nondet!(/** explored */))
        .all_ticks()
        .broadcast_closed(&members, TCP.fail_stop().name("batched").bincode());
    let _out = batched.sim_cluster_output();
    let sim = flow
        .sim()
        .with_cluster_size(&members, 4)
        .with_network_hook_points()
        .with_ir_pass(IrPassStage::PostNetwork, |ir| {
            assert_eq!(fanout::mark(ir), 1);
        })
        .compiled();
    let schedules = assert_fan_out_order(&sim, 4, async || {
        send.send(1);
        send.send(2);
        send.send(3);
    });
    assert!(schedules > 1, "the batch splits the inputs in several ways");
}

/// Only the exact shape is recognised: a hand-written demux to every member, in reverse
/// order, is not.
#[cfg(stageleft_runtime)]
#[test]
fn a_hand_written_fan_out_is_not_recognised() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let out = input
        .flat_map_ordered(q!(|v| (0..3u32)
            .rev()
            .map(move |m| (hydro_lang::location::MemberId::<()>::from_raw_id(m), v))))
        .into_keyed()
        .demux(&members, TCP.fail_stop().name("reversed").bincode());
    let _out = out.sim_cluster_output();
    let sim = flow
        .sim()
        .with_cluster_size(&members, 3)
        .with_network_hook_points()
        .with_ir_pass(IrPassStage::PostNetwork, |ir| {
            assert_eq!(fanout::mark(ir), 0);
        })
        .compiled();
    assert!(marked(&sim).is_empty());
    let _ = send;
}
