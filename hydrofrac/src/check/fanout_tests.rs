//! Closed-broadcast fan-out order (docs/design/hydrofrac-races.md §4.2): the recognised shape,
//! and, under every schedule, the order of the sends it makes.
//!
//! Every item is `cfg(stageleft_runtime)`: the module must exist in the staged copy of the crate
//! (its staged closures resolve through it), while what the tests use is host-side only.

#[cfg(stageleft_runtime)]
use std::collections::{BTreeMap, BTreeSet};
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
use crate::provenance::fanout::{self, FANOUT_MARK, is_fan_out};

/// The channels `sim` marked as closed broadcasts, by id.
#[cfg(stageleft_runtime)]
fn marked(sim: &CompiledSim) -> Vec<usize> {
    sim.channels()
        .iter()
        .filter(|c| is_fan_out(c.op.id))
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
                ..
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

/// The channels `sim` marked as sequenced closed broadcasts, by name.
#[cfg(stageleft_runtime)]
fn sequenced_names(sim: &CompiledSim) -> Vec<String> {
    sim.channels()
        .iter()
        .filter(|c| c.op.id == Some(fanout::SEQUENCED_MARK))
        .map(|c| c.name.clone().unwrap_or_default())
        .collect()
}

/// Only a broadcast whose data is directly a `TotalOrder`, `ExactlyOnce` harness input of the
/// sending process is sequenced: not one through a map, an unordered input, a relay (S2's
/// shape), a cluster's input, or a teed input.
#[cfg(stageleft_runtime)]
#[test]
fn only_a_direct_harness_input_of_a_process_is_sequenced() {
    use hydro_lang::live_collections::stream::NoOrder;
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let relay = flow.process::<()>();
    let members = flow.cluster::<()>();
    let net = |name: &'static str| TCP.fail_stop().name(name).bincode();
    let (_s1, direct) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let _o1 = direct
        .broadcast_closed(&members, net("direct"))
        .sim_cluster_output();
    let (_s2, mapped) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let _o2 = mapped
        .map(q!(|v| v + 1))
        .broadcast_closed(&members, net("mapped"))
        .sim_cluster_output();
    let (_s3, unordered) = source.sim_input::<u32, NoOrder, ExactlyOnce>();
    let _o3 = unordered
        .broadcast_closed(&members, net("unordered"))
        .sim_cluster_output();
    let (_s4, hop) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let _o4 = hop
        .send(&relay, net("hop"))
        .broadcast_closed(&members, net("relayed"))
        .sim_cluster_output();
    let (_s5, own) = members.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let _o5 = own
        .broadcast_closed(&members, net("from_members"))
        .values()
        .sim_cluster_output();
    let (_s6, teed) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let _o6 = teed
        .clone()
        .broadcast_closed(&members, net("teed"))
        .sim_cluster_output();
    let _o7 = teed.sim_output();
    let sim = flow
        .sim()
        .with_cluster_size(&members, 2)
        .with_network_hook_points()
        .with_ir_pass(IrPassStage::PostNetwork, |ir| {
            assert_eq!(fanout::mark(ir), 6, "every closed broadcast is recognised");
        })
        .compiled();
    assert_eq!(marked(&sim).len(), 6);
    assert_eq!(sequenced_names(&sim), vec!["direct".to_owned()]);
}

/// Runs every schedule of `sim` under `faults`, returning the records.
#[cfg(stageleft_runtime)]
fn records_under(
    sim: &CompiledSim,
    faults: crate::search::FaultSet<MessageId>,
    body: &mut impl AsyncFnMut(),
) -> Vec<crate::fault::InstanceRecord> {
    let hook = Rc::new(FaultHook::new(&FaultPlan { faults }, sim));
    sim.set_network_hook(hook.clone());
    let mut body = std::panic::AssertUnwindSafe(body);
    sim.exhaustive(async || {
        (*body)().await;
        quiesce().await;
    });
    sim.clear_network_hook();
    hook.drain_instances()
}

/// Checks the claims of `order` in one run: the run agrees with the ranks
/// ([`races::FanOutOrder::check`]), and whenever it sends `l`, every message of `universe` the
/// order says precedes `l` was sent before. Returns how many claims between two payloads held.
#[cfg(stageleft_runtime)]
fn assert_claims(
    order: &crate::races::FanOutOrder,
    universe: &[MessageId],
    record: &crate::fault::InstanceRecord,
) -> usize {
    use crate::races::SendOrder;
    order.check(record.log.sends().map(|(_, m, _, _)| m));
    let sent: Vec<&MessageId> = record
        .log
        .sends()
        .filter(|(_, _, _, fate)| *fate != crate::fault::Fate::SenderDead)
        .map(|(_, m, _, _)| m)
        .collect();
    let mut across = 0;
    for (i, later) in sent.iter().enumerate() {
        for earlier in universe {
            if order.precedes(earlier, later) {
                assert!(
                    sent[..i].contains(&earlier),
                    "{earlier} should precede {later} in {:?}",
                    record.log
                );
                across += usize::from(earlier.bytes != later.bytes);
            }
        }
    }
    across
}

/// The sequence claim (docs/design/hydrofrac-soundness.md §6.4) against brute force. A process
/// broadcasts three harness inputs over a lossy channel to two members. A relay gets the same
/// inputs over a lossy hop, batches them (a schedule choice) and broadcasts what arrives: S2's
/// shape, which must not be sequenced. Under every schedule of every fault set of at most two
/// faults (start crashes and crash points of every node, omissions of every lossy send, and each
/// pair of these with distinct crashed nodes), each run agrees with the ranks the
/// failure-free runs taught, and every proved precedence holds.
///
/// Marking the relay's broadcast sequenced (accepting any data in `is_sequenced`) fails this
/// test, even without its name check: an omission on the hop makes the relay send `1, 3`,
/// against the ranks `1, 2, 3`, and [`races::FanOutOrder::check`] panics.
#[cfg(stageleft_runtime)]
#[test]
fn sequenced_broadcasts_keep_the_harness_order_under_every_fault_set() {
    use crate::fault::{NodeNames, is_lossy};
    use crate::search::{Fault, FaultSet};
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let relay = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let first = input.broadcast_closed(
        &members,
        TCP.lossy_delayed_forever().name("first").bincode(),
    );
    let _first = first.sim_cluster_output();
    let (send_hop, hop) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let _relayed = hop
        .send(&relay, TCP.lossy_delayed_forever().name("hop").bincode())
        .batch(&relay.tick(), nondet!(/** explored */))
        .all_ticks()
        .broadcast_closed(&members, TCP.fail_stop().name("relayed").bincode())
        .sim_cluster_output();
    let sim = flow
        .sim()
        .test_safety_only()
        .with_cluster_size(&members, 2)
        .with_network_hook_points()
        .with_ir_pass(IrPassStage::PostNetwork, |ir| {
            assert_eq!(fanout::mark(ir), 2);
        })
        .compiled();
    assert_eq!(sequenced_names(&sim), vec!["first".to_owned()]);
    let lossy: Vec<usize> = sim
        .channels()
        .iter()
        .filter(|c| is_lossy(&c.networking))
        .map(|c| c.id)
        .collect();
    let mut body = async || {
        for v in [1, 2, 3] {
            send.send(v);
            send_hop.send(v);
        }
    };
    let probe = records_under(&sim, FaultSet::new(), &mut body);
    let sequenced = sim
        .channels()
        .iter()
        .filter(|c| c.op.id == Some(fanout::SEQUENCED_MARK))
        .map(|c| c.id);
    let fanned = sim
        .channels()
        .iter()
        .filter(|c| c.op.id == Some(FANOUT_MARK))
        .map(|c| c.id);
    let mut order = crate::races::FanOutOrder::new(fanned, sequenced);
    for record in &probe {
        order.learn(record.log.sends().map(|(_, m, _, _)| m));
    }
    let names = NodeNames::new(sim.locations());
    let mut universe: Vec<MessageId> = vec![];
    let mut faults: BTreeSet<Fault<MessageId>> = BTreeSet::new();
    for node in names.crashable() {
        faults.insert(Fault::crash_after(names.name(node), None));
    }
    for record in &probe {
        for (_, m, _, _) in record.log.sends() {
            if !universe.contains(m) {
                universe.push(m.clone());
            }
            faults.insert(Fault::crash_after(names.name(&m.from), Some(m.clone())));
            if lossy.contains(&m.channel) {
                faults.insert(Fault::Omit(m.clone()));
            }
        }
    }
    let faults: Vec<Fault<MessageId>> = faults.into_iter().collect();
    let mut sets: Vec<FaultSet<MessageId>> = vec![FaultSet::new()];
    for (i, a) in faults.iter().enumerate() {
        sets.push(FaultSet::from([a.clone()]));
        for b in &faults[i + 1..] {
            let (na, nb) = (a.crashed_node(), b.crashed_node());
            if na.is_none() || nb.is_none() || na != nb {
                sets.push(FaultSet::from([a.clone(), b.clone()]));
            }
        }
    }
    let (mut runs, mut across) = (0, 0);
    for set in &sets {
        for record in records_under(&sim, set.clone(), &mut body) {
            across += assert_claims(&order, &universe, &record);
            runs += 1;
        }
    }
    eprintln!(
        "{} fault sets, {runs} runs, {across} claims across payloads held",
        sets.len()
    );
    assert!(
        across > 0,
        "some precedence between two payloads was claimed"
    );
}

/// A payload the harness sends twice, apart, is not ranked: no claim between payloads is made.
#[cfg(stageleft_runtime)]
#[test]
fn a_payload_sent_twice_is_not_ranked() {
    use crate::races::SendOrder;
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let _out = input
        .broadcast_closed(&members, TCP.fail_stop().name("first").bincode())
        .sim_cluster_output();
    let sim = flow
        .sim()
        .with_cluster_size(&members, 2)
        .with_network_hook_points()
        .with_ir_pass(IrPassStage::PostNetwork, |ir| {
            fanout::mark(ir);
        })
        .compiled();
    let channels: Vec<usize> = sim.channels().iter().map(|c| c.id).collect();
    let records = records_under(&sim, Default::default(), &mut async || {
        send.send(1);
        send.send(2);
        send.send(1);
    });
    let mut order = crate::races::FanOutOrder::new([], channels);
    let sends: Vec<MessageId> = records[0]
        .log
        .sends()
        .map(|(_, m, _, _)| m.clone())
        .collect();
    order.learn(&sends);
    for e in &sends {
        for l in &sends {
            if e.bytes != l.bytes {
                assert!(!order.precedes(e, l), "{e} before {l}");
            }
        }
    }
}

/// A payload the harness sends twice back to back is not ranked either: with copies named, the
/// second copy opens a block of its own (the conservative reading of the rule).
#[cfg(stageleft_runtime)]
#[test]
fn a_payload_sent_twice_back_to_back_is_not_ranked() {
    use crate::races::SendOrder;
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let _out = input
        .broadcast_closed(&members, TCP.fail_stop().name("first").bincode())
        .sim_cluster_output();
    let sim = flow
        .sim()
        .with_cluster_size(&members, 2)
        .with_network_hook_points()
        .with_ir_pass(IrPassStage::PostNetwork, |ir| {
            fanout::mark(ir);
        })
        .compiled();
    let channels: Vec<usize> = sim.channels().iter().map(|c| c.id).collect();
    let records = records_under(&sim, Default::default(), &mut async || {
        send.send(1);
        send.send(1);
        send.send(2);
    });
    let mut order = crate::races::FanOutOrder::new([], channels);
    let sends: Vec<MessageId> = records[0]
        .log
        .sends()
        .map(|(_, m, _, _)| m.clone())
        .collect();
    order.learn(&sends);
    for e in &sends {
        for l in &sends {
            if e.bytes != l.bytes {
                assert!(!order.precedes(e, l), "{e} before {l}");
            }
        }
    }
}

/// A run whose payloads come in another order than the failure-free runs' panics (A8 broken).
#[cfg(stageleft_runtime)]
#[test]
#[should_panic(expected = "the harness is not one fixed sequence (A8)")]
fn a_run_against_the_ranks_panics() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let _out = input
        .broadcast_closed(&members, TCP.fail_stop().name("first").bincode())
        .sim_cluster_output();
    let sim = flow
        .sim()
        .with_cluster_size(&members, 2)
        .with_network_hook_points()
        .with_ir_pass(IrPassStage::PostNetwork, |ir| {
            fanout::mark(ir);
        })
        .compiled();
    let channels: Vec<usize> = sim.channels().iter().map(|c| c.id).collect();
    let run = |a: u32, b: u32| {
        let mut body = async || {
            send.send(a);
            send.send(b);
        };
        records_under(&sim, Default::default(), &mut body)
    };
    let mut order = crate::races::FanOutOrder::new([], channels);
    order.learn(run(1, 2)[0].log.sends().map(|(_, m, _, _)| m));
    order.check(run(2, 1)[0].log.sends().map(|(_, m, _, _)| m));
}
