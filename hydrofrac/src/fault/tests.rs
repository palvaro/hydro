use std::panic::{AssertUnwindSafe, RefUnwindSafe};
use std::rc::Rc;

use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
use hydro_lang::location::Location;
use hydro_lang::networking::TCP;
use hydro_lang::prelude::*;
use hydro_lang::sim::compiled::CompiledSim;
use hydro_lang::sim::{SimReceiver, SimSender};
use stageleft::q;

use super::*;

/// Runs `thunk` exhaustively under `plan`; the hook and the record of every instance.
fn run(
    sim: &CompiledSim,
    plan: &FaultPlan,
    thunk: impl AsyncFnMut() + RefUnwindSafe,
) -> Vec<InstanceRecord> {
    let hook = Rc::new(FaultHook::new(plan, sim));
    sim.set_network_hook(hook.clone());
    sim.exhaustive(thunk);
    sim.clear_network_hook();
    hook.drain_instances()
}

fn delivered_on(rec: &InstanceRecord, channel: usize) -> usize {
    rec.log
        .sends()
        .filter(|(_, m, _, f)| m.channel == channel && *f == Fate::Delivered)
        .count()
}

fn channel_id(sim: &CompiledSim, name: &str) -> usize {
    sim.channels()
        .iter()
        .find(|c| c.name.as_deref() == Some(name))
        .unwrap_or_else(|| panic!("no channel `{name}`"))
        .id
}

/// Every message the failure-free first instance sends, in send order, without repeats.
fn probe_messages(sim: &CompiledSim, thunk: impl AsyncFnMut() + RefUnwindSafe) -> Vec<MessageId> {
    let records = run(sim, &FaultPlan::default(), thunk);
    let mut msgs: Vec<MessageId> = Vec::new();
    for (_, m, _, _) in records[0].log.sends() {
        if !msgs.contains(m) {
            msgs.push(m.clone());
        }
    }
    msgs
}

#[test]
fn node_names_are_short_and_disambiguated() {
    let mut flow = FlowBuilder::new();
    let p1 = flow.process::<()>();
    let p2 = flow.process::<()>();
    let workers = flow.cluster::<()>();
    let (_in_send, input) = p1.sim_input::<i32, TotalOrder, ExactlyOnce>();
    let _out = input
        .broadcast_closed(&workers, TCP.fail_stop().bincode())
        .send(&p2, TCP.fail_stop().bincode())
        .values()
        .sim_output();
    let sim = flow
        .sim()
        .with_cluster_size(&workers, 2)
        .with_network_hook_points()
        .compiled();

    let names = NodeNames::new(sim.locations());
    let mut all: Vec<Node> = names.crashable().iter().map(|n| names.name(n)).collect();
    all.sort();
    assert_eq!(
        all,
        vec!["cluster@0", "cluster@1", "process#0", "process#1"]
    );
    let nodes: usize = (sim.locations().iter())
        .map(|l| l.members.as_ref().map_or(1, Vec::len))
        .sum();
    assert_eq!(names.crashable().len(), nodes);
    for n in names.crashable() {
        assert_eq!(names.id(&names.name(n)), Some(n));
        assert_eq!(n.node_name(sim.locations()), names.name(n));
    }
    assert_ne!(
        names.name(&NodeId::new(&p1.id(), None)),
        names.name(&NodeId::new(&p2.id(), None))
    );
}

#[test]
fn omissions_apply_only_on_lossy_channels() {
    let mut flow = FlowBuilder::new();
    let p1 = flow.process::<()>();
    let p2 = flow.process::<()>();
    let (in_send, input) = p1.sim_input::<i32, TotalOrder, ExactlyOnce>();
    let lossy = input
        .clone()
        .send(&p2, TCP.lossy_delayed_forever().bincode().name("lossy"));
    let reliable = input.send(&p2, TCP.fail_stop().bincode().name("reliable"));
    let out = lossy.merge_unordered(reliable).sim_output();
    let sim = flow
        .sim()
        .test_safety_only()
        .with_network_hook_points()
        .compiled();
    let lossy_id = channel_id(&sim, "lossy");
    let reliable_id = channel_id(&sim, "reliable");

    let msgs = probe_messages(&sim, async || {
        in_send.send(1);
        let _: Vec<i32> = out.collect_sorted().await;
    });
    assert_eq!(msgs.len(), 2);
    // Omit every message, on both channels: only the lossy channel is faulty.
    let plan = FaultPlan {
        faults: msgs.into_iter().map(Fault::Omit).collect(),
    };
    let records = run(&sim, &plan, async || {
        in_send.send(1);
        let got: Vec<i32> = out.collect_sorted().await;
        assert_eq!(got, vec![1]);
    });
    assert!(!records.is_empty());
    for rec in &records {
        assert_eq!(delivered_on(rec, lossy_id), 0);
        assert_eq!(delivered_on(rec, reliable_id), 1);
        assert!(
            rec.log
                .sends()
                .any(|(_, m, _, f)| m.channel == lossy_id && f == Fate::Omitted)
        );
        assert!(rec.crashed.is_empty());
    }
}

/// `p1` forwards each input, plus one, to `p2` on the fail-stop channel `fwd`.
struct Forward {
    sim: CompiledSim,
    p1: NodeId,
    p2: NodeId,
    channel: usize,
    input: SimSender<i32, TotalOrder, ExactlyOnce>,
    out: SimReceiver<i32, TotalOrder, ExactlyOnce>,
}

fn forward() -> Forward {
    let mut flow = FlowBuilder::new();
    let p1 = flow.process::<()>();
    let p2 = flow.process::<()>();
    let (input, stream) = p1.sim_input::<i32, _, _>();
    let out = stream
        .map(q!(|x| x + 1))
        .send(&p2, TCP.fail_stop().bincode().name("fwd"))
        .sim_output();
    let sim = flow.sim().with_network_hook_points().compiled();
    let channel = channel_id(&sim, "fwd");
    Forward {
        p1: NodeId::new(&p1.id(), None),
        p2: NodeId::new(&p2.id(), None),
        sim,
        channel,
        input,
        out,
    }
}

#[test]
fn positional_crash_delivers_its_message_and_stops_later_sends() {
    let f = forward();
    let (input, out) = (&f.input, f.out);
    let msgs = probe_messages(&f.sim, async || {
        input.send(1);
        input.send(2);
        input.send(3);
        let _: Vec<i32> = out.collect().await;
    });
    assert_eq!(msgs.len(), 3);
    let names = NodeNames::new(f.sim.locations());
    let second = msgs[1].clone();
    let plan = FaultPlan {
        faults: [Fault::crash_after(names.name(&f.p1), Some(second.clone()))].into(),
    };
    let records = run(&f.sim, &plan, async || {
        input.send(1);
        input.send(2);
        input.send(3);
        let got: Vec<i32> = out.collect().await;
        assert_eq!(got, vec![2, 3]);
    });
    assert!(!records.is_empty());
    for rec in &records {
        assert_eq!(rec.crashed, [f.p1.clone()].into());
        let fates: Vec<(u64, Fate)> = rec.log.sends().map(|(_, _, o, f)| (o, f)).collect();
        assert_eq!(fates[0], (0, Fate::Delivered));
        assert_eq!(fates[1], (1, Fate::Delivered));
        // The third message is never sent, or is attempted (in the same execution) and dropped.
        assert!(fates[2..].iter().all(|(_, f)| *f == Fate::SenderDead));
        // The crash fires right after its message.
        let events = rec.log.events();
        let sent = events
            .iter()
            .position(|e| matches!(e, Event::Sent { msg, .. } if *msg == second))
            .unwrap();
        assert_eq!(events[sent + 1], Event::Halted { node: f.p1.clone() });
    }
}

#[test]
fn crash_before_any_send_sends_nothing() {
    let f = forward();
    let (input, out) = (&f.input, f.out);
    let names = NodeNames::new(f.sim.locations());
    let plan = FaultPlan {
        faults: [Fault::crash_after(names.name(&f.p1), None)].into(),
    };
    let records = run(&f.sim, &plan, async || {
        input.send(1);
        out.assert_no_more().await;
    });
    assert!(!records.is_empty());
    for rec in &records {
        assert_eq!(rec.crashed, [f.p1.clone()].into());
        assert_eq!(rec.log.events()[0], Event::Halted { node: f.p1.clone() });
        assert!(rec.log.sends().all(|(_, _, _, f)| f == Fate::SenderDead));
        assert!(
            !rec.log
                .events()
                .iter()
                .any(|e| matches!(e, Event::Output { .. }))
        );
    }
}

#[test]
fn heal_stops_omissions_and_crashes() {
    let mut flow = FlowBuilder::new();
    let p1 = flow.process::<()>();
    let p2 = flow.process::<()>();
    let (in_send, input) = p1.sim_input::<i32, _, _>();
    let out = input
        .send(&p2, TCP.lossy_delayed_forever().bincode().name("lossy"))
        .sim_output();
    let sim = flow
        .sim()
        .test_safety_only()
        .with_network_hook_points()
        .compiled();
    let p1 = NodeId::new(&p1.id(), None);
    let names = NodeNames::new(sim.locations());

    let msgs = probe_messages(&sim, async || {
        in_send.send(7);
        in_send.send(8);
        let _: Vec<i32> = out.collect_sorted().await;
    });
    let (seven, eight) = (msgs[0].clone(), msgs[1].clone());
    // Omit both, and crash `p1` after it sends 8; but 8 is only sent after the heal.
    let plan = FaultPlan {
        faults: [
            Fault::Omit(seven),
            Fault::Omit(eight.clone()),
            Fault::crash_after(names.name(&p1), Some(eight)),
        ]
        .into(),
    };
    let hook = Rc::new(FaultHook::new(&plan, &sim));
    sim.set_network_hook(hook.clone());
    let h = AssertUnwindSafe(hook.clone());
    sim.exhaustive(async || {
        in_send.send(7);
        out.assert_no_more().await;
        h.heal();
        in_send.send(8);
        in_send.send(7);
        let got: Vec<i32> = out.collect_sorted().await;
        assert_eq!(got, vec![7, 8]);
        assert!(h.crashed().is_empty());
    });
    let records = hook.drain_instances();
    assert!(!records.is_empty());
    for rec in records {
        let events = rec.log.events();
        let heal = events.iter().position(|e| *e == Event::Heal).unwrap();
        let mut sends = 0;
        for (i, e) in events.iter().enumerate() {
            if let Event::Sent { msg, fate, .. } = e {
                sends += 1;
                let expect = if i < heal {
                    Fate::Omitted
                } else {
                    Fate::Delivered
                };
                assert_eq!(*fate, expect, "{msg}");
            }
        }
        assert_eq!(sends, 3);
        assert!(rec.crashed.is_empty());
    }
}

#[test]
fn event_log_is_in_program_order_and_tags_round_trip() {
    let f = forward();
    let (input, out) = (&f.input, f.out);
    let records = run(&f.sim, &FaultPlan::default(), async || {
        input.send(1);
        input.send(2);
        let _: Vec<i32> = out.collect().await;
    });
    assert!(!records.is_empty());
    for rec in &records {
        let mut open: Option<&NodeId> = None;
        let mut sent = BTreeMap::new();
        let mut received = BTreeMap::new();
        let mut outputs = 0;
        for e in rec.log.events() {
            match e {
                Event::Exec { node, .. } => {
                    assert!(open.is_none(), "nested execution");
                    open = Some(node);
                }
                Event::ExecEnd => {
                    assert!(open.take().is_some(), "end without begin");
                }
                Event::Sent { id, msg, fate, .. } => {
                    assert_eq!(open, Some(&f.p1), "send outside its sender's execution");
                    assert_eq!(*fate, Fate::Delivered);
                    sent.insert(*id, msg.channel);
                }
                Event::Recv { id, channel, to } => {
                    assert!(sent.contains_key(id), "receive before its send");
                    assert_eq!(
                        open,
                        Some(&f.p2),
                        "receive outside the receiver's execution"
                    );
                    assert_eq!(*to, f.p2);
                    received.insert(*id, *channel);
                }
                Event::Output { node, .. } => {
                    assert_eq!(*node, f.p2);
                    outputs += 1;
                }
                Event::Halted { .. } | Event::Heal => panic!("no faults in this plan"),
            }
        }
        assert_eq!(sent, received);
        assert!(sent.values().all(|c| *c == f.channel));
        // Fresh tags in every instance, from 1.
        assert_eq!(sent.keys().copied().collect::<Vec<_>>(), vec![1, 2]);
        assert_eq!(outputs, 2);
    }
}
