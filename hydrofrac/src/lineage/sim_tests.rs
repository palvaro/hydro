//! The lineage of real runs, recorded by [`crate::fault::FaultHook`].

use std::rc::Rc;

use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
use hydro_lang::location::Location;
use hydro_lang::networking::TCP;
use hydro_lang::prelude::*;
use stageleft::q;

use super::*;
use crate::fault::{FaultHook, FaultPlan};
use crate::search::Fault;

/// `p1 → p2 → p3` on lossy channels, observed at `p3`. Without the provenance rewrite every
/// record is opaque, so the execution rule derives the observation from both hops.
#[test]
fn execution_rule_on_a_two_hop_run() {
    let mut flow = FlowBuilder::new();
    let p1 = flow.process::<()>();
    let p2 = flow.process::<()>();
    let p3 = flow.process::<()>();
    let (in_send, input) = p1.sim_input::<i32, TotalOrder, ExactlyOnce>();
    let out = input
        .send(&p2, TCP.lossy_delayed_forever().bincode().name("hop1"))
        .map(q!(|x| x * 10))
        .send(&p3, TCP.lossy_delayed_forever().bincode().name("hop2"))
        .sim_output();
    let sim = flow
        .sim()
        .test_safety_only()
        .with_network_hook_points()
        .compiled();
    let channel = |name: &str| {
        sim.channels()
            .iter()
            .find(|c| c.name.as_deref() == Some(name))
            .unwrap()
            .id
    };
    let (hop1, hop2) = (channel("hop1"), channel("hop2"));
    let tables = Tables::new(sim.channels(), sim.locations());
    let (n1, n3) = (NodeId::new(&p1.id(), None), NodeId::new(&p3.id(), None));

    let hook = Rc::new(FaultHook::new(&FaultPlan::default(), &sim));
    sim.set_network_hook(hook.clone());
    sim.exhaustive(async || {
        in_send.send(1);
        let got: Vec<i32> = out.collect_sorted().await;
        assert_eq!(got, vec![10]);
    });
    sim.clear_network_hook();
    let records = hook.drain_instances();
    assert!(!records.is_empty());

    for rec in &records {
        let lineage = derive(&rec.log, &tables);
        let observations: Vec<_> = lineage.observations().collect();
        assert_eq!(observations.len(), 1);
        assert_eq!(*observations[0].node, n3);
        let clause = lineage.clause_for(observations[0].event);
        let channels: Vec<usize> = clause.deps.iter().map(|d| d.message.channel).collect();
        assert_eq!(channels.len(), 2, "{clause:?}");
        assert!(channels.contains(&hop1) && channels.contains(&hop2));
        assert!(clause.deps.iter().all(|d| d.lossy && d.sender.is_some()));
        // Omitting either hop, or crashing the first sender before it sends, kills it.
        for dep in &clause.deps {
            assert!(clause.hit_by(&Fault::Omit(dep.message.clone())));
        }
        assert!(clause.hit_by(&Fault::crash_after(tables.names().name(&n1), None)));
        assert!(lineage.opaque_fallbacks() >= 1);
    }
}

/// `p1 → p2 → p3` as above, plus an unrelated `noise` message `p1 → p3` that `p3` may receive
/// before it computes its output. The execution rule cannot tell the two apart; record
/// provenance (the rewrite, end to end through the network hook) names only the two hops.
#[test]
fn record_provenance_is_precise_end_to_end() {
    let derived_channels = |provenance: bool| {
        let mut flow = FlowBuilder::new();
        let p1 = flow.process::<()>();
        let p2 = flow.process::<()>();
        let p3 = flow.process::<()>();
        let (in_send, input) = p1.sim_input::<i32, TotalOrder, ExactlyOnce>();
        let (noise_send, noise) = p1.sim_input::<i32, TotalOrder, ExactlyOnce>();
        noise
            .send(&p3, TCP.fail_stop().bincode().name("noise"))
            .for_each(q!(|_| {}));
        let out = input
            .send(&p2, TCP.lossy_delayed_forever().bincode().name("hop1"))
            .map(q!(|x| x * 10))
            .send(&p3, TCP.lossy_delayed_forever().bincode().name("hop2"))
            .sim_output();
        let sim = flow.sim().test_safety_only().with_network_hook_points();
        let sim = if provenance {
            crate::provenance::install(sim, None)
        } else {
            sim
        }
        .compiled();
        let noise_channel = sim
            .channels()
            .iter()
            .find(|c| c.name.as_deref() == Some("noise"))
            .unwrap()
            .id;
        let tables = Tables::new(sim.channels(), sim.locations());

        let hook = Rc::new(FaultHook::new(&FaultPlan::default(), &sim));
        sim.set_network_hook(hook.clone());
        sim.exhaustive(async || {
            noise_send.send(0);
            in_send.send(1);
            let got: Vec<i32> = out.collect_sorted().await;
            assert_eq!(got, vec![10]);
        });
        sim.clear_network_hook();
        let records = hook.drain_instances();
        assert!(!records.is_empty());

        let mut with_noise = 0;
        for rec in &records {
            let lineage = derive(&rec.log, &tables);
            let observations: Vec<_> = lineage.observations().collect();
            assert_eq!(observations.len(), 1);
            if provenance {
                assert!(
                    matches!(decode_meta(observations[0].meta), RecordProv::Ids(_)),
                    "the output carries precise provenance"
                );
                assert_eq!(lineage.opaque_fallbacks(), 0);
            }
            let clause = lineage.clause_for(observations[0].event);
            assert_eq!(
                clause
                    .deps
                    .iter()
                    .filter(|d| d.message.channel != noise_channel)
                    .count(),
                2,
                "{clause:?}"
            );
            if clause
                .deps
                .iter()
                .any(|d| d.message.channel == noise_channel)
            {
                with_noise += 1;
            }
        }
        (with_noise, records.len())
    };

    let (with_noise, _) = derived_channels(true);
    assert_eq!(
        with_noise, 0,
        "provenance never blames the unrelated message"
    );
    let (with_noise, schedules) = derived_channels(false);
    assert!(
        with_noise > 0,
        "the execution rule blames the unrelated message in some schedule (of {schedules})"
    );
}
