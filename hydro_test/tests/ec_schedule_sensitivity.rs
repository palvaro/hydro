//! Which EC labels of the corpus have a schedule-sensitive causal cone: the static diagnostic
//! `hydrofrac::provenance::sensitivity::label_cones`, over every corpus program (doc
//! docs/design/hydrofrac-lite.md §7).
//!
//! No simulation: each program is built and its IR walked. Run with `--nocapture` for the
//! table (tab-separated: program, label, operator, kind, sensitive?, summary, sites).

use std::collections::BTreeSet;

use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
use hydro_lang::location::Location;
use hydro_lang::networking::TCP;
use hydro_lang::nondet::nondet;
use hydro_lang::prelude::FlowBuilder;
use hydrofrac::provenance::sensitivity::{LabelCone, label_cones, table};

fn cones(program: &str, flow: FlowBuilder<'_>) -> Vec<LabelCone> {
    let built = flow.finalize();
    let cones = label_cones(built.ir());
    print!("{}", table(program, &cones));
    cones
}

fn find<'c>(cones: &'c [LabelCone], name: &str) -> &'c LabelCone {
    cones
        .iter()
        .find(|c| c.collection.display_name() == name)
        .unwrap_or_else(|| {
            panic!(
                "no label `{name}` among {:?}",
                cones
                    .iter()
                    .map(|c| c.collection.display_name().to_owned())
                    .collect::<Vec<_>>()
            )
        })
}

#[derive(Clone, Copy, Debug)]
enum Broadcast {
    Live,
    ReliableClosed,
    ReliableLive,
    Uniform,
}

fn std_broadcast(which: Broadcast) -> Vec<LabelCone> {
    use hydro_std::ec_inference_demos::broadcast_live::broadcast_live_from_process;
    use hydro_std::ec_inference_demos::reliable_broadcast::{
        reliable_broadcast_closed, reliable_broadcast_live,
    };
    use hydro_std::ec_inference_demos::uniform_broadcast::uniform_reliable_broadcast_closed;

    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (_send, items) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let delivered = match which {
        Broadcast::Live => {
            broadcast_live_from_process(items, &members, TCP.fail_stop().name("bcast").bincode())
                .weaken_consistency()
        }
        Broadcast::ReliableClosed => {
            reliable_broadcast_closed(items, &members).weaken_consistency()
        }
        Broadcast::ReliableLive => reliable_broadcast_live(items, &members).weaken_consistency(),
        Broadcast::Uniform => {
            uniform_reliable_broadcast_closed(items, &members, 2).weaken_consistency()
        }
    };
    let _out = delivered.sim_cluster_output();
    cones(&format!("hydro_std {which:?}"), flow)
}

fn gossip(live: bool) -> Vec<LabelCone> {
    use hydro_std::ec_inference_demos::crdt_gossip::{g_set_gossip, g_set_gossip_live};

    let mut flow = FlowBuilder::new();
    let members = flow.cluster::<()>();
    let (_update, updates) = members.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let (_tick, ticks) = members.sim_input::<(), TotalOrder, ExactlyOnce>();
    let updates = updates.weaken_ordering::<NoOrder>();
    let set = if live {
        g_set_gossip_live(&members, updates, ticks)
    } else {
        g_set_gossip(&members, updates, ticks)
    };
    let member_tick = members.tick();
    let _out = set
        .weaken_consistency()
        .snapshot(
            &member_tick,
            nondet!(/** every change of the set is observed */),
        )
        .into_stream()
        .all_ticks()
        .sim_cluster_output();
    cones(
        if live {
            "g_set_gossip_live"
        } else {
            "g_set_gossip"
        },
        flow,
    )
}

fn leader_merge() -> Vec<LabelCone> {
    use hydro_std::ec_inference_demos::leader_merge::leader_merge_broadcast;

    let mut flow = FlowBuilder::new();
    let writers = flow.cluster::<()>();
    let leader = flow.process::<()>();
    let replicas = flow.cluster::<()>();
    let (_write, writes) = writers.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let at_leader = writes.send(&leader, TCP.fail_stop().name("to_leader").bincode());
    let _out = leader_merge_broadcast(
        at_leader,
        &replicas,
        TCP.fail_stop().name("merged").bincode(),
        nondet!(/** the leader's interleaving of the writers */),
    )
    .weaken_consistency()
    .sim_cluster_output();
    cones("leader_merge_broadcast", flow)
}

fn multi_paxos() -> Vec<LabelCone> {
    use hydro_std::ec_inference_demos::multi_paxos::multi_paxos;

    let mut flow = FlowBuilder::new();
    let proposers = flow.cluster::<()>();
    let acceptors = flow.cluster::<()>();
    let learners = flow.cluster::<()>();
    let (_lead, leads) = proposers.sim_input::<u64, TotalOrder, ExactlyOnce>();
    let (_command, commands) = proposers.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let outputs = multi_paxos(&acceptors, &learners, 2, leads, commands);
    let _out = outputs.learned.weaken_consistency().sim_cluster_output();
    cones("multi_paxos", flow)
}

fn abd(reliable: bool) -> Vec<LabelCone> {
    use hydro_std::ec_inference_demos::abd::{abd_register, abd_register_reliable_phase2};

    let mut flow = FlowBuilder::new();
    let replicas = flow.cluster::<()>();
    let clients = flow.cluster::<()>();
    let (_w, writes) = clients.sim_input::<(u64, u32), TotalOrder, ExactlyOnce>();
    let (_r, reads) = clients.sim_input::<u64, TotalOrder, ExactlyOnce>();
    let outs = if reliable {
        abd_register_reliable_phase2(&replicas, 2, writes, reads)
    } else {
        abd_register(&replicas, 2, writes, reads)
    };
    let _done = outs.write_done.sim_cluster_output();
    let _read = outs.read_result.sim_cluster_output();
    cones(
        if reliable {
            "abd_register_reliable_phase2"
        } else {
            "abd_register"
        },
        flow,
    )
}

fn test_broadcast(echo: bool) -> Vec<LabelCone> {
    use hydro_test::cluster::reliable_broadcast::{Broadcaster, Member, Payload, broadcast};

    let mut flow = FlowBuilder::new();
    let broadcaster = flow.process::<Broadcaster>();
    let members = flow.cluster::<Member>();
    let (_send, source) = broadcaster.sim_input::<Payload, TotalOrder, ExactlyOnce>();
    let _out = broadcast(
        &members,
        source,
        echo,
        nondet!(/** test */),
        nondet!(/** test */),
    )
    .sim_cluster_output();
    cones(
        if echo {
            "hydro_test broadcast (echo)"
        } else {
            "hydro_test broadcast (plain)"
        },
        flow,
    )
}

fn two_pc(ctp: bool) -> Vec<LabelCone> {
    use hydro_test::cluster::two_pc_ctp::{Coordinator, Participant, Txn, two_pc};

    let mut flow = FlowBuilder::new();
    let coordinator = flow.process::<Coordinator>();
    let participants = flow.cluster::<Participant>();
    let (_txn, txns) = coordinator.sim_input::<Txn, TotalOrder, ExactlyOnce>();
    let (_timeout, timeouts) = participants.sim_input::<(), TotalOrder, ExactlyOnce>();
    let _committed = two_pc(
        &coordinator,
        &participants,
        3,
        txns,
        timeouts,
        ctp,
        nondet!(/** test */),
        nondet!(/** test */),
        nondet!(/** test */),
        nondet!(/** test */),
        nondet!(/** test */),
    )
    .sim_cluster_output();
    cones(if ctp { "two_pc (ctp)" } else { "two_pc" }, flow)
}

/// A process broadcasting its input over `fail_stop` (the research `ec_ldfi` programs): no
/// non-deterministic operator anywhere, so every derivation is schedule-independent at the
/// operator level.
#[test]
fn a_plain_closed_broadcast_has_a_deterministic_cone() {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (_send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let _out = input
        .broadcast_closed(&members, TCP.fail_stop().bincode())
        .sim_cluster_output();
    let cones = cones("closed broadcast", flow);
    assert!(!cones.is_empty());
    assert!(cones.iter().all(|c| !c.schedule_sensitive()), "{cones:?}");
}

/// The 2PC `committed` label (agreement) is computed through the participants' batches: the
/// case where a derivation holds only under the schedule that produced it.
#[test]
fn two_pc_committed_is_schedule_sensitive() {
    for ctp in [false, true] {
        let cones = two_pc(ctp);
        let committed = cones
            .iter()
            .find(|c| c.collection.name.as_deref() == Some("committed"))
            .or_else(|| cones.last())
            .unwrap();
        assert!(committed.schedule_sensitive(), "{committed:?}");
        assert!(
            committed.sites.iter().any(|s| s.op == "batch"),
            "{committed:?}"
        );
    }
}

/// The whole corpus, for the table. Asserts only that every program's labels are found.
#[test]
fn corpus_table() {
    let mut all: Vec<(String, Vec<LabelCone>)> = vec![];
    for which in [
        Broadcast::Live,
        Broadcast::ReliableClosed,
        Broadcast::ReliableLive,
        Broadcast::Uniform,
    ] {
        all.push((format!("{which:?}"), std_broadcast(which)));
    }
    all.push(("g_set_gossip".into(), gossip(false)));
    all.push(("g_set_gossip_live".into(), gossip(true)));
    all.push(("leader_merge".into(), leader_merge()));
    all.push(("multi_paxos".into(), multi_paxos()));
    all.push(("abd_register".into(), abd(false)));
    all.push(("abd_register_reliable_phase2".into(), abd(true)));
    all.push(("broadcast (plain)".into(), test_broadcast(false)));
    all.push(("broadcast (echo)".into(), test_broadcast(true)));
    all.push(("two_pc".into(), two_pc(false)));
    all.push(("two_pc (ctp)".into(), two_pc(true)));
    let mut labels = 0;
    let mut sensitive = 0;
    let mut programs_sensitive: BTreeSet<String> = BTreeSet::new();
    for (program, cones) in &all {
        assert!(!cones.is_empty(), "{program}: no labels");
        labels += cones.len();
        for c in cones {
            if c.schedule_sensitive() {
                sensitive += 1;
                programs_sensitive.insert(program.clone());
            }
        }
    }
    println!(
        "SUMMARY\t{labels} labels\t{sensitive} with a schedule-sensitive cone\t{} of {} programs: {programs_sensitive:?}",
        programs_sensitive.len(),
        all.len()
    );
    let _ = find(&all[0].1, all[0].1[0].collection.display_name());
}
