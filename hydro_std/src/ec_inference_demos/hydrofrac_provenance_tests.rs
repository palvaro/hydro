//! The hydrofrac provenance rewrite on the EC corpus's programs (docs/design/hydrofrac-lite.md
//! §7): each compiles with provenance, runs failure-free over sampled schedules, and every
//! lineage it reports is well-formed. The opaque counts are printed (`--nocapture`) for the report.
//!
//! The tests live here rather than in `hydrofrac` because a simulation dylib can only name the
//! crate under test and its normal dependencies (these programs' types are this crate's).

use std::collections::BTreeSet;

use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
use hydro_lang::location::Location;
use hydro_lang::networking::TCP;
use hydro_lang::nondet::nondet;
use hydro_lang::prelude::FlowBuilder;
use stageleft::q;

#[cfg(stageleft_runtime)]
use support::*;

#[cfg(stageleft_runtime)]
mod support {
    pub(super) use hydrofrac::provenance::check::run_checked as run_corpus;
}

#[cfg(stageleft_runtime)]
const MEMBERS: usize = 3;

#[cfg(stageleft_runtime)]
#[derive(Clone, Copy)]
enum Broadcast {
    Live,
    ReliableClosed,
    ReliableLive,
    Uniform,
}

#[cfg(stageleft_runtime)]
fn broadcast(which: Broadcast, name: &str) {
    use crate::ec_inference_demos::broadcast_live::broadcast_live_from_process;
    use crate::ec_inference_demos::reliable_broadcast::{
        reliable_broadcast_closed, reliable_broadcast_live,
    };
    use crate::ec_inference_demos::uniform_broadcast::uniform_reliable_broadcast_closed;

    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (send, items) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let delivered = match which {
        Broadcast::Live => {
            broadcast_live_from_process(items, &members, TCP.fail_stop().name("bcast").bincode())
                .weaken_consistency()
        }
        Broadcast::ReliableClosed => reliable_broadcast_closed(items, &members).weaken_consistency(),
        Broadcast::ReliableLive => reliable_broadcast_live(items, &members).weaken_consistency(),
        Broadcast::Uniform => {
            uniform_reliable_broadcast_closed(items, &members, 2).weaken_consistency()
        }
    };
    let out = delivered.sim_cluster_output();
    let stats = run_corpus(
        name,
        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&members, MEMBERS),
        8,
        async || {
            send.send(1);
            send.send(2);
            for m in 0..MEMBERS as u32 {
                let got: Vec<u32> = out.collect_n_sorted(m, 2).await;
                assert_eq!(got, vec![1, 2]);
            }
        },
    );
    assert_eq!(stats.opaque_outputs, 0, "{name}: {stats:?}");
}

#[test]
fn corpus_broadcast_live() {
    broadcast(Broadcast::Live, "broadcast_live_from_process");
}

#[test]
fn corpus_reliable_broadcast_closed() {
    broadcast(Broadcast::ReliableClosed, "reliable_broadcast_closed");
}

#[test]
fn corpus_reliable_broadcast_live() {
    broadcast(Broadcast::ReliableLive, "reliable_broadcast_live");
}

#[test]
fn corpus_uniform_broadcast() {
    broadcast(Broadcast::Uniform, "uniform_reliable_broadcast_closed(2)");
}

#[cfg(stageleft_runtime)]
fn gossip(live: bool, name: &str) {
    use crate::ec_inference_demos::crdt_gossip::{g_set_gossip, g_set_gossip_live};

    let mut flow = FlowBuilder::new();
    let members = flow.cluster::<()>();
    let (update, updates) = members.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let (tick, ticks) = members.sim_input::<(), TotalOrder, ExactlyOnce>();
    let updates = updates.weaken_ordering::<NoOrder>();
    let set = if live {
        g_set_gossip_live(&members, updates, ticks)
    } else {
        g_set_gossip(&members, updates, ticks)
    };
    let member_tick = members.tick();
    let out = set
        .weaken_consistency()
        .snapshot(&member_tick, nondet!(/** every change of the set is observed */))
        .into_stream()
        .flat_map_unordered(q!(|set: BTreeSet<u32>| set))
        .all_ticks()
        .sim_cluster_output();
    run_corpus(
        name,
        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&members, MEMBERS),
        8,
        async || {
            for m in 0..MEMBERS as u32 {
                update.send(m, 10 + m);
            }
            hydro_lang::sim::compiled::quiesce().await;
            for _ in 0..3 {
                tick.send_many((0..MEMBERS as u32).map(|m| (m, ())));
                hydro_lang::sim::compiled::quiesce().await;
            }
            let _ = &out;
        },
    );
}

#[test]
fn corpus_g_set_gossip() {
    gossip(false, "g_set_gossip");
}

#[test]
fn corpus_g_set_gossip_live() {
    gossip(true, "g_set_gossip_live");
}

#[test]
fn corpus_leader_merge() {
    use crate::ec_inference_demos::leader_merge::leader_merge_broadcast;

    let mut flow = FlowBuilder::new();
    let writers = flow.cluster::<()>();
    let leader = flow.process::<()>();
    let replicas = flow.cluster::<()>();
    let (write, writes) = writers.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let at_leader = writes.send(&leader, TCP.fail_stop().name("to_leader").bincode());
    let out = leader_merge_broadcast(
        at_leader,
        &replicas,
        TCP.fail_stop().name("merged").bincode(),
        nondet!(/** the leader's interleaving of the writers */),
    )
    .weaken_consistency()
    .sim_cluster_output();
    let stats = run_corpus(
        "leader_merge_broadcast",
        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&writers, 2)
            .with_cluster_size(&replicas, MEMBERS),
        8,
        async || {
            write.send(0, 100);
            write.send(1, 200);
            hydro_lang::sim::compiled::quiesce().await;
            let _ = &out;
        },
    );
    assert_eq!(stats.opaque_outputs + stats.opaque_sends, 0, "{stats:?}");
}

#[test]
fn corpus_multi_paxos() {
    use crate::ec_inference_demos::multi_paxos::multi_paxos;

    let mut flow = FlowBuilder::new();
    let proposers = flow.cluster::<()>();
    let acceptors = flow.cluster::<()>();
    let learners = flow.cluster::<()>();
    let (lead, leads) = proposers.sim_input::<u64, TotalOrder, ExactlyOnce>();
    let (command, commands) = proposers.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let outputs = multi_paxos(&acceptors, &learners, 2, leads, commands);
    let out = outputs.learned.weaken_consistency().sim_cluster_output();
    run_corpus(
        "multi_paxos",
        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&proposers, 1)
            .with_cluster_size(&acceptors, 3)
            .with_cluster_size(&learners, MEMBERS),
        8,
        async || {
            lead.send(0, 1);
            hydro_lang::sim::compiled::quiesce().await;
            command.send(0, 7);
            command.send(0, 8);
            hydro_lang::sim::compiled::quiesce().await;
            let _ = &out;
        },
    );
}

#[cfg(stageleft_runtime)]
fn abd(reliable: bool, name: &str) {
    use crate::ec_inference_demos::abd::{abd_register, abd_register_reliable_phase2};

    let mut flow = FlowBuilder::new();
    let replicas = flow.cluster::<()>();
    let clients = flow.cluster::<()>();
    let (w_send, writes) = clients.sim_input::<(u64, u32), TotalOrder, ExactlyOnce>();
    let (_r_send, reads) = clients.sim_input::<u64, TotalOrder, ExactlyOnce>();
    let outs = if reliable {
        abd_register_reliable_phase2(&replicas, MEMBERS / 2 + 1, writes, reads)
    } else {
        abd_register(&replicas, MEMBERS / 2 + 1, writes, reads)
    };
    let done = outs.write_done.sim_cluster_output();
    let _read = outs.read_result.sim_cluster_output();
    run_corpus(
        name,
        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&replicas, MEMBERS)
            .with_cluster_size(&clients, 1),
        8,
        async || {
            w_send.send(0, (1, 10u32));
            let _: Vec<_> = done.collect_n_sorted(0, 1).await;
        },
    );
}

#[test]
fn corpus_abd_register() {
    abd(false, "abd_register");
}

#[test]
fn corpus_abd_register_reliable_phase2() {
    abd(true, "abd_register_reliable_phase2");
}
