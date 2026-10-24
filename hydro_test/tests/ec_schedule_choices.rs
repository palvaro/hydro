//! Which corpus programs have schedule choices (docs/design/hydrofrac-lite.md §4.9): the static
//! scan `hydrofrac::provenance::choices::program_choices`, over every corpus program. A program
//! with none is the only kind whose labels hydrofrac certifies.
//!
//! No simulation: each program is built and its IR scanned. Run with `--nocapture` for the
//! table (tab-separated: program, labels, choices).

use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
use hydro_lang::location::Location;
use hydro_lang::networking::TCP;
use hydro_lang::nondet::nondet;
use hydro_lang::prelude::FlowBuilder;
use hydrofrac::observe::ec_collections;
use hydrofrac::provenance::choices::{ChoiceSite, program_choices};

/// A corpus program's labels (by display name) and schedule choices.
struct Scanned {
    labels: Vec<String>,
    choices: Vec<ChoiceSite>,
}

fn scan(program: &str, flow: FlowBuilder<'_>) -> Scanned {
    let built = flow.finalize();
    let scanned = Scanned {
        labels: ec_collections(built.ir())
            .iter()
            .map(|c| c.display_name().to_owned())
            .collect(),
        choices: program_choices(built.ir()),
    };
    println!(
        "{program}\t{}\t{}",
        scanned.labels.len(),
        if scanned.choices.is_empty() {
            "-".to_owned()
        } else {
            scanned
                .choices
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("; ")
        }
    );
    scanned
}

#[derive(Clone, Copy, Debug)]
enum Broadcast {
    Live,
    ReliableClosed,
    ReliableLive,
    Uniform,
}

fn std_broadcast(which: Broadcast) -> Scanned {
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
    scan(&format!("hydro_std {which:?}"), flow)
}

fn gossip(live: bool) -> Scanned {
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
    scan(
        if live {
            "g_set_gossip_live"
        } else {
            "g_set_gossip"
        },
        flow,
    )
}

fn leader_merge() -> Scanned {
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
    scan("leader_merge_broadcast", flow)
}

fn multi_paxos() -> Scanned {
    use hydro_std::ec_inference_demos::multi_paxos::multi_paxos;

    let mut flow = FlowBuilder::new();
    let proposers = flow.cluster::<()>();
    let acceptors = flow.cluster::<()>();
    let learners = flow.cluster::<()>();
    let (_lead, leads) = proposers.sim_input::<u64, TotalOrder, ExactlyOnce>();
    let (_command, commands) = proposers.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let outputs = multi_paxos(&acceptors, &learners, 2, leads, commands);
    let _out = outputs.learned.weaken_consistency().sim_cluster_output();
    scan("multi_paxos", flow)
}

fn abd(reliable: bool) -> Scanned {
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
    scan(
        if reliable {
            "abd_register_reliable_phase2"
        } else {
            "abd_register"
        },
        flow,
    )
}

fn test_broadcast(echo: bool) -> Scanned {
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
    scan(
        if echo {
            "hydro_test broadcast (echo)"
        } else {
            "hydro_test broadcast (plain)"
        },
        flow,
    )
}

fn two_pc(ctp: bool) -> Scanned {
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
    scan(if ctp { "two_pc (ctp)" } else { "two_pc" }, flow)
}

/// A process broadcasting its input over `fail_stop` (the research `ec_ldfi` programs).
fn closed_broadcast() -> Scanned {
    let mut flow = FlowBuilder::new();
    let source = flow.process::<()>();
    let members = flow.cluster::<()>();
    let (_send, input) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
    let _out = input
        .broadcast_closed(&members, TCP.fail_stop().bincode())
        .sim_cluster_output();
    scan("closed broadcast", flow)
}

/// Every corpus program, by name.
fn corpus() -> Vec<(&'static str, Scanned)> {
    vec![
        ("closed broadcast", closed_broadcast()),
        (
            "broadcast_live_from_process",
            std_broadcast(Broadcast::Live),
        ),
        (
            "reliable_broadcast_closed",
            std_broadcast(Broadcast::ReliableClosed),
        ),
        (
            "reliable_broadcast_live",
            std_broadcast(Broadcast::ReliableLive),
        ),
        (
            "uniform_reliable_broadcast_closed",
            std_broadcast(Broadcast::Uniform),
        ),
        ("g_set_gossip", gossip(false)),
        ("g_set_gossip_live", gossip(true)),
        ("leader_merge_broadcast", leader_merge()),
        ("multi_paxos", multi_paxos()),
        ("abd_register", abd(false)),
        ("abd_register_reliable_phase2", abd(true)),
        ("hydro_test broadcast (plain)", test_broadcast(false)),
        ("hydro_test broadcast (echo)", test_broadcast(true)),
        ("two_pc", two_pc(false)),
        ("two_pc (ctp)", two_pc(true)),
    ]
}

/// The whole-program classification of the corpus. Only the plain closed broadcast and the
/// `hydro_std` broadcasts (`broadcast_live_from_process`, and the reliable broadcasts, closed
/// and live) have no schedule choice. Every other program batches, folds an unordered stream or
/// observes somewhere, so none of its labels can be certified.
#[test]
fn corpus_schedule_choices() {
    let corpus = corpus();
    for (program, scanned) in &corpus {
        assert!(!scanned.labels.is_empty(), "{program}: no labels");
    }
    let choice_free: Vec<&str> = corpus
        .iter()
        .filter(|(_, s)| s.choices.is_empty())
        .map(|(program, _)| *program)
        .collect();
    println!(
        "SUMMARY\t{} of {} programs choice-free: {choice_free:?}",
        choice_free.len(),
        corpus.len()
    );
    assert_eq!(
        choice_free,
        [
            "closed broadcast",
            "broadcast_live_from_process",
            "reliable_broadcast_closed",
            "reliable_broadcast_live"
        ]
    );
}

/// 2PC batches at the participants, with or without cooperative termination: the case where a
/// derivation holds only under the schedule that produced it.
#[test]
fn two_pc_has_a_batch() {
    for ctp in [false, true] {
        let scanned = two_pc(ctp);
        assert!(
            scanned
                .choices
                .iter()
                .any(|c| c.op == "batch" && c.site.starts_with("two_pc_ctp.rs:")),
            "{:?}",
            scanned.choices
        );
    }
}
