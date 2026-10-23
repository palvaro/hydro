//! Which EC labels of the corpus have a schedule-sensitive causal cone: the static diagnostic
//! `hydrofrac::provenance::sensitivity::label_cones`, over every corpus program (doc
//! docs/design/hydrofrac-lite.md §4.9, §7).
//!
//! No simulation: each program is built and its IR walked, by the order-insensitivity analysis
//! (`Analysis::Calm`, what the check uses) and by the conservative walk it refines
//! (`Analysis::Conservative`), which every program checks it against: the sharper analysis may
//! only drop sites. Run with `--nocapture` for the tables (tab-separated: program, label,
//! operator, kind, sensitive?, sensitive through its own cone?, summary, sites, discharged
//! choices; the conservative walk's lines start with `OLD`).

use std::collections::BTreeSet;

use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
use hydro_lang::location::Location;
use hydro_lang::networking::TCP;
use hydro_lang::nondet::nondet;
use hydro_lang::prelude::FlowBuilder;
use hydrofrac::provenance::sensitivity::{
    LabelCone, NondetSite, label_cones, label_cones_conservative, table,
};

/// Whether two sites are the same site (whatever the path that reached them).
fn same_site(a: &NondetSite, b: &NondetSite) -> bool {
    a.site == b.site && a.op == b.op && a.location == b.location
}

/// Checks that `calm` refines `old`, label by label: every site it reports, the conservative
/// walk reports too, so every label it calls sensitive is sensitive there.
fn assert_refines(program: &str, calm: &[LabelCone], old: &[LabelCone]) {
    assert_eq!(calm.len(), old.len(), "{program}");
    for (c, o) in calm.iter().zip(old) {
        assert_eq!(
            (c.collection.display_name(), &c.collection.op),
            (o.collection.display_name(), &o.collection.op),
            "{program}"
        );
        for site in &c.sites {
            assert!(
                o.sites.iter().any(|s| same_site(s, site)),
                "{program} `{}`: {site} is not among the conservative walk's sites {:?}",
                c.collection.display_name(),
                o.sites
            );
        }
        if c.schedule_sensitive() {
            assert!(o.schedule_sensitive(), "{program}");
        }
    }
}

/// Both analyses of `flow`'s labels, printed; the conservative one must be refined.
fn both(program: &str, flow: FlowBuilder<'_>) -> (Vec<LabelCone>, Vec<LabelCone>) {
    let built = flow.finalize();
    let calm = label_cones(built.ir());
    let old = label_cones_conservative(built.ir());
    print!("{}", table(program, &calm));
    for line in table(program, &old).lines() {
        println!("OLD\t{line}");
    }
    assert_refines(program, &calm, &old);
    OLD.with(|o| o.borrow_mut().push(old.clone()));
    (calm, old)
}

thread_local! {
    /// The conservative cones of every program [`both`] analysed on this thread, in order.
    static OLD: std::cell::RefCell<Vec<Vec<LabelCone>>> = const { std::cell::RefCell::new(vec![]) };
}

fn cones(program: &str, flow: FlowBuilder<'_>) -> Vec<LabelCone> {
    both(program, flow).0
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

/// Cooperative termination's race (docs §4.9, U1) is in `committed`'s own cone, not only in a
/// sender's send order: a participant answers a query with what it has decided *in the tick
/// the query arrives* (a join inside the tick), so the order-insensitivity analysis keeps the
/// batches, naming the join. The cone is the same for any number of participants (the
/// three-participant program is the one the false certificate was found on).
#[test]
fn two_pc_ctp_committed_is_sensitive_in_its_own_cone() {
    let cones = two_pc(true);
    let committed = cones
        .iter()
        .find(|c| c.collection.name.as_deref() == Some("committed"))
        .expect("committed");
    assert!(committed.existence_sensitive(), "{committed:?}");
    assert!(
        committed.sites.iter().any(|s| s.op == "batch"
            && !s.send_order
            && s.via.as_deref().is_some_and(|v| v.contains("Join"))),
        "a batch, through the answers' join: {committed:?}"
    );
}

/// The echoing broadcast's deliveries go through `filter_not_in` against the member's own
/// history, which the order-insensitivity analysis discharges: its batch matters only for the
/// order of the members' echoes (U2), so the label stays sensitive, through its senders only.
#[test]
fn the_echoing_broadcast_is_sensitive_only_through_its_senders() {
    let cones = test_broadcast(true);
    let deliveries = find(&cones, "deliveries");
    assert!(deliveries.schedule_sensitive(), "{deliveries:?}");
    assert!(!deliveries.existence_sensitive(), "{deliveries:?}");
}

/// The whole corpus, for the table. Asserts that every program's labels are found and prints
/// the totals of both analyses (`SUMMARY`; docs §4.9).
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
    let old: Vec<Vec<LabelCone>> = OLD.with(|o| o.borrow().clone());
    assert_eq!(old.len(), all.len());
    let count = |which: &str,
                 cones: &[(String, &Vec<LabelCone>)],
                 f: &dyn Fn(&LabelCone) -> bool| {
        let mut labels = 0;
        let mut sensitive = 0;
        let mut programs: BTreeSet<String> = BTreeSet::new();
        for (program, cones) in cones {
            assert!(!cones.is_empty(), "{program}: no labels");
            labels += cones.len();
            for c in cones.iter() {
                if f(c) {
                    sensitive += 1;
                    programs.insert(program.clone());
                }
            }
        }
        println!(
            "SUMMARY\t{which}\t{labels} labels\t{sensitive} sensitive\t{} of {} programs: {programs:?}",
            programs.len(),
            cones.len()
        );
        sensitive
    };
    let calm: Vec<(String, &Vec<LabelCone>)> = all.iter().map(|(p, c)| (p.clone(), c)).collect();
    let conservative: Vec<(String, &Vec<LabelCone>)> = all
        .iter()
        .zip(&old)
        .map(|((p, _), c)| (p.clone(), c))
        .collect();
    let old_total = count("conservative", &conservative, &|c| c.schedule_sensitive());
    let old_u1 = count("conservative, own cone (U1)", &conservative, &|c| {
        c.existence_sensitive()
    });
    let new_total = count("calm", &calm, &|c| c.schedule_sensitive());
    let new_u1 = count("calm, own cone (U1)", &calm, &|c| c.existence_sensitive());
    assert!(new_total <= old_total && new_u1 <= old_u1);
    let _ = find(&all[0].1, all[0].1[0].collection.display_name());
}
