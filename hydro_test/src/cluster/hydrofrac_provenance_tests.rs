//! The hydrofrac provenance rewrite on this crate's EC corpus programs
//! (docs/design/hydrofrac-lite.md §7): each compiles with provenance, runs failure-free over
//! sampled schedules, and every lineage it reports is well-formed. The opaque counts are printed
//! (`--nocapture`). The hydro_std programs are covered in
//! `hydro_std::ec_inference_demos::hydrofrac_provenance_tests`.

use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
use hydro_lang::prelude::*;

use super::reliable_broadcast::{Broadcaster, Member, Payload, broadcast};
use super::two_pc_ctp::{Coordinator, Participant, Txn, two_pc};

#[cfg(stageleft_runtime)]
const MEMBERS: u32 = 3;

#[cfg(stageleft_runtime)]
fn reliable_broadcast(echo: bool, name: &str) {
    let mut flow = FlowBuilder::new();
    let broadcaster = flow.process::<Broadcaster>();
    let members = flow.cluster::<Member>();
    let (source_send, source) = broadcaster.sim_input::<Payload, TotalOrder, ExactlyOnce>();
    let out = broadcast(
        &members,
        source,
        echo,
        nondet!(/** test */),
        nondet!(/** test */),
    )
    .sim_cluster_output();
    let stats = hydrofrac::provenance::check::run_checked(
        name,
        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&members, MEMBERS as usize),
        8,
        async || {
            source_send.send(Payload(42));
            for member in 0..MEMBERS {
                let got: Vec<Payload> = out.collect_n_sorted(member, 1).await;
                assert_eq!(got, vec![Payload(42)], "member {member}");
            }
        },
    );
    assert_eq!(stats.opaque_outputs, 0, "{name}: {stats:?}");
}

#[test]
fn provenance_plain_broadcast() {
    reliable_broadcast(false, "reliable_broadcast::broadcast (plain)");
}

#[test]
fn provenance_echo_broadcast() {
    reliable_broadcast(true, "reliable_broadcast::broadcast (echo)");
}

#[cfg(stageleft_runtime)]
fn two_phase_commit(ctp: bool, name: &str) {
    const PARTICIPANTS: u32 = 2;
    let mut flow = FlowBuilder::new();
    let coordinator = flow.process::<Coordinator>();
    let participants = flow.cluster::<Participant>();
    let (txn_send, txns) = coordinator.sim_input::<Txn, TotalOrder, ExactlyOnce>();
    let (timeout_send, timeouts) = participants.sim_input::<(), TotalOrder, ExactlyOnce>();
    let committed = two_pc(
        &coordinator,
        &participants,
        PARTICIPANTS as usize,
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
    hydrofrac::provenance::check::run_checked(
        name,
        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&participants, PARTICIPANTS as usize),
        16,
        async || {
            txn_send.send(Txn(1));
            timeout_send.send_many((0..PARTICIPANTS).map(|p| (p, ())));
            for p in 0..PARTICIPANTS {
                let got: Vec<Txn> = committed.collect_n_sorted(p, 1).await;
                assert_eq!(got, vec![Txn(1)], "participant {p}");
            }
        },
    );
}

#[test]
fn provenance_two_pc() {
    two_phase_commit(false, "two_pc_ctp::two_pc (no cooperative termination)");
}

#[test]
fn provenance_two_pc_with_cooperative_termination() {
    two_phase_commit(true, "two_pc_ctp::two_pc (cooperative termination)");
}
