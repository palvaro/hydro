//! Top-level protocol composition: wires consensus-zoo primitives together
//! into a primary/backup replication protocol with Paxos-backed view changes.
//!
//! The composition:
//!   1. View manager (Paxos via hydro_test) sequences view change proposals
//!   2. State transfer (discover primitive) reconciles seq on view change
//!   3. Slot assignment on the primary (resumes from reconciled seq)
//!   4. Star accumulate with dynamic quorum for replication
//!   5. Decide (consensus-zoo) joins confirmed slots with commands
//!   6. Ordered deliver (consensus-zoo) emits committed commands in order
//!   7. Application adapter applies commands through ReplicableService
//!   8. Read-only commands bypass replication (leader-local reads)
//!
//! The protocol is command-agnostic — it sequences opaque `Vec<u8>` payloads.
//! Only the application adapter deserializes and interprets commands.

use std::time::Duration;

use hydro_lang::live_collections::stream::{NoOrder, TotalOrder};
use hydro_lang::location::cluster::CLUSTER_SELF_ID;
use hydro_lang::location::MemberId;
use hydro_lang::prelude::*;
use hydro_test::cluster::paxos::{Acceptor, CorePaxos, Proposer};
use hydro_test::cluster::paxos_with_client::PaxosLike;
use serde::de::DeserializeOwned;
use serde::Serialize;
use stageleft::q;
use std::fmt::Debug;

use crate::messages::{TransparentReplica, View};
use crate::Router;

/// Output of the protocol: committed commands in order, ready for application.
pub struct ProtocolOutput<'a> {
    /// Committed `(seq, payload)` pairs delivered in slot order on the primary.
    pub committed_in_order: Stream<(usize, Vec<u8>), Cluster<'a, TransparentReplica>, Unbounded>,
    /// Replicated `(seq, payload)` pairs — fires on ALL replicas for every validated Replicate.
    /// Use this to maintain hot standby state on backups.
    pub replicated: Stream<(usize, Vec<u8>), Cluster<'a, TransparentReplica>, Unbounded, NoOrder>,
    /// Current view — use to determine which replica is primary.
    pub current_view: Singleton<View, Cluster<'a, TransparentReplica>, Unbounded>,
    /// Max replicated seq on each replica (for state transfer).
    pub max_replicated_seq: Optional<usize, Cluster<'a, TransparentReplica>, Unbounded>,
}

/// Top-level protocol composition using consensus-zoo primitives.
pub fn compose_protocol<'a>(
    replicas: &Cluster<'a, TransparentReplica>,
    proposers: &Cluster<'a, Proposer>,
    acceptors: &Cluster<'a, Acceptor>,
    client_payloads: Stream<Vec<u8>, Cluster<'a, TransparentReplica>, Unbounded>,
    external_view_proposals: Stream<View, Cluster<'a, TransparentReplica>, Unbounded, NoOrder>,
    config: crate::ReplicateConfig,
) -> ProtocolOutput<'a> {
    let initial_member_count = config.initial_members.len();

    // ═══════════════════════════════════════════════════════════════════════
    // Step 1: View manager — Paxos sequences view change proposals
    // ═══════════════════════════════════════════════════════════════════════
    let proposals_at_proposers = external_view_proposals
        .broadcast(proposers, TCP.fail_stop().bincode(), nondet!(/** view proposals to proposers */))
        .values();

    let a_checkpoint: Optional<usize, Cluster<'a, Acceptor>, Unbounded> =
        acceptors.singleton(q!(None::<usize>)).into_optional().into();

    let hydro_paxos_config = hydro_test::cluster::paxos::PaxosConfig {
        f: config.paxos_config.f,
        i_am_leader_send_timeout: config.paxos_config.i_am_leader_send_timeout,
        i_am_leader_check_timeout: config.paxos_config.i_am_leader_check_timeout,
        i_am_leader_check_timeout_delay_multiplier: config.paxos_config.i_am_leader_check_timeout_delay_multiplier,
    };

    let paxos = CorePaxos {
        proposers: proposers.clone(),
        acceptors: acceptors.clone(),
        paxos_config: hydro_paxos_config,
    };

    let committed_views_on_proposers = paxos.build(
        move |_new_leader_elected| {
            proposals_at_proposers.assume_ordering(nondet!(/** Paxos sequences regardless of arrival order */))
        },
        a_checkpoint,
        nondet!(/** Paxos leader election */),
        nondet!(/** Paxos commit ordering */),
    );

    let committed_views_on_replicas = committed_views_on_proposers
        .filter_map(q!(|(_slot, opt_view)| opt_view))
        .broadcast(replicas, TCP.fail_stop().bincode(), nondet!(/** committed views to replicas */))
        .values()
        .weaken_ordering::<NoOrder>();

    let current_view: Singleton<View, _, _> = committed_views_on_replicas
        .fold(
            q!(move || View {
                view_num: 0,
                members: (0..initial_member_count as u32).collect(),
            }),
            q!(|current: &mut View, new: View| {
                if new.view_num > current.view_num {
                    *current = new;
                }
            }, commutative = manual_proof!(/** max is commutative */)),
        )
        .into();

    // ═══════════════════════════════════════════════════════════════════════
    // Step 2: State transfer — forward ref for max_replicated_seq cycle
    // ═══════════════════════════════════════════════════════════════════════
    let (max_seq_complete, max_seq_ref) =
        replicas.forward_ref::<Optional<usize, _, Unbounded>>();

    let reconciled_seq = crate::primitives::discover::state_transfer(
        replicas, current_view.clone(), max_seq_ref,
    );

    // ═══════════════════════════════════════════════════════════════════════
    // Step 3: Slot assignment — primary assigns sequence numbers
    // ═══════════════════════════════════════════════════════════════════════
    let tick = replicas.tick();

    let view_in_tick = current_view.clone().snapshot(&tick, nondet!(/** stale view ok */));
    let is_primary = view_in_tick.clone()
        .filter(q!(move |v| CLUSTER_SELF_ID.get_raw_id() == v.primary()))
        .map(q!(|_| ()));

    // Only the primary assigns slots
    let batch = client_payloads.batch(&tick, nondet!(/** batch boundaries */));
    let primary_batch = batch.filter_if_some(is_primary.clone());

    // Assign contiguous sequence numbers, resuming from reconciled_seq after view change
    let base_seq = reconciled_seq.snapshot(&tick, nondet!(/** reconciled base */));

    let (next_slot_complete, next_slot) =
        tick.cycle_with_initial(tick.singleton(q!(0usize)));

    // If reconciled_seq is available, use it + 1 as base; otherwise use next_slot
    let effective_base = base_seq
        .map(q!(|s| s + 1))
        .unwrap_or(next_slot);

    let indexed = primary_batch
        .enumerate()
        .cross_singleton(effective_base.clone())
        .map(q!(|((i, payload), base)| (base + i, payload)));

    let count = indexed.clone().count();
    let updated_next = count.zip(effective_base).map(q!(|(n, b)| b + n));
    next_slot_complete.complete_next_tick(updated_next);

    // ═══════════════════════════════════════════════════════════════════════
    // Step 4: Broadcast + Quorum (accumulate pattern)
    // ═══════════════════════════════════════════════════════════════════════
    // Primary broadcasts replicates to all replicas.
    // Note: broadcast uses cluster membership which is deterministic once
    // the view stabilizes. The view_num check on receivers rejects stale messages.
    let replicates = indexed.clone()
        .cross_singleton(view_in_tick.clone())
        .map(q!(move |((seq, payload), v)| crate::messages::Replicate {
            view_num: v.view_num,
            seq,
            payload,
            sender: CLUSTER_SELF_ID.clone(),
        }))
        .all_ticks()
        .broadcast(replicas, TCP.fail_stop().bincode(), nondet!(/** cluster membership — deterministic once view stabilizes */))
        .values();

    // Each replica validates view_num and acks back to sender
    let valid = replicates
        .batch(&tick, nondet!(/** network delivery timing */))
        .cross_singleton(view_in_tick.clone())
        .filter(q!(|(r, v)| r.view_num == v.view_num))
        .map(q!(|(r, _)| r));

    // Track max replicated seq (for state transfer)
    let max_replicated_seq = valid.clone()
        .map(q!(|r| r.seq))
        .across_ticks(|s| s.fold(
            q!(|| None::<usize>),
            q!(|max, seq| { *max = Some(max.map_or(seq, |m: usize| m.max(seq))); },
               commutative = manual_proof!(/** max is commutative */)),
        ))
        .filter_map(q!(|opt| opt))
        .all_ticks()
        .max();

    // Complete the forward ref
    max_seq_complete.complete(max_replicated_seq.clone());

    // Replicated (seq, payload) on ALL replicas — for hot standby state
    let replicated = valid.clone()
        .map(q!(|r| (r.seq, r.payload)))
        .weaken_ordering::<NoOrder>()
        .all_ticks();

    let acks = valid.clone()
        .map(q!(move |r| (
            r.sender.clone(),
            (r.seq, Ok::<(), ()>(())),
        )))
        .all_ticks()
        .demux(replicas, TCP.fail_stop().bincode())
        .values();

    // Dynamic quorum: commit when all view members have acked
    let quorum_size: Singleton<usize, _, _> = current_view.clone()
        .map(q!(|v: View| v.members.len()))
        .into();

    let confirmed_slots = hydro_std::quorum::collect_dynamic_quorum(acks, quorum_size);

    // ═══════════════════════════════════════════════════════════════════════
    // Step 5: Decide — join confirmed slots with their payloads
    // ═══════════════════════════════════════════════════════════════════════
    let slot_payloads = indexed
        .weaken_ordering::<NoOrder>()
        .all_ticks();

    let committed = crate::primitives::decide::join_confirmed(
        slot_payloads,
        confirmed_slots,
    );

    // ═══════════════════════════════════════════════════════════════════════
    // Step 6: Ordered deliver — emit committed payloads in slot order
    // ═══════════════════════════════════════════════════════════════════════
    let committed_in_order = crate::primitives::ordered_deliver::deliver_in_order(
        committed,
        replicas,
    );

    ProtocolOutput {
        committed_in_order,
        replicated,
        current_view,
        max_replicated_seq,
    }
}

/// Router-driven failure detection using the silence_detector primitive.
///
/// Arms when a command is sent. Fires when no response arrives within timeout.
/// On timeout, pings all replicas and proposes a view from responders.
pub fn router_failure_detector<'a>(
    routers: &Cluster<'a, Router>,
    replicas: &Cluster<'a, TransparentReplica>,
    responses_at_router: Stream<Vec<u8>, Cluster<'a, Router>, Unbounded, NoOrder>,
    _current_view_at_replicas: Singleton<View, Cluster<'a, TransparentReplica>, Unbounded>,
    timeout_ms: u64,
    _initial_member_count: usize,
) -> Stream<View, Cluster<'a, TransparentReplica>, Unbounded, NoOrder> {
    // Use silence_detector primitive: fires when responses stop arriving
    let timeouts = crate::primitives::liveness::silence_detector(
        responses_at_router,
        routers,
        1000, // check every second
        timeout_ms,
    );

    // On timeout, ping all replicas to determine who's alive
    let pings_at_replicas = routers
        .source_interval(q!(Duration::from_millis(500)), nondet!(/** periodic ping */))
        .map(q!(move |_| CLUSTER_SELF_ID.get_raw_id()))
        .broadcast(replicas, TCP.fail_stop().bincode(), nondet!(/** ping */))
        .values();

    // Replicas reply with their ID
    let ping_responses: Stream<u32, Cluster<'a, Router>, Unbounded, NoOrder> = pings_at_replicas
        .map(q!(move |router_id: u32| (
            MemberId::<Router>::from_raw_id(router_id),
            CLUSTER_SELF_ID.get_raw_id(),
        )))
        .demux(routers, TCP.fail_stop().bincode())
        .values()
        .weaken_ordering::<NoOrder>();

    // Combine timeout + ping responses into view proposals via scan
    let timeout_events = timeouts.map(q!(|_| (0u8, 0u32)));
    let ping_events = ping_responses.map(q!(|id| (1u8, id)));

    let proposals = timeout_events
        .interleave(ping_events)
        .assume_ordering::<TotalOrder>(nondet!(/** FD scan order doesn't affect correctness */))
        .scan(
            q!(move || (false, std::collections::HashSet::<u32>::new(), 0u64)),
            q!(move |state: &mut (bool, std::collections::HashSet<u32>, u64), (tag, val): (u8, u32)| {
                match tag {
                    0 => {
                        // Timeout fired — start collecting pings
                        state.0 = true;
                        state.1.clear();
                        None
                    }
                    1 => {
                        if state.0 {
                            state.1.insert(val);
                            // After collecting for a bit, propose view from alive members
                            if state.1.len() > 0 {
                                state.0 = false;
                                let mut alive: Vec<u32> = state.1.drain().collect();
                                alive.sort();
                                state.2 += 1;
                                Some((state.2, alive))
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    }
                    _ => None,
                }
            }),
        )
        .map(q!(|(view_num, members)| View { view_num, members }));

    // Broadcast proposals to replicas
    proposals
        .broadcast(replicas, TCP.fail_stop().bincode(), nondet!(/** FD proposals */))
        .values()
        .weaken_ordering::<NoOrder>()
}

/// State for the router failure detector scan.
#[derive(Clone)]
pub struct FdState {
    pub armed_at: Option<std::time::Instant>,
    pub pinging: bool,
    pub ping_started: Option<std::time::Instant>,
    pub ping_replies: std::collections::HashSet<u32>,
    pub view_num: u64,
    pub members: Vec<u32>,
    pub pending_proposal: Option<(u64, Vec<u32>)>,
    pub warmed_up: bool,
}

/// Process-based router failure detection. The router arms a timer when a
/// command is sent, fires when no response arrives within timeout, pings
/// replicas to determine who's alive, and proposes a new view.
pub fn process_router_failure_detector<'a, R: 'a, C: Clone + Serialize + DeserializeOwned + 'static>(
    router: &Process<'a, R>,
    replicas: &Cluster<'a, TransparentReplica>,
    commands_at_router: Stream<C, Process<'a, R>, Unbounded, NoOrder>,
    responses_at_router: Stream<String, Process<'a, R>, Unbounded, NoOrder>,
    current_view_at_replicas: Singleton<View, Cluster<'a, TransparentReplica>, Unbounded>,
    timeout_ms: u64,
    initial_member_count: usize,
) -> Stream<View, Cluster<'a, TransparentReplica>, Unbounded, NoOrder> {
    let _view_at_router: Singleton<View, Process<'a, R>, Unbounded> =
        current_view_at_replicas
            .sample_eager(nondet!(/** send view to router */))
            .send(router, TCP.fail_stop().bincode())
            .values()
            .weaken_ordering::<NoOrder>()
            .fold(
                q!(move || View {
                    view_num: 0,
                    members: (0..initial_member_count as u32).collect(),
                }),
                q!(|current: &mut View, new: View| {
                    if new.view_num > current.view_num { *current = new; }
                }, commutative = manual_proof!(/** max */),
                   idempotent = manual_proof!(/** max */)),
            )
            .into();

    let fd_tick = router.tick();

    router
        .source_interval(q!(Duration::from_millis(1000)), nondet!(/** fd heartbeat */))
        .batch(&fd_tick, nondet!(/** fd heartbeat tick */))
        .for_each(q!(|_| {}));

    let pings_at_replicas = router
        .source_interval(q!(Duration::from_millis(500)), nondet!(/** ping */))
        .map(q!(|_| ()))
        .broadcast(replicas, TCP.fail_stop().bincode(), nondet!(/** ping broadcast */));

    let ping_responses: Stream<u32, Process<'a, R>, Unbounded, NoOrder> = pings_at_replicas
        .map(q!(move |_| CLUSTER_SELF_ID.get_raw_id()))
        .send(router, TCP.fail_stop().bincode())
        .values()
        .weaken_ordering::<NoOrder>();

    let resp_events = responses_at_router.map(q!(|_| (0u8, 0u64)));
    let ping_events = ping_responses.map(q!(|id: u32| (1u8, id as u64)));
    let heartbeat_events = router
        .source_interval(q!(Duration::from_millis(1000)), nondet!(/** scan heartbeat */))
        .map(q!(|_| (2u8, 0u64)));
    let cmd_events = commands_at_router.map(q!(|_| (3u8, 0u64)));

    let all_events = resp_events
        .interleave(ping_events)
        .interleave(heartbeat_events)
        .interleave(cmd_events);

    let proposals = all_events
        .batch(&fd_tick, nondet!(/** batch */))
        .weaken_ordering::<NoOrder>()
        .all_ticks()
        .assume_ordering::<TotalOrder>(nondet!(/** FD scan order doesn't affect correctness */))
        .scan(
            q!(move || crate::protocol::FdState {
                armed_at: None,
                pinging: false,
                ping_started: None,
                ping_replies: std::collections::HashSet::new(),
                view_num: 0,
                members: (0..initial_member_count as u32).collect(),
                pending_proposal: None,
                warmed_up: false,
            }),
            q!(move |fd: &mut crate::protocol::FdState, (tag, val): (u8, u64)| -> Option<Option<(u64, Vec<u32>)>> {
                match tag {
                    0 => {
                        if let Some((vn, m)) = fd.pending_proposal.take() {
                            fd.view_num = vn;
                            fd.members = m;
                        }
                        fd.armed_at = None;
                        fd.pinging = false;
                        fd.ping_started = None;
                        fd.ping_replies.clear();
                        fd.warmed_up = true;
                        Some(None)
                    }
                    1 => {
                        if fd.pinging { fd.ping_replies.insert(val as u32); }
                        Some(None)
                    }
                    2 => {
                        let now = std::time::Instant::now();
                        let timeout = std::time::Duration::from_millis(timeout_ms);
                        let ping_wait = std::time::Duration::from_millis(2000);

                        if fd.pinging {
                            if let Some(start) = fd.ping_started {
                                if now.duration_since(start) > ping_wait {
                                    fd.pinging = false;
                                    fd.ping_started = None;
                                    let alive: Vec<u32> = (0..initial_member_count as u32)
                                        .filter(|m| fd.ping_replies.contains(m))
                                        .collect();
                                    fd.ping_replies.clear();

                                    if alive.is_empty() {
                                        fd.armed_at = Some(now);
                                        return Some(None);
                                    }
                                    if let Some(ref p) = fd.pending_proposal {
                                        fd.armed_at = Some(now);
                                        return Some(Some(p.clone()));
                                    }
                                    if alive == fd.members {
                                        fd.armed_at = Some(now);
                                        return Some(None);
                                    }
                                    let new_view_num = fd.view_num + 1;
                                    println!("[ROUTER-FD] Proposing view change: {:?} -> {:?} (view_num={})", fd.members, alive, new_view_num);
                                    fd.pending_proposal = Some((new_view_num, alive.clone()));
                                    fd.armed_at = Some(now);
                                    return Some(Some((new_view_num, alive)));
                                }
                            }
                        } else if let Some(armed) = fd.armed_at {
                            if now.duration_since(armed) > timeout {
                                println!("[ROUTER-FD] Response timeout — pinging replicas...");
                                fd.armed_at = None;
                                fd.pinging = true;
                                fd.ping_started = Some(now);
                                fd.ping_replies.clear();
                            }
                        } else if fd.pending_proposal.is_some() {
                            // Unconfirmed proposal — re-arm to keep retrying
                            fd.armed_at = Some(now);
                        }
                        Some(None)
                    }
                    3 => {
                        if fd.warmed_up && fd.armed_at.is_none() && !fd.pinging {
                            fd.armed_at = Some(std::time::Instant::now());
                        }
                        Some(None)
                    }
                    _ => Some(None),
                }
            }),
        )
        .filter_map(q!(|x| x));

    let view_proposals = proposals
        .map(q!(|(view_num, members)| View { view_num, members }));

    view_proposals
        .broadcast(replicas, TCP.fail_stop().bincode(), nondet!(/** FD proposals to replicas */))
        .weaken_ordering::<NoOrder>()
}

/// Full replicated service: protocol + application adapter.
///
/// Serializes commands to `Vec<u8>` before entering the protocol,
/// deserializes and applies them through the `ReplicableService` after
/// the protocol delivers them in order.
///
/// Read-only commands bypass replication entirely (leader-local reads).
pub fn replicate_service<'a, S: crate::ReplicableService>(
    replicas: &Cluster<'a, TransparentReplica>,
    proposers: &Cluster<'a, Proposer>,
    acceptors: &Cluster<'a, Acceptor>,
    client_commands: Stream<S::Command, Cluster<'a, TransparentReplica>, Unbounded>,
    external_view_proposals: Stream<View, Cluster<'a, TransparentReplica>, Unbounded, NoOrder>,
    config: crate::ReplicateConfig,
) -> Stream<(usize, S::Response), Cluster<'a, TransparentReplica>, Unbounded, TotalOrder>
where
    S::Command: Clone + Serialize + DeserializeOwned + Debug + Send + 'static,
    S::Response: Clone + Serialize + DeserializeOwned + Debug + Send + 'static,
{
    // Split: read-only commands bypass replication (leader-local reads pattern)
    let mutating = client_commands.clone()
        .filter(q!(|cmd: &S::Command| !S::is_read_only(cmd)));
    let _read_only = client_commands
        .filter(q!(|cmd: &S::Command| S::is_read_only(cmd)));

    // Serialize mutating commands to opaque bytes
    let payloads = mutating
        .map(q!(|cmd: S::Command| bincode::serialize(&cmd).unwrap()));

    let output = compose_protocol(
        replicas, proposers, acceptors, payloads,
        external_view_proposals, config,
    );

    // Application adapter: deserialize and apply through the service.
    // committed_in_order is already TotalOrder from deliver_in_order — no batching needed.
    output.committed_in_order
        .scan(
            q!(|| (None::<S>, 0usize)),
            q!(|state: &mut (Option<S>, usize), (seq, payload): (usize, Vec<u8>)| {
                let service = state.0.get_or_insert_with(S::default);
                let cmd: S::Command = bincode::deserialize(&payload).unwrap();
                let resp = service.apply(cmd);
                let result = (seq, resp);
                state.1 = seq + 1;
                Some(result)
            }),
        )
}
