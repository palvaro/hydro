//! Rung 2 of the quorum→consensus ladder: the **ABD register** — a
//! multi-writer atomic register from quorums alone. No leader, no epochs, no
//! consensus.
//!
//! # Why this rung matters
//!
//! ABD (Attiya–Bar-Noy–Dolev) is the strongest thing quorums buy *without*
//! succession: a linearizable single register. It composes the two quorum
//! certificate patterns — `Durable` (a write quorum stored the fact) and the
//! covering read (a read quorum saw everything durable) — with **zero** epoch
//! machinery. Its portfolio payoff: total order on a single cell with
//! **progress at F = 1 and no leader**, the row that shows succession is the
//! cost of a *log*, not of TO itself.
//!
//! # The protocol
//!
//! Clients are a **cluster** (symmetric logic, one copy of the dataflow,
//! replicated per member by the language; requester identity is carried by
//! the channels, not by payload fields, so it cannot be forged). Replicas
//! hold `(Ts, T)` under max-merge by timestamp. Timestamps are
//! `(round, writer)` where `writer` is the client's own member id
//! (`CLUSTER_SELF_ID`), so distinct writers never tie.
//!
//! - **write(v)**: query all replicas; at a majority of responses, adopt
//!   `round = max_seen + 1` (the authored choice — the `nondet!` seam that
//!   `leader_merge` puts at the leader, ABD puts at each client); send
//!   `(ts, v)` to all replicas; the write completes at a majority of acks
//!   (a `Durable` certificate on the write).
//! - **read()**: query all replicas; at a majority, adopt the max `(ts, v)`
//!   seen; **write it back** to a majority (read-repair, through the same
//!   phase-2 path as writes) before returning it. The write-back is what
//!   makes reads linearizable: a read that returns v forces v onto a
//!   majority, so no later read can return anything older.
//!
//! Why any majority works: two majorities intersect, so a covering read's
//! max is at least the timestamp of every completed write — the intersection
//! argument, the one genuinely irreducible quorum fact
//! (`2026-08_quorum_certificates.md` §2).
//!
//! # What is typed, and what deliberately is not
//!
//! - **The replica register is EC, inferred.** It is a top-level lattice
//!   fold (max by total `Ts` order) over the EC-delivered write stream —
//!   `g_set_gossip`'s pattern with max instead of set-union — so "all
//!   replica registers converge" is compiler-derived, with only the fold's
//!   honest combiner obligations. The price: acks can no longer ride tick
//!   atomicity and must be **gated** on a register snapshot showing
//!   `ts_applied ≥ ts_written` (applied or superseded). The gate is sound
//!   because the fold is monotone and snapshots of a monotone singleton are
//!   monotone across ticks: any query answered at-or-after an ack reflects
//!   at least what was acked.
//! - **Client-side streams are indexical, not EC — correctly.** Each client
//!   member converses about its own request ids; those streams do not
//!   converge across members and carry no consistency claim. EC is the wrong
//!   property for quorum request/response traffic: broadcast-shaped
//!   protocols are EC-shaped, quorum protocols are not.
//! - **Linearizability is not typed at all.** The register's headline
//!   property is an ordering claim, relational across operations and
//!   real-time; nothing in the label lattice can say it. It is enforced by
//!   the protocol and attacked by the crash simulator only.
//!
//! # Honest ledger, remaining
//!
//! - **One outstanding operation per client member.** A client that issues a
//!   second write before the first completes can mint the same timestamp
//!   twice (both phase-1 reads see the same max), invalidating the max-merge
//!   tie argument. The dataflow does not enforce this classical ABD
//!   contract; callers must await completion. Tests obey. Request ids must
//!   be unique per client member across writes AND reads.
//! - The covering read is implemented inline (count + max in one fold,
//!   fired once per request); rung 3 decides what a general `Covering` mint
//!   looks like. Its `nondet!` is load-bearing and justified: *which*
//!   majority answers picks *which* covering read this is, and any majority
//!   dominates every completed write.

use std::hash::Hash;

use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
use hydro_lang::location::MemberId;
use hydro_lang::location::cluster::{CLUSTER_SELF_ID, EventualConsistency};
use hydro_lang::prelude::*;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::quorum::{covering_quorum, quorum};
pub use super::quorum::Ts;

/// Messages from clients to replicas. Requester identity rides on the channel
/// keying (cluster→cluster broadcasts arrive keyed by sender), not in the
/// payload — clients cannot claim to be someone else.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
enum ToReplica<T> {
    /// Phase 1 (both ops): "send me your current register".
    Query { rid: u64 },
    /// Phase 2 (both ops): "store this, then ack".
    Write { rid: u64, ts: Ts, value: T },
}

/// Messages from replicas back to clients (the replica's identity likewise
/// rides on the channel keying).
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
enum ToClient<T> {
    QueryResp { rid: u64, reg: Option<(Ts, T)> },
    WriteAck { rid: u64 },
}

/// A client member's outputs from the register.
pub struct AbdOutputs<'a, CL, T> {
    /// Write completions: `(request_id, timestamp the write was applied at)`.
    pub write_done: Stream<(u64, Ts), Cluster<'a, CL>, Unbounded, NoOrder, ExactlyOnce>,
    /// Read results: `(request_id, value)` — `None` iff the register has
    /// never been written.
    pub read_result:
        Stream<(u64, Option<(Ts, T)>), Cluster<'a, CL>, Unbounded, NoOrder, ExactlyOnce>,
}

/// The multi-writer ABD register. `replicas` hold the state; every member of
/// `clients` may read and write. `majority` must exceed half the replica
/// cluster size (callers: `N / 2 + 1`).
///
/// `writes` carries `(request_id, value)` and `reads` carries `request_id`,
/// per client member; request ids must be unique per member across both, and
/// each member must have at most one operation outstanding (see module docs).
pub fn abd_register<'a, T, CL, R>(
    replicas: &Cluster<'a, R>,
    majority: usize,
    writes: Stream<(u64, T), Cluster<'a, CL>, Unbounded, TotalOrder, ExactlyOnce>,
    reads: Stream<u64, Cluster<'a, CL>, Unbounded, TotalOrder, ExactlyOnce>,
) -> AbdOutputs<'a, CL, T>
where
    T: Clone + Eq + Hash + Serialize + DeserializeOwned + 'a,
    CL: 'a,
    R: 'a,
{
    // ---- Phase 1 request plumbing (client -> replica) ---------------------
    // Both operations begin identically: ask every replica for its register.
    let queries: Stream<(MemberId<CL>, ToReplica<T>), _, Unbounded, NoOrder, ExactlyOnce> = reads
        .clone()
        .map(q!(|rid| ToReplica::Query { rid }))
        .merge_unordered(
            writes
                .clone()
                .map(q!(|(rid, _v)| ToReplica::Query { rid })),
        )
        .broadcast_closed(replicas, TCP.fail_stop().bincode())
        .entries(); // (requester, Query) at each replica; EC minted by broadcast.

    // Phase-2 writes reach replicas the same way, but they close a cycle
    // (writes depend on phase-1 responses, which depend on the replica):
    // declare them as a forward_ref on the EC location, RB-style.
    let (phase2_handle, phase2_fwd) = queries
        .location()
        .forward_ref::<Stream<(MemberId<CL>, ToReplica<T>), _, Unbounded, NoOrder>>();

    // ---- The register: a top-level lattice fold, EC INFERRED --------------
    // All replicas fold the same EC write stream through max-by-Ts, so "all
    // replica registers converge" is derived by the compiler; the only
    // obligations are the combiner's, and they are honest lattice facts.
    // The explicit annotation makes the EC claim compiler-checked: if any
    // step of this pipeline failed to preserve/earn EC, this would not build.
    let register: Singleton<Option<(Ts, T)>, Cluster<'a, R, EventualConsistency>, Unbounded> = phase2_fwd
        .clone()
        .filter_map(q!(|(_requester, w)| match w {
            ToReplica::Write { ts, value, .. } => Some((ts, value)),
            _ => None,
        }))
        .fold(
            q!(|| None),
            q!(|acc: &mut Option<(Ts, _)>, (ts, v)| {
                if acc.as_ref().map(|(a, _)| *a < ts).unwrap_or(true) {
                    *acc = Some((ts, v));
                }
            },
            commutative = manual_proof!(
                /** max by the total Ts order is commutative: writers never tie
                (writer id is in the timestamp) and one client member has at
                most one outstanding write (module-doc contract), so equal
                timestamps imply equal values. */
            ),
            idempotent = manual_proof!(
                /** max is idempotent: re-applying an element never changes
                the maximum. */
            )),
        );

    // ---- The replica: ack gate + query service ----------------------------
    // Acks must not outrun the register (a write is acked only once a
    // register snapshot shows ts_applied >= ts_written: applied or
    // superseded). Pending acks wait across ticks; the snapshot is monotone,
    // so any query answered at-or-after an ack reflects what was acked.
    let replica_out = sliced! {
        let reg_now = use::snapshot(register.clone(), nondet!(
            /// Gate/answer timing. The register is monotone in Ts, so a later
            /// snapshot only ever dominates an earlier one: acking against
            /// any snapshot that covers the write, and answering queries from
            /// any current-or-later snapshot, are both safe.
        ));

        let new_writes = use::batch(phase2_fwd, nondet!(
            /// Write-arrival timing: which writes are considered this tick.
            /// Pending (un-acked) writes persist, so batching only delays.
        ));
        let query_batch = use::batch(queries, nondet!(
            /// Query-arrival timing: a query answered later sees a larger
            /// register, which reads tolerate (they adopt and write back).
        ));

        let mut waiting =
            use::state_null::<Stream<(MemberId<CL>, (u64, Ts)), _, Bounded, NoOrder>>();

        let candidates = waiting
            .chain(new_writes.filter_map(q!(|(requester, w)| match w {
                ToReplica::Write { rid, ts, .. } => Some((requester, (rid, ts))),
                _ => None,
            })))
            .cross_singleton(reg_now.clone());

        let acks = candidates.clone().filter_map(q!(|((requester, (rid, ts)), reg)| {
            if reg.map(|(applied, _)| applied >= ts).unwrap_or(false) {
                Some((requester, ToClient::WriteAck { rid }))
            } else {
                None
            }
        }));

        waiting = candidates.filter_map(q!(|((requester, (rid, ts)), reg)| {
            if reg.map(|(applied, _)| applied >= ts).unwrap_or(false) {
                None
            } else {
                Some((requester, (rid, ts)))
            }
        }));

        let resps = query_batch
            .cross_singleton(reg_now)
            .filter_map(q!(|((requester, qmsg), reg)| match qmsg {
                ToReplica::Query { rid } => {
                    Some((requester, ToClient::QueryResp { rid, reg }))
                }
                _ => None,
            }));

        acks.chain(resps)
    };

    // Route each response to its requester; the arriving stream at each
    // client is keyed by the responding replica.
    let from_replicas = replica_out
        .into_keyed()
        .demux(writes.location(), TCP.fail_stop().bincode())
        .entries(); // (replica, ToClient) at each client member

    let query_resps = from_replicas.clone().filter_map(q!(|(replica, msg)| match msg {
        ToClient::QueryResp { rid, reg } => Some((rid, (replica, reg))),
        _ => None,
    }));

    let write_acks = from_replicas.filter_map(q!(|(replica, msg)| match msg {
        ToClient::WriteAck { rid } => Some((rid, replica)),
        _ => None,
    }));

    // ---- The covering read (client side): the extracted mint --------------
    // At a majority of distinct responders per rid, adopt the max register
    // seen, exactly once per rid (`covering_quorum`, shared with synod).
    let covered = covering_quorum(majority, query_resps)
        .map(q!(|(rid, cov)| (rid, cov.into_aggregate())));

    // ---- Phase transitions, as rid-keyed joins ------------------------------
    // "Having covered, THEN send phase 2" — the join is the phase transition,
    // the rid is the continuation.

    // Writes: stamp a fresh timestamp above everything the covering saw.
    let write_stamped = covered
        .clone()
        .join(writes.map(q!(|(rid, v)| (rid, v))))
        .map(q!(move |(rid, (max, v))| {
            let round = max.map(|(ts, _)| ts.round).unwrap_or(0) + 1;
            (rid, Ts { round, writer: CLUSTER_SELF_ID.clone().into_tagless() }, v)
        }));

    let write_phase2 = write_stamped
        .clone()
        .map(q!(|(rid, ts, value)| ToReplica::Write { rid, ts, value }));

    // Reads: adopt the covered max. Empty register => done immediately
    // (nothing to repair). Otherwise write back what was adopted, at its
    // ORIGINAL timestamp, through the same phase-2 path.
    let read_covered = covered
        .join(reads.map(q!(|rid| (rid, ()))))
        .map(q!(|(rid, (max, ()))| (rid, max)));

    let read_empty = read_covered.clone().filter(q!(|(_rid, max)| max.is_none()));

    let read_adopted =
        read_covered.filter_map(q!(|(rid, max)| max.map(|tv| (rid, tv))));

    let read_writeback = read_adopted
        .clone()
        .map(q!(|(rid, (ts, v))| ToReplica::Write { rid, ts, value: v }));

    // Phase 2 to the replicas — fresh writes and read-repairs alike — closing
    // the forward_ref cycle.
    let phase2 = write_phase2
        .merge_unordered(read_writeback)
        .broadcast_closed(replicas, TCP.fail_stop().bincode())
        .entries();
    phase2_handle.complete(phase2);

    // ---- Completion: a Durable certificate on the rid ----------------------
    let certified = quorum(majority, write_acks).map(q!(|cert| cert.into_fact()));

    let write_done = certified
        .clone()
        .map(q!(|rid| (rid, ())))
        .join(write_stamped.map(q!(|(rid, ts, _v)| (rid, ts))))
        .map(q!(|(rid, ((), ts))| (rid, ts)))
        .weaken_ordering::<NoOrder>();

    let read_result = certified
        .map(q!(|rid| (rid, ())))
        .join(read_adopted)
        .map(q!(|(rid, ((), tv))| (rid, Some(tv))))
        .weaken_ordering::<NoOrder>()
        .merge_unordered(read_empty.weaken_ordering::<NoOrder>());

    AbdOutputs {
        write_done,
        read_result,
    }
}

#[cfg(test)]
mod tests {
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;

    use super::{Ts, abd_register};

    const N: usize = 3;
    const MAJORITY: usize = 2; // N/2 + 1
    /// Crash budget for the crash tests; MAJORITY replicas must survive.
    const F: usize = 1;

    /// Smoke: one client member, write then read.
    #[test]
    fn abd_write_then_read() {
        let mut flow = FlowBuilder::new();
        let replicas = flow.cluster::<()>();
        let clients = flow.cluster::<()>();

        let (w_send, writes) = clients.sim_input::<(u64, u32), TotalOrder, ExactlyOnce>();
        let (r_send, reads) = clients.sim_input::<u64, TotalOrder, ExactlyOnce>();

        let outs = abd_register(&replicas, MAJORITY, writes, reads);
        let done_recv = outs.write_done.sim_cluster_output();
        let read_recv = outs.read_result.sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&replicas, N)
            .with_cluster_size(&clients, 1)
            .unit_test_fuzz_iterations(1024)
            .fuzz(async || {
                w_send.send(0, (1, 10u32));
                let done: Vec<(u64, Ts)> = done_recv.collect_n_sorted(0, 1).await;
                assert_eq!(done[0].0, 1, "write rid 1 must complete");

                r_send.send(0, 2);
                let got: Vec<(u64, Option<(Ts, u32)>)> =
                    read_recv.collect_n_sorted(0, 1).await;
                assert_eq!(got[0].0, 2);
                assert_eq!(
                    got[0].1.as_ref().map(|(_, v)| *v),
                    Some(10),
                    "read must return the completed write"
                );
            });
    }

    /// Linearizability's heart, sequentially: ops by DIFFERENT client members
    /// in real-time order. Member 1's write begins after member 0's
    /// completes, so its covering read intersects member 0's write quorum,
    /// its timestamp dominates, and every subsequent read (from either
    /// member) returns member 1's value.
    #[test]
    fn abd_sequential_cross_client_ops_respect_real_time() {
        let mut flow = FlowBuilder::new();
        let replicas = flow.cluster::<()>();
        let clients = flow.cluster::<()>();

        let (w_send, writes) = clients.sim_input::<(u64, u32), TotalOrder, ExactlyOnce>();
        let (r_send, reads) = clients.sim_input::<u64, TotalOrder, ExactlyOnce>();

        let outs = abd_register(&replicas, MAJORITY, writes, reads);
        let done_recv = outs.write_done.sim_cluster_output();
        let read_recv = outs.read_result.sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&replicas, N)
            .with_cluster_size(&clients, 2)
            .unit_test_fuzz_iterations(1024)
            .fuzz(async || {
                w_send.send(0, (1, 10u32));
                let _: Vec<(u64, Ts)> = done_recv.collect_n_sorted(0, 1).await;

                w_send.send(1, (1, 20u32));
                let _: Vec<(u64, Ts)> = done_recv.collect_n_sorted(1, 1).await;

                r_send.send(0, 2);
                r_send.send(1, 2);
                let a: Vec<(u64, Option<(Ts, u32)>)> = read_recv.collect_n_sorted(0, 1).await;
                let b: Vec<(u64, Option<(Ts, u32)>)> = read_recv.collect_n_sorted(1, 1).await;
                assert_eq!(
                    a[0].1.as_ref().map(|(_, v)| *v),
                    Some(20),
                    "member 0's read must observe member 1's later write"
                );
                assert_eq!(
                    b[0].1.as_ref().map(|(_, v)| *v),
                    Some(20),
                    "member 1's read must observe its own write"
                );
            });
    }

    /// **The portfolio row.** One untargeted REPLICA crash (budget F = 1):
    /// in EVERY explored execution both writes and the read COMPLETE
    /// (progress at F = 1 — no leader, no dead state; contrast
    /// `member_leader_single_crash_can_block_progress`) and the read returns
    /// the latest completed write (safety). Progress is asserted by
    /// `collect_n_sorted` itself: a blocked register quiesces without output
    /// and fails the collect.
    #[test]
    fn abd_progress_and_reads_latest_under_replica_crash() {
        let mut flow = FlowBuilder::new();
        let replicas = flow.cluster::<()>();
        let clients = flow.cluster::<()>();

        let (w_send, writes) = clients.sim_input::<(u64, u32), TotalOrder, ExactlyOnce>();
        let (r_send, reads) = clients.sim_input::<u64, TotalOrder, ExactlyOnce>();

        let outs = abd_register(&replicas, MAJORITY, writes, reads);
        let done_recv = outs.write_done.sim_cluster_output();
        let read_recv = outs.read_result.sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&replicas, N)
            .with_cluster_size(&clients, 1)
            .with_crashable_cluster(&replicas, F)
            .fuzz(async || {
                w_send.send(0, (1, 10u32));
                let _: Vec<(u64, Ts)> = done_recv.collect_n_sorted(0, 1).await;

                w_send.send(0, (2, 20u32));
                let _: Vec<(u64, Ts)> = done_recv.collect_n_sorted(0, 1).await;

                r_send.send(0, 3);
                let got: Vec<(u64, Option<(Ts, u32)>)> =
                    read_recv.collect_n_sorted(0, 1).await;
                assert_eq!(
                    got[0].1.as_ref().map(|(_, v)| *v),
                    Some(20),
                    "read must return the latest completed write, despite the crash"
                );
            });
    }

    /// **The test the client-cluster design unlocks: an untargeted CLIENT
    /// crash.** A client may die mid-phase-2 — the classic incomplete write,
    /// stored on some replicas but never certified. Survivor-agnostic
    /// assertions at quiescence: every member's observed reads are
    /// ts-monotone (an adopted incomplete write may appear, but once seen it
    /// is repaired onto a majority and can never regress), and reads return
    /// only values that were actually written.
    #[test]
    fn abd_reads_monotone_under_client_crash() {
        let mut flow = FlowBuilder::new();
        let replicas = flow.cluster::<()>();
        let clients = flow.cluster::<()>();

        let (w_send, writes) = clients.sim_input::<(u64, u32), TotalOrder, ExactlyOnce>();
        let (r_send, reads) = clients.sim_input::<u64, TotalOrder, ExactlyOnce>();

        let outs = abd_register(&replicas, MAJORITY, writes, reads);
        let read_recv = outs.read_result.sim_cluster_output();
        let done_recv = outs.write_done.sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&replicas, N)
            .with_cluster_size(&clients, 2)
            .with_crashable_cluster(&clients, 1)
            .fuzz(async || {
                // Both members write (either may crash mid-phase-2), then
                // both issue two sequential reads.
                w_send.send(0, (1, 10u32));
                w_send.send(1, (1, 20u32));
                hydro_lang::sim::quiesce().await;
                for member in 0..2u32 {
                    r_send.send(member, 2);
                }
                hydro_lang::sim::quiesce().await;
                for member in 0..2u32 {
                    r_send.send(member, 3);
                }

                // Drain everything at quiescence; crashed members simply
                // produce less.
                let _dones: Vec<Vec<(u64, Ts)>> = {
                    let mut v = Vec::new();
                    for member in 0..2u32 {
                        v.push(done_recv.collect_sorted(member).await);
                    }
                    v
                };
                for member in 0..2u32 {
                    let results: Vec<(u64, Option<(Ts, u32)>)> =
                        read_recv.collect_sorted(member).await;
                    // Reads sorted by rid = issue order (rid 2 then 3).
                    let mut last_ts: Option<Ts> = None;
                    for (rid, res) in &results {
                        if let Some((ts, v)) = res {
                            assert!(
                                [10u32, 20u32].contains(v),
                                "member {member} rid {rid}: read invented value {v}"
                            );
                            assert!(
                                last_ts.as_ref().map(|l| l <= ts).unwrap_or(true),
                                "member {member}: reads regressed: {last_ts:?} then {ts:?}"
                            );
                            last_ts = Some(ts.clone());
                        }
                    }
                }
            });
    }
}
