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
//!
//! The complete assume/guarantee contracts, bounded exhaustive audit map, and
//! manual linearizability/progress composition argument live in
//! `design_docs/2026-08_abd_compositional_verification.md`. End-to-end sim
//! tests are regression sentinels and independent bug-finding evidence; the
//! theorem itself cites the component contracts rather than sampled histories.

use std::collections::HashMap;
use std::hash::Hash;

use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
use hydro_lang::location::MemberId;
use hydro_lang::location::cluster::{CLUSTER_SELF_ID, EventualConsistency, NoConsistency};
use hydro_lang::location::member_id::TaglessMemberId;
use hydro_lang::prelude::*;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

pub use super::quorum::Ts;
use super::quorum::{covering_quorum, covering_quorum_retiring, quorum, quorum_retiring};

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
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
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

/// The replica register as a top-level max lattice fold. Keeping this exact
/// production fold behind a function lets the component tests exercise its
/// batching/order invariance without substituting a model implementation.
fn abd_register_state<'a, T, CL, R>(
    phase2: Stream<
        (MemberId<CL>, ToReplica<T>),
        Cluster<'a, R, EventualConsistency>,
        Unbounded,
        NoOrder,
        ExactlyOnce,
    >,
) -> Singleton<Option<(Ts, T)>, Cluster<'a, R, EventualConsistency>, Unbounded>
where
    T: Clone + Eq + Hash + Serialize + DeserializeOwned + 'a,
    CL: 'a,
    R: 'a,
{
    phase2
        .filter_map(q!(|(_requester, w)| match w {
            ToReplica::Write { ts, value, .. } => Some((ts, value)),
            _ => None,
        }))
        .fold(
            q!(|| None),
            q!(
                |acc: &mut Option<(Ts, _)>, (ts, v)| {
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
                )
            ),
        )
}

/// ABD's replica-local service kernel. Kept separate from the client
/// orchestration so its two local contracts can be checked directly:
///
/// 1. an ack for `ts` is emitted only after a register snapshot covers `ts`;
/// 2. queries report a snapshot of the same monotone register.
///
/// This is deliberately ABD-specific rather than a generic voting callback:
/// its snapshot/pending-state behavior is the Hydro realization of ABD's
/// replica rule, and hiding it behind generic policy would obscure the proof
/// boundary this function exists to expose.
fn abd_replica<'a, T, CL, R>(
    register: Singleton<Option<(Ts, T)>, Cluster<'a, R, EventualConsistency>, Unbounded>,
    queries: Stream<
        (MemberId<CL>, ToReplica<T>),
        Cluster<'a, R, EventualConsistency>,
        Unbounded,
        NoOrder,
        ExactlyOnce,
    >,
    phase2: Stream<
        (MemberId<CL>, ToReplica<T>),
        Cluster<'a, R, EventualConsistency>,
        Unbounded,
        NoOrder,
        ExactlyOnce,
    >,
) -> Stream<
    (MemberId<CL>, ToClient<T>),
    Cluster<'a, R, NoConsistency>,
    Unbounded,
    NoOrder,
    ExactlyOnce,
>
where
    T: Clone + Eq + Hash + Serialize + DeserializeOwned + 'a,
    CL: 'a,
    R: 'a,
{
    sliced! {
        let reg_now = use::snapshot(register, nondet!(
            /// Schedule-authored snapshot timing. Different snapshots may
            /// change whether an ack fires now and what a concurrent query
            /// returns; this choice is not locally erased. Safety instead
            /// holds for every choice: register timestamps only increase, an
            /// ack requires `observed >= requested`, and a query may observe
            /// any register state current during its execution.
        ));

        let new_writes = use::batch(phase2, nondet!(
            /// Schedule-authored admission of phase-2 writes to this tick.
            /// Different batches may delay an ack or let a higher write
            /// supersede a lower one first. The max-register is
            /// order-insensitive and unacknowledged requests persist in
            /// `waiting`, so no choice can acknowledge an uncovered timestamp
            /// or lose a pending acknowledgement.
        ));
        let query_batch = use::batch(queries, nondet!(
            /// Schedule-authored query processing time. A later batch may
            /// return a higher timestamp and thereby change the client's
            /// covering; this choice is observable, not locally resolved. It
            /// is legal because a query concurrent with writes may observe any
            /// state reached during its execution, and a read repairs the
            /// adopted pair to a quorum before returning.
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
                ToReplica::Query { rid } => Some((requester, ToClient::QueryResp { rid, reg })),
                _ => None,
            }));

        acks.chain(resps)
    }
}

/// The operation kind consumed by ABD's pure post-covering planner.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AbdOp<T> {
    Write(T),
    Read,
}

/// The protocol-specific action selected after phase 1. Public only because
/// staged code is expanded in the generated crate; it is not stable API.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AbdPlan<T> {
    Write { rid: u64, ts: Ts, value: T },
    ReadRepair { rid: u64, pair: (Ts, T) },
    EmptyRead { rid: u64 },
}

impl<T> AbdPlan<T> {
    /// Plan the phase after a covering. This is the pure rule used by the
    /// staged client dataflow and exhaustively enumerated in unit tests.
    pub fn from_covering(
        rid: u64,
        op: AbdOp<T>,
        covered: Option<(Ts, T)>,
        writer: TaglessMemberId,
    ) -> Self {
        match op {
            AbdOp::Write(value) => {
                let round = covered.map(|(ts, _)| ts.round).unwrap_or(0) + 1;
                AbdPlan::Write {
                    rid,
                    ts: Ts { round, writer },
                    value,
                }
            }
            AbdOp::Read => match covered {
                Some(pair) => AbdPlan::ReadRepair { rid, pair },
                None => AbdPlan::EmptyRead { rid },
            },
        }
    }
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
        .merge_unordered(writes.clone().map(q!(|(rid, _v)| ToReplica::Query { rid })))
        .broadcast_closed(replicas, TCP.fail_stop().bincode())
        .entries(); // (requester, Query) at each replica; EC minted by broadcast.

    // Phase-2 writes reach replicas the same way, but they close a cycle
    // (writes depend on phase-1 responses, which depend on the replica):
    // declare them as a forward_ref on the EC location, RB-style.
    let (phase2_handle, phase2_fwd) =
        queries
            .location()
            .forward_ref::<Stream<(MemberId<CL>, ToReplica<T>), _, Unbounded, NoOrder>>();

    // ---- The register: a top-level lattice fold, EC INFERRED --------------
    // All replicas fold the same EC write stream through max-by-Ts, so "all
    // replica registers converge" is derived by the compiler; the only
    // obligations are the combiner's, and they are honest lattice facts.
    // The explicit annotation makes the EC claim compiler-checked: if any
    // step of this pipeline failed to preserve/earn EC, this would not build.
    let register: Singleton<Option<(Ts, T)>, Cluster<'a, R, EventualConsistency>, Unbounded> =
        abd_register_state(phase2_fwd.clone());

    // ---- The replica: ack gate + query service ----------------------------
    // Acks must not outrun the register (a write is acked only once a
    // register snapshot shows ts_applied >= ts_written: applied or
    // superseded). Pending acks wait across ticks; the snapshot is monotone,
    // so any query answered at-or-after an ack reflects what was acked.
    let replica_out = abd_replica(register, queries, phase2_fwd);

    // Route each response to its requester; the arriving stream at each
    // client is keyed by the responding replica.
    let from_replicas = replica_out
        .into_keyed()
        .demux(writes.location(), TCP.fail_stop().bincode())
        .entries(); // (replica, ToClient) at each client member

    let query_resps = from_replicas
        .clone()
        .filter_map(q!(|(replica, msg)| match msg {
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
    let covered =
        covering_quorum(majority, query_resps).map(q!(|(rid, cov)| (rid, cov.into_aggregate())));

    // ---- Phase transitions, as rid-keyed joins ------------------------------
    // "Having covered, THEN send phase 2" — the join is the phase transition,
    // the rid is the continuation.

    // The pure planner is the protocol-specific seam after covering: writes
    // mint above the maximum, reads either repair the adopted pair or finish
    // empty. The rid-keyed join is the phase transition/continuation.
    let operations = writes
        .clone()
        .map(q!(|(rid, value)| (rid, AbdOp::Write(value))))
        .merge_unordered(reads.clone().map(q!(|rid| (rid, AbdOp::Read))));
    let planned = covered.join(operations).map(q!(move |(rid, (max, op))| {
        AbdPlan::from_covering(rid, op, max, CLUSTER_SELF_ID.clone().into_tagless())
    }));

    let write_stamped = planned.clone().filter_map(q!(|plan| match plan {
        AbdPlan::Write { rid, ts, value } => Some((rid, ts, value)),
        _ => None,
    }));

    let write_phase2 = write_stamped
        .clone()
        .map(q!(|(rid, ts, value)| ToReplica::Write { rid, ts, value }));

    let read_empty = planned.clone().filter_map(q!(|plan| match plan {
        AbdPlan::EmptyRead { rid } => Some((rid, None)),
        _ => None,
    }));

    let read_adopted = planned.filter_map(q!(|plan| match plan {
        AbdPlan::ReadRepair { rid, pair } => Some((rid, pair)),
        _ => None,
    }));

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

// ===========================================================================
// Keyed ABD: a key-value store is a *vector of independent registers*.
// ===========================================================================
//
// Linearizability is a *local* property (Herlihy & Wing 1990): if every
// object in a system is individually linearizable, the whole system is
// linearizable. A KV store whose keys are independent atomic registers is
// therefore end-to-end linearizable *by composition* — no cross-key
// coordination, no log, no consensus. This is the entire idea: run one ABD
// register per key.
//
// The generalization from [`abd_register`] is deliberately minimal — the
// single-cell version is the `K = ()` special case:
//
// - The replica register becomes `HashMap<K, (Ts, V)>` instead of
//   `Option<(Ts, V)>`, merged *per key* under the same max-by-`Ts` order.
//   It is still ONE top-level lattice fold over the EC write stream, so
//   "all replica maps converge" stays compiler-inferred with only the same
//   two honest combiner obligations (a per-key max is still a max).
// - Every phase carries the operation's key `K` alongside its `rid`. Phase 1
//   asks for the register cell *of that key*; the covering read aggregates
//   the max `(Ts, V)` for that cell exactly as before.
// - `rid` remains the continuation token. The covering mint keys on `rid`
//   (one op = one rid = one key), and the key rides along so phase 2 and the
//   result can be re-addressed to the right cell.
//
// Every contract from the single-register module docs carries over verbatim,
// now *per key*: one outstanding op per (client member) at a time, rids
// unique per client member across reads and writes, and read completion
// still write-backs (linearizable reads, not merely regular ones).

/// A client member's outputs from the keyed register (the KV store).
pub struct AbdKvOutputs<'a, CL, K, V> {
    /// Write completions: `(request_id, (key, timestamp applied at))`.
    pub write_done: Stream<(u64, (K, Ts)), Cluster<'a, CL>, Unbounded, NoOrder, ExactlyOnce>,
    /// Read results: `(request_id, (key, value))` — value is `None` iff the
    /// key has never been written.
    pub read_result:
        Stream<(u64, (K, Option<(Ts, V)>)), Cluster<'a, CL>, Unbounded, NoOrder, ExactlyOnce>,
}

/// Messages from replicas back to clients in the keyed register. Public only
/// because staged (`q!`) code is compiled outside this module in deploy mode.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ToClientKv<V> {
    QueryResp { rid: u64, reg: Option<(Ts, V)> },
    WriteAck { rid: u64 },
}

/// Internal request metadata. Public only because staged (`q!`) code is
/// compiled outside this module in deploy mode.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub enum KvOp<K, V> {
    Write { key: K, value: V },
    Read { key: K },
}

/// Internal phase-2 completion metadata. Public only for staged deploy code.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub enum KvCompletion<K, V> {
    Write { key: K, ts: Ts },
    Read { key: K, value: Option<(Ts, V)> },
}

/// A **keyed** multi-writer ABD register: an end-to-end linearizable
/// key-value store built from quorums alone (one register per key). `replicas`
/// hold the per-key state; every member of `clients` may read and write any
/// key. `majority` must exceed half the replica cluster size (`N / 2 + 1`).
///
/// `writes` carries `(request_id, (key, value))` and `reads` carries
/// `(request_id, key)`, per client member. The per-member contracts from
/// [`abd_register`] apply unchanged (see the module docs): request ids unique
/// per member across reads and writes, and at most one operation outstanding
/// per member at a time.
///
/// A *delete* is not a protocol primitive — it is a write of a caller-chosen
/// tombstone value (`V` carries the "absent" marker), so it participates in
/// the same per-key timestamp ordering as any other write.
pub fn abd_kv_register<'a, K, V, CL, R>(
    replicas: &Cluster<'a, R>,
    majority: usize,
    writes: Stream<(u64, (K, V)), Cluster<'a, CL>, Unbounded, TotalOrder, ExactlyOnce>,
    reads: Stream<(u64, K), Cluster<'a, CL>, Unbounded, TotalOrder, ExactlyOnce>,
) -> AbdKvOutputs<'a, CL, K, V>
where
    K: Clone + Eq + Hash + Serialize + DeserializeOwned + 'a,
    V: Clone + Eq + Hash + Serialize + DeserializeOwned + 'a,
    CL: 'a,
    R: 'a,
{
    // ---- Phase 1 request plumbing (client -> replica) ---------------------
    // Both operations begin identically: ask every replica for the cell of
    // the operation's key.
    let queries = reads
        .clone()
        .merge_unordered(writes.clone().map(q!(|(rid, (key, _v))| (rid, key))))
        .broadcast_closed(replicas, TCP.fail_stop().bincode().name("abd_kv_query"))
        .entries(); // (requester, (rid, key)) at each replica; EC minted by broadcast.

    // Phase-2 writes reach replicas the same way, closing a cycle: declare
    // them as a forward_ref on the EC location, exactly as `abd_register`.
    // The tuple is `(requester, (rid, key, ts, value))`.
    let (phase2_handle, phase2_fwd) = queries.location().forward_ref::<Stream<
        (MemberId<CL>, (u64, K, Ts, V)),
        Cluster<'a, R, EventualConsistency>,
        Unbounded,
        NoOrder,
    >>();

    // ---- The register: a top-level KEYED lattice fold, EC INFERRED --------
    // Identical to the single-register fold, but the accumulator is a
    // per-key map and the max-by-`Ts` merge is applied within each key. This
    // is still one lattice fold over the EC write stream, so "all replica
    // maps converge" remains compiler-derived; the combiner obligations are
    // the same honest lattice facts, now witnessed per key.
    let register: Singleton<HashMap<K, (Ts, V)>, Cluster<'a, R, EventualConsistency>, Unbounded> =
        phase2_fwd
            .clone()
            .map(q!(|(_requester, (_rid, key, ts, value))| (
                key,
                (ts, value)
            )))
            .fold(
                q!(|| HashMap::new()),
                q!(
                    |acc, (key, (ts, v))| {
                        let dominated = acc.get(&key).map(|(a, _)| *a < ts).unwrap_or(true);
                        if dominated {
                            acc.insert(key, (ts, v));
                        }
                    },
                    commutative = manual_proof!(
                        /** per-key max by the total Ts order: writers never tie
                        (writer id is in the timestamp) and one client member has
                        at most one outstanding write (module-doc contract), so
                        equal timestamps imply equal values within a key. Distinct
                        keys touch disjoint map entries and trivially commute. */
                    ),
                    idempotent = manual_proof!(
                        /** per-key max is idempotent: re-applying an element never
                        changes that key's maximum. */
                    )
                ),
            );

    // ---- The replica: ack gate + query service ----------------------------
    // The ack gate is unchanged in spirit; it now consults the register cell
    // *for the written key*. A write is acked once a snapshot shows that
    // key's applied Ts >= the written Ts (applied or superseded).
    let replica_out = sliced! {
        let reg_now = use::snapshot(register.clone(), nondet!(
            /// Gate/answer timing. Each key's cell is monotone in Ts, so a
            /// later snapshot only dominates an earlier one per key: acking
            /// against any snapshot that covers the write, and answering
            /// queries from any current-or-later snapshot, are both safe.
        ));

        let new_writes = use::batch(phase2_fwd, nondet!(
            /// Write-arrival timing: which writes are considered this tick.
            /// Pending (un-acked) writes persist, so batching only delays.
        ));
        let query_batch = use::batch(queries, nondet!(
            /// Query-arrival timing: a query answered later sees a larger cell,
            /// which reads tolerate (they adopt and write back).
        ));

        let mut waiting =
            use::state_null::<Stream<(MemberId<CL>, (u64, K, Ts)), _, Bounded, NoOrder>>();

          let candidates = waiting
              .chain(new_writes.map(q!(|(requester, (rid, key, ts, _value))| {
                  (requester, (rid, key, ts))
              })))
              .cross_singleton(reg_now.clone());

        let acks = candidates.clone().filter_map(q!(|((requester, (rid, key, ts)), reg)| {
            if reg.get(&key).map(|(applied, _)| *applied >= ts).unwrap_or(false) {
                Some((requester, ToClientKv::WriteAck { rid }))
            } else {
                None
            }
        }));

        waiting = candidates.filter_map(q!(|((requester, (rid, key, ts)), reg)| {
            if reg.get(&key).map(|(applied, _)| *applied >= ts).unwrap_or(false) {
                None
            } else {
                Some((requester, (rid, key, ts)))
            }
        }));

          let resps = query_batch
              .cross_singleton(reg_now)
              .map(q!(|((requester, (rid, key)), reg)| {
                  (requester, ToClientKv::QueryResp { rid, reg: reg.get(&key).cloned() })
              }));

        acks.chain(resps)
    };

    // Route each response to its requester; keyed by responding replica.
    let from_replicas = replica_out
        .into_keyed()
        .demux(
            writes.location(),
            TCP.fail_stop().bincode().name("abd_kv_response"),
        )
        .entries(); // (replica, ToClientKv) at each client member

    let query_resps = from_replicas
        .clone()
        .filter_map(q!(|(replica, msg)| match msg {
            ToClientKv::QueryResp { rid, reg } => Some((rid, (replica, reg))),
            _ => None,
        }));

    let write_acks = from_replicas.filter_map(q!(|(replica, msg)| match msg {
        ToClientKv::WriteAck { rid } => Some((rid, replica)),
        _ => None,
    }));

    // ---- The covering read (client side) ----------------------------------
    // Carry operation metadata through the bounded quorum accumulator. This
    // avoids retaining the full historical writes/reads streams in joins.
    let active_ops = writes
        .clone()
        .map(q!(|(rid, (key, value))| (rid, KvOp::Write { key, value })))
        .merge_unordered(
            reads
                .clone()
                .map(q!(|(rid, key)| (rid, KvOp::Read { key })))
                .weaken_ordering::<NoOrder>(),
        )
        .weaken_ordering::<NoOrder>();
    let covered =
        covering_quorum_retiring(majority, active_ops, query_resps)
            .map(q!(|(rid, (op, cov))| (rid, (op, cov.into_aggregate()))));

    // ---- Phase transitions -------------------------------------------------
    let write_stamped = covered
        .clone()
        .filter_map(q!(move |(rid, (op, max))| match op {
            KvOp::Write { key, value } => {
                let round = max.map(|(ts, _)| ts.round).unwrap_or(0) + 1;
                Some((
                    rid,
                    key,
                    Ts {
                        round,
                        writer: CLUSTER_SELF_ID.clone().into_tagless(),
                    },
                    value,
                ))
            }
            KvOp::Read { .. } => None,
        }));

    let write_phase2 = write_stamped
        .clone()
        .map(q!(|(rid, key, ts, value)| (rid, key, ts, value)));

    let read_covered = covered.filter_map(q!(|(rid, (op, max))| match op {
        KvOp::Read { key } => Some((rid, key, max)),
        KvOp::Write { .. } => None,
    }));

    let read_empty = read_covered
        .clone()
        .filter(q!(|(_rid, _key, max)| max.is_none()))
        .map(q!(|(rid, key, _max)| (rid, key)));

    let read_adopted =
        read_covered.filter_map(q!(|(rid, key, max)| max.map(|tv| (rid, (key, tv)))));

    let read_writeback = read_adopted
        .clone()
        .map(q!(|(rid, (key, (ts, v)))| (rid, key, ts, v)));

    // Phase 2 to the replicas — fresh writes and read-repairs alike — closing
    // the forward_ref cycle.
    let phase2 = write_phase2
        .merge_unordered(read_writeback)
        .broadcast_closed(replicas, TCP.fail_stop().bincode().name("abd_kv_phase2"))
        .entries();
    phase2_handle.complete(phase2);

    // ---- Completion: request-scoped quorum state ---------------------------
    // Phase-2 metadata is retired with the ack accumulator when quorum fires,
    // so completed requests leave no joins, unique sets, or tombstones behind.
    let phase2_active = write_stamped
        .map(q!(|(rid, key, ts, _value)| (
            rid,
            KvCompletion::Write { key, ts },
        )))
        .weaken_ordering::<NoOrder>()
        .merge_unordered(
            read_adopted
                .map(q!(|(rid, (key, value))| (
                    rid,
                    KvCompletion::Read {
                        key,
                        value: Some(value),
                    },
                )))
                .weaken_ordering::<NoOrder>(),
        );
    let certified = quorum_retiring(majority, phase2_active, write_acks);

    let write_done = certified
        .clone()
        .filter_map(q!(|(cert, completion)| match completion {
            KvCompletion::Write { key, ts } => Some((cert.into_fact(), (key, ts))),
            KvCompletion::Read { .. } => None,
        }))
        .weaken_ordering::<NoOrder>();

    let read_result = certified
        .filter_map(q!(|(cert, completion)| match completion {
            KvCompletion::Read { key, value } => Some((cert.into_fact(), (key, value))),
            KvCompletion::Write { .. } => None,
        }))
        .weaken_ordering::<NoOrder>()
        .merge_unordered(
            read_empty
                .map(q!(|(rid, key)| (rid, (key, None))))
                .weaken_ordering::<NoOrder>(),
        );

    AbdKvOutputs {
        write_done,
        read_result,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
    use hydro_lang::location::MemberId;
    use hydro_lang::location::cluster::EventualConsistency;
    use hydro_lang::prelude::*;

    use super::{
        AbdOp, AbdPlan, ToClient, ToReplica, Ts, abd_kv_register, abd_register, abd_register_state,
        abd_replica,
    };

    const N: usize = 3;
    const MAJORITY: usize = 2; // N/2 + 1
    /// Crash budget for the crash tests; MAJORITY replicas must survive.
    const F: usize = 1;

    /// Keyed composition: writes to independent keys remain isolated, while a
    /// later read of each key observes that key's completed write.
    #[test]
    fn abd_kv_independent_keys_are_linearizable_registers() {
        let mut flow = FlowBuilder::new();
        let replicas = flow.cluster::<()>();
        let clients = flow.cluster::<()>();

        let (w_send, writes) = clients.sim_input::<(u64, (u32, u32)), TotalOrder, ExactlyOnce>();
        let (r_send, reads) = clients.sim_input::<(u64, u32), TotalOrder, ExactlyOnce>();

        let outs = abd_kv_register(&replicas, MAJORITY, writes, reads);
        let done_recv = outs.write_done.sim_cluster_output();
        let read_recv = outs.read_result.sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&replicas, N)
            .with_cluster_size(&clients, 2)
            .unit_test_fuzz_iterations(256)
            .fuzz(async || {
                w_send.send(0, (1, (10, 100)));
                let _: Vec<(u64, (u32, Ts))> = done_recv.collect_n_sorted(0, 1).await;

                w_send.send(1, (1, (20, 200)));
                let _: Vec<(u64, (u32, Ts))> = done_recv.collect_n_sorted(1, 1).await;

                r_send.send(0, (2, 20));
                r_send.send(1, (2, 10));
                let a: Vec<(u64, (u32, Option<(Ts, u32)>))> =
                    read_recv.collect_n_sorted(0, 1).await;
                let b: Vec<(u64, (u32, Option<(Ts, u32)>))> =
                    read_recv.collect_n_sorted(1, 1).await;

                assert_eq!(a[0].1.0, 20);
                assert_eq!(a[0].1.1.as_ref().map(|(_, value)| *value), Some(200));
                assert_eq!(b[0].1.0, 10);
                assert_eq!(b[0].1.1.as_ref().map(|(_, value)| *value), Some(100));
            });
    }

    fn ts(round: u64) -> Ts {
        Ts {
            round,
            writer: MemberId::<()>::from_raw_id(0).into_tagless(),
        }
    }

    /// Finite exhaustive enumeration of the production post-covering planner:
    /// writes strictly dominate the covering maximum, reads preserve the
    /// adopted pair exactly, empty reads skip phase 2, and request ids survive.
    #[test]
    fn abd_planner_contract_finite_enumeration() {
        for rid in [0u64, 1] {
            for writer_raw in [0u32, 1] {
                let writer = MemberId::<()>::from_raw_id(writer_raw).into_tagless();
                for covered_round in [None, Some(0u64), Some(1), Some(2)] {
                    let covered = covered_round.map(|round| (ts(round), 99u32));
                    let plan = AbdPlan::from_covering(
                        rid,
                        AbdOp::Write(7u32),
                        covered.clone(),
                        writer.clone(),
                    );
                    match plan {
                        AbdPlan::Write {
                            rid: planned_rid,
                            ts: planned_ts,
                            value,
                        } => {
                            assert_eq!(planned_rid, rid);
                            assert_eq!(planned_ts.writer, writer);
                            assert_eq!(planned_ts.round, covered_round.unwrap_or(0) + 1);
                            assert!(
                                covered
                                    .as_ref()
                                    .map(|(maximum, _)| planned_ts > *maximum)
                                    .unwrap_or(true)
                            );
                            assert_eq!(value, 7);
                        }
                        other => panic!("write planned as {other:?}"),
                    }
                }

                assert_eq!(
                    AbdPlan::<u32>::from_covering(rid, AbdOp::Read, None, writer.clone()),
                    AbdPlan::EmptyRead { rid }
                );
                for round in [0u64, 1, 2] {
                    let pair = (ts(round), 99u32);
                    assert_eq!(
                        AbdPlan::from_covering(
                            rid,
                            AbdOp::Read,
                            Some(pair.clone()),
                            writer.clone(),
                        ),
                        AbdPlan::ReadRepair { rid, pair }
                    );
                }
            }
        }

        let writer0 = MemberId::<()>::from_raw_id(0).into_tagless();
        let writer1 = MemberId::<()>::from_raw_id(1).into_tagless();
        let AbdPlan::Write { ts: a, .. } =
            AbdPlan::from_covering(0, AbdOp::Write(1u32), None, writer0)
        else {
            unreachable!()
        };
        let AbdPlan::Write { ts: b, .. } =
            AbdPlan::from_covering(0, AbdOp::Write(1u32), None, writer1)
        else {
            unreachable!()
        };
        assert_ne!(a, b, "distinct writers must not tie");
    }

    /// Finite arithmetic audit of the proof's general intersection lemma.
    /// This enumerates every pair of subsets through N=7; the theorem still
    /// rests on the mathematical fact `2Q > N => intersection`, not this bound.
    #[test]
    fn majority_quorums_intersect_finite_audit() {
        for n in 1usize..=7 {
            let q = n / 2 + 1;
            let subsets: Vec<usize> = (0..(1usize << n))
                .filter(|mask| mask.count_ones() as usize >= q)
                .collect();
            for &a in &subsets {
                for &b in &subsets {
                    assert_ne!(a & b, 0, "disjoint quorums for N={n}, Q={q}: {a:b}, {b:b}");
                }
            }

            if n >= 2 {
                let broken_q = n / 2;
                let broken: Vec<usize> = (0..(1usize << n))
                    .filter(|mask| mask.count_ones() as usize >= broken_q)
                    .collect();
                assert!(
                    broken.iter().any(|a| broken.iter().any(|b| a & b == 0)),
                    "expected a disjoint witness for broken N={n}, Q={broken_q}"
                );
            }
        }
    }

    /// The exact production max fold and ack gate, isolated from network and
    /// quorum rounds: every batching/order schedule must converge to the
    /// highest timestamp, and a lower write superseded by it must still ack.
    #[test]
    fn abd_replica_max_merge_and_superseded_ack_exhaustive() {
        let mut flow = FlowBuilder::new();
        let replicas = flow.cluster::<()>();

        let (q_send, queries) =
            replicas.sim_input::<(MemberId<()>, ToReplica<u32>), NoOrder, ExactlyOnce>();
        let (w_send, writes) =
            replicas.sim_input::<(MemberId<()>, ToReplica<u32>), NoOrder, ExactlyOnce>();

        let writes = writes.assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(
            manual_proof!(/** Test harness: all replicas receive the same finite write bag. */),
        );
        let queries = queries.assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(
            manual_proof!(/** Test harness: all replicas receive the same finite query bag. */),
        );
        let register = abd_register_state(writes.clone());
        let replies = abd_replica(register, queries, writes).sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&replicas, 1)
            .exhaustive(async || {
                let requester = MemberId::from_raw_id(0);
                w_send.send_many_unordered([
                    (
                        0,
                        (
                            requester.clone(),
                            ToReplica::Write {
                                rid: 1,
                                ts: ts(1),
                                value: 10,
                            },
                        ),
                    ),
                    (
                        0,
                        (
                            requester,
                            ToReplica::Write {
                                rid: 2,
                                ts: ts(2),
                                value: 20,
                            },
                        ),
                    ),
                ]);

                let ack_replies: Vec<(MemberId<()>, ToClient<u32>)> =
                    replies.collect_n_sorted(0, 2).await;
                let acked: BTreeSet<u64> = ack_replies
                    .into_iter()
                    .filter_map(|(_member, msg)| match msg {
                        ToClient::WriteAck { rid } => Some(rid),
                        _ => None,
                    })
                    .collect();
                assert_eq!(acked, BTreeSet::from([1, 2]));

                q_send.send_many_unordered([(
                    0,
                    (MemberId::from_raw_id(0), ToReplica::Query { rid: 3 }),
                )]);
                let response: Vec<(MemberId<()>, ToClient<u32>)> =
                    replies.collect_n_sorted(0, 1).await;
                assert!(matches!(
                    &response[0].1,
                    ToClient::QueryResp {
                        rid: 3,
                        reg: Some((seen, 20)),
                    } if *seen == ts(2)
                ));
            });
    }

    /// Once an ack has been observed, a subsequently issued query must report
    /// a snapshot at least as new. This directly audits the gate property used
    /// by the composition proof's replica-persistence lemma.
    #[test]
    fn abd_replica_ack_then_query_never_regresses_exhaustive() {
        let mut flow = FlowBuilder::new();
        let replicas = flow.cluster::<()>();

        let (q_send, queries) =
            replicas.sim_input::<(MemberId<()>, ToReplica<u32>), NoOrder, ExactlyOnce>();
        let (w_send, writes) =
            replicas.sim_input::<(MemberId<()>, ToReplica<u32>), NoOrder, ExactlyOnce>();
        let writes = writes.assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(
            manual_proof!(/** Test harness: all replicas receive the same finite write bag. */),
        );
        let queries = queries.assert_has_consistency_of::<Cluster<'_, (), EventualConsistency>>(
            manual_proof!(/** Test harness: all replicas receive the same finite query bag. */),
        );
        let register = abd_register_state(writes.clone());
        let replies = abd_replica(register, queries, writes).sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&replicas, 1)
            .exhaustive(async || {
                let requester = MemberId::from_raw_id(0);
                w_send.send_many_unordered([(
                    0,
                    (
                        requester.clone(),
                        ToReplica::Write {
                            rid: 1,
                            ts: ts(1),
                            value: 10,
                        },
                    ),
                )]);
                let ack: Vec<(MemberId<()>, ToClient<u32>)> = replies.collect_n_sorted(0, 1).await;
                assert!(matches!(ack[0].1, ToClient::WriteAck { rid: 1 }));

                q_send.send_many_unordered([(0, (requester, ToReplica::Query { rid: 2 }))]);
                let response: Vec<(MemberId<()>, ToClient<u32>)> =
                    replies.collect_n_sorted(0, 1).await;
                match &response[0].1 {
                    ToClient::QueryResp {
                        rid: 2,
                        reg: Some((seen, value)),
                    } => {
                        assert!(seen >= &ts(1));
                        assert_eq!(*value, 10);
                    }
                    other => panic!("expected covered query response, got {other:?}"),
                }
            });
    }

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
                let got: Vec<(u64, Option<(Ts, u32)>)> = read_recv.collect_n_sorted(0, 1).await;
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
                let got: Vec<(u64, Option<(Ts, u32)>)> = read_recv.collect_n_sorted(0, 1).await;
                assert_eq!(
                    got[0].1.as_ref().map(|(_, v)| *v),
                    Some(20),
                    "read must return the latest completed write, despite the crash"
                );
            });
    }

    /// **RED: the one-outstanding-op contract is load-bearing.** A client
    /// that issues a second write before the first completes can have both
    /// phase-1 coverings observe the same max, minting the SAME timestamp
    /// for two different values — falsifying the max-merge fold's
    /// commutativity `manual_proof!` (equal timestamps no longer imply equal
    /// values). The search must witness the forgery: two completed writes
    /// with equal `Ts`. Fittingly, the machinery that finds it is the sim's
    /// standing distrust of commutativity claims — it permutes fold batches
    /// precisely because the proof exists, and the violation is what makes
    /// the proof a lie.
    #[test]
    fn abd_violating_one_outstanding_op_forges_timestamps() {
        let mut flow = FlowBuilder::new();
        let replicas = flow.cluster::<()>();
        let clients = flow.cluster::<()>();

        let (w_send, writes) = clients.sim_input::<(u64, u32), TotalOrder, ExactlyOnce>();
        let (_r_send, reads) = clients.sim_input::<u64, TotalOrder, ExactlyOnce>();

        let outs = abd_register(&replicas, MAJORITY, writes, reads);
        let done_recv = outs.write_done.sim_cluster_output();

        let mut saw_forged_timestamp = false;

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&replicas, N)
            .with_cluster_size(&clients, 1)
            .fuzz(async || {
                // CONTRACT VIOLATION: two writes in flight at once.
                w_send.send(0, (1, 10u32));
                w_send.send(0, (2, 20u32));

                let done: Vec<(u64, Ts)> = done_recv.collect_sorted(0).await;
                if done.len() == 2 && done[0].1 == done[1].1 {
                    saw_forged_timestamp = true;
                }
            });

        assert!(
            saw_forged_timestamp,
            "violating one-outstanding-op must let the search mint two completed writes \
             with the SAME timestamp (the uniqueness lemma's premise, broken)"
        );
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
