//! This implements the Maelstrom `lin-kv` workload, driven by
//! [`broadcast_transcript_consensus`](crate::cluster::broadcast_transcript_consensus)
//! for linearizable replication.
//!
//! See <https://github.com/jepsen-io/maelstrom/blob/main/doc/06-raft/01-key-value.md>
//! and `doc/workloads.md#workload-lin-kv` in the Maelstrom repo.
//!
//! Every client `read`/`write`/`cas` request is proposed to consensus. Once a
//! request commits, every member applies it — in the same committed slot
//! order — to an identical in-memory `HashMap` state machine (the same
//! pattern `kv_replica` uses). Because every member's state machine sees the
//! same committed sequence, all members converge to the same map, and reads
//! observe a linearizable history — exactly what Maelstrom's Jepsen-based
//! `lin-kv` checker (a Knossos `CASRegister` model) verifies externally.

use std::collections::HashMap;
use std::marker::PhantomData;

use hydro_lang::location::cluster::{CLUSTER_SELF_ID, ClusterIds};
use hydro_lang::location::dynamic::LocationId;
use hydro_lang::location::{Location, MemberId};
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

use crate::cluster::broadcast_transcript_consensus::{
    BroadcastConsensusConfig, broadcast_transcript_consensus,
};
use crate::cluster::raft::{self, RaftConfig};

/// A KV value, stored as canonical JSON text rather than `serde_json::Value`
/// directly.
///
/// `serde_json::Value`'s own `Deserialize` impl always calls
/// `deserialize_any` (a JSON value is dynamically typed — number, string,
/// object, ... — so the deserializer must be asked to accept whatever it
/// finds). `deserialize_any` is only supported by self-describing formats.
/// `broadcast_transcript_consensus` carries its wire format over `bincode`
/// (not self-describing), so ANY type containing a raw `serde_json::Value`
/// crashes every node with `DeserializeAnyNotSupported` the moment a
/// committed entry needs to come back off the transcript — confirmed by a
/// live run. Storing the value's canonical JSON *text* instead sidesteps
/// this entirely: `String`'s `Deserialize` impl just reads a length-prefixed
/// byte sequence, which every format (including `bincode`) supports.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct JsonValue(pub String);

impl JsonValue {
    /// Captures a `serde_json::Value` (from the wire-format request body) as
    /// its canonical JSON text.
    pub fn from_value(v: serde_json::Value) -> Self {
        JsonValue(v.to_string())
    }

    /// Recovers the `serde_json::Value` for building a response body. The
    /// text always round-trips: it was produced by `serde_json::Value`'s own
    /// `Display`/`to_string`, so re-parsing it cannot fail.
    pub fn to_value(&self) -> serde_json::Value {
        serde_json::from_str(&self.0).expect("JsonValue always holds valid JSON text")
    }
}

/// Type alias so the KV state machine's map type can be named inside a `q!`
/// staged closure without a turbofish (`HashMap::<i64, JsonValue>::new()`
/// does not parse correctly inside the `sliced!`/`use::state` macro syntax).
type KvStore = HashMap<i64, JsonValue>;

/// `{"type": "read", "msg_id": .., "key": ..}`
///
/// `key` is `i64`, not `String`: Maelstrom's real `lin-kv` client generator
/// sends integer keys (confirmed by a live run — the doc's own tutorial
/// example uses strings for pedagogical simplicity, but the actual generator
/// does not).
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct ReadOp {
    pub msg_id: usize,
    pub key: i64,
}

/// `{"type": "write", "msg_id": .., "key": .., "value": ..}`
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct WriteOp {
    pub msg_id: usize,
    pub key: i64,
    pub value: JsonValue,
}

/// `{"type": "cas", "msg_id": .., "key": .., "from": .., "to": ..}`
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct CasOp {
    pub msg_id: usize,
    pub key: i64,
    pub from: JsonValue,
    pub to: JsonValue,
}

/// The internal (non-wire) request representation. Deliberately a PLAIN enum
/// with no `#[serde(tag = ...)]` — serde's default *externally* tagged
/// representation is the only enum encoding compatible with `bincode` (which
/// `broadcast_transcript_consensus` uses for its wire format): bincode is not
/// self-describing and does not support `deserialize_any`, which internally-
/// (and untagged-) enum representations require.
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum Request {
    Read(ReadOp),
    Write(WriteOp),
    Cas(CasOp),
}

/// Wire-format op bodies: `value`/`from`/`to` are raw `serde_json::Value`,
/// matching Maelstrom's JSON exactly. Deserialized ONLY via `serde_json`
/// (self-describing, supports `deserialize_any`) — never sent through
/// consensus directly.
#[derive(Deserialize, Clone)]
pub struct WireWriteOp {
    pub msg_id: usize,
    pub key: i64,
    pub value: serde_json::Value,
}

#[derive(Deserialize, Clone)]
pub struct WireCasOp {
    pub msg_id: usize,
    pub key: i64,
    pub from: serde_json::Value,
    pub to: serde_json::Value,
}

/// Wire-format type matching Maelstrom's actual JSON body shape exactly
/// (`{"type": "read", ...}`, internally tagged, with raw `serde_json::Value`
/// payloads). Used ONLY to decode the initial client request via
/// `serde_json`. Immediately converted into [`Request`] (which encodes
/// values as JSON text via [`JsonValue`]) before entering consensus.
#[derive(Deserialize, Clone)]
#[serde(tag = "type")]
pub enum WireRequest {
    #[serde(alias = "read")]
    Read(ReadOp),
    #[serde(alias = "write")]
    Write(WireWriteOp),
    #[serde(alias = "cas")]
    Cas(WireCasOp),
}

impl From<WireRequest> for Request {
    fn from(w: WireRequest) -> Self {
        match w {
            WireRequest::Read(op) => Request::Read(op),
            WireRequest::Write(op) => Request::Write(WriteOp {
                msg_id: op.msg_id,
                key: op.key,
                value: JsonValue::from_value(op.value),
            }),
            WireRequest::Cas(op) => Request::Cas(CasOp {
                msg_id: op.msg_id,
                key: op.key,
                from: JsonValue::from_value(op.from),
                to: JsonValue::from_value(op.to),
            }),
        }
    }
}

/// A client-tagged KV operation submitted to consensus. Carrying the
/// originating `client_id` alongside the request lets ANY member construct
/// and send the response once the op commits: Maelstrom's network routes
/// messages by their `dest` field regardless of which physical node emits
/// them, so the member that originally received the request need not be the
/// one that replies.
#[derive(Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct KvOp {
    pub client_id: String,
    pub request: Request,
}

/// Drives the Maelstrom `lin-kv` workload via `broadcast_transcript_consensus`.
///
/// Client requests are tagged with their originating client id and proposed
/// to consensus (member 0 is the stable leader; other members' election
/// timers are effectively disabled). Once a request commits, EVERY member
/// applies it — in committed slot order — to its own KV state machine
/// (identical replication, exactly like `kv_replica`). Only member 0 emits
/// the response, avoiding duplicate replies to the same client.
pub fn lin_kv_server<'a, C: 'a, Net>(
    cluster: &Cluster<'a, C>,
    cluster_size: usize,
    input: KeyedStream<String, WireRequest, Cluster<'a, C>>,
    net: impl Fn() -> Net,
) -> KeyedStream<String, serde_json::Value, Cluster<'a, C>>
where
    Net: hydro_lang::networking::NetworkFor<
            crate::cluster::broadcast_transcript_consensus::TranscriptMsg<KvOp, C>,
            ConsistencyGuarantee = hydro_lang::location::cluster::EventualConsistency,
        >,
    hydro_lang::live_collections::stream::NoOrder: hydro_lang::live_collections::stream::MinOrder<
            Net::OrderingGuarantee,
            Min = hydro_lang::live_collections::stream::NoOrder,
        >,
{
    // The cluster membership list, resolved on each member at runtime. Used
    // to identify "the first member" in a deployment-agnostic way: Maelstrom
    // node ids are strings ("n0", "n1", ...), not the small integers
    // `MemberId::from_raw_id`/`get_raw_id()` assume, so we compare positions
    // in the membership list instead (the same technique
    // `broadcast_transcript_consensus` itself uses internally for
    // `member_index`).
    let LocationId::Cluster(cluster_key) = Location::id(cluster) else {
        unreachable!("lin_kv_server always runs on a cluster")
    };
    let cluster_members = ClusterIds {
        key: cluster_key,
        _phantom: PhantomData,
    };

    // Every member fires the SAME election timer interval — unlike the
    // benchmarks (which pin a single "member 0" as a fixed stable leader),
    // this lets Paxos's own ballot ordering elect a leader from genuinely
    // concurrent campaigns, exercising real leader election under the
    // Maelstrom workload (already proven safe by
    // `concurrent_elections_never_fork` / `at_most_one_leader_per_ballot`).
    // `source_interval` requires its quoted `Duration` expression to be
    // `Copy`, so this deliberately does NOT capture the (non-`Copy`)
    // `cluster_members` — no per-member branching is needed here.
    let election_timer_interrupts =
        cluster.source_interval(q!(std::time::Duration::from_millis(300)));

    let requests = input.entries().map(q!(|(client_id, request)| KvOp {
        client_id,
        request: request.into()
    }));

    let outputs = broadcast_transcript_consensus(
        cluster,
        requests,
        election_timer_interrupts,
        BroadcastConsensusConfig { cluster_size },
        net,
        nondet!(
            /// Maelstrom lin-kv workload: request arrival order and leader
            /// election timing are externally nondeterministic (driven by the
            /// Jepsen test harness). Linearizability of the resulting history —
            /// not any particular commit order — is exactly what Maelstrom's
            /// checker verifies independently of this annotation.
        ),
    );

    sliced! {
        // All `use::` declarations must come first: the `sliced!` macro's
        // parser stops recognizing `use::batch`/`use::state`/`use::snapshot`
        // syntax as soon as it hits the first non-`use::` statement, so any
        // regular processing (like the `.fold()` below) must come after all
        // of them.
        let committed_batch = use::batch(
            outputs.committed.end_atomic().weaken_consistency(),
            nondet!(
                /// Batching within a tick does not change apply order: the
                /// batch is sorted by `LogEntry.slot` before being applied, so
                /// the resulting KV state and responses are identical
                /// regardless of tick boundaries.
            )
        );
        let mut kv_state = use::state(|l| l.singleton(q!(KvStore::new())));

        // Accumulate this tick's committed batch into a Vec. The fold's own
        // accumulation order does not matter because the consumer below sorts
        // by `LogEntry.slot` before applying — so the *result* (KV state +
        // responses) is invariant to fold order, which is what commutativity
        // requires here.
        let committed_vec = committed_batch.fold(
            q!(|| Vec::new()),
            q!(
                |acc, entry| { acc.push(entry); },
                commutative = manual_proof!(
                    /** The consumer sorts this Vec by `LogEntry.slot` before
                    applying any entry, so the fold's accumulation order never
                    affects the resulting KV state or emitted responses —
                    only the post-sort application order does, which is fixed
                    by slot number regardless of how the batch was folded. */
                )
            ),
        );

        let tick = committed_vec.location().clone();
        let state_ref = kv_state.by_mut();
        let committed_vec_ref = committed_vec.by_ref();

        // Computed once per tick as a `Singleton` + `by_ref()` (the same
        // indirection `broadcast_transcript_consensus` itself uses for
        // `other_members`/`election_fired`/etc.), rather than capturing
        // `cluster_members` directly into the `FnMut` closure below: that
        // free-variable type is meant to be spliced inside a `q!(...)` call
        // directly, not moved into an ordinary Rust closure capture that gets
        // invoked on every tick.
        let is_first_member_singleton = tick.singleton(q!(
            cluster_members
                .iter()
                .next()
                .map(|id| MemberId::from_tagless(id.clone()))
                == Some(CLUSTER_SELF_ID.clone())
        ));
        let is_first_member_ref = is_first_member_singleton.by_ref();

        // `q!` staged closures cannot directly call an external free function
        // (stageleft needs to splice the logic in; only methods on captured
        // values or fully inline code work), so the KV state-machine
        // transition — normally factored as `apply_op` — is inlined here.
        tick.singleton(q!(())).into_stream().flat_map_ordered(q!(move |_| {
            // Apply every member's replica of the state machine identically
            // (in slot order), but only the first member (by membership-list
            // position — deployment-agnostic, unlike a raw numeric id) emits
            // responses. Maelstrom routes by `dest`, not by originating node,
            // so a single designated responder avoids duplicate replies while
            // every member still keeps a fully up-to-date,
            // independently-verifiable state machine (the same replication
            // discipline as `kv_replica`).
            let is_first_member = *is_first_member_ref;

            let mut entries = committed_vec_ref.clone();
            entries.sort_by_key(|e| e.slot);

            let mut out = Vec::new();
            for entry in entries {
                let resp = match &entry.message.request {
                    Request::Read(op) => match state_ref.get(&op.key) {
                        Some(v) => serde_json::json!({
                            "type": "read_ok",
                            "value": v.to_value(),
                            "in_reply_to": op.msg_id
                        }),
                        None => serde_json::json!({
                            "type": "error",
                            "code": 20,
                            "text": "key does not exist",
                            "in_reply_to": op.msg_id
                        }),
                    },
                    Request::Write(op) => {
                        state_ref.insert(op.key.clone(), op.value.clone());
                        serde_json::json!({ "type": "write_ok", "in_reply_to": op.msg_id })
                    }
                    Request::Cas(op) => match state_ref.get(&op.key) {
                        None => serde_json::json!({
                            "type": "error",
                            "code": 20,
                            "text": "key does not exist",
                            "in_reply_to": op.msg_id
                        }),
                        Some(cur) if cur == &op.from => {
                            state_ref.insert(op.key.clone(), op.to.clone());
                            serde_json::json!({ "type": "cas_ok", "in_reply_to": op.msg_id })
                        }
                        Some(cur) => serde_json::json!({
                            "type": "error",
                            "code": 22,
                            "text": format!("expected {:?}, had {:?}", op.from.0, cur.0),
                            "in_reply_to": op.msg_id
                        }),
                    },
                };
                if is_first_member {
                    out.push((entry.message.client_id.clone(), resp));
                }
            }
            out
        }))
    }
    .into_keyed()
}

/// The same Maelstrom `lin-kv` workload as [`lin_kv_server`], but backed by
/// `raft::raft` instead of `broadcast_transcript_consensus`. Exists purely as
/// an adversarial-testing baseline: running the identical Jepsen/Knossos
/// linearizability check, under the identical fault injection (partitions,
/// high concurrency, repeated randomized runs), against a well-established
/// consensus protocol lets a failure of ONE and not the other be attributed
/// to the protocol rather than to the shared test harness/wiring.
pub fn raft_lin_kv_server<'a, C: 'a, Net>(
    cluster: &Cluster<'a, C>,
    cluster_size: usize,
    input: KeyedStream<String, WireRequest, Cluster<'a, C>>,
    net: impl Fn() -> Net,
) -> KeyedStream<String, serde_json::Value, Cluster<'a, C>>
where
    Net: hydro_lang::networking::NetworkFor<raft::RaftRpc<KvOp, C>>,
    hydro_lang::live_collections::stream::NoOrder: hydro_lang::live_collections::stream::MinOrder<
            Net::OrderingGuarantee,
            Min = hydro_lang::live_collections::stream::NoOrder,
        >,
{
    let LocationId::Cluster(cluster_key) = Location::id(cluster) else {
        unreachable!("raft_lin_kv_server always runs on a cluster")
    };
    let cluster_members = ClusterIds {
        key: cluster_key,
        _phantom: PhantomData,
    };

    // Symmetric across all members (genuine concurrent elections), matching
    // `lin_kv_server`. Raft additionally needs a heartbeat timer: a live
    // leader's heartbeats suppress followers' election timers.
    let election_timer_interrupts =
        cluster.source_interval(q!(std::time::Duration::from_millis(300)));
    let heartbeat_timer_interrupts =
        cluster.source_interval(q!(std::time::Duration::from_millis(50)));

    let requests = input.entries().map(q!(|(client_id, request)| KvOp {
        client_id,
        request: request.into()
    }));

    let (committed, _redirected) = raft::raft(
        requests,
        election_timer_interrupts,
        heartbeat_timer_interrupts,
        RaftConfig { cluster_size },
        net,
        nondet!(
            /// Maelstrom lin-kv workload: request arrival order and leader
            /// election timing are externally nondeterministic. Linearizability
            /// of the resulting history is exactly what Maelstrom's checker
            /// verifies independently of this annotation.
        ),
    );

    sliced! {
        let committed_batch = use::batch(
            committed.end_atomic().weaken_consistency(),
            nondet!(
                /// Batching within a tick does not change apply order: the
                /// batch is sorted by `LogEntry.index` before being applied.
            )
        );
        let mut kv_state = use::state(|l| l.singleton(q!(KvStore::new())));

        let committed_vec = committed_batch.fold(
            q!(|| Vec::new()),
            q!(
                |acc, entry| { acc.push(entry); },
                commutative = manual_proof!(
                    /** The consumer sorts this Vec by `LogEntry.index` before
                    applying any entry, so the fold's accumulation order never
                    affects the resulting KV state or emitted responses. */
                )
            ),
        );

        let tick = committed_vec.location().clone();
        let state_ref = kv_state.by_mut();
        let committed_vec_ref = committed_vec.by_ref();

        let is_first_member_singleton = tick.singleton(q!(
            cluster_members
                .iter()
                .next()
                .map(|id| MemberId::from_tagless(id.clone()))
                == Some(CLUSTER_SELF_ID.clone())
        ));
        let is_first_member_ref = is_first_member_singleton.by_ref();

        tick.singleton(q!(())).into_stream().flat_map_ordered(q!(move |_| {
            let is_first_member = *is_first_member_ref;

            let mut entries = committed_vec_ref.clone();
            entries.sort_by_key(|e| e.index);

            let mut out = Vec::new();
            for entry in entries {
                let resp = match &entry.message.request {
                    Request::Read(op) => match state_ref.get(&op.key) {
                        Some(v) => serde_json::json!({
                            "type": "read_ok",
                            "value": v.to_value(),
                            "in_reply_to": op.msg_id
                        }),
                        None => serde_json::json!({
                            "type": "error",
                            "code": 20,
                            "text": "key does not exist",
                            "in_reply_to": op.msg_id
                        }),
                    },
                    Request::Write(op) => {
                        state_ref.insert(op.key.clone(), op.value.clone());
                        serde_json::json!({ "type": "write_ok", "in_reply_to": op.msg_id })
                    }
                    Request::Cas(op) => match state_ref.get(&op.key) {
                        None => serde_json::json!({
                            "type": "error",
                            "code": 20,
                            "text": "key does not exist",
                            "in_reply_to": op.msg_id
                        }),
                        Some(cur) if cur == &op.from => {
                            state_ref.insert(op.key.clone(), op.to.clone());
                            serde_json::json!({ "type": "cas_ok", "in_reply_to": op.msg_id })
                        }
                        Some(cur) => serde_json::json!({
                            "type": "error",
                            "code": 22,
                            "text": format!("expected {:?}, had {:?}", op.from.0, cur.0),
                            "in_reply_to": op.msg_id
                        }),
                    },
                };
                if is_first_member {
                    out.push((entry.message.client_id.clone(), resp));
                }
            }
            out
        }))
    }
    .into_keyed()
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::str::FromStr;

    use hydro_lang::deploy::maelstrom::deploy_maelstrom::{
        MaelstromClusterSpec, MaelstromDeployment,
    };
    use hydro_lang::deploy::maelstrom::maelstrom_bidi_clients;

    use super::*;

    /// Single-node lin-kv: no replication story to test, just the state
    /// machine + wire protocol. Mirrors the first run in Maelstrom's own Raft
    /// tutorial (`node-count 1`), which must pass trivially before multi-node
    /// linearizability is meaningful.
    #[tokio::test]
    #[cfg_attr(not(maelstrom_available), ignore)]
    async fn lin_kv_single_node_maelstrom() {
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<()>();

        let (input, output_handle) = maelstrom_bidi_clients(&cluster);
        output_handle.complete(lin_kv_server(&cluster, 1, input, || {
            TCP.fail_stop().bincode()
        }));

        let mut deployment = MaelstromDeployment::new("lin-kv")
            .maelstrom_path(PathBuf::from_str(&std::env::var("MAELSTROM_PATH").expect(
                "MAELSTROM_PATH env var not set, set it to the maelstrom executable path",
            ))
            .unwrap())
            .node_count(1)
            .time_limit(20)
            .rate(10)
            // lin-kv's per-key generator needs at least 2 concurrent worker
            // threads per key group; the default (`1n` = 1 worker for a
            // 1-node test) is too few and Maelstrom asserts on it before any
            // requests are even sent.
            .extra_args(["--concurrency", "2n"]);

        let _ = flow
            .with_cluster(&cluster, MaelstromClusterSpec)
            .deploy(&mut deployment);

        deployment.run().unwrap();
    }

    /// Multi-node lin-kv driven by `broadcast_transcript_consensus`: this is
    /// the real test — a naive "independent copy per node" KV store would
    /// fail Maelstrom's linearizability checker here (as the tutorial shows
    /// happens with an un-replicated store), but consensus-backed replication
    /// should pass.
    #[tokio::test]
    #[cfg_attr(not(maelstrom_available), ignore)]
    async fn lin_kv_3_node_maelstrom() {
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<()>();

        let (input, output_handle) = maelstrom_bidi_clients(&cluster);
        output_handle.complete(lin_kv_server(&cluster, 3, input, || {
            TCP.fail_stop().bincode()
        }));

        let mut deployment = MaelstromDeployment::new("lin-kv")
            .maelstrom_path(
                PathBuf::from_str(&std::env::var("MAELSTROM_PATH").expect(
                    "MAELSTROM_PATH env var not set, set it to the maelstrom executable path",
                ))
                .unwrap(),
            )
            .node_count(3)
            .time_limit(20)
            .rate(10)
            .extra_args(["--concurrency", "6n"]);

        let _ = flow
            .with_cluster(&cluster, MaelstromClusterSpec)
            .deploy(&mut deployment);

        deployment.run().unwrap();
    }

    /// Adversarial stress test: network partitions injected every ~5s, high
    /// concurrency (12 workers across 3 nodes), higher request rate, longer
    /// duration, and 3 independent randomized repetitions — a genuinely
    /// harder adversarial workload than the smoke tests above, run against
    /// `broadcast_transcript_consensus`.
    #[tokio::test]
    #[cfg_attr(not(maelstrom_available), ignore)]
    async fn lin_kv_3_node_partition_stress() {
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<()>();

        let (input, output_handle) = maelstrom_bidi_clients(&cluster);
        // `lossy_delayed_forever`, not `fail_stop`: Maelstrom's partition
        // nemesis drops/delays packets and later heals the connection, which
        // `fail_stop` (permanent-death-only) cannot express — the deployment
        // backend rejects that combination at compile-adjacent runtime.
        // `lossy_delayed_forever` still guarantees eventual delivery (no
        // permanent loss), which is exactly what the transcript's EC
        // ("eventually consistent") guarantee needs, so the module's EC-
        // inference argument holds unchanged under this policy.
        output_handle.complete(lin_kv_server(&cluster, 3, input, || {
            TCP.lossy_delayed_forever().bincode()
        }));

        let mut deployment = MaelstromDeployment::new("lin-kv")
            .maelstrom_path(
                PathBuf::from_str(&std::env::var("MAELSTROM_PATH").expect(
                    "MAELSTROM_PATH env var not set, set it to the maelstrom executable path",
                ))
                .unwrap(),
            )
            .node_count(3)
            .time_limit(45)
            .rate(30)
            .nemesis("partition")
            .extra_args(["--concurrency", "12n", "--nemesis-interval", "5"]);

        let _ = flow
            .with_cluster(&cluster, MaelstromClusterSpec)
            .deploy(&mut deployment);

        // 3 independent repetitions, driven from Rust rather than Maelstrom's
        // own `--test-count` flag (see `run_repeated`'s docs for why).
        deployment.run_repeated(3).unwrap();
    }

    /// Same adversarial config as [`lin_kv_3_node_partition_stress`], but
    /// against `raft::raft` instead. A true side-by-side baseline: identical
    /// fault injection, identical Jepsen/Knossos linearizability check,
    /// different protocol under the hood.
    #[tokio::test]
    #[cfg_attr(not(maelstrom_available), ignore)]
    async fn raft_lin_kv_3_node_partition_stress() {
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<()>();

        let (input, output_handle) = maelstrom_bidi_clients(&cluster);
        // Raft's own docs recommend `lossy_delayed_forever` for exactly this
        // scenario: lost AppendEntries are re-sent on the next heartbeat,
        // lost vote traffic is retried at the next election timeout.
        output_handle.complete(raft_lin_kv_server(&cluster, 3, input, || {
            TCP.lossy_delayed_forever().bincode()
        }));

        let mut deployment = MaelstromDeployment::new("lin-kv")
            .maelstrom_path(
                PathBuf::from_str(&std::env::var("MAELSTROM_PATH").expect(
                    "MAELSTROM_PATH env var not set, set it to the maelstrom executable path",
                ))
                .unwrap(),
            )
            .node_count(3)
            .time_limit(45)
            .rate(30)
            .nemesis("partition")
            .extra_args(["--concurrency", "12n", "--nemesis-interval", "5"]);

        let _ = flow
            .with_cluster(&cluster, MaelstromClusterSpec)
            .deploy(&mut deployment);

        // 3 independent repetitions, driven from Rust rather than Maelstrom's
        // own `--test-count` flag (see `run_repeated`'s docs for why).
        deployment.run_repeated(3).unwrap();
    }

    /// Leader/member-crash tolerance: Maelstrom's `kill` nemesis SIGKILLs a
    /// random node's process and restarts it from scratch (empty in-memory
    /// state), unlike `partition` which only drops/delays messages while the
    /// process stays alive. `lin_kv_server` designates member 0 as the sole
    /// responder to Maelstrom, and reads are proposed through consensus
    /// exactly like writes — so if member 0 is killed and restarts, it must
    /// re-observe the committed transcript before it can answer anything
    /// again. This exercises the leader-recovery path more aggressively than
    /// the partition test: a restarted member has no memory at all, unlike a
    /// merely-partitioned one.
    ///
    /// `fail_stop` is fine here (only `partition` requires `lossy`/
    /// `lossy_delayed_forever` — see the panic guard in `m2m_sink_source`):
    /// the fault under test is the OS-level process kill itself, not network
    /// loss.
    #[tokio::test]
    #[cfg_attr(not(maelstrom_available), ignore)]
    async fn lin_kv_3_node_kill_stress() {
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<()>();

        let (input, output_handle) = maelstrom_bidi_clients(&cluster);
        output_handle.complete(lin_kv_server(&cluster, 3, input, || {
            TCP.fail_stop().bincode()
        }));

        let mut deployment = MaelstromDeployment::new("lin-kv")
            .maelstrom_path(
                PathBuf::from_str(&std::env::var("MAELSTROM_PATH").expect(
                    "MAELSTROM_PATH env var not set, set it to the maelstrom executable path",
                ))
                .unwrap(),
            )
            .node_count(3)
            .time_limit(45)
            .rate(30)
            .nemesis("kill")
            .extra_args(["--concurrency", "12n", "--nemesis-interval", "5"]);

        let _ = flow
            .with_cluster(&cluster, MaelstromClusterSpec)
            .deploy(&mut deployment);

        deployment.run_repeated(3).unwrap();
    }

    /// Same `kill` nemesis config as [`lin_kv_3_node_kill_stress`], but
    /// against `raft::raft`. Raft explicitly retransmits missed entries to
    /// lagging followers via `nextIndex`/`matchIndex` regardless of whether
    /// an election happens, so a killed-and-restarted member 0 should catch
    /// back up even if the leader never changes — unlike
    /// `broadcast_transcript_consensus`, which (per the documented gap in
    /// spec task 15.4) only recovers a lagging member's state via a new
    /// leader's Paxos-recovery re-`Accept`s, not through any stable-leader
    /// retransmission path.
    #[tokio::test]
    #[cfg_attr(not(maelstrom_available), ignore)]
    async fn raft_lin_kv_3_node_kill_stress() {
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<()>();

        let (input, output_handle) = maelstrom_bidi_clients(&cluster);
        output_handle.complete(raft_lin_kv_server(&cluster, 3, input, || {
            TCP.fail_stop().bincode()
        }));

        let mut deployment = MaelstromDeployment::new("lin-kv")
            .maelstrom_path(
                PathBuf::from_str(&std::env::var("MAELSTROM_PATH").expect(
                    "MAELSTROM_PATH env var not set, set it to the maelstrom executable path",
                ))
                .unwrap(),
            )
            .node_count(3)
            .time_limit(45)
            .rate(30)
            .nemesis("kill")
            .extra_args(["--concurrency", "12n", "--nemesis-interval", "5"]);

        let _ = flow
            .with_cluster(&cluster, MaelstromClusterSpec)
            .deploy(&mut deployment);

        deployment.run_repeated(3).unwrap();
    }

    /// Targeted single-node fail-over test: unlike
    /// [`lin_kv_3_node_kill_stress`], this does NOT use the default `kill`
    /// nemesis (which randomly kills anywhere from one node to the entire
    /// cluster, confounding "does fail-over work" with "does the system
    /// recover once a majority-loss outage ends"). Requires a local patch to
    /// Maelstrom (`nemesis.clj`, restricting `:kill` to `:targets [:one]`) —
    /// no released or `main`-branch Maelstrom CLI flag exposes this, so this
    /// test only runs against a source build with that patch applied (set
    /// `MAELSTROM_PATH` to a wrapper invoking the patched checkout).
    ///
    /// A longer `--nemesis-interval` (15s vs. the stress tests' 5s) gives
    /// real quiet time between the single kill and the restart, so recovery
    /// (or its absence) is observable rather than drowned out by continuous
    /// churn.
    #[tokio::test]
    #[cfg_attr(not(maelstrom_available), ignore)]
    async fn lin_kv_3_node_single_kill_failover() {
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<()>();

        let (input, output_handle) = maelstrom_bidi_clients(&cluster);
        output_handle.complete(lin_kv_server(&cluster, 3, input, || {
            TCP.fail_stop().bincode()
        }));

        let mut deployment = MaelstromDeployment::new("lin-kv")
            .maelstrom_path(
                PathBuf::from_str(&std::env::var("MAELSTROM_PATH").expect(
                    "MAELSTROM_PATH env var not set, set it to the maelstrom executable path",
                ))
                .unwrap(),
            )
            .node_count(3)
            .time_limit(60)
            .rate(30)
            .nemesis("kill")
            .extra_args(["--concurrency", "12n", "--nemesis-interval", "15"]);

        let _ = flow
            .with_cluster(&cluster, MaelstromClusterSpec)
            .deploy(&mut deployment);

        deployment.run_repeated(3).unwrap();
    }

    /// Same targeted single-kill config as
    /// [`lin_kv_3_node_single_kill_failover`], but against `raft::raft`.
    #[tokio::test]
    #[cfg_attr(not(maelstrom_available), ignore)]
    async fn raft_lin_kv_3_node_single_kill_failover() {
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<()>();

        let (input, output_handle) = maelstrom_bidi_clients(&cluster);
        output_handle.complete(raft_lin_kv_server(&cluster, 3, input, || {
            TCP.fail_stop().bincode()
        }));

        let mut deployment = MaelstromDeployment::new("lin-kv")
            .maelstrom_path(
                PathBuf::from_str(&std::env::var("MAELSTROM_PATH").expect(
                    "MAELSTROM_PATH env var not set, set it to the maelstrom executable path",
                ))
                .unwrap(),
            )
            .node_count(3)
            .time_limit(60)
            .rate(30)
            .nemesis("kill")
            .extra_args(["--concurrency", "12n", "--nemesis-interval", "15"]);

        let _ = flow
            .with_cluster(&cluster, MaelstromClusterSpec)
            .deploy(&mut deployment);

        deployment.run_repeated(3).unwrap();
    }
}
