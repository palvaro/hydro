//! The Maelstrom `lin-kv` workload served by the RAFT implementation in
//! [`crate::cluster::raft`], so that Jepsen's linearizability checker can be pointed at it.
//!
//! See <https://github.com/jepsen-io/maelstrom/blob/main/doc/workloads.md#workload-lin-kv>.
//!
//! Every client operation (`read`, `write`, `cas`) is a RAFT log entry: the node that
//! receives it from a Maelstrom client tags it with its own identity and hands it to the
//! RAFT server. Non-leaders forward the operation to the leader they know of (or fail it
//! with Maelstrom's definite error 11, *temporarily unavailable*, if they know of none, so
//! the client retries). Every member applies the committed log to a local key-value map in
//! log order; the member that originally received an operation replies to its client when
//! it applies the corresponding entry. Reads go through the log too, which is what makes
//! them linearizable.
//!
//! This exists to run the fault-injection experiment of
//! [`RaftConfig::election_restriction_ignores_term`] under Maelstrom's nemeses (see the
//! tests at the bottom), so the configuration and the timer periods are parameters.
//!
//! # Experiment log (Maelstrom v0.2.4, 3 and 5 nodes, `lin-kv`)
//!
//! Correct configuration: linearizable in every run, with and without the `partition`
//! nemesis. Injected bug: **not found** in 12 runs totalling 29 minutes of test time
//! (~1,900 leader elections) across `partition` (intervals 1–3 s), `--latency`
//! 20–150 ms (uniform / exponential), rates 3–100 ops/s, 3 and 5 nodes; neither the
//! linearizability checker nor the in-node oracle in [`apply_committed`] (which reports
//! a committed index being re-emitted) ever fired. The bug needs a deposed leader that
//! still holds un-replicated entries, whose log *index* is at least the new majority's
//! while the new leader has committed, and whose election timer fires before any
//! heartbeat of the new leader reaches it. Maelstrom's faults are whole-link partitions
//! (for 3 nodes: isolate one node, or an asymmetric ring in which nobody can be elected)
//! and delays; it cannot drop the two specific `AppendEntries` messages that the
//! simulator-based LDFI search (`hydro_test/src/cluster/raft_ldfi.rs`) finds in ~80 runs of a
//! few milliseconds each, and under partitions the isolated old leader's log is almost
//! always *shorter* than the majority's (its clients block on 1 s timeouts while the
//! majority keeps committing), so the index comparison alone already denies it the vote.

use std::collections::HashMap;
use std::time::Duration;

use hydro_lang::live_collections::stream::NoOrder;
use hydro_lang::location::MemberId;
use hydro_lang::location::cluster::CLUSTER_SELF_ID;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

use crate::cluster::raft::{LogEntry, RaftConfig, raft_server};

/// A `lin-kv` client request as received from Maelstrom (JSON). Keys and values are the
/// integers Maelstrom's generator uses.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type")]
pub enum Request {
    /// Read the current value of `key`.
    #[serde(alias = "read")]
    Read {
        /// The client's message id, echoed in `in_reply_to`.
        msg_id: u64,
        /// The key to read.
        key: i64,
    },
    /// Blindly set `key` to `value`.
    #[serde(alias = "write")]
    Write {
        /// The client's message id, echoed in `in_reply_to`.
        msg_id: u64,
        /// The key to write.
        key: i64,
        /// The value to write.
        value: i64,
    },
    /// Set `key` to `to` iff its current value is `from`.
    #[serde(alias = "cas")]
    Cas {
        /// The client's message id, echoed in `in_reply_to`.
        msg_id: u64,
        /// The key to compare-and-set.
        key: i64,
        /// The expected current value.
        from: i64,
        /// The new value.
        to: i64,
    },
}

/// [`Request`] re-encoded as an externally tagged enum, which (unlike the internally
/// tagged JSON form) bincode can carry over the intra-cluster channel.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Op {
    /// See [`Request::Read`].
    Read(u64, i64),
    /// See [`Request::Write`].
    Write(u64, i64, i64),
    /// See [`Request::Cas`].
    Cas(u64, i64, i64, i64),
}

impl From<Request> for Op {
    fn from(request: Request) -> Self {
        match request {
            Request::Read { msg_id, key } => Op::Read(msg_id, key),
            Request::Write { msg_id, key, value } => Op::Write(msg_id, key, value),
            Request::Cas {
                msg_id,
                key,
                from,
                to,
            } => Op::Cas(msg_id, key, from, to),
        }
    }
}

impl Op {
    /// The client's message id.
    pub fn msg_id(&self) -> u64 {
        match self {
            Op::Read(msg_id, ..) | Op::Write(msg_id, ..) | Op::Cas(msg_id, ..) => *msg_id,
        }
    }
}

/// A client operation as it travels through the RAFT log: the request plus enough
/// provenance for the member that received it to reply.
///
/// Trait impls avoid derive bounds on `C` (it only appears inside [`MemberId`], which
/// implements everything for any tag).
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub struct KvOp<C> {
    /// The member that received the request from the Maelstrom client and owes it a reply.
    pub origin: MemberId<C>,
    /// The Maelstrom client id (`"c3"`).
    pub client: String,
    /// How many times the operation has been forwarded between members (bounded to break
    /// forwarding loops caused by stale leader hints).
    pub hops: u8,
    /// The operation itself.
    pub request: Op,
}

impl<C> Clone for KvOp<C> {
    fn clone(&self) -> Self {
        KvOp {
            origin: self.origin.clone(),
            client: self.client.clone(),
            hops: self.hops,
            request: self.request.clone(),
        }
    }
}

impl<C> std::fmt::Debug for KvOp<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KvOp")
            .field("origin", &self.origin)
            .field("client", &self.client)
            .field("hops", &self.hops)
            .field("request", &self.request)
            .finish()
    }
}

/// Timer periods of the RAFT members.
#[derive(Clone, Copy, Debug)]
pub struct Timing {
    /// Each election timeout is drawn uniformly from `election_min_ms..election_max_ms`
    /// (RAFT's randomized-timeout tie-breaking).
    pub election_min_ms: u64,
    /// See `election_min_ms`.
    pub election_max_ms: u64,
    /// The leader's heartbeat period; must be much shorter than the election timeout.
    pub heartbeat_ms: u64,
}

/// How many times a request may be forwarded towards the leader before giving up.
#[doc(hidden)]
pub const MAX_HOPS: u8 = 4;

/// A Maelstrom `error` reply body.
#[doc(hidden)]
pub fn error_body(code: u64, text: &str, in_reply_to: u64) -> serde_json::Value {
    serde_json::json!({
        "type": "error",
        "code": code,
        "text": text,
        "in_reply_to": in_reply_to,
    })
}

/// The replicated state machine of one member: the key-value map plus the index of the
/// last applied entry, which detects a committed entry being overwritten (the committed
/// stream re-emits the index; impossible without the injected bug).
///
/// Public only because staged (`q!`) code must reference it by path; not stable API.
#[doc(hidden)]
#[derive(Default, Clone)]
pub struct KvState {
    pub map: HashMap<i64, i64>,
    pub last_applied: usize,
}

/// Applies one committed log entry to the local key-value map and, if this member
/// received the operation from its client, returns the reply to send.
///
/// Public only because staged (`q!`) code must reference it by path; not stable API.
#[doc(hidden)]
pub fn apply_committed<C>(
    state: &mut KvState,
    entry: LogEntry<KvOp<C>>,
    me: &MemberId<C>,
) -> Option<(String, serde_json::Value)> {
    if entry.index <= state.last_applied {
        // Oracle for the experiment (visible with Maelstrom's `--log-stderr`): a stale
        // leader overwrote an entry this member had already applied.
        eprintln!(
            "COMMITTED ENTRY OVERWRITTEN at index {} (term {}, last applied {}): {:?}",
            entry.index, entry.term_received, state.last_applied, entry.message
        );
    }
    state.last_applied = entry.index;
    let kv = &mut state.map;
    let op = entry.message;
    let body = match op.request {
        Op::Read(msg_id, key) => match kv.get(&key) {
            Some(value) => serde_json::json!({
                "type": "read_ok",
                "value": value,
                "in_reply_to": msg_id,
            }),
            None => error_body(20, "key does not exist", msg_id),
        },
        Op::Write(msg_id, key, value) => {
            kv.insert(key, value);
            serde_json::json!({ "type": "write_ok", "in_reply_to": msg_id })
        }
        Op::Cas(msg_id, key, from, to) => match kv.get_mut(&key) {
            None => error_body(20, "key does not exist", msg_id),
            Some(current) if *current != from => {
                error_body(22, &format!("expected {from} but found {current}"), msg_id)
            }
            Some(current) => {
                *current = to;
                serde_json::json!({ "type": "cas_ok", "in_reply_to": msg_id })
            }
        },
    };
    (op.origin == *me).then_some((op.client, body))
}

/// A `lin-kv` server replicated with RAFT; see the module docs.
///
/// `input` is the stream of Maelstrom client requests keyed by client id (from
/// `maelstrom_bidi_clients`); the result is the stream of replies, keyed the same way.
pub fn lin_kv_raft_server<'a, C: 'a>(
    cluster: &Cluster<'a, C>,
    input: KeyedStream<String, Request, Cluster<'a, C>>,
    config: RaftConfig,
    timing: Timing,
) -> KeyedStream<String, serde_json::Value, Cluster<'a, C>, Unbounded, NoOrder> {
    let Timing {
        election_min_ms,
        election_max_ms,
        heartbeat_ms,
    } = timing;

    let heartbeat_timer = cluster.source_interval(q!(Duration::from_millis(heartbeat_ms)));
    let election_timer = cluster.source_stream(q!(Box::pin(futures::stream::unfold(
        (),
        move |()| async move {
            let timeout = rand::random_range(election_min_ms..election_max_ms);
            tokio::time::sleep(Duration::from_millis(timeout)).await;
            Some(((), ()))
        }
    ))));

    // Operations received from this member's clients.
    let local_ops = input.entries().map(q!(move |(client, request)| KvOp {
        origin: CLUSTER_SELF_ID.clone(),
        client,
        hops: 0,
        request: Op::from(request),
    }));

    // Operations forwarded by other members that believed we were the leader.
    let (forwarded_handle, forwarded) =
        cluster.forward_ref::<Stream<KvOp<C>, Cluster<'a, C>, Unbounded, NoOrder>>();

    let outputs = raft_server(
        cluster,
        local_ops.merge_unordered(forwarded),
        election_timer,
        heartbeat_timer,
        config,
        TCP.lossy_delayed_forever().bincode(),
        nondet!(
            /// Which member leads and how concurrent operations are ordered in the log
            /// is inherently non-deterministic; the linearizability checker validates
            /// that whichever order was picked is consistent with the replies.
        ),
    );

    // Diagnostics (visible with Maelstrom's `--log-stderr`).
    outputs
        .leader_views
        .for_each(q!(|view| eprintln!("view: {view:?}")));

    // Not-the-leader operations: forward to the known leader, or fail them so the
    // client retries. Forwarded operations that reach a member without a leader hint are
    // dropped (the client times out, which is a legitimate indefinite outcome).
    let redirected = outputs.redirected;
    let to_leader = redirected.clone().filter_map(q!(|(op, hint)| {
        match hint {
            Some(leader) if op.hops < crate::maelstrom::lin_kv_raft::MAX_HOPS => Some((
                leader,
                KvOp {
                    hops: op.hops + 1,
                    ..op
                },
            )),
            _ => None,
        }
    }));
    forwarded_handle.complete(
        to_leader
            .into_keyed()
            .demux(cluster, TCP.lossy_delayed_forever().bincode())
            .values(),
    );
    let unavailable = redirected.filter_map(q!(move |(op, hint)| {
        if (hint.is_none() || op.hops >= crate::maelstrom::lin_kv_raft::MAX_HOPS)
            && op.origin == CLUSTER_SELF_ID
        {
            Some((
                op.client,
                crate::maelstrom::lin_kv_raft::error_body(
                    11,
                    "no leader known",
                    op.request.msg_id(),
                ),
            ))
        } else {
            None
        }
    }));

    // The replicated state machine: apply committed entries in log order, replying to
    // the operations this member received.
    let committed = outputs.committed.end_atomic().weaken_consistency();
    let replies = sliced! {
        let batch = use::batch(committed, nondet!(
            /// Batching only groups consecutive committed entries; they are applied in
            /// log order either way.
        ));
        let mut kv = use::state(|l| l.singleton(q!(crate::maelstrom::lin_kv_raft::KvState::default())));
        let kv_ref = kv.by_mut();
        batch.filter_map(q!(move |entry| {
            crate::maelstrom::lin_kv_raft::apply_committed(&mut *kv_ref, entry, &CLUSTER_SELF_ID)
        }))
    };

    replies.merge_unordered(unavailable).into_keyed()
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

    /// Runs the `lin-kv` workload under Maelstrom. Knobs (environment variables, so the
    /// same test binary serves the control and the injected-bug experiment):
    ///
    /// * `RAFT_BUG=1`: inject [`RaftConfig::election_restriction_ignores_term`].
    /// * `MAELSTROM_NODES`: cluster size (default 3).
    /// * `MAELSTROM_NEMESIS`: comma-separated nemeses (`partition`, `pause`, `kill`, ...).
    /// * `MAELSTROM_TIME_LIMIT`, `MAELSTROM_RATE`, `MAELSTROM_EXTRA_ARGS` (space-separated).
    fn run(bug: bool) -> Result<(), std::io::Error> {
        let env_u64 = |name: &str, default: u64| {
            std::env::var(name)
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(default)
        };

        let nodes = env_u64("MAELSTROM_NODES", 3) as usize;

        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<()>();

        let (input, output_handle) = maelstrom_bidi_clients(&cluster);
        let replies = lin_kv_raft_server(
            &cluster,
            input,
            RaftConfig {
                cluster_size: nodes,
                election_restriction_ignores_term: bug,
            },
            Timing {
                election_min_ms: env_u64("RAFT_ELECTION_MIN_MS", 300),
                election_max_ms: env_u64("RAFT_ELECTION_MAX_MS", 600),
                heartbeat_ms: env_u64("RAFT_HEARTBEAT_MS", 50),
            },
        );
        output_handle.complete(replies.assume_ordering(nondet!(/** test */)));

        let mut deployment = MaelstromDeployment::new("lin-kv")
            .maelstrom_path(
                PathBuf::from_str(&std::env::var("MAELSTROM_PATH").expect(
                    "MAELSTROM_PATH env var not set, set it to the maelstrom executable path",
                ))
                .unwrap(),
            )
            .node_count(nodes)
            .time_limit(env_u64("MAELSTROM_TIME_LIMIT", 30))
            .rate(env_u64("MAELSTROM_RATE", 20))
            // Maelstrom's `lin-kv` generator needs at least 6 concurrent clients.
            .extra_args([
                "--concurrency".to_owned(),
                std::env::var("MAELSTROM_CONCURRENCY").unwrap_or_else(|_| "2n".to_owned()),
            ]);
        if let Ok(nemesis) = std::env::var("MAELSTROM_NEMESIS") {
            deployment = deployment.nemesis(nemesis);
        }
        if let Ok(extra) = std::env::var("MAELSTROM_EXTRA_ARGS") {
            deployment = deployment.extra_args(extra.split_whitespace().map(str::to_owned));
        }

        let _ = flow
            .with_cluster(&cluster, MaelstromClusterSpec)
            .deploy(&mut deployment);

        deployment.run()
    }

    /// Control: the correct protocol must pass the linearizability checker.
    #[tokio::test]
    #[cfg_attr(
        not(maelstrom_available),
        ignore = "requires the Maelstrom binary (set the MAELSTROM_PATH env var)"
    )]
    async fn lin_kv_raft_maelstrom() {
        run(std::env::var("RAFT_BUG").is_ok_and(|v| v == "1")).unwrap();
    }
}
