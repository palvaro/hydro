//! Benchmark comparison for Raft and Paxos-EC consensus protocols using the
//! `bench_client` infrastructure from `hydro_std`.
//!
//! Both benchmarks share the same structure:
//! 1. A `BenchClient` cluster generates virtual client requests
//! 2. Requests are routed to the consensus protocol's replica cluster (leader)
//! 3. Committed entries are routed back to the originating virtual client
//! 4. Latencies are computed and aggregated for throughput/latency reporting

use hydro_lang::live_collections::stream::NoOrder;
use hydro_lang::location::cluster::CLUSTER_SELF_ID;
use hydro_lang::location::MemberId;
use hydro_lang::prelude::*;
use hydro_std::bench_client::{
    aggregate_bench_results, bench_client, compute_throughput_latency, pretty_print_bench_results,
};

use super::broadcast_transcript_consensus::{self, BroadcastConsensusConfig};
use super::kv_replica::{self, KvPayload};
use super::paxos_bench::inc_i32_workload_generator;
use super::raft::{self, RaftConfig, Replica};

/// Cluster tag for benchmark client nodes.
pub struct BenchClient;

/// Process tag for the benchmark aggregator node.
pub struct BenchAggregator;

/// Benchmarks the Raft consensus protocol using `bench_client`.
///
/// Requests from virtual clients are routed to replica member 0 (assumed leader
/// after the first election). Committed entries are routed back to the originating
/// client by preserving the virtual_client_id in the payload.
#[cfg(feature = "tokio")]
pub fn raft_bench<'a>(
    clients: &Cluster<'a, BenchClient>,
    num_clients_per_node: Singleton<usize, Cluster<'a, BenchClient>, Bounded>,
    client_aggregator: &Process<'a, BenchAggregator>,
    replicas: &Cluster<'a, Replica>,
    cluster_size: usize,
    client_interval_millis: u64,
    aggregate_interval_millis: u64,
) {
    // Election timer: only member 0 fires quickly to guarantee it wins leadership.
    // Other members have very long timeouts so they never campaign.
    let election_timer_interrupts =
        replicas.source_interval(q!(std::time::Duration::from_millis(
            if CLUSTER_SELF_ID.get_raw_id() == 0 {
                200
            } else {
                60_000
            }
        )));

    // Heartbeat timer: only the leader (member 0) sends heartbeats.
    // NOTE: this interval directly paces raft's commit latency — entries
    // replicate on heartbeats — so it is the dominant throughput knob in this
    // closed-loop benchmark. Set aggressively (1ms) so raft is not artificially
    // throttled when compared against broadcast-transcript (which has no
    // heartbeat and reacts every tick).
    let heartbeat_timer_interrupts =
        replicas.source_interval(q!(std::time::Duration::from_millis(
            if CLUSTER_SELF_ID.get_raw_id() == 0 {
                1
            } else {
                60_000
            }
        )));

    let latencies = bench_client(
        clients,
        num_clients_per_node,
        inc_i32_workload_generator,
        |input| {
            // input: KeyedStream<u32, i32, Cluster<BenchClient>>
            // We need to send (virtual_client_id, payload) to the Raft leader (member 0),
            // then route committed entries back.

            // Convert to entries and tag with the originating client member ID so we can
            // route back after commitment.
            let to_leader = input
                .entries()
                .map(q!(move |(virtual_id, payload)| {
                    let leader: MemberId<Replica> = MemberId::from_raw_id(0);
                    (leader, (CLUSTER_SELF_ID.clone(), virtual_id, payload))
                }))
                .into_keyed()
                .demux(replicas, TCP.fail_stop().bincode());

            // On the replica cluster: extract the payload for Raft.
            // The request stream carries (MemberId<BenchClient>, u32, i32).
            // Raft only sees the full tuple as its T.
            let requests_on_replicas = to_leader.values();

            // Run Raft - committed log carries LogEntry<(MemberId<BenchClient>, u32, i32)>
            let (committed, _redirected) = raft::raft(
                requests_on_replicas,
                election_timer_interrupts,
                heartbeat_timer_interrupts,
                RaftConfig { cluster_size },
                || TCP.fail_stop().bincode(),
                nondet!(
                    /// Which member leads and request interleaving is non-deterministic;
                    /// the benchmark only measures throughput/latency, not correctness.
                ),
            );

            // Extract committed entries and route back to the originating client.
            // committed: Stream<LogEntry<(MemberId<BenchClient>, u32, i32)>,
            //            Atomic<Cluster<Replica, EventualConsistency>>, Unbounded, TotalOrder>
            //
            // IMPORTANT: Only member 0 (the leader) routes responses back to clients.
            // All members receive committed entries via broadcast, but if all 3 demux
            // back to clients, each request gets 3 responses which breaks bench_client's
            // closed-loop feedback.
            let committed_payloads = committed
                .end_atomic()
                .weaken_consistency()
                .filter(q!(move |_| CLUSTER_SELF_ID.get_raw_id() == 0))
                .map(q!(|entry| {
                    let (client_id, virtual_id, payload) = entry.message;
                    (client_id, (virtual_id, payload))
                }))
                .into_keyed()
                .demux(clients, TCP.fail_stop().bincode());

            // committed_payloads: KeyedStream<MemberId<Replica>, (u32, i32), Cluster<BenchClient>>
            // Use .values() to drop the replica sender key, then into_keyed on (u32, i32)
            committed_payloads.values().into_keyed()
        },
    )
    .entries()
    .map(q!(|(_virtual_client_id, (_output, latency))| latency));

    // Create throughput/latency graphs
    let bench_results = compute_throughput_latency(
        clients,
        latencies,
        client_interval_millis,
        nondet!(/** bench measurement window */),
    );
    let aggregate_results =
        aggregate_bench_results(bench_results, client_aggregator, aggregate_interval_millis);
    pretty_print_bench_results(aggregate_results);
}

/// Benchmarks the quorum ladder's multi-decree consensus
/// ([`multi_paxos_live`](hydro_std::ec_inference_demos::multi_paxos_live::multi_paxos_live))
/// using `bench_client`.
///
/// Mirrors [`raft_bench`] exactly so the two are directly comparable: same
/// client workload, same routing (requests to member 0, the pinned leader —
/// only member 0's election timer fires fast, so it self-elects once and
/// then runs phase-2-only appends), same cluster size, colocated roles.
/// Protocol-intrinsic differences: no heartbeat timer at all (elections are
/// stall-triggered, so a progressing leader never re-elects), and learning
/// continues asynchronously as an O(n²) echo among learners (like
/// broadcast-transcript, unlike Raft's O(n)). Responses are routed from member
/// 0's leader-local `chosen` certificate, matching Raft's leader-local majority
/// commit point instead of charging Paxos for replica dissemination. A
/// `unique()` dedup still ensures exactly one response per request because the
/// wrapper's redo queue may legitimately place a value at multiple slots.
#[cfg(feature = "tokio")]
pub fn multi_paxos_bench<'a>(
    clients: &Cluster<'a, BenchClient>,
    num_clients_per_node: Singleton<usize, Cluster<'a, BenchClient>, Bounded>,
    client_aggregator: &Process<'a, BenchAggregator>,
    replicas: &Cluster<'a, Replica>,
    cluster_size: usize,
    client_interval_millis: u64,
    aggregate_interval_millis: u64,
) {
    use hydro_std::ec_inference_demos::multi_paxos_live::multi_paxos_live;

    // Election timer: only member 0 fires. A two-second period is long enough
    // that a saturated but progressing leader is not mistaken for a stalled
    // one by the wrapper's timeout-to-timeout progress check.
    let election_timeouts = replicas.source_interval(q!(std::time::Duration::from_millis(
        if CLUSTER_SELF_ID.get_raw_id() == 0 {
            2_000
        } else {
            60_000
        }
    )));

    let latencies = bench_client(
        clients,
        num_clients_per_node,
        inc_i32_workload_generator,
        |input| {
            // Tag requests with the originating client member's raw id (the
            // wrapper's redo queue needs `V: Ord`, which `MemberId` does not
            // provide) and route everything to the pinned leader.
            let to_leader = input
                .entries()
                .map(q!(move |(virtual_id, payload)| {
                    let leader: MemberId<Replica> = MemberId::from_raw_id(0);
                    (leader, (CLUSTER_SELF_ID.get_raw_id(), virtual_id, payload))
                }))
                .into_keyed()
                .demux(replicas, TCP.fail_stop().bincode());

            let requests_on_replicas = to_leader.values();

            let outs = multi_paxos_live(
                replicas,
                replicas,
                cluster_size / 2 + 1,
                cluster_size,
                election_timeouts,
                requests_on_replicas,
            );

            // Match Raft's completion semantics: respond from member 0 as
            // soon as its local majority certificate marks the slot chosen.
            // The learner EC broadcast/echo remains live in `outs.learned`,
            // but replica dissemination is not part of client commit latency.
            // Deduplicate because the redo queue may choose one request at
            // multiple slots before observing its first completion.
            outs.chosen
                .filter_map(q!(|(_epoch, _start, _slot, v)| v))
                .filter(q!(move |_| CLUSTER_SELF_ID.get_raw_id() == 0))
                .unique()
                .map(q!(|(client_raw, virtual_id, payload)| {
                    let client: MemberId<BenchClient> = MemberId::from_raw_id(client_raw);
                    (client, (virtual_id, payload))
                }))
                .into_keyed()
                .demux(clients, TCP.fail_stop().bincode())
                .values()
                .into_keyed()
        },
    )
    .entries()
    .map(q!(|(_virtual_client_id, (_output, latency))| latency));

    let bench_results = compute_throughput_latency(
        clients,
        latencies,
        client_interval_millis,
        nondet!(/** bench measurement window */),
    );
    let aggregate_results =
        aggregate_bench_results(bench_results, client_aggregator, aggregate_interval_millis);
    pretty_print_bench_results(aggregate_results);
}

/// Benchmarks the broadcast-transcript consensus protocol using `bench_client`.
///
/// Mirrors [`raft_bench`] exactly so the two are directly comparable: same client
/// workload, same routing, same cluster size. The only differences are protocol-
/// intrinsic: broadcast-transcript needs no heartbeat timer (leader activity is
/// observed from the transcript), and every protocol message is broadcast to all
/// members (O(n²) messages/round vs Raft's O(n)).
#[cfg(feature = "tokio")]
pub fn broadcast_transcript_bench<'a>(
    clients: &Cluster<'a, BenchClient>,
    num_clients_per_node: Singleton<usize, Cluster<'a, BenchClient>, Bounded>,
    client_aggregator: &Process<'a, BenchAggregator>,
    replicas: &Cluster<'a, Replica>,
    cluster_size: usize,
    client_interval_millis: u64,
    aggregate_interval_millis: u64,
) {
    // Election timer: only member 0 fires quickly to guarantee it wins leadership.
    // Other members have very long timeouts so they never campaign. No heartbeat
    // timer is needed — broadcast-transcript observes leader activity directly.
    let election_timer_interrupts =
        replicas.source_interval(q!(std::time::Duration::from_millis(
            if CLUSTER_SELF_ID.get_raw_id() == 0 {
                200
            } else {
                60_000
            }
        )));

    let latencies = bench_client(
        clients,
        num_clients_per_node,
        inc_i32_workload_generator,
        |input| {
            // Route requests to member 0 (leader) tagged with the originating client.
            let to_leader = input
                .entries()
                .map(q!(move |(virtual_id, payload)| {
                    let leader: MemberId<Replica> = MemberId::from_raw_id(0);
                    (leader, (CLUSTER_SELF_ID.clone(), virtual_id, payload))
                }))
                .into_keyed()
                .demux(replicas, TCP.fail_stop().bincode());

            let requests_on_replicas = to_leader.values();

            // Run broadcast-transcript consensus. Committed log carries
            // LogEntry<(MemberId<BenchClient>, u32, i32)>.
            let outputs = broadcast_transcript_consensus::broadcast_transcript_consensus(
                replicas,
                requests_on_replicas,
                election_timer_interrupts,
                BroadcastConsensusConfig { cluster_size },
                || TCP.fail_stop().bincode(),
                nondet!(
                    /// Which member leads and request interleaving is non-deterministic;
                    /// the benchmark only measures throughput/latency, not correctness.
                ),
            );

            // Only member 0 (the leader) routes responses back to clients, matching
            // raft_bench: all members receive committed entries via broadcast, but
            // routing all of them back would give each request N responses and break
            // bench_client's closed-loop feedback.
            let committed_payloads = outputs
                .committed
                .end_atomic()
                .weaken_consistency()
                .filter(q!(move |_| CLUSTER_SELF_ID.get_raw_id() == 0))
                .map(q!(|entry| {
                    let (client_id, virtual_id, payload) = entry.message;
                    (client_id, (virtual_id, payload))
                }))
                .into_keyed()
                .demux(clients, TCP.fail_stop().bincode());

            committed_payloads.values().into_keyed()
        },
    )
    .entries()
    .map(q!(|(_virtual_client_id, (_output, latency))| latency));

    let bench_results = compute_throughput_latency(
        clients,
        latencies,
        client_interval_millis,
        nondet!(/** bench measurement window */),
    );
    let aggregate_results =
        aggregate_bench_results(bench_results, client_aggregator, aggregate_interval_millis);
    pretty_print_bench_results(aggregate_results);
}

/// End-to-end replicated key-value store benchmark for broadcast-transcript
/// consensus, routed through [`kv_replica`](super::kv_replica::kv_replica) —
/// mirroring how [`paxos_bench`](super::paxos_bench::paxos_bench) drives
/// MultiPaxos — so the two can be compared apples-to-apples.
///
/// The closed loop is: `bench_client` → route request to leader (member 0) →
/// broadcast-transcript consensus → adapt committed log to the sequenced-KV
/// interface → `kv_replica` (applies the write, checkpoints periodically) →
/// route the processed KV payload back to the originating client → latency /
/// throughput.
///
/// Consensus and `kv_replica` both run on the same `Replica` cluster (from
/// [`super::kv_replica`]), so the deployment shape matches `paxos_bench`'s
/// replica tier and the comparison is fair.
#[cfg(feature = "tokio")]
#[expect(clippy::too_many_arguments, reason = "benchmark harness configuration")]
pub fn broadcast_transcript_kv_bench<'a>(
    clients: &Cluster<'a, BenchClient>,
    num_clients_per_node: Singleton<usize, Cluster<'a, BenchClient>, Bounded>,
    client_aggregator: &Process<'a, BenchAggregator>,
    replicas: &Cluster<'a, kv_replica::Replica>,
    cluster_size: usize,
    checkpoint_frequency: usize,
    client_interval_millis: u64,
    aggregate_interval_millis: u64,
) {
    // Election timer: only member 0 fires quickly to guarantee it wins leadership.
    // Other members have very long timeouts so they never campaign. No heartbeat
    // timer is needed — broadcast-transcript observes leader activity directly.
    let election_timer_interrupts =
        replicas.source_interval(q!(std::time::Duration::from_millis(
            if CLUSTER_SELF_ID.get_raw_id() == 0 {
                200
            } else {
                60_000
            }
        )));

    let latencies = bench_client(
        clients,
        num_clients_per_node,
        inc_i32_workload_generator,
        |input| {
            // Route requests to member 0 (leader). The KV key is the virtual
            // client id; the value carries the originating client member id (so
            // we can route the response back) alongside the counter payload.
            let to_leader = input
                .entries()
                .map(q!(move |(virtual_id, payload)| {
                    let leader: MemberId<kv_replica::Replica> = MemberId::from_raw_id(0);
                    (
                        leader,
                        KvPayload {
                            key: virtual_id,
                            value: (CLUSTER_SELF_ID.clone(), payload),
                        },
                    )
                }))
                .into_keyed()
                .demux(replicas, TCP.fail_stop().bincode());

            let requests_on_replicas = to_leader.values();

            // Run broadcast-transcript consensus. The committed log carries
            // KvPayload<u32, (MemberId<BenchClient>, i32)>.
            let outputs = broadcast_transcript_consensus::broadcast_transcript_consensus(
                replicas,
                requests_on_replicas,
                election_timer_interrupts,
                BroadcastConsensusConfig { cluster_size },
                || TCP.fail_stop().bincode(),
                nondet!(
                    /// Which member leads and request interleaving is non-deterministic;
                    /// the benchmark only measures throughput/latency, not correctness.
                ),
            );

            // Adapt the committed log to the sequenced-KV interface expected by
            // kv_replica: (slot, Some(payload)), weakened to NoOrder. kv_replica
            // reorders by seq internally, so ordering here does not matter.
            let sequenced = outputs
                .committed
                .end_atomic()
                .weaken_consistency()
                .map(q!(|entry| (entry.slot, Some(entry.message))))
                .weaken_ordering::<NoOrder>();

            // kv_replica applies the writes and periodically checkpoints. The
            // checkpoint-seq output is unused here: broadcast-transcript
            // self-checkpoints internally (no external signal is threaded back).
            let (_checkpoint_seq, processed) =
                kv_replica::kv_replica(replicas, sequenced, checkpoint_frequency);

            // Only member 0 routes responses back to clients, matching the other
            // benches: all members apply committed entries, but routing all of
            // them back would give each request N responses and break
            // bench_client's closed-loop feedback.
            let responses = processed
                .filter(q!(move |_| CLUSTER_SELF_ID.get_raw_id() == 0))
                .map(q!(|kv| {
                    let (client, counter) = kv.value;
                    (client, (kv.key, counter))
                }))
                .into_keyed()
                .demux(clients, TCP.fail_stop().bincode());

            // responses: KeyedStream<MemberId<Replica>, (u32, i32), Cluster<BenchClient>>
            // Drop the replica sender key, then re-key by virtual_id (u32).
            responses.values().into_keyed()
        },
    )
    .entries()
    .map(q!(|(_virtual_client_id, (_output, latency))| latency));

    let bench_results = compute_throughput_latency(
        clients,
        latencies,
        client_interval_millis,
        nondet!(/** bench measurement window */),
    );
    let aggregate_results =
        aggregate_bench_results(bench_results, client_aggregator, aggregate_interval_millis);
    pretty_print_bench_results(aggregate_results);
}

#[cfg(test)]
mod tests {
    use hydro_deploy::Deployment;
    use hydro_lang::deploy::{DeployCrateWrapper, TrybuildHost};

    const CLUSTER_SIZE: usize = 3;
    const NUM_VIRTUAL_CLIENTS: usize = 100;
    const CLIENT_INTERVAL_MILLIS: u64 = 100;
    const AGGREGATE_INTERVAL_MILLIS: u64 = 1000;

    #[cfg(stageleft_runtime)]
    fn create_raft_bench<'a>(
        clients: &hydro_lang::location::Cluster<'a, super::BenchClient>,
        client_aggregator: &hydro_lang::location::Process<'a, super::BenchAggregator>,
        replicas: &hydro_lang::location::Cluster<'a, super::Replica>,
    ) {
        use hydro_lang::location::Location;
        use stageleft::q;

        super::raft_bench(
            clients,
            clients.singleton(q!(NUM_VIRTUAL_CLIENTS)),
            client_aggregator,
            replicas,
            CLUSTER_SIZE,
            CLIENT_INTERVAL_MILLIS,
            AGGREGATE_INTERVAL_MILLIS,
        );
    }

    #[cfg(stageleft_runtime)]
    fn create_broadcast_transcript_bench<'a>(
        clients: &hydro_lang::location::Cluster<'a, super::BenchClient>,
        client_aggregator: &hydro_lang::location::Process<'a, super::BenchAggregator>,
        replicas: &hydro_lang::location::Cluster<'a, super::Replica>,
    ) {
        use hydro_lang::location::Location;
        use stageleft::q;

        super::broadcast_transcript_bench(
            clients,
            clients.singleton(q!(NUM_VIRTUAL_CLIENTS)),
            client_aggregator,
            replicas,
            CLUSTER_SIZE,
            CLIENT_INTERVAL_MILLIS,
            AGGREGATE_INTERVAL_MILLIS,
        );
    }

    #[cfg(stageleft_runtime)]
    fn create_multi_paxos_bench<'a>(
        clients: &hydro_lang::location::Cluster<'a, super::BenchClient>,
        client_aggregator: &hydro_lang::location::Process<'a, super::BenchAggregator>,
        replicas: &hydro_lang::location::Cluster<'a, super::Replica>,
    ) {
        use hydro_lang::location::Location;
        use stageleft::q;

        super::multi_paxos_bench(
            clients,
            clients.singleton(q!(NUM_VIRTUAL_CLIENTS)),
            client_aggregator,
            replicas,
            CLUSTER_SIZE,
            CLIENT_INTERVAL_MILLIS,
            AGGREGATE_INTERVAL_MILLIS,
        );
    }

    /// How many sequence numbers to commit before checkpointing, matching
    /// `paxos_bench` so the KV comparison is apples-to-apples.
    const CHECKPOINT_FREQUENCY: usize = 1000;

    #[cfg(stageleft_runtime)]
    fn create_broadcast_transcript_kv_bench<'a>(
        clients: &hydro_lang::location::Cluster<'a, super::BenchClient>,
        client_aggregator: &hydro_lang::location::Process<'a, super::BenchAggregator>,
        replicas: &hydro_lang::location::Cluster<'a, crate::cluster::kv_replica::Replica>,
    ) {
        use hydro_lang::location::Location;
        use stageleft::q;

        super::broadcast_transcript_kv_bench(
            clients,
            clients.singleton(q!(NUM_VIRTUAL_CLIENTS)),
            client_aggregator,
            replicas,
            CLUSTER_SIZE,
            CHECKPOINT_FREQUENCY,
            CLIENT_INTERVAL_MILLIS,
            AGGREGATE_INTERVAL_MILLIS,
        );
    }

    /// Per-size variants of [`create_broadcast_transcript_kv_bench`] that pass a
    /// LITERAL `cluster_size` into the staged bench. The cluster size flows into
    /// `q!` staged code (via `BroadcastConsensusConfig`/quorum sizing), so it
    /// must be a compile-time literal at the create site — a runtime value
    /// cannot be captured. These mirror `create_broadcast_transcript_kv_bench`
    /// exactly, differing only in the literal replica count (3/5/7), so the
    /// cluster-size sweep deploys 3/5/7 replicas.
    #[cfg(stageleft_runtime)]
    fn create_btc_kv_n3<'a>(
        clients: &hydro_lang::location::Cluster<'a, super::BenchClient>,
        client_aggregator: &hydro_lang::location::Process<'a, super::BenchAggregator>,
        replicas: &hydro_lang::location::Cluster<'a, crate::cluster::kv_replica::Replica>,
    ) {
        use hydro_lang::location::Location;
        use stageleft::q;

        super::broadcast_transcript_kv_bench(
            clients,
            clients.singleton(q!(NUM_VIRTUAL_CLIENTS)),
            client_aggregator,
            replicas,
            3usize,
            CHECKPOINT_FREQUENCY,
            CLIENT_INTERVAL_MILLIS,
            AGGREGATE_INTERVAL_MILLIS,
        );
    }

    #[cfg(stageleft_runtime)]
    fn create_btc_kv_n5<'a>(
        clients: &hydro_lang::location::Cluster<'a, super::BenchClient>,
        client_aggregator: &hydro_lang::location::Process<'a, super::BenchAggregator>,
        replicas: &hydro_lang::location::Cluster<'a, crate::cluster::kv_replica::Replica>,
    ) {
        use hydro_lang::location::Location;
        use stageleft::q;

        super::broadcast_transcript_kv_bench(
            clients,
            clients.singleton(q!(NUM_VIRTUAL_CLIENTS)),
            client_aggregator,
            replicas,
            5usize,
            CHECKPOINT_FREQUENCY,
            CLIENT_INTERVAL_MILLIS,
            AGGREGATE_INTERVAL_MILLIS,
        );
    }

    #[cfg(stageleft_runtime)]
    fn create_btc_kv_n7<'a>(
        clients: &hydro_lang::location::Cluster<'a, super::BenchClient>,
        client_aggregator: &hydro_lang::location::Process<'a, super::BenchAggregator>,
        replicas: &hydro_lang::location::Cluster<'a, crate::cluster::kv_replica::Replica>,
    ) {
        use hydro_lang::location::Location;
        use stageleft::q;

        super::broadcast_transcript_kv_bench(
            clients,
            clients.singleton(q!(NUM_VIRTUAL_CLIENTS)),
            client_aggregator,
            replicas,
            7usize,
            CHECKPOINT_FREQUENCY,
            CLIENT_INTERVAL_MILLIS,
            AGGREGATE_INTERVAL_MILLIS,
        );
    }

    /// Standalone deployed flow for the broadcast-transcript consensus
    /// integration test. Unlike the throughput benches, there are no clients or
    /// aggregator: every replica both *generates* requests (one every 100ms,
    /// tagged uniquely per member and nanosecond timestamp) and *prints* the
    /// entries it commits. This exercises the protocol through real process
    /// boundaries and real `TCP.fail_stop()` serialization.
    ///
    /// Only member 0 campaigns (fires the election timer), so leadership is
    /// stable and there is no failover — deployed failover under `fail_stop`
    /// is fragile, so this variant deliberately avoids it.
    ///
    /// `T = String` here (`String: Clone + Eq + Serialize + DeserializeOwned`).
    /// Each committed entry is printed with a parseable `[COMMITTED]` prefix
    /// from every member so the test can collect and cross-check them.
    #[cfg(stageleft_runtime)]
    fn create_btc_integration_impl<'a>(
        replicas: &hydro_lang::location::Cluster<'a, super::Replica>,
    ) {
        use hydro_lang::location::cluster::CLUSTER_SELF_ID;
        use hydro_lang::location::Location;
        use hydro_lang::nondet::nondet;
        use hydro_lang::prelude::TCP;
        use stageleft::q;

        use super::broadcast_transcript_consensus::{self, BroadcastConsensusConfig};

        // Each member generates a unique request every 100ms.
        let requests = replicas
            .source_interval(q!(std::time::Duration::from_millis(100)))
            .map(q!(move |_| {
                let ts = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos();
                format!("req-{}-{}", CLUSTER_SELF_ID.get_raw_id(), ts)
            }))
            .weaken_ordering::<hydro_lang::live_collections::stream::NoOrder>();

        // Only member 0 campaigns: stable leader, no failover.
        let election_timer_interrupts = replicas
            .source_interval(q!(std::time::Duration::from_millis(200)))
            .filter(q!(move |_| CLUSTER_SELF_ID.get_raw_id() == 0));

        let outputs = broadcast_transcript_consensus::broadcast_transcript_consensus(
            replicas,
            requests,
            election_timer_interrupts,
            BroadcastConsensusConfig { cluster_size: 3 },
            || TCP.fail_stop().bincode(),
            nondet!(/** integration test */),
        );

        // Print every committed entry from every member with a parseable prefix.
        outputs
            .committed
            .end_atomic()
            .weaken_consistency()
            .for_each(q!(|entry| println!(
                "[COMMITTED] slot={} ballot={} msg={}",
                entry.slot, entry.ballot, entry.message
            )));
    }

    /// DEPLOYED (real localhost processes) end-to-end functional test for
    /// `broadcast_transcript_consensus`, complementing the deterministic
    /// simulation tests by validating the protocol through real process
    /// boundaries and real network serialization under `TCP.fail_stop()`.
    ///
    /// This is the RELIABLE sustained-load variant: a stable leader (member 0),
    /// no leader-kill failover. It asserts three protocol guarantees observed
    /// across ALL three members' committed streams:
    ///
    /// - **Progress**: at least 3 distinct slots commit under sustained load.
    /// - **Agreement (safety)**: no two members ever commit different messages
    ///   for the same slot (no fork).
    /// - **Contiguity (gap-fill)**: the committed slots include slot 0 and are
    ///   gap-free up to the max observed slot.
    #[tokio::test]
    async fn broadcast_transcript_sustained_load() {
        use std::collections::{HashMap, HashSet};

        use regex::Regex;

        let mut builder = hydro_lang::compile::builder::FlowBuilder::new();
        let replicas = builder.cluster();

        create_btc_integration_impl(&replicas);

        let mut deployment = Deployment::new();

        let nodes = builder
            .with_cluster(
                &replicas,
                (0..CLUSTER_SIZE).map(|_| TrybuildHost::new(deployment.Localhost())),
            )
            .deploy(&mut deployment);

        deployment.deploy().await.unwrap();

        // Collect [COMMITTED] stdout lines from ALL members.
        let mut member_outs = nodes
            .get_cluster(&replicas)
            .members()
            .iter()
            .map(|node| node.stdout_filter("[COMMITTED]"))
            .collect::<Vec<_>>();
        assert_eq!(member_outs.len(), 3, "expected a 3-node replica cluster");

        deployment.start().await.unwrap();

        // Warm up so leadership is established. Entries committed during warm-up
        // remain buffered in the unbounded stdout channels, so we still observe
        // slot 0 once we start draining (which contiguity depends on).
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        // Drain all three members for ~10s using a biased select with a deadline.
        let mut r2 = member_outs.pop().unwrap();
        let mut r1 = member_outs.pop().unwrap();
        let mut r0 = member_outs.pop().unwrap();

        let deadline = tokio::time::sleep(std::time::Duration::from_secs(10));
        tokio::pin!(deadline);

        let mut lines: Vec<String> = Vec::new();
        loop {
            tokio::select! {
                biased;
                _ = &mut deadline => break,
                Some(line) = r0.recv() => lines.push(line),
                Some(line) = r1.recv() => lines.push(line),
                Some(line) = r2.recv() => lines.push(line),
            }
        }

        // Parse: [COMMITTED] slot=<n> ballot=<n> msg=<...>
        let re = Regex::new(r"slot=(\d+) ballot=(\d+) msg=(.+)").unwrap();
        let mut parsed: Vec<(usize, usize, String)> = Vec::new();
        for line in &lines {
            if let Some(caps) = re.captures(line) {
                let slot: usize = caps[1].parse().unwrap();
                let ballot: usize = caps[2].parse().unwrap();
                let msg = caps[3].to_string();
                parsed.push((slot, ballot, msg));
            }
        }

        println!(
            "broadcast_transcript_sustained_load: observed {} committed lines, {} parsed",
            lines.len(),
            parsed.len()
        );

        // (b) AGREEMENT: no two members may commit different messages for the
        // same slot. Building a slot -> msg map across ALL members' lines and
        // checking for a mismatch detects any fork.
        let mut slot_to_msg: HashMap<usize, String> = HashMap::new();
        for (slot, _ballot, msg) in &parsed {
            if let Some(existing) = slot_to_msg.get(slot) {
                assert_eq!(
                    existing, msg,
                    "SAFETY VIOLATION: slot {} committed with conflicting messages \
                     {:?} vs {:?} across members (fork detected)",
                    slot, existing, msg
                );
            } else {
                slot_to_msg.insert(*slot, msg.clone());
            }
        }

        // (a) PROGRESS: at least ~3 distinct slots committed under sustained load.
        let slots: HashSet<usize> = slot_to_msg.keys().copied().collect();
        assert!(
            slots.len() >= 3,
            "expected at least 3 committed slots under sustained load, saw {} (slots: {:?})",
            slots.len(),
            {
                let mut s: Vec<usize> = slots.iter().copied().collect();
                s.sort_unstable();
                s
            }
        );

        // (c) CONTIGUITY (gap-fill): committed slots include slot 0 and are
        // gap-free up to the max observed slot.
        let max_slot = *slots.iter().max().unwrap();
        assert!(
            slots.contains(&0),
            "expected slot 0 to be committed, but it was not observed (slots up to {})",
            max_slot
        );
        for s in 0..=max_slot {
            assert!(
                slots.contains(&s),
                "CONTIGUITY VIOLATION: slot {} missing (gap-fill guarantee); \
                 observed slots 0..={}",
                s,
                max_slot
            );
        }

        println!(
            "broadcast_transcript_sustained_load: PASS — {} contiguous slots (0..={}), \
             agreement holds across all 3 members",
            slots.len(),
            max_slot
        );
    }

    /// Collects 15 throughput windows from a started deployment's `client_out`
    /// and reports steady-state stats. Size-independent so the n=3/5/7 sweep
    /// tests do not duplicate the collect/report body.
    async fn collect_report(
        mut client_out: tokio::sync::mpsc::UnboundedReceiver<String>,
        label: &str,
    ) {
        use std::str::FromStr;

        use regex::Regex;

        let re = Regex::new(r"Throughput: ([^ ]+) requests/s").unwrap();
        let mut readings: Vec<f64> = Vec::new();
        while let Some(line) = client_out.recv().await {
            if let Some(caps) = re.captures(&line)
                && let Ok(v) = f64::from_str(&caps[1])
                && 0.0 < v
            {
                readings.push(v);
                if readings.len() >= 15 {
                    break;
                }
            }
        }
        report_throughput(label, &readings);
    }

    #[tokio::test]
    async fn broadcast_transcript_kv_throughput_n3() {
        let mut builder = hydro_lang::compile::builder::FlowBuilder::new();
        let clients = builder.cluster();
        let client_aggregator = builder.process();
        let replicas = builder.cluster();

        create_btc_kv_n3(&clients, &client_aggregator, &replicas);

        let mut deployment = Deployment::new();

        let nodes = builder
            .with_cluster(&clients, vec![TrybuildHost::new(deployment.Localhost())])
            .with_process(
                &client_aggregator,
                TrybuildHost::new(deployment.Localhost()),
            )
            .with_cluster(
                &replicas,
                (0..3).map(|_| TrybuildHost::new(deployment.Localhost())),
            )
            .deploy(&mut deployment);

        deployment.deploy().await.unwrap();

        let aggregator_node = &nodes.get_process(&client_aggregator);
        let client_out = aggregator_node.stdout_filter("Throughput:");

        deployment.start().await.unwrap();

        collect_report(client_out, "Broadcast-transcript KV (n=3)").await;
    }

    #[tokio::test]
    async fn broadcast_transcript_kv_throughput_n5() {
        let mut builder = hydro_lang::compile::builder::FlowBuilder::new();
        let clients = builder.cluster();
        let client_aggregator = builder.process();
        let replicas = builder.cluster();

        create_btc_kv_n5(&clients, &client_aggregator, &replicas);

        let mut deployment = Deployment::new();

        let nodes = builder
            .with_cluster(&clients, vec![TrybuildHost::new(deployment.Localhost())])
            .with_process(
                &client_aggregator,
                TrybuildHost::new(deployment.Localhost()),
            )
            .with_cluster(
                &replicas,
                (0..5).map(|_| TrybuildHost::new(deployment.Localhost())),
            )
            .deploy(&mut deployment);

        deployment.deploy().await.unwrap();

        let aggregator_node = &nodes.get_process(&client_aggregator);
        let client_out = aggregator_node.stdout_filter("Throughput:");

        deployment.start().await.unwrap();

        collect_report(client_out, "Broadcast-transcript KV (n=5)").await;
    }

    #[tokio::test]
    async fn broadcast_transcript_kv_throughput_n7() {
        let mut builder = hydro_lang::compile::builder::FlowBuilder::new();
        let clients = builder.cluster();
        let client_aggregator = builder.process();
        let replicas = builder.cluster();

        create_btc_kv_n7(&clients, &client_aggregator, &replicas);

        let mut deployment = Deployment::new();

        let nodes = builder
            .with_cluster(&clients, vec![TrybuildHost::new(deployment.Localhost())])
            .with_process(
                &client_aggregator,
                TrybuildHost::new(deployment.Localhost()),
            )
            .with_cluster(
                &replicas,
                (0..7).map(|_| TrybuildHost::new(deployment.Localhost())),
            )
            .deploy(&mut deployment);

        deployment.deploy().await.unwrap();

        let aggregator_node = &nodes.get_process(&client_aggregator);
        let client_out = aggregator_node.stdout_filter("Throughput:");

        deployment.start().await.unwrap();

        collect_report(client_out, "Broadcast-transcript KV (n=7)").await;
    }

    #[tokio::test]
    async fn raft_some_throughput() {
        let mut builder = hydro_lang::compile::builder::FlowBuilder::new();
        let clients = builder.cluster();
        let client_aggregator = builder.process();
        let replicas = builder.cluster();

        create_raft_bench(&clients, &client_aggregator, &replicas);

        let mut deployment = Deployment::new();

        let _nodes = builder
            .with_cluster(&clients, vec![TrybuildHost::new(deployment.Localhost())])
            .with_process(
                &client_aggregator,
                TrybuildHost::new(deployment.Localhost()),
            )
            .with_cluster(
                &replicas,
                (0..CLUSTER_SIZE).map(|_| TrybuildHost::new(deployment.Localhost())),
            )
            .deploy(&mut deployment);

        deployment.deploy().await.unwrap();

        let aggregator_node = &_nodes.get_process(&client_aggregator);
        let client_out = aggregator_node.stdout_filter("Throughput:");

        deployment.start().await.unwrap();

        use std::str::FromStr;
        use regex::Regex;

        let re = Regex::new(r"Throughput: ([^ ]+) requests/s").unwrap();
        let mut readings: Vec<f64> = Vec::new();
        let mut client_out = client_out;
        // Collect up to 15 windows; the first ~3 are warmup (leader election
        // + ramp-up) and are discarded before computing steady-state stats.
        while let Some(line) = client_out.recv().await {
            if let Some(caps) = re.captures(&line)
                && let Ok(v) = f64::from_str(&caps[1])
                && 0.0 < v
            {
                readings.push(v);
                if readings.len() >= 15 {
                    break;
                }
            }
        }
        report_throughput("Raft", &readings);
    }

    /// Same harness, workload, cluster size, and reporting as
    /// [`raft_some_throughput`] — the apples-to-apples run for the quorum
    /// ladder's multi-decree + liveness wrapper.
    #[tokio::test]
    async fn multi_paxos_some_throughput() {
        let mut builder = hydro_lang::compile::builder::FlowBuilder::new();
        let clients = builder.cluster();
        let client_aggregator = builder.process();
        let replicas = builder.cluster();

        create_multi_paxos_bench(&clients, &client_aggregator, &replicas);

        let mut deployment = Deployment::new();

        let _nodes = builder
            .with_cluster(&clients, vec![TrybuildHost::new(deployment.Localhost())])
            .with_process(
                &client_aggregator,
                TrybuildHost::new(deployment.Localhost()),
            )
            .with_cluster(
                &replicas,
                (0..CLUSTER_SIZE).map(|_| TrybuildHost::new(deployment.Localhost())),
            )
            .deploy(&mut deployment);

        deployment.deploy().await.unwrap();

        let aggregator_node = &_nodes.get_process(&client_aggregator);
        let client_out = aggregator_node.stdout_filter("Throughput:");

        deployment.start().await.unwrap();

        use std::str::FromStr;
        use regex::Regex;

        let re = Regex::new(r"Throughput: ([^ ]+) requests/s").unwrap();
        let mut readings: Vec<f64> = Vec::new();
        let mut client_out = client_out;
        while let Some(line) = client_out.recv().await {
            if let Some(caps) = re.captures(&line)
                && let Ok(v) = f64::from_str(&caps[1])
                && 0.0 < v
            {
                readings.push(v);
                if readings.len() >= 15 {
                    break;
                }
            }
        }
        report_throughput("Multi-Paxos (quorum ladder)", &readings);
    }

    #[tokio::test]
    async fn broadcast_transcript_some_throughput() {
        let mut builder = hydro_lang::compile::builder::FlowBuilder::new();
        let clients = builder.cluster();
        let client_aggregator = builder.process();
        let replicas = builder.cluster();

        create_broadcast_transcript_bench(&clients, &client_aggregator, &replicas);

        let mut deployment = Deployment::new();

        let _nodes = builder
            .with_cluster(&clients, vec![TrybuildHost::new(deployment.Localhost())])
            .with_process(
                &client_aggregator,
                TrybuildHost::new(deployment.Localhost()),
            )
            .with_cluster(
                &replicas,
                (0..CLUSTER_SIZE).map(|_| TrybuildHost::new(deployment.Localhost())),
            )
            .deploy(&mut deployment);

        deployment.deploy().await.unwrap();

        let aggregator_node = &_nodes.get_process(&client_aggregator);
        let client_out = aggregator_node.stdout_filter("Throughput:");

        deployment.start().await.unwrap();

        use std::str::FromStr;
        use regex::Regex;

        let re = Regex::new(r"Throughput: ([^ ]+) requests/s").unwrap();
        let mut readings: Vec<f64> = Vec::new();
        let mut client_out = client_out;
        while let Some(line) = client_out.recv().await {
            if let Some(caps) = re.captures(&line)
                && let Ok(v) = f64::from_str(&caps[1])
                && 0.0 < v
            {
                readings.push(v);
                if readings.len() >= 15 {
                    break;
                }
            }
        }
        report_throughput("Broadcast-transcript", &readings);
    }

    #[tokio::test]
    async fn broadcast_transcript_kv_some_throughput() {
        let mut builder = hydro_lang::compile::builder::FlowBuilder::new();
        let clients = builder.cluster();
        let client_aggregator = builder.process();
        let replicas = builder.cluster();

        create_broadcast_transcript_kv_bench(&clients, &client_aggregator, &replicas);

        let mut deployment = Deployment::new();

        let _nodes = builder
            .with_cluster(&clients, vec![TrybuildHost::new(deployment.Localhost())])
            .with_process(
                &client_aggregator,
                TrybuildHost::new(deployment.Localhost()),
            )
            .with_cluster(
                &replicas,
                (0..CLUSTER_SIZE).map(|_| TrybuildHost::new(deployment.Localhost())),
            )
            .deploy(&mut deployment);

        deployment.deploy().await.unwrap();

        let aggregator_node = &_nodes.get_process(&client_aggregator);
        let client_out = aggregator_node.stdout_filter("Throughput:");

        deployment.start().await.unwrap();

        use std::str::FromStr;
        use regex::Regex;

        let re = Regex::new(r"Throughput: ([^ ]+) requests/s").unwrap();
        let mut readings: Vec<f64> = Vec::new();
        let mut client_out = client_out;
        while let Some(line) = client_out.recv().await {
            if let Some(caps) = re.captures(&line)
                && let Ok(v) = f64::from_str(&caps[1])
                && 0.0 < v
            {
                readings.push(v);
                if readings.len() >= 15 {
                    break;
                }
            }
        }
        report_throughput("Broadcast-transcript-KV", &readings);
    }

    /// Discards the first 3 warmup windows and prints min/median/max over the
    /// remaining steady-state readings, so Raft and broadcast-transcript are
    /// compared on the same steady-state basis.
    fn report_throughput(label: &str, readings: &[f64]) {
        println!("{} raw throughput windows: {:?}", label, readings);
        let steady: Vec<f64> = readings.iter().skip(3).copied().collect();
        let sample = if steady.is_empty() { readings } else { &steady };
        if sample.is_empty() {
            println!("{} steady-state: NO READINGS", label);
            return;
        }
        let mut sorted = sample.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let min = sorted.first().copied().unwrap();
        let max = sorted.last().copied().unwrap();
        let median = sorted[sorted.len() / 2];
        let mean = sample.iter().sum::<f64>() / sample.len() as f64;
        println!(
            "{} STEADY-STATE req/s over {} windows: min={:.0} median={:.0} mean={:.0} max={:.0}",
            label,
            sample.len(),
            min,
            median,
            mean,
            max
        );
    }
}
