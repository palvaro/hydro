//! Benchmark: lego_replicate throughput/latency using hydro_std::bench_client.
//!
//! Same harness as paxos_bench — measures replication overhead with in-memory KV.
//!
//! Run: cargo test -p lego_replicate --test bench --release -- --nocapture

#[cfg(stageleft_runtime)]
use hydro_lang::live_collections::stream::NoOrder;
#[cfg(stageleft_runtime)]
use hydro_lang::location::cluster::CLUSTER_SELF_ID;
#[cfg(stageleft_runtime)]
use hydro_lang::prelude::*;
#[cfg(stageleft_runtime)]
use hydro_std::bench_client::{
    BenchResult, aggregate_bench_results, bench_client, compute_throughput_latency,
    pretty_print_bench_results,
};
#[cfg(stageleft_runtime)]
use hydro_test::cluster::paxos::{Acceptor, Proposer};
#[cfg(stageleft_runtime)]
use lego_replicate::messages::TransparentReplica;
#[cfg(stageleft_runtime)]
use lego_replicate::{Router, ReplicateConfig, View};
use lego_replicate::messages::{BenchClient, BenchAggregator};




const F: usize = 1;
const N: usize = 2 * F + 1;

#[cfg(stageleft_runtime)]
fn lego_bench<'a>(
    replicas: &Cluster<'a, TransparentReplica>,
    proposers: &Cluster<'a, Proposer>,
    acceptors: &Cluster<'a, Acceptor>,
    clients: &Cluster<'a, BenchClient>,
    client_aggregator: &Process<'a, BenchAggregator>,
) {
    let config = ReplicateConfig {
        f: F,
        initial_members: (0..N as u32).collect(),
        ..ReplicateConfig::default()
    };

    let num_clients_per_node = clients.singleton(q!(100usize));

    let latencies = bench_client(
        clients,
        num_clients_per_node,
        // Workload generator: incrementing i32
        |ids_and_prev| {
            ids_and_prev.map(q!(move |payload| {
                if let Some(counter) = payload { counter + 1 } else { 0i32 }
            }))
        },
        // Protocol: serialize → send to replicas → compose_protocol → route back
        |input| {
            // Serialize (virtual_client_id, payload) as bytes
            let at_replicas = input
                .entries()
                .map(q!(move |(vid, payload): (u32, i32)| {
                    bincode::serialize(&(CLUSTER_SELF_ID.get_raw_id(), vid, payload)).unwrap()
                }))
                .broadcast(replicas, TCP.fail_stop().bincode(), nondet!(/** */))
                .values()
                .assume_ordering::<hydro_lang::live_collections::stream::TotalOrder>(nondet!(/** */));

            let no_proposals: Stream<View, _, Unbounded, NoOrder> = replicas
                .source_iter(q!(Vec::<View>::new())).weaken_ordering::<NoOrder>().into();

            let output = lego_replicate::protocol::compose_protocol(
                replicas, proposers, acceptors, at_replicas, no_proposals, config,
            );

            // Route committed responses back to originating client
            output.committed_in_order
                .map(q!(|(_seq, payload): (usize, Vec<u8>)| {
                    let (client_raw, vid, value): (u32, u32, i32) = bincode::deserialize(&payload).unwrap();
                    (client_raw, (vid, value))
                }))
                .map(q!(|(client_raw, payload)| (
                    hydro_lang::location::MemberId::from_raw_id(client_raw),
                    payload,
                )))
                .demux(clients, TCP.fail_stop().bincode())
                .values()
                .into_keyed()
        },
    )
    .entries()
    .map(q!(|(_vid, (_output, latency))| latency));

    let bench_results = compute_throughput_latency(clients, latencies, 100, nondet!(/** */));
    let aggregate = aggregate_bench_results(bench_results, client_aggregator, 1000);
    pretty_print_bench_results(aggregate);
}

#[tokio::test]
async fn lego_replicate_throughput() {
    use hydro_deploy::Deployment;
    use hydro_lang::deploy::{DeployCrateWrapper, TrybuildHost};

    let mut builder = FlowBuilder::new();
    let replicas = builder.cluster::<TransparentReplica>();
    let proposers = builder.cluster::<Proposer>();
    let acceptors = builder.cluster::<Acceptor>();
    let clients = builder.cluster::<BenchClient>();
    let aggregator = builder.process::<BenchAggregator>();

    #[cfg(stageleft_runtime)]
    lego_bench(&replicas, &proposers, &acceptors, &clients, &aggregator);

    let mut deployment = Deployment::new();

    let nodes = builder
        .with_cluster(&replicas, (0..N).map(|_| TrybuildHost::new(deployment.Localhost())))
        .with_cluster(&proposers, (0..N).map(|_| TrybuildHost::new(deployment.Localhost())))
        .with_cluster(&acceptors, (0..2*F+1).map(|_| TrybuildHost::new(deployment.Localhost())))
        .with_cluster(&clients, vec![TrybuildHost::new(deployment.Localhost())])
        .with_process(&aggregator, TrybuildHost::new(deployment.Localhost()))
        .deploy(&mut deployment);

    deployment.deploy().await.unwrap();

    let agg_node = nodes.get_process(&aggregator);
    let mut out = agg_node.stdout_filter("Throughput:");

    deployment.start().await.unwrap();

    let mut found = 0;
    while let Some(line) = out.recv().await {
        println!("{}", line);
        if line.contains("Throughput:") {
            found += 1;
            if found >= 2 { break; }
        }
    }
    assert!(found >= 2, "Expected at least 2 throughput measurements");
}
