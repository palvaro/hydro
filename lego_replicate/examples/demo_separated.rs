//! Demo: separated architecture. The Hydro dataflow handles replication,
//! a separate Process handles the backend (via ServiceRunner).
//!
//! Topology:
//!   External → Router → Replicas (replication) → ServiceProcess (applies to redb)
//!                                                       ↓
//!                                               Router → External
//!
//! The ServiceProcess owns the RedbService independently. On restart it could
//! resume from persisted state (not demonstrated here due to Hydro framework
//! limitations, but the ServiceRunner API supports it).
//!
//! Run: cargo run -p lego_replicate --features backend_redb --example demo_separated

#[cfg(stageleft_runtime)]
use hydro_lang::location::cluster::CLUSTER_SELF_ID;
#[cfg(stageleft_runtime)]
use hydro_lang::location::external_process::{ExternalBincodeSink, ExternalBincodeStream};
#[cfg(stageleft_runtime)]
use hydro_lang::prelude::*;
#[cfg(stageleft_runtime)]
use hydro_test::cluster::paxos::{Acceptor, Proposer};
#[cfg(stageleft_runtime)]
use lego_replicate::messages::TransparentReplica;
#[cfg(stageleft_runtime)]
use lego_replicate::{Router, ReplicateConfig, View};

struct ServiceProcess;

const F: usize = 1;
const N: usize = 2 * F + 1;

#[cfg(stageleft_runtime)]
fn build_pipeline<'a>(
    external: &External<'a, ()>,
    replicas: &Cluster<'a, TransparentReplica>,
    proposers: &Cluster<'a, Proposer>,
    acceptors: &Cluster<'a, Acceptor>,
    router: &Process<'a, Router>,
    service: &Process<'a, ServiceProcess>,
) -> (
    ExternalBincodeSink<String>,
    ExternalBincodeStream<String, hydro_lang::live_collections::stream::NoOrder>,
) {
    use hydro_lang::live_collections::stream::NoOrder;

    let config = ReplicateConfig {
        f: F,
        initial_members: (0..N as u32).collect(),
        ..ReplicateConfig::default()
    };

    let (cmd_sink, cmds_at_router) = router.source_external_bincode(external);

    let puts_at_router = cmds_at_router.clone()
        .filter(q!(|cmd: &String| cmd.starts_with("PUT:")));
    let gets_at_router = cmds_at_router
        .filter(q!(|cmd: &String| cmd.starts_with("GET:")));

    let puts_at_replicas: Stream<Vec<u8>, Cluster<'a, TransparentReplica>, Unbounded> =
        puts_at_router
            .map(q!(|cmd: String| bincode::serialize(&cmd).unwrap()))
            .broadcast(replicas, TCP.fail_stop().bincode(), nondet!(/** */))
            .assume_ordering(nondet!(/** */));

    let gets_at_replicas = gets_at_router
        .broadcast(replicas, TCP.fail_stop().bincode(), nondet!(/** */));

    let no_proposals: Stream<View, _, Unbounded, NoOrder> = replicas
        .source_iter(q!(Vec::<View>::new())).weaken_ordering::<NoOrder>().into();

    let output = lego_replicate::protocol::compose_protocol(
        replicas, proposers, acceptors, puts_at_replicas, no_proposals, config,
    );

    // === SEPARATION: send committed commands to the ServiceProcess ===
    // The dataflow's job ends here — it delivers (seq, payload) to the service.
    let at_service = output.replicated
        .send(service, TCP.fail_stop().bincode())
        .values();

    // GETs also go to the service process (routed through primary first)
    let scan_tick = replicas.tick();
    let _hb = replicas.source_interval(q!(std::time::Duration::from_millis(100)), nondet!(/** */))
        .batch(&scan_tick, nondet!(/** */));
    let is_primary = output.current_view
        .snapshot(&scan_tick, nondet!(/** */))
        .filter(q!(move |v: &View| CLUSTER_SELF_ID.get_raw_id() == v.primary()))
        .map(q!(|_| ()));

    let gets_on_primary = gets_at_replicas
        .batch(&scan_tick, nondet!(/** */))
        .filter_if_some(is_primary)
        .weaken_ordering::<NoOrder>()
        .all_ticks();

    let gets_at_service = gets_on_primary
        .map(q!(|cmd: String| (false, 0usize, cmd)))
        .send(service, TCP.fail_stop().bincode())
        .values();

    // === SERVICE PROCESS: owns the backend, applies commands ===
    // This is the separated service — it could be a standalone process with
    // its own lifecycle, persisting state independently.
    let commits_tagged = at_service
        .map(q!(|(seq, payload): (usize, Vec<u8>)| {
            let cmd: String = bincode::deserialize(&payload).unwrap();
            (true, seq, cmd)
        }));

    let service_tick = service.tick();
    let responses = commits_tagged
        .interleave(gets_at_service)
        .batch(&service_tick, nondet!(/** */))
        .sort()
        .all_ticks()
        .scan(
            q!(|| lego_replicate::applier::RedbApplierState::new()),
            q!(|state: &mut lego_replicate::applier::RedbApplierState,
                (is_put, seq, cmd): (bool, usize, String)| {
                Some(state.apply_command(if is_put { seq } else { 0 }, &cmd))
            }),
        );

    // Responses back to router → external
    let responses_at_router = responses
        .send(router, TCP.fail_stop().bincode());

    let resp_stream = responses_at_router.weaken_ordering::<hydro_lang::live_collections::stream::NoOrder>().send_bincode_external(external);
    (cmd_sink, resp_stream)
}

fn parse_nonce(resp: &str) -> Option<u64> {
    resp.split("nonce=").nth(1)?.split_whitespace().next()?.parse().ok()
}

#[tokio::main]
async fn main() {
    use futures::{SinkExt, StreamExt};
    use hydro_deploy::Deployment;
    use hydro_lang::deploy::TrybuildHost;

    println!("=== Separated Architecture Demo ===");
    println!("  Dataflow handles replication, ServiceProcess handles backend\n");

    let mut deployment = Deployment::new();
    let mut builder = FlowBuilder::new();

    let external = builder.external::<()>();
    let replicas = builder.cluster::<TransparentReplica>();
    let proposers = builder.cluster::<Proposer>();
    let acceptors = builder.cluster::<Acceptor>();
    let router = builder.process::<Router>();
    let service = builder.process::<ServiceProcess>();

    #[cfg(stageleft_runtime)]
    let (cmd_sink, resp_stream) =
        build_pipeline(&external, &replicas, &proposers, &acceptors, &router, &service);

    let features = vec!["backend_redb".to_string()];
    let nodes = builder
        .with_external(&external, deployment.Localhost())
        .with_cluster(&replicas, (0..N).map(|_| TrybuildHost::new(deployment.Localhost()).features(features.clone())))
        .with_cluster(&proposers, (0..N).map(|_| TrybuildHost::new(deployment.Localhost()).features(features.clone())))
        .with_cluster(&acceptors, (0..N).map(|_| TrybuildHost::new(deployment.Localhost()).features(features.clone())))
        .with_process(&router, TrybuildHost::new(deployment.Localhost()).features(features.clone()))
        .with_process(&service, TrybuildHost::new(deployment.Localhost()).features(features.clone()))
        .deploy(&mut deployment);

    deployment.deploy().await.unwrap();

    #[cfg(stageleft_runtime)]
    let mut sender = nodes.connect(cmd_sink).await;
    #[cfg(stageleft_runtime)]
    let mut receiver = nodes.connect(resp_stream).await;

    deployment.start().await.unwrap();
    println!("All processes started.\n");

    #[cfg(stageleft_runtime)]
    {
        // PUT and GET
        for (i, (op, key, val)) in [("PUT", "x", "hello"), ("PUT", "y", "world"), ("GET", "x", ""), ("GET", "y", "")].iter().enumerate() {
            let nonce = i + 1;
            let cmd = if *op == "PUT" { format!("PUT:{}:{}:{}", key, val, nonce) } else { format!("GET:{}:{}", key, nonce) };
            sender.send(cmd.clone()).await.unwrap();

            let nonce_str = format!("nonce={}", nonce);
            match tokio::time::timeout(std::time::Duration::from_secs(10), async {
                while let Some(r) = receiver.next().await {
                    if r.contains(&nonce_str) { return Some(r); }
                }
                None
            }).await {
                Ok(Some(r)) => println!("  {} → {}", cmd, r),
                _ => { println!("  {} → TIMEOUT", cmd); std::process::exit(1); }
            }
        }
        println!("\nPASS: separated architecture works");
    }
}
