//! Interactive EC2 demo with rusqlite backend.
//!
//! f=2: 5 replicas + 1 router = 6 EC2 instances.
//! Commands: PUT <key> <value> | GET <key> | quit
//!
//! Run with:
//!   cargo run -p lego_replicate --features backend_rusqlite --example ec2_demo2

use std::sync::Arc;
use futures::{SinkExt, StreamExt};
use hydro_deploy::{Deployment, Host};
use hydro_lang::deploy::TrybuildHost;
use hydro_lang::location::cluster::CLUSTER_SELF_ID;
use hydro_lang::location::external_process::{ExternalBincodeSink, ExternalBincodeStream};
use hydro_lang::prelude::*;
use hydro_test::cluster::paxos::{Acceptor, Proposer};
use lego_replicate::messages::TransparentReplica;
use lego_replicate::{Router, ReplicateConfig, View};

const F: usize = 2;
const N: usize = 2 * F + 1;

#[cfg(stageleft_runtime)]
fn build_pipeline<'a>(
    external: &External<'a, ()>,
    replicas: &Cluster<'a, TransparentReplica>,
    proposers: &Cluster<'a, Proposer>,
    acceptors: &Cluster<'a, Acceptor>,
    router: &Process<'a, Router>,
) -> (
    ExternalBincodeSink<String>,
    ExternalBincodeStream<String, hydro_lang::live_collections::stream::NoOrder>,
) {
    let config = ReplicateConfig {
        f: F,
        initial_members: (0..N as u32).collect(),
        commit_timeout_ms: 3000,
        notification_interval_ms: 200,
        paxos_config: lego_replicate::config::PaxosConfig {
            f: F,
            i_am_leader_send_timeout: 2,
            i_am_leader_check_timeout: 5,
            i_am_leader_check_timeout_delay_multiplier: 3,
        },
        ..ReplicateConfig::default()
    };

    let _timeout_ms = config.commit_timeout_ms;
    let _initial_member_count = config.initial_members.len();
    let config2 = config.clone();

    // External → Router
    let (cmd_sink, cmds_at_router) = router.source_external_bincode(external);

    let cmds_for_fd = cmds_at_router.clone();

    // Split PUTs and GETs at router
    let puts_at_router = cmds_at_router.clone()
        .filter(q!(|cmd: &String| cmd.starts_with("PUT:")));
    let gets_at_router = cmds_at_router
        .filter(q!(|cmd: &String| cmd.starts_with("GET:")));

    // Route PUTs to replicas as serialized bytes
    let puts_at_replicas: Stream<Vec<u8>, Cluster<'a, TransparentReplica>, Unbounded> =
        puts_at_router
            .map(q!(|cmd: String| bincode::serialize(&cmd).unwrap()))
            .broadcast(replicas, TCP.fail_stop().bincode(), nondet!(/** broadcast PUTs */))
            .assume_ordering(nondet!(/** ordering */));

    // Route GETs to replicas (for primary-only processing)
    let gets_at_replicas = gets_at_router
        .broadcast(replicas, TCP.fail_stop().bincode(), nondet!(/** broadcast GETs */));

    // Forward ref for FD proposals (breaks the cycle: FD needs responses, responses need protocol)
    let (proposals_complete, proposals_ref) =
        replicas.forward_ref::<Stream<View, _, Unbounded, hydro_lang::live_collections::stream::NoOrder>>();

    // Run the lego protocol
    let output = lego_replicate::protocol::compose_protocol(
        replicas, proposers, acceptors, puts_at_replicas, proposals_ref, config,
    );

    // Apply committed commands + GETs on primary
    let scan_tick = replicas.tick();
    let _hb = replicas
        .source_interval(q!(std::time::Duration::from_millis(100)), nondet!(/** heartbeat */))
        .batch(&scan_tick, nondet!(/** heartbeat tick */));

    let is_primary = output.current_view.clone()
        .snapshot(&scan_tick, nondet!(/** stale view ok */))
        .filter(q!(move |v: &View| CLUSTER_SELF_ID.get_raw_id() == v.primary()))
        .map(q!(|_| ()));

    let gets_on_primary = gets_at_replicas
        .batch(&scan_tick, nondet!(/** batch GETs */))
        .filter_if_some(is_primary)
        .weaken_ordering::<hydro_lang::live_collections::stream::NoOrder>()
        .all_ticks()
        .map(q!(|cmd| (false, 0usize, cmd)));

    let committed_puts = output.replicated
        .map(q!(|(seq, payload): (usize, Vec<u8>)| {
            let cmd: String = bincode::deserialize(&payload).unwrap();
            (true, seq, cmd)
        }));

    let responses = committed_puts
        .interleave(gets_on_primary)
        .batch(&scan_tick, nondet!(/** batch */))
        .sort()
        .all_ticks()
        .scan(
            q!(|| lego_replicate::applier::RusqliteApplierState::new()),
            q!(|state: &mut lego_replicate::applier::RusqliteApplierState,
                (is_put, seq, cmd): (bool, usize, String)| {
                Some(state.apply_command(if is_put { seq } else { 0 }, &cmd))
            }),
        );

    // Responses → Router → External
    let responses_at_router = responses
        .send(router, TCP.fail_stop().bincode())
        .values();

    // Wire the failure detector
    let fd_proposals = lego_replicate::protocol::process_router_failure_detector(
        router, replicas,
        cmds_for_fd.weaken_ordering::<hydro_lang::live_collections::stream::NoOrder>(),
        responses_at_router.clone().weaken_ordering::<hydro_lang::live_collections::stream::NoOrder>(),
        output.current_view,
        config2.commit_timeout_ms,
        config2.initial_members.len(),
    );

    proposals_complete.complete(fd_proposals);

    let resp_stream = responses_at_router.send_bincode_external(external);
    (cmd_sink, resp_stream)
}

fn parse_nonce(resp: &str) -> Option<u64> {
    resp.split("nonce=").nth(1)?.split_whitespace().next()?.parse().ok()
}

#[tokio::main]
async fn main() {
    let region = "us-east-1";
    let ami = "ami-0e95a5e2743ec9ec9";
    let instance_type = "t3.micro";
    let rustflags = "-C opt-level=3 -C codegen-units=1";

    println!("=== Lego Replicate EC2 Demo (rusqlite) ===");
    println!("  f={}: {} nodes, 1 router = {} EC2 instances", F, N, N + 1);
    println!("Commands: PUT <key> <value>  |  GET <key>  |  quit");
    println!();

    let mut deployment = Deployment::new();
    let existing_vpc = hydro_deploy::AwsNetwork::new(
        region,
        None,
    );

    let hosts: Vec<Arc<dyn Host>> = (0..N)
        .map(|i| -> Arc<dyn Host> {
            let name = if i == 0 { "PRIMARY".to_string() } else { format!("backup-{}", i) };
            deployment.AwsEc2Host()
                .region(region).instance_type(instance_type).ami(ami)
                .network(existing_vpc.clone()).display_name(name).add()
        })
        .collect();

    let router_host: Arc<dyn Host> = deployment.AwsEc2Host()
        .region(region).instance_type(instance_type).ami(ami)
        .network(existing_vpc.clone()).display_name("router".to_string()).add();

    let mut builder = FlowBuilder::new();
    let external = builder.external::<()>();
    let replicas = builder.cluster::<TransparentReplica>();
    let proposers = builder.cluster::<Proposer>();
    let acceptors = builder.cluster::<Acceptor>();
    let router = builder.process::<Router>();

    #[cfg(stageleft_runtime)]
    let (cmd_sink, resp_stream) =
        build_pipeline(&external, &replicas, &proposers, &acceptors, &router);

    let features = vec!["backend_rusqlite".to_string()];
    let nodes = builder
        .with_external(&external, deployment.Localhost())
        .with_cluster(&replicas, hosts.iter().map(|h|
            TrybuildHost::new(h.clone()).rustflags(rustflags).features(features.clone())))
        .with_cluster(&proposers, hosts.iter().map(|h|
            TrybuildHost::new(h.clone()).rustflags(rustflags).features(features.clone())))
        .with_cluster(&acceptors, hosts.iter().map(|h|
            TrybuildHost::new(h.clone()).rustflags(rustflags).features(features.clone())))
        .with_process(&router, TrybuildHost::new(router_host.clone()).rustflags(rustflags).features(features.clone()))
        .deploy(&mut deployment);

    println!("Deploying to EC2 ({} instances)...", N + 1);
    deployment.deploy().await.unwrap();

    #[cfg(stageleft_runtime)]
    let mut sender = nodes.connect(cmd_sink).await;
    #[cfg(stageleft_runtime)]
    let mut receiver = nodes.connect(resp_stream).await;

    deployment.start().await.unwrap();
    println!("All processes started. Ready.\n");

    #[cfg(stageleft_runtime)]
    {
        let mut nonce: u64 = 1;
        let stdin = tokio::io::stdin();
        let mut lines = tokio::io::AsyncBufReadExt::lines(tokio::io::BufReader::new(stdin));

        loop {
            print!("> ");
            use std::io::Write;
            std::io::stdout().flush().unwrap();

            let line = match lines.next_line().await { Ok(Some(l)) => l, _ => break };
            let line = line.trim().to_string();
            if line.is_empty() { continue; }
            if line == "quit" || line == "exit" { break; }

            let parts: Vec<&str> = line.splitn(3, ' ').collect();
            let cmd_str = match parts[0].to_uppercase().as_str() {
                "PUT" if parts.len() == 3 => format!("PUT:{}:{}:{}", parts[1], parts[2], nonce),
                "GET" if parts.len() == 2 => format!("GET:{}:{}", parts[1], nonce),
                _ => { println!("Usage: PUT <key> <value>  |  GET <key>  |  quit"); continue; }
            };

            let current_nonce = nonce;
            nonce += 1;
            sender.send(cmd_str).await.unwrap();

            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(3);
            let result = loop {
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() { break None; }
                match tokio::time::timeout(remaining, receiver.next()).await {
                    Ok(Some(resp)) => {
                        if let Some(n) = parse_nonce(&resp) {
                            if n == current_nonce { break Some(resp); }
                        }
                    }
                    _ => break None,
                }
            };

            match result {
                Some(resp) => println!("{}", resp),
                None => println!("❌ No response (timeout — retry in a moment)"),
            }
        }
    }
    println!("Bye.");
}
