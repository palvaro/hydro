//! EC2 multi-router test: 2 independent Process<Router> instances.
//! Verifies system works through both, kills one, verifies the other still works.
//!
//! Run: cargo run -p lego_replicate --features backend_redb --example ec2_multi_router_test --release

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

struct Router2;

const F: usize = 1;
const N: usize = 2 * F + 1;

#[cfg(stageleft_runtime)]
fn build_pipeline<'a>(
    external: &External<'a, ()>,
    replicas: &Cluster<'a, TransparentReplica>,
    proposers: &Cluster<'a, Proposer>,
    acceptors: &Cluster<'a, Acceptor>,
    router1: &Process<'a, Router>,
    router2: &Process<'a, Router2>,
) -> (
    ExternalBincodeSink<String>,
    ExternalBincodeStream<String, hydro_lang::live_collections::stream::NoOrder>,
    ExternalBincodeSink<String>,
    ExternalBincodeStream<String, hydro_lang::live_collections::stream::NoOrder>,
) {
    use hydro_lang::live_collections::stream::NoOrder;

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

    let initial_member_count = config.initial_members.len();
    let timeout_ms = config.commit_timeout_ms;
    let config2 = config.clone();

    // Router 1
    let (cmd_sink1, cmds_at_r1) = router1.source_external_bincode(external);
    let cmds_for_fd1 = cmds_at_r1.clone();
    let puts_at_r1 = cmds_at_r1.clone().filter(q!(|cmd: &String| cmd.starts_with("PUT:")));
    let gets_at_r1 = cmds_at_r1.filter(q!(|cmd: &String| cmd.starts_with("GET:")));

    // Router 2
    let (cmd_sink2, cmds_at_r2) = router2.source_external_bincode(external);
    let cmds_for_fd2 = cmds_at_r2.clone();
    let puts_at_r2 = cmds_at_r2.clone().filter(q!(|cmd: &String| cmd.starts_with("PUT:")));
    let gets_at_r2 = cmds_at_r2.filter(q!(|cmd: &String| cmd.starts_with("GET:")));

    // Merge PUTs from both routers at replicas
    let puts_from_r1: Stream<Vec<u8>, Cluster<'a, TransparentReplica>, Unbounded> =
        puts_at_r1.map(q!(|cmd: String| bincode::serialize(&cmd).unwrap()))
            .broadcast(replicas, TCP.fail_stop().bincode(), nondet!(/** r1 PUTs */))
            .assume_ordering(nondet!(/** ordering */));
    let puts_from_r2: Stream<Vec<u8>, Cluster<'a, TransparentReplica>, Unbounded> =
        puts_at_r2.map(q!(|cmd: String| bincode::serialize(&cmd).unwrap()))
            .broadcast(replicas, TCP.fail_stop().bincode(), nondet!(/** r2 PUTs */))
            .assume_ordering(nondet!(/** ordering */));
    let all_puts: Stream<Vec<u8>, Cluster<'a, TransparentReplica>, Unbounded> =
        puts_from_r1.interleave(puts_from_r2)
            .assume_ordering(nondet!(/** merged PUT ordering */));

    // Merge GETs from both routers
    let gets_from_r1 = gets_at_r1.broadcast(replicas, TCP.fail_stop().bincode(), nondet!(/** r1 GETs */));
    let gets_from_r2 = gets_at_r2.broadcast(replicas, TCP.fail_stop().bincode(), nondet!(/** r2 GETs */));
    let all_gets = gets_from_r1.interleave(gets_from_r2);

    // Forward ref for FD proposals from BOTH routers
    let (proposals_complete, proposals_ref) =
        replicas.forward_ref::<Stream<View, _, Unbounded, NoOrder>>();

    let output = lego_replicate::protocol::compose_protocol(
        replicas, proposers, acceptors, all_puts, proposals_ref, config,
    );

    let scan_tick = replicas.tick();
    let _hb = replicas
        .source_interval(q!(std::time::Duration::from_millis(100)), nondet!(/** heartbeat */))
        .batch(&scan_tick, nondet!(/** heartbeat tick */));

    let is_primary = output.current_view.clone()
        .snapshot(&scan_tick, nondet!(/** stale view ok */))
        .filter(q!(move |v: &View| CLUSTER_SELF_ID.get_raw_id() == v.primary()))
        .map(q!(|_| ()));

    let gets_on_primary = all_gets
        .batch(&scan_tick, nondet!(/** batch GETs */))
        .filter_if_some(is_primary)
        .weaken_ordering::<NoOrder>()
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
            q!(|| lego_replicate::applier::RedbApplierState::new()),
            q!(|state: &mut lego_replicate::applier::RedbApplierState,
                (is_put, seq, cmd): (bool, usize, String)| {
                Some(state.apply_command(if is_put { seq } else { 0 }, &cmd))
            }),
        );

    // Send responses to BOTH routers
    let resp_at_r1 = responses.clone()
        .send(router1, TCP.fail_stop().bincode()).values();
    let resp_at_r2 = responses
        .send(router2, TCP.fail_stop().bincode()).values();

    // FD on router 1
    let fd1 = lego_replicate::protocol::process_router_failure_detector(
        router1, replicas,
        cmds_for_fd1.weaken_ordering::<NoOrder>(),
        resp_at_r1.clone().weaken_ordering::<NoOrder>(),
        output.current_view.clone(), timeout_ms, initial_member_count,
    );

    // FD on router 2
    let fd2 = lego_replicate::protocol::process_router_failure_detector(
        router2, replicas,
        cmds_for_fd2.weaken_ordering::<NoOrder>(),
        resp_at_r2.clone().weaken_ordering::<NoOrder>(),
        output.current_view, config2.commit_timeout_ms, initial_member_count,
    );

    // Merge proposals from both FDs
    proposals_complete.complete(fd1.interleave(fd2));

    let resp_stream1 = resp_at_r1.send_bincode_external(external);
    let resp_stream2 = resp_at_r2.send_bincode_external(external);

    (cmd_sink1, resp_stream1, cmd_sink2, resp_stream2)
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

    println!("=== Lego Replicate Multi-Router Test (2 Process<Router>) ===\n");

    let mut deployment = Deployment::new();
    let existing_vpc = hydro_deploy::AwsNetwork::new(
        region,
        None,
    );

    let hosts: Vec<Arc<dyn Host>> = (0..N).map(|i| -> Arc<dyn Host> {
        deployment.AwsEc2Host().region(region).instance_type(instance_type).ami(ami)
            .network(existing_vpc.clone()).display_name(format!("node-{}", i)).add()
    }).collect();

    let router1_host: Arc<dyn Host> = deployment.AwsEc2Host().region(region).instance_type(instance_type).ami(ami)
        .network(existing_vpc.clone()).display_name("router-0".to_string()).add();
    let router2_host: Arc<dyn Host> = deployment.AwsEc2Host().region(region).instance_type(instance_type).ami(ami)
        .network(existing_vpc.clone()).display_name("router-1".to_string()).add();

    let mut builder = FlowBuilder::new();
    let external = builder.external::<()>();
    let replicas = builder.cluster::<TransparentReplica>();
    let proposers = builder.cluster::<Proposer>();
    let acceptors = builder.cluster::<Acceptor>();
    let router1 = builder.process::<Router>();
    let router2 = builder.process::<Router2>();

    #[cfg(stageleft_runtime)]
    let (cmd_sink1, resp_stream1, cmd_sink2, resp_stream2) =
        build_pipeline(&external, &replicas, &proposers, &acceptors, &router1, &router2);

    let features = vec!["backend_redb".to_string()];
    let nodes = builder
        .with_external(&external, deployment.Localhost())
        .with_cluster(&replicas, hosts.iter().map(|h| TrybuildHost::new(h.clone()).rustflags(rustflags).features(features.clone())))
        .with_cluster(&proposers, hosts.iter().map(|h| TrybuildHost::new(h.clone()).rustflags(rustflags).features(features.clone())))
        .with_cluster(&acceptors, hosts.iter().map(|h| TrybuildHost::new(h.clone()).rustflags(rustflags).features(features.clone())))
        .with_process(&router1, TrybuildHost::new(router1_host).rustflags(rustflags).features(features.clone()))
        .with_process(&router2, TrybuildHost::new(router2_host).rustflags(rustflags).features(features.clone()))
        .deploy(&mut deployment);

    println!("Deploying to EC2...");
    deployment.deploy().await.unwrap();

    #[cfg(stageleft_runtime)]
    let mut sender1 = nodes.connect(cmd_sink1).await;
    #[cfg(stageleft_runtime)]
    let mut receiver1 = nodes.connect(resp_stream1).await;
    #[cfg(stageleft_runtime)]
    let mut sender2 = nodes.connect(cmd_sink2).await;
    #[cfg(stageleft_runtime)]
    let mut receiver2 = nodes.connect(resp_stream2).await;

    deployment.start().await.unwrap();
    println!("All processes started.\n");

    #[cfg(stageleft_runtime)]
    {
        // Phase 1: PUT through router 1
        println!("=== Phase 1: PUT k1..k3 through router 1 ===");
        for i in 1..=3u64 {
            sender1.send(format!("PUT:k{}:v{}:{}", i, i, i)).await.unwrap();
            let nonce_str = format!("nonce={}", i);
            match tokio::time::timeout(std::time::Duration::from_secs(15), async {
                loop { match receiver1.next().await { Some(r) if r.contains(&nonce_str) => return r, Some(_) => {}, _ => return String::new() } }
            }).await {
                Ok(r) if !r.is_empty() => println!("  {}", r),
                _ => { println!("FAIL phase 1"); std::process::exit(1); }
            }
        }
        println!("Phase 1 PASS\n");

        // Phase 2: GET through router 2 (proves both routers see same state)
        println!("=== Phase 2: GET k1..k3 through router 2 ===");
        for i in 1..=3u64 {
            let nonce = 10 + i;
            sender2.send(format!("GET:k{}:{}", i, nonce)).await.unwrap();
            let nonce_str = format!("nonce={}", nonce);
            match tokio::time::timeout(std::time::Duration::from_secs(15), async {
                loop { match receiver2.next().await { Some(r) if r.contains(&nonce_str) => return r, Some(_) => {}, _ => return String::new() } }
            }).await {
                Ok(r) if !r.is_empty() => {
                    let expected = format!("k{}=v{}", i, i);
                    assert!(r.contains(&expected), "Expected {}, got: {}", expected, r);
                    println!("  {}", r);
                }
                _ => { println!("FAIL phase 2"); std::process::exit(1); }
            }
        }
        println!("Phase 2 PASS\n");

        // Phase 3: Kill router-0, verify router-1 still works
        println!("=== Phase 3: Kill router-0, PUT/GET through router-1 ===");
        let out = tokio::process::Command::new("aws")
            .args(["ec2", "describe-instances", "--region", region,
                "--filters", "Name=tag:Name,Values=*router-0*", "Name=instance-state-name,Values=running",
                "Name=vpc-id,Values=vpc-041b334556d749bfb",
                "--query", "Reservations[0].Instances[0].InstanceId",
                "--output", "text", "--no-paginate", "--no-cli-pager"])
            .output().await.unwrap();
        let router0_id = String::from_utf8_lossy(&out.stdout).trim().to_string();
        if router0_id.is_empty() || router0_id == "None" { println!("FAIL: can't find router-0"); std::process::exit(1); }
        tokio::process::Command::new("aws")
            .args(["ec2", "stop-instances", "--region", region, "--instance-ids", &router0_id, "--no-paginate", "--no-cli-pager"])
            .output().await.unwrap();
        tokio::process::Command::new("aws")
            .args(["ec2", "wait", "instance-stopped", "--region", region, "--instance-ids", &router0_id])
            .output().await.unwrap();
        println!("  router-0 stopped.");

        // PUT through router 2
        let nonce = 20u64;
        sender2.send(format!("PUT:k4:v4:{}", nonce)).await.unwrap();
        let nonce_str = format!("nonce={}", nonce);
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        let mut ok = false;
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(std::time::Duration::from_millis(1000), async {
                loop { match receiver2.next().await { Some(r) if r.contains(&nonce_str) => return Some(r), Some(_) => {}, _ => return None } }
            }).await {
                Ok(Some(r)) => { println!("  PUT k4 via router-1: {}", r); ok = true; break; }
                _ => { sender2.send(format!("PUT:k4:v4:{}", nonce)).await.unwrap(); tokio::time::sleep(std::time::Duration::from_millis(500)).await; }
            }
        }
        if !ok { println!("FAIL phase 3: PUT through router-1 after killing router-0"); std::process::exit(1); }

        // GET through router 2
        let nonce = 21u64;
        sender2.send(format!("GET:k4:{}", nonce)).await.unwrap();
        let nonce_str = format!("nonce={}", nonce);
        match tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop { match receiver2.next().await { Some(r) if r.contains(&nonce_str) => return r, Some(_) => {}, _ => return String::new() } }
        }).await {
            Ok(r) if r.contains("k4=v4") => println!("  GET k4 via router-1: {}", r),
            _ => { println!("FAIL phase 3: GET through router-1"); std::process::exit(1); }
        }

        println!("Phase 3 PASS\n=== ALL PHASES PASSED ===");
    }
}
