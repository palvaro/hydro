//! Non-interactive EC2 test: deploy, PUT keys, kill primary via EC2 API, verify failover.
//! Run: cargo run -p lego_replicate --features backend_redb --example ec2_autotest --release
//!
//! f=1: 3 replicas + 1 router = 4 EC2 instances.

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

    let (cmd_sink, cmds_at_router) = router.source_external_bincode(external);
    let cmds_for_fd = cmds_at_router.clone();

    let puts_at_router = cmds_at_router.clone()
        .filter(q!(|cmd: &String| cmd.starts_with("PUT:")));
    let gets_at_router = cmds_at_router
        .filter(q!(|cmd: &String| cmd.starts_with("GET:")));

    let puts_at_replicas: Stream<Vec<u8>, Cluster<'a, TransparentReplica>, Unbounded> =
        puts_at_router
            .map(q!(|cmd: String| bincode::serialize(&cmd).unwrap()))
            .broadcast(replicas, TCP.fail_stop().bincode(), nondet!(/** broadcast PUTs */))
            .assume_ordering(nondet!(/** ordering */));

    let gets_at_replicas = gets_at_router
        .broadcast(replicas, TCP.fail_stop().bincode(), nondet!(/** broadcast GETs */));

    let (proposals_complete, proposals_ref) =
        replicas.forward_ref::<Stream<View, _, Unbounded, NoOrder>>();

    let output = lego_replicate::protocol::compose_protocol(
        replicas, proposers, acceptors, puts_at_replicas, proposals_ref, config,
    );

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

    let responses_at_router = responses
        .send(router, TCP.fail_stop().bincode())
        .values();

    let fd_proposals = lego_replicate::protocol::process_router_failure_detector(
        router, replicas,
        cmds_for_fd.weaken_ordering::<NoOrder>(),
        responses_at_router.clone().weaken_ordering::<NoOrder>(),
        output.current_view, timeout_ms, initial_member_count,
    );

    proposals_complete.complete(fd_proposals);

    let resp_stream = responses_at_router.send_bincode_external(external);
    (cmd_sink, resp_stream)
}

#[tokio::main]
async fn main() {
    let region = "us-east-1";
    let ami = "ami-0e95a5e2743ec9ec9";
    let instance_type = "t3.micro";
    let rustflags = "-C opt-level=3 -C codegen-units=1";

    println!("=== Lego Replicate EC2 Autotest ===");
    println!("  f={}: {} nodes + 1 router = {} instances\n", F, N, N + 1);

    let mut deployment = Deployment::new();
    let existing_vpc = hydro_deploy::AwsNetwork::new(
        region,
        Some(hydro_deploy::aws::NetworkResources::new(
            "vpc-041b334556d749bfb",
            "subnet-099a08497349a365a",
            "sg-05d8b0bfd0d33b8c4",
        )),
    );

    let hosts: Vec<Arc<dyn Host>> = (0..N)
        .map(|i| -> Arc<dyn Host> {
            deployment.AwsEc2Host()
                .region(region).instance_type(instance_type).ami(ami)
                .network(existing_vpc.clone()).display_name(format!("node-{}", i)).add()
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

    let features = vec!["backend_redb".to_string()];
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

    println!("Deploying to EC2...");
    deployment.deploy().await.unwrap();

    #[cfg(stageleft_runtime)]
    let mut sender = nodes.connect(cmd_sink).await;
    #[cfg(stageleft_runtime)]
    let mut receiver = nodes.connect(resp_stream).await;

    deployment.start().await.unwrap();
    println!("All processes started.\n");

    #[cfg(stageleft_runtime)]
    {
        // === Phase 1: PUT keys ===
        println!("=== Phase 1: PUT keys ===");
        for i in 1..=3u64 {
            sender.send(format!("PUT:k{}:value{}:{}", i, i, i)).await.unwrap();
        }

        let mut got = 0;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() { println!("TIMEOUT phase 1"); std::process::exit(1); }
            match tokio::time::timeout(remaining, receiver.next()).await {
                Ok(Some(resp)) => {
                    println!("  {}", resp);
                    if resp.starts_with("OK") { got += 1; if got >= 3 { break; } }
                }
                _ => { println!("FAIL phase 1"); std::process::exit(1); }
            }
        }
        println!("Phase 1 PASS: {} PUT responses\n", got);

        // === Phase 2: Stop primary (node 0) ===
        println!("=== Phase 2: Stop node 0 (primary) ===");
        let find = |name: &str, state: &str| {
            let region = "us-east-1";
            let name = name.to_string();
            let state = state.to_string();
            async move {
                let out = tokio::process::Command::new("aws")
                    .args(["ec2", "describe-instances", "--region", region,
                        "--filters", &format!("Name=tag:Name,Values=*{}*", name), &format!("Name=instance-state-name,Values={}", state),
                        "Name=vpc-id,Values=vpc-041b334556d749bfb",
                        "--query", "Reservations[0].Instances[0].InstanceId",
                        "--output", "text", "--no-paginate", "--no-cli-pager"])
                    .output().await.unwrap();
                String::from_utf8_lossy(&out.stdout).trim().to_string()
            }
        };

        let stop = |id: String| {
            let region = "us-east-1";
            async move {
                tokio::process::Command::new("aws")
                    .args(["ec2", "stop-instances", "--region", region, "--instance-ids", &id, "--no-paginate", "--no-cli-pager"])
                    .output().await.unwrap();
                tokio::process::Command::new("aws")
                    .args(["ec2", "wait", "instance-stopped", "--region", region, "--instance-ids", &id])
                    .output().await.unwrap();
            }
        };

        let start = |id: String| {
            let region = "us-east-1";
            async move {
                tokio::process::Command::new("aws")
                    .args(["ec2", "start-instances", "--region", region, "--instance-ids", &id, "--no-paginate", "--no-cli-pager"])
                    .output().await.unwrap();
                tokio::process::Command::new("aws")
                    .args(["ec2", "wait", "instance-running", "--region", region, "--instance-ids", &id])
                    .output().await.unwrap();
            }
        };

        let node0_id = find("node-0", "running").await;
        println!("  Node 0: {}", node0_id);
        if node0_id.is_empty() || node0_id == "None" { println!("FAIL: can't find node-0"); std::process::exit(1); }
        stop(node0_id.clone()).await;
        println!("  Node 0 stopped.\n");

        // === Phase 3: Stop backup (node 1) ===
        println!("=== Phase 3: Stop node 1 (backup) ===");
        let node1_id = find("node-1", "running").await;
        println!("  Node 1: {}", node1_id);
        if node1_id.is_empty() || node1_id == "None" { println!("FAIL: can't find node-1"); std::process::exit(1); }
        stop(node1_id.clone()).await;
        println!("  Node 1 stopped.\n");

        // === Phase 4: Start primary (node 0) ===
        println!("=== Phase 4: Start node 0 ===");
        start(node0_id.clone()).await;
        println!("  Node 0 started. Waiting for reconnection...");
        tokio::time::sleep(std::time::Duration::from_secs(15)).await;

        // === Phase 5: Stop another backup (node 2) ===
        println!("=== Phase 5: Stop node 2 (backup) ===");
        let node2_id = find("node-2", "running").await;
        println!("  Node 2: {}", node2_id);
        if node2_id.is_empty() || node2_id == "None" { println!("FAIL: can't find node-2"); std::process::exit(1); }
        stop(node2_id.clone()).await;
        println!("  Node 2 stopped.\n");

        // === Phase 6: Verify recovery — GET must work ===
        // Alive: node 0 (restarted), node 3, node 4. That's 3 of 5 — quorum.
        println!("=== Phase 6: Verify recovery (GET k1) ===");
        let mut nonce = 100u64;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(90);
        let mut recovered = false;
        while tokio::time::Instant::now() < deadline {
            sender.send(format!("GET:k1:{}", nonce)).await.unwrap();
            let nonce_str = format!("nonce={}", nonce);
            match tokio::time::timeout(std::time::Duration::from_millis(2000), async {
                while let Some(r) = receiver.next().await {
                    println!("    got: {}", r);
                    if r.contains(&nonce_str) { return Some(r); }
                }
                None
            }).await {
                Ok(Some(r)) => {
                    println!("  RECOVERED: {}", r);
                    assert!(r.contains("k1=value1"), "Expected value1, got: {}", r);
                    recovered = true;
                    break;
                }
                _ => {
                    nonce += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
                }
            }
        }
        if !recovered {
            println!("FAIL: system did not recover after 60s");
            std::process::exit(1);
        }
        println!("Phase 6 PASS: recovered with restarted node in view\n");

        println!("=== ALL PHASES PASSED ===");
    }
}
