//! Non-interactive EC2 test: deploy, send 3 PUTs, verify responses, exit.
//! Run: AWS_ACCESS_KEY_ID=... cargo run -p hydro_transparent_replicate --features backend_redb --example ec2_autotest --release

use std::sync::Arc;
use futures::SinkExt;
use futures::StreamExt;
use hydro_deploy::{Deployment, Host};
use hydro_lang::deploy::TrybuildHost;
use hydro_lang::location::cluster::CLUSTER_SELF_ID;
use hydro_lang::location::external_process::{ExternalBincodeSink, ExternalBincodeStream};
use hydro_lang::prelude::*;
use hydro_test::cluster::paxos::{Acceptor, Proposer};
use hydro_transparent_replicate::messages::TransparentReplica;
use hydro_transparent_replicate::protocol::{coordinator_failure_detector, replicate_service_raw};
use hydro_transparent_replicate::{Coordinator, ReplicateConfig};

const F: usize = 1;
const N: usize = 2 * F + 1; // 3 replicas for faster deploy

#[cfg(stageleft_runtime)]
fn build_pipeline<'a>(
   external: &External<'a, ()>,
   replicas: &Cluster<'a, TransparentReplica>,
   proposers: &Cluster<'a, Proposer>,
   acceptors: &Cluster<'a, Acceptor>,
   coordinator: &Process<'a, Coordinator>,
) -> (
   ExternalBincodeSink<String>,
   ExternalBincodeStream<String, hydro_lang::live_collections::stream::NoOrder>,
) {
    let config = ReplicateConfig {
        f: F,
        initial_members: (0..N as u32).collect(),
        commit_timeout_ms: 3000,
        notification_interval_ms: 200,
        paxos_config: hydro_transparent_replicate::config::PaxosConfig {
            f: F,
            i_am_leader_send_timeout: 2,
            i_am_leader_check_timeout: 5,
            i_am_leader_check_timeout_delay_multiplier: 3,
        },
        ..ReplicateConfig::default()
    };

    let initial_member_count = config.initial_members.len();
    let timeout_ms = config.commit_timeout_ms;

    let (cmd_sink, cmds_at_coord) = coordinator.source_external_bincode(external);

    let cmds_for_fd = cmds_at_coord.clone();

    let puts_at_coord = cmds_at_coord.clone()
        .filter(q!(|cmd: &String| cmd.starts_with("PUT:")));
    let gets_at_coord = cmds_at_coord
        .filter(q!(|cmd: &String| cmd.starts_with("GET:")));

    let puts_at_replicas: Stream<String, Cluster<'a, TransparentReplica>, Unbounded> =
        puts_at_coord
            .broadcast(replicas, TCP.fail_stop().bincode(), nondet!(/** broadcast PUTs */))
            .assume_ordering(nondet!(/** ordering */));

    let gets_at_replicas: Stream<String, Cluster<'a, TransparentReplica>, Unbounded> =
        gets_at_coord
            .broadcast(replicas, TCP.fail_stop().bincode(), nondet!(/** broadcast GETs */));

    let (proposals_complete, proposals_ref) =
        replicas.forward_ref::<Stream<hydro_transparent_replicate::View, _, Unbounded, hydro_lang::live_collections::stream::NoOrder>>();

    let raw = replicate_service_raw::<String>(
        replicas, proposers, acceptors, puts_at_replicas, proposals_ref, config,
    );

    let scan_tick = replicas.tick();

    let _hb = replicas
        .source_interval(q!(std::time::Duration::from_millis(100)), nondet!(/** heartbeat */))
        .batch(&scan_tick, nondet!(/** heartbeat tick */));

    let is_primary = raw.current_view.clone()
        .snapshot(&scan_tick, nondet!(/** stale view ok */))
        .filter(q!(move |v: &hydro_transparent_replicate::messages::View| {
            CLUSTER_SELF_ID.get_raw_id() == v.primary()
        }))
        .map(q!(|_| ()));

    let gets_on_primary = gets_at_replicas
        .batch(&scan_tick, nondet!(/** batch GETs */))
        .filter_if_some(is_primary)
        .weaken_ordering::<hydro_lang::live_collections::stream::NoOrder>()
        .all_ticks()
        .map(q!(|cmd| (false, 0usize, cmd)));

    let committed_puts = raw.replicated
        .map(q!(|(seq, cmd)| (true, seq, cmd)));

    let responses = committed_puts
        .interleave(gets_on_primary)
        .batch(&scan_tick, nondet!(/** batch */))
        .sort()
        .all_ticks()
        .scan(
            q!(|| hydro_transparent_replicate::applier::RedbApplierState::new()),
            q!(|state: &mut hydro_transparent_replicate::applier::RedbApplierState,
                (is_put, seq, cmd): (bool, usize, String)| {
                Some(state.apply_command(if is_put { seq } else { 0 }, &cmd))
            }),
        );

    let responses_at_coord = responses
        .send(coordinator, TCP.fail_stop().bincode())
        .values();

    let fd_proposals = coordinator_failure_detector(
        coordinator, replicas,
        cmds_for_fd.weaken_ordering::<hydro_lang::live_collections::stream::NoOrder>(),
        responses_at_coord.clone(),
        raw.current_view, timeout_ms, initial_member_count,
    );
    proposals_complete.complete(fd_proposals);

    let resp_stream = responses_at_coord.send_bincode_external(external);
    (cmd_sink, resp_stream)
}

#[tokio::main]
async fn main() {
    let region = "us-east-1";
    let ami = "ami-0e95a5e2743ec9ec9";
    let instance_type = "t3.micro";
    let rustflags = "-C opt-level=3 -C codegen-units=1";

    println!("=== EC2 Automated Test (f={}, N={}) ===", F, N);

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

    let coordinator_host: Arc<dyn Host> = deployment.AwsEc2Host()
        .region(region).instance_type(instance_type).ami(ami)
        .network(existing_vpc.clone()).display_name("coordinator".to_string()).add();

    let mut builder = FlowBuilder::new();
    let external = builder.external::<()>();
    let replicas = builder.cluster::<TransparentReplica>();
    let proposers = builder.cluster::<Proposer>();
    let acceptors = builder.cluster::<Acceptor>();
    let coordinator = builder.process::<Coordinator>();

    #[cfg(stageleft_runtime)]
    let (cmd_sink, resp_stream) =
        build_pipeline(&external, &replicas, &proposers, &acceptors, &coordinator);

    let features = vec!["backend_redb".to_string()];
    let nodes = builder
        .with_external(&external, deployment.Localhost())
        .with_cluster(&replicas, hosts.iter().map(|h|
            TrybuildHost::new(h.clone()).rustflags(rustflags).features(features.clone())))
        .with_cluster(&proposers, hosts.iter().map(|h|
            TrybuildHost::new(h.clone()).rustflags(rustflags).features(features.clone())))
        .with_cluster(&acceptors, hosts.iter().map(|h|
            TrybuildHost::new(h.clone()).rustflags(rustflags).features(features.clone())))
        .with_process(&coordinator, TrybuildHost::new(coordinator_host.clone()).rustflags(rustflags).features(features.clone()))
        .deploy(&mut deployment);

    println!("Deploying to EC2...");
    deployment.deploy().await.unwrap();

    #[cfg(stageleft_runtime)]
    let mut sender = nodes.connect(cmd_sink).await;
    #[cfg(stageleft_runtime)]
    let mut receiver = nodes.connect(resp_stream).await;

    deployment.start().await.unwrap();
    println!("All processes started.");

    #[cfg(stageleft_runtime)]
    {
        // === Phase 1: PUT keys and confirm ===
        println!("\n=== Phase 1: PUT keys ===");
        for i in 1..=3u64 {
            let cmd = format!("PUT:k{}:value{}:{}", i, i, i);
            println!("SENDING: {}", cmd);
            sender.send(cmd).await.unwrap();
        }

        let mut got = 0;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() { println!("TIMEOUT phase 1"); std::process::exit(1); }
            match tokio::time::timeout(remaining, receiver.next()).await {
                Ok(Some(resp)) => {
                    println!("  RESPONSE: {}", resp);
                    if resp.starts_with("OK") { got += 1; if got >= 3 { break; } }
                }
                _ => { println!("FAIL phase 1"); std::process::exit(1); }
            }
        }
        println!("Phase 1 PASS: {} PUT responses", got);

        // === Phase 2: Stop node 0 EC2 instance, verify failover ===
        println!("\n=== Phase 2: Stop node 0 (EC2 instance stop) ===");

        // Find node-0 instance (Name tag is "hydro-ec2-instance-XXXX-node-0")
        let find_instance = tokio::process::Command::new("aws")
            .args(["ec2", "describe-instances", "--region", region,
                "--filters", "Name=tag:Name,Values=*node-0*", "Name=instance-state-name,Values=running",
                "Name=vpc-id,Values=vpc-041b334556d749bfb",
                "--query", "Reservations[0].Instances[0].InstanceId",
                "--output", "text", "--no-paginate", "--no-cli-pager"])
            .output().await.unwrap();
        let node0_id = String::from_utf8_lossy(&find_instance.stdout).trim().to_string();
        println!("  Node 0 instance ID: {}", node0_id);

        if node0_id.is_empty() || node0_id == "None" {
            println!("FAIL phase 2: could not find node-0 instance"); std::process::exit(1);
        }

        // Stop it
        tokio::process::Command::new("aws")
            .args(["ec2", "stop-instances", "--region", region, "--instance-ids", &node0_id, "--no-paginate", "--no-cli-pager"])
            .output().await.unwrap();
        tokio::process::Command::new("aws")
            .args(["ec2", "wait", "instance-stopped", "--region", region, "--instance-ids", &node0_id])
            .output().await.unwrap();
        println!("Node 0 stopped.");

        // Retry GETs until one succeeds
        let mut nonce = 10u64;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        let mut recovered = false;
        while tokio::time::Instant::now() < deadline {
            let cmd = format!("GET:k1:{}", nonce);
            sender.send(cmd).await.unwrap();
            let nonce_str = format!("nonce={}", nonce);
            match tokio::time::timeout(std::time::Duration::from_millis(1000), async {
                while let Some(r) = receiver.next().await {
                    if r.contains(&nonce_str) { return Some(r); }
                }
                None
            }).await {
                Ok(Some(r)) => {
                    println!("  RECOVERED: {}", r);
                    recovered = true;
                    break;
                }
                _ => {
                    nonce += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
            }
        }
        if !recovered { println!("FAIL phase 2: failover did not recover"); std::process::exit(1); }
        println!("Phase 2 PASS: failover recovered after killing node 0");

        // === Phase 3: Restart node 0 via EC2 API (testing rejoin) ===
        println!("\n=== Phase 3: Restart node 0 via EC2 stop/start ===");

        // Find the EC2 instance for node-0 by name tag (it's stopped now)
        let find_instance = tokio::process::Command::new("aws")
            .args(["ec2", "describe-instances", "--region", region,
                "--filters", "Name=tag:Name,Values=*node-0*", "Name=instance-state-name,Values=stopped",
                "Name=vpc-id,Values=vpc-041b334556d749bfb",
                "--query", "Reservations[0].Instances[0].InstanceId",
                "--output", "text", "--no-paginate", "--no-cli-pager"])
            .output().await.unwrap();
        let instance_id = String::from_utf8_lossy(&find_instance.stdout).trim().to_string();
        println!("  Node 0 instance ID: {}", instance_id);

        if instance_id.is_empty() || instance_id == "None" {
            println!("FAIL phase 3: could not find stopped node-0 instance"); std::process::exit(1);
        }

        // Start it again (it's already stopped from Phase 2)
        println!("  Starting instance {}...", instance_id);
        tokio::process::Command::new("aws")
            .args(["ec2", "start-instances", "--region", region, "--instance-ids", &instance_id, "--no-paginate", "--no-cli-pager"])
            .output().await.unwrap();

        // Wait for it to be running
        println!("  Waiting for instance to start...");
        tokio::process::Command::new("aws")
            .args(["ec2", "wait", "instance-running", "--region", region, "--instance-ids", &instance_id])
            .output().await.unwrap();

        println!("  Instance restarted. Waiting 10s for process to reconnect...");
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;

        // Stop node 1 via EC2 (current primary after first failover)
        println!("\n=== Phase 4: Stop node 1 (current primary) ===");
        let find_instance = tokio::process::Command::new("aws")
            .args(["ec2", "describe-instances", "--region", region,
                "--filters", "Name=tag:Name,Values=*node-1*", "Name=instance-state-name,Values=running",
                "Name=vpc-id,Values=vpc-041b334556d749bfb",
                "--query", "Reservations[0].Instances[0].InstanceId",
                "--output", "text", "--no-paginate", "--no-cli-pager"])
            .output().await.unwrap();
        let node1_id = String::from_utf8_lossy(&find_instance.stdout).trim().to_string();
        println!("  Node 1 instance ID: {}", node1_id);
        if node1_id.is_empty() || node1_id == "None" {
            println!("FAIL phase 4: could not find node-1 instance"); std::process::exit(1);
        }
        tokio::process::Command::new("aws")
            .args(["ec2", "stop-instances", "--region", region, "--instance-ids", &node1_id, "--no-paginate", "--no-cli-pager"])
            .output().await.unwrap();
        tokio::process::Command::new("aws")
            .args(["ec2", "wait", "instance-stopped", "--region", region, "--instance-ids", &node1_id])
            .output().await.unwrap();
        println!("Node 1 stopped.");

        // Retry GETs — node 0 must have rejoined for quorum to exist
        nonce = 100;
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        let mut recovered2 = false;
        while tokio::time::Instant::now() < deadline {
            let cmd = format!("GET:k1:{}", nonce);
            sender.send(cmd).await.unwrap();
            let nonce_str = format!("nonce={}", nonce);
            match tokio::time::timeout(std::time::Duration::from_millis(1000), async {
                while let Some(r) = receiver.next().await {
                    println!("    got: {}", r);
                    if r.contains(&nonce_str) { return Some(r); }
                }
                None
            }).await {
                Ok(Some(r)) => {
                    println!("  RECOVERED: {}", r);
                    recovered2 = true;
                    break;
                }
                _ => {
                    nonce += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                }
            }
        }
        if !recovered2 {
            println!("FAIL phase 4: node 0 did not rejoin the view (no quorum after killing node 1)");
            std::process::exit(1);
        }
        println!("Phase 4 PASS: node 0 rejoined — quorum maintained after killing node 1");

        println!("\n=== DONE ===");
    }
}
