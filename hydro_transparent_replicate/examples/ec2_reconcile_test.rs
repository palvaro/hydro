//! EC2 reconciliation test: writes happen while a node is down, then that node
//! rejoins and must serve those writes after the current primary dies.
//!
//! Sequence:
//!   1. PUT k1..k3 (all nodes see them)
//!   2. Kill node 0 (primary)
//!   3. PUT k4..k6 while node 0 is down (only nodes 1,2 see them)
//!   4. Restart node 0 — it must reconcile the missed writes
//!   5. Kill node 1 (current primary) — node 0 must serve GETs for k4..k6
//!
//! If reconciliation works, Phase 5 GETs return the values written in Phase 3.
//! If not, the test fails.
//!
//! Run: AWS_ACCESS_KEY_ID=... cargo run -p hydro_transparent_replicate --features backend_redb --example ec2_reconcile_test

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
const N: usize = 2 * F + 1;

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

/// Send a PUT and wait for OK response matching the nonce.
async fn send_put_and_confirm(
    sender: &mut (impl futures::Sink<String, Error = impl std::fmt::Debug> + Unpin),
    receiver: &mut (impl futures::Stream<Item = String> + Unpin),
    key: &str, value: &str, nonce: u64,
) -> bool {
    let cmd = format!("PUT:{}:{}:{}", key, value, nonce);
    println!("  SENDING: {}", cmd);
    sender.send(cmd).await.unwrap();
    let nonce_str = format!("nonce={}", nonce);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() { return false; }
        match tokio::time::timeout(remaining, receiver.next()).await {
            Ok(Some(r)) => {
                if r.contains(&nonce_str) { println!("    OK: {}", r); return true; }
            }
            _ => return false,
        }
    }
}

/// Send a GET and wait for response matching the nonce. Returns the response or None.
async fn send_get(
    sender: &mut (impl futures::Sink<String, Error = impl std::fmt::Debug> + Unpin),
    receiver: &mut (impl futures::Stream<Item = String> + Unpin),
    key: &str, nonce: u64,
) -> Option<String> {
    let cmd = format!("GET:{}:{}", key, nonce);
    sender.send(cmd).await.unwrap();
    let nonce_str = format!("nonce={}", nonce);
    match tokio::time::timeout(std::time::Duration::from_millis(2000), async {
        loop {
            match receiver.next().await {
                Some(r) if r.contains(&nonce_str) => return r,
                Some(_) => continue,
                None => return String::new(),
            }
        }
    }).await {
        Ok(r) if !r.is_empty() => Some(r),
        _ => None,
    }
}

/// Find an EC2 instance by name pattern and state.
async fn find_instance(region: &str, name_pattern: &str, state: &str) -> Option<String> {
    let out = tokio::process::Command::new("aws")
        .args(["ec2", "describe-instances", "--region", region,
            "--filters",
            &format!("Name=tag:Name,Values=*{}*", name_pattern),
            &format!("Name=instance-state-name,Values={}", state),
            "Name=vpc-id,Values=vpc-041b334556d749bfb",
            "--query", "Reservations[0].Instances[0].InstanceId",
            "--output", "text", "--no-paginate", "--no-cli-pager"])
        .output().await.unwrap();
    let id = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if id.is_empty() || id == "None" { None } else { Some(id) }
}

async fn stop_instance(region: &str, id: &str) {
    tokio::process::Command::new("aws")
        .args(["ec2", "stop-instances", "--region", region, "--instance-ids", id, "--no-paginate", "--no-cli-pager"])
        .output().await.unwrap();
    tokio::process::Command::new("aws")
        .args(["ec2", "wait", "instance-stopped", "--region", region, "--instance-ids", id])
        .output().await.unwrap();
}

async fn start_instance(region: &str, id: &str) {
    tokio::process::Command::new("aws")
        .args(["ec2", "start-instances", "--region", region, "--instance-ids", id, "--no-paginate", "--no-cli-pager"])
        .output().await.unwrap();
    tokio::process::Command::new("aws")
        .args(["ec2", "wait", "instance-running", "--region", region, "--instance-ids", id])
        .output().await.unwrap();
}

#[tokio::main]
async fn main() {
    let region = "us-east-1";
    let ami = "ami-0e95a5e2743ec9ec9";
    let instance_type = "t3.micro";
    let rustflags = "-C opt-level=3 -C codegen-units=1";

    println!("=== EC2 Reconciliation Test (f={}, N={}) ===", F, N);
    println!("Tests that a rejoining node reconciles writes it missed.\n");

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
        let mut nonce: u64 = 1;

        // === Phase 1: PUT k1..k3 (all nodes see them) ===
        println!("\n=== Phase 1: PUT k1..k3 (all nodes alive) ===");
        for i in 1..=3u64 {
            if !send_put_and_confirm(&mut sender, &mut receiver, &format!("k{}", i), &format!("v{}", i), nonce).await {
                println!("FAIL phase 1: PUT k{} timed out", i);
                std::process::exit(1);
            }
            nonce += 1;
        }
        println!("Phase 1 PASS: k1..k3 written");

        // === Phase 2: Kill node 0 (primary) ===
        println!("\n=== Phase 2: Kill node 0 (primary) ===");
        let node0_id = find_instance(region, "node-0", "running").await
            .unwrap_or_else(|| { println!("FAIL: cannot find node-0"); std::process::exit(1); });
        println!("  Stopping instance {}...", node0_id);
        stop_instance(region, &node0_id).await;
        println!("  Node 0 stopped.");

        // Wait for failover
        println!("  Waiting for failover...");
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
        let mut failover_ok = false;
        while tokio::time::Instant::now() < deadline {
            if let Some(r) = send_get(&mut sender, &mut receiver, "k1", nonce).await {
                println!("  Failover confirmed: {}", r);
                failover_ok = true;
                break;
            }
            nonce += 1;
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
        if !failover_ok { println!("FAIL phase 2: failover did not complete"); std::process::exit(1); }
        nonce += 1;
        println!("Phase 2 PASS: failover complete");

        // === Phase 3: PUT k4..k6 while node 0 is down ===
        println!("\n=== Phase 3: PUT k4..k6 (node 0 is DOWN) ===");
        for i in 4..=6u64 {
            if !send_put_and_confirm(&mut sender, &mut receiver, &format!("k{}", i), &format!("v{}", i), nonce).await {
                println!("FAIL phase 3: PUT k{} timed out", i);
                std::process::exit(1);
            }
            nonce += 1;
        }
        println!("Phase 3 PASS: k4..k6 written while node 0 was down");

        // === Phase 4: Restart node 0 ===
        println!("\n=== Phase 4: Restart node 0 ===");
        let node0_stopped = find_instance(region, "node-0", "stopped").await
            .unwrap_or_else(|| { println!("FAIL: cannot find stopped node-0"); std::process::exit(1); });
        println!("  Starting instance {}...", node0_stopped);
        start_instance(region, &node0_stopped).await;
        println!("  Node 0 running. Waiting 15s for reconnect + reconciliation...");
        tokio::time::sleep(std::time::Duration::from_secs(15)).await;
        println!("Phase 4 PASS: node 0 restarted");

        // === Phase 5: Kill node 1 (current primary), then GET k4..k6 from node 0 ===
        println!("\n=== Phase 5: Kill node 1, verify node 0 has k4..k6 ===");
        let node1_id = find_instance(region, "node-1", "running").await
            .unwrap_or_else(|| { println!("FAIL: cannot find node-1"); std::process::exit(1); });
        println!("  Stopping instance {}...", node1_id);
        stop_instance(region, &node1_id).await;
        println!("  Node 1 stopped.");

        // Now only nodes 0 and 2 are alive. If node 0 reconciled, it has k4..k6.
        println!("  Verifying k4..k6 are accessible...");
        let mut all_ok = true;
        for i in 4..=6u64 {
            let key = format!("k{}", i);
            let expected_value = format!("v{}", i);
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
            let mut found = false;
            while tokio::time::Instant::now() < deadline {
                if let Some(r) = send_get(&mut sender, &mut receiver, &key, nonce).await {
                    if r.contains(&expected_value) {
                        println!("    GET {} => {} ✓", key, r);
                        found = true;
                        break;
                    } else {
                        println!("    GET {} => {} (wrong value!)", key, r);
                    }
                }
                nonce += 1;
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            if !found {
                println!("    FAIL: GET {} did not return expected value '{}'", key, expected_value);
                all_ok = false;
            }
            nonce += 1;
        }

        if all_ok {
            println!("\nPhase 5 PASS: node 0 reconciled missed writes — k4..k6 accessible");
            println!("\n=== ALL PHASES PASSED ===");
        } else {
            println!("\nPhase 5 FAIL: reconciliation did not work — node 0 missing writes from while it was down");
            std::process::exit(1);
        }
    }
}
