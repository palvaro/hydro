//! EC2 multi-router test: deploys with 2 coordinators (Cluster<Coordinator>),
//! verifies the system works through both, then kills one and verifies the
//! system still works through the other.
//!
//! Run: AWS_ACCESS_KEY_ID=... cargo run -p hydro_transparent_replicate --features backend_redb --example ec2_multi_router_test

use std::sync::Arc;
use futures::SinkExt;
use futures::StreamExt;
use hydro_deploy::{Deployment, Host};
use hydro_lang::deploy::TrybuildHost;
use hydro_lang::location::cluster::CLUSTER_SELF_ID;
use hydro_lang::location::external_process::{ExternalBincodeBidi};
use hydro_lang::prelude::*;
use hydro_test::cluster::paxos::{Acceptor, Proposer};
use hydro_transparent_replicate::messages::TransparentReplica;
use hydro_transparent_replicate::protocol::{cluster_coordinator_failure_detector, replicate_service_raw};
use hydro_transparent_replicate::{Coordinator, ReplicateConfig};

const F: usize = 1;
const N: usize = 2 * F + 1;
const NUM_COORDINATORS: usize = 2;

#[cfg(stageleft_runtime)]
fn build_pipeline<'a>(
   external: &External<'a, ()>,
   replicas: &Cluster<'a, TransparentReplica>,
   proposers: &Cluster<'a, Proposer>,
   acceptors: &Cluster<'a, Acceptor>,
   coordinators: &Cluster<'a, Coordinator>,
) -> (
   ExternalBincodeBidi<String, String, hydro_lang::location::external_process::NotMany>,
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

    // Bidi connection: external sends String commands, receives String responses
    let (bidi_port, cmds_at_coord, resp_sink) =
        coordinators.bind_single_client_bincode::<_, String, String>(external);

    let cmds_for_fd = cmds_at_coord.clone()
        .weaken_ordering::<hydro_lang::live_collections::stream::NoOrder>();

    let puts_at_coord = cmds_at_coord.clone()
        .filter(q!(|cmd: &String| cmd.starts_with("PUT:")));
    let gets_at_coord = cmds_at_coord
        .filter(q!(|cmd: &String| cmd.starts_with("GET:")));

    // Each coordinator broadcasts PUTs/GETs to replicas
    let puts_at_replicas: Stream<String, Cluster<'a, TransparentReplica>, Unbounded> =
        puts_at_coord
            .broadcast(replicas, TCP.fail_stop().bincode(), nondet!(/** broadcast PUTs */))
            .values()
            .assume_ordering(nondet!(/** ordering */));

    let gets_at_replicas =
        gets_at_coord
            .broadcast(replicas, TCP.fail_stop().bincode(), nondet!(/** broadcast GETs */))
            .values();

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

    // Responses go to coordinators for FD and back to external via bidi
    let responses_at_coord: Stream<String, Cluster<'a, Coordinator>, Unbounded, hydro_lang::live_collections::stream::NoOrder> = responses
        .broadcast(coordinators, TCP.fail_stop().bincode(), nondet!(/** responses to coordinators */))
        .values()
        .weaken_ordering::<hydro_lang::live_collections::stream::NoOrder>();

    let fd_proposals = cluster_coordinator_failure_detector(
        coordinators, replicas,
        cmds_for_fd,
        responses_at_coord.clone(),
        raw.current_view, timeout_ms, initial_member_count,
    );
    proposals_complete.complete(fd_proposals);

    // Complete the bidi sink with responses (goes back to external)
    resp_sink.complete(
        responses_at_coord
            .assume_ordering::<hydro_lang::live_collections::stream::TotalOrder>(nondet!(/** bidi response ordering */))
            .assume_retries::<hydro_lang::live_collections::stream::ExactlyOnce>(nondet!(/** bidi response retries */))
    );

    (bidi_port,)
}

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

async fn send_put(
    sender: &mut (impl futures::Sink<String, Error = impl std::fmt::Debug> + Unpin),
    receiver: &mut (impl futures::Stream<Item = String> + Unpin),
    key: &str, value: &str, nonce: u64,
) -> bool {
    let cmd = format!("PUT:{}:{}:{}", key, value, nonce);
    println!("  SENDING: {}", cmd);
    if sender.send(cmd).await.is_err() { return false; }
    let nonce_str = format!("nonce={}", nonce);
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() { return false; }
        match tokio::time::timeout(remaining, receiver.next()).await {
            Ok(Some(r)) => { if r.contains(&nonce_str) { println!("    OK: {}", r); return true; } }
            _ => return false,
        }
    }
}

async fn send_get(
    sender: &mut (impl futures::Sink<String, Error = impl std::fmt::Debug> + Unpin),
    receiver: &mut (impl futures::Stream<Item = String> + Unpin),
    key: &str, nonce: u64,
) -> Option<String> {
    let cmd = format!("GET:{}:{}", key, nonce);
    if sender.send(cmd).await.is_err() { return None; }
    let nonce_str = format!("nonce={}", nonce);
    match tokio::time::timeout(std::time::Duration::from_secs(3), async {
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

#[tokio::main]
async fn main() {
    let region = "us-east-1";
    let ami = "ami-0e95a5e2743ec9ec9";
    let instance_type = "t3.micro";
    let rustflags = "-C opt-level=3 -C codegen-units=1";

    println!("=== EC2 Multi-Router Test (f={}, N={}, coordinators={}) ===", F, N, NUM_COORDINATORS);
    println!("Tests that multiple coordinators work simultaneously.\n");

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

    let coord_hosts: Vec<Arc<dyn Host>> = (0..NUM_COORDINATORS)
        .map(|i| -> Arc<dyn Host> {
            deployment.AwsEc2Host()
                .region(region).instance_type(instance_type).ami(ami)
                .network(existing_vpc.clone()).display_name(format!("coord-{}", i)).add()
        })
        .collect();

    let mut builder = FlowBuilder::new();
    let external = builder.external::<()>();
    let replicas = builder.cluster::<TransparentReplica>();
    let proposers = builder.cluster::<Proposer>();
    let acceptors = builder.cluster::<Acceptor>();
    let coordinators = builder.cluster::<Coordinator>();

    #[cfg(stageleft_runtime)]
    let (bidi_port,) =
        build_pipeline(&external, &replicas, &proposers, &acceptors, &coordinators);

    let features = vec!["backend_redb".to_string()];
    let nodes = builder
        .with_external(&external, deployment.Localhost())
        .with_cluster(&replicas, hosts.iter().map(|h|
            TrybuildHost::new(h.clone()).rustflags(rustflags).features(features.clone())))
        .with_cluster(&proposers, hosts.iter().map(|h|
            TrybuildHost::new(h.clone()).rustflags(rustflags).features(features.clone())))
        .with_cluster(&acceptors, hosts.iter().map(|h|
            TrybuildHost::new(h.clone()).rustflags(rustflags).features(features.clone())))
        .with_cluster(&coordinators, coord_hosts.iter().map(|h|
            TrybuildHost::new(h.clone()).rustflags(rustflags).features(features.clone())))
        .deploy(&mut deployment);

    println!("Deploying to EC2...");
    deployment.deploy().await.unwrap();

    #[cfg(stageleft_runtime)]
    let (mut receiver, mut sender) = nodes.connect_bincode(bidi_port).await;

    deployment.start().await.unwrap();
    println!("All processes started.");

    #[cfg(stageleft_runtime)]
    {
        let mut nonce: u64 = 1;

        // === Phase 1: PUT k1..k3 through both coordinators ===
        println!("\n=== Phase 1: PUT k1..k3 (both coordinators alive) ===");
        for i in 1..=3u64 {
            if !send_put(&mut sender, &mut receiver, &format!("k{}", i), &format!("v{}", i), nonce).await {
                println!("FAIL phase 1: PUT k{} timed out", i);
                std::process::exit(1);
            }
            nonce += 1;
        }
        println!("Phase 1 PASS: PUTs work with 2 coordinators");

        // === Phase 2: Kill coord-0 ===
        println!("\n=== Phase 2: Kill coord-0 ===");
        let coord0_id = find_instance(region, "coord-0", "running").await
            .unwrap_or_else(|| { println!("FAIL: cannot find coord-0"); std::process::exit(1); });
        println!("  coord-0 instance: {}", coord0_id);
        stop_instance(region, &coord0_id).await;
        println!("  coord-0 stopped.");

        // === Phase 3: Verify system still works through coord-1 ===
        println!("\n=== Phase 3: PUT k4..k6 and GET k1..k6 (only coord-1 alive) ===");

        let mut puts_ok = true;
        for i in 4..=6u64 {
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
            let mut ok = false;
            while tokio::time::Instant::now() < deadline {
                if send_put(&mut sender, &mut receiver, &format!("k{}", i), &format!("v{}", i), nonce).await {
                    ok = true;
                    break;
                }
                nonce += 1;
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
            if !ok {
                println!("  FAIL: PUT k{} did not succeed with one coordinator down", i);
                puts_ok = false;
                break;
            }
            nonce += 1;
        }

        if !puts_ok {
            println!("FAIL phase 3: PUTs failed with one coordinator down");
            std::process::exit(1);
        }
        println!("  PUTs k4..k6 succeeded");

        let mut gets_ok = true;
        for i in 1..=6u64 {
            let key = format!("k{}", i);
            let expected = format!("v{}", i);
            if let Some(r) = send_get(&mut sender, &mut receiver, &key, nonce).await {
                if r.contains(&expected) {
                    println!("    GET {} => {} ✓", key, expected);
                } else {
                    println!("    GET {} => WRONG: {}", key, r);
                    gets_ok = false;
                }
            } else {
                println!("    GET {} => no response", key);
                gets_ok = false;
            }
            nonce += 1;
        }

        if gets_ok {
            println!("\nPhase 3 PASS: system works with one coordinator down");
            println!("\n=== ALL PHASES PASSED ===");
        } else {
            println!("\nPhase 3 FAIL: system broken with one coordinator down");
            std::process::exit(1);
        }
    }
}
