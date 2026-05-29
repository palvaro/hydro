//! Comprehensive failover scenario tests.
//!
//! Tests multiple interleavings of kills and commands to verify the coordinator
//! FD handles all cases correctly.

use hydro_deploy::Service;
use hydro_lang::deploy::DeployCrateWrapper;

#[cfg(stageleft_runtime)]
use hydro_lang::location::cluster::CLUSTER_SELF_ID;
#[cfg(stageleft_runtime)]
use hydro_lang::location::external_process::{ExternalBincodeSink, ExternalBincodeStream};
#[cfg(stageleft_runtime)]
use hydro_lang::prelude::*;
#[cfg(stageleft_runtime)]
use hydro_test::cluster::paxos::{Acceptor, Proposer};
#[cfg(stageleft_runtime)]
use hydro_transparent_replicate::messages::TransparentReplica;
#[cfg(stageleft_runtime)]
use hydro_transparent_replicate::protocol::{coordinator_failure_detector, replicate_service_raw};
#[cfg(stageleft_runtime)]
use hydro_transparent_replicate::{Coordinator, ReplicateConfig};

const F: usize = 2;
const N: usize = 2 * F + 1; // 5 replicas, tolerates 2 failures

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
        responses_at_coord.clone().weaken_ordering::<hydro_lang::live_collections::stream::NoOrder>(),
        raw.current_view, timeout_ms, initial_member_count,
    );

    proposals_complete.complete(fd_proposals);

    let resp_stream = responses_at_coord.send_bincode_external(external);
    (cmd_sink, resp_stream)
}

/// Helper: retry GETs until one succeeds.
async fn retry_get(
    sender: &mut (impl futures::Sink<String, Error = impl std::fmt::Debug> + Unpin),
    receiver: &mut (impl futures::Stream<Item = String> + Unpin),
    key: &str,
    start_nonce: &mut u64,
    timeout_secs: u64,
) -> Option<String> {
    use futures::{SinkExt, StreamExt};

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(timeout_secs);
    loop {
        if tokio::time::Instant::now() >= deadline {
            return None;
        }
        let nonce = *start_nonce;
        *start_nonce += 1;
        let cmd = format!("GET:{}:{}", key, nonce);
        sender.send(cmd).await.unwrap();

        let remaining = deadline - tokio::time::Instant::now();
        let nonce_str = format!("nonce={}", nonce);
        match tokio::time::timeout(std::time::Duration::from_millis(500).min(remaining), async {
            while let Some(r) = receiver.next().await {
                if r.contains(&nonce_str) {
                    return Some(r);
                }
            }
            None
        }).await {
            Ok(Some(r)) => return Some(r),
            _ => {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 1: Single failover (kill primary, verify recovery)
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn single_failover() {
    use futures::SinkExt;
    use futures::StreamExt;
    use hydro_deploy::Deployment;
    use hydro_lang::deploy::TrybuildHost;

    let mut deployment = Deployment::new();
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
        .with_cluster(&replicas, (0..N).map(|_| TrybuildHost::new(deployment.Localhost()).features(features.clone())))
        .with_cluster(&proposers, (0..N).map(|_| TrybuildHost::new(deployment.Localhost()).features(features.clone())))
        .with_cluster(&acceptors, (0..N).map(|_| TrybuildHost::new(deployment.Localhost()).features(features.clone())))
        .with_process(&coordinator, TrybuildHost::new(deployment.Localhost()).features(features.clone()))
        .deploy(&mut deployment);

    deployment.deploy().await.unwrap();

    #[cfg(stageleft_runtime)]
    let mut sender = nodes.connect(cmd_sink).await;
    #[cfg(stageleft_runtime)]
    let mut receiver = nodes.connect(resp_stream).await;

    deployment.start().await.unwrap();

    #[cfg(stageleft_runtime)]
    {
        // PUT and confirm
        sender.send("PUT:x:hello:1".to_string()).await.unwrap();
        let resp = tokio::time::timeout(std::time::Duration::from_secs(15), receiver.next()).await;
        assert!(resp.is_ok() && resp.unwrap().is_some(), "initial PUT failed");

        // Kill primary
        nodes.get_cluster(&replicas).members()[0].underlying().stop().await.unwrap();
        nodes.get_cluster(&proposers).members()[0].underlying().stop().await.unwrap();

        // Retry GETs until recovery
        let mut nonce = 2u64;
        let resp = retry_get(&mut sender, &mut receiver, "x", &mut nonce, 20).await;
        assert!(resp.is_some(), "single failover did not recover within 20s");
        assert!(resp.unwrap().contains("x=hello"));
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 2: Double failover (kill primary, recover, kill new primary)
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn double_failover() {
    use futures::SinkExt;
    use futures::StreamExt;
    use hydro_deploy::Deployment;
    use hydro_lang::deploy::TrybuildHost;

    let mut deployment = Deployment::new();
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
        .with_cluster(&replicas, (0..N).map(|_| TrybuildHost::new(deployment.Localhost()).features(features.clone())))
        .with_cluster(&proposers, (0..N).map(|_| TrybuildHost::new(deployment.Localhost()).features(features.clone())))
        .with_cluster(&acceptors, (0..N).map(|_| TrybuildHost::new(deployment.Localhost()).features(features.clone())))
        .with_process(&coordinator, TrybuildHost::new(deployment.Localhost()).features(features.clone()))
        .deploy(&mut deployment);

    deployment.deploy().await.unwrap();

    #[cfg(stageleft_runtime)]
    let mut sender = nodes.connect(cmd_sink).await;
    #[cfg(stageleft_runtime)]
    let mut receiver = nodes.connect(resp_stream).await;

    deployment.start().await.unwrap();

    #[cfg(stageleft_runtime)]
    {
        let mut nonce = 1u64;

        // PUT and confirm
        sender.send(format!("PUT:x:hello:{}", nonce)).await.unwrap();
        nonce += 1;
        let resp = tokio::time::timeout(std::time::Duration::from_secs(15), receiver.next()).await;
        assert!(resp.is_ok() && resp.unwrap().is_some(), "initial PUT failed");

        // Kill primary (replica 0)
        println!("Killing replica 0 (primary)...");
        nodes.get_cluster(&replicas).members()[0].underlying().stop().await.unwrap();
        nodes.get_cluster(&proposers).members()[0].underlying().stop().await.unwrap();

        // Wait for recovery
        let resp = retry_get(&mut sender, &mut receiver, "x", &mut nonce, 20).await;
        assert!(resp.is_some(), "first failover did not recover");
        assert!(resp.unwrap().contains("x=hello"));
        println!("First failover recovered.");

        // Kill new primary (replica 1)
        println!("Killing replica 1 (new primary)...");
        nodes.get_cluster(&replicas).members()[1].underlying().stop().await.unwrap();
        nodes.get_cluster(&proposers).members()[1].underlying().stop().await.unwrap();

        // Wait for second recovery
        let resp = retry_get(&mut sender, &mut receiver, "x", &mut nonce, 20).await;
        assert!(resp.is_some(), "second failover did not recover");
        assert!(resp.unwrap().contains("x=hello"));
        println!("Second failover recovered.");
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 3: No spurious view change during idle
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn no_spurious_during_idle() {
    use futures::SinkExt;
    use futures::StreamExt;
    use hydro_deploy::Deployment;
    use hydro_lang::deploy::TrybuildHost;

    let mut deployment = Deployment::new();
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
        .with_cluster(&replicas, (0..N).map(|_| TrybuildHost::new(deployment.Localhost()).features(features.clone())))
        .with_cluster(&proposers, (0..N).map(|_| TrybuildHost::new(deployment.Localhost()).features(features.clone())))
        .with_cluster(&acceptors, (0..N).map(|_| TrybuildHost::new(deployment.Localhost()).features(features.clone())))
        .with_process(&coordinator, TrybuildHost::new(deployment.Localhost()).features(features.clone()))
        .deploy(&mut deployment);

    deployment.deploy().await.unwrap();

    #[cfg(stageleft_runtime)]
    let mut sender = nodes.connect(cmd_sink).await;
    #[cfg(stageleft_runtime)]
    let mut receiver = nodes.connect(resp_stream).await;

    deployment.start().await.unwrap();

    #[cfg(stageleft_runtime)]
    {
        // PUT and confirm
        sender.send("PUT:x:hello:1".to_string()).await.unwrap();
        let resp = tokio::time::timeout(std::time::Duration::from_secs(15), receiver.next()).await;
        assert!(resp.is_ok() && resp.unwrap().is_some(), "initial PUT failed");

        // Wait 10 seconds idle (longer than timeout_ms=3000)
        println!("Waiting 10s idle...");
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;

        // GET should still work (no spurious view change broke things)
        sender.send("GET:x:2".to_string()).await.unwrap();
        let resp = tokio::time::timeout(std::time::Duration::from_secs(5), receiver.next()).await;
        match resp {
            Ok(Some(r)) => {
                assert!(r.contains("x=hello"), "GET after idle returned wrong value: {}", r);
                println!("PASS: no spurious view change during idle");
            }
            _ => panic!("GET after idle failed — spurious view change likely broke the system"),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Test 4: Command in flight during kill
// ─────────────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn command_in_flight_during_kill() {
    use futures::SinkExt;
    use futures::StreamExt;
    use hydro_deploy::Deployment;
    use hydro_lang::deploy::TrybuildHost;

    let mut deployment = Deployment::new();
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
        .with_cluster(&replicas, (0..N).map(|_| TrybuildHost::new(deployment.Localhost()).features(features.clone())))
        .with_cluster(&proposers, (0..N).map(|_| TrybuildHost::new(deployment.Localhost()).features(features.clone())))
        .with_cluster(&acceptors, (0..N).map(|_| TrybuildHost::new(deployment.Localhost()).features(features.clone())))
        .with_process(&coordinator, TrybuildHost::new(deployment.Localhost()).features(features.clone()))
        .deploy(&mut deployment);

    deployment.deploy().await.unwrap();

    #[cfg(stageleft_runtime)]
    let mut sender = nodes.connect(cmd_sink).await;
    #[cfg(stageleft_runtime)]
    let mut receiver = nodes.connect(resp_stream).await;

    deployment.start().await.unwrap();

    #[cfg(stageleft_runtime)]
    {
        // PUT and confirm (warms up the FD)
        sender.send("PUT:x:before:1".to_string()).await.unwrap();
        let resp = tokio::time::timeout(std::time::Duration::from_secs(15), receiver.next()).await;
        assert!(resp.is_ok() && resp.unwrap().is_some(), "initial PUT failed");

        // Send a PUT then immediately kill the primary
        sender.send("PUT:y:during:2".to_string()).await.unwrap();
        nodes.get_cluster(&replicas).members()[0].underlying().stop().await.unwrap();
        nodes.get_cluster(&proposers).members()[0].underlying().stop().await.unwrap();

        // The in-flight PUT may or may not succeed. Either way, the system should recover.
        // Retry GETs for key "x" (which was committed before the kill).
        let mut nonce = 3u64;
        let resp = retry_get(&mut sender, &mut receiver, "x", &mut nonce, 20).await;
        assert!(resp.is_some(), "system did not recover after kill during in-flight command");
        assert!(resp.unwrap().contains("x=before"));
    }
}
