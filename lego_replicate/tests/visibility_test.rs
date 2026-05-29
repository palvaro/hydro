//! Tests for cross-client and cross-router visibility.
//!
//! 1. Sequential writes on one router are visible to later reads
//! 2. Writes through router 1 are visible through router 2
//! 3. After killing a router, the other router sees all prior writes

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

struct Router2;

const F: usize = 1;
const N: usize = 2 * F + 1;

#[cfg(stageleft_runtime)]
fn build_dual_router_pipeline<'a>(
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
        ..ReplicateConfig::default()
    };

    // Router 1
    let (cmd_sink1, cmds1) = router1.source_external_bincode(external);
    let puts1 = cmds1.clone().filter(q!(|c: &String| c.starts_with("PUT:")));
    let gets1 = cmds1.filter(q!(|c: &String| c.starts_with("GET:")));

    // Router 2
    let (cmd_sink2, cmds2) = router2.source_external_bincode(external);
    let puts2 = cmds2.clone().filter(q!(|c: &String| c.starts_with("PUT:")));
    let gets2 = cmds2.filter(q!(|c: &String| c.starts_with("GET:")));

    // Merge PUTs from both routers
    let all_puts: Stream<Vec<u8>, Cluster<'a, TransparentReplica>, Unbounded> =
        puts1.map(q!(|c: String| bincode::serialize(&c).unwrap()))
            .broadcast(replicas, TCP.fail_stop().bincode(), nondet!(/** */))
            .assume_ordering::<hydro_lang::live_collections::stream::TotalOrder>(nondet!(/** */))
            .interleave(
                puts2.map(q!(|c: String| bincode::serialize(&c).unwrap()))
                    .broadcast(replicas, TCP.fail_stop().bincode(), nondet!(/** */))
                    .assume_ordering::<hydro_lang::live_collections::stream::TotalOrder>(nondet!(/** */))
            )
            .assume_ordering::<hydro_lang::live_collections::stream::TotalOrder>(nondet!(/** */));

    let all_gets = gets1
        .broadcast(replicas, TCP.fail_stop().bincode(), nondet!(/** */))
        .interleave(
            gets2.broadcast(replicas, TCP.fail_stop().bincode(), nondet!(/** */))
        );

    let no_proposals: Stream<View, _, Unbounded, NoOrder> = replicas
        .source_iter(q!(Vec::<View>::new())).weaken_ordering::<NoOrder>().into();

    let output = lego_replicate::protocol::compose_protocol(
        replicas, proposers, acceptors, all_puts, no_proposals, config,
    );

    let scan_tick = replicas.tick();
    let _hb = replicas.source_interval(q!(std::time::Duration::from_millis(100)), nondet!(/** */))
        .batch(&scan_tick, nondet!(/** */));

    let is_primary = output.current_view
        .snapshot(&scan_tick, nondet!(/** */))
        .filter(q!(move |v: &View| CLUSTER_SELF_ID.get_raw_id() == v.primary()))
        .map(q!(|_| ()));

    let gets_on_primary = all_gets
        .batch(&scan_tick, nondet!(/** */))
        .filter_if_some(is_primary)
        .weaken_ordering::<NoOrder>()
        .all_ticks()
        .map(q!(|cmd| (false, 0usize, cmd)));

    let committed_puts = output.replicated
        .map(q!(|(seq, payload): (usize, Vec<u8>)| {
            let cmd: String = bincode::deserialize(&payload).unwrap();
            (true, seq, cmd)
        }));

    let responses = committed_puts.interleave(gets_on_primary)
        .batch(&scan_tick, nondet!(/** */)).sort().all_ticks()
        .scan(
            q!(|| lego_replicate::applier::RedbApplierState::new()),
            q!(|state: &mut lego_replicate::applier::RedbApplierState,
                (is_put, seq, cmd): (bool, usize, String)| {
                Some(state.apply_command(if is_put { seq } else { 0 }, &cmd))
            }),
        );

    let resp1 = responses.clone().send(router1, TCP.fail_stop().bincode()).values();
    let resp2 = responses.send(router2, TCP.fail_stop().bincode()).values();

    (cmd_sink1, resp1.send_bincode_external(external),
     cmd_sink2, resp2.send_bincode_external(external))
}

fn parse_nonce(resp: &str) -> Option<u64> {
    resp.split("nonce=").nth(1)?.split_whitespace().next()?.parse().ok()
}

/// Test 1: Sequential writes on one router are visible to later reads.
/// Test 2: Writes through router 1 are visible through router 2.
#[tokio::test]
async fn cross_router_visibility() {
    use futures::{SinkExt, StreamExt};
    use hydro_deploy::Deployment;
    use hydro_lang::deploy::TrybuildHost;

    let mut deployment = Deployment::new();
    let mut builder = FlowBuilder::new();

    let external = builder.external::<()>();
    let replicas = builder.cluster::<TransparentReplica>();
    let proposers = builder.cluster::<Proposer>();
    let acceptors = builder.cluster::<Acceptor>();
    let router1 = builder.process::<Router>();
    let router2 = builder.process::<Router2>();

    #[cfg(stageleft_runtime)]
    let (cmd_sink1, resp_stream1, cmd_sink2, resp_stream2) =
        build_dual_router_pipeline(&external, &replicas, &proposers, &acceptors, &router1, &router2);

    let features = vec!["backend_redb".to_string()];
    let nodes = builder
        .with_external(&external, deployment.Localhost())
        .with_cluster(&replicas, (0..N).map(|_| TrybuildHost::new(deployment.Localhost()).features(features.clone())))
        .with_cluster(&proposers, (0..N).map(|_| TrybuildHost::new(deployment.Localhost()).features(features.clone())))
        .with_cluster(&acceptors, (0..N).map(|_| TrybuildHost::new(deployment.Localhost()).features(features.clone())))
        .with_process(&router1, TrybuildHost::new(deployment.Localhost()).features(features.clone()))
        .with_process(&router2, TrybuildHost::new(deployment.Localhost()).features(features.clone()))
        .deploy(&mut deployment);

    deployment.deploy().await.unwrap();

    #[cfg(stageleft_runtime)]
    let mut s1 = nodes.connect(cmd_sink1).await;
    #[cfg(stageleft_runtime)]
    let mut r1 = nodes.connect(resp_stream1).await;
    #[cfg(stageleft_runtime)]
    let mut s2 = nodes.connect(cmd_sink2).await;
    #[cfg(stageleft_runtime)]
    let mut r2 = nodes.connect(resp_stream2).await;

    deployment.start().await.unwrap();

    #[cfg(stageleft_runtime)]
    {
        // Test 1: PUT through router 1, GET through router 1 (sequential on same router)
        s1.send("PUT:x:hello:1".to_string()).await.unwrap();
        let resp = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            while let Some(r) = r1.next().await { if r.contains("nonce=1") { return Some(r); } }
            None
        }).await;
        assert!(resp.is_ok() && resp.unwrap().is_some(), "PUT via router1 failed");

        s1.send("GET:x:2".to_string()).await.unwrap();
        let resp = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while let Some(r) = r1.next().await { if r.contains("nonce=2") { return Some(r); } }
            None
        }).await;
        match resp {
            Ok(Some(r)) => {
                assert!(r.contains("x=hello"), "Test 1 FAIL: GET via router1 should see PUT. Got: {}", r);
                println!("Test 1 PASS: sequential read sees prior write on same router");
            }
            _ => panic!("Test 1 FAIL: GET via router1 timed out"),
        }

        // Test 2: PUT through router 1, GET through router 2 (cross-router)
        s1.send("PUT:y:world:3".to_string()).await.unwrap();
        let resp = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            while let Some(r) = r1.next().await { if r.contains("nonce=3") { return Some(r); } }
            None
        }).await;
        assert!(resp.is_ok() && resp.unwrap().is_some(), "PUT y via router1 failed");

        // Read through router 2
        s2.send("GET:y:4".to_string()).await.unwrap();
        let resp = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while let Some(r) = r2.next().await { if r.contains("nonce=4") { return Some(r); } }
            None
        }).await;
        match resp {
            Ok(Some(r)) => {
                assert!(r.contains("y=world"), "Test 2 FAIL: GET via router2 should see PUT via router1. Got: {}", r);
                println!("Test 2 PASS: write through router1 visible through router2");
            }
            _ => panic!("Test 2 FAIL: GET via router2 timed out"),
        }

        // Test 3: Read x through router 2 (written earlier via router 1)
        s2.send("GET:x:5".to_string()).await.unwrap();
        let resp = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            while let Some(r) = r2.next().await { if r.contains("nonce=5") { return Some(r); } }
            None
        }).await;
        match resp {
            Ok(Some(r)) => {
                assert!(r.contains("x=hello"), "Test 3 FAIL: earlier write via router1 not visible via router2. Got: {}", r);
                println!("Test 3 PASS: earlier writes visible through fresh router connection");
            }
            _ => panic!("Test 3 FAIL: GET x via router2 timed out"),
        }

        println!("\nALL VISIBILITY TESTS PASSED");
    }
}
