//! End-to-end failover test: PUT keys, kill primary, verify GET still works.

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
use lego_replicate::messages::TransparentReplica;
#[cfg(stageleft_runtime)]
use lego_replicate::{Router, ReplicateConfig, View};

const F: usize = 1;
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
        commit_timeout_ms: 2000,
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

#[tokio::test]
async fn failover_get_after_primary_kill() {
    use futures::{SinkExt, StreamExt};
    use hydro_deploy::Deployment;
    use hydro_lang::deploy::TrybuildHost;

    let mut deployment = Deployment::new();
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
        .with_cluster(&replicas, (0..N).map(|_| TrybuildHost::new(deployment.Localhost()).features(features.clone())))
        .with_cluster(&proposers, (0..N).map(|_| TrybuildHost::new(deployment.Localhost()).features(features.clone())))
        .with_cluster(&acceptors, (0..N).map(|_| TrybuildHost::new(deployment.Localhost()).features(features.clone())))
        .with_process(&router, TrybuildHost::new(deployment.Localhost()).features(features.clone()))
        .deploy(&mut deployment);

    deployment.deploy().await.unwrap();

    #[cfg(stageleft_runtime)]
    let mut sender = nodes.connect(cmd_sink).await;
    #[cfg(stageleft_runtime)]
    let mut receiver = nodes.connect(resp_stream).await;

    deployment.start().await.unwrap();

    #[cfg(stageleft_runtime)]
    {
        // PUT a key and confirm
        sender.send("PUT:x:hello:1".to_string()).await.unwrap();
        let resp = tokio::time::timeout(std::time::Duration::from_secs(15), receiver.next()).await;
        assert!(resp.is_ok() && resp.unwrap().is_some(), "PUT before kill failed");
        println!("PUT OK before kill");

        // Kill primary (replica 0, proposer 0)
        println!("Killing primary...");
        nodes.get_cluster(&replicas).members()[0].underlying().stop().await.unwrap();
        nodes.get_cluster(&proposers).members()[0].underlying().stop().await.unwrap();
        println!("Primary killed.");

        // Send GETs repeatedly until one gets through (view change takes time)
        println!("Sending GETs after kill...");
        let resp = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let mut nonce = 2u64;
            loop {
                sender.send(format!("GET:x:{}", nonce)).await.unwrap();
                match tokio::time::timeout(std::time::Duration::from_millis(500), async {
                    while let Some(r) = receiver.next().await {
                        println!("  got: {}", r);
                        if r.contains(&format!("nonce={}", nonce)) {
                            return Some(r);
                        }
                    }
                    None
                }).await {
                    Ok(Some(r)) => return Some(r),
                    _ => {
                        nonce += 1;
                        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    }
                }
            }
        }).await;

        match resp {
            Ok(Some(r)) => {
                println!("PASS: failover recovered. Response: {}", r);
                assert!(r.contains("x=hello"), "Expected value 'hello', got: {}", r);
            }
            Ok(None) => panic!("FAIL: stream ended without response"),
            Err(_) => panic!("FAIL: 30s timeout — failover did not recover"),
        }
    }
}
