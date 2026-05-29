//! Failover test: send PUTs, kill primary, verify recovery.

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

    let all_cmds_for_fd = cmds_at_coord.clone();

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
        all_cmds_for_fd.weaken_ordering::<hydro_lang::live_collections::stream::NoOrder>(),
        responses_at_coord.clone().weaken_ordering::<hydro_lang::live_collections::stream::NoOrder>(),
        raw.current_view, timeout_ms, initial_member_count,
    );
    proposals_complete.complete(fd_proposals);

    let resp_stream = responses_at_coord.send_bincode_external(external);
    (cmd_sink, resp_stream)
}

#[tokio::test]
async fn failover_after_primary_kill() {
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
        .with_cluster(
            &replicas,
            (0..N).map(|_| TrybuildHost::new(deployment.Localhost()).features(features.clone())),
        )
        .with_cluster(
            &proposers,
            (0..N).map(|_| TrybuildHost::new(deployment.Localhost()).features(features.clone())),
        )
        .with_cluster(
            &acceptors,
            (0..N).map(|_| TrybuildHost::new(deployment.Localhost()).features(features.clone())),
        )
        .with_process(
            &coordinator,
            TrybuildHost::new(deployment.Localhost()).features(features.clone()),
        )
        .deploy(&mut deployment);

    deployment.deploy().await.unwrap();

    #[cfg(stageleft_runtime)]
    let mut sender = nodes.connect(cmd_sink).await;
    #[cfg(stageleft_runtime)]
    let mut receiver = nodes.connect(resp_stream).await;

    deployment.start().await.unwrap();

    #[cfg(stageleft_runtime)]
    {
        // Step 1: PUT a key
        sender.send("PUT:x:hello:1".to_string()).await.unwrap();
        let resp = tokio::time::timeout(
            std::time::Duration::from_secs(15),
            receiver.next(),
        ).await;
        assert!(resp.is_ok(), "PUT timed out before kill");
        println!("PUT OK: {:?}", resp.unwrap());

        // Step 2: Kill primary (replica 0)
        println!("KILLING primary (replica 0)...");
        // In Hydro deploy, we can stop a specific cluster member
        // For now, just send a GET and see if it recovers
        // The primary is replica 0 — kill its process
        deployment.kill_process(&replicas, 0).await;
        println!("Primary killed.");

        // Step 3: Wait for failover, then try GET
        println!("Waiting for failover...");
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;

        sender.send("GET:x:2".to_string()).await.unwrap();
        let resp = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            receiver.next(),
        ).await;
        match resp {
            Ok(Some(r)) => println!("GET after failover: {}", r),
            Ok(None) => panic!("Stream ended — failover failed"),
            Err(_) => panic!("TIMEOUT — failover did not recover!"),
        }
    }
}
