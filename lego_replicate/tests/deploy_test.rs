//! Deploy test: run the lego-replicate protocol end-to-end.
//!
//! Sources commands at replicas, runs the protocol, sends committed
//! results to an applier process, verifies correct output via stdout.

#[cfg(stageleft_runtime)]
use hydro_lang::live_collections::stream::NoOrder;
#[cfg(stageleft_runtime)]
use hydro_lang::prelude::*;
#[cfg(stageleft_runtime)]
use hydro_test::cluster::paxos::{Acceptor, Proposer};
#[cfg(stageleft_runtime)]
use lego_replicate::messages::TransparentReplica;
#[cfg(stageleft_runtime)]
use lego_replicate::{ReplicateConfig, View};

struct Applier;

#[cfg(stageleft_runtime)]
fn build_pipeline<'a>(
    replicas: &Cluster<'a, TransparentReplica>,
    proposers: &Cluster<'a, Proposer>,
    acceptors: &Cluster<'a, Acceptor>,
    applier: &Process<'a, Applier>,
) {
    let config = ReplicateConfig::default();

    // Source commands at all replicas (only primary will sequence them)
    let commands: Stream<Vec<u8>, _, Unbounded> = replicas
        .source_iter(q!({
            vec![
                bincode::serialize(&"PUT:x:1".to_string()).unwrap(),
                bincode::serialize(&"PUT:y:7".to_string()).unwrap(),
                bincode::serialize(&"GET:y".to_string()).unwrap(),
                bincode::serialize(&"GET:x".to_string()).unwrap(),
            ]
        }))
        .into();

    let no_proposals: Stream<View, _, Unbounded, NoOrder> = replicas
        .source_iter(q!(Vec::<View>::new()))
        .weaken_ordering::<NoOrder>()
        .into();

    let output = lego_replicate::protocol::compose_protocol(
        replicas, proposers, acceptors, commands, no_proposals, config,
    );

    // Send committed results to applier process
    let at_applier = output.committed_in_order
        .send(applier, TCP.fail_stop().bincode())
        .values();

    // Applier: deserialize and apply commands to a HashMap
    let applier_tick = applier.tick();
    at_applier
        .batch(&applier_tick, nondet!(/** applier batch */))
        .sort()
        .across_ticks(|s| s.fold(
            q!(|| std::collections::HashMap::<String, String>::new()),
            q!(|store: &mut std::collections::HashMap<String, String>, (seq, payload): (usize, Vec<u8>)| {
                let cmd: String = bincode::deserialize(&payload).unwrap();
                let parts: Vec<&str> = cmd.splitn(3, ':').collect();
                let response = match parts[0] {
                    "PUT" => {
                        let key = parts[1].to_string();
                        let value = parts[2].to_string();
                        store.insert(key.clone(), value.clone());
                        format!("[RESULT] seq={} PUT {}={}", seq, key, value)
                    }
                    "GET" => {
                        let key = parts[1].to_string();
                        let value = store.get(&key).cloned().unwrap_or("(nil)".to_string());
                        format!("[RESULT] seq={} GET {}={}", seq, key, value)
                    }
                    _ => format!("[RESULT] seq={} UNKNOWN", seq),
                };
                println!("{}", response);
            }),
        ))
        .all_ticks()
        .for_each(q!(|_| {}));
}

#[tokio::test]
async fn deploy_put_then_get() {
    use hydro_deploy::Deployment;
    use hydro_lang::deploy::{TrybuildHost, DeployCrateWrapper};

    let mut builder = FlowBuilder::new();
    let replicas = builder.cluster::<TransparentReplica>();
    let proposers = builder.cluster::<Proposer>();
    let acceptors = builder.cluster::<Acceptor>();
    let applier = builder.process::<Applier>();

    #[cfg(stageleft_runtime)]
    build_pipeline(&replicas, &proposers, &acceptors, &applier);

    let mut deployment = Deployment::new();

    let nodes = builder
        .with_cluster(&replicas, (0..3).map(|_| TrybuildHost::new(deployment.Localhost())))
        .with_cluster(&proposers, (0..3).map(|_| TrybuildHost::new(deployment.Localhost())))
        .with_cluster(&acceptors, (0..3).map(|_| TrybuildHost::new(deployment.Localhost())))
        .with_process(&applier, TrybuildHost::new(deployment.Localhost()))
        .deploy(&mut deployment);

    deployment.deploy().await.unwrap();

    let applier_node = nodes.get_process(&applier);
    let mut stdout = applier_node.stdout_filter("[RESULT]");

    deployment.start().await.unwrap();

    let mut responses: Vec<String> = Vec::new();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(30),
        async {
            while let Some(line) = stdout.recv().await {
                println!("  {}", line);
                responses.push(line);
                if responses.len() >= 4 { break; }
            }
        },
    ).await;

    assert!(result.is_ok(), "Timed out. Got {} responses: {:?}", responses.len(), responses);
    assert!(responses.len() >= 4, "Expected 4 responses, got {}: {:?}", responses.len(), responses);

    // Verify GETs return correct values
    let get_y = responses.iter().any(|r| r.contains("GET y=7"));
    let get_x = responses.iter().any(|r| r.contains("GET x=1"));
    assert!(get_y, "GET y should return 7: {:?}", responses);
    assert!(get_x, "GET x should return 1: {:?}", responses);

    println!("PASS: lego-replicate delivers commands correctly");
}
