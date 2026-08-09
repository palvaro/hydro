use hydro_lang::compile::builder::FlowBuilder;
use hydro_lang::determination::analyze_depth;
use hydro_lang::prelude::*;
use hydro_test::cluster::paxos::{CorePaxos, PaxosConfig};
use hydro_test::cluster::paxos_bench;
use stageleft::q;

#[test]
fn paxos_determination_depth() {
    let mut builder = FlowBuilder::new();
    let proposers = builder.cluster();
    let acceptors = builder.cluster();
    let clients = builder.cluster();
    let client_aggregator = builder.process();
    let replicas = builder.cluster();

    paxos_bench::paxos_bench(
        1000, // checkpoint_frequency
        1,    // f
        2,    // f + 1
        CorePaxos {
            proposers: proposers.clone(),
            acceptors: acceptors.clone(),
            paxos_config: PaxosConfig {
                f: 1,
                i_am_leader_send_timeout: 5,
                i_am_leader_check_timeout: 10,
                i_am_leader_check_timeout_delay_multiplier: 15,
            },
        },
        &clients,
        clients.singleton(q!(100usize)),
        &client_aggregator,
        &replicas,
        100,
        1000,
        hydro_std::bench_client::pretty_print_bench_results,
    );

    let built = builder.finalize();
    let result = analyze_depth(built.ir());

    println!("=== PAXOS DETERMINATION DEPTH ===");
    println!("  nondet_points: {} found", result.nondet_points.len());
    let absorbed_count = result.nondet_points.iter().filter(|p| p.absorbed).count();
    let genuine_count = result.genuine_commitments.len();
    println!("  absorbed: {}", absorbed_count);
    println!("  genuine_commitments: {}", genuine_count);
    println!("  dependency_edges: {} edges", result.dependency_edges.len());
    println!("  layers: {} layers", result.layers.len());
    for (i, layer) in result.layers.iter().enumerate() {
        println!("    layer {}: {} commitments", i, layer.len());
    }
    println!("  depth: {} (unbounded: {})", result.depth, result.unbounded);
    println!();
    println!("  === DEPENDENCY CHAINS (longest path) ===");
    // Print the longest dependency chain
    // Find a node in the last layer and trace back
    if let Some(last_layer) = result.layers.last() {
        if let Some(&deepest) = last_layer.first() {
            println!("  Deepest commitment: id={}", deepest);
            // Trace predecessors
            let mut current = deepest;
            let mut chain = vec![current];
            loop {
                // Find a predecessor
                let pred = result.dependency_edges.iter()
                    .find(|(_, b)| *b == current)
                    .map(|(a, _)| *a);
                if let Some(p) = pred {
                    chain.push(p);
                    current = p;
                } else {
                    break;
                }
            }
            chain.reverse();
            println!("  Chain (length {}): {:?}", chain.len(), chain);
        }
    }
    println!("=================================");
}
