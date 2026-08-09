use hydro_lang::compile::builder::FlowBuilder;
use hydro_lang::determination::analyze_depth;
use hydro_lang::live_collections::stream::NoOrder;
use hydro_lang::prelude::*;
use hydro_test::cluster::paxos::{Acceptor, Proposer};
use lego_replicate::messages::{TransparentReplica, View};
use lego_replicate::ReplicateConfig;
use stageleft::q;

#[test]
fn lego_replicate_determination_depth() {
    let mut flow = FlowBuilder::new();
    let replicas = flow.cluster::<TransparentReplica>();
    let proposers = flow.cluster::<Proposer>();
    let acceptors = flow.cluster::<Acceptor>();

    let config = ReplicateConfig::default();

    let (_, commands) = replicas.sim_input::<Vec<u8>>();

    let no_proposals: Stream<View, _, Unbounded, NoOrder> = replicas
        .source_iter(q!(Vec::<View>::new()))
        .weaken_ordering::<NoOrder>()
        .into();

    let output = lego_replicate::protocol::compose_protocol(
        &replicas, &proposers, &acceptors, commands, no_proposals, config,
    );

    output.committed_in_order.for_each(q!(|_| {}));

    let built = flow.finalize();
    let result = analyze_depth(built.ir());

    println!("=== LEGO REPLICATE DETERMINATION DEPTH ===");
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
    println!("==========================================");
}
