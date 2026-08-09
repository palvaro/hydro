use std::collections::HashSet;
use hydro_lang::compile::builder::FlowBuilder;
use hydro_lang::determination::analyze_depth;
use hydro_lang::prelude::*;
use stageleft::q;

/// The actual monotone_set_accumulation pattern.
/// We DO NOT add any sink — just let the IR contain whatever roots
/// the broadcast internally creates. This avoids polluting the output
/// path with test harness nondet.
#[test]
fn actual_example1_monotone_accumulation() {
    let mut flow = FlowBuilder::new();
    let cluster = flow.cluster::<()>();

    let (_send, local_values) = cluster.sim_input::<i32>();

    // Broadcast to all peers (clone because broadcast consumes)
    let received = local_values.clone().broadcast_bincode(&cluster, nondet!(/** stable membership */));

    // Sliced: batch both local and received, chain, fold into set
    let _accumulated = sliced! {
        let local_batch = use(local_values, nondet!(/** local delivery */));
        let recv_batch = use(received, nondet!(/** remote delivery */));

        local_batch.chain(recv_batch.values()).fold(
            q!(|| HashSet::<i32>::new()),
            q!(|set, value| { set.insert(value); },
               commutative = manual_proof!(/** set insert is commutative */)),
        )
    };

    // Don't consume _accumulated — just finalize and analyze whatever IR exists
    let built = flow.finalize();
    println!("  IR roots: {}", built.ir().len());
    let result = analyze_depth(built.ir());

    println!("=== ACTUAL EXAMPLE 1: monotone_set_accumulation (no test sink) ===");
    println!("  nondet_points: {} found", result.nondet_points.len());
    for p in &result.nondet_points {
        println!("    id={}, trusted={}, absorbed={}", p.id, p.trusted, p.absorbed);
    }
    println!("  genuine_commitments: {:?}", result.genuine_commitments);
    println!("  dependency_edges: {:?}", result.dependency_edges);
    println!("  layers: {:?}", result.layers);
    println!("  depth: {} (unbounded: {})", result.depth, result.unbounded);
    println!("===================================================");
}
