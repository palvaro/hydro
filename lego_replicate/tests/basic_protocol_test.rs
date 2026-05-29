//! Basic integration test: verify the lego-replicate protocol compiles
//! a valid dataflow graph.

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

/// Verify the protocol compiles into a valid IR (dataflow graph).
/// This proves the composition is structurally correct.
#[test]
fn protocol_ir_compiles() {
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

    // Consume the output so the graph is complete
    output.committed_in_order.for_each(q!(|_| {}));

    let built = flow.finalize();
    let ir = built.ir();
    assert!(!ir.is_empty(), "IR should be non-empty");

    // Emit IR JSON for coord-analysis
    let json = built.ir_json().expect("failed to serialize IR");
    let out_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/lego_replicate_ir.json");
    std::fs::create_dir_all(out_path.parent().unwrap()).ok();
    std::fs::write(&out_path, &json).unwrap();
    println!("IR compiled successfully ({} roots, {} bytes JSON)", ir.len(), json.len());
    println!("Run: hydro-coord {}", out_path.display());
}
