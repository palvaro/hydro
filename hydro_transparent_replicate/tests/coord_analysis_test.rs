//! Emit IR JSON for coord-analysis.

#[cfg(stageleft_runtime)]
use hydro_lang::prelude::*;
#[cfg(stageleft_runtime)]
use hydro_lang::live_collections::stream::NoOrder;
#[cfg(stageleft_runtime)]
use hydro_test::cluster::paxos::{Acceptor, Proposer};
#[cfg(stageleft_runtime)]
use hydro_transparent_replicate::messages::TransparentReplica;
#[cfg(stageleft_runtime)]
use hydro_transparent_replicate::{Coordinator, ReplicateConfig};

#[test]
fn emit_ir_for_coord_analysis() {
    let mut flow = FlowBuilder::new();
    let replicas = flow.cluster::<TransparentReplica>();
    let proposers = flow.cluster::<Proposer>();
    let acceptors = flow.cluster::<Acceptor>();

    let config = ReplicateConfig::default();

    let commands: Stream<String, _, Unbounded> = replicas
        .source_iter(q!(Vec::<String>::new()))
        .into();

    let no_proposals: Stream<hydro_transparent_replicate::View, _, Unbounded, NoOrder> = replicas
        .source_iter(q!(Vec::<hydro_transparent_replicate::View>::new()))
        .weaken_ordering::<NoOrder>()
        .into();

    let raw = hydro_transparent_replicate::protocol::replicate_service_raw::<String>(
        &replicas, &proposers, &acceptors, commands, no_proposals, config,
    );

    raw.committed.assume_ordering::<hydro_lang::live_collections::stream::TotalOrder>(nondet!(/** */)).for_each(q!(|_| {}));
    raw.replicated.assume_ordering::<hydro_lang::live_collections::stream::TotalOrder>(nondet!(/** */)).for_each(q!(|_| {}));
    raw.read_only.assume_ordering::<hydro_lang::live_collections::stream::TotalOrder>(nondet!(/** */)).for_each(q!(|_| {}));

    let built = flow.finalize();
    let json = built.ir_json().expect("failed to serialize IR");
    let out_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/transparent_replicate_ir.json");
    std::fs::create_dir_all(out_path.parent().unwrap()).ok();
    std::fs::write(&out_path, &json).unwrap();
    println!("IR: {} bytes at {}", json.len(), out_path.display());
}
