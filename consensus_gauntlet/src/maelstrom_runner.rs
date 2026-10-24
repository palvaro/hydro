//! Execution of ready Maelstrom plan cells.

use std::path::Path;

use hydro_lang::deploy::maelstrom::deploy_maelstrom::{MaelstromClusterSpec, MaelstromDeployment};
use hydro_lang::deploy::maelstrom::maelstrom_bidi_clients;
use hydro_lang::prelude::*;

use crate::backend::BackendId;
use crate::maelstrom::{LinKvConfig, Nemesis, PlannedRun, plan_for_backend};

/// Run every supported rung for one backend. Capability gaps are returned to
/// the caller and never silently replaced by an easier workload.
pub fn run_backend(backend: BackendId, maelstrom_path: &Path) -> Result<Vec<PlannedRun>, String> {
    let plan = plan_for_backend(backend);
    for run in &plan.runs {
        if let PlannedRun::Ready(config) = run {
            run_one(backend, maelstrom_path, config)?;
        }
    }
    Ok(plan.runs)
}

/// Run one supported rung for a backend.
pub fn run_rung(
    backend: BackendId,
    maelstrom_path: &Path,
    config: &LinKvConfig,
) -> Result<(), String> {
    run_one(backend, maelstrom_path, config)
}

fn run_one(backend: BackendId, maelstrom_path: &Path, config: &LinKvConfig) -> Result<(), String> {
    use hydro_test::maelstrom::lin_kv::{
        lin_kv_server, quorum_ladder_lin_kv_server, raft_lin_kv_server,
    };

    let mut flow = FlowBuilder::with_name(format!(
        "consensus-gauntlet-maelstrom-{}-{}",
        backend,
        config.rung.as_str()
    ));
    let cluster = flow.cluster::<()>();
    let (input, output) = maelstrom_bidi_clients(&cluster);
    match backend {
        BackendId::Raft => {
            if config.nemesis == Some(Nemesis::Partition) {
                output.complete(raft_lin_kv_server(
                    &cluster,
                    config.node_count,
                    input,
                    || TCP.lossy_delayed_forever().bincode(),
                ));
            } else {
                output.complete(raft_lin_kv_server(
                    &cluster,
                    config.node_count,
                    input,
                    || TCP.fail_stop().bincode(),
                ));
            }
        }
        BackendId::BroadcastTranscript => {
            if config.nemesis == Some(Nemesis::Partition) {
                output.complete(lin_kv_server(&cluster, config.node_count, input, || {
                    TCP.lossy_delayed_forever().bincode()
                }));
            } else {
                output.complete(lin_kv_server(&cluster, config.node_count, input, || {
                    TCP.fail_stop().bincode()
                }));
            }
        }
        BackendId::QuorumLadderConsensus => output.complete(quorum_ladder_lin_kv_server(
            &cluster,
            config.node_count,
            input,
        )),
        BackendId::LibraryPaxos
        | BackendId::CompartmentalizedPaxos
        | BackendId::PaxosEc
        | BackendId::TypedConsensus => {
            return Err(format!("{} is a declared Maelstrom adapter gap", backend));
        }
    }

    let mut deployment = MaelstromDeployment::new("lin-kv")
        .maelstrom_path(maelstrom_path)
        .node_count(config.node_count)
        .time_limit(config.time_limit_seconds)
        .rate(config.rate as u64)
        .extra_args(config.extra_args());
    if let Some(nemesis) = config.nemesis {
        deployment = deployment.nemesis(nemesis.as_str());
    }
    let _ = flow
        .with_cluster(&cluster, MaelstromClusterSpec)
        .deploy(&mut deployment);
    deployment
        .run_repeated(config.repetitions)
        .map_err(|error| error.to_string())
}
