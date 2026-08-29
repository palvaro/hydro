//! Deployment boundary for the performance tier.
//!
//! Flow construction is shared by localhost execution and ECS manifest export;
//! only placement differs. Metrics cross that boundary as prefixed JSON stdout,
//! so ECS task logs and localhost process logs use the same parser.

use std::time::Duration;

use hydro_deploy::Deployment;
use hydro_lang::deploy::{DeployCrateWrapper, TrybuildHost};
use hydro_lang::location::Location;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};
#[cfg(feature = "ecs")]
use std::path::{Path, PathBuf};

use crate::backend::BackendId;
use crate::perf::{
    METRIC_PREFIX, PerfConfig, PerfError, PerfSummary, SweepConfig, WindowMetrics,
    parse_metric_line,
};

const CLUSTER_SIZE: usize = 3;
const DEFAULT_CONCURRENCY: usize = 100;
const CLIENT_WINDOW_MILLIS: u64 = 100;
const AGGREGATE_WINDOW_MILLIS: u64 = 1_000;
const CHECKPOINT_FREQUENCY: usize = 1_000;
const COLLECTION_TIMEOUT: Duration = Duration::from_secs(90);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EcsRunSpec {
    pub backend: String,
    pub topology: String,
    pub replica_count: usize,
    pub metric_prefix: String,
    pub expected_windows: usize,
    pub warmup_windows: usize,
    pub steady_windows: usize,
    pub concurrency_env: String,
    pub sweep: SweepConfig,
}

fn build_common_flow<'a>(
    backend: BackendId,
    builder: &mut FlowBuilder<'a>,
) -> (
    Cluster<'a, hydro_test::cluster::consensus_bench::BenchClient>,
    Process<'a, hydro_test::cluster::consensus_bench::BenchAggregator>,
    Cluster<'a, hydro_test::cluster::raft::Replica>,
) {
    use hydro_test::cluster::consensus_bench::{
        broadcast_transcript_bench, quorum_ladder_bench, raft_bench,
    };

    let clients = builder.cluster();
    let aggregator = builder.process();
    let replicas = builder.cluster();
    let num_clients = clients.singleton(q!(std::env::var("GAUNTLET_CONCURRENCY_PER_NODE")
        .expect("GAUNTLET_CONCURRENCY_PER_NODE must be set by the gauntlet runner")
        .parse::<usize>()
        .expect("GAUNTLET_CONCURRENCY_PER_NODE must be a positive integer")));
    match backend {
        BackendId::Raft => raft_bench(
            &clients,
            num_clients,
            &aggregator,
            &replicas,
            CLUSTER_SIZE,
            CLIENT_WINDOW_MILLIS,
            AGGREGATE_WINDOW_MILLIS,
        ),
        BackendId::BroadcastTranscript => broadcast_transcript_bench(
            &clients,
            num_clients,
            &aggregator,
            &replicas,
            CLUSTER_SIZE,
            CLIENT_WINDOW_MILLIS,
            AGGREGATE_WINDOW_MILLIS,
        ),
        BackendId::QuorumLadderConsensus => quorum_ladder_bench(
            &clients,
            num_clients,
            &aggregator,
            &replicas,
            CLUSTER_SIZE,
            CLIENT_WINDOW_MILLIS,
            AGGREGATE_WINDOW_MILLIS,
        ),
        BackendId::LibraryPaxos
        | BackendId::CompartmentalizedPaxos
        | BackendId::PaxosEc
        | BackendId::TypedConsensus => {
            unreachable!("backend does not use the common colocated topology")
        }
    }
    (clients, aggregator, replicas)
}

async fn collect_metrics(
    mut output: tokio::sync::mpsc::UnboundedReceiver<String>,
    expected: usize,
) -> Result<Vec<WindowMetrics>, String> {
    tokio::time::timeout(COLLECTION_TIMEOUT, async move {
        let mut metrics = Vec::with_capacity(expected);
        while let Some(line) = output.recv().await {
            match parse_metric_line(&line).map_err(|error| error.to_string())? {
                Some(metric) => {
                    metrics.push(metric);
                    if metrics.len() == expected {
                        return Ok(metrics);
                    }
                }
                None => {}
            }
        }
        Err(format!(
            "metric stream ended after {} of {expected} windows",
            metrics.len()
        ))
    })
    .await
    .map_err(|_| format!("benchmark timed out after {COLLECTION_TIMEOUT:?}"))?
}

/// Run one backend with real localhost processes and collect exactly 15 windows.
pub async fn run_localhost(backend: BackendId) -> Result<PerfSummary, String> {
    run_localhost_at_concurrency(backend, DEFAULT_CONCURRENCY).await
}

/// Run one fresh localhost deployment at a controlled closed-loop concurrency.
pub async fn run_localhost_at_concurrency(
    backend: BackendId,
    concurrency_per_node: usize,
) -> Result<PerfSummary, String> {
    match backend {
        BackendId::LibraryPaxos => run_library_paxos_localhost(concurrency_per_node).await,
        BackendId::CompartmentalizedPaxos => {
            run_compartmentalized_paxos_localhost(concurrency_per_node).await
        }
        BackendId::PaxosEc | BackendId::TypedConsensus => Err(format!(
            "{} does not compile on the current Hydro API",
            backend
        )),
        _ => run_common_localhost(backend, concurrency_per_node).await,
    }
}

fn deploy_host(
    host: std::sync::Arc<dyn hydro_deploy::Host>,
    concurrency_per_node: usize,
) -> TrybuildHost {
    TrybuildHost::new(host).feature("deploy").env(
        "GAUNTLET_CONCURRENCY_PER_NODE",
        concurrency_per_node.to_string(),
    )
}

async fn run_common_localhost(
    backend: BackendId,
    concurrency_per_node: usize,
) -> Result<PerfSummary, String> {
    let mut builder = FlowBuilder::with_name(format!("consensus-gauntlet-{}", backend));
    let (clients, aggregator, replicas) = build_common_flow(backend, &mut builder);
    let mut deployment = Deployment::new();
    let nodes = builder
        .with_cluster(
            &clients,
            [deploy_host(deployment.Localhost(), concurrency_per_node)],
        )
        .with_process(
            &aggregator,
            deploy_host(deployment.Localhost(), concurrency_per_node),
        )
        .with_cluster(
            &replicas,
            (0..CLUSTER_SIZE).map(|_| deploy_host(deployment.Localhost(), concurrency_per_node)),
        )
        .deploy(&mut deployment);
    deployment
        .deploy()
        .await
        .map_err(|error| error.to_string())?;
    let output = nodes
        .get_process(&aggregator)
        .stdout_filter(METRIC_PREFIX.to_owned());
    deployment
        .start()
        .await
        .map_err(|error| error.to_string())?;
    let collected = collect_metrics(output, PerfConfig::default().total_windows()).await;
    let stop_result = deployment.stop().await.map_err(|error| error.to_string());
    let metrics = collected?;
    stop_result?;
    PerfSummary::new(metrics, PerfConfig::default()).map_err(|error: PerfError| error.to_string())
}

async fn run_library_paxos_localhost(concurrency_per_node: usize) -> Result<PerfSummary, String> {
    use hydro_test::cluster::paxos::{CorePaxos, PaxosConfig};
    use hydro_test::cluster::paxos_bench::{Aggregator, Client, paxos_bench};

    let mut builder = FlowBuilder::with_name("consensus-gauntlet-library-paxos");
    let proposers = builder.cluster();
    let acceptors = builder.cluster();
    let clients = builder.cluster::<Client>();
    let aggregator = builder.process::<Aggregator>();
    let replicas = builder.cluster();
    paxos_bench(
        CHECKPOINT_FREQUENCY,
        1,
        2,
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
        clients.singleton(q!(std::env::var("GAUNTLET_CONCURRENCY_PER_NODE")
            .unwrap()
            .parse::<usize>()
            .unwrap())),
        &aggregator,
        &replicas,
        CLIENT_WINDOW_MILLIS,
        AGGREGATE_WINDOW_MILLIS,
        hydro_std::bench_client::pretty_print_bench_results,
    );

    let mut deployment = Deployment::new();
    let host = deployment.Localhost();
    let nodes = builder
        .with_cluster(
            &proposers,
            (0..2).map(|_| deploy_host(host.clone(), concurrency_per_node)),
        )
        .with_cluster(
            &acceptors,
            (0..3).map(|_| deploy_host(host.clone(), concurrency_per_node)),
        )
        .with_cluster(&clients, [deploy_host(host.clone(), concurrency_per_node)])
        .with_process(&aggregator, deploy_host(host.clone(), concurrency_per_node))
        .with_cluster(
            &replicas,
            (0..2).map(|_| deploy_host(host.clone(), concurrency_per_node)),
        )
        .deploy(&mut deployment);
    deployment
        .deploy()
        .await
        .map_err(|error| error.to_string())?;
    let output = nodes
        .get_process(&aggregator)
        .stdout_filter(METRIC_PREFIX.to_owned());
    deployment
        .start()
        .await
        .map_err(|error| error.to_string())?;
    let collected = collect_metrics(output, PerfConfig::default().total_windows()).await;
    let stop_result = deployment.stop().await.map_err(|error| error.to_string());
    let metrics = collected?;
    stop_result?;
    PerfSummary::new(metrics, PerfConfig::default()).map_err(|error| error.to_string())
}

async fn run_compartmentalized_paxos_localhost(
    concurrency_per_node: usize,
) -> Result<PerfSummary, String> {
    use hydro_test::cluster::compartmentalized_paxos::{
        CompartmentalizedPaxosConfig, CoreCompartmentalizedPaxos,
    };
    use hydro_test::cluster::paxos::{PaxosConfig, Proposer};
    use hydro_test::cluster::paxos_bench::{Aggregator, Client, paxos_bench};

    const NUM_PROXY_LEADERS: usize = 10;
    const GRID_ROWS: usize = 2;
    const GRID_COLS: usize = 2;
    const NUM_REPLICAS: usize = 4;

    let mut builder = FlowBuilder::with_name("consensus-gauntlet-compartmentalized-paxos");
    let proposers = builder.cluster::<Proposer>();
    let proxy_leaders = builder.cluster();
    let acceptors = builder.cluster();
    let clients = builder.cluster::<Client>();
    let aggregator = builder.process::<Aggregator>();
    let replicas = builder.cluster();
    paxos_bench(
        CHECKPOINT_FREQUENCY,
        1,
        NUM_REPLICAS,
        CoreCompartmentalizedPaxos {
            proposers: proposers.clone(),
            proxy_leaders: proxy_leaders.clone(),
            acceptors: acceptors.clone(),
            config: CompartmentalizedPaxosConfig {
                paxos_config: PaxosConfig {
                    f: 1,
                    i_am_leader_send_timeout: 5,
                    i_am_leader_check_timeout: 10,
                    i_am_leader_check_timeout_delay_multiplier: 15,
                },
                num_proxy_leaders: NUM_PROXY_LEADERS,
                acceptor_grid_rows: GRID_ROWS,
                acceptor_grid_cols: GRID_COLS,
                num_replicas: NUM_REPLICAS,
                acceptor_retry_timeout: 10,
            },
        },
        &clients,
        clients.singleton(q!(std::env::var("GAUNTLET_CONCURRENCY_PER_NODE")
            .unwrap()
            .parse::<usize>()
            .unwrap())),
        &aggregator,
        &replicas,
        CLIENT_WINDOW_MILLIS,
        AGGREGATE_WINDOW_MILLIS,
        hydro_std::bench_client::pretty_print_bench_results,
    );

    let mut deployment = Deployment::new();
    let host = deployment.Localhost();
    let nodes = builder
        .with_cluster(
            &proposers,
            (0..2).map(|_| deploy_host(host.clone(), concurrency_per_node)),
        )
        .with_cluster(
            &proxy_leaders,
            (0..NUM_PROXY_LEADERS).map(|_| deploy_host(host.clone(), concurrency_per_node)),
        )
        .with_cluster(
            &acceptors,
            (0..GRID_ROWS * GRID_COLS).map(|_| deploy_host(host.clone(), concurrency_per_node)),
        )
        .with_cluster(&clients, [deploy_host(host.clone(), concurrency_per_node)])
        .with_process(&aggregator, deploy_host(host.clone(), concurrency_per_node))
        .with_cluster(
            &replicas,
            (0..NUM_REPLICAS).map(|_| deploy_host(host.clone(), concurrency_per_node)),
        )
        .deploy(&mut deployment);
    deployment
        .deploy()
        .await
        .map_err(|error| error.to_string())?;
    let output = nodes
        .get_process(&aggregator)
        .stdout_filter(METRIC_PREFIX.to_owned());
    deployment
        .start()
        .await
        .map_err(|error| error.to_string())?;
    let collected = collect_metrics(output, PerfConfig::default().total_windows()).await;
    let stop_result = deployment.stop().await.map_err(|error| error.to_string());
    let metrics = collected?;
    stop_result?;
    PerfSummary::new(metrics, PerfConfig::default()).map_err(|error| error.to_string())
}

/// Run the complete concurrency sweep, using a fresh deployment per repetition.
pub async fn run_sweep_localhost(
    backend: BackendId,
    config: crate::perf::SweepConfig,
) -> Result<crate::perf::SaturationCurve, String> {
    config.validate().map_err(|error| error.to_string())?;
    let mut points = Vec::with_capacity(config.concurrency.len());
    for &concurrency in &config.concurrency {
        let per_node = concurrency / config.client_nodes;
        let mut repetitions = Vec::with_capacity(config.repetitions);
        for _ in 0..config.repetitions {
            repetitions.push(run_localhost_at_concurrency(backend, per_node).await?);
        }
        points.push(
            crate::perf::SaturationPoint::new(
                concurrency,
                config.client_nodes,
                repetitions,
                config.repetitions,
            )
            .map_err(|error| error.to_string())?,
        );
    }
    crate::perf::SaturationCurve::new(
        backend.to_string(),
        crate::perf::ExecutionMetadata::localhost(
            std::process::Command::new("hostname")
                .output()
                .ok()
                .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_owned())
                .unwrap_or_else(|| "unknown".to_owned()),
        ),
        config,
        points,
    )
    .map_err(|error| error.to_string())
}

/// Export the exact benchmark graph for an external ECS orchestrator.
///
/// Hydro currently exports an ECS manifest but does not launch tasks or collect
/// their CloudWatch logs. `run-spec.json` records the collection contract; feed
/// the prefixed aggregator task lines back through `perf::parse_metric_lines`.
#[cfg(feature = "ecs")]
pub async fn export_ecs(backend: BackendId, output_dir: &Path) -> Result<PathBuf, String> {
    export_ecs_sweep(backend, SweepConfig::default(), output_dir).await
}

/// Export the same backend topology and concurrency sweep contract used by the
/// localhost runner. ECS orchestration supplies `GAUNTLET_CONCURRENCY_PER_NODE`
/// for each point/repetition and returns aggregator stdout to the shared parser.
#[cfg(feature = "ecs")]
pub async fn export_ecs_sweep(
    backend: BackendId,
    sweep: SweepConfig,
    output_dir: &Path,
) -> Result<PathBuf, String> {
    sweep.validate().map_err(|error| error.to_string())?;
    use hydro_lang::deploy::EcsDeploy;

    match backend {
        BackendId::Raft | BackendId::BroadcastTranscript | BackendId::QuorumLadderConsensus => {
            export_common_ecs(backend, sweep, output_dir).await
        }
        BackendId::LibraryPaxos => export_library_paxos_ecs(sweep, output_dir).await,
        BackendId::CompartmentalizedPaxos => {
            export_compartmentalized_paxos_ecs(sweep, output_dir).await
        }
        BackendId::PaxosEc | BackendId::TypedConsensus => Err(format!(
            "{backend} does not compile on the current Hydro API"
        )),
    }
}

#[cfg(feature = "ecs")]
async fn export_common_ecs(
    backend: BackendId,
    sweep: SweepConfig,
    output_dir: &Path,
) -> Result<PathBuf, String> {
    use hydro_lang::deploy::EcsDeploy;

    let mut builder = FlowBuilder::with_name(format!("consensus-gauntlet-{backend}"));
    let (clients, aggregator, replicas) = build_common_flow(backend, &mut builder);
    let mut deployment = EcsDeploy::new();
    let nodes = builder
        .with_cluster(&clients, deployment.add_ecs_cluster(sweep.client_nodes))
        .with_process(&aggregator, deployment.add_ecs_process())
        .with_cluster(&replicas, deployment.add_ecs_cluster(CLUSTER_SIZE))
        .deploy(&mut deployment);
    let manifest = deployment.export(&nodes);
    write_ecs_artifacts(
        backend,
        "colocated",
        CLUSTER_SIZE,
        sweep,
        output_dir,
        &manifest,
    )
    .await
}

#[cfg(feature = "ecs")]
async fn export_library_paxos_ecs(
    sweep: SweepConfig,
    output_dir: &Path,
) -> Result<PathBuf, String> {
    use hydro_lang::deploy::EcsDeploy;
    use hydro_test::cluster::paxos::{CorePaxos, PaxosConfig};
    use hydro_test::cluster::paxos_bench::{Aggregator, Client, paxos_bench};

    let mut builder = FlowBuilder::with_name("consensus-gauntlet-library-paxos");
    let proposers = builder.cluster();
    let acceptors = builder.cluster();
    let clients = builder.cluster::<Client>();
    let aggregator = builder.process::<Aggregator>();
    let replicas = builder.cluster();
    paxos_bench(
        CHECKPOINT_FREQUENCY,
        1,
        2,
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
        clients.singleton(q!(std::env::var("GAUNTLET_CONCURRENCY_PER_NODE")
            .unwrap()
            .parse::<usize>()
            .unwrap())),
        &aggregator,
        &replicas,
        CLIENT_WINDOW_MILLIS,
        AGGREGATE_WINDOW_MILLIS,
        hydro_std::bench_client::pretty_print_bench_results,
    );
    let mut deployment = EcsDeploy::new();
    let nodes = builder
        .with_cluster(&proposers, deployment.add_ecs_cluster(2))
        .with_cluster(&acceptors, deployment.add_ecs_cluster(3))
        .with_cluster(&clients, deployment.add_ecs_cluster(sweep.client_nodes))
        .with_process(&aggregator, deployment.add_ecs_process())
        .with_cluster(&replicas, deployment.add_ecs_cluster(2))
        .deploy(&mut deployment);
    let manifest = deployment.export(&nodes);
    write_ecs_artifacts(
        BackendId::LibraryPaxos,
        "proposers+acceptors+replicas",
        2,
        sweep,
        output_dir,
        &manifest,
    )
    .await
}

#[cfg(feature = "ecs")]
async fn export_compartmentalized_paxos_ecs(
    sweep: SweepConfig,
    output_dir: &Path,
) -> Result<PathBuf, String> {
    use hydro_lang::deploy::EcsDeploy;
    use hydro_test::cluster::compartmentalized_paxos::{
        CompartmentalizedPaxosConfig, CoreCompartmentalizedPaxos,
    };
    use hydro_test::cluster::paxos::{PaxosConfig, Proposer};
    use hydro_test::cluster::paxos_bench::{Aggregator, Client, paxos_bench};

    const PROXIES: usize = 10;
    const ROWS: usize = 2;
    const COLS: usize = 2;
    const REPLICAS: usize = 4;
    let mut builder = FlowBuilder::with_name("consensus-gauntlet-compartmentalized-paxos");
    let proposers = builder.cluster::<Proposer>();
    let proxy_leaders = builder.cluster();
    let acceptors = builder.cluster();
    let clients = builder.cluster::<Client>();
    let aggregator = builder.process::<Aggregator>();
    let replicas = builder.cluster();
    paxos_bench(
        CHECKPOINT_FREQUENCY,
        1,
        REPLICAS,
        CoreCompartmentalizedPaxos {
            proposers: proposers.clone(),
            proxy_leaders: proxy_leaders.clone(),
            acceptors: acceptors.clone(),
            config: CompartmentalizedPaxosConfig {
                paxos_config: PaxosConfig {
                    f: 1,
                    i_am_leader_send_timeout: 5,
                    i_am_leader_check_timeout: 10,
                    i_am_leader_check_timeout_delay_multiplier: 15,
                },
                num_proxy_leaders: PROXIES,
                acceptor_grid_rows: ROWS,
                acceptor_grid_cols: COLS,
                num_replicas: REPLICAS,
                acceptor_retry_timeout: 10,
            },
        },
        &clients,
        clients.singleton(q!(std::env::var("GAUNTLET_CONCURRENCY_PER_NODE")
            .unwrap()
            .parse::<usize>()
            .unwrap())),
        &aggregator,
        &replicas,
        CLIENT_WINDOW_MILLIS,
        AGGREGATE_WINDOW_MILLIS,
        hydro_std::bench_client::pretty_print_bench_results,
    );
    let mut deployment = EcsDeploy::new();
    let nodes = builder
        .with_cluster(&proposers, deployment.add_ecs_cluster(2))
        .with_cluster(&proxy_leaders, deployment.add_ecs_cluster(PROXIES))
        .with_cluster(&acceptors, deployment.add_ecs_cluster(ROWS * COLS))
        .with_cluster(&clients, deployment.add_ecs_cluster(sweep.client_nodes))
        .with_process(&aggregator, deployment.add_ecs_process())
        .with_cluster(&replicas, deployment.add_ecs_cluster(REPLICAS))
        .deploy(&mut deployment);
    let manifest = deployment.export(&nodes);
    write_ecs_artifacts(
        BackendId::CompartmentalizedPaxos,
        "proposers+proxy-leaders+acceptor-grid+replicas",
        REPLICAS,
        sweep,
        output_dir,
        &manifest,
    )
    .await
}

#[cfg(feature = "ecs")]
async fn write_ecs_artifacts(
    backend: BackendId,
    topology: &str,
    replica_count: usize,
    sweep: SweepConfig,
    output_dir: &Path,
    manifest: &hydro_lang::deploy::HydroManifest,
) -> Result<PathBuf, String> {
    tokio::fs::create_dir_all(output_dir)
        .await
        .map_err(|error| error.to_string())?;
    let manifest_path = output_dir.join("hydro-manifest.json");
    tokio::fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(manifest).map_err(|error| error.to_string())?,
    )
    .await
    .map_err(|error| error.to_string())?;
    let windows = sweep.windows;
    let spec = EcsRunSpec {
        backend: backend.to_string(),
        topology: topology.to_owned(),
        replica_count,
        metric_prefix: METRIC_PREFIX.to_owned(),
        expected_windows: windows.total_windows(),
        warmup_windows: windows.warmup_windows,
        steady_windows: windows.steady_windows,
        concurrency_env: "GAUNTLET_CONCURRENCY_PER_NODE".to_owned(),
        sweep,
    };
    tokio::fs::write(
        output_dir.join("run-spec.json"),
        serde_json::to_vec_pretty(&spec).map_err(|error| error.to_string())?,
    )
    .await
    .map_err(|error| error.to_string())?;
    Ok(manifest_path)
}

/// Parse collected ECS/CloudWatch aggregator logs and summarize one run with
/// the same validation and statistics used by localhost.
pub fn ingest_ecs_metrics(log: &str, config: PerfConfig) -> Result<PerfSummary, String> {
    let metrics =
        crate::perf::parse_metric_lines(log.lines()).map_err(|error| error.to_string())?;
    PerfSummary::new(metrics, config).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ecs_log_ingestion_uses_shared_metric_contract() {
        let config = PerfConfig::default();
        let mut log = String::from("unrelated task output\n");
        for sequence in 0..config.total_windows() {
            let metric = WindowMetrics {
                sequence: sequence as u64,
                throughput_rps: 1000.0 + sequence as f64,
                p50_ms: 1.0,
                p99_ms: 2.0,
                p999_ms: 3.0,
                samples: 1000,
            };
            log.push_str(&crate::perf::format_metric_line(&metric).unwrap());
            log.push('\n');
        }
        let summary = ingest_ecs_metrics(&log, config).unwrap();
        assert_eq!(summary.windows.len(), 15);
        assert_eq!(summary.steady_windows().len(), 12);
    }
}
