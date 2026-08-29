use std::path::PathBuf;
use std::process::Command;

use clap::{Parser, Subcommand, ValueEnum};
use consensus_gauntlet::backend::BackendId;
use consensus_gauntlet::census::census_source;
use consensus_gauntlet::perf::{
    ExecutionMetadata, ExecutionTarget, PerfConfig, PerfSummary, parse_metric_lines,
};
use consensus_gauntlet::report::{Environment, GauntletReport, Outcome, Status};

#[derive(Parser)]
#[command(about = "Run or render the standardized consensus gauntlet")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Produce a report from existing result JSON files.
    Report {
        #[arg(long)]
        output: Option<PathBuf>,
        /// Attach `backend=perf-summary.json` (repeatable, legacy point runs).
        #[arg(long = "perf", value_name = "BACKEND=PATH")]
        performance: Vec<String>,
        /// Attach a saturation-curve JSON file (repeatable).
        #[arg(long = "curve", value_name = "PATH")]
        curves: Vec<PathBuf>,
        /// Attach a pinned sequestered-review JSON file (repeatable).
        #[arg(long = "review", value_name = "PATH")]
        reviews: Vec<PathBuf>,
        /// Emit Markdown instead of the default self-contained HTML.
        #[arg(long, value_enum, default_value = "html")]
        format: ReportFormat,
    },
    /// Invoke an external LLM adapter independently for each selected source.
    Review {
        /// Executable that reads one JSON request from stdin and returns strict JSON.
        #[arg(long, env = "CONSENSUS_GAUNTLET_REVIEW_ADAPTER")]
        adapter: PathBuf,
        /// Model alias requested from the adapter; the artifact records the actual model too.
        #[arg(long, env = "CONSENSUS_GAUNTLET_REVIEW_MODEL")]
        model: String,
        #[arg(long, value_enum, default_value = "all")]
        backend: ReviewBackend,
        #[arg(long, default_value = "consensus-gauntlet-reviews")]
        output_dir: PathBuf,
    },
    /// Run all available tiers and always write a self-contained HTML report.
    #[cfg(feature = "maelstrom")]
    Run {
        #[arg(long, default_value = "consensus-gauntlet-report.html")]
        output: PathBuf,
        #[arg(long, default_value = "consensus-gauntlet-results")]
        artifacts: PathBuf,
        #[arg(long, value_enum, default_value = "all")]
        backend: RunBackend,
        #[arg(long, value_delimiter = ',', default_value = "1,32,128,256")]
        concurrency: Vec<usize>,
        /// Repetitions per performance point; one is the pilot default.
        #[arg(long, default_value_t = 1)]
        repetitions: usize,
        /// Publication mode forces three repetitions per performance point.
        #[arg(long)]
        publication: bool,
        #[arg(long)]
        maelstrom_path: Option<PathBuf>,
        /// Skip the long Maelstrom correctness ladder.
        #[arg(long)]
        skip_maelstrom: bool,
        /// Skip localhost performance deployments.
        #[arg(long)]
        skip_performance: bool,
    },
    /// Compare Raft and Quorum-Ladder Consensus with one identical sweep and report.
    #[cfg(feature = "deploy")]
    Compare {
        /// Backend to compare against Raft.
        #[arg(long, value_enum, default_value = "quorum-ladder-consensus")]
        other: BackendArg,
        #[arg(long, default_value = "raft-comparison.html")]
        output: PathBuf,
        #[arg(long, default_value = "consensus-gauntlet-results")]
        artifacts: PathBuf,
        #[arg(long, value_delimiter = ',', default_value = "1,32,128,256")]
        concurrency: Vec<usize>,
        /// Repetitions per point; one is the development-pilot default.
        #[arg(long, default_value_t = 1)]
        repetitions: usize,
        /// Publication mode forces three repetitions per point.
        #[arg(long)]
        publication: bool,
    },
    /// Run one backend locally at one concurrency (debugging/compatibility).
    #[cfg(feature = "deploy")]
    Perf {
        #[arg(value_enum)]
        backend: BackendArg,
        #[arg(long, default_value_t = 100)]
        concurrency: usize,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Run the classic concurrency saturation sweep for one backend.
    #[cfg(feature = "deploy")]
    Sweep {
        #[arg(value_enum)]
        backend: BackendArg,
        #[arg(
            long,
            value_delimiter = ',',
            default_value = "1,2,4,8,16,32,64,128,256,512"
        )]
        concurrency: Vec<usize>,
        #[arg(long, default_value_t = 3)]
        repetitions: usize,
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Render externally collected localhost/ECS metric logs with the same stats code.
    RenderMetrics {
        input: PathBuf,
        #[arg(long, value_enum, default_value = "ecs")]
        target: TargetArg,
    },
    /// Export the target-neutral performance flow as an ECS deployment manifest.
    #[cfg(feature = "ecs")]
    ExportEcs {
        #[arg(value_enum)]
        backend: BackendArg,
        #[arg(long, value_delimiter = ',', default_value = "1,32,128,256")]
        concurrency: Vec<usize>,
        #[arg(long, default_value_t = 1)]
        repetitions: usize,
        #[arg(long)]
        publication: bool,
        output_dir: PathBuf,
    },
    /// Run all supported external lin-kv ladder cells for one backend.
    #[cfg(feature = "maelstrom")]
    LinKv {
        #[arg(value_enum)]
        backend: BackendArg,
        #[arg(long, value_enum, default_value = "all")]
        rung: RungArg,
        #[arg(long, env = "MAELSTROM_PATH")]
        maelstrom_path: Option<PathBuf>,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum ReportFormat {
    Html,
    Markdown,
}

#[derive(Clone, Copy, ValueEnum)]
enum RunBackend {
    All,
    Raft,
    LibraryPaxos,
    CompartmentalizedPaxos,
    BroadcastTranscript,
    PaxosEc,
    TypedConsensus,
    QuorumLadderConsensus,
}

#[cfg(feature = "maelstrom")]
impl RunBackend {
    fn selected(self) -> Vec<BackendId> {
        match self {
            Self::All => BackendId::ALL.to_vec(),
            Self::Raft => vec![BackendId::Raft],
            Self::LibraryPaxos => vec![BackendId::LibraryPaxos],
            Self::CompartmentalizedPaxos => vec![BackendId::CompartmentalizedPaxos],
            Self::BroadcastTranscript => vec![BackendId::BroadcastTranscript],
            Self::PaxosEc => vec![BackendId::PaxosEc],
            Self::TypedConsensus => vec![BackendId::TypedConsensus],
            Self::QuorumLadderConsensus => vec![BackendId::QuorumLadderConsensus],
        }
    }
}

#[derive(Clone, Copy, ValueEnum)]
enum ReviewBackend {
    All,
    Raft,
    LibraryPaxos,
    CompartmentalizedPaxos,
    BroadcastTranscript,
    PaxosEc,
    TypedConsensus,
    QuorumLadderConsensus,
}

impl ReviewBackend {
    fn selected(self) -> Vec<BackendId> {
        match self {
            Self::All => BackendId::ALL.to_vec(),
            Self::Raft => vec![BackendId::Raft],
            Self::LibraryPaxos => vec![BackendId::LibraryPaxos],
            Self::CompartmentalizedPaxos => vec![BackendId::CompartmentalizedPaxos],
            Self::BroadcastTranscript => vec![BackendId::BroadcastTranscript],
            Self::PaxosEc => vec![BackendId::PaxosEc],
            Self::TypedConsensus => vec![BackendId::TypedConsensus],
            Self::QuorumLadderConsensus => vec![BackendId::QuorumLadderConsensus],
        }
    }
}

#[derive(Clone, Copy, ValueEnum)]
enum RungArg {
    All,
    Smoke,
    Kill,
    Partition,
}

#[derive(Clone, Copy, ValueEnum)]
enum BackendArg {
    Raft,
    LibraryPaxos,
    CompartmentalizedPaxos,
    BroadcastTranscript,
    PaxosEc,
    TypedConsensus,
    QuorumLadderConsensus,
}

impl From<BackendArg> for BackendId {
    fn from(value: BackendArg) -> Self {
        match value {
            BackendArg::Raft => Self::Raft,
            BackendArg::LibraryPaxos => Self::LibraryPaxos,
            BackendArg::CompartmentalizedPaxos => Self::CompartmentalizedPaxos,
            BackendArg::BroadcastTranscript => Self::BroadcastTranscript,
            BackendArg::PaxosEc => Self::PaxosEc,
            BackendArg::TypedConsensus => Self::TypedConsensus,
            BackendArg::QuorumLadderConsensus => Self::QuorumLadderConsensus,
        }
    }
}

#[derive(Clone, Copy, ValueEnum)]
enum TargetArg {
    Localhost,
    Ecs,
}

fn discover_environment(target: ExecutionTarget) -> Environment {
    let host = Command::new("hostname")
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_owned())
        .unwrap_or_else(|| "unknown".to_owned());
    let commit = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_owned())
        .unwrap_or_else(|| "unknown".to_owned());
    let date = Command::new("date")
        .args(["-u", "+%Y-%m-%dT%H:%M:%SZ"])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_owned())
        .unwrap_or_else(|| "unknown".to_owned());
    let mut execution = ExecutionMetadata::localhost(host.clone());
    execution.target = target;
    Environment {
        host,
        date,
        commit,
        execution,
    }
}

fn report_with_census(target: ExecutionTarget) -> GauntletReport {
    let mut report = GauntletReport::new(discover_environment(target));
    let sources = [
        (
            BackendId::Raft,
            include_str!("../../hydro_test/src/cluster/raft.rs"),
        ),
        (
            BackendId::LibraryPaxos,
            include_str!("../../hydro_test/src/cluster/paxos.rs"),
        ),
        (
            BackendId::BroadcastTranscript,
            include_str!("../../hydro_test/src/cluster/broadcast_transcript_consensus.rs"),
        ),
        (
            BackendId::QuorumLadderConsensus,
            include_str!("../../hydro_std/src/ec_inference_demos/multi_paxos.rs"),
        ),
    ];
    for (backend, source) in sources {
        let row = report.backend_mut(backend);
        row.census = Some(census_source(source));
        row.census_status = Status::new(Outcome::Passed, "mechanical source census");
    }
    // The current Maelstrom deploy surface has no multi-location adapter; build
    // failures and adapter gaps remain explicit for every affected backend.
    for backend in BackendId::ALL {
        let cap = backend.capabilities();
        let row = report.backend_mut(backend);
        match cap.build {
            consensus_gauntlet::backend::BuildStatus::Builds => {}
            consensus_gauntlet::backend::BuildStatus::Broken(reason) => {
                row.perf_status = Status::new(Outcome::Failed, reason);
            }
        }
        if let consensus_gauntlet::backend::SupportStatus::Gap(reason) = cap.maelstrom {
            row.lin_kv.smoke = Status::new(Outcome::CapabilityGap, reason);
            row.lin_kv.kill = Status::new(Outcome::CapabilityGap, reason);
            row.lin_kv.partition = Status::new(Outcome::CapabilityGap, reason);
        }
        // Source-level census remains available even when the backend is
        // disabled, so broken variants do not disappear from the comparison.
        let source = match backend {
            BackendId::CompartmentalizedPaxos => Some(include_str!(
                "../../hydro_test/src/cluster/compartmentalized_paxos.rs"
            )),
            BackendId::PaxosEc => Some(include_str!("../../hydro_test/src/cluster/paxos_ec.rs")),
            BackendId::TypedConsensus => Some(include_str!(
                "../../hydro_test/src/cluster/typed_consensus.rs"
            )),
            _ => None,
        };
        if let Some(source) = source {
            row.census = Some(census_source(source));
            row.census_status = Status::new(Outcome::Passed, "mechanical source census");
        }
    }
    report
}

fn attach_performance(report: &mut GauntletReport, specs: &[String]) -> Result<(), String> {
    for spec in specs {
        let (backend, path) = spec
            .split_once('=')
            .ok_or_else(|| format!("invalid --perf {spec:?}; expected backend=path"))?;
        let backend = match backend {
            "raft" => BackendId::Raft,
            "library-paxos" => BackendId::LibraryPaxos,
            "compartmentalized-paxos" => BackendId::CompartmentalizedPaxos,
            "broadcast-transcript" => BackendId::BroadcastTranscript,
            "paxos-ec" => BackendId::PaxosEc,
            "typed-consensus" => BackendId::TypedConsensus,
            "quorum-ladder-consensus" => BackendId::QuorumLadderConsensus,
            _ => return Err(format!("unknown performance backend {backend:?}")),
        };
        let summary: PerfSummary = serde_json::from_slice(
            &std::fs::read(path).map_err(|error| format!("{path}: {error}"))?,
        )
        .map_err(|error| format!("{path}: {error}"))?;
        let target = report.environment.execution.target.to_string();
        let row = report.backend_mut(backend);
        row.performance = Some(summary);
        row.perf_status = Status::new(Outcome::Passed, target);
    }
    Ok(())
}

fn attach_curves(report: &mut GauntletReport, paths: &[PathBuf]) -> Result<(), String> {
    for path in paths {
        let curve: consensus_gauntlet::perf::SaturationCurve = serde_json::from_slice(
            &std::fs::read(path).map_err(|error| format!("{}: {error}", path.display()))?,
        )
        .map_err(|error| format!("{}: {error}", path.display()))?;
        report.saturation_curves.push(curve);
    }
    Ok(())
}

fn attach_reviews(report: &mut GauntletReport, paths: &[PathBuf]) -> Result<(), String> {
    for path in paths {
        let artifact: consensus_gauntlet::review::ReviewArtifact = serde_json::from_slice(
            &std::fs::read(path).map_err(|error| format!("{}: {error}", path.display()))?,
        )
        .map_err(|error| format!("{}: {error}", path.display()))?;
        consensus_gauntlet::review::validate_artifact_against_workspace(
            &artifact,
            &consensus_gauntlet::review::workspace_root(),
        )
        .map_err(|error| format!("{}: {error}", path.display()))?;
        if report
            .reviews
            .iter()
            .any(|existing| existing.backend == artifact.backend)
        {
            return Err(format!(
                "duplicate review for backend {:?}",
                artifact.backend
            ));
        }
        report.reviews.push(artifact);
    }
    report.reviews.sort_by_key(|review| {
        BackendId::ALL
            .iter()
            .position(|backend| backend.as_str() == review.backend)
            .unwrap_or(usize::MAX)
    });
    Ok(())
}

fn run_reviews(
    adapter: PathBuf,
    model: String,
    backend: ReviewBackend,
    output_dir: PathBuf,
) -> Result<(), String> {
    std::fs::create_dir_all(&output_dir)
        .map_err(|error| format!("create {}: {error}", output_dir.display()))?;
    let root = consensus_gauntlet::review::workspace_root();
    for backend in backend.selected() {
        // Deliberately invoke a new process for each backend. The adapter receives
        // one implementation and cannot observe any earlier verdict or peer source.
        let artifact = consensus_gauntlet::review::invoke_review(&root, backend, &adapter, &model)?;
        let output = output_dir.join(format!("{}.review.json", backend.as_str()));
        write_json(&output, &artifact)?;
        eprintln!("wrote {}", output.display());
    }
    Ok(())
}

fn render_report(report: &GauntletReport, format: ReportFormat) -> String {
    match format {
        ReportFormat::Html => consensus_gauntlet::html::render_html(report),
        ReportFormat::Markdown => report.to_markdown(),
    }
}

fn write_or_print(content: &str, output: Option<PathBuf>) -> Result<(), String> {
    if let Some(path) = output {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        std::fs::write(path, content).map_err(|error| error.to_string())
    } else {
        print!("{content}");
        Ok(())
    }
}

#[tokio::main]
#[cfg(feature = "deploy")]
async fn main() -> Result<(), String> {
    run().await
}

#[cfg(not(feature = "deploy"))]
fn main() -> Result<(), String> {
    run_sync()
}

#[cfg(not(feature = "deploy"))]
fn run_sync() -> Result<(), String> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Report {
            output,
            performance,
            curves,
            reviews,
            format,
        } => {
            let mut report = report_with_census(ExecutionTarget::Localhost);
            attach_performance(&mut report, &performance)?;
            attach_curves(&mut report, &curves)?;
            attach_reviews(&mut report, &reviews)?;
            write_or_print(&render_report(&report, format), output)
        }
        Commands::Review {
            adapter,
            model,
            backend,
            output_dir,
        } => run_reviews(adapter, model, backend, output_dir),
        Commands::RenderMetrics { input, target } => render_metrics(input, target),
    }
}

#[cfg(feature = "deploy")]
async fn run() -> Result<(), String> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Report {
            output,
            performance,
            curves,
            reviews,
            format,
        } => {
            let mut report = report_with_census(ExecutionTarget::Localhost);
            attach_performance(&mut report, &performance)?;
            attach_curves(&mut report, &curves)?;
            attach_reviews(&mut report, &reviews)?;
            write_or_print(&render_report(&report, format), output)
        }
        Commands::Review {
            adapter,
            model,
            backend,
            output_dir,
        } => run_reviews(adapter, model, backend, output_dir),
        Commands::Compare {
            output,
            other,
            artifacts,
            concurrency,
            repetitions,
            publication,
        } => {
            let repetitions = if publication { 3 } else { repetitions };
            compare_raft_against(other.into(), output, artifacts, concurrency, repetitions).await
        }
        Commands::Perf {
            backend,
            concurrency,
            output,
        } => {
            let backend = BackendId::from(backend);
            let summary =
                consensus_gauntlet::runner::run_localhost_at_concurrency(backend, concurrency)
                    .await?;
            let json = serde_json::to_string_pretty(&summary).map_err(|error| error.to_string())?;
            write_or_print(&json, output)
        }
        Commands::Sweep {
            backend,
            concurrency,
            repetitions,
            output,
        } => {
            let backend = BackendId::from(backend);
            let curve = consensus_gauntlet::runner::run_sweep_localhost(
                backend,
                consensus_gauntlet::perf::SweepConfig {
                    concurrency,
                    repetitions,
                    ..consensus_gauntlet::perf::SweepConfig::default()
                },
            )
            .await?;
            let json = serde_json::to_string_pretty(&curve).map_err(|error| error.to_string())?;
            write_or_print(&json, output)
        }
        Commands::RenderMetrics { input, target } => render_metrics(input, target),
        #[cfg(feature = "maelstrom")]
        Commands::Run {
            output,
            artifacts,
            backend,
            concurrency,
            repetitions,
            publication,
            maelstrom_path,
            skip_maelstrom,
            skip_performance,
        } => {
            let repetitions = if publication { 3 } else { repetitions };
            run_gauntlet(
                output,
                artifacts,
                backend,
                concurrency,
                repetitions,
                maelstrom_path,
                skip_maelstrom,
                skip_performance,
            )
            .await
        }
        #[cfg(feature = "ecs")]
        Commands::ExportEcs {
            backend,
            concurrency,
            repetitions,
            publication,
            output_dir,
        } => {
            let repetitions = if publication { 3 } else { repetitions };
            let path = consensus_gauntlet::runner::export_ecs_sweep(
                backend.into(),
                consensus_gauntlet::perf::SweepConfig {
                    concurrency,
                    repetitions,
                    ..consensus_gauntlet::perf::SweepConfig::default()
                },
                &output_dir,
            )
            .await?;
            println!("{}", path.display());
            Ok(())
        }
        #[cfg(feature = "maelstrom")]
        Commands::LinKv {
            backend,
            rung,
            maelstrom_path,
        } => {
            use consensus_gauntlet::maelstrom::{LinKvConfig, LinKvRung};
            let path = consensus_gauntlet::install::ensure_maelstrom(maelstrom_path.as_deref())?;
            let backend = backend.into();
            match rung {
                RungArg::All => {
                    consensus_gauntlet::maelstrom_runner::run_backend(backend, &path)?;
                }
                RungArg::Smoke => consensus_gauntlet::maelstrom_runner::run_rung(
                    backend,
                    &path,
                    &LinKvConfig::for_rung(LinKvRung::Smoke),
                )?,
                RungArg::Kill => consensus_gauntlet::maelstrom_runner::run_rung(
                    backend,
                    &path,
                    &LinKvConfig::for_rung(LinKvRung::Kill),
                )?,
                RungArg::Partition => consensus_gauntlet::maelstrom_runner::run_rung(
                    backend,
                    &path,
                    &LinKvConfig::for_rung(LinKvRung::Partition),
                )?,
            }
            Ok(())
        }
    }
}

#[cfg(feature = "maelstrom")]
fn apply_plan_gaps(report: &mut GauntletReport, backend: BackendId) {
    use consensus_gauntlet::maelstrom::{LinKvRung, PlannedRun, plan_for_backend};
    for run in plan_for_backend(backend).runs {
        if let PlannedRun::CapabilityGap { rung, reason } = run {
            let status = Status::new(Outcome::CapabilityGap, reason);
            let row = report.backend_mut(backend);
            match rung {
                LinKvRung::Smoke => row.lin_kv.smoke = status,
                LinKvRung::Kill => row.lin_kv.kill = status,
                LinKvRung::Partition => row.lin_kv.partition = status,
            }
        }
    }
}

#[cfg(feature = "maelstrom")]
async fn run_gauntlet(
    output: PathBuf,
    artifacts: PathBuf,
    selection: RunBackend,
    concurrency: Vec<usize>,
    repetitions: usize,
    explicit_maelstrom: Option<PathBuf>,
    skip_maelstrom: bool,
    skip_performance: bool,
) -> Result<(), String> {
    use consensus_gauntlet::maelstrom::{LinKvRung, PlannedRun, plan_for_backend};

    let selected = selection.selected();
    let mut report = report_with_census(ExecutionTarget::Localhost);
    // The build tier is executable: enabled packages are checked and disabled
    // variants are compiled in isolated probe crates. Failures are report data.
    for backend in &selected {
        let outcome = consensus_gauntlet::registry::run_build_probe(*backend);
        let row = report.backend_mut(*backend);
        match outcome {
            consensus_gauntlet::registry::BuildOutcome::Passed => {}
            consensus_gauntlet::registry::BuildOutcome::Failed { diagnostics } => {
                row.perf_status =
                    Status::new(Outcome::Failed, format!("build probe: {diagnostics}"));
            }
            consensus_gauntlet::registry::BuildOutcome::HarnessError { detail } => {
                row.perf_status =
                    Status::new(Outcome::Failed, format!("build probe error: {detail}"));
            }
        }
    }
    write_or_print(
        &consensus_gauntlet::html::render_html(&report),
        Some(output.clone()),
    )?;
    let sweep_config = consensus_gauntlet::perf::SweepConfig {
        concurrency,
        repetitions,
        client_nodes: 1,
        windows: PerfConfig::default(),
    };
    sweep_config.validate().map_err(|error| error.to_string())?;
    std::fs::create_dir_all(&artifacts).map_err(|error| error.to_string())?;
    let mut report = report_with_census(ExecutionTarget::Localhost);
    for backend in BackendId::ALL {
        apply_plan_gaps(&mut report, backend);
    }
    let maelstrom = if skip_maelstrom {
        None
    } else {
        match consensus_gauntlet::install::ensure_maelstrom(explicit_maelstrom.as_deref()) {
            Ok(path) => Some(path),
            Err(error) => {
                for backend in &selected {
                    let row = report.backend_mut(*backend);
                    for status in [
                        &mut row.lin_kv.smoke,
                        &mut row.lin_kv.kill,
                        &mut row.lin_kv.partition,
                    ] {
                        if status.outcome == Outcome::NotRun {
                            *status =
                                Status::new(Outcome::Failed, format!("Maelstrom setup: {error}"));
                        }
                    }
                }
                None
            }
        }
    };

    if !skip_maelstrom {
        if let Some(path) = &maelstrom {
            for backend in &selected {
                for planned in plan_for_backend(*backend).runs {
                    let PlannedRun::Ready(config) = planned else {
                        continue;
                    };
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        consensus_gauntlet::maelstrom_runner::run_rung(*backend, path, &config)
                    }));
                    let status = match result {
                        Ok(Ok(())) => {
                            Status::new(Outcome::Passed, "Maelstrom/Knossos accepted the history")
                        }
                        Ok(Err(error)) => Status::new(Outcome::Failed, error),
                        Err(payload) => Status::new(
                            Outcome::Failed,
                            format!("Maelstrom runner panicked: {}", panic_message(payload)),
                        ),
                    };
                    let row = report.backend_mut(*backend);
                    match config.rung {
                        LinKvRung::Smoke => row.lin_kv.smoke = status,
                        LinKvRung::Kill => row.lin_kv.kill = status,
                        LinKvRung::Partition => row.lin_kv.partition = status,
                    }
                    // A failed rung does not prevent the HTML artifact or other
                    // independent backends from being produced.
                }
            }
        }
    }

    if !skip_performance {
        for backend in &selected {
            match consensus_gauntlet::runner::run_sweep_localhost(*backend, sweep_config.clone())
                .await
            {
                Ok(curve) => {
                    write_json(
                        &artifacts.join(format!("{}.json", backend.as_str())),
                        &curve,
                    )?;
                    let median_point = curve
                        .points
                        .iter()
                        .find(|point| point.requested_concurrency == 128)
                        .or_else(|| curve.points.last())
                        .map(|point| point.throughput_rps.median);
                    report.saturation_curves.push(curve);
                    let row = report.backend_mut(*backend);
                    row.perf_status = Status::new(
                        Outcome::Passed,
                        median_point
                            .map(|value| {
                                format!("localhost saturation sweep; c=128: {value:.0} req/s")
                            })
                            .unwrap_or_else(|| "localhost saturation sweep".to_owned()),
                    );
                }
                Err(error) => {
                    report.backend_mut(*backend).perf_status = Status::new(Outcome::Failed, error);
                }
            }
        }
        // Always checkpoint the HTML after each backend/tier outcome.
        write_or_print(
            &consensus_gauntlet::html::render_html(&report),
            Some(output.clone()),
        )?;
    }

    write_or_print(
        &consensus_gauntlet::html::render_html(&report),
        Some(output),
    )
}

#[cfg(feature = "deploy")]
async fn compare_raft_against(
    other: BackendId,
    output: PathBuf,
    artifacts: PathBuf,
    concurrency: Vec<usize>,
    repetitions: usize,
) -> Result<(), String> {
    use consensus_gauntlet::perf::SweepConfig;

    if other == BackendId::Raft {
        return Err("--other must not be Raft".to_owned());
    }
    match other.capabilities().performance {
        consensus_gauntlet::backend::SupportStatus::Supported
        | consensus_gauntlet::backend::SupportStatus::Partial(_) => {}
        consensus_gauntlet::backend::SupportStatus::Gap(reason) => {
            return Err(format!("{other} performance gap: {reason}"));
        }
    }
    let config = SweepConfig {
        concurrency,
        repetitions,
        client_nodes: 1,
        windows: PerfConfig::default(),
    };
    config.validate().map_err(|error| error.to_string())?;
    std::fs::create_dir_all(&artifacts).map_err(|error| error.to_string())?;

    // Interleave backend order per repetition to reduce thermal/order bias. Every
    // invocation below is a fresh deployment.
    let mut raft_points = Vec::with_capacity(config.concurrency.len());
    let mut other_points = Vec::with_capacity(config.concurrency.len());
    for (point_index, &requested) in config.concurrency.iter().enumerate() {
        let per_node = requested / config.client_nodes;
        let mut raft_runs = Vec::with_capacity(config.repetitions);
        let mut other_runs = Vec::with_capacity(config.repetitions);
        for repetition in 0..config.repetitions {
            let raft_first = (point_index + repetition) % 2 == 0;
            if raft_first {
                raft_runs.push(
                    consensus_gauntlet::runner::run_localhost_at_concurrency(
                        BackendId::Raft,
                        per_node,
                    )
                    .await
                    .map_err(|error| {
                        format!("Raft c={requested} r={repetition} failed: {error}")
                    })?,
                );
                other_runs.push(
                    consensus_gauntlet::runner::run_localhost_at_concurrency(other, per_node)
                        .await
                        .map_err(|error| {
                            format!("{other} c={requested} r={repetition} failed: {error}")
                        })?,
                );
            } else {
                other_runs.push(
                    consensus_gauntlet::runner::run_localhost_at_concurrency(other, per_node)
                        .await
                        .map_err(|error| {
                            format!("{other} c={requested} r={repetition} failed: {error}")
                        })?,
                );
                raft_runs.push(
                    consensus_gauntlet::runner::run_localhost_at_concurrency(
                        BackendId::Raft,
                        per_node,
                    )
                    .await
                    .map_err(|error| {
                        format!("Raft c={requested} r={repetition} failed: {error}")
                    })?,
                );
            }
        }
        raft_points.push(
            consensus_gauntlet::perf::SaturationPoint::new(
                requested,
                config.client_nodes,
                raft_runs,
                config.repetitions,
            )
            .map_err(|error| error.to_string())?,
        );
        other_points.push(
            consensus_gauntlet::perf::SaturationPoint::new(
                requested,
                config.client_nodes,
                other_runs,
                config.repetitions,
            )
            .map_err(|error| error.to_string())?,
        );
        // Checkpoint raw completed points so interruption never loses earlier
        // measurements.
        write_json(&artifacts.join("raft-points.partial.json"), &raft_points)?;
        write_json(
            &artifacts.join(format!("{}-points.partial.json", other.as_str())),
            &other_points,
        )?;
    }

    let execution = ExecutionMetadata::localhost(
        Command::new("hostname")
            .output()
            .ok()
            .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_owned())
            .unwrap_or_else(|| "unknown".to_owned()),
    );
    let raft = consensus_gauntlet::perf::SaturationCurve::new(
        BackendId::Raft.to_string(),
        execution.clone(),
        config.clone(),
        raft_points,
    )
    .map_err(|error| error.to_string())?;
    let other_curve = consensus_gauntlet::perf::SaturationCurve::new(
        other.to_string(),
        execution,
        config,
        other_points,
    )
    .map_err(|error| error.to_string())?;
    write_json(&artifacts.join("raft.json"), &raft)?;
    write_json(
        &artifacts.join(format!("{}.json", other.as_str())),
        &other_curve,
    )?;

    let mut report = report_with_census(ExecutionTarget::Localhost);
    report
        .backends
        .retain(|row| row.backend == BackendId::Raft || row.backend == other);
    for curve in [&raft, &other_curve] {
        let backend = if curve.backend == BackendId::Raft.as_str() {
            BackendId::Raft
        } else {
            other
        };
        let peak = curve
            .points
            .iter()
            .max_by(|left, right| {
                left.throughput_rps
                    .median
                    .total_cmp(&right.throughput_rps.median)
            })
            .expect("validated sweep is non-empty");
        report.backend_mut(backend).perf_status = Status::new(
            Outcome::Passed,
            format!(
                "peak median {:.0} req/s at concurrency {} (p50 {:.3} ms)",
                peak.throughput_rps.median, peak.requested_concurrency, peak.p50_ms.median,
            ),
        );
    }
    report.saturation_curves = vec![raft, other_curve];
    write_or_print(
        &consensus_gauntlet::html::render_html(&report),
        Some(output),
    )
}

fn write_json(path: &std::path::Path, value: &impl serde::Serialize) -> Result<(), String> {
    let bytes = serde_json::to_vec_pretty(value).map_err(|error| error.to_string())?;
    std::fs::write(path, bytes).map_err(|error| error.to_string())
}

fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else if let Some(message) = payload.downcast_ref::<&str>() {
        (*message).to_owned()
    } else {
        "non-string panic payload".to_owned()
    }
}

fn render_metrics(input: PathBuf, target: TargetArg) -> Result<(), String> {
    let input = std::fs::read_to_string(input).map_err(|error| error.to_string())?;
    let metrics = parse_metric_lines(input.lines()).map_err(|error| error.to_string())?;
    let summary =
        PerfSummary::new(metrics, PerfConfig::default()).map_err(|error| error.to_string())?;
    let target = match target {
        TargetArg::Localhost => ExecutionTarget::Localhost,
        TargetArg::Ecs => ExecutionTarget::Ecs,
    };
    let mut report = report_with_census(target);
    let row = report.backend_mut(BackendId::Raft);
    row.performance = Some(summary);
    row.perf_status = Status::new(Outcome::Passed, "imported metric log");
    println!("{}", consensus_gauntlet::html::render_html(&report));
    Ok(())
}
