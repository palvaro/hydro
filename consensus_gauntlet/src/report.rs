//! Deterministic Markdown reports for the standardized consensus gauntlet.

use std::fmt::{self, Write};

use crate::backend::{BackendId, Checkpointing, ConsistencyOutput, TimerInput};
use crate::census::Census;
use crate::perf::{ExecutionMetadata, PerfSummary};

/// Reproducibility information printed at the top of every report.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Environment {
    pub host: String,
    pub date: String,
    pub commit: String,
    pub execution: ExecutionMetadata,
}

/// A result category. Capability gaps are distinct from skipped or failed runs.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Passed,
    Failed,
    Skipped,
    CapabilityGap,
    NotRun,
}

impl Outcome {
    const fn label(self) -> &'static str {
        match self {
            Self::Passed => "pass",
            Self::Failed => "FAIL",
            Self::Skipped => "skipped",
            Self::CapabilityGap => "capability gap",
            Self::NotRun => "not run",
        }
    }
}

/// Status and human-auditable context for one gauntlet cell.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Status {
    pub outcome: Outcome,
    pub detail: String,
}

impl Status {
    pub fn new(outcome: Outcome, detail: impl Into<String>) -> Self {
        Self {
            outcome,
            detail: detail.into(),
        }
    }

    pub fn not_run() -> Self {
        Self::new(Outcome::NotRun, "")
    }

    fn markdown(&self) -> String {
        if self.detail.is_empty() {
            self.outcome.label().to_owned()
        } else {
            format!("{} — {}", self.outcome.label(), escape_cell(&self.detail))
        }
    }
}

/// External-checker rungs for one backend.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LinKvResults {
    pub smoke: Status,
    pub kill: Status,
    pub partition: Status,
}

impl Default for LinKvResults {
    fn default() -> Self {
        Self {
            smoke: Status::not_run(),
            kill: Status::not_run(),
            partition: Status::not_run(),
        }
    }
}

/// All collected results for one backend.
#[derive(Clone, Debug)]
pub struct BackendReport {
    pub backend: BackendId,
    pub lin_kv: LinKvResults,
    pub perf_status: Status,
    pub performance: Option<PerfSummary>,
    pub census_status: Status,
    pub census: Option<Census>,
}

impl BackendReport {
    fn empty(backend: BackendId) -> Self {
        let capabilities = backend.capabilities();
        let partition = if capabilities.supports_partition_nemesis {
            Status::not_run()
        } else {
            Status::new(
                Outcome::CapabilityGap,
                "backend network policy is fixed to fail-stop",
            )
        };
        Self {
            backend,
            lin_kv: LinKvResults {
                partition,
                ..LinKvResults::default()
            },
            perf_status: Status::not_run(),
            performance: None,
            census_status: Status::not_run(),
            census: None,
        }
    }
}

/// One complete, paste-ready gauntlet report.
#[derive(Clone, Debug)]
pub struct GauntletReport {
    pub environment: Environment,
    /// Always initialized in [`BackendId::ALL`] order.
    pub backends: Vec<BackendReport>,
    /// Optional classic concurrency saturation curves, one per runnable backend.
    pub saturation_curves: Vec<crate::perf::SaturationCurve>,
    /// Evidence-bearing deep trust ledgers. Missing classifications remain
    /// explicit and are distinct from the shallow mechanical census.
    pub trust_ledgers: Vec<crate::trust::TrustLedger>,
    /// Pinned qualitative verdicts produced by isolated external reviewer
    /// invocations. Missing backends remain visibly unreviewed.
    pub reviews: Vec<crate::review::ReviewArtifact>,
}

impl GauntletReport {
    pub fn new(environment: Environment) -> Self {
        Self {
            environment,
            backends: BackendId::ALL
                .into_iter()
                .map(BackendReport::empty)
                .collect(),
            saturation_curves: Vec::new(),
            trust_ledgers: BackendId::ALL
                .into_iter()
                .map(crate::trust::ledger_for_backend)
                .collect(),
            reviews: Vec::new(),
        }
    }

    pub fn backend_mut(&mut self, backend: BackendId) -> &mut BackendReport {
        self.backends
            .iter_mut()
            .find(|row| row.backend == backend)
            .expect("all backend rows are installed by GauntletReport::new")
    }

    /// Render stable Markdown without timestamps or environment discovery hidden
    /// inside the formatter. Callers supply all non-deterministic metadata.
    pub fn to_markdown(&self) -> String {
        let mut out = String::new();
        writeln!(out, "# Consensus gauntlet report\n").unwrap();
        writeln!(out, "- **Date:** {}", self.environment.date).unwrap();
        writeln!(out, "- **Commit:** `{}`", self.environment.commit).unwrap();
        writeln!(out, "- **Host:** {}", self.environment.host).unwrap();
        writeln!(
            out,
            "- **Execution target:** {}",
            self.environment.execution.target
        )
        .unwrap();
        if let Some(region) = &self.environment.execution.region {
            writeln!(out, "- **Region:** {}", region).unwrap();
        }
        if let Some(cluster) = &self.environment.execution.ecs_cluster {
            writeln!(out, "- **ECS cluster:** {}", cluster).unwrap();
        }
        if let Some(cpu) = self.environment.execution.task_cpu {
            writeln!(out, "- **Task CPU:** {}", cpu).unwrap();
        }
        if let Some(memory) = self.environment.execution.task_memory_mib {
            writeln!(out, "- **Task memory:** {} MiB", memory).unwrap();
        }
        if let Some(digest) = &self.environment.execution.image_digest {
            writeln!(out, "- **Image digest:** `{}`", digest).unwrap();
        }

        out.push_str("\n## Backend capabilities\n\n");
        out.push_str("| backend | partition nemesis | timers | checkpointing | consistency output | S5 forwarded obligations |\n");
        out.push_str("|---|---:|---|---|---|---:|\n");
        for row in &self.backends {
            let cap = row.backend.capabilities();
            writeln!(
                out,
                "| {} | {} | {} | {} | {} | {} |",
                row.backend,
                yes_no(cap.supports_partition_nemesis),
                timer_list(cap.timers),
                checkpointing(cap.checkpointing),
                consistency(cap.consistency_output),
                cap.forwarded_nondet_obligations,
            )
            .unwrap();
        }

        out.push_str("\n## Tier 1: Maelstrom lin-kv ladder\n\n");
        out.push_str("| backend | smoke (20 s) | kill (60 s × 3) | partition (45 s × 3) |\n");
        out.push_str("|---|---|---|---|\n");
        for row in &self.backends {
            writeln!(
                out,
                "| {} | {} | {} | {} |",
                row.backend,
                row.lin_kv.smoke.markdown(),
                row.lin_kv.kill.markdown(),
                row.lin_kv.partition.markdown(),
            )
            .unwrap();
        }

        out.push_str("\n## Tier 2: performance\n\n");
        out.push_str(
            "The first 3 windows are warmup; statistics use exactly the following 12 windows.\n\n",
        );
        out.push_str("| backend | status | min req/s | median req/s | mean req/s | max req/s |\n");
        out.push_str("|---|---|---:|---:|---:|---:|\n");
        for row in &self.backends {
            if let Some(perf) = &row.performance {
                writeln!(
                    out,
                    "| {} | {} | {:.2} | {:.2} | {:.2} | {:.2} |",
                    row.backend,
                    row.perf_status.markdown(),
                    perf.throughput.min,
                    perf.throughput.median,
                    perf.throughput.mean,
                    perf.throughput.max,
                )
                .unwrap();
            } else {
                writeln!(
                    out,
                    "| {} | {} | — | — | — | — |",
                    row.backend,
                    row.perf_status.markdown(),
                )
                .unwrap();
            }
        }

        for row in self.backends.iter().filter(|row| row.performance.is_some()) {
            let perf = row.performance.as_ref().unwrap();
            writeln!(out, "\n### {} window growth\n", row.backend).unwrap();
            out.push_str(
                "| window | phase | throughput req/s | p50 ms | p99 ms | p99.9 ms | samples |\n",
            );
            out.push_str("|---:|---|---:|---:|---:|---:|---:|\n");
            for (index, window) in perf.windows.iter().enumerate() {
                let phase = if index < perf.config.warmup_windows {
                    "warmup"
                } else {
                    "steady"
                };
                writeln!(
                    out,
                    "| {} | {} | {:.2} | {:.3} | {:.3} | {:.3} | {} |",
                    window.sequence,
                    phase,
                    window.throughput_rps,
                    window.p50_ms,
                    window.p99_ms,
                    window.p999_ms,
                    window.samples,
                )
                .unwrap();
            }
        }

        out.push_str("\n## Tier 3: complexity and trust census\n\n");
        out.push_str(
            "| backend | status | body LOC | S1 | S2 | S3 | S4 | S5 | kernels | cuts | cycles |\n",
        );
        out.push_str("|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|\n");
        for row in &self.backends {
            if let Some(census) = &row.census {
                writeln!(
                    out,
                    "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
                    row.backend,
                    row.census_status.markdown(),
                    census.body_loc,
                    census.consistency_mints,
                    census.algebra_proofs,
                    census.assumers,
                    census.introducer_nondets,
                    census.forwarded_nondet_params,
                    census.kernels,
                    census.cuts,
                    census.cycles,
                )
                .unwrap();
            } else {
                writeln!(
                    out,
                    "| {} | {} | — | — | — | — | — | — | — | — | — |",
                    row.backend,
                    row.census_status.markdown(),
                )
                .unwrap();
            }
        }
        out.push_str("\n## Sequestered qualitative review\n\n");
        out.push_str("Each verdict is an independent blind read of one current implementation. The reviewer saw no gauntlet measurements, trust ledger, previous verdict, or competing implementation. Historical research notes were labeled old context, not current evidence. A missing verdict means not reviewed.\n\n");
        out.push_str("| backend | readability | checkability | research alignment | model |\n");
        out.push_str("|---|---|---|---|---|\n");
        for backend in BackendId::ALL {
            if let Some(review) = self
                .reviews
                .iter()
                .find(|review| review.backend == backend.as_str())
            {
                writeln!(
                    out,
                    "| {} | {} | {} | {} | {} |",
                    backend,
                    review.verdict.readability.rating.label(),
                    review.verdict.checkability.rating.label(),
                    review.verdict.research_alignment.alignment.label(),
                    escape_cell(&review.actual_model),
                )
                .unwrap();
            } else {
                writeln!(
                    out,
                    "| {} | not reviewed | not reviewed | not reviewed | — |",
                    backend
                )
                .unwrap();
            }
        }
        for review in &self.reviews {
            let verdict = &review.verdict;
            writeln!(out, "\n### {} reviewer verdict\n", review.backend).unwrap();
            writeln!(
                out,
                "- **Readability — {}:** {} {}",
                verdict.readability.rating.label(),
                verdict.readability.rationale,
                markdown_citations(&review.source.path, &verdict.readability.citations),
            )
            .unwrap();
            writeln!(
                out,
                "- **Checkability — {}:** {} {}",
                verdict.checkability.rating.label(),
                verdict.checkability.rationale,
                markdown_citations(&review.source.path, &verdict.checkability.citations),
            )
            .unwrap();
            writeln!(
                out,
                "- **Research alignment — {}:** {} {}",
                verdict.research_alignment.alignment.label(),
                verdict.research_alignment.rationale,
                markdown_citations(&review.source.path, &verdict.research_alignment.citations),
            )
            .unwrap();
            if !verdict.guarantees.is_empty() {
                out.push_str(
                    "\n| claimed guarantee | verdict | rationale | evidence |\n|---|---|---|---|\n",
                );
                for guarantee in &verdict.guarantees {
                    writeln!(
                        out,
                        "| {} | {} | {} | {} |",
                        escape_cell(&guarantee.guarantee),
                        guarantee.status.label(),
                        escape_cell(&guarantee.rationale),
                        escape_cell(&markdown_citations(
                            &review.source.path,
                            &guarantee.citations
                        )),
                    )
                    .unwrap();
                }
            }
            writeln!(
                out,
                "\nArtifact provenance: requested model `{}`, actual model `{}`, prompt SHA-256 `{}`, response SHA-256 `{}`.",
                review.requested_model, review.actual_model, review.prompt_sha256, review.response_sha256
            )
            .unwrap();
        }
        out
    }
}

impl fmt::Display for GauntletReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_markdown())
    }
}

fn markdown_citations(path: &str, citations: &[crate::review::SourceCitation]) -> String {
    citations
        .iter()
        .map(|citation| {
            format!(
                "`{}:{}-{}` ({})",
                path,
                citation.start_line,
                citation.end_line,
                citation.note.replace('\n', " ")
            )
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

fn timer_list(timers: &[TimerInput]) -> String {
    timers
        .iter()
        .map(|timer| match timer {
            TimerInput::Election => "election",
            TimerInput::Heartbeat => "heartbeat",
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn checkpointing(value: Checkpointing) -> &'static str {
    match value {
        Checkpointing::Unsupported => "unsupported",
        Checkpointing::External => "external",
        Checkpointing::Internal => "internal",
    }
}

fn consistency(value: ConsistencyOutput) -> &'static str {
    match value {
        ConsistencyOutput::Asserted => "asserted",
        ConsistencyOutput::Inferred => "inferred",
        ConsistencyOutput::Unlabeled => "unlabeled",
    }
}

fn escape_cell(value: &str) -> String {
    value.replace('|', "\\|").replace('\n', " ")
}

#[cfg(test)]
mod tests {
    use crate::perf::{ExecutionMetadata, PerfConfig, PerfSummary, WindowMetrics};

    use super::*;

    fn environment() -> Environment {
        Environment {
            host: "test-host".to_owned(),
            date: "2026-08-31".to_owned(),
            commit: "abc123".to_owned(),
            execution: ExecutionMetadata::localhost("test-host"),
        }
    }

    fn perf() -> PerfSummary {
        let windows = (0..15)
            .map(|sequence| WindowMetrics {
                sequence,
                throughput_rps: 1_000.0 + sequence as f64,
                p50_ms: sequence as f64,
                p99_ms: sequence as f64 + 1.0,
                p999_ms: sequence as f64 + 2.0,
                samples: 100,
            })
            .collect();
        PerfSummary::new(windows, PerfConfig::default()).unwrap()
    }

    #[test]
    fn constructor_installs_all_backends_and_real_capability_gaps() {
        let report = GauntletReport::new(environment());
        assert_eq!(report.backends.len(), BackendId::ALL.len());
        assert_eq!(report.backends[0].backend, BackendId::Raft);
        assert_eq!(
            report.backends.last().unwrap().backend,
            BackendId::QuorumLadderConsensus
        );
        assert_eq!(
            report
                .backends
                .iter()
                .find(|row| row.backend == BackendId::QuorumLadderConsensus)
                .unwrap()
                .lin_kv
                .partition
                .outcome,
            Outcome::CapabilityGap
        );
    }

    #[test]
    fn markdown_contains_every_tier_and_the_full_growth_curve() {
        let mut report = GauntletReport::new(environment());
        let raft = report.backend_mut(BackendId::Raft);
        raft.lin_kv.smoke = Status::new(Outcome::Passed, "Knossos valid");
        raft.perf_status = Status::new(Outcome::Passed, "localhost");
        raft.performance = Some(perf());
        raft.census_status = Status::new(Outcome::Passed, "mechanical");
        raft.census = Some(Census {
            body_loc: 1151,
            consistency_mints: 1,
            algebra_proofs: 1,
            assumers: 2,
            introducer_nondets: 6,
            forwarded_nondet_params: 2,
            kernels: 3,
            kernel_total_loc: 119,
            kernel_largest_loc: 119,
            cuts: 0,
            cycles: 3,
        });

        let markdown = report.to_markdown();
        assert!(markdown.contains("## Backend capabilities"));
        assert!(markdown.contains("## Tier 1: Maelstrom lin-kv ladder"));
        assert!(markdown.contains("## Tier 2: performance"));
        assert!(markdown.contains("## Tier 3: complexity and trust census"));
        assert!(markdown.contains("pass — Knossos valid"));
        assert!(markdown.contains("quorum-ladder-consensus | no"));
        assert!(markdown.contains("capability gap — backend network policy is fixed to fail-stop"));
        assert!(markdown.contains("### raft window growth"));
        assert!(markdown.contains("| 0 | warmup |"));
        assert!(markdown.contains("| 3 | steady |"));
        assert!(markdown.contains("| 14 | steady |"));
        assert!(
            markdown
                .contains("| raft | pass — mechanical | 1151 | 1 | 1 | 2 | 6 | 2 | 3 | 0 | 3 |")
        );
    }

    #[test]
    fn report_escapes_status_details_in_table_cells() {
        let mut report = GauntletReport::new(environment());
        report.backend_mut(BackendId::LibraryPaxos).lin_kv.smoke =
            Status::new(Outcome::Skipped, "needs A|B\nand adapter");
        let markdown = report.to_markdown();
        assert!(markdown.contains("skipped — needs A\\|B and adapter"));
    }
}
