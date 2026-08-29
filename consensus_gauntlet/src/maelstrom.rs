//! Backend-neutral Maelstrom `lin-kv` gauntlet configuration.
//!
//! This module deliberately stops at a run plan. The deployment adapter owns
//! Hydro graph construction, while this plan guarantees that every supported
//! backend receives byte-for-byte equivalent workload settings. Unsupported
//! combinations remain explicit capability gaps instead of being weakened into
//! easier workloads.

use crate::backend::BackendId;

/// One rung of the external linearizability-checking ladder.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum LinKvRung {
    Smoke,
    Kill,
    Partition,
}

impl LinKvRung {
    pub const ALL: [Self; 3] = [Self::Smoke, Self::Kill, Self::Partition];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Smoke => "smoke",
            Self::Kill => "kill",
            Self::Partition => "partition",
        }
    }
}

/// Maelstrom nemesis selected for a rung.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Nemesis {
    Kill,
    Partition,
}

impl Nemesis {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Kill => "kill",
            Self::Partition => "partition",
        }
    }
}

/// Complete workload settings for one Maelstrom invocation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LinKvConfig {
    pub rung: LinKvRung,
    pub node_count: usize,
    pub time_limit_seconds: u64,
    pub rate: usize,
    pub concurrency: &'static str,
    pub nemesis: Option<Nemesis>,
    pub nemesis_interval_seconds: Option<u64>,
    pub repetitions: usize,
    /// A portable description of any prerequisite not expressible directly in
    /// the selected Maelstrom distribution.
    pub prerequisite: Option<&'static str>,
}

impl LinKvConfig {
    /// The standardized ladder copied from the existing `lin_kv.rs` tests.
    pub const fn for_rung(rung: LinKvRung) -> Self {
        match rung {
            LinKvRung::Smoke => Self {
                rung,
                node_count: 3,
                time_limit_seconds: 20,
                rate: 10,
                concurrency: "6n",
                nemesis: None,
                nemesis_interval_seconds: None,
                repetitions: 1,
                prerequisite: None,
            },
            LinKvRung::Kill => Self {
                rung,
                node_count: 3,
                time_limit_seconds: 60,
                rate: 30,
                concurrency: "12n",
                nemesis: Some(Nemesis::Kill),
                nemesis_interval_seconds: Some(15),
                repetitions: 3,
                prerequisite: Some(
                    "uses the checksum-pinned source-built Maelstrom v0.2.4 variant whose kill nemesis targets exactly one node",
                ),
            },
            LinKvRung::Partition => Self {
                rung,
                node_count: 3,
                time_limit_seconds: 45,
                rate: 30,
                concurrency: "12n",
                nemesis: Some(Nemesis::Partition),
                nemesis_interval_seconds: Some(5),
                repetitions: 3,
                prerequisite: None,
            },
        }
    }

    /// Arguments that supplement `MaelstromDeployment`'s workload, node-count,
    /// time-limit, rate, and nemesis builder methods.
    pub fn extra_args(&self) -> Vec<String> {
        let mut args = vec!["--concurrency".to_owned(), self.concurrency.to_owned()];
        if let Some(interval) = self.nemesis_interval_seconds {
            args.push("--nemesis-interval".to_owned());
            args.push(interval.to_string());
        }
        args
    }
}

/// Whether a backend/rung combination can be run by the current adapters.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PlannedRun {
    Ready(LinKvConfig),
    CapabilityGap {
        rung: LinKvRung,
        reason: &'static str,
    },
}

impl PlannedRun {
    pub const fn rung(&self) -> LinKvRung {
        match self {
            Self::Ready(config) => config.rung,
            Self::CapabilityGap { rung, .. } => *rung,
        }
    }
}

/// The complete correctness-tier plan for one backend.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendLinKvPlan {
    pub backend: BackendId,
    pub runs: Vec<PlannedRun>,
}

/// Construct the standardized ladder while preserving unsupported combinations
/// as first-class report rows.
pub fn plan_for_backend(backend: BackendId) -> BackendLinKvPlan {
    use crate::registry::{MaelstromAdapter, backend as registered};

    let runs = LinKvRung::ALL
        .into_iter()
            .map(|rung| {
                match registered(backend).maelstrom {
                    Err(reason) => return PlannedRun::CapabilityGap { rung, reason },
                    Ok(MaelstromAdapter::QuorumLadderConsensus)
                        if rung == LinKvRung::Partition =>
                    {
                    return PlannedRun::CapabilityGap {
                        rung,
                        reason: "the quorum-ladder safety core fixes its network links to fail-stop TCP",
                    };
                }
                Ok(_) => {}
            }
            PlannedRun::Ready(LinKvConfig::for_rung(rung))
        })
        .collect();

    BackendLinKvPlan { backend, runs }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ready_config(plan: &BackendLinKvPlan, rung: LinKvRung) -> &LinKvConfig {
        match plan.runs.iter().find(|run| run.rung() == rung).unwrap() {
            PlannedRun::Ready(config) => config,
            PlannedRun::CapabilityGap { reason, .. } => panic!("unexpected gap: {reason}"),
        }
    }

    #[test]
    fn ladder_has_exact_standardized_settings() {
        let smoke = LinKvConfig::for_rung(LinKvRung::Smoke);
        assert_eq!(
            (
                smoke.time_limit_seconds,
                smoke.rate,
                smoke.concurrency,
                smoke.repetitions
            ),
            (20, 10, "6n", 1)
        );
        assert_eq!(smoke.nemesis, None);

        let kill = LinKvConfig::for_rung(LinKvRung::Kill);
        assert_eq!(
            (
                kill.time_limit_seconds,
                kill.rate,
                kill.concurrency,
                kill.repetitions
            ),
            (60, 30, "12n", 3)
        );
        assert_eq!(kill.nemesis, Some(Nemesis::Kill));
        assert_eq!(kill.nemesis_interval_seconds, Some(15));
        assert!(kill.prerequisite.unwrap().contains("exactly one node"));

        let partition = LinKvConfig::for_rung(LinKvRung::Partition);
        assert_eq!(
            (
                partition.time_limit_seconds,
                partition.rate,
                partition.concurrency,
                partition.repetitions,
            ),
            (45, 30, "12n", 3)
        );
        assert_eq!(partition.nemesis, Some(Nemesis::Partition));
        assert_eq!(partition.nemesis_interval_seconds, Some(5));
    }

    #[test]
    fn supported_backends_receive_identical_configs() {
        let raft = plan_for_backend(BackendId::Raft);
        let btc = plan_for_backend(BackendId::BroadcastTranscript);
        for rung in LinKvRung::ALL {
            assert_eq!(ready_config(&raft, rung), ready_config(&btc, rung));
        }
    }

    #[test]
    fn library_paxos_exposes_topology_gap_for_every_rung() {
        let plan = plan_for_backend(BackendId::LibraryPaxos);
        assert_eq!(plan.runs.len(), 3);
        assert!(plan.runs.iter().all(|run| matches!(
            run,
            PlannedRun::CapabilityGap { reason, .. }
                if reason.contains("one logical cluster")
        )));
    }

    #[test]
    fn quorum_ladder_runs_smoke_and_kill_but_not_partition() {
        let plan = plan_for_backend(BackendId::QuorumLadderConsensus);
        assert!(matches!(plan.runs[0], PlannedRun::Ready(_)));
        assert!(matches!(plan.runs[1], PlannedRun::Ready(_)));
        assert!(matches!(
            plan.runs[2],
            PlannedRun::CapabilityGap { reason, .. } if reason.contains("fail-stop")
        ));
    }

    #[test]
    fn extra_args_are_runner_ready() {
        assert_eq!(
            LinKvConfig::for_rung(LinKvRung::Smoke).extra_args(),
            ["--concurrency", "6n"]
        );
        assert_eq!(
            LinKvConfig::for_rung(LinKvRung::Partition).extra_args(),
            ["--concurrency", "12n", "--nemesis-interval", "5"]
        );
    }
}
