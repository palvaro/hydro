//! Single registry for backend identity, tier adapters, and executable build probes.
//!
//! Tier runners dispatch through the adapter enums here rather than duplicating
//! backend matches. A registry entry may remain present even when its source is
//! disabled: the build tier then runs an isolated compile probe and records the
//! compiler's real result.

use std::fmt;
use std::path::Path;
use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::backend::{BackendCapabilities, BackendId, BuildStatus, SupportStatus};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PerformanceAdapter {
    CommonColocated,
    LibraryPaxos,
    CompartmentalizedPaxos,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MaelstromAdapter {
    Raft,
    BroadcastTranscript,
    QuorumLadderConsensus,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BuildProbe {
    /// Compiles the package which currently exposes this enabled module.
    HydroTest,
    /// Compiles the package which currently exposes this enabled module.
    HydroStd,
    /// Compiles a disabled module in a tiny generated crate so its failure is
    /// measured rather than copied from stale prose.
    DisabledHydroTestModule { module: &'static str },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BackendRegistration {
    pub id: BackendId,
    pub capabilities: BackendCapabilities,
    pub build_probe: BuildProbe,
    pub performance: Result<PerformanceAdapter, &'static str>,
    pub maelstrom: Result<MaelstromAdapter, &'static str>,
}

impl BackendRegistration {
    pub const fn supports_performance(self) -> bool {
        self.performance.is_ok()
    }

    pub const fn supports_maelstrom(self) -> bool {
        self.maelstrom.is_ok()
    }
}

pub const REGISTRY: [BackendRegistration; 7] = [
    registration(
        BackendId::Raft,
        BuildProbe::HydroTest,
        Ok(PerformanceAdapter::CommonColocated),
        Ok(MaelstromAdapter::Raft),
    ),
    registration(
        BackendId::LibraryPaxos,
        BuildProbe::HydroTest,
        Ok(PerformanceAdapter::LibraryPaxos),
        Err("current Maelstrom deployment supports one logical cluster"),
    ),
    registration(
        BackendId::CompartmentalizedPaxos,
        BuildProbe::HydroTest,
        Ok(PerformanceAdapter::CompartmentalizedPaxos),
        Err("current Maelstrom deployment supports one logical cluster"),
    ),
    registration(
        BackendId::BroadcastTranscript,
        BuildProbe::HydroTest,
        Ok(PerformanceAdapter::CommonColocated),
        Ok(MaelstromAdapter::BroadcastTranscript),
    ),
    registration(
        BackendId::PaxosEc,
        BuildProbe::DisabledHydroTestModule { module: "paxos_ec" },
        Err("backend does not currently compile"),
        Err("backend does not currently compile"),
    ),
    registration(
        BackendId::TypedConsensus,
        BuildProbe::DisabledHydroTestModule {
            module: "typed_consensus",
        },
        Err("backend does not currently compile"),
        Err("backend does not currently compile"),
    ),
    registration(
        BackendId::QuorumLadderConsensus,
        BuildProbe::HydroStd,
        Ok(PerformanceAdapter::CommonColocated),
        Ok(MaelstromAdapter::QuorumLadderConsensus),
    ),
];

const fn registration(
    id: BackendId,
    build_probe: BuildProbe,
    performance: Result<PerformanceAdapter, &'static str>,
    maelstrom: Result<MaelstromAdapter, &'static str>,
) -> BackendRegistration {
    BackendRegistration {
        id,
        capabilities: id.capabilities(),
        build_probe,
        performance,
        maelstrom,
    }
}

pub fn backend(id: BackendId) -> &'static BackendRegistration {
    REGISTRY
        .iter()
        .find(|entry| entry.id == id)
        .expect("every BackendId must have exactly one registry entry")
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BuildOutcome {
    Passed,
    Failed { diagnostics: String },
    HarnessError { detail: String },
}

impl BuildOutcome {
    pub const fn passed(&self) -> bool {
        matches!(self, Self::Passed)
    }
}

impl fmt::Display for BuildOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Passed => f.write_str("passed"),
            Self::Failed { diagnostics } => write!(f, "failed: {diagnostics}"),
            Self::HarnessError { detail } => write!(f, "probe error: {detail}"),
        }
    }
}

pub fn run_build_probe(id: BackendId) -> BuildOutcome {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("gauntlet crate is in workspace root");
    match backend(id).build_probe {
        BuildProbe::HydroTest => run_workspace_check(root, "hydro_test"),
        BuildProbe::HydroStd => run_workspace_check(root, "hydro_std"),
        BuildProbe::DisabledHydroTestModule { module } => probe_disabled_module(root, module),
    }
}

fn run_workspace_check(root: &Path, package: &str) -> BuildOutcome {
    match Command::new("cargo")
        .args(["check", "--quiet", "-p", package, "--lib"])
        .current_dir(root)
        .output()
    {
        Ok(output) => outcome(output),
        Err(error) => BuildOutcome::HarnessError {
            detail: format!("failed to launch cargo for {package}: {error}"),
        },
    }
}

fn probe_disabled_module(root: &Path, module: &str) -> BuildOutcome {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let probe = root
        .join("target")
        .join("consensus-gauntlet-build-probes")
        .join(format!("{module}-{nonce}"));
    let src = probe.join("src");
    if let Err(error) = std::fs::create_dir_all(&src) {
        return BuildOutcome::HarnessError {
            detail: format!("create probe directory: {error}"),
        };
    }
    let hydro_lang = root.join("hydro_lang");
    let source = root
        .join("hydro_test/src/cluster")
        .join(format!("{module}.rs"));
    let manifest = format!(
        "[workspace]\n[package]\nname = \"gauntlet-{module}-probe\"\nversion = \"0.0.0\"\nedition = \"2024\"\n\n[dependencies]\nhydro_lang = {{ path = {:?}, features = [\"deploy\"] }}\nserde = {{ version = \"1\", features = [\"derive\"] }}\n",
        hydro_lang
    );
    let main = format!(
        "#![allow(dead_code)]\nmod cluster {{ #[path = {:?}] pub mod {module}; }}\nfn main() {{}}\n",
        source
    );
    let written = std::fs::write(probe.join("Cargo.toml"), manifest)
        .and_then(|()| std::fs::write(src.join("main.rs"), main));
    if let Err(error) = written {
        let _ = std::fs::remove_dir_all(&probe);
        return BuildOutcome::HarnessError {
            detail: format!("write probe crate: {error}"),
        };
    }
    let result = Command::new("cargo")
        .args(["check", "--quiet"])
        .current_dir(&probe)
        .env(
            "CARGO_TARGET_DIR",
            root.join("target/consensus-gauntlet-probe-target"),
        )
        .output();
    let _ = std::fs::remove_dir_all(&probe);
    match result {
        Ok(output) => outcome(output),
        Err(error) => BuildOutcome::HarnessError {
            detail: format!("failed to launch disabled-module probe: {error}"),
        },
    }
}

fn outcome(output: Output) -> BuildOutcome {
    if output.status.success() {
        BuildOutcome::Passed
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let diagnostics = stderr
            .lines()
            .filter(|line| line.contains("error") || line.trim_start().starts_with("-->"))
            .take(12)
            .collect::<Vec<_>>()
            .join("\n");
        BuildOutcome::Failed {
            diagnostics: if diagnostics.is_empty() {
                format!("cargo exited with {}", output.status)
            } else {
                diagnostics
            },
        }
    }
}

/// Registry-derived support declaration used by tier planning.
pub const fn maelstrom_support(id: BackendId) -> SupportStatus {
    id.capabilities().maelstrom
}

/// Registry-derived build declaration, retained separately from the executable
/// probe result so stale declarations can be detected.
pub const fn declared_build(id: BackendId) -> BuildStatus {
    id.capabilities().build
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_is_total_unique_and_in_backend_order() {
        assert_eq!(REGISTRY.len(), BackendId::ALL.len());
        for (entry, id) in REGISTRY.iter().zip(BackendId::ALL) {
            assert_eq!(entry.id, id);
            assert_eq!(backend(id), entry);
            assert_eq!(entry.capabilities, id.capabilities());
        }
    }

    #[test]
    fn adapter_dispatch_matches_portfolio() {
        assert_eq!(
            backend(BackendId::Raft).maelstrom,
            Ok(MaelstromAdapter::Raft)
        );
        assert_eq!(
            backend(BackendId::LibraryPaxos).performance,
            Ok(PerformanceAdapter::LibraryPaxos)
        );
        assert_eq!(
            backend(BackendId::CompartmentalizedPaxos).performance,
            Ok(PerformanceAdapter::CompartmentalizedPaxos)
        );
        assert!(!backend(BackendId::PaxosEc).supports_performance());
        assert!(!backend(BackendId::TypedConsensus).supports_maelstrom());
    }

    #[test]
    fn disabled_sources_are_real_compile_failures() {
        let paxos = run_build_probe(BackendId::PaxosEc);
        assert!(matches!(paxos, BuildOutcome::Failed { .. }), "{paxos}");
        let typed = run_build_probe(BackendId::TypedConsensus);
        assert!(matches!(typed, BuildOutcome::Failed { .. }), "{typed}");
    }
}
