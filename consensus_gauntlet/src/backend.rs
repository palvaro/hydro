//! Backend identities, declared capability gaps, and canonical committed-log helpers.
//!
//! This module intentionally contains no Hydro graph construction.  The deployed
//! adapters can therefore share these declarations with report generation and
//! unit tests without coupling either to a particular execution target.

use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;

/// Consensus artifacts included in the gauntlet portfolio.
///
/// Disabled sources remain identities so build failures and source-level trust
/// measurements appear in reports instead of disappearing from comparison.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum BackendId {
    Raft,
    LibraryPaxos,
    CompartmentalizedPaxos,
    BroadcastTranscript,
    PaxosEc,
    TypedConsensus,
    QuorumLadderConsensus,
}

impl BackendId {
    pub const ALL: [Self; 7] = [
        Self::Raft,
        Self::LibraryPaxos,
        Self::CompartmentalizedPaxos,
        Self::BroadcastTranscript,
        Self::PaxosEc,
        Self::TypedConsensus,
        Self::QuorumLadderConsensus,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Raft => "raft",
            Self::LibraryPaxos => "library-paxos",
            Self::CompartmentalizedPaxos => "compartmentalized-paxos",
            Self::BroadcastTranscript => "broadcast-transcript",
            Self::PaxosEc => "paxos-ec",
            Self::TypedConsensus => "typed-consensus",
            Self::QuorumLadderConsensus => "quorum-ladder-consensus",
        }
    }

    pub const fn source_path(self) -> &'static str {
        match self {
            Self::Raft => "hydro_test/src/cluster/raft.rs",
            Self::LibraryPaxos => "hydro_test/src/cluster/paxos.rs",
            Self::CompartmentalizedPaxos => "hydro_test/src/cluster/compartmentalized_paxos.rs",
            Self::BroadcastTranscript => "hydro_test/src/cluster/broadcast_transcript_consensus.rs",
            Self::PaxosEc => "hydro_test/src/cluster/paxos_ec.rs",
            Self::TypedConsensus => "hydro_test/src/cluster/typed_consensus.rs",
            Self::QuorumLadderConsensus => "hydro_std/src/ec_inference_demos/multi_paxos.rs",
        }
    }

    /// Capabilities are declarations, not promises synthesized by the harness.
    pub const fn capabilities(self) -> BackendCapabilities {
        match self {
            Self::Raft => BackendCapabilities::new(
                true,
                &[TimerInput::Election, TimerInput::Heartbeat],
                Checkpointing::Unsupported,
                ConsistencyOutput::Asserted,
                2,
                BuildStatus::Builds,
                Topology::Colocated,
                SupportStatus::Supported,
                SupportStatus::Supported,
            ),
            Self::LibraryPaxos => BackendCapabilities::new(
                false,
                &[TimerInput::Election],
                Checkpointing::External,
                ConsistencyOutput::Unlabeled,
                8,
                BuildStatus::Builds,
                Topology::ProposersAcceptorsReplicas,
                SupportStatus::Gap("current Maelstrom deployment supports one logical cluster"),
                SupportStatus::Supported,
            ),
            Self::CompartmentalizedPaxos => BackendCapabilities::new(
                false,
                &[TimerInput::Election],
                Checkpointing::External,
                ConsistencyOutput::Unlabeled,
                5,
                BuildStatus::Builds,
                Topology::Compartmentalized,
                SupportStatus::Gap("current Maelstrom deployment supports one logical cluster"),
                SupportStatus::Supported,
            ),
            Self::BroadcastTranscript => BackendCapabilities::new(
                true,
                &[TimerInput::Election],
                Checkpointing::Internal,
                ConsistencyOutput::Asserted,
                1,
                BuildStatus::Builds,
                Topology::Colocated,
                SupportStatus::Supported,
                SupportStatus::Supported,
            ),
            Self::PaxosEc => BackendCapabilities::new(
                false,
                &[TimerInput::Election],
                Checkpointing::Internal,
                ConsistencyOutput::Asserted,
                0,
                BuildStatus::Broken("disabled: uses removed broadcast_from_member API"),
                Topology::Colocated,
                SupportStatus::Gap("backend does not currently compile"),
                SupportStatus::Gap("backend does not currently compile"),
            ),
            Self::TypedConsensus => BackendCapabilities::new(
                false,
                &[TimerInput::Election],
                Checkpointing::Internal,
                ConsistencyOutput::Asserted,
                0,
                BuildStatus::Broken(
                    "disabled: removed broadcast_from_member API and T/usize mismatch",
                ),
                Topology::Colocated,
                SupportStatus::Gap("backend does not currently compile"),
                SupportStatus::Gap("backend does not currently compile"),
            ),
            Self::QuorumLadderConsensus => BackendCapabilities::new(
                false,
                &[TimerInput::Election],
                Checkpointing::Unsupported,
                ConsistencyOutput::Inferred,
                0,
                BuildStatus::Builds,
                Topology::Colocated,
                SupportStatus::Partial("partition unsupported: core links are fail-stop"),
                SupportStatus::Supported,
            ),
        }
    }
}

impl fmt::Display for BackendId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Timer streams that a backend adapter requires the harness to drive.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum TimerInput {
    Election,
    Heartbeat,
}

/// How a backend bounds committed-log state.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Checkpointing {
    Unsupported,
    /// The protocol consumes an applied-slot checkpoint from its caller.
    External,
    /// The protocol truncates its own state without a harness feedback stream.
    Internal,
}

/// Provenance of the consistency label on a backend's native output.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ConsistencyOutput {
    /// A protocol-local consistency assertion mints the label.
    Asserted,
    /// The label follows from typed composition without a local assertion.
    Inferred,
    /// The backend deliberately makes no consistency claim in its output type.
    Unlabeled,
}

/// Whether the current source is part of the build.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BuildStatus {
    Builds,
    Broken(&'static str),
}

/// Logical deployment shape; important for fair resource accounting.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Topology {
    Colocated,
    ProposersAcceptorsReplicas,
    Compartmentalized,
}

/// Adapter coverage. Partial and gap reasons are report data.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SupportStatus {
    Supported,
    Partial(&'static str),
    Gap(&'static str),
}

/// Differences that the report must expose rather than smoothing over.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BackendCapabilities {
    pub supports_partition_nemesis: bool,
    pub timers: &'static [TimerInput],
    pub checkpointing: Checkpointing,
    pub consistency_output: ConsistencyOutput,
    /// Tier-0 S5 count in the protocol body.  In particular, library Paxos's
    /// unlabeled output still forwards these proof obligations to its caller.
    pub forwarded_nondet_obligations: usize,
    pub build: BuildStatus,
    pub topology: Topology,
    pub maelstrom: SupportStatus,
    pub performance: SupportStatus,
}

impl BackendCapabilities {
    #[expect(
        clippy::too_many_arguments,
        reason = "capability profile is intentionally explicit"
    )]
    const fn new(
        supports_partition_nemesis: bool,
        timers: &'static [TimerInput],
        checkpointing: Checkpointing,
        consistency_output: ConsistencyOutput,
        forwarded_nondet_obligations: usize,
        build: BuildStatus,
        topology: Topology,
        maelstrom: SupportStatus,
        performance: SupportStatus,
    ) -> Self {
        Self {
            supports_partition_nemesis,
            timers,
            checkpointing,
            consistency_output,
            forwarded_nondet_obligations,
            build,
            topology,
            maelstrom,
            performance,
        }
    }
}

/// Canonical committed-log representation used at the adapter boundary.
///
/// `None` is a protocol no-op or a duplicate request suppressed by the state
/// machine; retaining its slot keeps the committed log contiguous.
pub type CanonicalEntry<V> = (usize, Option<V>);

/// Canonicalize a backend entry which always contains an application value.
pub fn committed_value<V>(slot: usize, value: V) -> CanonicalEntry<V> {
    (slot, Some(value))
}

/// Canonicalize a backend entry that already represents protocol no-ops.
pub fn committed_optional<V>(slot: usize, value: Option<V>) -> CanonicalEntry<V> {
    (slot, value)
}

/// Canonicalize an iterator of ordinary slotted values.
pub fn canonicalize_values<V>(
    entries: impl IntoIterator<Item = (usize, V)>,
) -> Vec<CanonicalEntry<V>> {
    entries
        .into_iter()
        .map(|(slot, value)| committed_value(slot, value))
        .collect()
}

/// Suppress redo-log duplicates while retaining every canonical slot.
///
/// Quorum-Ladder Consensus may legitimately choose one client request at several
/// slots: its redo queue can release work again before observing the first
/// completion.  State-machine application keeps the value at the *lowest slot*
/// and changes later occurrences to `None`.  This is based on slot, not arrival
/// order, so an unordered committed stream is canonicalized deterministically.
/// Existing protocol no-ops remain `None` and do not participate in dedup.
pub fn deduplicate_redo_entries<V, RequestId>(
    entries: impl IntoIterator<Item = CanonicalEntry<V>>,
    request_id: impl Fn(&V) -> RequestId,
) -> Vec<CanonicalEntry<V>>
where
    RequestId: Eq + Hash,
{
    let entries: Vec<_> = entries.into_iter().collect();
    let mut earliest = HashMap::<RequestId, usize>::new();

    for (slot, value) in &entries {
        if let Some(value) = value {
            earliest
                .entry(request_id(value))
                .and_modify(|earliest_slot| *earliest_slot = (*earliest_slot).min(*slot))
                .or_insert(*slot);
        }
    }

    entries
        .into_iter()
        .map(|(slot, value)| {
            let value = value.filter(|value| earliest.get(&request_id(value)) == Some(&slot));
            (slot, value)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct Request {
        client: u32,
        message: u32,
        payload: &'static str,
    }

    fn request(client: u32, message: u32, payload: &'static str) -> Request {
        Request {
            client,
            message,
            payload,
        }
    }

    #[test]
    fn all_backends_have_honest_capability_declarations() {
        assert_eq!(BackendId::ALL.len(), 7);
        assert!(BackendId::Raft.capabilities().supports_partition_nemesis);
        assert_eq!(
            BackendId::Raft.capabilities().timers,
            &[TimerInput::Election, TimerInput::Heartbeat]
        );

        let paxos = BackendId::LibraryPaxos.capabilities();
        assert_eq!(paxos.consistency_output, ConsistencyOutput::Unlabeled);
        assert_eq!(paxos.forwarded_nondet_obligations, 8);
        assert_eq!(paxos.checkpointing, Checkpointing::External);

        let compartmentalized = BackendId::CompartmentalizedPaxos.capabilities();
        assert_eq!(compartmentalized.build, BuildStatus::Builds);
        assert_eq!(compartmentalized.topology, Topology::Compartmentalized);
        assert!(matches!(compartmentalized.maelstrom, SupportStatus::Gap(_)));

        for broken in [BackendId::PaxosEc, BackendId::TypedConsensus] {
            let capabilities = broken.capabilities();
            assert!(matches!(capabilities.build, BuildStatus::Broken(_)));
            assert!(matches!(capabilities.performance, SupportStatus::Gap(_)));
        }

        let multi = BackendId::QuorumLadderConsensus.capabilities();
        assert!(!multi.supports_partition_nemesis);
        assert_eq!(multi.consistency_output, ConsistencyOutput::Inferred);
        assert_eq!(multi.checkpointing, Checkpointing::Unsupported);
    }

    #[test]
    fn canonicalization_preserves_slots_and_noops() {
        assert_eq!(
            canonicalize_values([(2, "b"), (1, "a")]),
            vec![(2, Some("b")), (1, Some("a"))]
        );
        assert_eq!(committed_optional::<u8>(7, None), (7, None));
    }

    #[test]
    fn redo_dedup_uses_earliest_slot_not_arrival_order() {
        let entries = vec![
            (8, Some(request(1, 4, "late redo"))),
            (3, Some(request(1, 4, "original"))),
            (4, Some(request(2, 9, "other"))),
        ];

        let got = deduplicate_redo_entries(entries, |r| (r.client, r.message));
        assert_eq!(got[0], (8, None));
        assert_eq!(got[1], (3, Some(request(1, 4, "original"))));
        assert_eq!(got[2], (4, Some(request(2, 9, "other"))));
    }

    #[test]
    fn redo_dedup_retains_slots_and_preexisting_noops() {
        let entries = vec![
            (0, None),
            (1, Some(request(7, 1, "first"))),
            (2, Some(request(7, 1, "retry"))),
            (3, None),
        ];
        let got = deduplicate_redo_entries(entries, |r| (r.client, r.message));

        assert_eq!(got.len(), 4);
        assert_eq!(got[0], (0, None));
        assert_eq!(got[1], (1, Some(request(7, 1, "first"))));
        assert_eq!(got[2], (2, None));
        assert_eq!(got[3], (3, None));
    }
}
