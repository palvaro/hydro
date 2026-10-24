//! Evidence-bearing trust accounting for consensus artifacts.
//!
//! This module is deliberately separate from the mechanical token census. It
//! records only classifications already supported by source/design citations;
//! an unperformed reading pass is [`Assessed::Missing`], never a zero.

use crate::backend::BackendId;

/// The complete trust-seam taxonomy from the accounting design.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SeamKind {
    S1ConsistencyMint,
    S2AlgebraProof,
    S3Assumer,
    S4Introducer,
    S5ForwardedObligation,
    S6CallerContract,
    S7ConventionSeal,
    S8TrustedBaseImport,
}

impl SeamKind {
    pub const fn code(self) -> &'static str {
        match self {
            Self::S1ConsistencyMint => "S1",
            Self::S2AlgebraProof => "S2",
            Self::S3Assumer => "S3",
            Self::S4Introducer => "S4",
            Self::S5ForwardedObligation => "S5",
            Self::S6CallerContract => "S6",
            Self::S7ConventionSeal => "S7",
            Self::S8TrustedBaseImport => "S8",
        }
    }

    pub const fn is_local_consistency_mint(self) -> bool {
        matches!(self, Self::S1ConsistencyMint)
    }

    pub const fn is_contract_or_seal(self) -> bool {
        matches!(self, Self::S6CallerContract | Self::S7ConventionSeal)
    }
}

/// Ordinal scope of the code/contracts required to audit a seam.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum AntecedentScope {
    A0Closure,
    A1Combinator,
    A2Phase,
    A3CrossLocation,
    A4ContractDependent,
}

impl AntecedentScope {
    pub const fn code(self) -> &'static str {
        match self {
            Self::A0Closure => "A0",
            Self::A1Combinator => "A1",
            Self::A2Phase => "A2",
            Self::A3CrossLocation => "A3",
            Self::A4ContractDependent => "A4",
        }
    }
}

/// How an S3/S4/S5 nondeterministic choice is accounted for.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NondetClass {
    N0MechanicallyErased,
    N1ProseErased,
    N2NamedFreedom,
    N3UnaccountedEscape,
}

impl NondetClass {
    pub const fn code(self) -> &'static str {
        match self {
            Self::N0MechanicallyErased => "N0",
            Self::N1ProseErased => "N1",
            Self::N2NamedFreedom => "N2",
            Self::N3UnaccountedEscape => "N3",
        }
    }
}

/// Strongest evidence that actually touches an obligation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EvidenceGrade {
    E0Prose,
    E1Exhaustive,
    E1Fuzz,
    E2RedTested,
    E3Adversarial,
    E4TypeRefused,
}

impl EvidenceGrade {
    pub const fn code(self) -> &'static str {
        match self {
            Self::E0Prose => "E0",
            Self::E1Exhaustive => "E1e",
            Self::E1Fuzz => "E1f",
            Self::E2RedTested => "E2",
            Self::E3Adversarial => "E3",
            Self::E4TypeRefused => "E4",
        }
    }

    /// The accounting's contract headline is “E2+”. This is not presented as
    /// a total ordering of evidence types; it means a mechanical check at E2,
    /// E3, or E4 rather than green/prose evidence alone.
    pub const fn is_e2_or_stronger(self) -> bool {
        matches!(
            self,
            Self::E2RedTested | Self::E3Adversarial | Self::E4TypeRefused
        )
    }

    /// Mutation red coverage counts witnessed failures and compiler refusals.
    pub const fn is_red_covered(self) -> bool {
        matches!(self, Self::E2RedTested | Self::E4TypeRefused)
    }
}

/// Whether a field was produced mechanically or by an evidence-cited reading.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AssessmentProvenance {
    Mechanical,
    Manual,
}

/// Auditable source/design reference. Lines are the cited revision's lines and
/// may drift; `note` keeps the proposition independently findable.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Citation {
    pub path: &'static str,
    pub line: Option<usize>,
    pub note: &'static str,
}

/// A classified value or explicit missing data.
#[derive(Clone, Debug, PartialEq)]
pub enum Assessed<T> {
    Known {
        value: T,
        provenance: AssessmentProvenance,
        citations: Vec<Citation>,
    },
    Missing {
        reason: &'static str,
    },
}

impl<T> Assessed<T> {
    pub fn manual(value: T, citations: Vec<Citation>) -> Self {
        Self::Known {
            value,
            provenance: AssessmentProvenance::Manual,
            citations,
        }
    }

    pub fn mechanical(value: T, citations: Vec<Citation>) -> Self {
        Self::Known {
            value,
            provenance: AssessmentProvenance::Mechanical,
            citations,
        }
    }

    pub const fn missing(reason: &'static str) -> Self {
        Self::Missing { reason }
    }

    pub const fn value(&self) -> Option<&T> {
        match self {
            Self::Known { value, .. } => Some(value),
            Self::Missing { .. } => None,
        }
    }
}

/// One trust seam/contract with both antecedent and consequent scope.
#[derive(Clone, Debug, PartialEq)]
pub struct SeamRecord {
    pub id: &'static str,
    pub kind: SeamKind,
    pub proposition: &'static str,
    pub antecedent: Assessed<AntecedentScope>,
    /// Spec claims invalidated if the seam proposition is false.
    pub blast_scope: Assessed<Vec<&'static str>>,
    pub evidence: Assessed<EvidenceGrade>,
    /// Applicable to S3/S4/S5. Other seam kinds use `Known(None)` only when a
    /// reading explicitly establishes non-applicability.
    pub nondeterminism: Assessed<Option<NondetClass>>,
    pub justified_by: Vec<&'static str>,
    pub citations: Vec<Citation>,
}

/// Bill of materials for one typed or untyped protocol claim.
#[derive(Clone, Debug, PartialEq)]
pub struct ClaimBill {
    pub id: &'static str,
    pub claim: &'static str,
    pub typed: bool,
    pub seam_ids: Assessed<Vec<&'static str>>,
    pub citations: Vec<Citation>,
}

/// Graph-shape complexity. `None` means the IR pass has not supplied the value;
/// it must never render as zero.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GraphShape {
    pub operators: Option<usize>,
    pub edges: Option<usize>,
    pub locations: Option<usize>,
    pub message_types: Option<usize>,
    pub kernel_operators: Option<usize>,
    pub behavioral_executions: Option<u64>,
}

/// Full evidence ledger for one backend.
#[derive(Clone, Debug, PartialEq)]
pub struct TrustLedger {
    pub backend: BackendId,
    pub seams: Vec<SeamRecord>,
    pub claims: Vec<ClaimBill>,
    pub graph_shape: GraphShape,
    /// Explains scope limitations (for example, a pending reading pass).
    pub notes: Vec<&'static str>,
}

/// Derived profile. Unavailable metrics retain their reason rather than
/// silently becoming zero.
#[derive(Clone, Debug, PartialEq)]
pub struct TrustSummary {
    pub inference_ratio: Assessed<f64>,
    pub worst_gap: Assessed<AntecedentScope>,
    pub gap_distribution: Assessed<[usize; 5]>,
    pub trusted_surface_ratio: Assessed<f64>,
    pub contract_e2_ratio: Assessed<f64>,
    pub red_coverage: Assessed<f64>,
    pub nondet_counts: Assessed<[usize; 4]>,
}

impl TrustLedger {
    pub fn summary(&self) -> TrustSummary {
        let typed: Vec<_> = self.claims.iter().filter(|claim| claim.typed).collect();
        let inference_ratio = if typed.is_empty() {
            Assessed::missing("no typed correctness claims have been entered")
        } else if typed.iter().any(|claim| claim.seam_ids.value().is_none()) {
            Assessed::missing(
                "the evidence dependencies for one or more typed claims have not been classified",
            )
        } else {
            let inferred = typed
                .iter()
                .filter(|claim| {
                    claim.seam_ids.value().is_some_and(|ids| {
                        !ids.iter().any(|id| {
                            self.seams
                                .iter()
                                .any(|seam| seam.id == *id && seam.kind.is_local_consistency_mint())
                        })
                    })
                })
                .count();
            Assessed::mechanical(inferred as f64 / typed.len() as f64, vec![])
        };

        let scopes: Option<Vec<_>> = self
            .seams
            .iter()
            .filter(|seam| {
                matches!(
                    seam.kind,
                    SeamKind::S1ConsistencyMint | SeamKind::S2AlgebraProof
                )
            })
            .map(|seam| seam.antecedent.value().copied())
            .collect();
        let (worst_gap, gap_distribution) = match scopes {
            Some(scopes) if !scopes.is_empty() => {
                let mut distribution = [0; 5];
                for scope in &scopes {
                    distribution[*scope as usize] += 1;
                }
                (
                    Assessed::mechanical(*scopes.iter().max().unwrap(), vec![]),
                    Assessed::mechanical(distribution, vec![]),
                )
            }
            Some(_) => (
                Assessed::missing(
                    "no consistency assertions or manual algebra proofs have been classified",
                ),
                Assessed::missing(
                    "no consistency assertions or manual algebra proofs have been classified",
                ),
            ),
            None => (
                Assessed::missing(
                    "the source scope of one or more consistency assertions or manual algebra proofs is unclassified",
                ),
                Assessed::missing(
                    "the source scope of one or more consistency assertions or manual algebra proofs is unclassified",
                ),
            ),
        };

        let contracts: Vec<_> = self
            .seams
            .iter()
            .filter(|seam| seam.kind.is_contract_or_seal())
            .collect();
        let contract_e2_ratio = ratio_from_evidence(
            &contracts,
            "no caller or convention contracts have been entered for evidence review",
            EvidenceGrade::is_e2_or_stronger,
        );
        let red_coverage = ratio_from_evidence(
            &self.seams.iter().collect::<Vec<_>>(),
            "no claim sites have been entered for evidence review",
            EvidenceGrade::is_red_covered,
        );

        let nondet: Vec<_> = self
            .seams
            .iter()
            .filter(|seam| {
                matches!(
                    seam.kind,
                    SeamKind::S3Assumer | SeamKind::S4Introducer | SeamKind::S5ForwardedObligation
                )
            })
            .collect();
        let nondet_counts = if nondet.is_empty() {
            Assessed::missing(
                "no nondeterministic-choice sites have received a manual evidence classification",
            )
        } else if nondet
            .iter()
            .any(|seam| seam.nondeterminism.value().is_none())
        {
            Assessed::missing("one or more nondeterminism classifications are missing")
        } else {
            let mut counts = [0; 4];
            for seam in nondet {
                if let Some(class) = seam.nondeterminism.value().and_then(|class| *class) {
                    let index = match class {
                        NondetClass::N0MechanicallyErased => 0,
                        NondetClass::N1ProseErased => 1,
                        NondetClass::N2NamedFreedom => 2,
                        NondetClass::N3UnaccountedEscape => 3,
                    };
                    counts[index] += 1;
                }
            }
            Assessed::mechanical(counts, vec![])
        };

        TrustSummary {
            inference_ratio,
            worst_gap,
            gap_distribution,
            trusted_surface_ratio: Assessed::missing(
                "requires an IR operator count and measured, deduplicated source regions for every manual dependency",
            ),
            contract_e2_ratio,
            red_coverage,
            nondet_counts,
        }
    }
}

fn ratio_from_evidence(
    seams: &[&SeamRecord],
    empty_reason: &'static str,
    predicate: impl Fn(EvidenceGrade) -> bool,
) -> Assessed<f64> {
    if seams.is_empty() {
        return Assessed::missing(empty_reason);
    }
    if seams.iter().any(|seam| seam.evidence.value().is_none()) {
        return Assessed::missing("one or more evidence grades are unclassified");
    }
    let matching = seams
        .iter()
        .filter(|seam| seam.evidence.value().is_some_and(|grade| predicate(*grade)))
        .count();
    Assessed::mechanical(matching as f64 / seams.len() as f64, vec![])
}

const ACCOUNTING: &str = "design_docs/2026-08_trust_and_complexity_accounting.md";

fn cite(line: usize, note: &'static str) -> Citation {
    Citation {
        path: ACCOUNTING,
        line: Some(line),
        note,
    }
}

fn not_nondet() -> Assessed<Option<NondetClass>> {
    Assessed::manual(None, vec![cite(101, "seam taxonomy")])
}

/// Evidence-cited manual ledger. Only classifications explicitly made in the
/// accounting document are populated.
pub fn ledger_for_backend(backend: BackendId) -> TrustLedger {
    match backend {
        BackendId::Raft => raft_ledger(),
        BackendId::LibraryPaxos => TrustLedger {
            backend,
            seams: vec![],
            claims: vec![ClaimBill {
                id: "paxos-output",
                claim: "committed log output",
                typed: false,
                seam_ids: Assessed::missing(
                    "library Paxos deliberately claims no consistency output label",
                ),
                citations: vec![cite(417, "Paxos output is explicitly unlabeled")],
            }],
            graph_shape: GraphShape::default(),
            notes: vec![
                "Eight caller-supplied NonDet parameters and 26 local nondet! invocations are counted automatically, but no reviewer has classified how each choice can affect output or what evidence checks it.",
            ],
        },
        BackendId::QuorumLadderConsensus => quorum_ladder_ledger(),
        BackendId::BroadcastTranscript | BackendId::PaxosEc | BackendId::TypedConsensus => {
            TrustLedger {
                backend,
                seams: vec![],
                claims: vec![],
                graph_shape: GraphShape::default(),
                notes: vec![
                    "The accounting document explicitly says that the required per-site review—source scope, affected correctness claims, evidence, and claim dependencies—has not been performed.",
                ],
            }
        }
        BackendId::CompartmentalizedPaxos => TrustLedger {
            backend,
            seams: vec![],
            claims: vec![],
            graph_shape: GraphShape::default(),
            notes: vec![
                "No evidence-cited deep trust classification exists yet for compartmentalized Paxos.",
            ],
        },
    }
}

fn raft_ledger() -> TrustLedger {
    let seam = SeamRecord {
        id: "raft-committed-log-mint",
        kind: SeamKind::S1ConsistencyMint,
        proposition: "Raft's committed log is consistently ordered at every replica",
        antecedent: Assessed::manual(
            AntecedentScope::A3CrossLocation,
            vec![cite(
                396,
                "checking this claim spans election, replication, and commit logic",
            )],
        ),
        blast_scope: Assessed::manual(
            vec![
                "committed log consistency",
                "every downstream committed-log consumer",
            ],
            vec![cite(397, "blast is every downstream consumer")],
        ),
        evidence: Assessed::manual(
            EvidenceGrade::E1Fuzz,
            vec![cite(
                398,
                "prefix-consistency and progress fuzz; no red test",
            )],
        ),
        nondeterminism: not_nondet(),
        justified_by: vec![],
        citations: vec![Citation {
            path: "hydro_test/src/cluster/raft.rs",
            line: Some(1044),
            note: "committed-log consistency assertion (line in cited revision)",
        }],
    };
    TrustLedger {
        backend: BackendId::Raft,
        seams: vec![seam],
        claims: vec![ClaimBill {
            id: "raft-committed-log-consistency",
            claim: "committed log at every member is TotalOrder and eventually consistent",
            typed: true,
            seam_ids: Assessed::manual(
                vec!["raft-committed-log-mint"],
                vec![cite(396, "Raft committed-log classification")],
            ),
            citations: vec![cite(396, "typed committed-log output claim")],
        }],
        graph_shape: GraphShape::default(),
        notes: vec!["Only one Raft consistency assertion has received a cited manual review."],
    }
}

fn quorum_ladder_ledger() -> TrustLedger {
    let splice = SeamRecord {
        id: "ladder-splice-fold-premise",
        kind: SeamKind::S2AlgebraProof,
        proposition: "splice-fold inputs commute under globally distinct rounds",
        antecedent: Assessed::manual(
            AntecedentScope::A4ContractDependent,
            vec![cite(
                410,
                "splice-fold premise cites globally-distinct rounds",
            )],
        ),
        blast_scope: Assessed::missing(
            "forward claim slice has not been mechanically or manually enumerated",
        ),
        evidence: Assessed::manual(
            EvidenceGrade::E0Prose,
            vec![cite(
                412,
                "globally-distinct-rounds premise has no red test",
            )],
        ),
        nondeterminism: not_nondet(),
        justified_by: vec!["ladder-globally-distinct-rounds"],
        citations: vec![cite(
            410,
            "one of four manual algebra proofs; depends on the splice rule",
        )],
    };
    let rounds = SeamRecord {
        id: "ladder-globally-distinct-rounds",
        kind: SeamKind::S6CallerContract,
        proposition: "campaign rounds are globally distinct",
        antecedent: Assessed::missing(
            "the source and caller behavior needed to verify this contract have not been classified",
        ),
        blast_scope: Assessed::manual(
            vec!["agreement", "splice correctness"],
            vec![cite(
                410,
                "splice premise depends on globally-distinct rounds",
            )],
        ),
        evidence: Assessed::manual(
            EvidenceGrade::E0Prose,
            vec![cite(
                412,
                "the accounting document lists prose as the only evidence",
            )],
        ),
        nondeterminism: not_nondet(),
        justified_by: vec![],
        citations: vec![cite(410, "globally-distinct-rounds contract")],
    };
    let seal = SeamRecord {
        id: "ladder-learning-channel-seal",
        kind: SeamKind::S7ConventionSeal,
        proposition: "only audited chosen output can feed the learner channel",
        antecedent: Assessed::missing(
            "the code and conventions needed to verify learner-channel authenticity have not been classified",
        ),
        blast_scope: Assessed::manual(
            vec!["learned chosen facts", "spliced log consistency"],
            vec![cite(
                412,
                "learner-channel authority is identified as a convention rather than an enforced property",
            )],
        ),
        evidence: Assessed::manual(
            EvidenceGrade::E0Prose,
            vec![cite(
                412,
                "learner-channel authenticity is supported only by prose",
            )],
        ),
        nondeterminism: not_nondet(),
        justified_by: vec![],
        citations: vec![cite(412, "learning-channel authority decision")],
    };
    TrustLedger {
        backend: BackendId::QuorumLadderConsensus,
        seams: vec![splice, rounds, seal],
        claims: vec![
            ClaimBill {
                id: "ladder-learned-ec",
                claim: "learned chosen facts carry inferred eventual consistency",
                typed: true,
                seam_ids: Assessed::manual(
                    vec!["ladder-learning-channel-seal"],
                    vec![cite(
                        412,
                        "learning-channel authority underlies learned facts",
                    )],
                ),
                citations: vec![cite(
                    413,
                    "typed output claim has no local consistency assertion",
                )],
            },
            ClaimBill {
                id: "ladder-spliced-log-ec",
                claim: "spliced log carries inferred eventual consistency",
                typed: true,
                seam_ids: Assessed::manual(
                    vec![
                        "ladder-splice-fold-premise",
                        "ladder-globally-distinct-rounds",
                        "ladder-learning-channel-seal",
                    ],
                    vec![cite(410, "splice premise and learning channel ledger")],
                ),
                citations: vec![cite(
                    413,
                    "typed output claim has no local consistency assertion",
                )],
            },
            ClaimBill {
                id: "ladder-agreement",
                claim: "no two different values are chosen for one slot",
                typed: false,
                seam_ids: Assessed::missing(
                    "the accounting document names red-tested adoption and quorum obligations but does not enumerate a complete agreement bill",
                ),
                citations: vec![cite(
                    412,
                    "adopt-highest and sub-majority have deliberately failing tests",
                )],
            },
        ],
        graph_shape: GraphShape::default(),
        notes: vec![
            "Three other manual algebra proofs and five nondeterministic choices still need individual review of their source scope, affected claims, and evidence.",
            "There are deliberately failing tests for the adopt-highest rule and sub-majority rejection, but the current accounting document does not completely connect those tests to the agreement claim's dependency list.",
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raft_summary_exposes_wide_asserted_trust() {
        let summary = ledger_for_backend(BackendId::Raft).summary();
        assert_eq!(summary.inference_ratio.value(), Some(&0.0));
        assert_eq!(
            summary.worst_gap.value(),
            Some(&AntecedentScope::A3CrossLocation)
        );
        assert_eq!(summary.red_coverage.value(), Some(&0.0));
        assert!(summary.trusted_surface_ratio.value().is_none());
    }

    #[test]
    fn ladder_typed_claims_have_no_local_s1_but_missing_data_stays_missing() {
        let ledger = ledger_for_backend(BackendId::QuorumLadderConsensus);
        let summary = ledger.summary();
        assert_eq!(summary.inference_ratio.value(), Some(&1.0));
        assert_eq!(
            summary.worst_gap.value(),
            Some(&AntecedentScope::A4ContractDependent)
        );
        assert_eq!(summary.contract_e2_ratio.value(), Some(&0.0));
        assert!(summary.nondet_counts.value().is_none());
        assert!(ledger.graph_shape.operators.is_none());
    }

    #[test]
    fn unread_backends_are_not_reported_as_zero_risk() {
        for backend in [
            BackendId::BroadcastTranscript,
            BackendId::PaxosEc,
            BackendId::TypedConsensus,
            BackendId::CompartmentalizedPaxos,
        ] {
            let ledger = ledger_for_backend(backend);
            assert!(ledger.seams.is_empty());
            assert!(ledger.summary().inference_ratio.value().is_none());
            assert!(!ledger.notes.is_empty());
        }
    }
}
