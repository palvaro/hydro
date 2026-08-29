//! Sequestered, evidence-bound qualitative implementation review.
//!
//! Each adapter invocation receives one current backend source, the current
//! rubric, and explicitly historical research notes. It receives no gauntlet
//! measurements, generated trust ledger, previous verdict, or competing source.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::backend::BackendId;

pub const SCHEMA_VERSION: u32 = 1;
pub const RUBRIC_PATH: &str = "design_docs/2026-08_sequestered_review_rubric.md";
pub const HISTORICAL_PATHS: [&str; 2] = [
    "design_docs/2026-08_trust_and_complexity_accounting.md",
    "design_docs/2026-08_nondet_vs_manual_proof.md",
];

const SYSTEM_INSTRUCTIONS: &str = r#"You are the sequestered qualitative reviewer for the Hydro consensus gauntlet.
Read the supplied CURRENT_IMPLEMENTATION independently and skeptically as Hydro code.
Your headline questions are whether it is easy to read and whether it is easy to check.
Do not infer benchmark, test, census, or trust-ledger results: none are supplied.
Do not rank this implementation against alternatives. OLD_RESEARCH_HISTORY is context only:
it can contain stale claims and mentions of other implementations, is not evidence about current
source, and must never be cited in the verdict. Cite only one-indexed inclusive line ranges from
CURRENT_IMPLEMENTATION. Return only a JSON ReviewAdapterResponse matching the supplied schema."#;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum EaseRating {
    Easy,
    MostlyEasy,
    Mixed,
    Difficult,
    VeryDifficult,
}

impl EaseRating {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Easy => "easy",
            Self::MostlyEasy => "mostly easy",
            Self::Mixed => "mixed",
            Self::Difficult => "difficult",
            Self::VeryDifficult => "very difficult",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum GuaranteeStatus {
    Earned,
    PartlyEarned,
    NotEarned,
    Unclear,
}

impl GuaranteeStatus {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Earned => "earned",
            Self::PartlyEarned => "partly earned",
            Self::NotEarned => "not earned",
            Self::Unclear => "unclear",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum ResearchAlignment {
    Strong,
    Partial,
    Weak,
    Unclear,
}

impl ResearchAlignment {
    pub const fn label(self) -> &'static str {
        match self {
            Self::Strong => "strong",
            Self::Partial => "partial",
            Self::Weak => "weak",
            Self::Unclear => "unclear",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SourceCitation {
    pub start_line: usize,
    pub end_line: usize,
    pub note: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AxisVerdict {
    pub rating: EaseRating,
    pub rationale: String,
    pub citations: Vec<SourceCitation>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CitedObservation {
    pub observation: String,
    pub citations: Vec<SourceCitation>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GuaranteeVerdict {
    pub guarantee: String,
    pub status: GuaranteeStatus,
    pub rationale: String,
    pub citations: Vec<SourceCitation>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AlignmentVerdict {
    pub alignment: ResearchAlignment,
    pub rationale: String,
    pub citations: Vec<SourceCitation>,
}

/// Strict structured output expected from the independent reviewer.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StructuredVerdict {
    pub readability: AxisVerdict,
    pub checkability: AxisVerdict,
    pub guarantees: Vec<GuaranteeVerdict>,
    pub hydro_idiom_strengths: Vec<CitedObservation>,
    pub hydro_idiom_concerns: Vec<CitedObservation>,
    pub external_or_hidden_obligations: Vec<CitedObservation>,
    pub research_alignment: AlignmentVerdict,
    pub next_checks: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ReviewAdapterResponse {
    /// Exact provider/model identifier actually used, preferably including a
    /// revision. This may differ from the requested alias.
    pub model: String,
    pub verdict: StructuredVerdict,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DocumentFingerprint {
    pub path: String,
    pub sha256: String,
}

/// Pinned result rendered by reports. Raw source and historical prose are not
/// duplicated in the artifact; their hashes make drift visible.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ReviewArtifact {
    pub schema_version: u32,
    pub backend: String,
    pub source: DocumentFingerprint,
    pub rubric: DocumentFingerprint,
    pub historical_context: Vec<DocumentFingerprint>,
    pub adapter: String,
    pub requested_model: String,
    pub actual_model: String,
    pub prompt_sha256: String,
    pub response_sha256: String,
    pub verdict: StructuredVerdict,
}

#[derive(Serialize)]
struct ReviewRequest<'a> {
    schema_version: u32,
    requested_model: &'a str,
    system_instructions: &'static str,
    response_schema: serde_json::Value,
    current_rubric: InputDocument<'a>,
    old_research_history: Vec<InputDocument<'a>>,
    current_implementation: InputDocument<'a>,
}

#[derive(Serialize)]
struct InputDocument<'a> {
    role: &'static str,
    path: &'a str,
    content: &'a str,
}

/// Invoke an external adapter once for one backend. The executable receives a
/// single JSON request on stdin and must write only `ReviewAdapterResponse` JSON
/// to stdout. No shell is involved.
pub fn invoke_review(
    workspace_root: &Path,
    backend: BackendId,
    adapter: &Path,
    requested_model: &str,
) -> Result<ReviewArtifact, String> {
    let source_path = backend.source_path();
    let source = read(workspace_root, source_path)?;
    let rubric = read(workspace_root, RUBRIC_PATH)?;
    let history: Vec<_> = HISTORICAL_PATHS
        .iter()
        .map(|path| read(workspace_root, path).map(|content| (*path, content)))
        .collect::<Result<_, _>>()?;

    let request = ReviewRequest {
        schema_version: SCHEMA_VERSION,
        requested_model,
        system_instructions: SYSTEM_INSTRUCTIONS,
        response_schema: response_schema(),
        current_rubric: InputDocument {
            role: "CURRENT_RUBRIC",
            path: RUBRIC_PATH,
            content: &rubric,
        },
        old_research_history: history
            .iter()
            .map(|(path, content)| InputDocument {
                role: "OLD_RESEARCH_HISTORY_NOT_CURRENT_EVIDENCE",
                path,
                content,
            })
            .collect(),
        current_implementation: InputDocument {
            role: "CURRENT_IMPLEMENTATION_SOLE_VERDICT_EVIDENCE",
            path: source_path,
            content: &source,
        },
    };
    let request_bytes = serde_json::to_vec_pretty(&request).map_err(|error| error.to_string())?;

    let mut child = Command::new(adapter)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("launch review adapter {}: {error}", adapter.display()))?;
    child
        .stdin
        .take()
        .ok_or_else(|| "review adapter stdin unavailable".to_owned())?
        .write_all(&request_bytes)
        .map_err(|error| format!("write review request: {error}"))?;
    let output = child
        .wait_with_output()
        .map_err(|error| format!("wait for review adapter: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "review adapter failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let response: ReviewAdapterResponse = serde_json::from_slice(&output.stdout)
        .map_err(|error| format!("invalid review adapter response: {error}"))?;
    if response.model.trim().is_empty() {
        return Err("review adapter returned an empty model identifier".to_owned());
    }
    validate_verdict(&response.verdict, source.lines().count())?;

    Ok(ReviewArtifact {
        schema_version: SCHEMA_VERSION,
        backend: backend.as_str().to_owned(),
        source: fingerprint(source_path, &source),
        rubric: fingerprint(RUBRIC_PATH, &rubric),
        historical_context: history
            .iter()
            .map(|(path, content)| fingerprint(path, content))
            .collect(),
        adapter: adapter.display().to_string(),
        requested_model: requested_model.to_owned(),
        actual_model: response.model,
        prompt_sha256: sha256(&request_bytes),
        response_sha256: sha256(&output.stdout),
        verdict: response.verdict,
    })
}

pub fn validate_artifact(artifact: &ReviewArtifact) -> Result<(), String> {
    if artifact.schema_version != SCHEMA_VERSION {
        return Err(format!(
            "unsupported review schema {}; expected {}",
            artifact.schema_version, SCHEMA_VERSION
        ));
    }
    if !BackendId::ALL
        .iter()
        .any(|backend| backend.as_str() == artifact.backend)
    {
        return Err(format!("unknown reviewed backend {:?}", artifact.backend));
    }
    Ok(())
}

/// Refuse to render a pinned verdict against source, rubric, or historical
/// context different from what the reviewer actually saw.
pub fn validate_artifact_against_workspace(
    artifact: &ReviewArtifact,
    workspace_root: &Path,
) -> Result<(), String> {
    validate_artifact(artifact)?;
    let backend = BackendId::ALL
        .into_iter()
        .find(|backend| backend.as_str() == artifact.backend)
        .expect("validate_artifact established backend identity");
    validate_fingerprint(workspace_root, &artifact.source, backend.source_path())?;
    validate_fingerprint(workspace_root, &artifact.rubric, RUBRIC_PATH)?;
    if artifact.historical_context.len() != HISTORICAL_PATHS.len() {
        return Err(format!(
            "review has {} historical documents; expected {}",
            artifact.historical_context.len(),
            HISTORICAL_PATHS.len()
        ));
    }
    for (actual, expected_path) in artifact.historical_context.iter().zip(HISTORICAL_PATHS) {
        validate_fingerprint(workspace_root, actual, expected_path)?;
    }
    let source = read(workspace_root, backend.source_path())?;
    validate_verdict(&artifact.verdict, source.lines().count())
}

fn validate_fingerprint(
    root: &Path,
    actual: &DocumentFingerprint,
    expected_path: &str,
) -> Result<(), String> {
    if actual.path != expected_path {
        return Err(format!(
            "review document path {:?}; expected {expected_path:?}",
            actual.path
        ));
    }
    let content = read(root, expected_path)?;
    let expected = fingerprint(expected_path, &content);
    if actual.sha256 != expected.sha256 {
        return Err(format!(
            "review document {expected_path} changed: artifact {}, current {}",
            actual.sha256, expected.sha256
        ));
    }
    Ok(())
}

fn read(root: &Path, relative: &str) -> Result<String, String> {
    std::fs::read_to_string(root.join(relative))
        .map_err(|error| format!("read {}: {error}", root.join(relative).display()))
}

fn fingerprint(path: &str, content: &str) -> DocumentFingerprint {
    DocumentFingerprint {
        path: path.to_owned(),
        sha256: sha256(content.as_bytes()),
    }
}

fn sha256(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn validate_verdict(verdict: &StructuredVerdict, source_lines: usize) -> Result<(), String> {
    validate_text("readability rationale", &verdict.readability.rationale)?;
    validate_citations(
        "readability",
        &verdict.readability.citations,
        source_lines,
        true,
    )?;
    validate_text("checkability rationale", &verdict.checkability.rationale)?;
    validate_citations(
        "checkability",
        &verdict.checkability.citations,
        source_lines,
        true,
    )?;
    for (index, guarantee) in verdict.guarantees.iter().enumerate() {
        validate_text(&format!("guarantee {index}"), &guarantee.guarantee)?;
        validate_text(
            &format!("guarantee {index} rationale"),
            &guarantee.rationale,
        )?;
        validate_citations(
            &format!("guarantee {index}"),
            &guarantee.citations,
            source_lines,
            true,
        )?;
    }
    for (group, observations) in [
        ("Hydro-idiom strength", &verdict.hydro_idiom_strengths),
        ("Hydro-idiom concern", &verdict.hydro_idiom_concerns),
        (
            "external or hidden obligation",
            &verdict.external_or_hidden_obligations,
        ),
    ] {
        for (index, observation) in observations.iter().enumerate() {
            validate_text(&format!("{group} {index}"), &observation.observation)?;
            validate_citations(
                &format!("{group} {index}"),
                &observation.citations,
                source_lines,
                true,
            )?;
        }
    }
    validate_text(
        "research-alignment rationale",
        &verdict.research_alignment.rationale,
    )?;
    validate_citations(
        "research alignment",
        &verdict.research_alignment.citations,
        source_lines,
        true,
    )?;
    for (index, check) in verdict.next_checks.iter().enumerate() {
        validate_text(&format!("next check {index}"), check)?;
    }
    Ok(())
}

fn validate_text(field: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        Err(format!("review field {field} is empty"))
    } else {
        Ok(())
    }
}

fn validate_citations(
    field: &str,
    citations: &[SourceCitation],
    source_lines: usize,
    required: bool,
) -> Result<(), String> {
    if required && citations.is_empty() {
        return Err(format!("review field {field} has no source citation"));
    }
    for citation in citations {
        if citation.start_line == 0
            || citation.end_line < citation.start_line
            || citation.end_line > source_lines
        {
            return Err(format!(
                "review field {field} has invalid source range {}-{} (source has {source_lines} lines)",
                citation.start_line, citation.end_line
            ));
        }
        validate_text(&format!("{field} citation note"), &citation.note)?;
    }
    Ok(())
}

fn response_schema() -> serde_json::Value {
    serde_json::json!({
        "model": "exact provider/model identifier actually used",
        "verdict": {
            "readability": axis_schema(),
            "checkability": axis_schema(),
            "guarantees": [{
                "guarantee": "claimed guarantee",
                "status": "earned | partly-earned | not-earned | unclear",
                "rationale": "skeptical evidence-bound explanation",
                "citations": [citation_schema()]
            }],
            "hydro_idiom_strengths": [observation_schema()],
            "hydro_idiom_concerns": [observation_schema()],
            "external_or_hidden_obligations": [observation_schema()],
            "research_alignment": {
                "alignment": "strong | partial | weak | unclear",
                "rationale": "alignment with current rubric",
                "citations": [citation_schema()]
            },
            "next_checks": ["specific evidence that would reduce uncertainty"]
        }
    })
}

fn axis_schema() -> serde_json::Value {
    serde_json::json!({
        "rating": "easy | mostly-easy | mixed | difficult | very-difficult",
        "rationale": "evidence-bound explanation",
        "citations": [citation_schema()]
    })
}

fn observation_schema() -> serde_json::Value {
    serde_json::json!({
        "observation": "specific observation",
        "citations": [citation_schema()]
    })
}

fn citation_schema() -> serde_json::Value {
    serde_json::json!({
        "start_line": 1,
        "end_line": 1,
        "note": "what these current-source lines establish"
    })
}

pub fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("gauntlet crate is in the workspace root")
        .to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prompt_contract_marks_history_old_and_current_source_sole_evidence() {
        assert!(SYSTEM_INSTRUCTIONS.contains("OLD_RESEARCH_HISTORY"));
        assert!(SYSTEM_INSTRUCTIONS.contains("must never be cited"));
        assert!(SYSTEM_INSTRUCTIONS.contains("CURRENT_IMPLEMENTATION"));
        let rubric = include_str!("../../design_docs/2026-08_sequestered_review_rubric.md");
        assert!(rubric.contains("easy to read"));
        assert!(rubric.contains("easy to check"));
        assert!(rubric.contains("old research history"));
    }

    #[test]
    fn citation_validation_rejects_missing_and_out_of_range_evidence() {
        assert!(validate_citations("axis", &[], 10, true).is_err());
        let invalid = SourceCitation {
            start_line: 3,
            end_line: 11,
            note: "outside source".to_owned(),
        };
        assert!(validate_citations("axis", &[invalid], 10, true).is_err());
    }

    #[test]
    fn artifact_backend_must_be_known() {
        let artifact = ReviewArtifact {
            schema_version: SCHEMA_VERSION,
            backend: "invented".to_owned(),
            source: fingerprint("source", "x"),
            rubric: fingerprint("rubric", "y"),
            historical_context: vec![],
            adapter: "adapter".to_owned(),
            requested_model: "requested".to_owned(),
            actual_model: "actual".to_owned(),
            prompt_sha256: sha256(b"prompt"),
            response_sha256: sha256(b"response"),
            verdict: StructuredVerdict {
                readability: AxisVerdict {
                    rating: EaseRating::Mixed,
                    rationale: "r".to_owned(),
                    citations: vec![],
                },
                checkability: AxisVerdict {
                    rating: EaseRating::Mixed,
                    rationale: "c".to_owned(),
                    citations: vec![],
                },
                guarantees: vec![],
                hydro_idiom_strengths: vec![],
                hydro_idiom_concerns: vec![],
                external_or_hidden_obligations: vec![],
                research_alignment: AlignmentVerdict {
                    alignment: ResearchAlignment::Unclear,
                    rationale: "u".to_owned(),
                    citations: vec![],
                },
                next_checks: vec![],
            },
        };
        assert!(validate_artifact(&artifact).is_err());
    }
}
