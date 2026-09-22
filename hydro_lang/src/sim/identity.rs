//! Static identity loci: where a program deduplicates or keys its state, read off the lineage
//! pass's operator table before any run (`design_docs/2026-09_amplification_as_adversarial_scheduling.md`,
//! "Identity is the idempotency key").
//!
//! The program knowledge the checker needs is which fields of a record are its identity (the
//! idempotency key) and which are stamps the protocol puts on it per attempt. A receiver that
//! deduplicates has an operator that names the key: `unique` (the whole payload), an anti-join
//! or `difference` (its key), a keyed fold, reduce or scan (its key), a join (its key). This module
//! lists those operators with the key type read from the operator's element type, so a harness can
//! compare them with its declared goal extractor, and so the search can start at the edges that
//! feed them. What it cannot see is deduplication inside a Rust closure (a `by_mut` step), and
//! what it cannot decide is whether a key is an identity that survives retries (a request id) or a
//! stamp minted per attempt (a term, a ballot, a slot); that is the harness's to say, or a run's
//! to show.

use super::lineage::OperatorInfo;

/// What kind of identity test an operator performs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// `unique`: a duplicate of the whole payload is dropped.
    Dedup,
    /// Anti-join or difference: a record is kept only if no record with its key is on the other
    /// side; the program's absence test.
    Absence,
    /// A keyed aggregate (`fold_keyed`, `reduce_keyed`, keyed `scan`): state per key.
    KeyedState,
    /// A join: records are matched by key.
    Match,
}

/// One identity locus.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Locus {
    /// The operator.
    pub op: u32,
    /// Its role.
    pub role: Role,
    /// The IR kind.
    pub kind: String,
    /// `file:line:col`.
    pub location: String,
    /// The source line.
    pub source_line: String,
    /// The key type, as far as it can be read from the element type: the first component of a
    /// tuple type, or the whole type for `unique`.
    pub key_type: String,
    /// The operator's element type.
    pub element_type: String,
}

/// The identity loci of a program, in operator order.
pub fn loci(operators: &[OperatorInfo]) -> Vec<Locus> {
    operators
        .iter()
        .filter_map(|o| {
            let role = match o.kind.as_str() {
                "Unique" => Role::Dedup,
                "AntiJoin" | "Difference" => Role::Absence,
                "FoldKeyed" | "ReduceKeyed" | "Scan" => Role::KeyedState,
                "Join" | "JoinHalf" => Role::Match,
                _ => return None,
            };
            let key_type = if role == Role::Dedup {
                o.element_type.clone()
            } else {
                first_tuple_component(&o.element_type).unwrap_or_else(|| o.element_type.clone())
            };
            Some(Locus {
                op: o.id,
                role,
                kind: o.kind.clone(),
                location: o.location.clone(),
                source_line: o.source_line.clone(),
                key_type,
                element_type: o.element_type.clone(),
            })
        })
        .collect()
}

/// The first component of a tuple type's text, `(A, B)` -> `A`, honoring nested brackets.
fn first_tuple_component(ty: &str) -> Option<String> {
    let inner = ty.trim().strip_prefix('(')?;
    let mut depth = 0i32;
    for (i, c) in inner.char_indices() {
        match c {
            '(' | '<' | '[' => depth += 1,
            ')' | '>' | ']' => {
                if depth == 0 {
                    return Some(inner[..i].trim().to_owned());
                }
                depth -= 1;
            }
            ',' if depth == 0 => return Some(inner[..i].trim().to_owned()),
            _ => {}
        }
    }
    None
}

/// Prints the loci.
pub fn print(program: &str, loci: &[Locus]) {
    println!("== identity loci of {program} ({} operators test identity):", loci.len());
    for l in loci {
        println!(
            "   op {:>3} {:<11} {:<10} key {:<40} at {}  {}",
            l.op,
            format!("{:?}", l.role),
            l.kind,
            l.key_type.chars().take(40).collect::<String>(),
            l.location.rsplit('/').next().unwrap_or(&l.location),
            l.source_line.chars().take(70).collect::<String>()
        );
    }
}
