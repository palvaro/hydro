//! Record provenance (docs/design/hydrofrac-lite.md §4.2, §5).
//!
//! The rewrite ([`install`]) instruments a simulated program so that every record carries the
//! set of network deliveries it was computed from. Deliveries are named by the E5 wire tag the
//! network hook assigns to each send (a `DeliveryId`). Each record that leaves a node, on a
//! `Network` channel or an external output, hands its lineage to the hook as metadata: the bincode
//! of a [`RecordProv`], which a `Network` send follows with its payload's `Debug` rendering for
//! reports ([`split_meta`]). Lineage closes that backwards: a delivery's own support is the
//! [`RecordProv`] its send carried.
//!
//! What the rewrite tracks:
//! - Records flow through operators keeping or merging their lineage: `map`/`filter` keep it,
//!   `join`/`cross` union both sides, `fold`/`reduce`/`scan` accumulate every input.
//! - A received record's lineage is exactly its delivery.
//! - A closure reading Hydro state through a `by_ref`/`by_mut` reference absorbs everything that
//!   state has absorbed (a sound over-approximation).
//! - Anything else the rewrite cannot see into is an *opaque island*: its inputs are stripped
//!   of lineage and its outputs are [`RecordProv::Opaque`], for which lineage falls back to the
//!   execution rule. See [`rewrite`] for the list of island operators.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

#[cfg(stageleft_runtime)]
#[doc(hidden)]
pub mod check;
pub mod rewrite;
/// A static diagnostic of which labels' derivations may depend on the schedule.
#[cfg(stageleft_runtime)]
#[doc(hidden)]
pub mod sensitivity;

pub use rewrite::{install, opaque_operators, rewrite};

/// The lineage of one record as handed to the network hook: the per-message metadata format.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RecordProv {
    /// The record was computed from exactly these deliveries (E5 wire tags), or from a superset
    /// of what it truly depends on. Empty: it depends on no delivery at all.
    Ids(BTreeSet<u64>),
    /// Some ancestor was computed by an operator the rewrite cannot see into, so the record's
    /// support is unknown; lineage must use the execution rule.
    Opaque,
}

impl RecordProv {
    /// Whether lineage must fall back to the execution rule for this record.
    pub fn is_opaque(&self) -> bool {
        matches!(self, RecordProv::Opaque)
    }

    /// The bincode encoding used as message metadata.
    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).expect("RecordProv serializes")
    }

    /// Decodes message metadata produced by the rewrite (the [`RecordProv`] part; see
    /// [`split_meta`] for the format).
    ///
    /// # Panics
    /// If `bytes` is not well-formed metadata.
    pub fn decode(bytes: &[u8]) -> Self {
        split_meta(bytes)
            .expect("message metadata is not a RecordProv")
            .0
    }
}

/// Decodes message metadata produced by the rewrite (see [`RecordProv::decode`]).
pub fn decode(bytes: &[u8]) -> RecordProv {
    RecordProv::decode(bytes)
}

/// Splits message metadata into its lineage and the `Debug` rendering of the message's payload.
///
/// The metadata of a `Network` send is the bincode (1.x, default options) of a [`RecordProv`],
/// followed, when the channel's element type implements `Debug`, by the bincode of a `String`:
/// the payload's `Debug` rendering, computed in the simulation where the type is known (the
/// host only sees bytes). External outputs carry the [`RecordProv`] alone.
///
/// Strict: `None` unless `bytes` is exactly a [`RecordProv`] optionally followed by exactly one
/// `String`, so malformed metadata never passes for lineage.
pub fn split_meta(bytes: &[u8]) -> Option<(RecordProv, Option<String>)> {
    use bincode::Options;
    let options = || {
        bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .allow_trailing_bytes()
    };
    let mut rest = bytes;
    let prov: RecordProv = options().deserialize_from(&mut rest).ok()?;
    if rest.is_empty() {
        return Some((prov, None));
    }
    let debug: String = options().reject_trailing_bytes().deserialize(rest).ok()?;
    Some((prov, Some(debug)))
}

/// The `Debug` rendering of a message's payload carried in its metadata, if any (see
/// [`split_meta`]).
pub fn payload_debug(meta: Option<&[u8]>) -> Option<String> {
    meta.and_then(split_meta).and_then(|(_, debug)| debug)
}

/// Appends a payload's `Debug` rendering to metadata (the format of [`split_meta`]); what the
/// rewrite's generated code does, for tests and hand-built logs.
pub fn with_payload_debug(mut meta: Vec<u8>, debug: &str) -> Vec<u8> {
    meta.extend(bincode::serialize(debug).expect("a string serializes"));
    meta
}

#[cfg(test)]
mod sensitivity_tests;
#[cfg(test)]
mod tests;
