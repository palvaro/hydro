//! Record provenance (docs/design/hydrofrac-lite.md §4.2, §5).
//!
//! The rewrite ([`install`]) instruments a simulated program so that every record carries the
//! set of network deliveries it was computed from. Deliveries are named by the E5 wire tag the
//! network hook assigns to each send (a `DeliveryId`). Each record that leaves a node, on a
//! `Network` channel or an external output, hands its lineage to the hook as metadata: the bincode
//! of a [`RecordProv`]. Lineage closes that backwards: a delivery's own support is the
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
//!   execution rule. See [`rewrite()`] for the list of island operators.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

#[cfg(stageleft_runtime)]
#[doc(hidden)]
pub mod check;
pub mod rewrite;

pub use rewrite::{install, rewrite};

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
    /// The bincode encoding used as message metadata.
    pub fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).expect("RecordProv serializes")
    }

    /// Decodes message metadata produced by the rewrite ([`split_meta`]).
    ///
    /// # Panics
    /// If `bytes` is not well-formed metadata.
    pub fn decode(bytes: &[u8]) -> Self {
        Self::try_decode(bytes).expect("message metadata is not a RecordProv")
    }

    /// The [`RecordProv`] of metadata produced by the rewrite ([`split_meta`]), or `None`.
    pub fn try_decode(bytes: &[u8]) -> Option<Self> {
        split_meta(bytes).map(|(prov, _)| prov)
    }
}

/// Splits message metadata into its lineage and its payload's `Debug` rendering: the bincode
/// (1.x, default options) of a [`RecordProv`], which a `Network` send whose payload type
/// implements `Debug` follows with the bincode of that rendering (a `String`). Strict: `None`
/// for anything else, so malformed metadata never passes for lineage.
pub fn split_meta(bytes: &[u8]) -> Option<(RecordProv, Option<String>)> {
    use bincode::Options;
    let options = || bincode::DefaultOptions::new().with_fixint_encoding();
    let mut rest = bytes;
    let prov = options()
        .allow_trailing_bytes()
        .deserialize_from(&mut rest)
        .ok()?;
    if rest.is_empty() {
        return Some((prov, None));
    }
    let debug = options().reject_trailing_bytes().deserialize(rest).ok()?;
    Some((prov, Some(debug)))
}

#[cfg(test)]
mod tests;
