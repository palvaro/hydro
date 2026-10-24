//! Which labelled collections the generated specification checks ([`plan`]).

use std::fmt::{self, Display};

use hydro_lang::compile::ir::CollectionKind;
use hydro_lang::location::dynamic::LocationId;

use super::{EcCollection, kind_name};

/// Why a labelled collection is not checked by the generated specification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NotCheckedReason {
    /// No network channel feeds the collection: no fault can withhold its inputs.
    LocalInputsOnly,
    /// A scalar (the named kind) that no checked labelled stream covers: the set-based
    /// specification needs a collection that only grows.
    NotAStream(String),
    /// The labelling node lives inside a tick: its per-tick contents are not the collection's.
    InsideTick,
}

impl Display for NotCheckedReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NotCheckedReason::LocalInputsOnly => write!(f, "local inputs only"),
            NotCheckedReason::NotAStream(kind) => {
                write!(f, "{kind} (not a set-valued collection)")
            }
            NotCheckedReason::InsideTick => write!(f, "inside a tick"),
        }
    }
}

/// One labelled collection, with whether and how the generated specification checks it.
#[derive(Clone, Debug)]
pub struct LabelPlan {
    /// The collection.
    pub collection: EcCollection,
    /// Why it is not checked, if it is not.
    pub not_checked: Option<NotCheckedReason>,
    /// For a labelled scalar checked at its input stream: that stream's plan index. Always
    /// `None` ([`plan`] no longer covers scalars).
    pub covered_by: Option<usize>,
}

impl LabelPlan {
    /// Whether the collection is tapped and searched in its own right.
    pub fn is_searched(&self) -> bool {
        self.not_checked.is_none() && self.covered_by.is_none()
    }

    /// The cluster the collection lives on.
    pub fn cluster(&self) -> &LocationId {
        &self.collection.location
    }
}

/// Whether the set-based specification applies to `collection`: local inputs, then kind (a
/// scalar may still be covered, see [`plan`]), then tick.
fn classify(collection: &EcCollection) -> Option<NotCheckedReason> {
    if !collection.fed_by_network {
        return Some(NotCheckedReason::LocalInputsOnly);
    }
    let kind = &collection.collection_kind;
    if !matches!(
        kind,
        CollectionKind::Stream { .. } | CollectionKind::KeyedStream { .. }
    ) {
        return Some(NotCheckedReason::NotAStream(kind_name(kind).to_owned()));
    }
    if collection.in_tick {
        return Some(NotCheckedReason::InsideTick);
    }
    None
}

/// The plan for every collection of the enumeration: `classify`. A scalar is never covered by
/// the stream it aggregates (`covered_by` stays `None`): the specification compares sets of
/// facts, and members that agree on a stream's set can still disagree on a fold of it (a count
/// or a sum over a duplicate, or any fold that is not idempotent). The IR does not record
/// whether a fold is idempotent, so such a scalar is reported not checked.
pub fn plan(collections: Vec<EcCollection>) -> Vec<LabelPlan> {
    collections
        .into_iter()
        .map(|collection| LabelPlan {
            not_checked: classify(&collection),
            collection,
            covered_by: None,
        })
        .collect()
}
