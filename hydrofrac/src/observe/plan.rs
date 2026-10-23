//! Which labelled collections the generated specification checks ([`plan`]).

use std::fmt::{self, Display};

use hydro_lang::compile::ir::CollectionKind;
use hydro_lang::location::dynamic::LocationId;

use super::EcCollection;
use crate::provenance::sensitivity::NondetSite;

/// Why a labelled collection is not checked by the generated specification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NotCheckedReason {
    /// No network channel feeds the collection: no fault in the model can withhold an input
    /// from some members, so there is nothing to check.
    LocalInputsOnly,
    /// The collection is a `Singleton`, `Optional` or `KeyedSingleton` whose value can change,
    /// and it is not an aggregation of a checked labelled stream (which would cover it,
    /// [`LabelPlan::covered_by`]). The set-based specification needs a collection that only
    /// grows; see docs/design/hydrofrac-lite.md §4.7. The string names the kind.
    NotAStream(String),
    /// The labelling node lives inside a tick: its bounded per-tick contents are not "the
    /// collection's contents".
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

/// One labelled collection of the program, with whether the generated specification checks
/// it.
#[derive(Clone, Debug)]
pub struct LabelPlan {
    /// The collection.
    pub collection: EcCollection,
    /// Why it is not checked, if it is not.
    pub not_checked: Option<NotCheckedReason>,
    /// For a labelled scalar checked at its input stream ([`EcCollection::covered_by`]): the
    /// index of that stream's plan, which is checked. The scalar itself is not tapped.
    pub covered_by: Option<usize>,
    /// For a merge-dominated hop ([`EcCollection::merged_into`]): the index of the nearest
    /// checked exit after the merge, whose verdict is authoritative. The hop is still tapped
    /// and searched; its verdict is informational.
    pub merged_at: Option<usize>,
    /// What makes the label's search schedule-sensitive
    /// ([`crate::provenance::sensitivity`]): the simulator's choices in its causal cone, those
    /// that may reorder the sends of a node in it, and what the walk cannot see through. Empty
    /// for a label whose fault sets each have one execution: only those can be certified
    /// (docs/design/hydrofrac-lite.md §4.9). Filled by the tap pass; [`plan`] leaves it empty.
    pub sites: Vec<NondetSite>,
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

/// Whether the set-based specification applies to `collection`: local inputs, then kind,
/// then tick. A scalar is reported here as not a stream; [`plan`] then checks it at its input
/// stream when it can.
fn classify(collection: &EcCollection) -> Option<NotCheckedReason> {
    if !collection.fed_by_network {
        return Some(NotCheckedReason::LocalInputsOnly);
    }
    match &collection.collection_kind {
        CollectionKind::Stream { .. } | CollectionKind::KeyedStream { .. } => {}
        CollectionKind::Singleton { .. } => {
            return Some(NotCheckedReason::NotAStream("Singleton".to_owned()));
        }
        CollectionKind::Optional { .. } => {
            return Some(NotCheckedReason::NotAStream("Optional".to_owned()));
        }
        CollectionKind::KeyedSingleton { .. } => {
            return Some(NotCheckedReason::NotAStream("KeyedSingleton".to_owned()));
        }
    }
    if collection.in_tick {
        return Some(NotCheckedReason::InsideTick);
    }
    None
}

/// The plan for every collection of the enumeration: [`classify`], then a scalar whose input
/// stream is checked is covered by it, and a merge-dominated hop is attributed to the nearest
/// checked exit after its merge.
pub fn plan(collections: Vec<EcCollection>) -> Vec<LabelPlan> {
    let mut plans: Vec<LabelPlan> = collections
        .into_iter()
        .map(|collection| LabelPlan {
            not_checked: classify(&collection),
            collection,
            covered_by: None,
            merged_at: None,
            sites: vec![],
        })
        .collect();
    for i in 0..plans.len() {
        if let Some(input) = plans[i].collection.covered_by
            && matches!(plans[i].not_checked, Some(NotCheckedReason::NotAStream(_)))
            && plans[input].not_checked.is_none()
        {
            plans[i].not_checked = None;
            plans[i].covered_by = Some(input);
        }
    }
    for i in 0..plans.len() {
        if !plans[i].is_searched() {
            continue;
        }
        plans[i].merged_at = plans[i]
            .collection
            .merged_into
            .iter()
            .copied()
            .find(|&exit| plans[exit].is_searched());
    }
    plans
}
