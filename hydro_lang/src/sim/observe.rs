//! The observation rewrite behind [`Spec::all_labels`](super::spec::Spec::all_labels): a
//! sim-only IR pass that brings every collection the program labels `EventualConsistency` to
//! the specification's observer, with nothing named by the test.
//!
//! `Spec::all_labels` leaves an **observation slot** at the observer: an empty, tagged source of
//! `(member, (collection index, bincode of the element))`. This pass, run by
//! `SimFlow::compiled` before network compilation and before the provenance rewrite:
//!
//! 1. enumerates the labelled collections ([`super::slice::ec_collections`]) and decides for
//!    each whether the set-based specification applies ([`NotCheckedReason`]);
//! 2. **taps** each collection it applies to: the labelling node becomes a `Tee` (as
//!    `Stream::clone` would make it) and a second branch is mapped to
//!    `(index, bincode(element))` and sent to the observer over a `fail_stop` channel named
//!    `observe`, exactly the IR `Stream::send(.., TCP.fail_stop().name("observe").bincode())`
//!    builds for a cluster → process send;
//! 3. replaces the slot with the union of the taps.
//!
//! A `Tee` branch is invisible to the rest of the program, so the program's own behaviour is
//! unchanged. The taps are inserted before the provenance rewrite so that `Level::Precise`
//! lineage flows through them like through any other operator; the collection kinds the pass
//! reads are therefore the plain, pre-rewrite ones.

use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Display};
use std::rc::Rc;

use syn::parse_quote;

use crate::compile::ir::{
    BoundKind, ClosureExpr, CollectionKind, DebugExpr, DebugInstantiate, DebugType, HydroNode,
    HydroRoot, NetworkRecv, NetworkSend, SharedNode, StreamOrder, StreamRetry,
};
use crate::live_collections::stream::networking::{
    deserialize_bincode_with_type, serialize_bincode_with_type,
};
use crate::networking::{NetworkingInfo, TcpFault};
use crate::sim::slice::{EcCollection, EcKey, ec_collections, ec_key, is_ec};
use crate::sim::spec::{SLOT_TAG, SLOT_TAG_SEPARATOR};
use crate::staging_util::get_this_crate;

/// Why a labelled collection is not checked by the automatic specification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NotCheckedReason {
    /// No network channel feeds the collection: no fault in the model can withhold an input
    /// from some members, so there is nothing to check.
    LocalInputsOnly,
    /// The collection is a `Singleton`, `Optional` or `KeyedSingleton`: a lattice value, which
    /// the set-based specification does not observe correctly (it needs an equality variant,
    /// not built). The string names the kind.
    NotAStream(String),
    /// The labelling node lives inside a tick: its bounded per-tick contents are not "the
    /// collection's contents".
    InsideTick,
    /// The test excluded the collection (`Spec::all_labels_except`).
    Excluded,
}

impl Display for NotCheckedReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NotCheckedReason::LocalInputsOnly => write!(f, "local inputs only"),
            NotCheckedReason::NotAStream(kind) => {
                write!(f, "{kind} (not a set-valued collection)")
            }
            NotCheckedReason::InsideTick => write!(f, "inside a tick"),
            NotCheckedReason::Excluded => write!(f, "excluded by the test"),
        }
    }
}

/// One labelled collection of the compiled program, with whether the automatic specification
/// observes it.
#[derive(Clone, Debug)]
pub struct LabelPlan {
    /// The collection.
    pub collection: EcCollection,
    /// Why it is not observed, if it is not.
    pub not_checked: Option<NotCheckedReason>,
}

/// Whether the set-based specification applies to `collection`, in the order the design
/// document fixes: local inputs, then kind, then tick.
fn classify(collection: &EcCollection, excluded: &[String]) -> Option<NotCheckedReason> {
    if excluded
        .iter()
        .any(|e| collection.label.contains(e.as_str()))
    {
        return Some(NotCheckedReason::Excluded);
    }
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

/// The observation slot's metadata and the excluded labels, if `ir` has a slot.
fn find_slot(ir: &[HydroRoot]) -> Option<(crate::compile::ir::HydroIrMetadata, Vec<String>)> {
    fn visit(
        node: &HydroNode,
        seen: &mut HashSet<*const HydroNode>,
        found: &mut Vec<crate::compile::ir::HydroIrMetadata>,
    ) {
        if !seen.insert(std::ptr::from_ref(node)) {
            return;
        }
        if let HydroNode::Source { metadata, .. } = node
            && metadata
                .tag
                .as_deref()
                .is_some_and(|t| t.starts_with(SLOT_TAG))
        {
            found.push(metadata.clone());
        }
        match node {
            HydroNode::Tee { inner, .. }
            | HydroNode::Reference { inner, .. }
            | HydroNode::PartitionSide { inner, .. } => visit(&inner.0.borrow(), seen, found),
            _ => {
                for input in node.input() {
                    visit(input, seen, found);
                }
            }
        }
    }
    let mut found = vec![];
    let mut seen = HashSet::new();
    for root in ir {
        visit(root.input(), &mut seen, &mut found);
    }
    assert!(
        found.len() <= 1,
        "a flow has one automatic specification (`Spec::all_labels`); found {}",
        found.len()
    );
    let metadata = found.pop()?;
    let tag = metadata.tag.clone().unwrap_or_default();
    let excluded = tag
        .split(SLOT_TAG_SEPARATOR)
        .skip(1)
        .map(ToOwned::to_owned)
        .collect();
    Some((metadata, excluded))
}

/// Visits every node under `ir` bottom-up, in place: unlike `transform_bottom_up`, a node a
/// `Tee` shares is edited inside its cell (once) rather than moved to a fresh cell, so the
/// branches this pass creates keep pointing at it.
fn walk_in_place(ir: &mut [HydroRoot], f: &mut impl FnMut(&mut HydroNode)) {
    fn walk(
        node: &mut HydroNode,
        seen: &mut HashSet<*const RefCell<HydroNode>>,
        f: &mut impl FnMut(&mut HydroNode),
    ) {
        match node {
            HydroNode::Tee { inner, .. }
            | HydroNode::Reference { inner, .. }
            | HydroNode::PartitionSide { inner, .. } => {
                if seen.insert(Rc::as_ptr(&inner.0)) {
                    walk(&mut inner.0.borrow_mut(), seen, f);
                }
            }
            HydroNode::VersionedNetwork { fork, .. } => {
                if seen.insert(Rc::as_ptr(&fork.0)) {
                    walk(&mut fork.0.borrow_mut(), seen, f);
                }
            }
            _ => node.transform_children(|child, _| walk(child, seen, f), &mut HashMap::new()),
        }
        f(node);
    }
    let mut seen = HashSet::new();
    for root in ir.iter_mut() {
        walk(root.input_mut(), &mut seen, f);
    }
}

/// Observes every labelled collection the specification applies to (see the module docs).
/// Returns the plan, one entry per labelled collection in enumeration order, or `None` when
/// the flow has no automatic specification (nothing is rewritten then).
pub(crate) fn tap_labels(ir: &mut [HydroRoot]) -> Option<Vec<LabelPlan>> {
    let (slot_metadata, excluded) = find_slot(ir)?;
    let root = get_this_crate();

    let plans: Vec<LabelPlan> = ec_collections(ir)
        .into_iter()
        .map(|collection| LabelPlan {
            not_checked: classify(&collection, &excluded),
            collection,
        })
        .collect();
    let index_of: HashMap<EcKey, usize> = plans
        .iter()
        .enumerate()
        .map(|(i, plan)| (plan.collection.key(), i))
        .collect();

    // The wire types: what each tap sends, and what the observer receives.
    let pair_type: syn::Type = parse_quote!((usize, ::std::vec::Vec<u8>));
    let observed_type: syn::Type = stageleft::quote_type::<crate::spec_views::Observed>();

    // Pass 1: tap. Labelled nodes are re-identified by the enumeration's key. (A `Tee` never
    // matches: the enumeration records what it shares, under that node's own key; a shared
    // partition or reference cell is not tapped as a whole.)
    let mut taps: Vec<HydroNode> = vec![];
    walk_in_place(ir, &mut |node: &mut HydroNode| {
        if !is_ec(node)
            || matches!(
                node,
                HydroNode::PartitionShared { .. }
                    | HydroNode::Reference { .. }
                    | HydroNode::VersionedNetwork { .. }
                    | HydroNode::VersionedNetworkFork { .. }
            )
        {
            return;
        }
        let Some(&index) = index_of.get(&ec_key(node)) else {
            return;
        };
        if plans[index].not_checked.is_some() {
            return;
        }
        // Share the node: make it a `Tee` if it is not one, and take a second branch.
        let node_metadata = node.metadata().clone();
        let shared: Rc<RefCell<HydroNode>> = if let HydroNode::Tee { inner, .. } = node {
            inner.0.clone()
        } else {
            let orig = std::mem::replace(node, HydroNode::Placeholder);
            let cell = Rc::new(RefCell::new(orig));
            *node = HydroNode::Tee {
                inner: SharedNode(cell.clone()),
                metadata: node_metadata.clone(),
            };
            cell
        };
        let branch = HydroNode::Tee {
            inner: SharedNode(shared),
            metadata: node_metadata.clone(),
        };
        // `(index, bincode(element))`, from the plain element type.
        let element_type: DebugType =
            super::provenance::element_type(&node_metadata.collection_kind);
        let element_type: &syn::Type = &element_type.0;
        let f: syn::Expr = parse_quote!(
            move |__x: #element_type| (
                #index,
                #root::runtime_support::bincode::serialize(&__x).unwrap()
            )
        );
        let mut map_metadata = node_metadata.clone();
        map_metadata.consistency = None;
        map_metadata.tag = None;
        map_metadata.op.sim_hook_id = None;
        map_metadata.op.id = None;
        map_metadata.collection_kind = CollectionKind::Stream {
            bound: BoundKind::Unbounded,
            order: StreamOrder::NoOrder,
            retry: StreamRetry::ExactlyOnce,
            element_type: DebugType::from(pair_type.clone()),
        };
        let mapped = HydroNode::Map {
            f: ClosureExpr::new(DebugExpr::from(f), vec![]),
            input: Box::new(branch),
            metadata: map_metadata,
        };
        // To the observer, as `Stream::send(.., TCP.fail_stop().name("observe").bincode())`
        // from a cluster builds it.
        let mut network_metadata = slot_metadata.clone();
        network_metadata.tag = None;
        network_metadata.op = node_metadata.op;
        network_metadata.op.sim_hook_id = None;
        network_metadata.op.id = None;
        taps.push(HydroNode::Network {
            name: Some("observe".to_owned()),
            networking_info: NetworkingInfo::Tcp {
                fault: TcpFault::FailStop,
            },
            serialize: NetworkSend::Custom {
                serialize_fn: Some(serialize_bincode_with_type(false, &pair_type).into()),
            },
            deserialize: NetworkRecv::Custom {
                deserialize_fn: Some(
                    deserialize_bincode_with_type(Some(&observed_type), &pair_type).into(),
                ),
            },
            instantiate_fn: DebugInstantiate::Building,
            input: Box::new(mapped),
            metadata: network_metadata,
        });
    });

    // Pass 2: splice the union of the taps into the slot. With no taps the slot stays an
    // empty source.
    if let Some(mut union) = taps.pop() {
        for tap in taps {
            let mut metadata = slot_metadata.clone();
            metadata.tag = None;
            union = HydroNode::Chain {
                first: Box::new(union),
                second: Box::new(tap),
                metadata,
            };
        }
        let mut union = Some(union);
        walk_in_place(ir, &mut |node: &mut HydroNode| {
            if let HydroNode::Source { metadata, .. } = node
                && metadata
                    .tag
                    .as_deref()
                    .is_some_and(|t| t.starts_with(SLOT_TAG))
                && let Some(union) = union.take()
            {
                *node = union;
            }
        });
        assert!(union.is_none(), "the observation slot was not found");
    }

    Some(plans)
}
