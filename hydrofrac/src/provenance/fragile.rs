//! Fragile steps (docs/design/hydrofrac-races.md §3.1, §4.1).
//!
//! A *fragile step* is an operator whose output records can exist or not depending on how the
//! simulator split its inputs into ticks or ordered them. [`classify`] finds every fragile step
//! of a program, before the provenance rewrite runs, and gives each one a site index. The
//! rewrite then adds the tag of that site to every record the step emits
//! ([`crate::provenance::install_fragile`]), and lineage collects the sites a derivation passed
//! through ([`crate::lineage::Lineage::fragile_sites`]). A derivation that passed none is
//! *robust*: it holds in every schedule.
//!
//! Fragile:
//! - negation: `anti_join`, `difference` (`filter_not_in`), except the dedup idiom
//!   `fresh = arrived.filter_not_in(seen); seen = seen.chain(fresh)` over a constant initial
//!   state (`View::is_dedup`);
//! - `cross_singleton` (so `filter_if`), anywhere: the singleton may not yet hold the value;
//! - `cross_product`, `join`, `join_half` inside a tick: the two records may land in different
//!   ticks;
//! - `fold`, `fold_keyed`, `reduce`, `reduce_keyed`, `scan` inside a tick: the value depends on
//!   how much the tick received;
//! - `enumerate` and `chain_first`, anywhere: they depend on order;
//! - a snapshot: a `batch` of a singleton, optional or keyed singleton;
//! - a closure that reads Hydro state by reference (`by_ref`/`by_mut`), anywhere;
//! - sources other than iterators and cluster membership (timers, intervals, streams).
//!
//! A structural step whose every input is *constant* (built from iterator and singleton sources
//! by stateless operators only, `View::constant`) is not fragile: it computes the same records
//! in every schedule. This is what keeps `use::state`'s initial value
//! (`initial.filter_if(first_tick)`) robust.
//!
//! Not fragile: element-wise operators with pure closures, `chain`, casts, `observe_nondet`,
//! `merge_ordered`, `sort`, `unique`, `defer_tick`, a `batch` of a stream, `all_ticks`, networks,
//! and joins outside any tick. `Counter` is a metrics probe that passes its items through
//! unchanged, so it is not fragile either. Opaque islands are left as they are: lineage treats
//! an opaque record as fragile.

use std::collections::{HashMap, HashSet};
use std::fmt::{self, Display};

use hydro_lang::compile::builder::CycleId;
use hydro_lang::compile::ir::{CollectionKind, HydroNode, HydroRoot, HydroSource};

use super::choices::{cell, closures};
use super::rewrite::node_key;
use crate::observe::ec_label;

/// One fragile step of the program: its site index (the tag the rewrite adds), what it is
/// (`anti_join`, `snapshot`, ...) and where (as [`super::choices::ChoiceSite`] reports it: the
/// operator's tag, else its user source line `file:line`, else its name).
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FragileSite {
    pub index: usize,
    pub op: String,
    pub site: String,
}

impl Display for FragileSite {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} at {} (#{})", self.op, self.site, self.index)
    }
}

/// Every fragile step of `ir` (see the [module docs](self)), with site indices from 0 in the
/// order the scan meets them, and a map from [`node_key`] to site index. The nodes keyed in
/// `trusted` (those inside a trusted `quorum`, [`super::quorum`]) are not sites.
pub(crate) fn classify(
    ir: &[HydroRoot],
    trusted: &HashSet<usize>,
) -> (Vec<FragileSite>, HashMap<usize, usize>) {
    let scan = View::new(ir);
    let mut sites = vec![];
    let mut by_key = HashMap::new();
    for ptr in scan.nodes() {
        // SAFETY: every pointer points into `ir`, which is borrowed for the whole scan.
        let node = unsafe { &*ptr };
        if trusted.contains(&node_key(node)) {
            continue;
        }
        if let Some(op) = scan.fragile(node) {
            by_key.insert(node_key(node), sites.len());
            sites.push(FragileSite {
                index: sites.len(),
                op: op.to_owned(),
                site: ec_label(node),
            });
        }
    }
    (sites, by_key)
}

/// A read-only view of the IR, by node address.
struct View<'a> {
    ir: &'a [HydroRoot],
    /// Each tick cycle's sink input.
    cycle_sinks: HashMap<CycleId, *const HydroNode>,
}

/// The inputs of `node`, including the node a `Tee`, `Reference` or `PartitionSide` shares.
fn inputs(node: &HydroNode) -> Vec<*const HydroNode> {
    match node {
        HydroNode::Tee { inner, .. } | HydroNode::PartitionSide { inner, .. } => {
            vec![std::ptr::from_ref(&*inner.0.borrow())]
        }
        HydroNode::Reference { .. } => cell(node).into_iter().collect(),
        _ => node.input().into_iter().map(std::ptr::from_ref).collect(),
    }
}

/// Follows `Tee`s to the node they share.
fn strip(mut ptr: *const HydroNode) -> *const HydroNode {
    loop {
        // SAFETY: `ptr` points into the IR the scan was built from.
        match unsafe { &*ptr } {
            HydroNode::Tee { inner, .. } => ptr = std::ptr::from_ref(&*inner.0.borrow()),
            _ => return ptr,
        }
    }
}

fn reads_state(node: &HydroNode) -> bool {
    closures(node)
        .iter()
        .any(|f| !f.singleton_refs().is_empty())
}

impl<'a> View<'a> {
    fn new(ir: &'a [HydroRoot]) -> Self {
        let cycle_sinks = ir
            .iter()
            .filter_map(|root| match root {
                HydroRoot::CycleSink {
                    cycle_id, input, ..
                } => Some((*cycle_id, std::ptr::from_ref(&**input))),
                _ => None,
            })
            .collect();
        View { ir, cycle_sinks }
    }

    /// Every node reachable from the roots (through shared nodes and the state closures read),
    /// once each, in a deterministic order.
    fn nodes(&self) -> Vec<*const HydroNode> {
        let (mut seen, mut stack, mut out) = (HashSet::new(), vec![], vec![]);
        for root in self.ir.iter().rev() {
            stack.push(std::ptr::from_ref(root.input()));
            if let HydroRoot::ForEach { f, .. } = root {
                stack.extend(f.singleton_refs().iter().filter_map(|(r, _)| cell(r)));
            }
        }
        while let Some(ptr) = stack.pop() {
            if !seen.insert(ptr) {
                continue;
            }
            // SAFETY: as in `classify`.
            let node = unsafe { &*ptr };
            if let HydroNode::Placeholder = node {
                continue;
            }
            out.push(ptr);
            for f in closures(node) {
                stack.extend(f.singleton_refs().iter().filter_map(|(r, _)| cell(r)));
            }
            stack.extend(inputs(node));
        }
        out
    }

    /// What fragile step `node` is, or `None` if it is not one.
    fn fragile(&self, node: &HydroNode) -> Option<&'static str> {
        use HydroNode::*;
        if reads_state(node) {
            return Some("closure_over_state");
        }
        let in_tick = !node.metadata().location_id.is_top_level();
        let op = match node {
            Source { source, .. } => {
                return match source {
                    HydroSource::Iter(_)
                    | HydroSource::ClusterMembers(..)
                    | HydroSource::ExternalNetwork() => None,
                    _ => Some("source"),
                };
            }
            // The partition closure runs at the shared node; its outputs leave through the sides.
            PartitionSide { inner, .. } => {
                return reads_state(&inner.0.borrow()).then_some("closure_over_state");
            }
            AntiJoin { .. } => "anti_join",
            Difference { .. } if self.is_dedup(node) => return None,
            Difference { .. } => "difference",
            CrossSingleton { .. } => "cross_singleton",
            CrossProduct { .. } if in_tick => "cross_product",
            Join { .. } if in_tick => "join",
            JoinHalf { .. } if in_tick => "join_half",
            Fold { .. } if in_tick => "fold",
            FoldKeyed { .. } if in_tick => "fold_keyed",
            Reduce { .. } if in_tick => "reduce",
            ReduceKeyed { .. } if in_tick => "reduce_keyed",
            Scan { .. } if in_tick => "scan",
            Enumerate { .. } => "enumerate",
            ChainFirst { .. } => "chain_first",
            Batch { inner, .. }
                if matches!(
                    inner.metadata().collection_kind,
                    CollectionKind::Singleton { .. }
                        | CollectionKind::Optional { .. }
                        | CollectionKind::KeyedSingleton { .. }
                ) =>
            {
                "snapshot"
            }
            _ => return None,
        };
        // Same records in every schedule if every input is.
        if inputs(node).into_iter().all(|i| self.constant(i)) {
            return None;
        }
        Some(op)
    }

    /// Whether `ptr`'s records are the same in every schedule: it is built from iterator and
    /// singleton sources by stateless operators with pure closures.
    fn constant(&self, ptr: *const HydroNode) -> bool {
        use HydroNode::*;
        // SAFETY: as in `strip`.
        let node = unsafe { &*ptr };
        let structural = match node {
            Source { source, .. } => return matches!(source, HydroSource::Iter(_)),
            SingletonSource { .. } => return true,
            Map { .. } | FlatMap { .. } | Filter { .. } | FilterMap { .. } | Inspect { .. } => {
                !reads_state(node)
            }
            Tee { .. } | Cast { .. } | ObserveNonDet { .. } | Chain { .. } | Unique { .. } => true,
            Sort { .. } | ChainFirst { .. } | CrossSingleton { .. } | CrossProduct { .. } => true,
            _ => false,
        };
        structural && inputs(node).into_iter().all(|i| self.constant(i))
    }

    /// Whether `node` is `filter_not_in` against its own history: a `Difference` whose `neg` is
    /// a tick state that starts as a constant and then accumulates the `Difference`'s own
    /// output (`seen = seen.chain(fresh)` with `fresh = arrived.filter_not_in(seen)`, the idiom
    /// of `use::state`). Its output over the run is then every record of `pos` not in the
    /// initial state, whatever ticks they arrive in. Ported from the research branch's
    /// `Cones::self_difference` (`origin/hydrofrac-sensitivity`, `sensitivity.rs`).
    fn is_dedup(&self, node: &HydroNode) -> bool {
        let HydroNode::Difference { neg, .. } = node else {
            return false;
        };
        let me = std::ptr::from_ref(node);
        let state = strip(std::ptr::from_ref(&**neg));
        // SAFETY: as in `strip`.
        let HydroNode::Chain { first, second, .. } = (unsafe { &*state }) else {
            return false;
        };
        // The cycle a side carries over from the previous tick, if it does.
        let carried = |side: &HydroNode| -> Option<CycleId> {
            // SAFETY: as in `strip`.
            let HydroNode::DeferTick { input, .. } = (unsafe { &*strip(std::ptr::from_ref(side)) })
            else {
                return None;
            };
            // SAFETY: as in `strip`.
            match unsafe { &*strip(std::ptr::from_ref(&**input)) } {
                HydroNode::CycleSource { cycle_id, .. } => Some(*cycle_id),
                _ => None,
            }
        };
        let (cycle, init) = match (carried(first), carried(second)) {
            (Some(c), _) => (c, std::ptr::from_ref(&**second)),
            (None, Some(c)) => (c, std::ptr::from_ref(&**first)),
            (None, None) => return false,
        };
        let Some(&sink) = self.cycle_sinks.get(&cycle) else {
            return false;
        };
        // SAFETY: as in `strip`.
        let HydroNode::Chain { first, second, .. } = (unsafe { &*strip(sink) }) else {
            return false;
        };
        let parts = (
            strip(std::ptr::from_ref(&**first)),
            strip(std::ptr::from_ref(&**second)),
        );
        (parts == (state, me) || parts == (me, state)) && self.constant(init)
    }
}
