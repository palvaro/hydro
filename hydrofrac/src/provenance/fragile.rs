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
//! **An allowlist.** An operator is robust only if [`View::fragile`] names it, with the reason it
//! is insensitive to arrival order and batching. Every other operator is fragile, including node
//! kinds this module does not know (a new IR kind is fragile until someone proves otherwise).
//! The robust entries, each justified in [`View::fragile`]:
//! - plumbing that passes every record through unchanged (casts, tees, references, atomic
//!   brackets, consistency assertions, counters, `observe_nondet`, cycle sources, `defer_tick`,
//!   `all_ticks`);
//! - element-wise operators with pure closures (`map`, `flat_map`, `filter`, `filter_map`,
//!   `inspect`, partitions);
//! - set-preserving merges and reorderings (`chain`, `merge_ordered`, `sort`, `unique`);
//! - networks, a `batch` of a stream, and sources that are the harness's inputs or fixed
//!   (iterators, singleton sources, cluster members, external inputs);
//! - joins and cross products over a whole run (top level), and a half join at top level, whose
//!   bounded build side is complete before it is probed;
//! - the dedup idiom `fresh = arrived.filter_not_in(seen); seen = seen.chain(fresh)` over a
//!   constant initial state (`View::is_dedup`);
//! - any structural step whose every input is *constant* (`View::constant`): it computes the same
//!   records in every schedule. This is what keeps `use::state`'s initial value
//!   (`initial.filter_if(first_tick)`) robust;
//! - the steps inside a trusted `quorum` (`super::quorum`), which [`classify`] skips.
//!
//! Everything else is fragile. That includes: negation, `cross_singleton` (so `filter_if`),
//! `enumerate`, `chain_first`; folds, reduces and scans anywhere, since even at top level their
//! input order can be a schedule choice (`assume_ordering` at top level is a simulator hook); joins
//! inside a tick; snapshots (a `batch` of a singleton, optional or keyed singleton); closures that
//! read Hydro state by reference; and other sources (timers, intervals, streams).
//!
//! Opaque islands (async and versioned operators, which the rewrite cannot see into) get no tag:
//! lineage treats every record through an opaque island as fragile, and an island makes the
//! program not fragile-free.

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

    /// What fragile step `node` is, or `None` if it is robust (or opaque). An allowlist: every
    /// robust arm says why it is insensitive to arrival order and batching, and anything not
    /// listed is fragile.
    fn fragile(&self, node: &HydroNode) -> Option<&'static str> {
        use HydroNode::*;
        if reads_state(node) {
            // A closure that reads Hydro state sees whatever the state holds at that moment.
            return Some("closure_over_state");
        }
        let top_level = node.metadata().location_id.is_top_level();
        let robust = match node {
            // Plumbing: every record passes through unchanged (or, for `observe_nondet`, the
            // records are reordered, never added or dropped).
            Cast { .. }
            | ObserveNonDet { .. }
            | Tee { .. }
            | Reference { .. }
            | BeginAtomic { .. }
            | EndAtomic { .. }
            | AssertIsConsistent { .. }
            | Counter { .. }
            | UnboundSingleton { .. }
            | CycleSource { .. }
            | DeferTick { .. }
            | YieldConcat { .. } => true,
            // Element-wise with a pure closure (`reads_state` is false here): each output is a
            // function of one input record.
            Map { .. } | FlatMap { .. } | Filter { .. } | FilterMap { .. } | Inspect { .. } => true,
            // The partition closure runs at the shared node; its outputs leave through the sides.
            PartitionSide { inner, .. } => {
                return reads_state(&inner.0.borrow()).then_some("closure_over_state");
            }
            PartitionShared { .. } => true,
            // Set-preserving: the records over the run are the union (or the same set reordered,
            // or the distinct ones) of the inputs', however they were split into ticks.
            Chain { .. } | MergeOrdered { .. } | Sort { .. } | Unique { .. } => true,
            // A network delivers each send's payload; faults, not schedules, remove deliveries.
            Network { .. } => true,
            // A batch of a stream decides when a record enters a tick, not whether.
            Batch { inner, .. } => !matches!(
                inner.metadata().collection_kind,
                CollectionKind::Singleton { .. }
                    | CollectionKind::Optional { .. }
                    | CollectionKind::KeyedSingleton { .. }
            ),
            // The harness's inputs, fixed values, and the (static) cluster membership. Decided
            // here: a source has no inputs, so the constant-inputs exemption below would hold
            // vacuously.
            Source { source, .. } => {
                return (!matches!(
                    source,
                    HydroSource::Iter(_)
                        | HydroSource::ClusterMembers(..)
                        | HydroSource::ExternalNetwork()
                ))
                .then_some("source");
            }
            SingletonSource { .. } | ExternalInput { .. } => true,
            // Over a whole run, a symmetric join emits every matching pair once both sides have
            // arrived; a half join's bounded build side is complete before it is probed.
            Join { .. } | CrossProduct { .. } | JoinHalf { .. } => top_level,
            // The dedup idiom: every record of `pos` not in the constant initial state.
            Difference { .. } => self.is_dedup(node),
            // Opaque islands: no tag (lineage treats their records as fragile, see the module
            // docs).
            ResolveFutures { .. }
            | ResolveFuturesBlocking { .. }
            | ResolveFuturesOrdered { .. }
            | FlatMapStreamBlocking { .. }
            | ScanAsyncBlocking { .. }
            | ReduceKeyedWatermark { .. }
            | VersionedNetwork { .. }
            | VersionedNetworkFork { .. } => return None,
            Placeholder => return None,
            _ => false,
        };
        if robust {
            return None;
        }
        // Same records in every schedule if every input is (every node here has an input).
        if inputs(node).into_iter().all(|i| self.constant(i)) {
            return None;
        }
        Some(match node {
            AntiJoin { .. } => "anti_join",
            Difference { .. } => "difference",
            CrossSingleton { .. } => "cross_singleton",
            CrossProduct { .. } => "cross_product",
            Join { .. } => "join",
            JoinHalf { .. } => "join_half",
            Fold { .. } => "fold",
            FoldKeyed { .. } => "fold_keyed",
            Reduce { .. } => "reduce",
            ReduceKeyed { .. } => "reduce_keyed",
            Scan { .. } => "scan",
            Enumerate { .. } => "enumerate",
            ChainFirst { .. } => "chain_first",
            Batch { .. } => "snapshot",
            _ => "unlisted",
        })
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
