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
//! - plumbing that passes every record through unchanged (casts, tees, references, the end of an
//!   atomic region, consistency assertions, counters, `observe_nondet`, cycle sources,
//!   `all_ticks`);
//! - element-wise operators with pure closures (`map`, `flat_map`, `filter`, `filter_map`,
//!   `inspect`, partitions);
//! - set-preserving merges and reorderings (`chain`, `merge_ordered`, `sort`, `unique`);
//! - networks; a `batch` (or `atomic`) of a stream into a tick that snapshots no singleton (see
//!   below); sources that are the harness's inputs or fixed (top-level iterators and singleton
//!   sources, cluster members, external inputs);
//! - joins and cross products over a whole run (top level), and a half join at top level, whose
//!   bounded build side is complete before it is probed;
//! - the dedup idiom `fresh = arrived.filter_not_in(seen); seen = seen.chain(fresh)` over a
//!   constant initial state (`View::dedup_idiom`), the `defer_tick` that carries its `seen` to
//!   the next tick, and the initial state's own nodes, provided nothing else reads `seen`, the
//!   carried edge or the initial state: then their records only reach the negation's negative
//!   side, whose tags the rewrite drops, and the idiom's output over the run does not depend on
//!   how many ticks run;
//! - any structural step whose every input is *constant* (`View::constant`): it computes the same
//!   records in every schedule. This is what keeps `use::state`'s initial value
//!   (`initial.filter_if(first_tick)`) robust;
//! - the steps inside a trusted `quorum` (`super::quorum`), which [`classify`] skips.
//!
//! Everything else is fragile. That includes: any operator whose closure (or a source's
//! expression) is not known pure ([`super::purity`], an allowlist: a closure literal naming only
//! listed methods, macros and `std` functions): such a closure may give different results for
//! the same input, so lineage's assumption that each output is a function of its inputs (A3) is
//! not checked for it; a constant inside a tick (`tick.singleton`, `optional_first_tick`, a
//! tick's `source_iter`): it is emitted in every tick that runs, and only if one runs; a `batch`
//! (or `atomic`) into a tick that also snapshots a singleton: the simulator runs that tick only
//! once the singleton has a value (`hydro_lang/src/sim/runtime.rs`, `SingletonHook::is_ready`),
//! so the batched records exist only if whatever feeds the singleton arrived;
//! negation, `cross_singleton` (so `filter_if`),
//! `enumerate`, `chain_first`; folds, reduces and scans anywhere, since even at top level their
//! input order can be a schedule choice (`assume_ordering` at top level is a simulator hook); joins
//! inside a tick; snapshots (a `batch` of a singleton, optional or keyed singleton); closures that
//! read Hydro state by reference; other sources (timers, intervals, streams); and `defer_tick`
//! (but the dedup idiom's): a deferred record reaches the next tick only if a next tick runs at
//! that location, and the simulator runs a tick only when some hook releases something
//! (`hydro_lang/src/sim/compiled.rs`, `SimTick::can_run`), so whether it is ever emitted depends
//! on how the deliveries were batched.
//!
//! Opaque islands (async and versioned operators, which the rewrite cannot see into) get no tag:
//! lineage treats every record through an opaque island as fragile, and an island makes the
//! program not fragile-free.

use std::collections::{HashMap, HashSet};
use std::fmt::{self, Display};

use hydro_lang::compile::builder::CycleId;
use hydro_lang::compile::ir::{CollectionKind, HydroNode, HydroRoot, HydroSource};
use hydro_lang::location::dynamic::LocationId;
use proc_macro2::TokenStream;

use super::choices::{cell, closures};
use super::rewrite::node_key;
use crate::observe::{ec_label, tick_of};

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
    let scan = View::new(ir, trusted);
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

/// Who reads a node's records: another node, or a root (by index in the IR).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Reader {
    Node(*const HydroNode),
    Root(usize),
}

/// A read-only view of the IR, by node address.
struct View<'a> {
    ir: &'a [HydroRoot],
    /// The nodes inside a trusted `quorum` ([`classify`] does not tag them).
    trusted: &'a HashSet<usize>,
    /// Each tick cycle's sink input, and the index of its `CycleSink` root.
    cycle_sinks: HashMap<CycleId, (*const HydroNode, usize)>,
    /// The readers of each node, as [`inputs`] and the closures' state references list them.
    readers: HashMap<*const HydroNode, Vec<Reader>>,
    /// The `DeferTick`s that carry a dedup idiom's state to the next tick ([`Self::dedup_idiom`]).
    dedup_edges: HashSet<*const HydroNode>,
    /// The nodes of the dedup idioms' initial states ([`Self::dedup_idiom`]).
    dedup_inits: HashSet<*const HydroNode>,
    /// The ticks that snapshot a singleton (a `batch` or `atomic` of a `Singleton` into them).
    snapshot_ticks: HashSet<LocationId>,
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

/// Whether one of `node`'s closures, or its source expression, is not known pure
/// ([`super::purity`], an allowlist: A3 is checked only for closures it accepts). An expression
/// that does not print as tokens is not known pure. hydrofrac's own taps are trusted.
pub(crate) fn impure(node: &HydroNode) -> bool {
    let closures = closures(node)
        .into_iter()
        .map(|f| (format!("{f:?}"), true))
        .filter(|(text, _)| !text.starts_with(crate::observe::TAP_CLOSURE_START));
    let source = match node {
        HydroNode::SingletonSource { value, .. } => Some(format!("{value:?}")),
        HydroNode::Source {
            source: HydroSource::Iter(expr),
            ..
        } => Some(format!("{expr:?}")),
        _ => None,
    };
    closures
        .chain(source.map(|s| (s, false)))
        .any(|(text, closure)| {
            text.parse::<TokenStream>()
                .map_or(true, |t| !super::purity::known_pure(t, closure))
        })
}

/// Whether `node`'s records are a stream (or keyed stream), not a snapshot of a scalar.
fn is_stream(node: &HydroNode) -> bool {
    !matches!(
        node.metadata().collection_kind,
        CollectionKind::Singleton { .. }
            | CollectionKind::Optional { .. }
            | CollectionKind::KeyedSingleton { .. }
    )
}

impl<'a> View<'a> {
    fn new(ir: &'a [HydroRoot], trusted: &'a HashSet<usize>) -> Self {
        let cycle_sinks = ir
            .iter()
            .enumerate()
            .filter_map(|(index, root)| match root {
                HydroRoot::CycleSink {
                    cycle_id, input, ..
                } => Some((*cycle_id, (std::ptr::from_ref(&**input), index))),
                _ => None,
            })
            .collect();
        let mut view = View {
            ir,
            trusted,
            cycle_sinks,
            readers: HashMap::new(),
            dedup_edges: HashSet::new(),
            dedup_inits: HashSet::new(),
            snapshot_ticks: HashSet::new(),
        };
        let nodes = view.nodes();
        let mut readers: HashMap<*const HydroNode, Vec<Reader>> = HashMap::new();
        for (index, root) in ir.iter().enumerate() {
            let mut read = |ptr| readers.entry(ptr).or_default().push(Reader::Root(index));
            read(std::ptr::from_ref(root.input()));
            if let HydroRoot::ForEach { f, .. } = root {
                f.singleton_refs()
                    .iter()
                    .filter_map(|(r, _)| cell(r))
                    .for_each(&mut read);
            }
        }
        for &ptr in &nodes {
            // SAFETY: as in `classify`.
            let node = unsafe { &*ptr };
            let state = closures(node)
                .into_iter()
                .flat_map(|f| f.singleton_refs().iter().filter_map(|(r, _)| cell(r)));
            for input in inputs(node).into_iter().chain(state) {
                readers.entry(input).or_default().push(Reader::Node(ptr));
            }
            if let HydroNode::Batch { inner, metadata }
            | HydroNode::Snapshot { inner, metadata }
            | HydroNode::BeginAtomic { inner, metadata } = node
                // Upstream's `latest()` snapshots an `Optional` (`InitNone`); treat it like a
                // singleton here, which can only mark more records fragile.
                && matches!(
                    inner.metadata().collection_kind,
                    CollectionKind::Singleton { .. } | CollectionKind::Optional { .. }
                )
                && let Some(tick) = tick_of(&metadata.location_id)
            {
                view.snapshot_ticks.insert(tick.clone());
            }
        }
        view.readers = readers;
        for &ptr in &nodes {
            // SAFETY: as in `classify`.
            if let Some((edge, init)) = view.dedup_idiom(unsafe { &*ptr }) {
                view.dedup_edges.insert(edge);
                view.dedup_inits.extend(init);
            }
        }
        view
    }

    /// The nodes and roots that read `ptr`'s records, looking through `Tee`s.
    fn consumers(&self, ptr: *const HydroNode) -> Vec<Reader> {
        let (mut seen, mut stack, mut out) = (HashSet::new(), vec![ptr], vec![]);
        while let Some(ptr) = stack.pop() {
            if !seen.insert(ptr) {
                continue;
            }
            for &reader in self.readers.get(&ptr).into_iter().flatten() {
                match reader {
                    // SAFETY: as in `classify`.
                    Reader::Node(r) if matches!(unsafe { &*r }, HydroNode::Tee { .. }) => {
                        stack.push(r);
                    }
                    reader => out.push(reader),
                }
            }
        }
        out
    }

    /// Whether only `allowed` read `ptr`'s records, besides readers whose every path to a root
    /// passes a node that tags what it emits ([`Self::covered`]).
    fn read_only_by(&self, ptr: *const HydroNode, allowed: &[Reader]) -> bool {
        self.consumers(ptr).iter().all(|r| {
            allowed.contains(r)
                || matches!(r, Reader::Node(n) if self.covered(*n, &mut HashSet::new()))
        })
    }

    /// Whether every record `ptr` emits reaches the roots only through a node that tags what it
    /// emits ([`Self::tags`]): `ptr` itself, or every reader of it, recursively.
    fn covered(&self, ptr: *const HydroNode, seen: &mut HashSet<*const HydroNode>) -> bool {
        if self.tags(ptr) || !seen.insert(ptr) {
            return true;
        }
        self.consumers(ptr).iter().all(|r| match r {
            Reader::Node(n) => self.covered(*n, seen),
            Reader::Root(_) => false,
        })
    }

    /// Whether `ptr` is fragile whatever its context: a fold, reduce or scan, a
    /// `cross_singleton` (`filter_if`), `enumerate`, `chain_first`, or a join inside a tick,
    /// outside a trusted `quorum`, with an input that is not constant. Its records carry its
    /// tag, so a record it reads needs no tag of its own.
    fn tags(&self, ptr: *const HydroNode) -> bool {
        use HydroNode::*;
        // SAFETY: as in `classify`.
        let node = unsafe { &*ptr };
        let kind = match node {
            Fold { .. } | FoldKeyed { .. } | Reduce { .. } | ReduceKeyed { .. } | Scan { .. } => {
                true
            }
            CrossSingleton { .. } | Enumerate { .. } | ChainFirst { .. } => true,
            Join { .. } | CrossProduct { .. } | JoinHalf { .. } => {
                !node.metadata().location_id.is_top_level()
            }
            _ => false,
        };
        kind && !self.trusted.contains(&node_key(node))
            && !inputs(node).into_iter().all(|i| self.constant(i))
    }

    /// `ptr` and every node it is computed from, through shared nodes.
    fn upstream(&self, ptr: *const HydroNode) -> Vec<*const HydroNode> {
        let (mut seen, mut stack, mut out) = (HashSet::new(), vec![ptr], vec![]);
        while let Some(ptr) = stack.pop() {
            if seen.insert(ptr) {
                out.push(ptr);
                // SAFETY: as in `classify`.
                stack.extend(inputs(unsafe { &*ptr }));
            }
        }
        out
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
        if let Placeholder = node {
            return None;
        }
        if impure(node) {
            // The closure may compute different records from the same inputs (A3).
            return Some("impure_closure");
        }
        if reads_state(node) {
            // A closure that reads Hydro state sees whatever the state holds at that moment.
            return Some("closure_over_state");
        }
        let top_level = node.metadata().location_id.is_top_level();
        let tick = tick_of(&node.metadata().location_id);
        // A constant at top level is emitted once. Inside a tick it is emitted in every tick that
        // runs, so its records over the run depend on whether (and, downstream, how often) the
        // tick ran, unless it is a dedup idiom's initial state, which only that idiom reads.
        let fixed = tick.is_none() || self.dedup_inits.contains(&std::ptr::from_ref(node));
        // The simulator runs a tick that snapshots a singleton only once that singleton has a
        // value, so a record batched into it exists only if what feeds the singleton arrived.
        let beside_snapshot = tick.is_some_and(|t| self.snapshot_ticks.contains(t));
        let robust = match node {
            // Plumbing: every record passes through unchanged (or, for `observe_nondet`, the
            // records are reordered, never added or dropped).
            Cast { .. }
            | ObserveNonDet { .. }
            | Tee { .. }
            | Reference { .. }
            | EndAtomic { .. }
            | AssertIsConsistent { .. }
            | Counter { .. }
            | UnboundSingleton { .. }
            | CycleSource { .. }
            | YieldConcat { .. } => true,
            // A deferred record exists only if a later tick runs (see the module docs), except on
            // the dedup idiom's state edge, which only feeds a negation's negative side.
            DeferTick { .. } => self.dedup_edges.contains(&std::ptr::from_ref(node)),
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
            // A batch of a stream decides when a record enters a tick, not whether, if nothing
            // else can keep the tick from running. The simulator lowers `atomic` to a batch
            // (`hydro_lang/src/sim/builder.rs`, `begin_atomic`).
            Batch { inner, .. } | Snapshot { inner, .. } | BeginAtomic { inner, .. } => {
                if is_stream(inner) && !beside_snapshot {
                    return None;
                }
                return Some(if is_stream(inner) {
                    "batch_beside_snapshot"
                } else {
                    "snapshot"
                });
            }
            // The harness's inputs, fixed values, and the (static) cluster membership. Decided
            // here: a source has no inputs, so the constant-inputs exemption below would hold
            // vacuously.
            Source { source, .. } => {
                return match source {
                    HydroSource::ClusterMembers(..) | HydroSource::ExternalNetwork() => None,
                    HydroSource::Iter(_) if fixed => None,
                    HydroSource::Iter(_) => Some("tick_constant"),
                    _ => Some("source"),
                };
            }
            SingletonSource { .. } => return (!fixed).then_some("tick_constant"),
            ExternalInput { .. } => true,
            // Over a whole run, a symmetric join emits every matching pair once both sides have
            // arrived; a half join's bounded build side is complete before it is probed.
            Join { .. } | CrossProduct { .. } | JoinHalf { .. } => top_level,
            // The dedup idiom: every record of `pos` not in the constant initial state.
            Difference { .. } => self.dedup_idiom(node).is_some(),
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
        // Same records in every schedule if every input is (every node here has an input). The
        // constants inside a tick are tagged at their source, so their tags still reach the
        // output. Not for `defer_tick`, whose records exist only if a later tick runs.
        if !matches!(node, DeferTick { .. }) && inputs(node).into_iter().all(|i| self.constant(i))
        {
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
            DeferTick { .. } => "defer_tick",
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

    /// If `node` is `filter_not_in` against its own history, the `DeferTick` that carries that
    /// history to the next tick. That is a `Difference` whose `neg` is a tick state that starts
    /// as a constant and then accumulates the `Difference`'s own output (`seen = seen.chain(fresh)`
    /// with `fresh = arrived.filter_not_in(seen)`, the idiom of `use::state`). Its output over the
    /// run is then every record of `pos` not in the initial state, whatever ticks they arrive in.
    /// Ported from the research branch's `Cones::self_difference`
    /// (`origin/hydrofrac-sensitivity`, `sensitivity.rs`).
    ///
    /// Only if nothing else reads the idiom's state untagged: `seen` is read only by the negation
    /// and the cycle's sink, the sink only by its `CycleSink`, the cycle source only by the
    /// carrying `DeferTick`, which only `seen` reads, and the initial state's nodes only by each
    /// other and `seen`, besides readers that tag everything they emit ([`Self::tags`]). Another
    /// reader of any of them (`seen.clone().all_ticks()`, say) would emit records that exist only
    /// if a later tick runs, untagged. Returns the carrying `DeferTick` and the nodes of the
    /// initial state.
    fn dedup_idiom(&self, node: &HydroNode) -> Option<(*const HydroNode, Vec<*const HydroNode>)> {
        let HydroNode::Difference { neg, .. } = node else {
            return None;
        };
        let me = std::ptr::from_ref(node);
        let state = strip(std::ptr::from_ref(&**neg));
        // SAFETY: as in `strip`.
        let HydroNode::Chain { first, second, .. } = (unsafe { &*state }) else {
            return None;
        };
        // The cycle a side carries over from the previous tick, and its `DeferTick`, if it does.
        let carried = |side: &HydroNode| -> Option<(CycleId, *const HydroNode)> {
            let defer = strip(std::ptr::from_ref(side));
            // SAFETY: as in `strip`.
            let HydroNode::DeferTick { input, .. } = (unsafe { &*defer }) else {
                return None;
            };
            // SAFETY: as in `strip`.
            match unsafe { &*strip(std::ptr::from_ref(&**input)) } {
                HydroNode::CycleSource { cycle_id, .. } => Some((*cycle_id, defer)),
                _ => None,
            }
        };
        let ((cycle, defer), init) = match (carried(first), carried(second)) {
            (Some(c), _) => (c, std::ptr::from_ref(&**second)),
            (None, Some(c)) => (c, std::ptr::from_ref(&**first)),
            (None, None) => return None,
        };
        let &(sink, root) = self.cycle_sinks.get(&cycle)?;
        let sink = strip(sink);
        // SAFETY: as in `strip`.
        let HydroNode::Chain { first, second, .. } = (unsafe { &*sink }) else {
            return None;
        };
        let parts = (
            strip(std::ptr::from_ref(&**first)),
            strip(std::ptr::from_ref(&**second)),
        );
        if !((parts == (state, me) || parts == (me, state)) && self.constant(init)) {
            return None;
        }
        // SAFETY: as in `strip`.
        let HydroNode::DeferTick { input, .. } = (unsafe { &*defer }) else {
            return None;
        };
        let source = strip(std::ptr::from_ref(&**input));
        let init_nodes = self.upstream(init);
        let mut in_init: Vec<Reader> = init_nodes.iter().map(|&n| Reader::Node(n)).collect();
        in_init.push(Reader::Node(state));
        let exclusive = self.read_only_by(state, &[Reader::Node(me), Reader::Node(sink)])
            && self.read_only_by(sink, &[Reader::Root(root)])
            && self.read_only_by(source, &[Reader::Node(defer)])
            && self.read_only_by(defer, &[Reader::Node(state)])
            && init_nodes.iter().all(|&n| self.read_only_by(n, &in_init));
        exclusive.then_some((defer, init_nodes))
    }
}
