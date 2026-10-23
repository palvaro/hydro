//! Schedule sensitivity of labelled collections (docs/design/hydrofrac-lite.md §4.9).
//!
//! The search prunes a hypothesis when some derivation seen in an earlier run survives it:
//! the derivation's deliveries all still happen, so (the reasoning goes) the fact is still
//! derived. That holds when each fault set has a single execution, as in Molly: the simulator
//! runs top-level (async) dataflow deterministically and branches only where the program took a
//! `nondet!` guard. Where it does branch, two things break the reasoning:
//! - **U1, existence.** Whether a record exists can depend on the choice: a `batch` decides which
//!   tick an element lands in and so which state it meets. A derivation through such a site is a
//!   derivation *under the schedule that produced it*.
//! - **U2, send order.** `crash(n after m)` cuts `n`'s sends at `m`, and which sends come before
//!   `m` depends on the order in which `n` sent them. A choice anywhere upstream of what `n`
//!   receives can reorder `n`'s sends, even when the derivation itself passes no site.
//!
//! [`label_cones`](crate::provenance::sensitivity::label_cones) lists, for every labelled collection of a program, the **sites** that make its
//! search schedule-sensitive. It walks the label's **causal cone**: everything upstream of it,
//! across locations (a network back to its sender), through cycles (a `forward_ref` back to its
//! sink) and through captured state (a `by_ref`/`by_mut` cell back to its producer and to every
//! closure that writes it), following the research branch's order check
//! (`hydro_lang/src/sim/order.rs` at `0d801bbafc`, `Cones::nondet_upstream`). For U2 it then
//! walks, for every location that sends a delivery in the cone, the cone of everything at that
//! location: a site there may reorder that location's sends.
//!
//! The walk is conservative. A site is either a place where the simulator makes a choice
//! ([`NondetSite::opaque`](crate::provenance::sensitivity::NondetSite::opaque) false: `batch`, an atomic section's entry, an untrusted
//! `observe_nondet` or the one a `by_mut` closure over an unordered input implies, `merge_ordered`,
//! a fold over an unordered input, timers), or a place the walk cannot see through (`opaque`
//! true): an operator it does not know (every IR node kind is classified explicitly, and a new
//! one does not compile until it is), state it cannot resolve, a cycle without its sink, an
//! external input whose order is unspecified, an opaque stream source, futures and async streams,
//! multi-version networks. Either kind makes the label schedule-sensitive. A label with no site
//! is one whose fault sets each have one execution, for which lineage pruning is Molly's.
//!
//! That is [`Analysis::Conservative`]. What the check uses is sharper, [`Analysis::Calm`]
//! (docs/design/hydrofrac-lite.md §4.9, "Which choices matter"):
//! - **U1.** A choice that only decides the order or the batching of records it passes through
//!   unchanged (`batch`, an atomic section's entry, `merge_ordered`, an order-only
//!   `observe_nondet`, an unordered external input) is discharged when every path from it to
//!   the label goes through operators whose output over the run is a monotone function of the
//!   *set* of their inputs' records over the run (pure `map`/`filter`/`flat_map`, unions,
//!   dedup, sorts, deferral, networks, `all_ticks`, joins of two persisted inputs, and
//!   `filter_not_in` against its own history). Anything that observes a tick (a snapshot, a
//!   per-tick aggregate or join, an anti-join), state read by reference, an impure closure or an
//!   unclassified operator on the path keeps it, and names that operator
//!   ([`NondetSite::via`]). Every other kind of site counts on any path.
//! - **U2** stays conservative, but only what a sender sends can reorder its sends: the walk
//!   takes the cone of the inputs of the location's networks, not of everything at it.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::{self, Display};

use hydro_lang::compile::builder::CycleId;
use hydro_lang::compile::ir::backtrace::Backtrace;
use hydro_lang::compile::ir::{
    ClosureExpr, CollectionKind, HydroNode, HydroRoot, HydroSource, StreamOrder, StreamRetry,
};
use hydro_lang::location::dynamic::{ClusterConsistency, LocationId};

use crate::observe::{EcCollection, ec_collections, kind_name};

/// One site that makes a label's search schedule-sensitive (see the [module docs](self)).
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NondetSite {
    /// The user's source line of the operator (`file:line`, the file by its name), else its
    /// tag, else the operator's name.
    pub site: String,
    /// What the site is (`batch`, `observe_nondet`, `unknown_operator`, ...).
    pub op: &'static str,
    /// The kind of collection the operator produces (`Stream`, `Singleton`, ...).
    pub kind: &'static str,
    /// The location the operator runs at (its root: the process or cluster).
    pub location: LocationId,
    /// Whether the operator runs at the labelled collection's own location (else it is upstream
    /// across a network).
    pub local: bool,
    /// Whether this is a place the walk cannot see through (an unknown operator, unresolved
    /// state, an opaque source, ...), rather than a choice the simulator makes.
    pub opaque: bool,
    /// Whether the site is outside the label's own cone, reached only through a location that
    /// sends a delivery in the cone: it may reorder that location's sends (U2), not the
    /// derivation itself.
    pub send_order: bool,
    /// For a site in the label's own cone reached on an order- or batch-sensitive path (under
    /// [`Analysis::Calm`]): the operator that made the path sensitive, the one nearest the site
    /// the walk found (`JoinHalf at two_pc_ctp.rs:156`). `None` for a site that counts by itself
    /// on an order-insensitive path (a fold over an unordered input, a timer, something opaque),
    /// for a send-order site, and under [`Analysis::Conservative`].
    pub via: Option<String>,
}

impl Display for NondetSite {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.opaque {
            write!(f, "cannot see through ")?;
        }
        write!(f, "{} at {}", self.op, self.site)?;
        if let Some(via) = &self.via {
            write!(f, " via {via}")?;
        }
        if self.send_order {
            write!(f, " (reorders a sender)")
        } else if !self.local {
            write!(f, " (upstream)")
        } else {
            Ok(())
        }
    }
}

/// The causal cone of one labelled collection.
#[derive(Clone, Debug)]
pub struct LabelCone {
    /// The labelled collection, as [`ec_collections`] lists it.
    pub collection: EcCollection,
    /// The sites that make its search schedule-sensitive: those in its cone, then those that may
    /// reorder the sends of a location that sends a delivery in the cone. Deduplicated by site,
    /// in discovery order.
    pub sites: Vec<NondetSite>,
    /// Choices in the cone that [`Analysis::Calm`] discharged: every path from them to the
    /// label is order- and batch-insensitive, and no sender's send order runs through them (else
    /// they are in [`sites`](Self::sites), as send-order sites). Empty under
    /// [`Analysis::Conservative`].
    pub harmless: Vec<NondetSite>,
    /// The IR operators in the cone (for scale: how much of the program feeds the label).
    pub cone_nodes: usize,
}

impl LabelCone {
    /// Whether some derivation of the collection, or the positional kill relation over it, may
    /// depend on the schedule (see the [module docs](self)).
    pub fn schedule_sensitive(&self) -> bool {
        !self.sites.is_empty()
    }

    /// Whether a site in the label's own cone may change which records exist (U1), as opposed
    /// to only the order of a sender's sends (U2).
    pub fn existence_sensitive(&self) -> bool {
        self.sites.iter().any(|s| !s.send_order)
    }

    /// The sites grouped by operator, for a one-line summary: `batch×3, observe_nondet×1`.
    pub fn summary(&self) -> String {
        let mut counts: BTreeMap<&'static str, usize> = BTreeMap::new();
        for s in &self.sites {
            *counts.entry(s.op).or_default() += 1;
        }
        if counts.is_empty() {
            return "-".to_owned();
        }
        counts
            .into_iter()
            .map(|(op, n)| format!("{op}×{n}"))
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// What a node is to the walk: a choice the simulator makes, something the walk cannot see
/// through, or neither.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeClass {
    /// The node's output is a function of its inputs, in every schedule.
    Deterministic,
    /// The simulator chooses here (the name says what the choice is).
    Choice(&'static str),
    /// The walk cannot tell, so it assumes the worst (the name says why).
    Opaque(&'static str),
}

/// Whether a stream-like collection kind leaves its elements' order unspecified.
fn unordered(kind: &CollectionKind) -> bool {
    matches!(
        kind,
        CollectionKind::Stream {
            order: StreamOrder::NoOrder,
            ..
        } | CollectionKind::KeyedStream {
            value_order: StreamOrder::NoOrder,
            ..
        }
    )
}

/// Whether `node` is an input from outside the program.
fn is_external_input(node: &HydroNode) -> bool {
    matches!(
        node,
        HydroNode::ExternalInput { .. }
            | HydroNode::Source {
                source: HydroSource::ExternalNetwork(),
                ..
            }
    )
}

/// An `observe_nondet` the simulator turns into the identity: a retry cast alone (`AtLeastOnce`
/// to `ExactlyOnce`, same order and boundedness), as `SimBuilder::observe_nondet` decides.
fn retry_cast_only(input: &CollectionKind, output: &CollectionKind) -> bool {
    match (input, output) {
        (
            CollectionKind::Stream {
                bound: b1,
                order: o1,
                retry: StreamRetry::AtLeastOnce,
                ..
            },
            CollectionKind::Stream {
                bound: b2,
                order: o2,
                retry: StreamRetry::ExactlyOnce,
                ..
            },
        ) => b1 == b2 && o1 == o2,
        (
            CollectionKind::KeyedStream {
                bound: b1,
                value_order: o1,
                value_retry: StreamRetry::AtLeastOnce,
                ..
            },
            CollectionKind::KeyedStream {
                bound: b2,
                value_order: o2,
                value_retry: StreamRetry::ExactlyOnce,
                ..
            },
        ) => b1 == b2 && o1 == o2,
        _ => false,
    }
}

/// Classifies one IR node (see [`NodeClass`]). Every node kind is listed: a new one does not
/// compile until it is classified here, and an unknown node (`Placeholder`) is opaque.
///
/// The choices follow what `SimBuilder` turns into a decision hook: `batch` and an atomic
/// section's entry (a batch hook), an untrusted `observe_nondet` that is not a bare retry cast,
/// the `observe_for_mut` a closure with a mutable reference implies over a non-strict input,
/// `merge_ordered`, a fold over an unordered input (a fold hook: which versions, or which order),
/// and the time-driven sources.
pub fn classify(node: &HydroNode) -> NodeClass {
    use NodeClass::{Choice, Deterministic, Opaque};
    let input_kind = |n: &HydroNode| -> Option<CollectionKind> {
        n.input()
            .first()
            .map(|i| i.metadata().collection_kind.clone())
    };
    // A closure with a mutable reference over a non-strict input is observed by the simulator
    // first (`maybe_observe_for_mut`).
    let observed_for_mut = |n: &HydroNode| -> bool {
        closures(n).iter().any(|f| f.has_mut_ref()) && input_kind(n).is_some_and(|k| !k.is_strict())
    };
    match node {
        HydroNode::Placeholder => Opaque("unknown_operator"),
        HydroNode::Batch { .. } => Choice("batch"),
        HydroNode::BeginAtomic { .. } => Choice("begin_atomic"),
        // An external input the program receives without an order (`sim_input::<_, NoOrder, _>`
        // is an ordered input weakened by a trusted cast): in a deployment it arrives in any
        // order, which the simulator does not explore.
        HydroNode::Cast { inner, metadata }
        | HydroNode::ObserveNonDet {
            inner,
            trusted: true,
            metadata,
        } if is_external_input(inner)
            && unordered(&metadata.collection_kind)
            && !unordered(&inner.metadata().collection_kind) =>
        {
            Opaque("unordered_external_input")
        }
        HydroNode::ObserveNonDet {
            inner,
            trusted,
            metadata,
        } => {
            if *trusted
                || retry_cast_only(&inner.metadata().collection_kind, &metadata.collection_kind)
            {
                Deterministic
            } else {
                Choice("observe_nondet")
            }
        }
        HydroNode::MergeOrdered { .. } => Choice("merge_ordered"),
        // Garbage collection by watermark: what survives depends on when the watermark arrives
        // relative to the values.
        HydroNode::ReduceKeyedWatermark { .. } => Choice("reduce_keyed_watermark"),
        HydroNode::Source { source, metadata } => match source {
            HydroSource::Spin() => Choice("spin"),
            HydroSource::Interval(_) => Choice("timer"),
            HydroSource::Stream(expr) => {
                let text = format!("{expr:?}");
                if text.contains("interval") || text.contains("Instant") {
                    Choice("timer")
                } else {
                    // An arbitrary stream: nothing says when or in what order it yields.
                    Opaque("stream_source")
                }
            }
            HydroSource::ExternalNetwork() => {
                if unordered(&metadata.collection_kind) {
                    Opaque("unordered_external_input")
                } else {
                    Deterministic
                }
            }
            HydroSource::Iter(_) | HydroSource::ClusterMembers(..) => Deterministic,
            HydroSource::Embedded(_) | HydroSource::EmbeddedSingleton(_) => {
                Opaque("embedded_source")
            }
        },
        HydroNode::ExternalInput { metadata, .. } => {
            if unordered(&metadata.collection_kind) {
                Opaque("unordered_external_input")
            } else {
                Deterministic
            }
        }
        HydroNode::ResolveFutures { .. }
        | HydroNode::ResolveFuturesBlocking { .. }
        | HydroNode::ResolveFuturesOrdered { .. } => Opaque("futures"),
        HydroNode::FlatMapStreamBlocking { .. } => Opaque("async_stream"),
        HydroNode::ScanAsyncBlocking { .. } => Opaque("async_scan"),
        HydroNode::VersionedNetworkFork { .. } | HydroNode::VersionedNetwork { .. } => {
            Opaque("versioned_network")
        }
        HydroNode::Fold { input, .. }
        | HydroNode::FoldKeyed { input, .. }
        | HydroNode::Reduce { input, .. }
        | HydroNode::ReduceKeyed { input, .. }
        | HydroNode::Scan { input, .. } => {
            if unordered(&input.metadata().collection_kind) {
                Choice("fold_unordered")
            } else if observed_for_mut(node) {
                Choice("observe_for_mut")
            } else {
                Deterministic
            }
        }
        HydroNode::Map { .. }
        | HydroNode::FlatMap { .. }
        | HydroNode::Filter { .. }
        | HydroNode::FilterMap { .. }
        | HydroNode::Inspect { .. }
        | HydroNode::PartitionShared { .. } => {
            if observed_for_mut(node) {
                Choice("observe_for_mut")
            } else {
                Deterministic
            }
        }
        HydroNode::Cast { .. }
        | HydroNode::SingletonSource { .. }
        | HydroNode::CycleSource { .. }
        | HydroNode::Tee { .. }
        | HydroNode::Reference { .. }
        | HydroNode::PartitionSide { .. }
        | HydroNode::EndAtomic { .. }
        | HydroNode::YieldConcat { .. }
        | HydroNode::Chain { .. }
        | HydroNode::ChainFirst { .. }
        | HydroNode::CrossProduct { .. }
        | HydroNode::CrossSingleton { .. }
        | HydroNode::Join { .. }
        | HydroNode::JoinHalf { .. }
        | HydroNode::Difference { .. }
        | HydroNode::AntiJoin { .. }
        | HydroNode::DeferTick { .. }
        | HydroNode::Enumerate { .. }
        | HydroNode::Unique { .. }
        | HydroNode::Sort { .. }
        | HydroNode::Network { .. }
        | HydroNode::Counter { .. }
        | HydroNode::AssertIsConsistent { .. }
        | HydroNode::UnboundSingleton { .. } => Deterministic,
    }
}

/// What a node contributes as a site, if it is one (see [`classify`]): its name, and whether
/// it is opaque rather than a choice.
pub fn nondet_op(node: &HydroNode) -> Option<(&'static str, bool)> {
    match classify(node) {
        NodeClass::Deterministic => None,
        NodeClass::Choice(op) => Some((op, false)),
        NodeClass::Opaque(op) => Some((op, true)),
    }
}

/// The closures of a node (those that may capture state). Every node kind is listed, as in
/// [`classify`].
fn closures(node: &HydroNode) -> Vec<&ClosureExpr> {
    match node {
        HydroNode::Map { f, .. }
        | HydroNode::FlatMap { f, .. }
        | HydroNode::FlatMapStreamBlocking { f, .. }
        | HydroNode::Filter { f, .. }
        | HydroNode::FilterMap { f, .. }
        | HydroNode::Inspect { f, .. }
        | HydroNode::Reduce { f, .. }
        | HydroNode::ReduceKeyed { f, .. }
        | HydroNode::ReduceKeyedWatermark { f, .. }
        | HydroNode::PartitionShared { f, .. } => vec![f],
        HydroNode::Fold { init, acc, .. }
        | HydroNode::FoldKeyed { init, acc, .. }
        | HydroNode::Scan { init, acc, .. }
        | HydroNode::ScanAsyncBlocking { init, acc, .. } => vec![init, acc],
        HydroNode::Placeholder
        | HydroNode::Cast { .. }
        | HydroNode::ObserveNonDet { .. }
        | HydroNode::Source { .. }
        | HydroNode::SingletonSource { .. }
        | HydroNode::CycleSource { .. }
        | HydroNode::Tee { .. }
        | HydroNode::Reference { .. }
        | HydroNode::PartitionSide { .. }
        | HydroNode::BeginAtomic { .. }
        | HydroNode::EndAtomic { .. }
        | HydroNode::Batch { .. }
        | HydroNode::YieldConcat { .. }
        | HydroNode::Chain { .. }
        | HydroNode::MergeOrdered { .. }
        | HydroNode::ChainFirst { .. }
        | HydroNode::CrossProduct { .. }
        | HydroNode::CrossSingleton { .. }
        | HydroNode::Join { .. }
        | HydroNode::JoinHalf { .. }
        | HydroNode::Difference { .. }
        | HydroNode::AntiJoin { .. }
        | HydroNode::ResolveFutures { .. }
        | HydroNode::ResolveFuturesBlocking { .. }
        | HydroNode::ResolveFuturesOrdered { .. }
        | HydroNode::DeferTick { .. }
        | HydroNode::Enumerate { .. }
        | HydroNode::Unique { .. }
        | HydroNode::Sort { .. }
        | HydroNode::Network { .. }
        | HydroNode::VersionedNetworkFork { .. }
        | HydroNode::VersionedNetwork { .. }
        | HydroNode::ExternalInput { .. }
        | HydroNode::Counter { .. }
        | HydroNode::AssertIsConsistent { .. }
        | HydroNode::UnboundSingleton { .. } => vec![],
    }
}

/// The state cells a closure captures: `(cell, is_mut)`, the cell by the address of the node
/// the reference shares; `None` for a captured reference the walk cannot resolve to its cell.
pub(crate) fn captures(f: &ClosureExpr) -> Vec<Option<(*const HydroNode, bool)>> {
    f.singleton_refs()
        .iter()
        .map(|(node, is_mut)| match node {
            HydroNode::Reference { inner, .. } => {
                Some((std::ptr::from_ref(&*inner.0.borrow()), *is_mut))
            }
            _ => None,
        })
        .collect()
}

/// The direct inputs of `node`, through what a `Tee`, reference or partition side shares.
fn shared_inputs(node: &HydroNode) -> Vec<*const HydroNode> {
    match node {
        HydroNode::Tee { inner, .. }
        | HydroNode::Reference { inner, .. }
        | HydroNode::PartitionSide { inner, .. } => vec![std::ptr::from_ref(&*inner.0.borrow())],
        // Its input is found "in separate special ways"; the node is opaque to the walk anyway.
        HydroNode::VersionedNetwork { .. } | HydroNode::Placeholder => vec![],
        _ => node.input().into_iter().map(std::ptr::from_ref).collect(),
    }
}

/// The `file:line` of the first frame of `backtrace` outside `hydro_lang` itself (as
/// `observe::user_span`), the file by its name.
fn user_span(backtrace: &Backtrace) -> Option<String> {
    let own = concat!("hydro_lang", "/src/");
    backtrace
        .elements()
        .find_map(|element| {
            let file = element.filename.as_ref()?;
            if file.contains(own) {
                return None;
            }
            Some(format!("{file}:{}", element.lineno?))
        })
        .or_else(|| backtrace.format_span())
        .map(|span| {
            span.rsplit(['/', '\\'])
                .next()
                .unwrap_or(span.as_str())
                .to_owned()
        })
}

fn op_name(node: &HydroNode) -> String {
    let name = node.print_root();
    name.split('(').next().unwrap_or(&name).to_owned()
}

fn site_of(node: &HydroNode) -> String {
    node.metadata()
        .tag
        .clone()
        .or_else(|| user_span(&node.metadata().op.backtrace))
        .unwrap_or_else(|| op_name(node))
}

/// The identity [`ec_collections`] gives a labelled node (as `observe`'s `ec_key`).
fn label_key(node: &HydroNode) -> (String, String, &'static str, LocationId) {
    (
        node.metadata()
            .tag
            .clone()
            .or_else(|| user_span(&node.metadata().op.backtrace))
            .unwrap_or_else(|| op_name(node)),
        op_name(node),
        kind_name(&node.metadata().collection_kind),
        node.metadata().location_id.root().clone(),
    )
}

fn is_ec(node: &HydroNode) -> bool {
    matches!(
        node.metadata().consistency,
        Some(ClusterConsistency::EventualConsistency)
    ) && matches!(node.metadata().location_id.root(), LocationId::Cluster(_))
}

/// The causal-cone walker over one IR.
struct Cones {
    /// Each `forward_ref`'s sink input.
    cycle_sinks: HashMap<CycleId, *const HydroNode>,
    /// Per state cell, the nodes that feed its writers: a node whose closure captures the cell
    /// mutably, or the input of a `for_each` whose closure does.
    writers: HashMap<*const HydroNode, Vec<*const HydroNode>>,
    /// Every labelled node, by its identity.
    labelled: HashMap<(String, String, &'static str, LocationId), Vec<*const HydroNode>>,
    /// Every node, by the root of its location (for the conservative send-order walk, U2).
    by_location: HashMap<LocationId, Vec<*const HydroNode>>,
    /// Per location, what it sends: the input of every network it sends on (and every
    /// multi-version network fork at it). Only these decide the order of its sends (U2).
    sends_by_location: HashMap<LocationId, Vec<*const HydroNode>>,
    /// `for_each` roots whose closures capture a reference the walk cannot resolve, by the
    /// location they run at: what they write is unknown.
    unresolved_writes: Vec<LocationId>,
}

/// One walk's findings: its sites, and every node it reached.
struct Walk {
    sites: Vec<NondetSite>,
    seen: HashSet<*const HydroNode>,
}

impl Cones {
    /// Indexes `ir`. The IR must not move or change while the walker is used: it holds raw
    /// pointers into it.
    fn new(ir: &[HydroRoot]) -> Self {
        let mut cones = Cones {
            cycle_sinks: HashMap::new(),
            writers: HashMap::new(),
            labelled: HashMap::new(),
            by_location: HashMap::new(),
            sends_by_location: HashMap::new(),
            unresolved_writes: vec![],
        };
        let mut seen: HashSet<*const HydroNode> = HashSet::new();
        fn index(node: &HydroNode, seen: &mut HashSet<*const HydroNode>, cones: &mut Cones) {
            if !seen.insert(std::ptr::from_ref(node)) {
                return;
            }
            match node {
                HydroNode::Network { input, .. } => cones
                    .sends_by_location
                    .entry(input.metadata().location_id.root().clone())
                    .or_default()
                    .push(std::ptr::from_ref(&**input)),
                HydroNode::VersionedNetworkFork { senders, .. } => {
                    for (_, sender, _) in senders {
                        cones
                            .sends_by_location
                            .entry(sender.metadata().location_id.root().clone())
                            .or_default()
                            .push(std::ptr::from_ref(node));
                    }
                }
                _ => {}
            }
            if !matches!(node, HydroNode::Placeholder) {
                cones
                    .by_location
                    .entry(node.metadata().location_id.root().clone())
                    .or_default()
                    .push(std::ptr::from_ref(node));
            }
            if is_ec(node) {
                cones
                    .labelled
                    .entry(label_key(node))
                    .or_default()
                    .push(std::ptr::from_ref(node));
            }
            for closure in closures(node) {
                for (cell, is_mut) in captures(closure).into_iter().flatten() {
                    if is_mut {
                        cones
                            .writers
                            .entry(cell)
                            .or_default()
                            .push(std::ptr::from_ref(node));
                    }
                    // SAFETY: `cell` points into the IR, which outlives this call.
                    index(unsafe { &*cell }, seen, cones);
                }
            }
            for input in shared_inputs(node) {
                // SAFETY: `input` points into the IR, which outlives this call.
                index(unsafe { &*input }, seen, cones);
            }
        }
        for root in ir {
            match root {
                HydroRoot::CycleSink {
                    cycle_id, input, ..
                } => {
                    cones
                        .cycle_sinks
                        .insert(*cycle_id, std::ptr::from_ref(&**input));
                }
                HydroRoot::ForEach { f, input, .. } => {
                    for capture in captures(f) {
                        match capture {
                            Some((cell, true)) => cones
                                .writers
                                .entry(cell)
                                .or_default()
                                .push(std::ptr::from_ref(&**input)),
                            Some((_, false)) => {}
                            None => cones
                                .unresolved_writes
                                .push(input.metadata().location_id.root().clone()),
                        }
                    }
                }
                _ => {}
            }
            index(root.input(), &mut seen, &mut cones);
        }
        cones
    }

    /// The sites in the causal cone of `starts`, labelled relative to the location `at`, and
    /// every node the walk reached.
    fn walk(&self, starts: &[*const HydroNode], at: &LocationId) -> Walk {
        let mut seen: HashSet<*const HydroNode> = HashSet::new();
        let mut sites: Vec<NondetSite> = vec![];
        let mut stack: Vec<*const HydroNode> = starts.to_vec();
        let found =
            |sites: &mut Vec<NondetSite>, node: &HydroNode, op: &'static str, opaque: bool| {
                let location = node.metadata().location_id.root().clone();
                let site = NondetSite {
                    site: site_of(node),
                    op,
                    kind: kind_name(&node.metadata().collection_kind),
                    local: &location == at,
                    location,
                    opaque,
                    send_order: false,
                    via: None,
                };
                if !sites.contains(&site) {
                    sites.push(site);
                }
            };
        while let Some(ptr) = stack.pop() {
            if !seen.insert(ptr) {
                continue;
            }
            // SAFETY: every pointer points into the IR `self` was built from, which does not
            // move or change while `self` is used.
            let node = unsafe { &*ptr };
            if let HydroNode::Placeholder = node {
                // No metadata to place it by: report it at the label's own location.
                sites.push(NondetSite {
                    site: "Placeholder".to_owned(),
                    op: "unknown_operator",
                    kind: "?",
                    location: at.clone(),
                    local: true,
                    opaque: true,
                    send_order: false,
                    via: None,
                });
                continue;
            }
            if let Some((op, opaque)) = nondet_op(node) {
                found(&mut sites, node, op, opaque);
            }
            stack.extend(shared_inputs(node));
            if let HydroNode::CycleSource { cycle_id, .. } = node {
                match self.cycle_sinks.get(cycle_id) {
                    Some(&sink) => stack.push(sink),
                    None => found(&mut sites, node, "unresolved_cycle", true),
                }
            }
            for closure in closures(node) {
                for capture in captures(closure) {
                    match capture {
                        Some((cell, _)) => {
                            stack.push(cell);
                            stack.extend(self.writers.get(&cell).into_iter().flatten().copied());
                        }
                        None => found(&mut sites, node, "unresolved_state", true),
                    }
                }
            }
        }
        Walk { sites, seen }
    }

    /// The sites that may reorder the sends of `location` (U2): those in the cone of everything
    /// at it, and an unresolved write there.
    fn location_sites(&self, location: &LocationId) -> Vec<NondetSite> {
        let starts = self.by_location.get(location).cloned().unwrap_or_default();
        let mut sites = self.walk(&starts, location).sites;
        if self.unresolved_writes.contains(location) {
            sites.push(NondetSite {
                site: "for_each".to_owned(),
                op: "unresolved_state",
                kind: "-",
                location: location.clone(),
                local: true,
                opaque: true,
                send_order: false,
                via: None,
            });
        }
        sites
    }

    /// The sites that may reorder the sends of `location` (U2), for [`Analysis::Calm`]: those
    /// in the cone of what it sends (the inputs of its networks), and an unresolved write there.
    /// A choice at the location that feeds none of its sends, directly or through state, cannot
    /// change their order.
    fn send_sites(&self, location: &LocationId) -> Vec<NondetSite> {
        let starts = self
            .sends_by_location
            .get(location)
            .cloned()
            .unwrap_or_default();
        let mut sites = self.walk(&starts, location).sites;
        if self.unresolved_writes.contains(location) {
            sites.push(NondetSite {
                site: "for_each".to_owned(),
                op: "unresolved_state",
                kind: "-",
                location: location.clone(),
                local: true,
                opaque: true,
                send_order: false,
                via: None,
            });
        }
        sites
    }
}

/// Which analysis decides whether a site in a label's cone makes it schedule-sensitive
/// (docs/design/hydrofrac-lite.md §4.9, "Which choices matter").
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Analysis {
    /// Every site in the label's cone (U1), and every site in the cone of anything at a location
    /// that sends a delivery in it (U2). The first version of the walk; kept as the oracle the
    /// sharper analysis must refine (it may only drop sites this one reports).
    Conservative,
    /// U1: a choice whose every path to the label is insensitive to the order and batching of
    /// what it carries is discharged (an order-insensitivity analysis in the spirit of CALM).
    /// U2: every site in the cone of what a location that sends a delivery in the cone sends.
    Calm,
}

/// How the path from a node to the label treats the node's output (for [`Analysis::Calm`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Mode {
    /// Only the set of records the node emits over the whole run reaches the label: every
    /// operator in between is insensitive to their order and to how they are split into ticks.
    Tolerant,
    /// Some operator in between (the reason, by index) observes order, batching or a
    /// snapshot of state.
    Strict(usize),
}

/// How an operator treats one of its inputs (for [`Analysis::Calm`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Edge {
    /// The set of the operator's output records over the run is a function of the set of the
    /// input's records over the run, and each output record exists as soon as the input
    /// records it is computed from exist (it is monotone): order and tick boundaries do not
    /// matter.
    SetPreserving,
    /// Anything else (a snapshot, a per-tick aggregate, a non-monotone or order-dependent
    /// operator, state, something unclassified).
    OrderSensitive,
}

/// The choices that only decide the order or the batching of records whose set they pass
/// through unchanged: under a [`Mode::Tolerant`] path they cannot change what the label holds.
/// Every other choice (a fold over an unordered input or a closure with a mutable reference,
/// which compute on the order; timers and spins, which decide how many records exist; garbage
/// collection by watermark) and everything opaque counts in any mode.
fn set_choice(op: &str) -> bool {
    matches!(
        op,
        "batch" | "begin_atomic" | "merge_ordered" | "observe_nondet" | "unordered_external_input"
    )
}

/// Whether a collection's contents are a set of records that only grows (a stream), as
/// opposed to a value that changes (a singleton, an optional).
fn stream_like(kind: &CollectionKind) -> bool {
    matches!(
        kind,
        CollectionKind::Stream { .. } | CollectionKind::KeyedStream { .. }
    )
}

/// Words that betray a closure with state of its own, or one that reads something other than
/// its argument (a cell, a global, the clock).
const IMPURE: &[&str] = &[
    "let mut",
    "static",
    "thread_local",
    "Cell",
    "unsafe",
    "Mutex",
    "Atomic",
    "Instant",
    "SystemTime",
    "rand",
];

/// Whether a closure is (taken to be) a function of its argument: it captures no state cell,
/// and its text has none of [`IMPURE`]. A heuristic: a closure literal that mutates a
/// variable it owns needs a `let mut` around it, which this catches; one that reaches state
/// some other way does not (docs §4.9).
fn pure_closure(f: &ClosureExpr) -> bool {
    if !f.singleton_refs().is_empty() {
        return false;
    }
    let text = f.expr().to_string();
    if std::env::var_os("HYDROFRAC_DUMP_CLOSURES").is_some() {
        eprintln!("CLOSURE\t{text}");
    }
    !IMPURE.iter().any(|w| text.contains(w))
}

/// Follows `Tee`s to the node they share.
fn strip(mut ptr: *const HydroNode) -> *const HydroNode {
    loop {
        // SAFETY: `ptr` points into the IR the walker was built from (see `Cones`).
        match unsafe { &*ptr } {
            HydroNode::Tee { inner, .. } => ptr = std::ptr::from_ref(&*inner.0.borrow()),
            _ => return ptr,
        }
    }
}

/// One [`Analysis::Calm`] walk's findings.
struct CalmWalk {
    /// Sites that count: reached on an order-sensitive path, or harmful in any mode.
    harmful: Vec<NondetSite>,
    /// Choices reached only on order-insensitive paths.
    harmless: Vec<NondetSite>,
    /// Every node reached.
    seen: HashSet<*const HydroNode>,
}

impl Cones {
    /// Whether `node` is `filter_not_in` against its own history: a `Difference` whose `neg` is
    /// a state that starts with some initial contents and then accumulates the `Difference`'s
    /// own output (`seen = seen.chain(fresh)` with `fresh = arrived.filter_not_in(seen)`, the
    /// idiom of `use::state`). Its output over the run is then every record of `pos` not in the
    /// initial contents, whatever ticks they arrive in. Returns the initial contents' node.
    fn self_difference(&self, node: &HydroNode) -> Option<*const HydroNode> {
        let HydroNode::Difference { neg, .. } = node else {
            return None;
        };
        let me = std::ptr::from_ref(node);
        let state = strip(std::ptr::from_ref(&**neg));
        // SAFETY: as in `strip`.
        let HydroNode::Chain { first, second, .. } = (unsafe { &*state }) else {
            return None;
        };
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
        let (cycle, init) = if let Some(c) = carried(first) {
            (c, std::ptr::from_ref(&**second))
        } else {
            (carried(second)?, std::ptr::from_ref(&**first))
        };
        let sink = strip(*self.cycle_sinks.get(&cycle)?);
        // SAFETY: as in `strip`.
        let HydroNode::Chain { first, second, .. } = (unsafe { &*sink }) else {
            return None;
        };
        let parts = (
            strip(std::ptr::from_ref(&**first)),
            strip(std::ptr::from_ref(&**second)),
        );
        (parts == (state, me) || parts == (me, state)).then_some(init)
    }

    /// The inputs of `node` and how it treats each (see [`Edge`]); not its cycle's sink, nor
    /// the state its closures capture, which the walk adds itself.
    fn edges(&self, node: &HydroNode) -> Vec<(*const HydroNode, Edge)> {
        use Edge::{OrderSensitive, SetPreserving};
        let all = |edge: Edge| -> Vec<(*const HydroNode, Edge)> {
            shared_inputs(node).into_iter().map(|i| (i, edge)).collect()
        };
        let top = |n: &HydroNode| n.metadata().location_id.is_top_level();
        match node {
            HydroNode::Tee { .. }
            | HydroNode::Reference { .. }
            | HydroNode::PartitionSide { .. } => all(SetPreserving),
            HydroNode::Map { f, .. }
            | HydroNode::FlatMap { f, .. }
            | HydroNode::Filter { f, .. }
            | HydroNode::FilterMap { f, .. }
            | HydroNode::Inspect { f, .. }
            | HydroNode::PartitionShared { f, .. } => all(if pure_closure(f) {
                SetPreserving
            } else {
                OrderSensitive
            }),
            // Order, boundedness or retries change; the records do not. A network carries a
            // set of content-addressed messages; a batch splits its input into ticks; a union,
            // a dedup or a sort within a tick keeps the set; a deferral moves it a tick later.
            HydroNode::Cast { .. }
            | HydroNode::ObserveNonDet { .. }
            | HydroNode::BeginAtomic { .. }
            | HydroNode::EndAtomic { .. }
            | HydroNode::Batch { .. }
            | HydroNode::YieldConcat { .. }
            | HydroNode::Chain { .. }
            | HydroNode::MergeOrdered { .. }
            | HydroNode::Unique { .. }
            | HydroNode::Sort { .. }
            | HydroNode::DeferTick { .. }
            | HydroNode::Network { .. }
            | HydroNode::AssertIsConsistent { .. } => all(SetPreserving),
            // Both sides persisted across ticks (outside any tick): every pair is produced
            // once both of its records have arrived, in any order. Inside a tick, a record
            // meets only what the other side holds in the same tick.
            HydroNode::Join { left, right, .. } | HydroNode::CrossProduct { left, right, .. } => {
                all(if top(left) && top(right) {
                    SetPreserving
                } else {
                    OrderSensitive
                })
            }
            // The probe side streams against a bounded build side: the build side is read
            // whole, as of when the probe runs.
            HydroNode::JoinHalf { left, right, .. } => vec![
                (
                    std::ptr::from_ref(&**left),
                    if top(left) && top(right) {
                        SetPreserving
                    } else {
                        OrderSensitive
                    },
                ),
                (std::ptr::from_ref(&**right), OrderSensitive),
            ],
            HydroNode::Difference { pos, .. } => match self.self_difference(node) {
                Some(init) => vec![
                    (std::ptr::from_ref(&**pos), SetPreserving),
                    (init, OrderSensitive),
                ],
                None => all(OrderSensitive),
            },
            // Snapshots (`cross_singleton`, so `filter_if`), anti-joins, `first`, positions,
            // aggregates (`fold`, `count`, `reduce`, `scan`), watermarks, futures, async streams,
            // multi-version networks, singletons made unbounded, counters, sources.
            HydroNode::Placeholder
            | HydroNode::Source { .. }
            | HydroNode::SingletonSource { .. }
            | HydroNode::CycleSource { .. }
            | HydroNode::ChainFirst { .. }
            | HydroNode::CrossSingleton { .. }
            | HydroNode::AntiJoin { .. }
            | HydroNode::ResolveFutures { .. }
            | HydroNode::ResolveFuturesBlocking { .. }
            | HydroNode::ResolveFuturesOrdered { .. }
            | HydroNode::FlatMapStreamBlocking { .. }
            | HydroNode::Enumerate { .. }
            | HydroNode::Fold { .. }
            | HydroNode::Scan { .. }
            | HydroNode::ScanAsyncBlocking { .. }
            | HydroNode::FoldKeyed { .. }
            | HydroNode::Reduce { .. }
            | HydroNode::ReduceKeyed { .. }
            | HydroNode::ReduceKeyedWatermark { .. }
            | HydroNode::VersionedNetworkFork { .. }
            | HydroNode::VersionedNetwork { .. }
            | HydroNode::ExternalInput { .. }
            | HydroNode::Counter { .. }
            | HydroNode::UnboundSingleton { .. } => all(OrderSensitive),
        }
    }

    /// The [`Analysis::Calm`] walk of the cone of `starts`, labelled relative to the location
    /// `at`. `tolerant` says whether the label itself is only its set of records (a stream
    /// outside any tick), else the walk starts strict.
    fn walk_calm(&self, starts: &[*const HydroNode], at: &LocationId, tolerant: bool) -> CalmWalk {
        let mut reasons: Vec<String> = vec!["the label (not a stream outside a tick)".to_owned()];
        let start = if tolerant {
            Mode::Tolerant
        } else {
            Mode::Strict(0)
        };
        let mut stack: Vec<(*const HydroNode, Mode)> = starts.iter().map(|&p| (p, start)).collect();
        let mut tolerant_seen: HashSet<*const HydroNode> = HashSet::new();
        let mut strict_seen: HashSet<*const HydroNode> = HashSet::new();
        let mut walk = CalmWalk {
            harmful: vec![],
            harmless: vec![],
            seen: HashSet::new(),
        };
        let same = |a: &NondetSite, b: &NondetSite| {
            a.site == b.site && a.op == b.op && a.location == b.location
        };
        let found = |list: &mut Vec<NondetSite>,
                     node: &HydroNode,
                     op: &'static str,
                     opaque: bool,
                     via: Option<String>| {
            let location = node.metadata().location_id.root().clone();
            let site = NondetSite {
                site: site_of(node),
                op,
                kind: kind_name(&node.metadata().collection_kind),
                local: &location == at,
                location,
                opaque,
                send_order: false,
                via,
            };
            if !list.iter().any(|s| same(s, &site)) {
                list.push(site);
            }
        };
        while let Some((ptr, mode)) = stack.pop() {
            if strict_seen.contains(&ptr) {
                continue;
            }
            match mode {
                Mode::Tolerant => {
                    if !tolerant_seen.insert(ptr) {
                        continue;
                    }
                }
                Mode::Strict(_) => {
                    strict_seen.insert(ptr);
                }
            }
            walk.seen.insert(ptr);
            let via = |reasons: &[String]| match mode {
                Mode::Tolerant => None,
                Mode::Strict(r) => Some(reasons[r].clone()),
            };
            // SAFETY: as in `Cones::walk`.
            let node = unsafe { &*ptr };
            if let HydroNode::Placeholder = node {
                walk.harmful.push(NondetSite {
                    site: "Placeholder".to_owned(),
                    op: "unknown_operator",
                    kind: "?",
                    location: at.clone(),
                    local: true,
                    opaque: true,
                    send_order: false,
                    via: None,
                });
                continue;
            }
            if let Some((op, opaque)) = nondet_op(node) {
                if mode == Mode::Tolerant && set_choice(op) {
                    found(&mut walk.harmless, node, op, opaque, None);
                } else {
                    found(&mut walk.harmful, node, op, opaque, via(&reasons));
                }
            }
            // The mode of an input reached through an edge of this node.
            let here = |reasons: &mut Vec<String>, what: &str| -> usize {
                reasons.push(format!("{what} at {}", site_of(node)));
                reasons.len() - 1
            };
            let next = |reasons: &mut Vec<String>, child: *const HydroNode, edge: Edge| -> Mode {
                // SAFETY: as above.
                let child_node = unsafe { &*child };
                let child_stream = !matches!(child_node, HydroNode::Placeholder)
                    && stream_like(&child_node.metadata().collection_kind);
                match (mode, edge) {
                    (Mode::Tolerant, Edge::SetPreserving) if child_stream => Mode::Tolerant,
                    (Mode::Tolerant, Edge::SetPreserving) => Mode::Strict(here(
                        reasons,
                        &format!(
                            "{} of a {}",
                            op_name(node),
                            kind_name(&child_node.metadata().collection_kind)
                        ),
                    )),
                    (_, Edge::OrderSensitive) => Mode::Strict(here(reasons, &op_name(node))),
                    (Mode::Strict(r), Edge::SetPreserving) => Mode::Strict(r),
                }
            };
            for (child, edge) in self.edges(node) {
                let m = next(&mut reasons, child, edge);
                stack.push((child, m));
            }
            if let HydroNode::CycleSource { cycle_id, .. } = node {
                match self.cycle_sinks.get(cycle_id) {
                    Some(&sink) => {
                        let m = next(&mut reasons, sink, Edge::SetPreserving);
                        stack.push((sink, m));
                    }
                    None => found(&mut walk.harmful, node, "unresolved_cycle", true, None),
                }
            }
            for closure in closures(node) {
                for capture in captures(closure) {
                    match capture {
                        Some((cell, _)) => {
                            let r = here(&mut reasons, &format!("{} reading state", op_name(node)));
                            stack.push((cell, Mode::Strict(r)));
                            for &writer in self.writers.get(&cell).into_iter().flatten() {
                                stack.push((writer, Mode::Strict(r)));
                            }
                        }
                        None => found(&mut walk.harmful, node, "unresolved_state", true, None),
                    }
                }
            }
        }
        walk.harmless
            .retain(|h| !walk.harmful.iter().any(|s| same(s, h)));
        walk
    }
}

/// For every collection `ir` labels `EventualConsistency` (as [`ec_collections`] lists them),
/// its causal cone and the sites that make its search schedule-sensitive, by
/// [`Analysis::Calm`] (see the [module docs](self)).
pub fn label_cones(ir: &[HydroRoot]) -> Vec<LabelCone> {
    cones_of(ir, ec_collections(ir))
}

/// [`label_cones`] by [`Analysis::Conservative`]: the oracle [`Analysis::Calm`] refines.
pub fn label_cones_conservative(ir: &[HydroRoot]) -> Vec<LabelCone> {
    cones_with(ir, ec_collections(ir), Analysis::Conservative)
}

/// [`label_cones`] for `collections`, which must be [`ec_collections`] of `ir`.
pub fn cones_of(ir: &[HydroRoot], collections: Vec<EcCollection>) -> Vec<LabelCone> {
    cones_with(ir, collections, Analysis::Calm)
}

/// The cones of `collections` (which must be [`ec_collections`] of `ir`) by `analysis`.
pub fn cones_with(
    ir: &[HydroRoot],
    collections: Vec<EcCollection>,
    analysis: Analysis,
) -> Vec<LabelCone> {
    let cones = Cones::new(ir);
    let mut by_location: HashMap<LocationId, Vec<NondetSite>> = HashMap::new();
    collections
        .into_iter()
        .map(|collection| {
            let key = (
                collection.label.clone(),
                collection.op.clone(),
                kind_name(&collection.collection_kind),
                collection.location.clone(),
            );
            let starts = cones.labelled.get(&key).cloned().unwrap_or_default();
            let (mut sites, harmless, seen) = if starts.is_empty() {
                // The labelled node cannot be found again: the walk cannot see its cone.
                (
                    vec![NondetSite {
                        site: collection.label.clone(),
                        op: "unlocated_label",
                        kind: kind_name(&collection.collection_kind),
                        location: collection.location.clone(),
                        local: true,
                        opaque: true,
                        send_order: false,
                        via: None,
                    }],
                    vec![],
                    HashSet::new(),
                )
            } else {
                match analysis {
                    Analysis::Conservative => {
                        let Walk { sites, seen } = cones.walk(&starts, &collection.location);
                        (sites, vec![], seen)
                    }
                    Analysis::Calm => {
                        // SAFETY: as in `Cones::walk`.
                        let label = unsafe { &*starts[0] };
                        let tolerant = stream_like(&collection.collection_kind)
                            && label.metadata().location_id.is_top_level();
                        let CalmWalk {
                            harmful,
                            harmless,
                            seen,
                        } = cones.walk_calm(&starts, &collection.location, tolerant);
                        (harmful, harmless, seen)
                    }
                }
            };
            // U2: a site that may reorder the sends of a node whose deliveries are in the cone
            // (the sender of each network in it), and so move its positional crash points.
            let locations: Vec<LocationId> = {
                let mut locations: Vec<LocationId> = vec![];
                for &ptr in &seen {
                    // SAFETY: as in `Cones::walk`.
                    let node = unsafe { &*ptr };
                    let HydroNode::Network { input, .. } = node else {
                        continue;
                    };
                    let location = input.metadata().location_id.root().clone();
                    if !locations.contains(&location) {
                        locations.push(location);
                    }
                }
                locations
            };
            for location in locations {
                let found = by_location
                    .entry(location.clone())
                    .or_insert_with(|| match analysis {
                        Analysis::Conservative => cones.location_sites(&location),
                        Analysis::Calm => cones.send_sites(&location),
                    });
                for site in found.iter() {
                    let mut site = site.clone();
                    site.local = site.location == collection.location;
                    if sites.iter().any(|s| {
                        s.site == site.site && s.op == site.op && s.location == site.location
                    }) {
                        continue;
                    }
                    site.send_order = true;
                    sites.push(site);
                }
            }
            let harmless = harmless
                .into_iter()
                .filter(|h| {
                    !sites
                        .iter()
                        .any(|s| s.site == h.site && s.op == h.op && s.location == h.location)
                })
                .collect();
            LabelCone {
                collection,
                sites,
                harmless,
                cone_nodes: seen.len(),
            }
        })
        .collect()
}

/// A table of [`label_cones`] for a report: one line per label (tab-separated: program,
/// label, operator, kind, sensitive?, sensitive through its own cone (U1)?, summary, sites,
/// discharged choices).
pub fn table(program: &str, cones: &[LabelCone]) -> String {
    let yes_no = |b: bool| if b { "yes" } else { "no" };
    let list = |sites: &[NondetSite]| {
        sites
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("; ")
    };
    let mut out = String::new();
    for cone in cones {
        let c = &cone.collection;
        out.push_str(&format!(
            "{program}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            c.display_name(),
            c.op,
            kind_name(&c.collection_kind),
            yes_no(cone.schedule_sensitive()),
            yes_no(cone.existence_sensitive()),
            cone.summary(),
            list(&cone.sites),
            list(&cone.harmless),
        ));
    }
    out
}
