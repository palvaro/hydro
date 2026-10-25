//! Every collection a program labels `EventualConsistency`, found on the IR before the taps
//! (the research branch's `ec_collections`).

use std::collections::{HashMap, HashSet};

use hydro_lang::compile::builder::CycleId;
use hydro_lang::compile::ir::{ClosureExpr, CollectionKind, HydroNode, HydroRoot, HydroSource};
use hydro_lang::location::dynamic::{ClusterConsistency, LocationId};
use quote::ToTokens;

use super::source_names;

/// A collection the program labels `EventualConsistency`, enumerated by [`ec_collections`].
#[derive(Clone, Debug)]
pub struct EcCollection {
    /// The node's tag, else the `file:line` of the user's labelling call ([`ec_label`]).
    pub label: String,
    /// The operator (`Network`, `Unique`, ...), to tell apart labelled nodes at one line.
    pub op: String,
    /// The cluster the collection lives on.
    pub location: LocationId,
    /// Whether a network channel feeds it (transitively): only then can a fault withhold it. A
    /// collection fed by none is still checked (its members can disagree without faults).
    pub fed_by_network: bool,
    /// The kind of the collection, as the enumerated IR typed it.
    pub collection_kind: CollectionKind,
    /// Whether the node lives inside a tick (or an atomic region).
    pub in_tick: bool,
    /// For a labelled scalar aggregated (`fold`, `reduce`, keyed or not) from a labelled
    /// stream: that stream's index. The scalar is a deterministic function of the stream, so
    /// the stream is what is checked (§3, `covered_by`).
    pub covered_by: Option<usize>,
    /// A name for reports: the node's tag, else the name read back from the source
    /// ([`super::source_names::site_name`]).
    pub name: Option<String>,
    /// The file name of the user's labelling call.
    pub file: Option<String>,
    /// The 0-based position among collections with the same file and display name.
    pub occurrence: usize,
    /// The address of the labelled node in the enumerated IR: its identity. Two nodes are two
    /// collections, even with one [`EcKey`] (a helper that labels on one line, called twice).
    pub(crate) node: usize,
}

impl EcCollection {
    /// The name reports use: [`Self::name`], else the operator.
    pub fn display_name(&self) -> &str {
        self.name.as_deref().unwrap_or(&self.op)
    }
}

/// `(label, operator, collection kind, cluster)`: what a labelled node's report names come
/// from. Not its identity: two nodes with one key are two collections ([`ec_collections`]). The kind separates a `broadcast_closed` from an
/// `assert_has_consistency_of` on a `fold` of it, which share a line and an operator.
pub(crate) type EcKey = (String, String, &'static str, LocationId);

/// The name of a collection kind (`Stream`, `Singleton`, ...).
pub fn kind_name(kind: &CollectionKind) -> &'static str {
    match kind {
        CollectionKind::Stream { .. } => "Stream",
        CollectionKind::Singleton { .. } => "Singleton",
        CollectionKind::Optional { .. } => "Optional",
        CollectionKind::KeyedStream { .. } => "KeyedStream",
        CollectionKind::KeyedSingleton { .. } => "KeyedSingleton",
    }
}

/// Every collection `ir` labels `EventualConsistency`: each labelled node where the label is
/// **minted** (no input is labelled) or where the collection **exits** the labelled region (an
/// unlabelled consumer other than a send, or a program output). The operators in between
/// inherit the label. Each labelled node is one collection, in first-seen order, even if another
/// node has the same [`EcKey`] (a library function instantiated twice, a helper labelling on one
/// line): merging them would let one collection's facts mask another's missing ones. A `Tee`
/// stands for what it shares.
///
/// A labelled scalar whose node is, through pass-throughs, an aggregation of a labelled
/// top-level stream is covered by that stream, which is listed if it is not already
/// ([`EcCollection::covered_by`]). A labelled collection computed from nothing but a cluster's
/// membership (the member-id list of a cluster-to-cluster `broadcast_closed`) is not listed.
pub fn ec_collections(ir: &[HydroRoot]) -> Vec<EcCollection> {
    // Raw pointers: cycle sinks are followed from `CycleSource`s to find network ancestors;
    // nothing in `ir` moves during this walk.
    let cycle_sinks: HashMap<CycleId, *const HydroNode> = ir
        .iter()
        .filter_map(|root| match root {
            HydroRoot::CycleSink {
                cycle_id, input, ..
            } => Some((*cycle_id, std::ptr::from_ref(&**input))),
            _ => None,
        })
        .collect();
    struct Found {
        by_node: HashMap<*const HydroNode, usize>,
        collections: Vec<EcCollection>,
        seen: HashSet<*const HydroNode>,
        /// The nodes recorded under each collection.
        nodes: HashMap<*const HydroNode, usize>,
        /// The keys of `nodes`, in first-recorded order.
        node_order: Vec<*const HydroNode>,
        /// A labelled scalar's node, and the node of the stream it aggregates.
        aggregations: Vec<(*const HydroNode, *const HydroNode)>,
        /// Each tick's entry points ([`tick_entries`]).
        entries: HashMap<LocationId, Vec<*const HydroNode>>,
    }
    impl Found {
        fn record(
            &mut self,
            node: &HydroNode,
            cycle_sinks: &HashMap<CycleId, *const HydroNode>,
        ) -> *const HydroNode {
            with_shared_resolved(node, |node| {
                let key = ec_key(node);
                let fed = sources(node, cycle_sinks, &self.entries).network;
                let ptr = std::ptr::from_ref(node);
                let index = *self.by_node.entry(ptr).or_insert_with(|| {
                    self.collections.push(EcCollection {
                        label: key.0,
                        op: key.1,
                        location: key.3,
                        fed_by_network: false,
                        collection_kind: node.metadata().collection_kind.clone(),
                        in_tick: !matches!(node.metadata().location_id, LocationId::Cluster(_)),
                        covered_by: None,
                        name: ec_name(node),
                        file: ec_file(node),
                        occurrence: 0,
                        node: ptr as usize,
                    });
                    self.collections.len() - 1
                });
                self.collections[index].fed_by_network |= fed;
                if self.nodes.insert(ptr, index).is_none() {
                    self.node_order.push(ptr);
                }
                ptr
            })
        }

        /// Records the stream the labelled scalar at `scalar` aggregates, if any.
        fn aggregation(
            &mut self,
            scalar: *const HydroNode,
            cycle_sinks: &HashMap<CycleId, *const HydroNode>,
        ) {
            // SAFETY: `scalar` points into `ir` (or a `Tee`'s cell), which does not move.
            if let Some(input) = aggregated_input(unsafe { &*scalar }) {
                let input = self.record(input, cycle_sinks);
                self.aggregations.push((scalar, input));
            }
        }
    }
    fn visit(
        node: &HydroNode,
        found: &mut Found,
        cycle_sinks: &HashMap<CycleId, *const HydroNode>,
    ) {
        if !found.seen.insert(std::ptr::from_ref(node)) {
            return;
        }
        let mut edge = |input: &HydroNode| {
            visit(input, found, cycle_sinks);
            // A labelled input of an unlabelled consumer: the collection exits here. A send
            // is not an exit: the collection continues at another location, labelled or not
            // by the transport's own rule.
            if is_ec(input) && !is_ec(node) && !matches!(node, HydroNode::Network { .. }) {
                with_casts_resolved(input, |input| found.record(input, cycle_sinks));
            }
        };
        match node {
            HydroNode::Tee { inner, .. }
            | HydroNode::Reference { inner, .. }
            | HydroNode::PartitionSide { inner, .. } => edge(&inner.0.borrow()),
            _ => {
                for input in node.input() {
                    edge(input);
                }
            }
        }
        if is_ec_minting(node) && !sources(node, cycle_sinks, &found.entries).membership_only() {
            let scalar = found.record(node, cycle_sinks);
            found.aggregation(scalar, cycle_sinks);
        }
    }
    let mut found = Found {
        by_node: HashMap::new(),
        collections: vec![],
        seen: HashSet::new(),
        nodes: HashMap::new(),
        node_order: vec![],
        aggregations: vec![],
        entries: tick_entries(ir),
    };
    for root in ir {
        let input = root.input();
        visit(input, &mut found, &cycle_sinks);
        // An output of the program (not a cycle, which continues inside it).
        if is_ec(input)
            && matches!(
                root,
                HydroRoot::SendExternal { .. }
                    | HydroRoot::ForEach { .. }
                    | HydroRoot::DestSink { .. }
                    | HydroRoot::EmbeddedOutput { .. }
            )
        {
            with_casts_resolved(input, |input| found.record(input, &cycle_sinks));
        }
    }

    // A labelled scalar that is not minted (its aggregation is labelled too) is recorded only
    // if it exits; look for its aggregation the same way.
    let scalars: Vec<*const HydroNode> = found
        .node_order
        .iter()
        .copied()
        .filter(|ptr| !found.aggregations.iter().any(|(s, _)| s == ptr))
        .collect();
    for scalar in scalars {
        found.aggregation(scalar, &cycle_sinks);
    }
    for (scalar, input) in &found.aggregations {
        let (scalar, input) = (found.nodes[scalar], found.nodes[input]);
        if scalar != input {
            found.collections[scalar].covered_by.get_or_insert(input);
        }
    }

    let mut collections = found.collections;
    let mut seen: HashMap<(Option<String>, String), usize> = HashMap::new();
    for c in &mut collections {
        let count = seen
            .entry((c.file.clone(), c.display_name().to_owned()))
            .or_default();
        c.occurrence = *count;
        *count += 1;
    }
    collections
}

/// The input stream a labelled scalar aggregates: `node`, through pass-throughs
/// (`AssertIsConsistent`, `Cast`, `ObserveNonDet`, `Tee`, references), is a `Fold`, `Reduce`,
/// `FoldKeyed` or `ReduceKeyed` whose input, through the same, is a labelled top-level stream.
fn aggregated_input(node: &HydroNode) -> Option<&HydroNode> {
    if !is_ec(node)
        || !matches!(
            node.metadata().collection_kind,
            CollectionKind::Singleton { .. }
                | CollectionKind::Optional { .. }
                | CollectionKind::KeyedSingleton { .. }
        )
    {
        return None;
    }
    /// Through the pass-throughs, as raw pointers: the cells' borrows end, the nodes stay.
    fn through(node: &HydroNode) -> *const HydroNode {
        match node {
            HydroNode::AssertIsConsistent { inner, .. }
            | HydroNode::Cast { inner, .. }
            | HydroNode::ObserveNonDet { inner, .. } => through(inner),
            HydroNode::Tee { inner, .. } | HydroNode::Reference { inner, .. } => {
                through(&inner.0.borrow())
            }
            _ => std::ptr::from_ref(node),
        }
    }
    // SAFETY: the pointers point into the IR, which outlives this call and does not move.
    let aggregation = unsafe { &*through(node) };
    let (HydroNode::Fold { input, .. }
    | HydroNode::Reduce { input, .. }
    | HydroNode::FoldKeyed { input, .. }
    | HydroNode::ReduceKeyed { input, .. }) = aggregation
    else {
        return None;
    };
    // SAFETY: as above.
    let input = unsafe { &*through(input) };
    (is_ec(input)
        && matches!(input.metadata().location_id, LocationId::Cluster(_))
        && matches!(
            input.metadata().collection_kind,
            CollectionKind::Stream { .. } | CollectionKind::KeyedStream { .. }
        ))
    .then_some(input)
}

/// Calls `f` on the first node under `node` that is not a labelled pass-through (a `Cast` of a
/// labelled input, or what a `Tee` shares): the operator an exit stands for.
fn with_casts_resolved<R>(node: &HydroNode, f: impl FnOnce(&HydroNode) -> R) -> R {
    match node {
        HydroNode::Tee { inner, .. } | HydroNode::Reference { inner, .. } => {
            with_casts_resolved(&inner.0.borrow(), f)
        }
        HydroNode::Cast { inner, .. } if is_ec(inner) => with_casts_resolved(inner, f),
        _ => f(node),
    }
}

/// Calls `f` on the node a chain of `Tee`s (or references) shares, or on `node` itself.
fn with_shared_resolved<R>(node: &HydroNode, f: impl FnOnce(&HydroNode) -> R) -> R {
    match node {
        HydroNode::Tee { inner, .. } | HydroNode::Reference { inner, .. } => {
            with_shared_resolved(&inner.0.borrow(), f)
        }
        // A partition side is the operator: its `PartitionShared` cannot be tapped whole.
        _ => f(node),
    }
}

/// Whether `node` carries the `EventualConsistency` label on a cluster.
pub(crate) fn is_ec(node: &HydroNode) -> bool {
    matches!(
        node.metadata().consistency,
        Some(ClusterConsistency::EventualConsistency)
    ) && matches!(node.metadata().location_id.root(), LocationId::Cluster(_))
}

/// Whether `node` mints an `EventualConsistency` label: labelled, and nothing that feeds it is
/// (a `CycleSource`, fed by its sink, never does).
pub(crate) fn is_ec_minting(node: &HydroNode) -> bool {
    if !is_ec(node) {
        return false;
    }
    match node {
        HydroNode::CycleSource { .. } => false,
        HydroNode::Tee { inner, .. }
        | HydroNode::Reference { inner, .. }
        | HydroNode::PartitionSide { inner, .. } => !is_ec(&inner.0.borrow()),
        _ => !node.input().into_iter().any(is_ec),
    }
}

/// The label of a labelled collection: the node's tag, else the user's `file:line` of the
/// labelling operator (the file by its name), else the operator's name.
pub(crate) fn ec_label(node: &HydroNode) -> String {
    let backtrace = &node.metadata().op.backtrace;
    node.metadata()
        .tag
        .clone()
        .or_else(|| {
            let span = source_names::user_frame(backtrace)
                .map(|frame| format!("{}:{}", frame.file, frame.line))
                .or_else(|| backtrace.format_span())?;
            Some(file_name(&span).to_owned())
        })
        .unwrap_or_else(|| ec_op(node))
}

/// The last component of a path.
fn file_name(path: &str) -> &str {
    path.rsplit(['/', '\\']).next().unwrap_or(path)
}

/// The report name of a labelled collection (see [`EcCollection::name`]).
fn ec_name(node: &HydroNode) -> Option<String> {
    node.metadata().tag.clone().or_else(|| {
        source_names::user_frame(&node.metadata().op.backtrace)
            .and_then(|frame| source_names::site_name(&frame))
    })
}

/// The file name of the user's call that built `node`.
fn ec_file(node: &HydroNode) -> Option<String> {
    let frame = source_names::user_frame(&node.metadata().op.backtrace)?;
    Some(file_name(&frame.file).to_owned())
}

/// The operator name of `node` (`Network`, `Unique`, ...).
fn ec_op(node: &HydroNode) -> String {
    let name = node.print_root();
    name.split('(').next().unwrap_or(&name).to_owned()
}

/// The identity of a labelled node (see [`EcKey`]).
pub(crate) fn ec_key(node: &HydroNode) -> EcKey {
    (
        ec_label(node),
        ec_op(node),
        kind_name(&node.metadata().collection_kind),
        node.metadata().location_id.root().clone(),
    )
}

/// What `node` is computed from, following `Tee`s, cycles (through their sinks) and the
/// singletons its closures read.
#[derive(Default)]
struct Sources {
    /// A network receive is upstream.
    network: bool,
    /// A cluster's membership is a source (`ClusterMembers`, or `source_iter` over `ClusterIds`).
    membership: bool,
    /// Some other leaf is.
    other: bool,
}

impl Sources {
    /// Only membership: the member-id list a cluster-to-cluster `broadcast_closed` labels,
    /// which would otherwise show up as a local duplicate row of the broadcast's name (§6.3).
    fn membership_only(&self) -> bool {
        self.membership && !self.network && !self.other
    }
}

/// The tick a location belongs to: itself for a tick, the enclosing tick for an atomic region,
/// `None` at top level.
pub(crate) fn tick_of(location: &LocationId) -> Option<&LocationId> {
    match location {
        LocationId::Tick(..) => Some(location),
        LocationId::Atomic(tick) => tick_of(tick),
        LocationId::Process(_) | LocationId::Cluster(_) => None,
    }
}

/// Each tick's entry points: the `batch`es and `atomic`s whose records enter it. The simulator
/// runs a tick only when one of them has something to release, so whether a tick runs at all
/// (and so whether its constants and cycles emit anything) depends on what feeds them.
pub(crate) fn tick_entries(ir: &[HydroRoot]) -> HashMap<LocationId, Vec<*const HydroNode>> {
    let (mut seen, mut out) = (HashSet::new(), HashMap::<LocationId, Vec<_>>::new());
    let mut stack: Vec<*const HydroNode> = ir.iter().map(|r| std::ptr::from_ref(r.input())).collect();
    while let Some(ptr) = stack.pop() {
        if !seen.insert(ptr) {
            continue;
        }
        // SAFETY: every pointer points into `ir`, which is borrowed for the whole walk.
        let node = unsafe { &*ptr };
        match node {
            HydroNode::Placeholder => continue,
            HydroNode::Batch { metadata, .. } | HydroNode::BeginAtomic { metadata, .. } => {
                if let Some(tick) = tick_of(&metadata.location_id) {
                    out.entry(tick.clone()).or_default().push(ptr);
                }
            }
            _ => {}
        }
        match node {
            HydroNode::Tee { inner, .. }
            | HydroNode::Reference { inner, .. }
            | HydroNode::PartitionSide { inner, .. } => {
                stack.push(std::ptr::from_ref(&*inner.0.borrow()));
            }
            _ => stack.extend(
                node.input()
                    .into_iter()
                    .chain(closure_refs(node))
                    .map(std::ptr::from_ref),
            ),
        }
    }
    out
}

/// What feeds `node`: networks, membership, other sources. A node inside a tick is also fed by
/// whatever feeds that tick's entry points ([`tick_entries`]): if no delivery reaches them, the
/// tick never runs.
fn sources(
    node: &HydroNode,
    cycle_sinks: &HashMap<CycleId, *const HydroNode>,
    entries: &HashMap<LocationId, Vec<*const HydroNode>>,
) -> Sources {
    fn go(
        node: &HydroNode,
        cycle_sinks: &HashMap<CycleId, *const HydroNode>,
        entries: &HashMap<LocationId, Vec<*const HydroNode>>,
        seen: &mut HashSet<*const HydroNode>,
        out: &mut Sources,
    ) {
        if !seen.insert(std::ptr::from_ref(node)) {
            return;
        }
        if !matches!(node, HydroNode::Placeholder)
            && let Some(tick) = tick_of(&node.metadata().location_id)
        {
            for &entry in entries.get(tick).into_iter().flatten() {
                // SAFETY: see the comment at the construction of `cycle_sinks`.
                go(unsafe { &*entry }, cycle_sinks, entries, seen, out);
            }
        }
        match node {
            HydroNode::Source {
                source: HydroSource::ClusterMembers(..),
                ..
            } => out.membership = true,
            // How `broadcast_closed` lists its destinations: `ClusterIds` splice as
            // `__hydro_lang_cluster_ids_<key>`.
            HydroNode::Source {
                source: HydroSource::Iter(expr),
                ..
            } if expr
                .to_token_stream()
                .to_string()
                .contains("__hydro_lang_cluster_ids_") =>
            {
                out.membership = true;
            }
            HydroNode::Network { .. } => out.network = true,
            HydroNode::Tee { inner, .. }
            | HydroNode::Reference { inner, .. }
            | HydroNode::PartitionSide { inner, .. } => {
                go(&inner.0.borrow(), cycle_sinks, entries, seen, out);
            }
            HydroNode::CycleSource { cycle_id, .. } => {
                if let Some(sink) = cycle_sinks.get(cycle_id) {
                    // SAFETY: see the comment at the construction of `cycle_sinks`.
                    go(unsafe { &**sink }, cycle_sinks, entries, seen, out);
                }
            }
            _ => {
                let inputs = node.input();
                out.other |= inputs.is_empty();
                for input in inputs.into_iter().chain(closure_refs(node)) {
                    go(input, cycle_sinks, entries, seen, out);
                }
            }
        }
    }
    let mut out = Sources::default();
    go(node, cycle_sinks, entries, &mut HashSet::new(), &mut out);
    out
}

/// The singleton references `node`'s closures read (not in [`HydroNode::input`]).
fn closure_refs(node: &HydroNode) -> impl Iterator<Item = &HydroNode> {
    let closures: Vec<&ClosureExpr> = match node {
        HydroNode::Map { f, .. }
        | HydroNode::FlatMap { f, .. }
        | HydroNode::FlatMapStreamBlocking { f, .. }
        | HydroNode::Filter { f, .. }
        | HydroNode::FilterMap { f, .. }
        | HydroNode::Inspect { f, .. }
        | HydroNode::PartitionShared { f, .. }
        | HydroNode::Reduce { f, .. }
        | HydroNode::ReduceKeyed { f, .. }
        | HydroNode::ReduceKeyedWatermark { f, .. } => vec![f],
        HydroNode::Fold { init, acc, .. }
        | HydroNode::Scan { init, acc, .. }
        | HydroNode::ScanAsyncBlocking { init, acc, .. }
        | HydroNode::FoldKeyed { init, acc, .. } => vec![init, acc],
        _ => vec![],
    };
    closures
        .into_iter()
        .flat_map(|f| f.singleton_refs().iter().map(|(node, _)| node))
}
