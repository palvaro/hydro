//! Every collection a program labels `EventualConsistency`, found on the IR before the taps
//! (ported from the research branch's `sim/slice.rs`, `ec_collections` and its helpers).

use std::collections::{HashMap, HashSet};

use hydro_lang::compile::builder::CycleId;
use hydro_lang::compile::ir::backtrace::Backtrace;
use hydro_lang::compile::ir::{ClosureExpr, CollectionKind, HydroNode, HydroRoot, HydroSource};
use hydro_lang::location::dynamic::{ClusterConsistency, LocationId};
use quote::ToTokens;

use super::source_names;

/// A collection the program labels `EventualConsistency`: a premise that the label's
/// convergence is not withheld by faults, which the generated specification over it
/// discharges ([`crate::spec`]) and which is otherwise open. Enumerated by [`ec_collections`]
/// on the IR before the taps ([`super::tap`]).
#[derive(Clone, Debug)]
pub struct EcCollection {
    /// The user's source location of the labelling operator, or its tag.
    pub label: String,
    /// The operator (`Network`, `Unique`, ...), to tell apart two labelled nodes at one source
    /// line.
    pub op: String,
    /// The cluster the collection lives on.
    pub location: LocationId,
    /// Whether a network channel feeds it (transitively): a fault can withhold its inputs.
    /// A labelled collection computed from local sources alone has nothing to check.
    pub fed_by_network: bool,
    /// The kind of the collection (its element types are those of the IR the enumeration ran
    /// on: plain before the provenance rewrite, tagged after).
    pub collection_kind: CollectionKind,
    /// Whether the node lives inside a tick (or an atomic region) rather than at the cluster's
    /// top level.
    pub in_tick: bool,
    /// The label is minted here: none of the node's inputs is labelled (a `broadcast_closed`
    /// over a transport that grants it, an `assert_has_consistency_of`). What the type system
    /// assumes about faults, it assumes here.
    pub minted: bool,
    /// The collection leaves the labelled region here: a consumer drops the label (a
    /// `weaken_consistency`, a `batch` into an unlabelled tick) or it is an output of the
    /// program. What the program delivers under the label, it delivers here. (A send to
    /// another location is not an exit: the collection continues there, labelled or not by the
    /// transport's own rule.)
    pub exit: bool,
    /// The collection is the input stream of a labelled scalar (see [`Self::covered_by`]).
    /// Usually it is also an exit (the aggregating operator itself is unlabelled); when it is
    /// not, this is why it is listed.
    pub aggregated: bool,
    /// For a labelled scalar (`Singleton`, `Optional`, `KeyedSingleton`) computed by an
    /// aggregation (`fold`, `reduce`, and their keyed forms) from a labelled stream: the index,
    /// in the enumeration, of that stream. The type system vouches that the scalar is a
    /// deterministic function of the stream (else the label would have been dropped), so the
    /// scalar converges if the stream's contents reach every correct member: the stream is
    /// what is checked.
    pub covered_by: Option<usize>,
    /// For a minted collection that is not itself an exit: the exits that a **merge**
    /// downstream of it reaches, nearest first, by index in the enumeration. Non-empty only
    /// when the collection is *merge-dominated* (see [`ec_collections`]): its contents are
    /// combined with another labelled stream before anything leaves the labelled region, so
    /// agreement is owed at the combination, which the exits check.
    pub merged_into: Vec<usize>,
    /// A name for reports: the node's tag, else the variable the user's statement binds, else
    /// the receiver of an unbound `<name>.assert_has_consistency_of(..)`, else the function
    /// whose tail expression it is, read back from the source (see
    /// [`super::source_names::site_name`]). `None` when none is available.
    pub name: Option<String>,
    /// The file name (no directories) of the user's call that labelled the collection, when
    /// the backtrace has one.
    pub file: Option<String>,
    /// Among the collections with the same [`Self::file`] and [`Self::display_name`], the
    /// 0-based position of this one in enumeration order (see [`Self::stable_key`]).
    pub occurrence: usize,
}

impl EcCollection {
    /// The location, as `Cluster(...)`, for reports.
    pub fn location_label(&self) -> String {
        format!("{:?}", self.location)
    }

    /// The name reports use: [`Self::name`], else the operator.
    pub fn display_name(&self) -> &str {
        self.name.as_deref().unwrap_or(&self.op)
    }

    /// An identity that survives edits elsewhere in the file: `(file, name, occurrence)`, with
    /// the operator standing in for a missing name. Line numbers move when the source changes;
    /// this key does not, so verdicts can be compared across versions of a program (the
    /// research records key by `file:line`, docs/design/hydrofrac-lite.md §7).
    pub fn stable_key(&self) -> (String, String, usize) {
        (
            self.file.clone().unwrap_or_default(),
            self.display_name().to_owned(),
            self.occurrence,
        )
    }

    /// The identity of the collection among the program's labelled nodes.
    pub(crate) fn key(&self) -> EcKey {
        (
            self.label.clone(),
            self.op.clone(),
            kind_name(&self.collection_kind),
            self.location.clone(),
        )
    }
}

/// `(label, operator, collection kind, cluster)`: how labelled nodes are told apart, and how the
/// tap pass re-identifies the nodes this enumeration found. The kind separates two labelled
/// nodes of one operator in one statement (a `broadcast_closed` and an
/// `assert_has_consistency_of` on a `fold` of it share `file:line` and `AssertIsConsistent`);
/// the research branch's key merged them.
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

/// Every collection `ir` labels `EventualConsistency`: every labelled node where the label is
/// **minted** (none of its inputs is labelled: the `broadcast_closed` or the
/// `assert_has_consistency_of`) and every labelled node where the collection **exits** the
/// labelled region (a consumer other than a send is unlabelled, or it is an output of the
/// program). The operators
/// in between inherit the label and are not listed. Nodes at the same source line, of the same
/// operator, on the same cluster (one per cluster member, or one per instantiation of a library
/// function on the same cluster) are reported once, in first-seen order; a search over such a
/// collection checks the union of their contents per member. A `Tee` stands for what it shares: it is neither a minting point
/// nor, by itself, an exit, and the shared node is reported under its own source line.
///
/// Two structural relations are recorded alongside, both resting on the same principle: the
/// type system keeps the label only on computations that are deterministic given their
/// inputs, so convergence is owed only where inputs are **combined**, and derived state need
/// not be checked on its own.
///
/// - **Scalars are covered by their input** ([`EcCollection::covered_by`]). A labelled
///   `Singleton` / `Optional` / `KeyedSingleton` whose labelling node is, through
///   pass-throughs (`assert_has_consistency_of`, casts, the `ObserveNonDet` of an
///   at-least-once input, `Tee`s), a `Fold`, `Reduce`, `FoldKeyed` or `ReduceKeyed` over a
///   labelled top-level stream is covered by that stream. The stream is listed (flagged
///   [`EcCollection::aggregated`]) if it is not already.
/// - **Merge-dominated hops** ([`EcCollection::merged_into`]). A minted collection that is not
///   an exit is merge-dominated when, following its consumers downstream through labelled
///   operators on its own cluster (a `Tee` to its branches, a `forward_ref`'s sink to its
///   `CycleSource`), **every** path reaches a merge (a `Chain`, `ChainFirst` or
///   `MergeOrdered` whose inputs are all labelled) before it reaches an exit or a send to
///   another location, and at least one path reaches an exit after a merge. The hop's
///   elements then reach the outside only combined with the other labelled inputs of the
///   merge (an echo, a retransmission), and the exits after the merge are where agreement on
///   the combination is checked. A hop that reaches an exit or a send unmerged is not
///   dominated: its own contents leave the region and its own verdict stands.
///
/// A labelled collection computed from nothing but a cluster's membership is not listed: it is
/// the member-id list a cluster-to-cluster `broadcast_closed` joins its payload with, not a
/// collection of the program (see `membership_only`).
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
        by_key: HashMap<EcKey, usize>,
        collections: Vec<EcCollection>,
        seen: HashSet<*const HydroNode>,
        /// The nodes recorded under each collection.
        nodes: HashMap<*const HydroNode, usize>,
        /// The keys of `nodes`, in first-recorded order.
        node_order: Vec<*const HydroNode>,
        /// The nodes recorded as exits (a collection's `exit` is the union over its nodes).
        exit_nodes: HashSet<*const HydroNode>,
        /// Labelled → labelled edges on one cluster, input to consumer.
        consumers: HashMap<*const HydroNode, Vec<*const HydroNode>>,
        /// Labelled nodes some send consumes.
        sent: HashSet<*const HydroNode>,
        /// Labelled merges.
        merges: HashSet<*const HydroNode>,
        /// A labelled scalar's node, and the node of the stream it aggregates.
        aggregations: Vec<(*const HydroNode, *const HydroNode)>,
    }
    impl Found {
        fn record(
            &mut self,
            node: &HydroNode,
            minted: bool,
            exit: bool,
            cycle_sinks: &HashMap<CycleId, *const HydroNode>,
        ) -> *const HydroNode {
            with_shared_resolved(node, |node| {
                let key = ec_key(node);
                let fed = has_network_ancestor(node, cycle_sinks);
                let index = *self.by_key.entry(key.clone()).or_insert_with(|| {
                    self.collections.push(EcCollection {
                        label: key.0,
                        op: key.1,
                        location: key.3,
                        fed_by_network: false,
                        collection_kind: node.metadata().collection_kind.clone(),
                        in_tick: !matches!(node.metadata().location_id, LocationId::Cluster(_)),
                        minted: false,
                        exit: false,
                        aggregated: false,
                        covered_by: None,
                        merged_into: vec![],
                        name: ec_name(node),
                        file: ec_file(node),
                        occurrence: 0,
                    });
                    self.collections.len() - 1
                });
                let found = &mut self.collections[index];
                found.fed_by_network |= fed;
                found.minted |= minted;
                found.exit |= exit;
                let ptr = std::ptr::from_ref(node);
                if self.nodes.insert(ptr, index).is_none() {
                    self.node_order.push(ptr);
                }
                if exit {
                    self.exit_nodes.insert(ptr);
                }
                ptr
            })
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
                with_casts_resolved(input, |input| found.record(input, false, true, cycle_sinks));
            }
            if is_ec(input) {
                if matches!(node, HydroNode::Network { .. }) {
                    found.sent.insert(std::ptr::from_ref(input));
                } else if is_ec(node) {
                    found
                        .consumers
                        .entry(std::ptr::from_ref(input))
                        .or_default()
                        .push(std::ptr::from_ref(node));
                }
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
        // A `forward_ref` continues the collection its sink receives.
        if let HydroNode::CycleSource { cycle_id, .. } = node
            && is_ec(node)
            && let Some(&sink) = cycle_sinks.get(cycle_id)
            // SAFETY: see the comment at the construction of `cycle_sinks`.
            && is_ec(unsafe { &*sink })
        {
            found
                .consumers
                .entry(sink)
                .or_default()
                .push(std::ptr::from_ref(node));
        }
        if is_ec(node)
            && matches!(
                node,
                HydroNode::Chain { .. }
                    | HydroNode::ChainFirst { .. }
                    | HydroNode::MergeOrdered { .. }
            )
            && node.input().into_iter().all(is_ec)
        {
            found.merges.insert(std::ptr::from_ref(node));
        }
        if is_ec_minting(node) && !membership_only(node, cycle_sinks) {
            let scalar = found.record(node, true, false, cycle_sinks);
            // SAFETY: `scalar` points into `ir` (or a `Tee`'s cell), which does not move.
            if let Some(input) = aggregated_input(unsafe { &*scalar }) {
                let input = found.record(input, false, false, cycle_sinks);
                let index = found.nodes[&input];
                found.collections[index].aggregated = true;
                found.aggregations.push((scalar, input));
            }
        }
    }
    let mut found = Found {
        by_key: HashMap::new(),
        collections: vec![],
        seen: HashSet::new(),
        nodes: HashMap::new(),
        node_order: vec![],
        exit_nodes: HashSet::new(),
        consumers: HashMap::new(),
        sent: HashSet::new(),
        merges: HashSet::new(),
        aggregations: vec![],
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
            with_casts_resolved(input, |input| {
                found.record(input, false, true, &cycle_sinks)
            });
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
        // SAFETY: as above.
        if let Some(input) = aggregated_input(unsafe { &*scalar }) {
            let input = found.record(input, false, false, &cycle_sinks);
            let index = found.nodes[&input];
            found.collections[index].aggregated = true;
            found.aggregations.push((scalar, input));
        }
    }
    for (scalar, input) in &found.aggregations {
        let (scalar, input) = (found.nodes[scalar], found.nodes[input]);
        if scalar != input {
            found.collections[scalar].covered_by.get_or_insert(input);
        }
    }

    // Merge-dominated hops (see the function docs): breadth-first downstream from each node of
    // a minted, non-exit collection, carrying whether a merge has been passed.
    for index in 0..found.collections.len() {
        let c = &found.collections[index];
        if !c.minted || c.exit {
            continue;
        }
        let starts: Vec<*const HydroNode> = found
            .node_order
            .iter()
            .copied()
            .filter(|ptr| found.nodes[ptr] == index)
            .collect();
        let mut queue: std::collections::VecDeque<(*const HydroNode, bool)> =
            starts.iter().map(|ptr| (*ptr, false)).collect();
        let mut seen: HashSet<(*const HydroNode, bool)> = queue.iter().copied().collect();
        let mut dominated = true;
        let mut merged_into: Vec<usize> = vec![];
        while let Some((ptr, merged)) = queue.pop_front() {
            let merged = merged || found.merges.contains(&ptr);
            let is_start = starts.contains(&ptr);
            if !is_start && found.exit_nodes.contains(&ptr) {
                if !merged {
                    dominated = false;
                    break;
                }
                let exit = found.nodes[&ptr];
                if !merged_into.contains(&exit) {
                    merged_into.push(exit);
                }
            }
            if !merged && found.sent.contains(&ptr) {
                dominated = false;
                break;
            }
            for &next in found.consumers.get(&ptr).into_iter().flatten() {
                if seen.insert((next, merged)) {
                    queue.push_back((next, merged));
                }
            }
        }
        if dominated {
            found.collections[index].merged_into = merged_into;
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

/// The input stream a labelled scalar aggregates (see [`ec_collections`]): `node`, through
/// pass-throughs (`AssertIsConsistent`, `Cast`, `ObserveNonDet` — the `assume_retries` an
/// idempotent `fold` puts on an at-least-once input —, `Tee`, references), is a `Fold`,
/// `Reduce`, `FoldKeyed` or `ReduceKeyed` whose input, through the same pass-throughs, is a
/// labelled stream at a cluster's top level. `None` for anything else.
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
    /// Through `AssertIsConsistent`, `Cast`, `ObserveNonDet`, `Tee` and references, as raw
    /// pointers: the
    /// shared cells' borrows end, the nodes do not move.
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

/// Calls `f` on the first node under `node` that is not a labelled pass-through (a `Cast`
/// whose input is labelled, or what a `Tee` shares): the operator an exit stands for, so that
/// an exit right after the minting operator (`broadcast_closed(..).values()` into a `batch`)
/// is the minting operator's row.
fn with_casts_resolved<R>(node: &HydroNode, f: impl FnOnce(&HydroNode) -> R) -> R {
    match node {
        HydroNode::Tee { inner, .. } | HydroNode::Reference { inner, .. } => {
            with_casts_resolved(&inner.0.borrow(), f)
        }
        HydroNode::Cast { inner, .. } if is_ec(inner) => with_casts_resolved(inner, f),
        _ => f(node),
    }
}

/// Calls `f` on the node a `Tee` (or reference) shares, through any chain of them, or on
/// `node` itself.
fn with_shared_resolved<R>(node: &HydroNode, f: impl FnOnce(&HydroNode) -> R) -> R {
    match node {
        HydroNode::Tee { inner, .. } | HydroNode::Reference { inner, .. } => {
            with_shared_resolved(&inner.0.borrow(), f)
        }
        // A partition side is one output of the shared `PartitionShared`, which cannot be
        // tapped as a whole; the side is the operator.
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

/// Whether `node` is where an `EventualConsistency` label is minted: labelled, and none of
/// what feeds it (including what a `Tee` shares) is. A `CycleSource` is fed by its cycle's
/// sink and is never a minting point.
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

/// The label of a labelled collection: the node's tag, else the user's source line of the
/// labelling operator (`file:line`, the file by its name), else the operator's name.
pub(crate) fn ec_label(node: &HydroNode) -> String {
    node.metadata()
        .tag
        .clone()
        .or_else(|| {
            user_span(&node.metadata().op.backtrace).map(|span| {
                span.rsplit(['/', '\\'])
                    .next()
                    .unwrap_or(span.as_str())
                    .to_owned()
            })
        })
        .unwrap_or_else(|| ec_op(node))
}

/// The report name of a labelled collection (see [`EcCollection::name`]).
fn ec_name(node: &HydroNode) -> Option<String> {
    node.metadata().tag.clone().or_else(|| {
        source_names::user_frame(&node.metadata().op.backtrace)
            .and_then(|frame| source_names::site_name(&frame))
    })
}

/// The file name of the user's call that built `node` (see [`EcCollection::file`]).
fn ec_file(node: &HydroNode) -> Option<String> {
    let frame = source_names::user_frame(&node.metadata().op.backtrace)?;
    Some(
        frame
            .file
            .rsplit(['/', '\\'])
            .next()
            .unwrap_or(frame.file.as_str())
            .to_owned(),
    )
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

/// Whether every source `node` is computed from is a cluster's membership (a
/// `HydroSource::ClusterMembers`, or a `source_iter` over `ClusterIds`), following `Tee`s and
/// cycles like [`has_network_ancestor`]. Such a labelled collection is the member-id list a
/// cluster-to-cluster `broadcast_closed` labels and joins with its payload, not anything the program computes. It
/// is local, so it would only ever be listed as not checked, and it takes the name of the
/// statement that broadcasts (an `echoed` row next to the real `echoed`). The design keeps
/// `broadcast_closed` unchanged and filters such rows here (docs/design/hydrofrac-lite.md §6.3).
fn membership_only(node: &HydroNode, cycle_sinks: &HashMap<CycleId, *const HydroNode>) -> bool {
    /// `Some(true)`: only membership sources; `Some(false)`: some other source or a network;
    /// `None`: nothing new below (already visited or a dangling cycle).
    fn go(
        node: &HydroNode,
        cycle_sinks: &HashMap<CycleId, *const HydroNode>,
        seen: &mut HashSet<*const HydroNode>,
    ) -> Option<bool> {
        if !seen.insert(std::ptr::from_ref(node)) {
            return None;
        }
        let combine = |results: &mut dyn Iterator<Item = Option<bool>>| {
            let mut any = None;
            for r in results {
                match r {
                    Some(false) => return Some(false),
                    Some(true) => any = Some(true),
                    None => {}
                }
            }
            any
        };
        match node {
            HydroNode::Source {
                source: HydroSource::ClusterMembers(..),
                ..
            } => Some(true),
            // `source_iter` over `ClusterIds`, which splices as `__hydro_lang_cluster_ids_<key>`:
            // how `broadcast_closed` lists its destinations.
            HydroNode::Source {
                source: HydroSource::Iter(expr),
                ..
            } => Some(
                expr.to_token_stream()
                    .to_string()
                    .contains("__hydro_lang_cluster_ids_"),
            ),
            HydroNode::Network { .. } => Some(false),
            HydroNode::Tee { inner, .. }
            | HydroNode::Reference { inner, .. }
            | HydroNode::PartitionSide { inner, .. } => go(&inner.0.borrow(), cycle_sinks, seen),
            HydroNode::CycleSource { cycle_id, .. } => cycle_sinks.get(cycle_id).and_then(|sink| {
                // SAFETY: see the comment at the construction of `cycle_sinks`.
                go(unsafe { &**sink }, cycle_sinks, seen)
            }),
            _ => {
                let inputs = node.input();
                if inputs.is_empty() {
                    // Any other leaf: an external input, a stream, an iterator, a singleton.
                    return Some(false);
                }
                combine(&mut inputs.into_iter().map(|input| go(input, cycle_sinks, seen)))
            }
        }
    }
    go(node, cycle_sinks, &mut HashSet::new()) == Some(true)
}

/// Whether a network receive is (transitively) upstream of `node`, following cycles through
/// their sinks and `Tee`s through what they share.
fn has_network_ancestor(
    node: &HydroNode,
    cycle_sinks: &HashMap<CycleId, *const HydroNode>,
) -> bool {
    fn go(
        node: &HydroNode,
        cycle_sinks: &HashMap<CycleId, *const HydroNode>,
        seen: &mut HashSet<*const HydroNode>,
    ) -> bool {
        if !seen.insert(std::ptr::from_ref(node)) {
            return false;
        }
        match node {
            HydroNode::Network { .. } => true,
            HydroNode::Tee { inner, .. }
            | HydroNode::Reference { inner, .. }
            | HydroNode::PartitionSide { inner, .. } => go(&inner.0.borrow(), cycle_sinks, seen),
            HydroNode::CycleSource { cycle_id, .. } => {
                cycle_sinks.get(cycle_id).is_some_and(|sink| {
                    // SAFETY: see the comment at the construction of `cycle_sinks`.
                    go(unsafe { &**sink }, cycle_sinks, seen)
                })
            }
            _ => {
                node.input()
                    .into_iter()
                    .any(|input| go(input, cycle_sinks, seen))
                    || closure_refs(node).any(|input| go(input, cycle_sinks, seen))
            }
        }
    }
    go(node, cycle_sinks, &mut HashSet::new())
}

/// The singleton references `node`'s closures read (`HydroNode::Reference`s, which
/// [`HydroNode::input`] does not list): a closure that reads state fed by the network is fed
/// by the network.
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

/// The `file:line` of the first frame of `backtrace` outside `hydro_lang` itself: where the
/// program called `send`, rather than the networking layer's own frames.
pub fn user_span(backtrace: &Backtrace) -> Option<String> {
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
}
