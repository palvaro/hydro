//! Static dependency slices of the simulated program, for lineage attribution.
//!
//! The lineage log (see [`super::runtime::lineage`]) records which hook releases fed a tick
//! execution and which network sends / external outputs it produced. Without more
//! information, every output of an execution must be assumed to depend on every input of
//! that execution and of every earlier execution of the same tick (persisted state). That
//! is sound but coarse: in a tick that handles several message types, an output caused by
//! one message is blamed on all the others too, and every earlier delivery becomes a
//! candidate cause of every later output.
//!
//! This module computes, once per compiled program, the backward slice of each **sink**
//! (a network send or an external output) through the Hydro IR:
//!
//! * which hook-bound batches feed it without crossing a `defer_tick` (`direct` inputs:
//!   deliveries in the *same* execution), and
//! * which feed it only through at least one `defer_tick` (`carried` inputs: deliveries
//!   in *earlier* executions of the same tick),
//!
//! resolving `forward_ref` cycles to a fixpoint. A channel consumed through an unscripted
//! `batch` is a `carried` [`SliceInput::Channel`]: the log does not say which execution
//! released each arrival, so every arrival at the node before the execution counts. A sink
//! whose slice reaches an input the lineage log cannot observe at all (a multi-version
//! network) is marked `opaque`, and the lineage falls back to the coarse rule for it.
//!
//! The same traversal collects the program's **channels** ([`ChannelInfo`]): every `Network`
//! node, with its endpoints, whether its transport admits loss, and a label.
//!
//! Sinks are keyed by the `StmtId` the emitter assigns to the `Network` node or the
//! `SendExternal` root; generated code tags `Sent` / `Output` events with the same id.
//! The IR is traversed with [`traverse_dfir`], which assigns ids exactly as `emit` does.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use crate::compile::builder::CycleId;
use crate::compile::ir::{
    ClosureExpr, CollectionKind, HydroNode, HydroRoot, SharedNode, traverse_dfir,
};
use crate::location::dynamic::LocationId;
use crate::networking::{NetworkingInfo, TcpFault};

/// One observable input of a sink's slice.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SliceInput {
    /// A scripted batch hook (by handle id); its releases appear as `Released` events.
    Hook(usize),
    /// A network channel (by sink id); its arrivals appear as `Recv` events. In a top-level
    /// sink's slice the channel is consumed directly; in a tick sink's slice it is consumed
    /// through an unscripted `batch`, whose releases are not logged, so every arrival at the
    /// node before the execution is a possible input (always `carried`).
    Channel(usize),
    /// Output yielded by a local tick (by tick location) into the top level. The slice's
    /// hooks name what the yielded stream was computed from inside that tick.
    Yield(LocationId),
}

/// The backward slice of one sink.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SinkSlice {
    /// Inputs reached without crossing a `defer_tick`.
    pub direct: BTreeSet<SliceInput>,
    /// Inputs reached only through a `defer_tick` (state carried from earlier executions).
    pub carried: BTreeSet<SliceInput>,
    /// Some input of the slice is not observable in the lineage log.
    pub opaque: bool,
}

impl RawSlice {
    fn merge(&mut self, other: &RawSlice) {
        self.slice.union_into(&other.slice, false);
        self.cycles.extend(other.cycles.iter().copied());
        self.channels.extend(other.channels.iter().copied());
        self.states.extend(other.states.iter().copied());
    }
}

impl SinkSlice {
    fn union_into(&mut self, other: &SinkSlice, carried: bool) {
        if carried {
            self.carried.extend(other.direct.iter().cloned());
        } else {
            self.direct.extend(other.direct.iter().cloned());
        }
        self.carried.extend(other.carried.iter().cloned());
        self.opaque |= other.opaque;
    }
}

/// One network channel of the program: a `Network` node, which is a fault point of the
/// simulator (see [`super::runtime::lineage`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChannelInfo {
    /// The sink id (the `StmtId` the emitter assigns to the node).
    pub id: usize,
    /// The sending location (a process or cluster).
    pub from: LocationId,
    /// The receiving location (a process or cluster).
    pub to: LocationId,
    /// Whether the declared transport admits message loss, so omission is in the channel's
    /// fault vocabulary. A `fail_stop` channel admits only its sender's crash.
    pub lossy: bool,
    /// A label for reports and fault names: the channel's name if the program gave it one
    /// (`.name("..")` on the networking config), else the `file:line` of the `send`, else
    /// `ch#id`. Made unique across the program.
    pub label: String,
}

/// The slices of every sink in a compiled program, and the program's channels.
#[derive(Clone, Debug, Default)]
pub struct Slices {
    /// Sink id → slice.
    pub sinks: HashMap<usize, SinkSlice>,
    /// Scripted hook id → the channels (sink ids) whose arrivals it batches.
    pub hook_channels: HashMap<usize, BTreeSet<usize>>,
    /// Scripted hook id → the local ticks whose yielded output it batches.
    pub hook_yields: HashMap<usize, BTreeSet<LocationId>>,
    /// Whether each sink is inside a tick (`true`) or at the top level.
    pub sink_in_tick: HashMap<usize, bool>,
    /// Every network channel, by sink id.
    pub channels: BTreeMap<usize, ChannelInfo>,
}

/// A collection the program labels `EventualConsistency`: a premise that the label's
/// convergence is not withheld by faults, which a specification over it discharges
/// (`Spec::eventual_consistency`, or every one at once with `Spec::all_labels`) and which is
/// otherwise open. Enumerated by [`ec_collections`] on the IR before the observation pass
/// (`sim::observe`); the compiled program keeps the list (`CompiledSim::labels`).
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
}

impl EcCollection {
    /// The location, as `Cluster(...)`, for reports.
    pub fn location_label(&self) -> String {
        format!("{:?}", self.location)
    }

    /// The identity of the collection among the program's labelled nodes.
    pub(crate) fn key(&self) -> EcKey {
        (self.label.clone(), self.op.clone(), self.location.clone())
    }
}

/// `(label, operator, cluster)`: how labelled nodes are told apart, and how the observation
/// pass re-identifies the nodes this enumeration found.
pub(crate) type EcKey = (String, String, LocationId);

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
    }
    impl Found {
        fn record(
            &mut self,
            node: &HydroNode,
            minted: bool,
            exit: bool,
            cycle_sinks: &HashMap<CycleId, *const HydroNode>,
        ) {
            with_shared_resolved(node, |node| {
                let key = ec_key(node);
                let fed = has_network_ancestor(node, cycle_sinks);
                let index = *self.by_key.entry(key.clone()).or_insert_with(|| {
                    self.collections.push(EcCollection {
                        label: key.0,
                        op: key.1,
                        location: key.2,
                        fed_by_network: false,
                        collection_kind: node.metadata().collection_kind.clone(),
                        in_tick: !matches!(
                            node.metadata().location_id,
                            LocationId::Cluster(_)
                        ),
                        minted: false,
                        exit: false,
                    });
                    self.collections.len() - 1
                });
                let found = &mut self.collections[index];
                found.fed_by_network |= fed;
                found.minted |= minted;
                found.exit |= exit;
            });
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
        if is_ec_minting(node) {
            found.record(node, true, false, cycle_sinks);
        }
    }
    let mut found = Found {
        by_key: HashMap::new(),
        collections: vec![],
        seen: HashSet::new(),
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
    found.collections
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
        Some(crate::location::dynamic::ClusterConsistency::EventualConsistency)
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
        node.metadata().location_id.root().clone(),
    )
}

impl Slices {
    /// The slice of `sink`, if known and fully observable.
    pub fn precise(&self, sink: usize) -> Option<&SinkSlice> {
        self.sinks.get(&sink).filter(|slice| !slice.opaque)
    }

    /// The channel with this sink id.
    pub fn channel(&self, id: usize) -> Option<&ChannelInfo> {
        self.channels.get(&id)
    }
}

/// A slice before cycle resolution: `cycles` are the `forward_ref` sources it reaches,
/// each with whether it was reached through a `defer_tick`.
#[derive(Default)]
struct RawSlice {
    slice: SinkSlice,
    cycles: BTreeSet<(CycleId, bool)>,
    /// Network nodes reached, by address (resolved to sink ids once the traversal has
    /// numbered every node).
    channels: BTreeSet<(*const HydroNode, bool)>,
    /// State cells read through a `Reference` capture, by address of the referenced node.
    /// Whatever writes them (closures capturing them mutably) feeds the slice too.
    states: BTreeSet<*const HydroNode>,
}

/// The closures a node evaluates (which may capture references to other nodes).
fn closures(node: &HydroNode) -> Vec<&ClosureExpr> {
    match node {
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
    }
}

/// `(referenced node address, is_mut)` for every reference a closure captures.
fn captures(closure: &ClosureExpr) -> Vec<(*const HydroNode, bool, &SharedNode)> {
    closure
        .singleton_refs
        .iter()
        .map(|(node, is_mut)| {
            let HydroNode::Reference { inner, .. } = node else {
                unreachable!("singleton_refs only hold HydroNode::Reference");
            };
            (std::ptr::from_ref(&*inner.0.borrow()), *is_mut, inner)
        })
        .collect()
}

/// What feeds a scripted batch hook at the top level, before cycle resolution.
#[derive(Default)]
struct RawFeeds {
    channels: BTreeSet<*const HydroNode>,
    yields: BTreeSet<LocationId>,
    cycles: BTreeSet<CycleId>,
}

struct Walker<'a> {
    raw: RawSlice,
    hook_feeds: &'a mut HashMap<usize, RawFeeds>,
}

impl Walker<'_> {
    fn walk(&mut self, node: &HydroNode, carried: bool) {
        let put = |slice: &mut SinkSlice, input: SliceInput| {
            if carried {
                slice.carried.insert(input);
            } else {
                slice.direct.insert(input);
            }
        };
        match node {
            HydroNode::Placeholder => unreachable!(),
            HydroNode::Batch { inner, metadata } => {
                if let Some(hook) = metadata.op.sim_hook_id {
                    put(&mut self.raw.slice, SliceInput::Hook(hook));
                    collect_feeds(inner, self.hook_feeds.entry(hook).or_default());
                } else {
                    // Entropy-driven batch: its releases are not in the lineage log, but
                    // what feeds it is. Whatever arrived at the node before the execution
                    // may have been released into it, so its channels, yields and cycles
                    // are inputs carried from any earlier point of the run.
                    let mut feeds = RawFeeds::default();
                    collect_feeds(inner, &mut feeds);
                    for channel in feeds.channels {
                        self.raw.channels.insert((channel, true));
                    }
                    for tick in feeds.yields {
                        self.raw.slice.carried.insert(SliceInput::Yield(tick));
                    }
                    for cycle in feeds.cycles {
                        self.raw.cycles.insert((cycle, true));
                    }
                }
            }
            HydroNode::DeferTick { input, .. } => self.walk(input, true),
            HydroNode::CycleSource { cycle_id, .. } => {
                self.raw.cycles.insert((*cycle_id, carried));
            }
            HydroNode::Network { .. } => {
                self.raw
                    .channels
                    .insert((std::ptr::from_ref(node), carried));
            }
            // A tick's output flowing to the top level: the sink depends on the tick's
            // executions, and within them on whatever the yielded stream was computed from.
            HydroNode::YieldConcat { inner, .. } => {
                put(
                    &mut self.raw.slice,
                    SliceInput::Yield(inner.metadata().location_id.clone()),
                );
                self.walk(inner, carried);
            }
            HydroNode::Tee { inner, .. } | HydroNode::PartitionSide { inner, .. } => {
                self.walk(&inner.0.borrow(), carried);
            }
            HydroNode::Reference { inner, .. } => self.read_state(inner, carried),
            HydroNode::VersionedNetwork { .. } | HydroNode::VersionedNetworkFork { .. } => {
                debug_log(|| "opaque at versioned network".to_owned());
                self.raw.slice.opaque = true;
            }
            // Constants and test-driven sources: not channel deliveries.
            HydroNode::Source { .. }
            | HydroNode::SingletonSource { .. }
            | HydroNode::ExternalInput { .. } => {}
            _ => {
                for input in node.input() {
                    self.walk(input, carried);
                }
            }
        }
        // References captured by the node's closures are inputs too.
        for closure in closures(node) {
            for (_, _, inner) in captures(closure) {
                self.read_state(inner, carried);
            }
        }
    }

    /// A read of the node `inner` refers to: depends on its value and on its writers.
    fn read_state(&mut self, inner: &SharedNode, carried: bool) {
        let node = inner.0.borrow();
        self.raw.states.insert(std::ptr::from_ref(&*node));
        self.walk(&node, carried);
    }
}

/// The network receives (by node address), local tick yields, and top-level cycles
/// upstream of a top-level node, stopping at those boundaries.
fn collect_feeds(node: &HydroNode, feeds: &mut RawFeeds) {
    match node {
        HydroNode::Network { .. } => {
            feeds.channels.insert(std::ptr::from_ref(node));
        }
        HydroNode::YieldConcat { inner, .. } => {
            feeds.yields.insert(inner.metadata().location_id.clone());
        }
        HydroNode::CycleSource { cycle_id, .. } => {
            feeds.cycles.insert(*cycle_id);
        }
        HydroNode::Tee { inner, .. }
        | HydroNode::Reference { inner, .. }
        | HydroNode::PartitionSide { inner, .. } => collect_feeds(&inner.0.borrow(), feeds),
        HydroNode::Batch { .. }
        | HydroNode::VersionedNetwork { .. }
        | HydroNode::VersionedNetworkFork { .. } => {}
        _ => {
            for input in node.input() {
                collect_feeds(input, feeds);
            }
        }
    }
}

/// Computes the slices of every sink in `ir`. Must run on the same IR, in the same state,
/// as the emit that produces the simulator, so that statement ids agree.
pub fn compute(ir: &mut [HydroRoot]) -> Slices {
    #[derive(Default)]
    struct State {
        network_ids: HashMap<*const HydroNode, usize>,
        raw_sinks: Vec<(usize, bool, RawSlice)>,
        raw_cycles: HashMap<CycleId, RawSlice>,
        hook_feeds: HashMap<usize, RawFeeds>,
        /// State cell → the merged slice of every node whose closure mutates it.
        writers: HashMap<*const HydroNode, RawSlice>,
        /// Every `Network` node, in traversal order: `(id, from, to, lossy, name, span)`.
        channels: Vec<(
            usize,
            LocationId,
            LocationId,
            bool,
            Option<String>,
            Option<String>,
        )>,
    }
    let state = RefCell::new(State::default());

    fn slice_of(state: &mut State, input: &HydroNode) -> RawSlice {
        let mut walker = Walker {
            raw: RawSlice::default(),
            hook_feeds: &mut state.hook_feeds,
        };
        walker.walk(input, false);
        walker.raw
    }

    /// Records `node` as a writer of every state its closures capture mutably.
    fn record_writers(state: &mut State, node: &HydroNode) {
        let mutated: Vec<*const HydroNode> = closures(node)
            .into_iter()
            .flat_map(captures)
            .filter(|(_, is_mut, _)| *is_mut)
            .map(|(ptr, _, _)| ptr)
            .collect();
        if mutated.is_empty() {
            return;
        }
        let raw = slice_of(state, node);
        for ptr in mutated {
            state.writers.entry(ptr).or_default().merge(&raw);
        }
    }

    // Cycle sinks get no statement id and no traversal callback; slice them directly.
    for root in ir.iter() {
        if let HydroRoot::CycleSink {
            cycle_id, input, ..
        } = root
        {
            let mut state = state.borrow_mut();
            let raw = slice_of(&mut state, input);
            debug_log(|| {
                format!(
                    "cycle {cycle_id:?} <- {} : {:?} cycles {:?}",
                    input.print_root(),
                    raw.slice,
                    raw.cycles
                )
            });
            state.raw_cycles.insert(*cycle_id, raw);
        }
    }

    if std::env::var("HYDRO_IR_VARIANTS").is_ok() {
        let counts: RefCell<BTreeMap<String, usize>> = Default::default();
        traverse_dfir(
            ir,
            |root, _| {
                let name = root.print_root();
                *counts
                    .borrow_mut()
                    .entry(format!("root {}", name.split('(').next().unwrap_or(&name)))
                    .or_default() += 1;
            },
            |node, _| {
                let name = node.print_root();
                let name = name.split('(').next().unwrap_or(&name).to_owned();
                let kind = match &node.metadata().collection_kind {
                    CollectionKind::Stream { .. } => "stream",
                    CollectionKind::Singleton { .. } => "singleton",
                    CollectionKind::Optional { .. } => "optional",
                    CollectionKind::KeyedStream { .. } => "keyed_stream",
                    CollectionKind::KeyedSingleton { .. } => "keyed_singleton",
                };
                *counts
                    .borrow_mut()
                    .entry(format!("{name} [{kind}]"))
                    .or_default() += 1;
            },
        );
        eprintln!("ir variants: {:?}", counts.borrow());
    }

    traverse_dfir(
        ir,
        |root, counter| {
            if let HydroRoot::SendExternal { input, .. } = root {
                let id = counter.next_count() - 1;
                let mut state = state.borrow_mut();
                let raw = slice_of(&mut state, input);
                let in_tick = !input.metadata().location_id.is_top_level();
                state.raw_sinks.push((id, in_tick, raw));
            }
        },
        |node, counter| {
            record_writers(&mut state.borrow_mut(), node);
            if let HydroNode::Network { .. } = node {
                let id = counter.next_count() - 1;
                let mut state = state.borrow_mut();
                state.network_ids.insert(std::ptr::from_ref(node), id);
                let input = node.input()[0];
                debug_log(|| format!("sink#{id} = {}", print_tree(input, 0)));
                let raw = slice_of(&mut state, input);
                let in_tick = !input.metadata().location_id.is_top_level();
                state.raw_sinks.push((id, in_tick, raw));
                let from = input.metadata().location_id.root().clone();
                if let HydroNode::Network {
                    name,
                    networking_info,
                    metadata,
                    ..
                } = node
                {
                    let lossy = match networking_info {
                        NetworkingInfo::Tcp { fault } => !matches!(fault, TcpFault::FailStop),
                        NetworkingInfo::Udp { .. } => true,
                    };
                    state.channels.push((
                        id,
                        from,
                        metadata.location_id.root().clone(),
                        lossy,
                        name.clone(),
                        user_span(&metadata.op.backtrace),
                    ));
                }
            }
        },
    );
    let State {
        network_ids,
        raw_sinks,
        raw_cycles,
        hook_feeds,
        writers,
        channels,
    } = state.into_inner();

    // Network nodes are numbered by the traversal, so channel inputs resolve only now.
    let resolve_channels = |raw: &RawSlice| -> SinkSlice {
        let mut slice = raw.slice.clone();
        for (ptr, carried) in &raw.channels {
            if let Some(id) = network_ids.get(ptr) {
                let input = SliceInput::Channel(*id);
                if *carried {
                    slice.carried.insert(input);
                } else {
                    slice.direct.insert(input);
                }
            } else {
                debug_log(|| format!("network node {ptr:?} was never numbered"));
                slice.opaque = true
            }
        }
        slice
    };
    #[expect(
        clippy::disallowed_methods,
        reason = "re-keyed into HashMaps; iteration order is irrelevant"
    )]
    let (cycle_slices, writer_slices): (
        HashMap<CycleId, SinkSlice>,
        HashMap<*const HydroNode, SinkSlice>,
    ) = (
        raw_cycles
            .iter()
            .map(|(id, raw)| (*id, resolve_channels(raw)))
            .collect(),
        writers
            .iter()
            .map(|(ptr, raw)| (*ptr, resolve_channels(raw)))
            .collect(),
    );

    let mut slices = Slices::default();
    for (id, in_tick, raw) in raw_sinks {
        let mut slice = resolve_channels(&raw);
        // Fixpoint over the cycles and state cells the sink reaches. A cycle is tracked
        // with whether it was reached through a `defer_tick`; a state cell's writers may
        // have run in this execution or any earlier one, so they count as both.
        let mut seen_cycles: HashSet<(CycleId, bool)> = HashSet::new();
        let mut seen_states: HashSet<*const HydroNode> = HashSet::new();
        let mut pending: Vec<(CycleId, bool)> = raw.cycles.into_iter().collect();
        let mut pending_states: Vec<*const HydroNode> = raw.states.into_iter().collect();
        loop {
            if let Some((cycle, carried)) = pending.pop() {
                if !seen_cycles.insert((cycle, carried)) {
                    continue;
                }
                if let Some(cycle_raw) = raw_cycles.get(&cycle) {
                    slice.union_into(&cycle_slices[&cycle], carried);
                    pending.extend(
                        cycle_raw
                            .cycles
                            .iter()
                            .map(|(next, next_carried)| (*next, carried || *next_carried)),
                    );
                    pending_states.extend(cycle_raw.states.iter().copied());
                } else {
                    debug_log(|| format!("sink#{id} reaches unknown cycle {cycle:?}"));
                    slice.opaque = true
                }
            } else if let Some(cell) = pending_states.pop() {
                if !seen_states.insert(cell) {
                    continue;
                }
                if let Some(writer_raw) = writers.get(&cell) {
                    let writer_slice = &writer_slices[&cell];
                    slice.union_into(writer_slice, false);
                    slice.union_into(writer_slice, true);
                    pending.extend(writer_raw.cycles.iter().copied());
                    pending_states.extend(writer_raw.states.iter().copied());
                }
            } else {
                break;
            }
        }
        debug_log(|| format!("sink#{id} in_tick={in_tick} {slice:?}"));
        slices.sinks.insert(id, slice);
        slices.sink_in_tick.insert(id, in_tick);
    }
    // A hook fed through a top-level `forward_ref` is fed by whatever feeds the cycle.
    for (hook, feeds) in hook_feeds {
        let mut channel_ptrs = feeds.channels.clone();
        let mut yields = feeds.yields.clone();
        let mut seen: HashSet<CycleId> = HashSet::new();
        let mut pending: Vec<CycleId> = feeds.cycles.iter().copied().collect();
        while let Some(cycle) = pending.pop() {
            if !seen.insert(cycle) {
                continue;
            }
            if let Some(raw) = raw_cycles.get(&cycle) {
                channel_ptrs.extend(raw.channels.iter().map(|(ptr, _)| *ptr));
                yields.extend(
                    raw.slice
                        .direct
                        .iter()
                        .chain(&raw.slice.carried)
                        .filter_map(|input| match input {
                            SliceInput::Yield(tick) => Some(tick.clone()),
                            _ => None,
                        }),
                );
                pending.extend(raw.cycles.iter().map(|(next, _)| *next));
            }
        }
        let ids = channel_ptrs
            .into_iter()
            .filter_map(|ptr| network_ids.get(&ptr).copied())
            .collect();
        slices.hook_channels.insert(hook, ids);
        slices.hook_yields.insert(hook, yields);
    }
    // Channel labels: the program's name, else the `send`'s source line, else the id; made
    // unique with the id where two channels would otherwise share a label.
    let base_labels: Vec<String> = channels
        .iter()
        .map(|(id, _, _, _, name, span)| {
            name.clone()
                .or_else(|| {
                    span.as_ref().map(|span| {
                        span.rsplit(['/', '\\'])
                            .next()
                            .unwrap_or(span.as_str())
                            .to_owned()
                    })
                })
                .unwrap_or_else(|| format!("ch{id}"))
        })
        .collect();
    for (i, (id, from, to, lossy, _, _)) in channels.into_iter().enumerate() {
        let label = &base_labels[i];
        let unique = base_labels.iter().filter(|l| *l == label).count() == 1;
        let label = if unique {
            label.clone()
        } else {
            format!("{label}#{id}")
        };
        slices.channels.insert(
            id,
            ChannelInfo {
                id,
                from,
                to,
                lossy,
                label,
            },
        );
    }
    slices
}

/// The `file:line` of the first frame of `backtrace` outside `hydro_lang` itself: where the
/// program called `send`, rather than the networking layer's own frames.
#[cfg(feature = "build")]
fn user_span(backtrace: &crate::compile::ir::backtrace::Backtrace) -> Option<String> {
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

#[cfg(not(feature = "build"))]
fn user_span(backtrace: &crate::compile::ir::backtrace::Backtrace) -> Option<String> {
    backtrace.format_span()
}

/// Prints a slicing diagnostic when `HYDRO_SLICE_DEBUG` is set.
fn debug_log(message: impl FnOnce() -> String) {
    if std::env::var("HYDRO_SLICE_DEBUG").is_ok() {
        eprintln!("slice: {}", message());
    }
}

/// A compact rendering of the IR upstream of `node` (debugging aid for slice gaps).
fn print_tree(node: &HydroNode, depth: usize) -> String {
    let indent = "  ".repeat(depth);
    let name = node.print_root();
    let name = name.split('(').next().unwrap_or(&name).to_owned();
    let hook = node
        .metadata()
        .op
        .sim_hook_id
        .map(|h| format!(" hook={h}"))
        .unwrap_or_default();
    let mut out = format!("\n{indent}{name}{hook}");
    match node {
        HydroNode::Tee { inner, .. }
        | HydroNode::Reference { inner, .. }
        | HydroNode::PartitionSide { inner, .. } => {
            out.push_str(&print_tree(&inner.0.borrow(), depth + 1));
        }
        HydroNode::CycleSource { cycle_id, .. } => out.push_str(&format!(" {cycle_id:?}")),
        _ => {
            for input in node.input() {
                out.push_str(&print_tree(input, depth + 1));
            }
        }
    }
    out
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
            _ => node
                .input()
                .into_iter()
                .any(|input| go(input, cycle_sinks, seen)),
        }
    }
    go(node, cycle_sinks, &mut HashSet::new())
}
