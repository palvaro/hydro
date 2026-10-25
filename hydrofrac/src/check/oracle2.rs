//! Oracle 2: a second cross-check of the search, independent of it and of the brute-force
//! enumeration (`brute_force.rs`) wherever that is cheap. Test-only.
//!
//! The brute-force oracle shares with the search the instrumented program (the tap pass and the
//! provenance rewrite), the fault hook, the labels and their taps, fact identity and the
//! evaluation of the specification. Oracle 2 has its own of each:
//!
//! - **Its own program.** [`EcCheck::instrument`] forks the user's flow before instrumenting it
//!   (`SimFlow::try_fork`), and oracle 2 compiles the fork, lazily, when
//!   [`EcCheck::cross_check`] runs: the user's IR with network hook points, without the
//!   provenance rewrite and without hydrofrac's tap pass.
//! - **Its own taps** ([`tap`]). A node labelled `EventualConsistency` on a cluster is tapped
//!   where the label is *minted* (no input is labelled) or where the collection *exits* the
//!   labelled region (an unlabelled consumer other than a network send, or a program output).
//!   One output per node, by node identity: two nodes are two collections, even if they come
//!   from one source line. `Tee`s and references stand for the node they share, and a `Cast` of
//!   a labelled input for that input. Only streams (and keyed streams) at the top level of a
//!   cluster are tapped; scalars and nodes inside ticks are listed as skipped. Each element
//!   leaves as `bincode(element)`, or as nothing when its type is not `Serialize` (the
//!   collection is then reported as not compared).
//! - **Its own faults** ([`Hook`]). An omission drops the `k`-th send of one content (channel,
//!   sender, receiver, payload) on a lossy channel; a crash halts a node before its first send,
//!   or right after its `k`-th send (counting all its sends, from 0). Faults act only until the
//!   harness heals, like the search's: the inputs run (with phase barriers forbidden), the run
//!   quiesces, the network heals, the run quiesces, the recovery phase runs (with no faults), and
//!   the run quiesces again.
//! - **Its own enumeration.** The universe is learned from the runs (every node's crash before
//!   any send; every pre-heal send's omission, if lossy, and its sender's crash after it) until a
//!   pass over every set of at most `max_faults` faults reveals nothing new. This mirrors the
//!   brute force's enumeration, rewritten over oracle 2's faults. A set is admissible for a
//!   collection under the same bounds as the search's: at most one crash per node, at most the
//!   `max_crashes` cap, never every node, and at most `|members| − 2` members of the
//!   collection's cluster.
//! - **Its own comparison.** Per run (every schedule, through `exhaustive_then`), per
//!   collection: the set of elements each correct (not crashed) member emitted. The run breaks
//!   the collection if two correct members' sets differ, or if the program panics. A collection
//!   breaks if some admissible set breaks it in some schedule. No lineage, no specification
//!   module, no pruning.
//!
//! **What it still shares** with the search: the simulator itself (the scheduler and its
//! schedule exploration), the user's IR, the harness's phases (restated, not called), the guard
//! against time-bounded exhaustive runs, and, to name things, the label's name
//! (`crate::observe::ec_label`) and its `(label, operator, kind, cluster)` key, which map oracle
//! 2's collections to the search's labels. A key that names one node on each side is an exact
//! match; several nodes with one key are matched as a group, by trying every one-to-one
//! assignment ([`disagreements`]).
//!
//! **Agreement** ([`disagreements`]), for every label the search searched and oracle 2 has a
//! compared collection of:
//! - a claim that the label holds with up to `k` crashes (a certificate, a budget's cleared
//!   bound, a counterexample's certified tolerance, a sampled search's "nothing found", or a
//!   `Vacuous` verdict, which holds for every `k`) is refuted by any breaking set of oracle 2
//!   with at most `k` crashes;
//! - a counterexample of `n ≤ max_faults` faults and `c` crashes (schedules not sampled) must be
//!   matched by a breaking set of oracle 2 with at most `n` faults and `c` crashes: each of the
//!   search's faults is one of oracle 2's in the run that breaks;
//! - the failure-free runs agree on vacuity: the search's probe found an empty domain
//!   (`Stats::domain`) exactly when no correct member emitted anything failure-free at oracle
//!   2's collection of the label.
//!
//! **Where it is weaker or different.**
//! - Same-key nodes are matched as a group: a wrong verdict hides if some other assignment of
//!   the group's collections to its labels explains it. Where one side lists more same-key
//!   nodes than the other (a membership-only `broadcast_closed` the search does not list, say),
//!   the extra ones are left unassigned, and one of them may stand in for the right one.
//! - A crash after a send whose payload the node sent before has no counterpart in the search's
//!   vocabulary (`crash(n after m)` fires at the first copy of `m`); oracle 2 tries it.
//! - Element types that are not `Serialize` are not compared (the search falls back to `Debug`
//!   or a hash). Scalars, in-tick nodes and labels mapped to no collection are not compared.

use std::cell::{OnceCell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fmt::{self, Display};
use std::panic::AssertUnwindSafe;
use std::rc::Rc;
use std::time::{Duration, Instant};

use hydro_lang::Counter;
use hydro_lang::compile::builder::ExternalPortId;
use hydro_lang::compile::ir::{
    ClosureExpr, CollectionKind, DebugExpr, DebugInstantiate, DebugType, HydroNode, HydroRoot,
    SharedNode,
};
use hydro_lang::live_collections::stream::networking::serialize_bincode_with_type;
use hydro_lang::location::LocationKey;
use hydro_lang::location::dynamic::{ClusterConsistency, LocationId};
use hydro_lang::networking::{NetworkingInfo, TcpFault};
use hydro_lang::sim::compiled::{CompiledSim, forbid_barriers, quiesce};
use hydro_lang::sim::flow::SimFlow;
use hydro_lang::sim::network_hook::{IrPassStage, LocationInfo, SimNetworkHook};
use syn::parse_quote;

use super::races_engine::refuse_truncated_exhaustive;
use super::{BARRIER_IN_FAULT_PHASE, EcCheck, program_panic};
use crate::check::results::{LabelResult, Results, VacuousReason, Verdict, label_name};
use crate::observe::{ec_label, kind_name};
use crate::search::Tolerance;

// ---------------------------------------------------------------------------------------------
// The program
// ---------------------------------------------------------------------------------------------

/// Oracle 2's program: a fork of the user's flow, compiled the first time it is needed.
pub(super) struct Program {
    flow: RefCell<Option<SimFlow<'static>>>,
    sim: OnceCell<CompiledSim>,
    taps: Rc<RefCell<Option<Taps>>>,
    /// Why there is no program, if there is none.
    unavailable: Option<String>,
}

impl Program {
    /// Forks `flow` (before hydrofrac instruments it) and registers oracle 2's own tap pass on
    /// the fork.
    pub(super) fn fork(flow: &SimFlow<'_>) -> Self {
        let taps = Rc::new(RefCell::new(None));
        let out = taps.clone();
        let fork = flow.try_fork().map(|fork| {
            fork.refuse_sim_faults("hydrofrac (oracle 2)")
                .skip_consistency_assertions()
                .test_safety_only()
                .with_network_hook_points()
                .with_ir_pass(IrPassStage::PreNetwork, move |ir| {
                    *out.borrow_mut() = Some(tap(ir));
                })
        });
        let unavailable = fork.is_none().then(|| {
            "the flow had IR passes registered before `EcCheck::instrument`, which cannot be \
             copied"
                .to_owned()
        });
        Program {
            flow: RefCell::new(fork),
            sim: OnceCell::new(),
            taps,
            unavailable,
        }
    }

    fn compiled(&self) -> &CompiledSim {
        self.sim.get_or_init(|| {
            self.flow
                .borrow_mut()
                .take()
                .expect("oracle 2's flow is compiled once")
                .compiled()
        })
    }
}

// ---------------------------------------------------------------------------------------------
// The taps
// ---------------------------------------------------------------------------------------------

/// `(label, operator, collection kind, cluster)`: the key the search groups labelled nodes by,
/// used only to map oracle 2's collections to the search's labels.
type Key = (String, String, &'static str, LocationId);

/// One tapped node.
#[derive(Clone, Debug)]
struct Tap {
    /// Its external output port.
    port: usize,
    /// Its cluster.
    cluster: LocationId,
    key: Key,
    /// Why it is tapped: `minted`, `exits` or `output`.
    why: &'static str,
}

/// What the tap pass found.
#[derive(Clone, Debug, Default)]
struct Taps {
    taps: Vec<Tap>,
    /// Labelled nodes it chose but could not tap, with why.
    skipped: Vec<String>,
}

/// Whether `node` carries `EventualConsistency` on a cluster (at any depth of ticks).
fn labelled(node: &HydroNode) -> bool {
    matches!(
        node.metadata().consistency,
        Some(ClusterConsistency::EventualConsistency)
    ) && matches!(node.metadata().location_id.root(), LocationId::Cluster(_))
}

/// `Map`, `Network`, ...
fn op_name(node: &HydroNode) -> String {
    let name = node.print_root();
    name.split('(').next().unwrap_or(&name).to_owned()
}

fn key(node: &HydroNode) -> Key {
    (
        ec_label(node),
        op_name(node),
        kind_name(&node.metadata().collection_kind),
        node.metadata().location_id.root().clone(),
    )
}

/// Calls `f` on the node `node` stands for: through `Tee`s and references to what they share,
/// and through a `Cast` of a labelled input to that input.
fn resolve<R>(node: &HydroNode, f: impl FnOnce(&HydroNode) -> R) -> R {
    match node {
        HydroNode::Tee { inner, .. } | HydroNode::Reference { inner, .. } => {
            resolve(&inner.0.borrow(), f)
        }
        HydroNode::Cast { inner, .. } if labelled(inner) => resolve(inner, f),
        _ => f(node),
    }
}

/// The nodes to tap, in first-chosen order, with why.
#[derive(Default)]
struct Choice {
    chosen: Vec<(*const HydroNode, &'static str)>,
    set: HashSet<*const HydroNode>,
    seen: HashSet<*const HydroNode>,
}

impl Choice {
    fn choose(&mut self, node: &HydroNode, why: &'static str) {
        resolve(node, |node| {
            let ptr = std::ptr::from_ref(node);
            if self.set.insert(ptr) {
                self.chosen.push((ptr, why));
            }
        });
    }

    fn visit(&mut self, node: &HydroNode) {
        if !self.seen.insert(std::ptr::from_ref(node)) {
            return;
        }
        let consumer = labelled(node);
        let send = matches!(node, HydroNode::Network { .. });
        let mut labelled_input = false;
        let mut edge = |this: &mut Self, input: &HydroNode| {
            this.visit(input);
            let input_labelled = resolve(input, labelled);
            labelled_input |= input_labelled;
            if input_labelled && !consumer && !send {
                this.choose(input, "exits");
            }
        };
        match node {
            HydroNode::Tee { inner, .. }
            | HydroNode::Reference { inner, .. }
            | HydroNode::PartitionSide { inner, .. } => edge(self, &inner.0.borrow()),
            _ => {
                for input in node.input() {
                    edge(self, input);
                }
            }
        }
        let passthrough = matches!(
            node,
            HydroNode::Tee { .. } | HydroNode::Reference { .. } | HydroNode::CycleSource { .. }
        );
        if consumer && !labelled_input && !passthrough {
            self.choose(node, "minted");
        }
    }
}

/// Visits every node under `ir` bottom-up, in place, each shared node once (inside its cell).
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
        let (HydroRoot::ForEach { input, .. }
        | HydroRoot::SendExternal { input, .. }
        | HydroRoot::DestSink { input, .. }
        | HydroRoot::CycleSink { input, .. }
        | HydroRoot::EmbeddedOutput { input, .. }
        | HydroRoot::Null { input, .. }) = root;
        walk(input, &mut seen, f);
    }
}

/// The closure a tap maps each element through: `Some(bincode(x))` if `T: Serialize`, else
/// `None` (autoref specialization, so any element type compiles).
fn tap_closure(element_type: &syn::Type) -> syn::Expr {
    parse_quote!(
        move |__oracle2_x: #element_type| -> ::core::option::Option<::std::vec::Vec<u8>> {
            struct __Oracle2Wrap<'__a, __T>(&'__a __T);
            trait __Oracle2ViaSerialize {
                fn __oracle2_id(&self) -> ::core::option::Option<::std::vec::Vec<u8>>;
            }
            impl<__T: hydro_lang::__staged::__deps::serde::Serialize> __Oracle2ViaSerialize for &__Oracle2Wrap<'_, __T> {
                fn __oracle2_id(&self) -> ::core::option::Option<::std::vec::Vec<u8>> {
                    ::core::option::Option::Some(hydro_lang::runtime_support::bincode::serialize(self.0).unwrap())
                }
            }
            trait __Oracle2NotSerialize {
                fn __oracle2_id(&self) -> ::core::option::Option<::std::vec::Vec<u8>>;
            }
            impl<__T> __Oracle2NotSerialize for __Oracle2Wrap<'_, __T> {
                fn __oracle2_id(&self) -> ::core::option::Option<::std::vec::Vec<u8>> {
                    ::core::option::Option::None
                }
            }
            (&&__Oracle2Wrap(&__oracle2_x)).__oracle2_id()
        }
    )
}

/// Oracle 2's tap pass: chooses the nodes (see the [module docs](self)) and tees each into an
/// external output of its own.
fn tap(ir: &mut Vec<HydroRoot>) -> Taps {
    let mut choice = Choice::default();
    for root in ir.iter() {
        let input = root.input();
        choice.visit(input);
        if resolve(input, labelled)
            && matches!(
                root,
                HydroRoot::SendExternal { .. }
                    | HydroRoot::ForEach { .. }
                    | HydroRoot::DestSink { .. }
                    | HydroRoot::EmbeddedOutput { .. }
            )
        {
            choice.choose(input, "output");
        }
    }

    // Fresh ports, above every port the program uses.
    let mut max: Option<usize> = None;
    for root in ir.iter() {
        if let HydroRoot::SendExternal { to_port_id, .. } = root {
            max = max.max(Some(to_port_id.into_inner()));
        }
    }
    walk_in_place(ir, &mut |node| {
        if let HydroNode::ExternalInput { from_port_id, .. } = node {
            max = max.max(Some(from_port_id.into_inner()));
        }
    });
    let mut ports = Counter::<ExternalPortId>::default();
    if let Some(max) = max {
        while ports.next_count() <= max {
            ports.get_and_increment();
        }
    }

    let why: HashMap<*const HydroNode, &'static str> = choice.chosen.iter().copied().collect();
    let order: HashMap<*const HydroNode, usize> = choice
        .chosen
        .iter()
        .enumerate()
        .map(|(i, (p, _))| (*p, i))
        .collect();
    let wire_type: syn::Type = parse_quote!(::core::option::Option<::std::vec::Vec<u8>>);
    let mut found: Vec<(usize, Tap)> = vec![];
    let mut skipped: Vec<(usize, String)> = vec![];
    let mut roots: Vec<HydroRoot> = vec![];
    walk_in_place(ir, &mut |node: &mut HydroNode| {
        let ptr = std::ptr::from_ref::<HydroNode>(node);
        let Some(&why) = why.get(&ptr) else {
            return;
        };
        let position = order[&ptr];
        let key = key(node);
        let describe = |key: &Key| format!("{} ({}, {})", key.0, key.1, kind_name_of(key));
        if matches!(
            node,
            HydroNode::PartitionShared { .. }
                | HydroNode::VersionedNetwork { .. }
                | HydroNode::VersionedNetworkFork { .. }
                | HydroNode::Placeholder
        ) {
            skipped.push((position, format!("{}: not tappable", describe(&key))));
            return;
        }
        let metadata = node.metadata().clone();
        let LocationId::Cluster(_) = &metadata.location_id else {
            skipped.push((position, format!("{}: inside a tick", describe(&key))));
            return;
        };
        let (element_type, bound, order, retry): (syn::Type, _, _, _) =
            match &metadata.collection_kind {
                CollectionKind::Stream {
                    element_type,
                    bound,
                    order,
                    retry,
                } => ((*element_type.0).clone(), bound, order, retry),
                CollectionKind::KeyedStream {
                    key_type,
                    value_type,
                    bound,
                    value_order,
                    value_retry,
                } => {
                    let (k, v) = (&*key_type.0, &*value_type.0);
                    (parse_quote!((#k, #v)), bound, value_order, value_retry)
                }
                _ => {
                    skipped.push((position, format!("{}: not a stream", describe(&key))));
                    return;
                }
            };
        // Share the node, and take a second branch to the tap.
        let shared: Rc<RefCell<HydroNode>> = if let HydroNode::Tee { inner, .. } = node {
            inner.0.clone()
        } else {
            let original = std::mem::replace(node, HydroNode::Placeholder);
            let cell = Rc::new(RefCell::new(original));
            *node = HydroNode::Tee {
                inner: SharedNode(cell.clone()),
                metadata: metadata.clone(),
            };
            cell
        };
        let branch = HydroNode::Tee {
            inner: SharedNode(shared),
            metadata: metadata.clone(),
        };
        let mut map_metadata = metadata.clone();
        map_metadata.consistency = None;
        map_metadata.tag = None;
        map_metadata.op.id = None;
        map_metadata.collection_kind = CollectionKind::Stream {
            bound: bound.clone(),
            order: order.clone(),
            retry: retry.clone(),
            element_type: DebugType::from(wire_type.clone()),
        };
        let mapped = HydroNode::Map {
            f: ClosureExpr::new(DebugExpr::from(tap_closure(&element_type)), vec![]),
            input: Box::new(branch),
            metadata: map_metadata,
        };
        let port = ports.get_and_increment();
        let mut op_metadata = metadata.op.clone();
        op_metadata.id = None;
        roots.push(HydroRoot::SendExternal {
            to_external_key: LocationKey::FIRST,
            to_port_id: port,
            to_many: false,
            unpaired: true,
            serialize_fn: Some(serialize_bincode_with_type(false, &wire_type).into()),
            instantiate_fn: DebugInstantiate::Building,
            input: Box::new(mapped),
            op_metadata,
        });
        found.push((
            position,
            Tap {
                port: port.into_inner(),
                cluster: metadata.location_id.root().clone(),
                key,
                why,
            },
        ));
    });
    ir.extend(roots);
    found.sort_by_key(|(position, _)| *position);
    skipped.sort_by_key(|(position, _)| *position);
    Taps {
        taps: found.into_iter().map(|(_, tap)| tap).collect(),
        skipped: skipped.into_iter().map(|(_, s)| s).collect(),
    }
}

fn kind_name_of(key: &Key) -> &'static str {
    key.2
}

// ---------------------------------------------------------------------------------------------
// The faults and the hook
// ---------------------------------------------------------------------------------------------

/// A process (`None`) or a cluster member, at its root location.
type Node = (LocationId, Option<u32>);

/// One of oracle 2's faults.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
enum Fault {
    /// Halts `node` before its first send (`None`) or right after its `k`-th send (from 0).
    Crash { node: Node, after: Option<u64> },
    /// Drops the `copy`-th send (from 0) of a content on a lossy channel, if it is pre-heal.
    Omit {
        channel: usize,
        from: Node,
        to: Node,
        bytes: Rc<[u8]>,
        copy: u32,
    },
}

impl Fault {
    fn crashed(&self) -> Option<&Node> {
        match self {
            Fault::Crash { node, .. } => Some(node),
            Fault::Omit { .. } => None,
        }
    }
}

type FaultSet = BTreeSet<Fault>;

/// A message's content: channel, sender, receiver and payload.
type Content = (usize, Node, Node, Rc<[u8]>);

fn crashes(set: &FaultSet) -> usize {
    set.iter().filter(|f| f.crashed().is_some()).count()
}

/// Whether a channel may lose messages: everything but `fail_stop` TCP.
fn lossy(networking: &NetworkingInfo) -> bool {
    !matches!(
        networking,
        NetworkingInfo::Tcp {
            fault: TcpFault::FailStop
        }
    )
}

/// What one instance (one schedule) left behind.
#[derive(Default)]
struct Record {
    crashed: BTreeSet<Node>,
    /// Port → member → the elements it emitted.
    facts: BTreeMap<usize, BTreeMap<u32, BTreeSet<Vec<u8>>>>,
    /// Ports that emitted an element whose type is not `Serialize`.
    unserializable: BTreeSet<usize>,
    /// The candidate faults the instance revealed (its pre-heal sends).
    revealed: BTreeSet<Fault>,
}

#[derive(Default)]
struct Instance {
    started: bool,
    healed: bool,
    sends: HashMap<Node, u64>,
    /// How many sends of each content so far.
    copies: HashMap<Content, u32>,
    halts: Vec<Node>,
    record: Record,
}

impl Instance {
    fn crash(&mut self, node: &Node) {
        if self.record.crashed.insert(node.clone()) {
            self.halts.push(node.clone());
        }
    }
}

/// Oracle 2's network hook: injects one fault set into every instance, and records what each
/// instance's taps emitted.
struct Hook {
    /// Channel → (sender, receiver, lossy).
    channels: HashMap<usize, (LocationId, LocationId, bool)>,
    /// Tap port → its cluster.
    ports: BTreeMap<usize, LocationId>,
    crashes: BTreeMap<Node, Option<u64>>,
    omits: BTreeSet<(Content, u32)>,
    current: RefCell<Instance>,
    finished: RefCell<Vec<Record>>,
}

impl Hook {
    fn new(sim: &CompiledSim, ports: BTreeMap<usize, LocationId>, faults: &FaultSet) -> Self {
        let mut crashes = BTreeMap::new();
        let mut omits = BTreeSet::new();
        for fault in faults {
            match fault {
                Fault::Crash { node, after } => {
                    let previous = crashes.insert(node.clone(), *after);
                    assert!(previous.is_none(), "two crashes of one node in a set");
                }
                Fault::Omit {
                    channel,
                    from,
                    to,
                    bytes,
                    copy,
                } => {
                    omits.insert(((*channel, from.clone(), to.clone(), bytes.clone()), *copy));
                }
            }
        }
        Hook {
            channels: sim
                .channels()
                .iter()
                .map(|c| {
                    (
                        c.id,
                        (
                            c.from.root().clone(),
                            c.to.root().clone(),
                            lossy(&c.networking),
                        ),
                    )
                })
                .collect(),
            ports,
            crashes,
            omits,
            current: RefCell::new(Instance::default()),
            finished: RefCell::new(vec![]),
        }
    }

    fn heal(&self) {
        self.current.borrow_mut().healed = true;
    }

    /// The records of every instance so far, the current one included (which ends).
    fn drain(&self) -> Vec<Record> {
        let mut out = std::mem::take(&mut *self.finished.borrow_mut());
        let current = std::mem::take(&mut *self.current.borrow_mut());
        if current.started {
            out.push(current.record);
        }
        out
    }
}

impl SimNetworkHook for Hook {
    fn on_instance_begin(&self) {
        let previous = std::mem::take(&mut *self.current.borrow_mut());
        if previous.started {
            self.finished.borrow_mut().push(previous.record);
        }
        let mut current = self.current.borrow_mut();
        current.started = true;
        for (node, after) in &self.crashes {
            if after.is_none() {
                current.crash(node);
            }
        }
    }

    fn on_send(
        &self,
        channel: usize,
        from_member: Option<u32>,
        to_member: Option<u32>,
        payload: &[u8],
        _meta: Option<Vec<u8>>,
    ) -> Option<u64> {
        let (from, to, lossy) = self
            .channels
            .get(&channel)
            .unwrap_or_else(|| panic!("oracle 2: send on unknown channel {channel}"));
        let from: Node = (from.clone(), from_member);
        let to: Node = (to.clone(), to_member);
        let mut current = self.current.borrow_mut();
        let ordinal = {
            let n = current.sends.entry(from.clone()).or_default();
            *n += 1;
            *n - 1
        };
        let bytes: Rc<[u8]> = payload.into();
        let content: Content = (channel, from.clone(), to.clone(), bytes.clone());
        let copy = {
            let n = current.copies.entry(content.clone()).or_default();
            *n += 1;
            *n - 1
        };
        if current.record.crashed.contains(&from) {
            return None;
        }
        let mut delivered = true;
        if !current.healed {
            current.record.revealed.insert(Fault::Crash {
                node: from.clone(),
                after: Some(ordinal),
            });
            if *lossy {
                delivered = !self.omits.contains(&(content, copy));
                current.record.revealed.insert(Fault::Omit {
                    channel,
                    from: from.clone(),
                    to,
                    bytes,
                    copy,
                });
            }
            if self.crashes.get(&from) == Some(&Some(ordinal)) {
                current.crash(&from);
            }
        }
        delivered.then_some(0)
    }

    fn on_output(
        &self,
        port: usize,
        member: Option<u32>,
        payload: &[u8],
        _meta: Option<Vec<u8>>,
    ) -> bool {
        let Some(cluster) = self.ports.get(&port) else {
            return true;
        };
        let element: Option<Vec<u8>> =
            bincode::deserialize(payload).expect("an oracle-2 tap output is `Option<Vec<u8>>`");
        let member = member.expect("oracle 2 taps only cluster collections");
        let mut current = self.current.borrow_mut();
        let record = &mut current.record;
        // A crashed member emits nothing (its current execution may still run until the
        // scheduler halts it).
        if record.crashed.contains(&(cluster.clone(), Some(member))) {
            return false;
        }
        match element {
            Some(bytes) => {
                record
                    .facts
                    .entry(port)
                    .or_default()
                    .entry(member)
                    .or_default()
                    .insert(bytes);
            }
            None => {
                record.unserializable.insert(port);
            }
        }
        true
    }

    fn take_halts(&self) -> Vec<(LocationId, Option<u32>)> {
        std::mem::take(&mut self.current.borrow_mut().halts)
    }
}

// ---------------------------------------------------------------------------------------------
// The report
// ---------------------------------------------------------------------------------------------

/// A breaking fault set of one collection.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct Oracle2Breaking {
    /// The set, rendered.
    pub faults: String,
    pub crashes: usize,
    pub size: usize,
}

/// One of oracle 2's collections.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct Oracle2Collection {
    /// `label (operator, kind) [minted|exits|output]`.
    pub name: String,
    /// The search's labels with its key (see [`disagreements`]), by index
    /// ([`crate::observe::Labels::plans`]): one when the key names one node on each side.
    pub labels: Vec<usize>,
    /// Its `(label, operator, kind, cluster)` key.
    key: Key,
    /// The members of its cluster.
    pub members: usize,
    /// Whether a correct member emitted anything in the failure-free run.
    pub failure_free_facts: bool,
    /// Whether its element type is not `Serialize` (so it is not compared).
    pub unserializable: bool,
    /// The admissible sets run for it, counting the failure-free one.
    pub admissible: usize,
    /// Its breaking admissible sets, fewest crashes first, then fewest faults.
    pub breaking: Vec<Oracle2Breaking>,
}

impl Oracle2Collection {
    /// Whether oracle 2 compares this collection with the search's label.
    fn compared(&self) -> bool {
        !self.labels.is_empty() && !self.unserializable && self.members > 1
    }
}

/// What oracle 2 found.
#[doc(hidden)]
#[derive(Clone, Debug, Default)]
pub struct Oracle2 {
    /// Why it did not run, if it did not.
    pub unavailable: Option<String>,
    pub max_faults: usize,
    /// Whether schedules were sampled (as the search's were).
    pub sampled: bool,
    pub collections: Vec<Oracle2Collection>,
    /// Labelled nodes it chose but did not tap, with why.
    pub skipped: Vec<String>,
    /// The final universe's size.
    pub universe: usize,
    /// The sets run, counting the failure-free one.
    pub sets: usize,
    pub schedules: usize,
    pub passes: usize,
    pub wall: Duration,
    /// The first set whose run panicked, and the panic.
    pub panic: Option<(String, String)>,
}

impl Display for Oracle2 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(why) = &self.unavailable {
            return writeln!(f, "oracle 2: not run: {why}");
        }
        writeln!(
            f,
            "oracle 2 up to {} fault(s){}: {} collection(s), {} candidate faults, {} sets, {} \
             schedules, {} pass(es), {:.2?}",
            self.max_faults,
            if self.sampled {
                " (schedules sampled)"
            } else {
                ""
            },
            self.collections.len(),
            self.universe,
            self.sets,
            self.schedules,
            self.passes,
            self.wall
        )?;
        for c in &self.collections {
            write!(f, "  {}: ", c.name)?;
            match c.labels.len() {
                0 => write!(f, "[no search label] ")?,
                1 => {}
                n => write!(f, "[one of {n} same-key labels] ")?,
            }
            if c.unserializable {
                write!(f, "not compared (element type is not `Serialize`)")?;
            } else if c.members <= 1 {
                write!(f, "single member")?;
            } else {
                match c.breaking.first() {
                    None => write!(f, "no admissible set breaks it")?,
                    Some(b) if b.size == 0 => write!(f, "the failure-free run breaks it")?,
                    Some(b) => write!(f, "breaks with {} crash(es): {}", b.crashes, b.faults)?,
                }
                write!(
                    f,
                    " ({} breaking of {} admissible sets{})",
                    c.breaking.len(),
                    c.admissible,
                    if c.failure_free_facts {
                        ""
                    } else {
                        ", no facts failure-free"
                    }
                )?;
            }
            writeln!(f)?;
        }
        for s in &self.skipped {
            writeln!(f, "  skipped {s}")?;
        }
        if let Some((set, panic)) = &self.panic {
            writeln!(f, "  the run of {set} panicked: {panic}")?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------------------------
// The enumeration
// ---------------------------------------------------------------------------------------------

/// The bounds of one cluster's collections.
struct Bound {
    cluster: LocationId,
    members: BTreeSet<u32>,
    cap: Option<usize>,
    nodes: usize,
}

impl Bound {
    fn admits(&self, set: &FaultSet) -> bool {
        let crashed: Vec<&Node> = set.iter().filter_map(Fault::crashed).collect();
        let distinct: BTreeSet<&Node> = crashed.iter().copied().collect();
        distinct.len() == crashed.len()
            && crashed.len() < self.nodes
            && self.cap.is_none_or(|cap| crashed.len() <= cap)
            && crashed
                .iter()
                .filter(|(l, m)| *l == self.cluster && m.is_some_and(|m| self.members.contains(&m)))
                .count()
                <= self.members.len().saturating_sub(2)
    }
}

/// Every subset of `universe` with exactly `size` faults.
fn subsets(universe: &[Fault], size: usize) -> Vec<FaultSet> {
    fn go(
        universe: &[Fault],
        start: usize,
        size: usize,
        current: &mut Vec<Fault>,
        out: &mut Vec<FaultSet>,
    ) {
        if current.len() == size {
            out.push(current.iter().cloned().collect());
            return;
        }
        for i in start..universe.len() {
            current.push(universe[i].clone());
            go(universe, i + 1, size, current, out);
            current.pop();
        }
    }
    let mut out = vec![];
    go(universe, 0, size, &mut vec![], &mut out);
    out
}

/// One fault set, over its schedules, by collection index.
#[derive(Default)]
struct Run {
    broken: BTreeSet<usize>,
    with_facts: BTreeSet<usize>,
    unserializable: BTreeSet<usize>,
    schedules: usize,
    /// The panic that ended the run, if it panicked.
    panic: Option<String>,
}

/// Node names for reports: `Type@member`, `Type` for a process.
struct Names(BTreeMap<LocationId, String>);

impl Names {
    fn new(locations: &[LocationInfo]) -> Self {
        Names(
            locations
                .iter()
                .map(|l| {
                    let name = l.name.split('<').next().unwrap_or(&l.name);
                    let name = name.rsplit("::").next().unwrap_or(name).trim();
                    let name = if name.is_empty() || name == "()" {
                        if l.members.is_some() {
                            "cluster"
                        } else {
                            "process"
                        }
                    } else {
                        name
                    };
                    (l.id.root().clone(), name.to_owned())
                })
                .collect(),
        )
    }

    fn node(&self, (l, m): &Node) -> String {
        let name = self.0.get(l).cloned().unwrap_or_else(|| format!("{l:?}"));
        match m {
            Some(m) => format!("{name}@{m}"),
            None => name,
        }
    }

    fn set(&self, set: &FaultSet) -> String {
        let faults: Vec<String> = set
            .iter()
            .map(|f| match f {
                Fault::Crash { node, after: None } => format!("crash({})", self.node(node)),
                Fault::Crash {
                    node,
                    after: Some(k),
                } => format!("crash({} after send #{k})", self.node(node)),
                Fault::Omit {
                    channel,
                    from,
                    to,
                    bytes,
                    copy,
                } => {
                    let hex: String = bytes.iter().take(8).map(|b| format!("{b:02x}")).collect();
                    format!(
                        "omit(c{channel} {}→{} {hex}{}{})",
                        self.node(from),
                        self.node(to),
                        if bytes.len() > 8 { "…" } else { "" },
                        if *copy > 0 { format!("#{copy}") } else { String::new() }
                    )
                }
            })
            .collect();
        format!("{{{}}}", faults.join(", "))
    }
}

impl EcCheck {
    /// Runs oracle 2 (see the [module docs](self)) with the harness (`inputs`, then `recover`),
    /// over every admissible set of at most `max_faults` faults, and maps its collections to
    /// `results`' labels.
    pub(super) fn oracle2<I: AsyncFnMut(), R: AsyncFnMut()>(
        &self,
        inputs: &mut I,
        recover: &mut R,
        max_faults: usize,
        results: &Results,
    ) -> Oracle2 {
        let start = Instant::now();
        let Some(program) = &self.oracle2 else {
            return Oracle2 {
                unavailable: Some("the flow did not pass through `EcCheck::instrument`".into()),
                ..Oracle2::default()
            };
        };
        if let Some(why) = &program.unavailable {
            return Oracle2 {
                unavailable: Some(why.clone()),
                ..Oracle2::default()
            };
        }
        refuse_truncated_exhaustive();
        let sim = program.compiled();
        let taps = program
            .taps
            .borrow()
            .clone()
            .expect("oracle 2's tap pass ran when its program was compiled");
        let names = Names::new(sim.locations());
        let members: BTreeMap<LocationId, BTreeSet<u32>> = sim
            .locations()
            .iter()
            .filter_map(|l| {
                l.members
                    .as_ref()
                    .map(|m| (l.id.root().clone(), m.iter().copied().collect()))
            })
            .collect();
        let nodes: Vec<Node> = sim
            .locations()
            .iter()
            .flat_map(|l| match &l.members {
                None => vec![(l.id.root().clone(), None)],
                Some(ms) => ms.iter().map(|m| (l.id.root().clone(), Some(*m))).collect(),
            })
            .collect();
        let bounds: Vec<Bound> = members
            .iter()
            .filter(|(_, m)| m.len() > 1)
            .map(|(cluster, m)| Bound {
                cluster: cluster.clone(),
                members: m.clone(),
                cap: self.max_crashes,
                nodes: nodes.len(),
            })
            .collect();
        let bound_of = |cluster: &LocationId| bounds.iter().find(|b| b.cluster == *cluster);
        let ports: BTreeMap<usize, LocationId> = taps
            .taps
            .iter()
            .map(|t| (t.port, t.cluster.clone()))
            .collect();
        let sample = self.sample_schedules;

        // One fault set over all its schedules.
        let mut execute = |faults: &FaultSet, universe: &mut BTreeSet<Fault>| -> Run {
            let hook = Rc::new(Hook::new(sim, ports.clone(), faults));
            sim.set_network_hook(hook.clone());
            let run = RefCell::new(Run::default());
            let revealed = RefCell::new(BTreeSet::new());
            let read = |records: Vec<Record>| {
                let mut run = run.borrow_mut();
                for record in records {
                    run.schedules += 1;
                    revealed
                        .borrow_mut()
                        .extend(record.revealed.iter().cloned());
                    for (i, tap) in taps.taps.iter().enumerate() {
                        if record.unserializable.contains(&tap.port) {
                            run.unserializable.insert(i);
                        }
                        let correct: Vec<u32> = members
                            .get(&tap.cluster)
                            .into_iter()
                            .flatten()
                            .copied()
                            .filter(|m| !record.crashed.contains(&(tap.cluster.clone(), Some(*m))))
                            .collect();
                        let empty = BTreeSet::new();
                        let sets: Vec<&BTreeSet<Vec<u8>>> = correct
                            .iter()
                            .map(|m| {
                                record
                                    .facts
                                    .get(&tap.port)
                                    .and_then(|by| by.get(m))
                                    .unwrap_or(&empty)
                            })
                            .collect();
                        if sets.iter().any(|s| !s.is_empty()) {
                            run.with_facts.insert(i);
                        }
                        if sets.windows(2).any(|w| w[0] != w[1]) {
                            run.broken.insert(i);
                        }
                    }
                }
            };
            let outcome = {
                let healer = AssertUnwindSafe(hook.clone());
                let mut inputs = AssertUnwindSafe(&mut *inputs);
                let mut recover = AssertUnwindSafe(&mut *recover);
                // The search's harness (`engine.rs` `drive`), restated: the fault phase with
                // barriers forbidden, quiesce, heal, quiesce, the recovery phase, quiesce.
                let thunk = async || {
                    forbid_barriers(BARRIER_IN_FAULT_PHASE, (*inputs)()).await;
                    quiesce().await;
                    healer.heal();
                    quiesce().await;
                    (*recover)().await;
                    quiesce().await;
                };
                let after = || read(hook.drain());
                std::panic::catch_unwind(AssertUnwindSafe(move || match sample {
                    Some(n) => sim.fuzz_bounded_then(n, thunk, after),
                    None => {
                        sim.exhaustive_then(thunk, after);
                    }
                }))
            };
            sim.clear_network_hook();
            read(hook.drain());
            let mut run = run.into_inner();
            if let Err(payload) = outcome {
                // A program that panics under injected faults breaks every collection.
                run.panic = Some(program_panic(&*payload));
                run.broken.extend(0..taps.taps.len());
            }
            universe.extend(revealed.into_inner());
            run
        };

        let mut universe: BTreeSet<Fault> = nodes
            .iter()
            .map(|n| Fault::Crash {
                node: n.clone(),
                after: None,
            })
            .collect();
        let probe = execute(&FaultSet::new(), &mut universe);
        let mut runs: Vec<(FaultSet, Run)> = vec![];
        let mut done: BTreeSet<FaultSet> = BTreeSet::from([FaultSet::new()]);
        let mut schedules = probe.schedules;
        let mut passes = 0;
        runs.push((FaultSet::new(), probe));
        loop {
            passes += 1;
            let candidates: Vec<Fault> = universe.iter().cloned().collect();
            let fresh: Vec<FaultSet> = (1..=max_faults)
                .flat_map(|size| subsets(&candidates, size))
                .filter(|set| !done.contains(set))
                .filter(|set| bounds.iter().any(|b| b.admits(set)))
                .collect();
            if fresh.is_empty() {
                break;
            }
            for set in fresh {
                done.insert(set.clone());
                let run = execute(&set, &mut universe);
                schedules += run.schedules;
                runs.push((set, run));
            }
        }

        let collections = taps
            .taps
            .iter()
            .enumerate()
            .map(|(i, tap)| {
                let labels: Vec<usize> = results
                    .results()
                    .iter()
                    .filter(|l| label_key(l) == tap.key)
                    .map(|l| l.index)
                    .collect();
                let bound = bound_of(&tap.cluster);
                let mut admissible = 0;
                let mut breaking = vec![];
                for (set, run) in &runs {
                    if !bound.is_some_and(|b| b.admits(set)) {
                        continue;
                    }
                    admissible += 1;
                    if run.broken.contains(&i) {
                        breaking.push((crashes(set), set.len(), set));
                    }
                }
                breaking.sort_by_key(|(c, n, _)| (*c, *n));
                Oracle2Collection {
                    name: format!("{} ({}, {}) [{}]", tap.key.0, tap.key.1, tap.key.2, tap.why),
                    labels,
                    key: tap.key.clone(),
                    members: members.get(&tap.cluster).map_or(0, BTreeSet::len),
                    failure_free_facts: runs[0].1.with_facts.contains(&i),
                    unserializable: runs.iter().any(|(_, r)| r.unserializable.contains(&i)),
                    admissible,
                    breaking: breaking
                        .into_iter()
                        .map(|(crashes, size, set)| Oracle2Breaking {
                            faults: names.set(set),
                            crashes,
                            size,
                        })
                        .collect(),
                }
            })
            .collect();
        let panic = runs
            .iter()
            .find_map(|(set, run)| Some((names.set(set), run.panic.clone()?)));
        Oracle2 {
            unavailable: None,
            panic,
            max_faults,
            sampled: sample.is_some(),
            collections,
            skipped: taps.skipped,
            universe: universe.len(),
            sets: runs.len(),
            schedules,
            passes,
            wall: start.elapsed(),
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Agreement
// ---------------------------------------------------------------------------------------------

/// A search label's `(label, operator, kind, cluster)` key, as [`key`] computes it for oracle
/// 2's nodes.
fn label_key(label: &LabelResult) -> Key {
    let c = &label.plan.collection;
    (
        c.label.clone(),
        c.op.clone(),
        kind_name(&c.collection_kind),
        c.location.root().clone(),
    )
}

/// The most same-key labels (or collections) [`disagreements`] matches by trying every
/// assignment.
const MAX_SAME_KEY: usize = 6;

/// Every way `results` and oracle 2 disagree (see the [module docs](self)), each prefixed
/// `oracle 2:`.
///
/// Oracle 2's program is a copy of the search's, so node addresses do not carry over: its
/// collections are matched to the search's labels by key. A key that names one node on each
/// side is an exact match. Several nodes with one key (a helper labelling on one line, called
/// twice) are matched as a group: the group agrees if some one-to-one assignment of its
/// collections to its labels has no disagreement, and otherwise reports the assignment with the
/// fewest. A side with fewer nodes leaves some of the other side's unassigned (a label without
/// a collection is not compared, as before). An oracle 2 that could not run is a disagreement.
pub(super) fn disagreements(results: &Results, oracle: &Oracle2) -> Vec<String> {
    // A cross-check that silently lost an oracle would look like agreement.
    if let Some(why) = &oracle.unavailable {
        return vec![format!("oracle 2: could not run: {why}")];
    }
    let mut groups: BTreeMap<String, (Vec<&LabelResult>, Vec<&Oracle2Collection>)> =
        BTreeMap::new();
    let group_of = |key: &Key| format!("{key:?}");
    for label in results.results() {
        groups
            .entry(group_of(&label_key(label)))
            .or_default()
            .0
            .push(label);
    }
    for c in oracle.collections.iter().filter(|c| c.compared()) {
        groups.entry(group_of(&c.key)).or_default().1.push(c);
    }
    let mut out = vec![];
    for (labels, collections) in groups.values() {
        if collections.is_empty() {
            continue;
        }
        let n = labels.len().max(collections.len());
        if n > MAX_SAME_KEY {
            out.push(format!(
                "oracle 2: {}: {} labels and {} collections share one key, too many to match",
                label_name(&labels[0].plan),
                labels.len(),
                collections.len()
            ));
            continue;
        }
        // Assign `slots[i]` (a collection, or none) to the `i`-th label, over every permutation.
        let mut slots: Vec<Option<&Oracle2Collection>> =
            collections.iter().copied().map(Some).collect();
        slots.resize(n, None);
        let mut best: Option<Vec<String>> = None;
        for_each_permutation(&mut slots, &mut |slots| {
            if best.as_ref().is_some_and(Vec::is_empty) {
                return;
            }
            let found: Vec<String> = labels
                .iter()
                .zip(slots.iter())
                .filter_map(|(l, c)| Some(label_disagreements(results, oracle, l, (*c)?)))
                .flatten()
                .collect();
            if best.as_ref().is_none_or(|b| found.len() < b.len()) {
                best = Some(found);
            }
        });
        let ambiguous = n > 1;
        out.extend(best.into_iter().flatten().map(|d| {
            if ambiguous {
                format!("{d} (under every matching of the {n} same-key nodes)")
            } else {
                d
            }
        }));
    }
    out
}

/// Calls `f` on every permutation of `items` (Heap's algorithm).
fn for_each_permutation<T>(items: &mut [T], f: &mut impl FnMut(&[T])) {
    fn go<T>(k: usize, items: &mut [T], f: &mut impl FnMut(&[T])) {
        if k <= 1 {
            f(items);
            return;
        }
        for i in 0..k {
            go(k - 1, items, f);
            if i + 1 < k {
                items.swap(if k.is_multiple_of(2) { i } else { 0 }, k - 1);
            }
        }
    }
    go(items.len(), items, f);
}

/// The disagreements of the search's `label` with oracle 2's `collection`.
fn label_disagreements(
    results: &Results,
    oracle: &Oracle2,
    label: &LabelResult,
    collection: &Oracle2Collection,
) -> Vec<String> {
    let report = results.report_names();
    let mut out = vec![];
    {
        let name = label_name(&label.plan);
        let mut push = |problem: String| out.push(format!("oracle 2: {name}: {problem}"));
        // Oracle 2's breaking set with the fewest crashes, then faults, within the limits.
        let breaking_within = |crashes: usize, size: usize| {
            collection
                .breaking
                .iter()
                .filter(|b| b.crashes <= crashes && b.size <= size)
                .min_by_key(|b| (b.crashes, b.size))
                .map(|b| (collection, b))
        };
        let with_facts = collection.failure_free_facts;
        // Every label with a failure-free probe: the search's domain is empty exactly when
        // oracle 2's correct members emitted nothing failure-free.
        let probed = matches!(
            label.verdict,
            Verdict::Vacuous(VacuousReason::EmptyDomain)
                | Verdict::Certified { .. }
                | Verdict::Counterexample(_)
                | Verdict::Budget { .. }
                | Verdict::Exhausted { .. }
        );
        if !probed {
            return out;
        }
        match (label.stats.domain > 0, with_facts) {
            (false, true) => push(
                "the search's failure-free run held no facts, but oracle 2's correct members \
                 emitted some"
                    .to_owned(),
            ),
            (true, false) => push(format!(
                "the search's failure-free run held {} fact(s), but oracle 2's correct members \
                 emitted none",
                label.stats.domain
            )),
            _ => {}
        }
        // A claim that the label holds with up to `k` crashes, and whether it is a sample's.
        let claim: Option<(usize, String, bool)> = match &label.verdict {
            Verdict::Certified {
                tolerates: Tolerance::All,
            } => Some((
                usize::MAX,
                "holds under every admissible fault set".into(),
                false,
            )),
            // An empty domain with no search: vacuous, so it holds under every set.
            Verdict::Vacuous(_) => Some((usize::MAX, "is vacuous".into(), false)),
            Verdict::Certified {
                tolerates: Tolerance::UpTo(k),
            } => Some((*k, format!("holds with up to {k} crash(es)"), false)),
            Verdict::Budget { tolerates: Some(t) } => Some((
                *t,
                format!("holds with up to {t} crash(es) (budget)"),
                false,
            )),
            Verdict::Exhausted {
                tolerates: Tolerance::All,
                ..
            } => Some((usize::MAX, "found no counterexample".into(), true)),
            Verdict::Exhausted {
                tolerates: Tolerance::UpTo(k),
                ..
            } => Some((
                *k,
                format!("found no counterexample with up to {k} crash(es)"),
                true,
            )),
            Verdict::Counterexample(cex) => cex.tolerates.map(|t| {
                (
                    t,
                    format!("tolerates {t} crash(es)"),
                    !cex.tolerance_certified,
                )
            }),
            _ => None,
        };
        if let Some((k, claim, sampled)) = claim
            && let Some((c, b)) = breaking_within(k, usize::MAX)
        {
            push(format!(
                "the search says it {claim}, but {} ({} crash(es)) breaks its collection {}{}",
                b.faults,
                b.crashes,
                c.name,
                if sampled {
                    " (schedules were sampled: a miss of the sample, not a false certificate)"
                } else {
                    ""
                }
            ));
        }
        if let Verdict::Counterexample(cex) = &label.verdict
            && !oracle.sampled
            && cex.faults.len() <= oracle.max_faults
            && breaking_within(cex.crashes(), cex.faults.len()).is_none()
        {
            push(format!(
                "the search's counterexample {} ({} fault(s), {} crash(es)) is within oracle 2's \
                 sizes, but no set of oracle 2 with at most as many faults and crashes breaks any \
                 of the label's collections",
                report.fault_set(&cex.faults),
                cex.faults.len(),
                cex.crashes()
            ));
        }
    }
    out
}
