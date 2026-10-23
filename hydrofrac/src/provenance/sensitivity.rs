//! Schedule sensitivity of labelled collections: a static diagnostic, not used by the check.
//!
//! The search prunes a hypothesis when some derivation seen in an earlier run survives it:
//! the derivation's deliveries all still happen, so (the reasoning goes) the fact is still
//! derived. That holds only if whether a record exists is a function of the deliveries it was
//! computed from. Hydro's safe operators guarantee this (an unbounded collection computed
//! through them reaches the same final contents in every schedule, given the same inputs), but
//! the operators that take a `nondet!` guard do not: a `batch` / `snapshot` (which tick an
//! element lands in, and so which state it meets), an `assume_ordering` / `assume_retries` (an
//! `ObserveNonDet` the simulator explores), a `merge_ordered` (an interleaving), and timer
//! sources. A derivation through one of them is a derivation *under the schedule that produced
//! it*.
//!
//! [`label_cones`] lists, for every labelled collection of a program, the non-deterministic
//! operators in its **causal cone**: everything upstream of it, across locations (a network
//! back to its sender), through cycles (a `forward_ref` back to its sink) and through captured
//! state (a `by_ref`/`by_mut` cell back to its producer and to every closure that writes it).
//! A label with an empty cone list is one whose derivations are schedule-independent at the
//! operator level. The walk follows the research branch's order check
//! (`hydro_lang/src/sim/order.rs` at `0d801bbafc`, `Cones::nondet_upstream`).
//!
//! This is the operator half of the question only. The positional crash model adds a second
//! dependence on the schedule: `crash(n after m)` cuts `n`'s sends at `m`, and which sends come
//! before `m` can differ between schedules even in a cone with no non-deterministic operator.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::{self, Display};

use hydro_lang::compile::builder::CycleId;
use hydro_lang::compile::ir::backtrace::Backtrace;
use hydro_lang::compile::ir::{ClosureExpr, HydroNode, HydroRoot, HydroSource};
use hydro_lang::location::dynamic::{ClusterConsistency, LocationId};

use crate::observe::{EcCollection, ec_collections, kind_name};

/// One non-deterministic operator in the causal cone of a labelled collection.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NondetSite {
    /// The user's source line of the operator (`file:line`, the file by its name), else its
    /// tag, else the operator's name.
    pub site: String,
    /// What the operator is (`batch`, `observe_nondet`, ...).
    pub op: &'static str,
    /// The kind of collection the operator produces (`Stream`, `Singleton`, ...).
    pub kind: &'static str,
    /// The location the operator runs at (its root: the process or cluster).
    pub location: LocationId,
    /// Whether the operator runs at the labelled collection's own location (else it is upstream
    /// across a network).
    pub local: bool,
}

impl Display for NondetSite {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}<{}> at {}{}",
            self.op,
            self.kind,
            self.site,
            if self.local { "" } else { " (upstream)" }
        )
    }
}

/// The causal cone of one labelled collection.
#[derive(Clone, Debug)]
pub struct LabelCone {
    /// The labelled collection, as [`ec_collections`] lists it.
    pub collection: EcCollection,
    /// The non-deterministic operators in its cone, deduplicated by site, in discovery order.
    pub sites: Vec<NondetSite>,
    /// The IR operators in the cone (for scale: how much of the program feeds the label).
    pub cone_nodes: usize,
}

impl LabelCone {
    /// Whether some derivation of the collection may depend on the schedule at the operator
    /// level (see the [module docs](self)).
    pub fn schedule_sensitive(&self) -> bool {
        !self.sites.is_empty()
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

/// What a node contributes as a non-deterministic operator, if it is one.
pub fn nondet_op(node: &HydroNode) -> Option<&'static str> {
    match node {
        HydroNode::Batch { .. } => Some("batch"),
        HydroNode::ObserveNonDet { trusted: false, .. } => Some("observe_nondet"),
        HydroNode::MergeOrdered { .. } => Some("merge_ordered"),
        // Garbage collection by watermark: what survives depends on when the watermark arrives
        // relative to the values.
        HydroNode::ReduceKeyedWatermark { .. } => Some("reduce_keyed_watermark"),
        HydroNode::Source { source, .. } => match source {
            HydroSource::Spin() => Some("spin"),
            HydroSource::Interval(_) => Some("timer"),
            HydroSource::Stream(expr) => {
                let text = format!("{expr:?}");
                (text.contains("interval") || text.contains("Instant")).then_some("timer")
            }
            _ => None,
        },
        _ => None,
    }
}

/// The closures of a node (those that may capture state).
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
        _ => vec![],
    }
}

/// The state cells a closure captures: `(cell, is_mut)`, the cell by the address of the node
/// the reference shares.
fn captures(f: &ClosureExpr) -> Vec<(*const HydroNode, bool)> {
    f.singleton_refs()
        .iter()
        .filter_map(|(node, is_mut)| match node {
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
}

impl Cones {
    /// Indexes `ir`. The IR must not move or change while the walker is used: it holds raw
    /// pointers into it.
    fn new(ir: &[HydroRoot]) -> Self {
        let mut cones = Cones {
            cycle_sinks: HashMap::new(),
            writers: HashMap::new(),
            labelled: HashMap::new(),
        };
        let mut seen: HashSet<*const HydroNode> = HashSet::new();
        fn index(node: &HydroNode, seen: &mut HashSet<*const HydroNode>, cones: &mut Cones) {
            if !seen.insert(std::ptr::from_ref(node)) {
                return;
            }
            if is_ec(node) {
                cones
                    .labelled
                    .entry(label_key(node))
                    .or_default()
                    .push(std::ptr::from_ref(node));
            }
            for closure in closures(node) {
                for (cell, is_mut) in captures(closure) {
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
                    for (cell, is_mut) in captures(f) {
                        if is_mut {
                            cones
                                .writers
                                .entry(cell)
                                .or_default()
                                .push(std::ptr::from_ref(&**input));
                        }
                    }
                }
                _ => {}
            }
            index(root.input(), &mut seen, &mut cones);
        }
        cones
    }

    /// The non-deterministic operators in the causal cone of `starts`, and the cone's size.
    fn nondet_upstream(
        &self,
        starts: &[*const HydroNode],
        at: &LocationId,
    ) -> (Vec<NondetSite>, usize) {
        let mut seen: HashSet<*const HydroNode> = HashSet::new();
        let mut found: Vec<NondetSite> = vec![];
        let mut stack: Vec<*const HydroNode> = starts.to_vec();
        while let Some(ptr) = stack.pop() {
            if !seen.insert(ptr) {
                continue;
            }
            // SAFETY: every pointer points into the IR `self` was built from, which does not
            // move or change while `self` is used.
            let node = unsafe { &*ptr };
            if let Some(op) = nondet_op(node) {
                let location = node.metadata().location_id.root().clone();
                let site = NondetSite {
                    site: site_of(node),
                    op,
                    kind: kind_name(&node.metadata().collection_kind),
                    local: &location == at,
                    location,
                };
                if !found.contains(&site) {
                    found.push(site);
                }
            }
            stack.extend(shared_inputs(node));
            if let HydroNode::CycleSource { cycle_id, .. } = node
                && let Some(&sink) = self.cycle_sinks.get(cycle_id)
            {
                stack.push(sink);
            }
            for closure in closures(node) {
                for (cell, _) in captures(closure) {
                    stack.push(cell);
                    stack.extend(self.writers.get(&cell).into_iter().flatten().copied());
                }
            }
        }
        (found, seen.len())
    }
}

/// For every collection `ir` labels `EventualConsistency` (as [`ec_collections`] lists them),
/// the non-deterministic operators in its causal cone (see the [module docs](self)).
pub fn label_cones(ir: &[HydroRoot]) -> Vec<LabelCone> {
    let cones = Cones::new(ir);
    ec_collections(ir)
        .into_iter()
        .map(|collection| {
            let key = (
                collection.label.clone(),
                collection.op.clone(),
                kind_name(&collection.collection_kind),
                collection.location.clone(),
            );
            let starts = cones.labelled.get(&key).cloned().unwrap_or_default();
            assert!(!starts.is_empty(), "no labelled node for {key:?}");
            let (sites, cone_nodes) = cones.nondet_upstream(&starts, &collection.location);
            LabelCone {
                collection,
                sites,
                cone_nodes,
            }
        })
        .collect()
}

/// A table of [`label_cones`] for a report: one line per label.
pub fn table(program: &str, cones: &[LabelCone]) -> String {
    let mut out = String::new();
    for cone in cones {
        let c = &cone.collection;
        out.push_str(&format!(
            "{program}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            c.display_name(),
            c.op,
            kind_name(&c.collection_kind),
            if cone.schedule_sensitive() {
                "yes"
            } else {
                "no"
            },
            cone.summary(),
            cone.sites
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("; "),
        ));
    }
    out
}
