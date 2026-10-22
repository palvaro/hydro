//! Which computations are **order-insensitive**: a static check over the IR for the
//! non-deterministic operators (the ones whose API takes a `nondet!` guard) upstream of a
//! collection.
//!
//! Hydro's safe operators are deterministic: an unbounded collection computed through them
//! reaches the same final contents whatever the network's delivery order, batching and
//! interleaving (an unordered stream's consumers are commutative, by type or by proof), given
//! the same inputs. Only the operators that take a non-determinism guard escape this: a
//! `batch` / `snapshot` (which tick an element lands in), an `assume_ordering` /
//! `assume_retries` (an order or a retry count the types cannot vouch for), a `merge_ordered`
//! (an interleaving), and the timer sources of `sample_every` and friends. If none of them
//! lies anywhere in the **causal cone** of a collection, across every location (following each
//! network channel back to its sender, each `forward_ref` back to its sink, each captured
//! state cell back to its writers), then the collection's final contents at each member are a
//! function of the inputs that reached the cone alone.
//!
//! Under a fault set made of omissions named by message identity and crashes named by the set
//! of messages the node got out ([`CrashModel::SentSet`](super::ldfi::CrashModel::SentSet)),
//! every send's fate is a function of the send's identity, so what each node receives is a
//! function of what its peers sent, and by induction over the cone every node's state, and
//! the checked collection, ends the same in every schedule. One schedule per hypothesis then
//! decides the hypothesis (see [`Ldfi::schedule_reduction`](super::ldfi::Ldfi::schedule_reduction)).
//!
//! The check is conservative: a `batch` whose consumer is in fact insensitive to the batch
//! boundaries (persisted state, a threshold over a set) is still a non-deterministic site,
//! because nothing in the IR says so; its `nondet!` justification is prose. A search may name
//! such sites as trusted ([`Ldfi::trust_order_insensitive`](super::ldfi::Ldfi::trust_order_insensitive)),
//! which is then an assumption the search reports, not something it proved.

use std::collections::{HashMap, HashSet};
use std::fmt::{self, Display};

use crate::compile::builder::CycleId;
use crate::compile::ir::{HydroNode, HydroRoot, HydroSource};
use crate::location::dynamic::LocationId;
use crate::sim::slice::{captures, closures, user_span};

/// One non-deterministic operator in the causal cone of a collection.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NondetSite {
    /// The user's source line of the operator (`file:line`, the file by its name), else its
    /// tag, else the operator's name.
    pub site: String,
    /// What the operator is (`batch`, `assume_ordering`, ...).
    pub op: String,
    /// The location the operator runs at (its root: the process or cluster).
    pub location: LocationId,
}

impl Display for NondetSite {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} at {} ({:?})", self.op, self.site, self.location)
    }
}

/// What a node contributes as a non-deterministic operator, if it is one.
fn nondet_op(node: &HydroNode) -> Option<&'static str> {
    match node {
        HydroNode::Batch { .. } => Some("batch"),
        HydroNode::ObserveNonDet { trusted: false, .. } => Some("assume_ordering/assume_retries"),
        HydroNode::MergeOrdered { .. } => Some("merge_ordered"),
        HydroNode::Source { source, .. } => match source {
            HydroSource::Spin() => Some("spin"),
            HydroSource::Stream(expr) => {
                let text = format!("{expr:?}");
                (text.contains("interval") || text.contains("Instant")).then_some("timer source")
            }
            _ => None,
        },
        _ => None,
    }
}

fn site_of(node: &HydroNode) -> String {
    let metadata = node.metadata();
    user_span(&metadata.op.backtrace)
        .map(|span| {
            span.rsplit(['/', '\\'])
                .next()
                .unwrap_or(span.as_str())
                .to_owned()
        })
        .or_else(|| metadata.tag.clone())
        .unwrap_or_else(|| {
            let name = node.print_root();
            name.split('(').next().unwrap_or(&name).to_owned()
        })
}

/// The causal-cone walker over one IR (see the module docs).
pub(crate) struct Cones {
    /// Each `forward_ref`'s sink input.
    cycle_sinks: HashMap<CycleId, *const HydroNode>,
    /// Per state cell (by address of the referenced node), the nodes whose closures capture
    /// it mutably: its writers.
    writers: HashMap<*const HydroNode, Vec<*const HydroNode>>,
}

impl Cones {
    /// Indexes `ir`'s cycles and state writers. The IR must not move or change while the
    /// returned walker is used: it holds raw pointers into it.
    pub(crate) fn new(ir: &[HydroRoot]) -> Self {
        let mut cones = Cones {
            cycle_sinks: HashMap::new(),
            writers: HashMap::new(),
        };
        for root in ir {
            if let HydroRoot::CycleSink {
                cycle_id, input, ..
            } = root
            {
                cones
                    .cycle_sinks
                    .insert(*cycle_id, std::ptr::from_ref(&**input));
            }
        }
        let mut seen: HashSet<*const HydroNode> = HashSet::new();
        fn index(
            node: &HydroNode,
            seen: &mut HashSet<*const HydroNode>,
            writers: &mut HashMap<*const HydroNode, Vec<*const HydroNode>>,
        ) {
            if !seen.insert(std::ptr::from_ref(node)) {
                return;
            }
            for closure in closures(node) {
                for (cell, is_mut, _) in captures(closure) {
                    if is_mut {
                        writers
                            .entry(cell)
                            .or_default()
                            .push(std::ptr::from_ref(node));
                    }
                }
            }
            for input in shared_inputs(node) {
                // SAFETY: `input` points into the IR, which outlives this call.
                index(unsafe { &*input }, seen, writers);
            }
        }
        for root in ir {
            index(root.input(), &mut seen, &mut cones.writers);
        }
        cones
    }

    /// The non-deterministic operators in the causal cone of `starts`, in discovery order.
    pub(crate) fn nondet_upstream(&self, starts: &[*const HydroNode]) -> Vec<NondetSite> {
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
                let site = NondetSite {
                    site: site_of(node),
                    op: op.to_owned(),
                    location: node.metadata().location_id.root().clone(),
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
                for (cell, _, _) in captures(closure) {
                    stack.push(cell);
                    stack.extend(self.writers.get(&cell).into_iter().flatten().copied());
                }
            }
        }
        found
    }

    /// The non-deterministic operators upstream of everything sent **to** each location: per
    /// root location `l`, the cone of every `Network` node whose destination is `l`. For a
    /// specification's observer, the cone of what it observes.
    pub(crate) fn nondet_by_destination(
        &self,
        ir: &[HydroRoot],
    ) -> HashMap<LocationId, Vec<NondetSite>> {
        let mut networks: HashMap<LocationId, Vec<*const HydroNode>> = HashMap::new();
        let mut seen: HashSet<*const HydroNode> = HashSet::new();
        fn collect(
            node: &HydroNode,
            seen: &mut HashSet<*const HydroNode>,
            networks: &mut HashMap<LocationId, Vec<*const HydroNode>>,
        ) {
            if !seen.insert(std::ptr::from_ref(node)) {
                return;
            }
            if let HydroNode::Network { .. } = node {
                networks
                    .entry(node.metadata().location_id.root().clone())
                    .or_default()
                    .push(std::ptr::from_ref(node));
            }
            for input in shared_inputs(node) {
                // SAFETY: `input` points into the IR, which outlives this call.
                collect(unsafe { &*input }, seen, networks);
            }
        }
        for root in ir {
            collect(root.input(), &mut seen, &mut networks);
        }
        networks
            .into_iter()
            .map(|(location, starts)| (location, self.nondet_upstream(&starts)))
            .collect()
    }
}

/// The direct inputs of `node`, through what a `Tee`, reference, partition side or versioned
/// network shares.
fn shared_inputs(node: &HydroNode) -> Vec<*const HydroNode> {
    match node {
        HydroNode::Tee { inner, .. }
        | HydroNode::Reference { inner, .. }
        | HydroNode::PartitionSide { inner, .. } => vec![std::ptr::from_ref(&*inner.0.borrow())],
        HydroNode::VersionedNetwork { fork, .. } => vec![std::ptr::from_ref(&*fork.0.borrow())],
        _ => node.input().into_iter().map(std::ptr::from_ref).collect(),
    }
}
