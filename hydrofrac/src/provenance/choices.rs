//! The program's schedule choices (docs/design/hydrofrac-lite.md §4.9).
//!
//! Molly's strong pruning rule is sound when each fault set has one execution. The simulator is
//! deterministic except at a schedule choice, so a label is certified only if
//! [`program_choices`] finds none anywhere in the program. A node is choice-free only if its
//! kind is on the [`ALLOWED`] list. Anything else is a choice, including an IR kind added later.

use std::collections::HashSet;
use std::fmt::{self, Display};

use hydro_lang::compile::ir::{
    ClosureExpr, CollectionKind, HydroNode, HydroRoot, HydroSource, StreamOrder, StreamRetry,
};
use hydro_lang::networking::{NetworkingInfo, TcpFault};

use crate::observe::ec_label;

/// The IR node kinds that make no schedule choice, by operator name. [`choice`] still rejects
/// some of their instances: an unordered fold, an observation or a lossy ordered network.
#[rustfmt::skip]
pub const ALLOWED: &[&str] = &[
    "AntiJoin", "AssertIsConsistent", "Cast", "Chain", "ChainFirst", "Counter", "CrossProduct",
    "CrossSingleton", "CycleSource", "DeferTick", "Difference", "EndAtomic", "Enumerate",
    "ExternalInput", "Filter", "FilterMap", "FlatMap", "Fold", "FoldKeyed", "Inspect", "Join",
    "JoinHalf", "Map", "Network", "ObserveNonDet", "PartitionShared", "PartitionSide", "Reduce",
    "ReduceKeyed", "Reference", "Scan", "SingletonSource", "Sort", "Tee", "UnboundSingleton",
    "Unique", "YieldConcat",
];

/// One schedule choice: what it is (`batch`, `fold_unordered`, ...) and where (the operator's
/// tag, else its user source line `file:line`, else its name).
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ChoiceSite {
    pub op: String,
    pub site: String,
}

impl Display for ChoiceSite {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} at {}", self.op, self.site)
    }
}

fn unordered(kind: &CollectionKind) -> bool {
    use CollectionKind::{KeyedStream, Stream};
    let no = |order: &StreamOrder| *order == StreamOrder::NoOrder;
    matches!(kind, Stream { order, .. } | KeyedStream { value_order: order, .. } if no(order))
}

/// The closures of the allowlisted nodes that have any.
fn closures(node: &HydroNode) -> Vec<&ClosureExpr> {
    use HydroNode::*;
    match node {
        Map { f, .. } | FlatMap { f, .. } | Filter { f, .. } | FilterMap { f, .. } => vec![f],
        Inspect { f, .. } | Reduce { f, .. } | ReduceKeyed { f, .. } => vec![f],
        PartitionShared { f, .. } => vec![f],
        Fold { init, acc, .. } | FoldKeyed { init, acc, .. } => vec![init, acc],
        Scan { init, acc, .. } => vec![init, acc],
        _ => vec![],
    }
}

/// The state cell a captured reference shares, or `None` if it does not resolve.
pub(crate) fn cell(reference: &HydroNode) -> Option<*const HydroNode> {
    let HydroNode::Reference { inner, .. } = reference else {
        return None;
    };
    Some(std::ptr::from_ref(&*inner.0.borrow()))
}

/// The choice `node` makes, by name, or `None` if it makes none.
pub fn choice(node: &HydroNode) -> Option<String> {
    use HydroNode::*;
    if let Placeholder = node {
        return Some("unknown_operator".to_owned());
    }
    let kind = |n: &HydroNode| n.metadata().collection_kind.clone();
    let external = |n: &HydroNode| match n {
        Source { source, .. } => matches!(source, HydroSource::ExternalNetwork()),
        _ => matches!(n, ExternalInput { .. }),
    };
    // The simulator observes a closure's mutable reference over a non-strict input.
    let observed = closures(node).iter().any(|f| f.has_mut_ref())
        && node.input().first().is_some_and(|i| !kind(i).is_strict());
    // A retry cast alone (`AtLeastOnce` to `ExactlyOnce`) is the identity in the simulator.
    let retry_cast = |inner: &HydroNode| {
        let (mut kind, once) = (kind(inner), StreamRetry::AtLeastOnce);
        match &mut kind {
            CollectionKind::Stream { retry: r, .. } if *r == once => *r = StreamRetry::ExactlyOnce,
            CollectionKind::KeyedStream { value_retry: r, .. } if *r == once => {
                *r = StreamRetry::ExactlyOnce;
            }
            _ => return false,
        }
        kind == node.metadata().collection_kind
    };
    let name = node.print_root();
    let name = name.split('(').next().unwrap_or_default();
    let lossy = NetworkingInfo::Tcp {
        fault: TcpFault::Lossy,
    };
    let op = match node {
        // Ordered and lossy: an omission is not a suffix of the sends, so a positional operator
        // downstream can create a record under a fault, which lineage cannot see.
        Network {
            networking_info, ..
        } if *networking_info == lossy => "ordered_lossy_network",
        // An external input without an order: a deployment delivers it in any order.
        _ if external(node) && unordered(&kind(node)) => "unordered_external_input",
        Cast { inner, .. } | ObserveNonDet { inner, .. }
            if external(inner) && unordered(&kind(node)) =>
        {
            "unordered_external_input"
        }
        Source { source, .. } => match source {
            HydroSource::Iter(_) | HydroSource::ClusterMembers(..) => return None,
            HydroSource::ExternalNetwork() => return None,
            _ => "source",
        },
        ObserveNonDet { inner, trusted, .. } if !*trusted && !retry_cast(inner) => "observe_nondet",
        Fold { input, .. }
        | FoldKeyed { input, .. }
        | Scan { input, .. }
        | Reduce { input, .. }
        | ReduceKeyed { input, .. }
            if unordered(&kind(input)) =>
        {
            "fold_unordered"
        }
        _ if observed => "observe_for_mut",
        _ if ALLOWED.contains(&name) => return None,
        _ => name,
    };
    // `Batch` is `batch`, `ReduceKeyedWatermark` is `reduce_keyed_watermark`.
    let mut snake = String::new();
    for (i, c) in op.char_indices() {
        if c.is_uppercase() && i > 0 {
            snake.push('_');
        }
        snake.extend(c.to_lowercase());
    }
    Some(snake)
}

/// Every schedule choice in `ir`, the whole program's IR (see the [module docs](self)),
/// deduplicated, in the order the scan meets them. Empty iff the labels can be certified.
pub fn program_choices(ir: &[HydroRoot]) -> Vec<ChoiceSite> {
    let mut out: Vec<ChoiceSite> = vec![];
    let mut add = |op: &str, site: String| {
        let choice = ChoiceSite {
            op: op.to_owned(),
            site,
        };
        if !out.contains(&choice) {
            out.push(choice);
        }
    };
    let (mut seen, mut stack) = (HashSet::new(), vec![]);
    // Last in, first out: pushed in reverse, so the scan meets the roots in program order.
    for root in ir.iter().rev() {
        stack.push(std::ptr::from_ref(root.input()));
        let HydroRoot::ForEach { f, .. } = root else {
            continue;
        };
        for (reference, _) in f.singleton_refs() {
            match cell(reference) {
                Some(cell) => stack.push(cell),
                None => add("unresolved_state", "for_each".to_owned()),
            }
        }
    }
    while let Some(ptr) = stack.pop() {
        if !seen.insert(ptr) {
            continue;
        }
        // SAFETY: every pointer points into `ir`, which is borrowed for the whole scan.
        let node = unsafe { &*ptr };
        if let HydroNode::Placeholder = node {
            add("unknown_operator", "Placeholder".to_owned());
            continue;
        }
        if let Some(op) = choice(node) {
            add(&op, ec_label(node));
        }
        for (reference, _) in closures(node).iter().flat_map(|f| f.singleton_refs()) {
            match cell(reference) {
                Some(cell) => stack.push(cell),
                None => add("unresolved_state", ec_label(node)),
            }
        }
        match node {
            HydroNode::Tee { inner, .. } | HydroNode::PartitionSide { inner, .. } => {
                stack.push(std::ptr::from_ref(&*inner.0.borrow()));
            }
            HydroNode::Reference { .. } => stack.extend(cell(node)),
            _ => stack.extend(node.input().into_iter().map(std::ptr::from_ref)),
        }
    }
    out
}
