//! The vocabulary of the search (docs/design/hydrofrac-races.md §1), shared by the races search
//! ([`crate::races`]), the engine and the brute-force oracle: faults and fault sets, the
//! deliveries a derivation depends on, crash bounds, and what a certificate tolerates. The
//! search itself is [`crate::races`].

use std::collections::BTreeSet;
use std::fmt::{self, Display};

/// A node of the program: a process (`label`) or a cluster member (`label@member`).
pub type Node = String;

/// One fault. The derived order is only for sets; `races::candidates` orders candidates.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum Fault<M> {
    /// Discard every copy of this message on its channel (lossy channels only).
    Omit(M),
    /// The node dies at the point `at` names, and stays dead for the rest of the run.
    Crash {
        /// The crashed node.
        node: Node,
        /// Where it dies.
        at: CrashAt<M>,
    },
}

/// Where a crashed node dies: Molly's positional crash, `crash(node after m)` (it sends up to
/// and including `m`). Named by a message, not a count, so it means the same in every run.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum CrashAt<M> {
    /// Before its first send.
    BeforeAnySend,
    /// Right after it sends this message.
    After(M),
}

impl<M> Fault<M> {
    /// `crash(node after m)`, or `crash(node)` before any send.
    pub fn crash_after(node: impl Into<Node>, after: Option<M>) -> Self {
        Fault::Crash {
            node: node.into(),
            at: match after {
                None => CrashAt::BeforeAnySend,
                Some(m) => CrashAt::After(m),
            },
        }
    }

    /// The node this fault crashes, if it is a crash.
    pub fn crashed_node(&self) -> Option<&Node> {
        match self {
            Fault::Crash { node, .. } => Some(node),
            Fault::Omit(_) => None,
        }
    }
}

impl<M: Display> Display for Fault<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Fault::Omit(m) => write!(f, "omit({m})"),
            Fault::Crash {
                node,
                at: CrashAt::BeforeAnySend,
            } => write!(f, "crash({node})"),
            Fault::Crash {
                node,
                at: CrashAt::After(m),
            } => write!(f, "crash({node} after {m})"),
        }
    }
}

/// A set of faults: one hypothesis.
pub type FaultSet<M> = BTreeSet<Fault<M>>;

/// The number of crashes in `set`.
pub fn crash_count<M>(set: &FaultSet<M>) -> usize {
    set.iter().filter(|f| f.crashed_node().is_some()).count()
}

/// One delivered message a derivation depends on, with what is known about its origin.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct Dependency<M> {
    /// The message.
    pub message: M,
    /// The sender (if it can crash), and the message's position among its sends in the run the
    /// derivation was read from. The position is only a tie-break: another schedule may order
    /// the sends differently, so no kill is ever decided by it (docs/design/hydrofrac-races.md
    /// §4.2).
    pub sender: Option<(Node, u64)>,
    /// Whether the channel admits omission.
    pub lossy: bool,
}

/// A derivation of a fact: the deliveries it was computed from.
pub type Derivation<M> = BTreeSet<Dependency<M>>;

/// The bounds on the crashes of one hypothesis (docs/design/hydrofrac-lite.md §4.5).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CrashBounds {
    /// The `max_crashes` cap; `None` is unbounded.
    pub cap: Option<usize>,
    /// The number of nodes: at most `nodes - 1` crash (`0`: no such bound, for tests).
    pub nodes: usize,
    /// The checked cluster, if the label lives on one.
    pub checked: Option<CheckedCluster>,
}

/// The members of the checked cluster, and how many of them one hypothesis may crash.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckedCluster {
    pub nodes: BTreeSet<Node>,
    pub cap: usize,
}

impl CheckedCluster {
    /// The cap is `|members| − 2`: a single survivor agrees with itself.
    pub fn new(members: impl IntoIterator<Item = Node>) -> Self {
        let nodes: BTreeSet<Node> = members.into_iter().collect();
        let cap = nodes.len().saturating_sub(2);
        CheckedCluster { nodes, cap }
    }
}

impl CrashBounds {
    /// The most crashes any hypothesis may hold: the cap and the every-node bound.
    pub fn max_crashes(&self) -> Option<usize> {
        let all_nodes = (self.nodes >= 1).then(|| self.nodes - 1);
        match (self.cap, all_nodes) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    /// These bounds with the cap lowered to `crashes`.
    pub fn capped_at(&self, crashes: usize) -> Self {
        CrashBounds {
            cap: Some(self.cap.map_or(crashes, |cap| cap.min(crashes))),
            ..self.clone()
        }
    }
}

/// The weight of each candidate in a universe of `n`, by position: `1` plus a tie-break that
/// prefers earlier (weaker) candidates. Each tie-break is below `0.5 / n`, so it never trades
/// cardinality (the research branch's `0.5 / n` per position did, for sets of size ≥ 3; §4.4).
pub fn weights(n: usize) -> impl Iterator<Item = f64> {
    let tie_break = 0.5 / (n as f64 * n as f64);
    (0..n).map(move |i| 1.0 + tie_break * i as f64)
}

/// The hypothesis budget, counting the failure-free probe.
pub const HYPOTHESIS_BUDGET: usize = 1024;

/// How many crashes a certified label tolerates.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Tolerance {
    /// Every admissible fault set (the vacuity bounds, or saturation, stopped the sweep).
    All,
    /// Every fault set with at most this many crashes (the `max_crashes` cap stopped it).
    UpTo(usize),
}

// Runtime-only: the staged copy of this crate (compiled into simulation dylibs) does not need
// the tests.
#[cfg(all(test, stageleft_runtime))]
mod tests;
