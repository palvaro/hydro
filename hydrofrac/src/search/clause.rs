//! Faults, derivations, and the positional relation between them.
//!
//! A derivation is the set of deliveries an observed fact was computed from. A fault kills a
//! delivery when it omits that delivery, or when it crashes the sender before it sent it.
//! Killing any one delivery of a derivation kills the derivation. A [`Clause`] is one
//! derivation, together with the facts about the run it was observed in that the kill relation
//! needs.
//!
//! The search is generic over `M`, the identity of a message. The omission identity is
//! `(channel, sender, destination, wire bytes)` (docs/design/hydrofrac-lite.md §3). A crash
//! point names a message of the crashing node, so the same identity serves both.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Debug, Display};
use std::rc::Rc;

/// A node of the program: a process (`label`) or a cluster member (`label@member`).
pub type Node = String;

/// One fault.
///
/// The derived order is only for sets. The search orders the candidates of one hypothesis
/// weakest first ([`universe`]).
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

/// Where a crashed node dies: Molly's positional crash, `crash(node after m)`.
///
/// The node performs its sends up to and including `m` and nothing after. The point is named by
/// a message rather than a send count so the fault means the same thing in every run. What it
/// kills still depends on the order in which the run has the node attempt its sends.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub enum CrashAt<M> {
    /// Before its first send.
    BeforeAnySend,
    /// Right after it sends this message.
    After(M),
}

impl<M> Fault<M> {
    /// The crash of `node` right after it sends `after`, or before any send.
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
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Dependency<M> {
    /// The message.
    pub message: M,
    /// The sending node and the message's position among that node's sends in the run it was
    /// observed in. The position is exact, not clamped to the heal point. Together with the
    /// run's send sequences and pre-heal send counts it decides whether a crash point kills
    /// the delivery ([`Self::killed_by`]).
    pub sender: Option<(Node, u64)>,
    /// Whether the channel admits omission.
    pub lossy: bool,
}

impl<M: Debug> Debug for Dependency<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.message)?;
        if let Some((sender, ordinal)) = &self.sender {
            write!(f, "←{sender}#{ordinal}")?;
        }
        Ok(())
    }
}

/// Every node's send sequence in one run. It gives crash points their meaning there:
/// `crash(node after m)` kills the sends after `m`'s position.
pub type SendSequences<M> = BTreeMap<Node, Vec<M>>;

/// How many of each node's sends in one run happened before the network healed.
///
/// Faults happen only while the harness drives inputs. Once it is done the network heals: no
/// more omissions, and no new crashes (crashed nodes stay dead). So a crash point fires only
/// if it names a pre-heal send.
pub type PreHealSends = BTreeMap<Node, usize>;

/// A derivation of a fact: the deliveries it was computed from. Killing any one of them kills
/// the derivation.
pub type Derivation<M> = BTreeSet<Dependency<M>>;

impl<M: PartialEq + Clone> Dependency<M> {
    /// Whether `fault` kills this delivery in the run described by `sends` and `pre_heal`.
    ///
    /// - An omission kills a lossy delivery of the same message.
    /// - A crash point of the sender kills the delivery if it fires in this run (it names a
    ///   pre-heal send) before the delivery's send. Crashing before any send kills every
    ///   delivery of the node.
    ///
    /// A pre-heal crash point kills every later send, including those after the heal point.
    /// Clamping the relation at the heal point would make a clause claim that a crash kills
    /// nothing of what it does kill, and a hypothesis that breaks the label would never be a
    /// hitting set: a false certificate.
    pub fn killed_by(
        &self,
        fault: &Fault<M>,
        sends: &SendSequences<M>,
        pre_heal: &PreHealSends,
    ) -> bool {
        match fault {
            Fault::Omit(message) => self.lossy && *message == self.message,
            Fault::Crash { node, at } => {
                let Some((sender, ordinal)) = &self.sender else {
                    return false;
                };
                if sender != node {
                    return false;
                }
                match at {
                    CrashAt::BeforeAnySend => true,
                    CrashAt::After(m) => {
                        let fires_before = pre_heal.get(node).copied().unwrap_or(0);
                        sends
                            .get(node)
                            .and_then(|seq| seq.iter().take(fires_before).position(|s| s == m))
                            .is_some_and(|at| (at as u64) < *ordinal)
                    }
                }
            }
        }
    }

    /// The weakest faults that kill this delivery: its omission, if the channel is lossy, and
    /// the crash of its sender right after the send that precedes this one.
    ///
    /// Crash points are named by pre-heal sends only. A delivery sent after the heal point is
    /// killed by every crash point of its sender that fires in this run, and the weakest of
    /// those is the crash right after the sender's last pre-heal send. [`Self::killed_by`]
    /// uses the exact position.
    pub fn candidate_faults(
        &self,
        sends: &SendSequences<M>,
        pre_heal: &PreHealSends,
    ) -> Vec<Fault<M>> {
        let mut faults = vec![];
        if self.lossy {
            faults.push(Fault::Omit(self.message.clone()));
        }
        if let Some((sender, ordinal)) = &self.sender {
            let cap = pre_heal.get(sender).copied().unwrap_or(0) as u64;
            let ordinal = (*ordinal).min(cap);
            let after = if ordinal > 0 {
                sends
                    .get(sender)
                    .and_then(|seq| seq.get(ordinal as usize - 1))
                    .cloned()
            } else {
                None
            };
            faults.push(Fault::crash_after(sender.clone(), after));
        }
        faults
    }
}

/// A derivation together with the facts about the run it was observed in that the kill relation
/// needs.
#[derive(Clone)]
pub struct Clause<M> {
    /// The deliveries the fact was derived from.
    pub deps: Derivation<M>,
    /// The run's send sequences.
    pub sends: Rc<SendSequences<M>>,
    /// The run's pre-heal send counts: the crash points that fire in this run, and so the ones
    /// this clause proposes and is hit by.
    pub pre_heal_sends: Rc<PreHealSends>,
    /// The crashes of the hypothesis the derivation was observed under. A derivation found in
    /// a world where a node was absent is sufficient only there: agreement is not monotone in
    /// a node's presence (the absent node, alive, may act against it). So a hypothesis that
    /// does not include all of these crashes counts as hitting the clause.
    pub premises: FaultSet<M>,
}

impl<M: Debug> Debug for Clause<M> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Clause")
            .field("deps", &self.deps)
            .field("premises", &self.premises)
            .finish_non_exhaustive()
    }
}

impl<M: Ord + Clone> Clause<M> {
    /// A clause of a run in which every send of `sends` happened before the heal point.
    pub fn new(deps: impl IntoIterator<Item = Dependency<M>>, sends: Rc<SendSequences<M>>) -> Self {
        let pre_heal_sends = Rc::new(
            sends
                .iter()
                .map(|(node, seq)| (node.clone(), seq.len()))
                .collect(),
        );
        Clause {
            deps: deps.into_iter().collect(),
            sends,
            pre_heal_sends,
            premises: BTreeSet::new(),
        }
    }

    /// Whether `fault` kills some delivery of this clause.
    pub fn hit_by(&self, fault: &Fault<M>) -> bool {
        self.deps
            .iter()
            .any(|dep| dep.killed_by(fault, &self.sends, &self.pre_heal_sends))
    }

    /// The weakest faults that kill some delivery of this clause.
    pub fn candidate_faults(&self) -> impl Iterator<Item = Fault<M>> + '_ {
        self.deps
            .iter()
            .flat_map(move |dep| dep.candidate_faults(&self.sends, &self.pre_heal_sends))
    }

    /// Whether no fault set can kill this derivation: it has no deliveries and holds in every
    /// world, whatever crashes.
    pub fn is_unkillable(&self) -> bool {
        self.deps.is_empty() && self.premises.is_empty()
    }
}

/// The candidate faults of one hypothesis, weakest first: the weakest faults that kill some
/// delivery of some clause, plus `extra`.
///
/// Omissions come first, then crashes. Within a node, a crash after a later message comes before
/// one after an earlier message, and the crash before any send comes last. A position counts
/// as the latest one at which any clause's run sent the message. The solver uses this order as
/// its tie-break, so a hypothesis leaves as much of the protocol running as a minimal hitting set
/// allows.
pub fn universe<'a, M: Ord + Clone + 'a>(
    clauses: &[Clause<M>],
    extra: impl IntoIterator<Item = &'a Fault<M>>,
) -> Vec<Fault<M>> {
    let mut universe: Vec<Fault<M>> = clauses
        .iter()
        .flat_map(Clause::candidate_faults)
        .chain(extra.into_iter().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let position = |fault: &Fault<M>| -> u64 {
        let Fault::Crash {
            node,
            at: CrashAt::After(m),
        } = fault
        else {
            return 0;
        };
        clauses
            .iter()
            .filter_map(|c| c.sends.get(node))
            .filter_map(|seq| seq.iter().position(|s| s == m))
            .map(|p| p as u64 + 1)
            .max()
            .unwrap_or(0)
    };
    universe.sort_by_cached_key(|f| match f {
        Fault::Omit(_) => (0, None, std::cmp::Reverse(0), f.clone()),
        Fault::Crash { node, .. } => (
            1,
            Some(node.clone()),
            std::cmp::Reverse(position(f)),
            f.clone(),
        ),
    });
    universe
}
