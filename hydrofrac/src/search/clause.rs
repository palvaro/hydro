//! Faults, derivations, and the positional kill relation between them.
//!
//! A derivation is the set of deliveries an observed fact was computed from; killing any one
//! kills it. A fault kills a delivery by omitting it, or by crashing its sender before it was
//! sent. The search is generic over the message identity `M`, which also names crash points
//! (docs/design/hydrofrac-lite.md §3).

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Display};
use std::rc::Rc;

/// A node of the program: a process (`label`) or a cluster member (`label@member`).
pub type Node = String;

/// One fault. The derived order is only for sets; [`universe`] orders candidates.
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
    /// The sender and the message's exact position among its sends in the observed run.
    pub sender: Option<(Node, u64)>,
    /// Whether the channel admits omission.
    pub lossy: bool,
}

/// Every node's send sequence in one run: what gives crash points their meaning there.
pub type SendSequences<M> = BTreeMap<Node, Vec<M>>;

/// How many of each node's sends in one run happened before the heal (§4.6): a crash point
/// fires only if it names one of them.
pub type PreHealSends = BTreeMap<Node, usize>;

/// A derivation of a fact: the deliveries it was computed from.
pub type Derivation<M> = BTreeSet<Dependency<M>>;

impl<M: PartialEq + Clone> Dependency<M> {
    /// Whether `fault` kills this delivery in the run described by `sends` and `pre_heal`: an
    /// omission of the same lossy message, or a sender's crash point that fires (names a
    /// pre-heal send) before this send. Not clamped at the heal: a pre-heal crash kills every
    /// later send, and clamping would give false certificates.
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

    /// The weakest faults that kill this delivery: its omission (if lossy), and its sender's
    /// crash right after the send before it, or after its last pre-heal send if that is earlier.
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

/// A derivation, with what the kill relation needs to know about the run it was observed in.
#[derive(Clone, Debug)]
pub struct Clause<M> {
    /// The deliveries the fact was derived from.
    pub deps: Derivation<M>,
    /// The run's send sequences.
    pub sends: Rc<SendSequences<M>>,
    /// The run's pre-heal send counts.
    pub pre_heal_sends: Rc<PreHealSends>,
    /// The crashes of the hypothesis it was observed under. Agreement is not monotone in a
    /// node's presence, so a hypothesis lacking one of these counts as hitting the clause.
    pub premises: FaultSet<M>,
}

impl<M: Ord + Clone> Clause<M> {
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

    /// Whether no fault set can kill this derivation (no deliveries, no premises).
    pub fn is_unkillable(&self) -> bool {
        self.deps.is_empty() && self.premises.is_empty()
    }

    /// Every crash of `node` that kills some delivery of this clause: before any send, and
    /// right after each pre-heal send of `node` before its last delivery here.
    pub fn crash_killers<'a>(&'a self, node: &'a Node) -> impl Iterator<Item = Fault<M>> + 'a {
        let fires = self.pre_heal_sends.get(node).copied().unwrap_or(0);
        let last = self
            .deps
            .iter()
            .filter_map(|d| d.sender.as_ref())
            .filter(|(sender, _)| sender == node)
            .map(|(_, ordinal)| *ordinal as usize)
            .max();
        let seq = self.sends.get(node);
        last.into_iter().flat_map(move |last| {
            std::iter::once(Fault::crash_after(node.clone(), None)).chain(
                seq.into_iter()
                    .flat_map(move |seq| seq.iter().take(fires.min(last)))
                    .map(move |m| Fault::crash_after(node.clone(), Some(m.clone()))),
            )
        })
    }
}

/// The faults to add to [`universe`] so that "no admissible hitting set" is a statement about
/// every positional fault set, not only the weakest candidates (which a crash point first seen
/// in a later run can undercut, hitting several old clauses at once):
///
/// 1. `crash(n)` for every sender `n` of a delivery: it kills everything any crash of `n` does;
/// 2. where `crash(n)` itself is a premise or in a vacuous set (so swapping it in is not
///    neutral), every crash point of `n` that kills some delivery instead;
/// 3. every fault of a premise or a vacuous set.
///
/// **Why that suffices.** Take any admissible `H` hitting every clause. Drop its faults that kill
/// no delivery of any clause (an omission that does is already in [`universe`]), and replace each
/// remaining crash of `n` outside the completed universe by `crash(n)` (rule 1; by rule 2 that
/// `crash(n)` is no premise and in no vacuous set). The result still hits every clause (the
/// replacement kills a superset, and no missing premise reappears), crashes a subset of `H`'s
/// nodes, and is a superset of a vacuous set only if `H` is. So if the completed universe admits
/// no hitting set at a bound, no fault set does.
pub fn completion<M: Ord + Clone>(
    clauses: &[Clause<M>],
    vacuous: &[FaultSet<M>],
) -> BTreeSet<Fault<M>> {
    let mentioned: BTreeSet<&Fault<M>> = clauses
        .iter()
        .flat_map(|c| c.premises.iter())
        .chain(vacuous.iter().flatten())
        .collect();
    let senders: BTreeSet<&Node> = clauses
        .iter()
        .flat_map(|c| c.deps.iter())
        .filter_map(|d| d.sender.as_ref().map(|(n, _)| n))
        .collect();
    let mut out: BTreeSet<Fault<M>> = mentioned.iter().map(|f| (*f).clone()).collect();
    for node in senders {
        let before_any = Fault::crash_after(node.clone(), None);
        if mentioned.contains(&before_any) {
            out.extend(clauses.iter().flat_map(|c| c.crash_killers(node)));
        } else {
            out.insert(before_any);
        }
    }
    out
}

/// The candidates of one hypothesis (each clause's weakest killers, plus `extra`), weakest first:
/// omissions, then per node crashes after later messages before earlier ones (by the latest
/// position any clause's run sent it at), `crash(n)` last. The solver's tie-break follows this
/// order, so a hypothesis leaves as much of the protocol running as it can.
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
