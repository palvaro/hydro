//! Ordering literals, and the two that are proved constant (docs/design/hydrofrac-races.md §12.1).
//!
//! A literal is a yes/no statement about a run. The search treats every literal as a free choice
//! of the adversary unless this module proves it has the same value in every run, under every
//! fault set and every schedule. Two proofs exist:
//! - **fan-out order** ([`FanOutOrder`]): on a closed broadcast, a node sends each payload to the
//!   destination members in increasing id order, and, if the broadcast data is directly a harness
//!   input of a process, sends the payloads in the harness's order;
//! - **never sent** (see [`after`] and the sweep's `sends_complete`): a crash after a message the
//!   node never sends never fires.
//!
//! Two kinds of literals occur in rows:
//! - **crash literals** `after(d, m)`: "the sender of `d` sent `d` after `m`". The crash
//!   `crash(n after m)` kills a delivery `d` that `n` sent only if `after(d, m)` holds;
//! - **premise literals** of a fragile step on a derivation: "this step produced the same record
//!   in this run" (one per fragile step, which is conservative). None is proved, so a derivation
//!   with a premise never protects a pair.
//!
//! **Free literals are eliminated in closed form.** A row reads
//! `Σ x_f·[f kills d, given the literals] + Σ (1 − ℓ) ≥ z_p`. Every literal enters with the sign
//! that makes a row easier to satisfy, and none appears anywhere else (bounds and no-goods are over
//! faults only). So the solver's best choice for a free literal is fixed: a free `after(d, m)` is
//! true (the crash kills `d`), and a free premise is false (the row holds whatever is chosen).
//! Substituting these values gives exactly the same solutions in the fault variables. The
//! programs in [`super::ilp`] are those substituted forms: fewer variables, no products.
//! The adversary may then pick literal values no run realizes (e.g. `after(d, m)` and `after(m, d)`
//! together). That only makes it stronger, which is sound.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::rc::Rc;

use crate::fault::{MessageId, NodeId};

/// The value of a literal: proved, or the adversary's choice.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Literal {
    /// Proved to have this value in every run.
    Fixed(bool),
    /// Not proved: the adversary picks it.
    Free,
}

/// What is known, in every run, of the order in which a node sends its messages.
///
/// `precedes(d, m)` claims: in every run (any fault set, any schedule) in which `m`'s sender
/// sends `m`, it has already sent `d`. It must be false unless `d` and `m` have the same sender.
/// It must also be a union of disjoint chains: each message lies in at most one chain, and the
/// chain is totally ordered ([`super::Sweep::is_exact`] relies on it).
pub trait SendOrder<M>: Debug {
    /// Whether every run that sends `later` has sent `earlier` before it (see the trait docs).
    fn precedes(&self, earlier: &M, later: &M) -> bool;
}

/// No order known: every pair of sends may come in either order.
impl<M> SendOrder<M> for () {
    fn precedes(&self, _: &M, _: &M) -> bool {
        false
    }
}

/// The crash literal `after(d, m)` for two sends `d ≠ m` of one node: whether `d` is sent after
/// `m` in the runs where the crash right after `m` fires.
/// - `Fixed(false)` if `order` proves `d` precedes `m`: then `d` is out before the crash;
/// - `Fixed(true)` if it proves `m` precedes `d`: then `d` can only be sent after `m`;
/// - `Free` otherwise.
///
/// If `n` never sends `m`, the crash never fires, and the literal does not matter: the never-sent
/// proof. The sweep uses it by not counting crashes after messages no run of a fragile-free program
/// showed (`sends_complete`).
pub fn after<M>(order: &dyn SendOrder<M>, d: &M, m: &M) -> Literal {
    if order.precedes(d, m) {
        Literal::Fixed(false)
    } else if order.precedes(m, d) {
        Literal::Fixed(true)
    } else {
        Literal::Free
    }
}

/// The proved order of closed-broadcast sends (`crate::provenance::fanout`).
///
/// - **Fan-out** (every marked channel): a node sends each payload to the destination cluster's
///   members in increasing id order, so `(v → j)` precedes `(v → k)` for `j < k`.
/// - **Sequence** (sequenced channels only, [`crate::provenance::fanout::SEQUENCED_MARK`]): the
///   sender sends whole payloads in the order the harness sent them, so if `a` came before `b`
///   in the probe, every `(a → j)` precedes every `(b → k)`. The rank of each payload is read
///   from the failure-free probe ([`Self::learn`]), and every later run is checked against it
///   ([`Self::check`]): a contradicting run panics. A payload the probe did not show, or one the
///   harness sent twice, is not ranked, and its literals stay free (but for fan-out).
///
/// The chains: per `(channel, sender)`, the ranked payloads' sends form one chain (by rank, then
/// member); each other payload's sends form one chain (by member).
///
/// **Proof of fan-out.** `broadcast_closed` lowers to `cross_product(data, ClusterIds)` → map →
/// `demux`. The cross product is a `join_multiset_half` with key `()`, whose build side (the
/// member ids, which the simulator lists as `0..n` from one `source_iter`) is read in full before
/// the probe side. So each probe record `v` comes out as `(v → 0), …, (v → n−1)`, in that order.
/// The maps and the cast before the network are one to one, and the network sends its items in
/// order. The channel is marked only when the IR matches this exactly
/// (`provenance::fanout::mark`); a hand-written fan-out keeps every literal free.
/// `check/fanout_tests.rs` checks the order under every schedule.
///
/// **Proof of sequence** (docs/design/hydrofrac-soundness.md §6.4). On a sequenced channel the
/// fanned-out data is directly a harness input of the sending process. The harness sends one
/// fixed sequence in every schedule of every fault set (A8); the input reaches the join in FIFO
/// order; the join emits whole fan-outs in probe order; a crash cuts a suffix of the sends and an
/// omission drops no send. So every run sends a prefix of one sequence of payload blocks, the
/// probe's. `check/fanout_tests.rs` checks it under every schedule of every fault set within its
/// bounds.
#[derive(Debug, Default)]
pub struct FanOutOrder {
    channels: BTreeSet<usize>,
    sequenced: BTreeSet<usize>,
    /// Per sequenced `(channel, sender)` the probe showed sending: each payload's rank, or `None`
    /// if a payload was sent in two separate blocks (sent twice by the harness).
    ranks: BTreeMap<(usize, NodeId), Option<BTreeMap<Rc<[u8]>, usize>>>,
}

/// Per sequenced `(channel, sender)`, the payloads one run sent, one entry per block of
/// consecutive sends of one payload.
type Blocks = BTreeMap<(usize, NodeId), Vec<Rc<[u8]>>>;

impl FanOutOrder {
    /// The order of the marked channels `channels`, of which `sequenced` are sequenced.
    pub fn new(
        channels: impl IntoIterator<Item = usize>,
        sequenced: impl IntoIterator<Item = usize>,
    ) -> Self {
        let sequenced: BTreeSet<usize> = sequenced.into_iter().collect();
        FanOutOrder {
            channels: channels.into_iter().chain(sequenced.clone()).collect(),
            sequenced,
            ranks: BTreeMap::new(),
        }
    }

    /// The payload blocks of one run's `sends`, in send order.
    fn blocks<'m>(&self, sends: impl IntoIterator<Item = &'m MessageId>) -> Blocks {
        let mut blocks = Blocks::new();
        for m in sends {
            if self.sequenced.contains(&m.channel) {
                let seq = blocks.entry((m.channel, m.from.clone())).or_default();
                if seq.last() != Some(&m.bytes) {
                    seq.push(m.bytes.clone());
                }
            }
        }
        blocks
    }

    /// Learns the ranks from one failure-free run's `sends` (in send order). Every failure-free
    /// run sends a prefix of one sequence, so the ranks only grow.
    ///
    /// # Panics
    /// If `sends` contradicts what earlier runs showed (assumption A8 fails).
    pub fn learn<'m>(&mut self, sends: impl IntoIterator<Item = &'m MessageId>) {
        for (key, seq) in self.blocks(sends) {
            let distinct: BTreeSet<&Rc<[u8]>> = seq.iter().collect();
            if distinct.len() < seq.len() {
                self.ranks.insert(key, None);
                continue;
            }
            let entry = self.ranks.entry(key.clone()).or_insert(Some(BTreeMap::new()));
            if let Some(rank) = entry {
                assert_prefix(&key, rank, &seq);
                for (i, bytes) in seq.into_iter().enumerate() {
                    rank.entry(bytes).or_insert(i);
                }
            }
        }
    }

    /// Checks one run's `sends` (in send order, any fault set) against the ranks.
    ///
    /// # Panics
    /// If the run sends a ranked payload twice or out of the probe's order (assumption A8 fails).
    pub fn check<'m>(&self, sends: impl IntoIterator<Item = &'m MessageId>) {
        for (key, seq) in self.blocks(sends) {
            if let Some(Some(rank)) = self.ranks.get(&key) {
                assert_prefix(&key, rank, &seq);
                let distinct: BTreeSet<&Rc<[u8]>> = seq.iter().collect();
                assert!(
                    distinct.len() == seq.len(),
                    "hydrofrac: sequenced channel {key:?} sent a payload twice in one run, which \
                     its failure-free runs never did: the harness is not one fixed sequence (A8)"
                );
            }
        }
    }

    /// The rank of `m`'s payload on its sequenced channel, if it has one.
    fn rank(&self, m: &MessageId) -> Option<usize> {
        let ranks = self.ranks.get(&(m.channel, m.from.clone()))?.as_ref()?;
        ranks.get(&m.bytes).copied()
    }
}

/// Asserts that `seq` (a run's blocks on `key`) agrees with `rank` as far as both go: the run's
/// `i`-th block is the payload of rank `i`, and a block past the ranked ones is not ranked.
fn assert_prefix(key: &(usize, NodeId), rank: &BTreeMap<Rc<[u8]>, usize>, seq: &[Rc<[u8]>]) {
    for (i, bytes) in seq.iter().enumerate() {
        let expected = if i < rank.len() { Some(&i) } else { None };
        assert!(
            rank.get(bytes) == expected,
            "hydrofrac: a run of sequenced channel {key:?} sent its payloads in another order than \
             the failure-free runs (block {i} has rank {:?}): the harness is not one fixed \
             sequence (A8)",
            rank.get(bytes)
        );
    }
}

impl SendOrder<MessageId> for FanOutOrder {
    fn precedes(&self, earlier: &MessageId, later: &MessageId) -> bool {
        if !(self.channels.contains(&later.channel)
            && earlier.channel == later.channel
            && earlier.from == later.from
            && earlier.to.location == later.to.location)
        {
            return false;
        }
        if earlier.bytes == later.bytes {
            return matches!(
                (earlier.to.member, later.to.member),
                (Some(j), Some(k)) if j < k
            );
        }
        matches!((self.rank(earlier), self.rank(later)), (Some(a), Some(b)) if a < b)
    }
}
