//! Ordering literals, and the two that are proved constant (docs/design/hydrofrac-races.md §12.1).
//!
//! A literal is a yes/no statement about a run. The search treats every literal as a free choice
//! of the adversary unless this module proves it has the same value in every run, under every
//! fault set and every schedule. Two proofs exist:
//! - **fan-out order** ([`FanOutOrder`]): on a closed broadcast, a node sends each payload to the
//!   destination members in increasing id order;
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

use std::collections::BTreeSet;
use std::fmt::Debug;

use crate::fault::MessageId;

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
/// `precedes(d, m)` claims: in every run (any fault set, any schedule), every copy of `m`'s
/// content its sender sends comes after some copy of `d`'s content. It reads contents only:
/// copies ([`crate::search::Copies`]) are numbered per run. It must be false unless `d` and `m`
/// have the same sender.
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

/// The order of closed-broadcast fan-outs (`crate::provenance::fanout`): on a marked channel, a
/// node sends each payload to the destination cluster's members in increasing id order, so
/// `(v → j)` precedes `(v → k)` for `j < k`: each copy of `(v → k)` follows the copy of
/// `(v → j)` of the same fan-out. Each `(channel, sender, payload)` is one chain.
///
/// **Proof.** `broadcast_closed` lowers to `cross_product(data, ClusterIds)` → map → `demux`. The
/// cross product is a `join_multiset_half` with key `()`, whose build side (the member ids, which
/// the simulator lists as `0..n` from one `source_iter`) is read in full before the probe side. So
/// each probe record `v` comes out as `(v → 0), …, (v → n−1)`, in that order. The maps and the cast
/// before the network are one to one, and the network sends its items in order. The channel is
/// marked only when the IR matches this exactly (`provenance::fanout::mark`); a hand-written
/// fan-out keeps every literal free. `check/fanout_tests.rs` checks the order under every
/// schedule.
#[derive(Debug, Default)]
pub struct FanOutOrder {
    channels: BTreeSet<usize>,
}

impl FanOutOrder {
    /// The order of the marked channels `channels`.
    pub fn new(channels: impl IntoIterator<Item = usize>) -> Self {
        FanOutOrder {
            channels: channels.into_iter().collect(),
        }
    }
}

impl SendOrder<MessageId> for FanOutOrder {
    fn precedes(&self, earlier: &MessageId, later: &MessageId) -> bool {
        self.channels.contains(&later.channel)
            && earlier.channel == later.channel
            && earlier.from == later.from
            && earlier.bytes == later.bytes
            && earlier.to.location == later.to.location
            && matches!((earlier.to.member, later.to.member), (Some(j), Some(k)) if j < k)
    }
}
