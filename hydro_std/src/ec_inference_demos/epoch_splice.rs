//! M1 of the epoch-keyed consensus ladder
//! (`design_docs/2026-08_epoch_keyed_consensus_splice.md`): the **splice
//! reader**.
//!
//! The log is a bag of `(epoch, slot, value)` entry facts plus a bag of
//! `(epoch, start_slot)` declarations. The splice reader is a *pure,
//! deterministic* function of the two bags:
//!
//! - **Ownership:** slot `i` is owned by the largest declared epoch `e` with
//!   `start_e <= i`. An entry counts only if its epoch owns its slot — entries
//!   of older epochs at slots at or beyond a successor's start are **dead**
//!   (truncation, doc §3).
//! - **Splice:** read slots `0, 1, 2, ...` taking the owning epoch's entry at
//!   each; stall at the first slot whose owning epoch has no entry there.
//!
//! # EC argument
//!
//! Both inputs are NoOrder,EC bags (entry facts and start declarations are
//! *observed facts once uttered* — doc §5). The fold that accumulates them is
//! commutative (keyed inserts into maps; the one `manual_proof!` here is that
//! ACI-genus obligation, not a consistency assertion), so the accumulated
//! state is EC, and the splice is deterministic on it — every member
//! converges to the same spliced log.
//!
//! **The raw splice is deliberately *non-monotone*:** a newly declared epoch
//! can retract a dead tail (truncation). That is the honest semantics — an
//! uncommitted suffix of a deposed author is not durable. Monotonicity of the
//! *emitted* log is exactly what the commit rule (M2) buys: restricted to
//! committed entries, the splice only grows. Types reflect this: the output is
//! a snapshot-style [`Singleton`] of the whole state, not an append-only
//! stream.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder};
use hydro_lang::location::cluster::EventualConsistency;
use hydro_lang::prelude::*;
use serde::Serialize;
use serde::de::DeserializeOwned;

/// A fact contributing to the epoch-keyed log: either a log entry authored by
/// some epoch, or an epoch's declared start slot.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, serde::Deserialize)]
pub enum SpliceFact<T> {
    /// Epoch `epoch` placed `value` at log position `slot`.
    Entry {
        /// The authoring epoch.
        epoch: u64,
        /// Global log position.
        slot: usize,
        /// The entry payload.
        value: T,
    },
    /// Epoch `epoch` declared that it continues the log from `start_slot`.
    Start {
        /// The declaring epoch.
        epoch: u64,
        /// The slot from which this epoch's authorship begins.
        start_slot: usize,
    },
}

/// One immutable node in the accumulated fact bag. Sharing the tail makes
/// state snapshots and single-fact updates O(1), while the public reader
/// remains a pure deterministic function of the same fact set.
#[derive(Clone, PartialEq, Eq)]
struct FactNode<T> {
    fact: SpliceFact<T>,
    previous: Option<Arc<FactNode<T>>>,
}

/// Accumulated splice state: all entry facts and start declarations seen so
/// far. A pure value — `splice()` derives the current log from it.
#[derive(Clone, Default)]
pub struct SpliceState<T> {
    facts: Option<Arc<FactNode<T>>>,
}

impl<T: PartialEq> PartialEq for SpliceState<T> {
    fn eq(&self, other: &Self) -> bool {
        self.materialize() == other.materialize()
    }
}

impl<T: Eq> Eq for SpliceState<T> {}

impl<T: fmt::Debug> fmt::Debug for SpliceState<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (entries, starts) = self.materialize();
        f.debug_struct("SpliceState")
            .field("entries", &entries)
            .field("starts", &starts)
            .finish()
    }
}

impl<T> SpliceState<T> {
    /// Creates an empty state.
    pub fn new() -> Self {
        Self { facts: None }
    }

    /// Absorbs one fact. Insertion order does not matter because readers
    /// materialize under keyed map inserts, matching the original ACI state.
    pub fn absorb(&mut self, fact: SpliceFact<T>) {
        self.facts = Some(Arc::new(FactNode {
            fact,
            previous: self.facts.take(),
        }));
    }

    fn materialize(&self) -> (BTreeMap<u64, BTreeMap<usize, &T>>, BTreeMap<u64, usize>) {
        let mut entries = BTreeMap::<u64, BTreeMap<usize, &T>>::new();
        let mut starts = BTreeMap::new();
        let mut cursor = self.facts.as_deref();
        while let Some(node) = cursor {
            match &node.fact {
                SpliceFact::Entry { epoch, slot, value } => {
                    entries.entry(*epoch).or_default().insert(*slot, value);
                }
                SpliceFact::Start { epoch, start_slot } => {
                    starts.insert(*epoch, *start_slot);
                }
            }
            cursor = node.previous.as_deref();
        }
        (entries, starts)
    }

    /// The owning epoch of a slot: the largest declared epoch whose start is
    /// at or before the slot. `None` if no declared epoch covers it.
    pub fn owner(&self, slot: usize) -> Option<u64> {
        let (_, starts) = self.materialize();
        starts
            .iter()
            .filter(|(_, start)| **start <= slot)
            .map(|(epoch, _)| *epoch)
            .next_back()
    }

    /// The spliced log: slots `0..` read from each slot's owning epoch,
    /// stalling at the first slot whose owner has no entry there.
    ///
    /// Deterministic on the state. **Non-monotone** under truncation: a newly
    /// declared epoch may retract a dead tail (see module docs).
    pub fn splice(&self) -> Vec<&T> {
        let (entries, starts) = self.materialize();
        let mut log = Vec::new();
        for slot in 0.. {
            let Some(owner) = starts
                .iter()
                .filter(|(_, start)| **start <= slot)
                .map(|(epoch, _)| *epoch)
                .next_back()
            else {
                break;
            };
            let Some(value) = entries.get(&owner).and_then(|m| m.get(&slot)) else {
                break;
            };
            log.push(*value);
        }
        log
    }
}

/// The splice reader as dataflow: fold the EC bag of [`SpliceFact`]s into a
/// [`SpliceState`] singleton at every member.
///
/// EC is preserved through the fold because the combinator is commutative
/// (keyed map inserts; the `manual_proof!` is that ACI obligation — there is
/// no consistency assertion anywhere in this reader). Every member's
/// `SpliceState` converges, and [`SpliceState::splice`] is deterministic on
/// it, so every member derives the same log.
pub fn splice_epoch_log<'a, T, L>(
    facts: Stream<
        SpliceFact<T>,
        Cluster<'a, L, EventualConsistency>,
        Unbounded,
        NoOrder,
        ExactlyOnce,
    >,
) -> Singleton<SpliceState<T>, Cluster<'a, L, EventualConsistency>, Unbounded>
where
    T: Clone + Serialize + DeserializeOwned + 'a,
    L: 'a,
{
    facts.fold(
        q!(|| SpliceState::new()),
        q!(
            |state, fact| {
                state.absorb(fact);
            },
            commutative = manual_proof!(
                /// `absorb` performs keyed inserts into maps: entries are keyed
                /// by (epoch, slot), starts by epoch, and each key is written
                /// by a single author (its epoch), so inserts across distinct
                /// facts commute.
            )
        ),
    )
}

#[cfg(test)]
mod tests {
    use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
    use hydro_lang::location::cluster::EventualConsistency;
    use hydro_lang::prelude::*;

    use super::{SpliceFact, SpliceState, splice_epoch_log};

    fn entry(epoch: u64, slot: usize, value: u32) -> SpliceFact<u32> {
        SpliceFact::Entry { epoch, slot, value }
    }

    fn start(epoch: u64, start_slot: usize) -> SpliceFact<u32> {
        SpliceFact::Start { epoch, start_slot }
    }

    fn state_of(facts: impl IntoIterator<Item = SpliceFact<u32>>) -> SpliceState<u32> {
        let mut s = SpliceState::new();
        for f in facts {
            s.absorb(f);
        }
        s
    }

    /// Single epoch = the base case: the splice is that epoch's dense prefix.
    #[test]
    fn single_epoch_is_dense_prefix() {
        let s = state_of([
            start(0, 0),
            entry(0, 0, 10),
            entry(0, 1, 11),
            entry(0, 3, 13),
        ]);
        // Stalls at the gap (slot 2), exactly like dense-prefix extraction.
        assert_eq!(s.splice(), vec![&10, &11]);
    }

    /// Succession without overlap: epoch 1 continues where epoch 0 ended.
    #[test]
    fn clean_succession_concatenates() {
        let s = state_of([
            start(0, 0),
            entry(0, 0, 10),
            entry(0, 1, 11),
            start(1, 2),
            entry(1, 2, 22),
        ]);
        assert_eq!(s.splice(), vec![&10, &11, &22]);
    }

    /// Truncation: epoch 0 half-published a tail beyond epoch 1's start; the
    /// tail is dead. The successor's entry wins at the contested slot.
    #[test]
    fn dead_tail_is_truncated() {
        let s = state_of([
            start(0, 0),
            entry(0, 0, 10),
            entry(0, 1, 11), // uncommitted tail — dies
            entry(0, 2, 12), // uncommitted tail — dies
            start(1, 1),     // successor read a quorum whose max prefix was slot 1
            entry(1, 1, 21),
            entry(1, 2, 22),
        ]);
        assert_eq!(s.splice(), vec![&10, &21, &22]);
    }

    /// Non-monotonicity, pinned deliberately: the splice can *retract* when a
    /// new epoch is declared. This is the honest raw semantics; monotone
    /// emission is what the M2 commit rule buys.
    #[test]
    fn splice_is_non_monotone_under_truncation() {
        let mut s = state_of([start(0, 0), entry(0, 0, 10), entry(0, 1, 11)]);
        assert_eq!(s.splice(), vec![&10, &11]);

        // Epoch 1 arrives, starting at slot 1 with no entry there yet: the
        // old tail at slot 1 is now dead, and the splice *shrinks*.
        s.absorb(start(1, 1));
        assert_eq!(s.splice(), vec![&10]);

        // The successor fills the slot; the log grows again — differently.
        s.absorb(entry(1, 1, 21));
        assert_eq!(s.splice(), vec![&10, &21]);
    }

    /// Ownership boundary: an epoch's own entries *before* its start slot are
    /// also dead (it never owned those slots).
    #[test]
    fn entries_before_own_start_are_dead() {
        let s = state_of([start(0, 0), entry(0, 0, 10), start(1, 1), entry(1, 0, 99)]);
        // Slot 0 is owned by epoch 0; epoch 1's stray entry there is ignored.
        assert_eq!(s.splice(), vec![&10]);
    }

    /// Commutativity spot-check backing the fold's manual_proof: absorbing the
    /// same facts in reversed order yields the same state.
    #[test]
    fn absorb_commutes() {
        let facts = vec![
            start(0, 0),
            entry(0, 0, 10),
            entry(0, 1, 11),
            start(1, 1),
            entry(1, 1, 21),
        ];
        let forward = state_of(facts.clone());
        let backward = state_of(facts.into_iter().rev());
        assert_eq!(forward, backward);
        assert_eq!(forward.splice(), vec![&10, &21]);
    }

    /// Compile-time pin: the dataflow reader takes an EC fact bag and yields
    /// an EC `SpliceState` singleton — commutative fold, no consistency
    /// assertion (the one `manual_proof!` is the fold's ACI obligation).
    #[test]
    fn splice_reader_preserves_ec() {
        let mut flow = FlowBuilder::new();
        let sender = flow.process::<()>();
        let cluster = flow.cluster::<()>();

        let (_send, facts) = sender.sim_input::<SpliceFact<u32>, TotalOrder, ExactlyOnce>();

        // Facts become an EC bag via broadcast (observed facts once uttered).
        let ec_facts: Stream<SpliceFact<u32>, Cluster<'_, (), EventualConsistency>, _, NoOrder, _> =
            facts
                .broadcast_closed(&cluster, TCP.fail_stop().bincode())
                .weaken_ordering();

        let state: Singleton<SpliceState<u32>, Cluster<'_, (), EventualConsistency>, _> =
            splice_epoch_log(ec_facts);

        let _ = state;
        let _ = flow.finalize();
    }

    /// Behavior test: the EC fact bag arrives in full at every member (two
    /// epochs, one dead tail), and the splice — deterministic on the bag, unit
    /// tests above — yields the same truncated log for each member's bag.
    ///
    /// (The dataflow fold itself is compile-pinned above; observing an
    /// *unbounded* singleton per-member in the sim needs `sliced!` snapshot
    /// machinery that is M2's concern, so this test splices at the harness.)
    #[test]
    fn all_members_agree_on_spliced_log() {
        let mut flow = FlowBuilder::new();
        let sender = flow.process::<()>();
        let cluster = flow.cluster::<()>();

        let (in_send, facts) = sender.sim_input::<SpliceFact<u32>, TotalOrder, ExactlyOnce>();

        let ec_facts: Stream<SpliceFact<u32>, Cluster<'_, (), EventualConsistency>, _, NoOrder, _> =
            facts
                .broadcast_closed(&cluster, TCP.fail_stop().bincode())
                .weaken_ordering();

        let out_recv = ec_facts.sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&cluster, 2)
            .exhaustive(async || {
                in_send.send(SpliceFact::Start {
                    epoch: 0,
                    start_slot: 0,
                });
                in_send.send(SpliceFact::Entry {
                    epoch: 0,
                    slot: 0,
                    value: 10,
                });
                in_send.send(SpliceFact::Entry {
                    epoch: 0,
                    slot: 1,
                    value: 11,
                }); // dies
                in_send.send(SpliceFact::Start {
                    epoch: 1,
                    start_slot: 1,
                });
                in_send.send(SpliceFact::Entry {
                    epoch: 1,
                    slot: 1,
                    value: 21,
                });

                for member in 0..2u32 {
                    let facts: Vec<SpliceFact<u32>> = out_recv.collect_n_sorted(member, 5).await;
                    let mut state = SpliceState::new();
                    for f in facts {
                        state.absorb(f);
                    }
                    let spliced: Vec<u32> = state.splice().into_iter().copied().collect();
                    assert_eq!(
                        spliced,
                        vec![10, 21],
                        "member {member} did not converge to the truncated splice"
                    );
                }
            });
    }
}
