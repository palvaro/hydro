//! Two-phase commit with optional cooperative termination: the LDFI paper's third example and
//! the classic blocking case, as a target for lineage-driven fault injection.
//!
//! The coordinator broadcasts `prepare`, collects a vote from every participant (here every
//! participant votes yes), and broadcasts `commit`. The property is **agreement**: if any
//! participant commits a transaction (even one that later crashes), every correct participant
//! commits it. Links are reliable (`fail_stop`), so the fault model is crashes.
//!
//! - Plain 2PC violates agreement under one crash: the coordinator dies after sending `commit`
//!   to some participants; those commit, the rest wait forever.
//! - With the **cooperative termination protocol** (`ctp`), a participant that voted yes and
//!   has heard no decision asks its peers when its timer fires, and a peer that knows the
//!   decision answers. That survives the coordinator's crash, but not the coordinator's crash
//!   *plus* the crash of the one participant that learned the decision before it could answer:
//!   the committed transaction is then known only to a dead node. Two crashes, as the paper
//!   found.
//!
//! # Shape
//!
//! Each participant runs one tick that consumes the coordinator's two channels, the peer
//! channel, and its timer through batches, keeps the set of transactions it is uncertain
//! about and the set it has decided, and emits votes, peer messages, and its commits. The
//! coordinator runs one tick that counts votes and emits a decision once every participant has
//! voted.
//!
//! # Labels
//!
//! The `prepare`, `commit` and `peer` broadcasts are labelled `EventualConsistency` by their
//! transport. The committed transactions carry the label as a *claim*: "every correct
//! participant ends with the same commits" is agreement, so a checker of `EventualConsistency`
//! labels checks agreement without a hand-written specification.

use hydro_lang::live_collections::stream::{NoOrder, TotalOrder};
use hydro_lang::location::MemberId;
use hydro_lang::location::cluster::EventualConsistency;
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

pub struct Coordinator {}

pub struct Participant {}

/// A transaction identifier.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Txn(pub u32);

/// Participant-to-participant traffic for cooperative termination.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum PeerMsg {
    /// "I voted yes on this transaction and have heard no decision; do you know one?"
    Query(Txn),
    /// "This transaction was committed."
    Decided(Txn),
}

/// Builds the protocol. `txns` are the transactions the coordinator runs; `timeouts` fire a
/// participant's uncertainty timer (one `()` per member, driven by the test). Returns the
/// transactions each participant committed.
///
/// # Non-Determinism
/// - `nondet_prepares`, `nondet_commits`, `nondet_peers`, `nondet_votes`: the batching of
///   each channel at its receiver.
/// - `nondet_timeouts`: when a participant's timer fires relative to the messages it
///   receives, which only decides whether it asks its peers before hearing the decision.
#[expect(
    clippy::too_many_arguments,
    reason = "one non-determinism witness per channel"
)]
pub fn two_pc<'a>(
    coordinator: &Process<'a, Coordinator>,
    participants: &Cluster<'a, Participant>,
    num_participants: usize,
    txns: Stream<Txn, Process<'a, Coordinator>, Unbounded, TotalOrder>,
    timeouts: Stream<(), Cluster<'a, Participant>, Unbounded, TotalOrder>,
    ctp: bool,
    nondet_prepares: NonDet,
    nondet_commits: NonDet,
    nondet_peers: NonDet,
    nondet_votes: NonDet,
    nondet_timeouts: NonDet,
) -> Stream<Txn, Cluster<'a, Participant, EventualConsistency>, Unbounded, NoOrder> {
    // Phase 1: the coordinator asks every participant to prepare.
    let prepares = txns.broadcast_closed(participants, TCP.fail_stop().name("prepare").bincode());

    // Phase 2: decisions, taken in the coordinator's tick below once every vote is in.
    let (decisions_handle, decisions) =
        coordinator.forward_ref::<Stream<Txn, Process<'a, Coordinator>, Unbounded, NoOrder>>();
    let commits =
        decisions.broadcast_closed(participants, TCP.fail_stop().name("commit").bincode());

    // Peer traffic (cooperative termination), closing a cycle through the network.
    let (peer_handle, peer_in) =
        prepares
            .location()
            .forward_ref::<Stream<(MemberId<Participant>, PeerMsg), _, Unbounded, NoOrder>>();

    #[expect(
        clippy::type_complexity,
        reason = "the sliced! outputs are annotated with their full stream types"
    )]
    let (votes, peer_out, committed): (
        Stream<Txn, Cluster<'a, Participant>, Unbounded, NoOrder>,
        Stream<PeerMsg, Cluster<'a, Participant>, Unbounded, NoOrder>,
        Stream<Txn, Cluster<'a, Participant>, Unbounded, NoOrder>,
    ) = sliced! {
        let prepare_batch = use::batch(prepares, nondet_prepares);
        let commit_batch = use::batch(commits, nondet_commits);
        let peer_batch = use::batch(peer_in, nondet_peers);
        let timeout_batch = use::batch(timeouts, nondet_timeouts);
        let mut uncertain = use::state(|l| {
            l.source_iter(q!(Vec::<Txn>::new())).weaken_ordering::<NoOrder>()
        });
        let mut decided = use::state(|l| {
            l.source_iter(q!(Vec::<Txn>::new())).weaken_ordering::<NoOrder>()
        });

        // Every prepare is voted yes.
        let votes = prepare_batch.clone().weaken_ordering::<NoOrder>();

        // Decisions learned this tick: from the coordinator, or from a peer that knows.
        let peer_decisions = peer_batch.clone().filter_map(q!(|(_, message)| match message {
            PeerMsg::Decided(txn) => Some(txn),
            PeerMsg::Query(_) => None,
        }));
        let newly_decided = commit_batch
            .chain(peer_decisions)
            .unique()
            .filter_not_in(decided.clone());
        let decided_now = decided.chain(newly_decided.clone());
        decided = decided_now.clone();

        // Uncertain: voted yes, no decision heard.
        let uncertain_now = uncertain
            .chain(votes.clone())
            .filter_not_in(decided_now.clone());
        uncertain = uncertain_now.clone();

        // Cooperative termination: on a timeout, ask peers about everything uncertain; answer
        // every query about a transaction we have decided.
        // `ctp` is fixed when the program is built, so it selects the dataflow here rather
        // than being captured by a staged closure.
        let timed_out = if ctp {
            timeout_batch.count().map(q!(|n| n > 0))
        } else {
            timeout_batch.count().map(q!(|_| false))
        };
        let queries = uncertain_now
            .filter_if(timed_out)
            .map(q!(|txn| PeerMsg::Query(txn)));
        let queried = peer_batch.filter_map(q!(|(_, message)| match message {
            PeerMsg::Query(txn) => Some((txn, ())),
            PeerMsg::Decided(_) => None,
        }));
        let answers = decided_now
            .map(q!(|txn| (txn, ())))
            .join(queried)
            .map(q!(|(txn, _)| PeerMsg::Decided(txn)));

        (votes, queries.chain(answers), newly_decided)
    };

    peer_handle.complete(
        peer_out
            .broadcast_closed(participants, TCP.fail_stop().name("peer").bincode())
            .entries(),
    );

    // The coordinator: count votes per transaction; decide once everyone has voted.
    let vote_entries = votes
        .send(coordinator, TCP.fail_stop().name("vote").bincode())
        .entries();
    let decided_at_coordinator: Stream<Txn, Process<'a, Coordinator>, Unbounded, NoOrder> = sliced! {
        let vote_batch = use::batch(vote_entries, nondet_votes);
        let mut all_votes = use::state(|l| {
            l.source_iter(q!(Vec::<(MemberId<Participant>, Txn)>::new()))
                .weaken_ordering::<NoOrder>()
        });
        let mut committed = use::state(|l| {
            l.source_iter(q!(Vec::<Txn>::new())).weaken_ordering::<NoOrder>()
        });

        let votes_now = all_votes.chain(vote_batch);
        all_votes = votes_now.clone();

        let complete = votes_now
            .map(q!(|(member, txn)| (txn, member)))
            .into_keyed()
            .fold(
                q!(|| 0usize),
                q!(
                    |count, _member| *count += 1,
                    commutative = manual_proof!(/** counting is commutative */)
                ),
            )
            .filter(q!(move |count| *count >= num_participants))
            .keys();
        let newly_committed = complete.filter_not_in(committed.clone());
        committed = committed.chain(newly_committed.clone());

        newly_committed
    };
    decisions_handle.complete(decided_at_coordinator);

    committed.assert_has_consistency_of::<Cluster<'a, Participant, EventualConsistency>>(
        manual_proof!(
            /** claimed, not proven: this is agreement. Plain 2PC refutes it with one crash (the
            coordinator dies mid-`commit` fan-out); with cooperative termination it takes two
            (the coordinator and the one participant that learned the decision). This label
            is the target of fault injection. */
        ),
    )
}

#[cfg(test)]
mod tests {
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;

    use super::{Coordinator, Participant, Txn, two_pc};

    const PARTICIPANTS: u32 = 2;

    /// Without faults every participant commits the transaction, with or without cooperative
    /// termination, whenever the timers fire.
    fn commits_everywhere(ctp: bool) {
        let mut flow = FlowBuilder::new();
        let coordinator = flow.process::<Coordinator>();
        let participants = flow.cluster::<Participant>();
        let (txn_send, txns) = coordinator.sim_input::<Txn, TotalOrder, ExactlyOnce>();
        let (timeout_send, timeouts) = participants.sim_input::<(), TotalOrder, ExactlyOnce>();
        let committed = two_pc(
            &coordinator,
            &participants,
            PARTICIPANTS as usize,
            txns,
            timeouts,
            ctp,
            nondet!(/** test */),
            nondet!(/** test */),
            nondet!(/** test */),
            nondet!(/** test */),
            nondet!(/** test */),
        )
        .sim_cluster_output();

        let compiled = flow
            .sim()
            .skip_consistency_assertions()
            .with_cluster_size(&participants, PARTICIPANTS as usize)
            .compiled();
        let run = async || {
            txn_send.send(Txn(1));
            timeout_send.send_many((0..PARTICIPANTS).map(|p| (p, ())));
            for p in 0..PARTICIPANTS {
                let got: Vec<Txn> = committed.collect_n_sorted(p, 1).await;
                assert_eq!(got, vec![Txn(1)], "participant {p}");
            }
        };
        if ctp {
            // Cooperative termination's timers and peer queries make the schedule space too
            // large to enumerate (each run takes about a millisecond); sample it.
            compiled.fuzz_bounded(256, run);
        } else {
            compiled.exhaustive(run);
        }
    }

    #[test]
    fn two_pc_commits_without_faults() {
        commits_everywhere(false);
    }

    #[test]
    fn two_pc_with_cooperative_termination_commits_without_faults() {
        commits_everywhere(true);
    }
}
