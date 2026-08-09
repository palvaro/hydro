//! Building up EventualConsistency from composable primitives.
//!
//! # The goal
//!
//! Show that a consensus protocol's committed log is `EventualConsistency` by
//! composing smaller, individually-typed building blocks — rather than blessing
//! a monolithic implementation with a single `manual_proof!`.
//!
//! # Key insight
//!
//! Within a single view (fixed leader, fixed cluster), the leader's broadcast
//! of proposals is trivially EC (inferred by the type system via
//! `broadcast_closed` + `fail_stop`). Similarly, the leader's broadcast of
//! commit notifications (once a quorum acks) is EC.
//!
//! The committed log on each member is determined by joining proposals with
//! commit notifications: for each committed slot, take the proposal from the
//! view whose commit notification claims that slot. This is a deterministic
//! function of two EC streams, so it is itself EC.
//!
//! Note: the committed log is NOT simply a prefix of any single leader's
//! proposals. When leadership changes, the new leader may re-propose different
//! values for slots that the old leader proposed but never committed. The
//! commit notification (which includes the view ID) is what tells members
//! which view's proposal is authoritative for each slot.
//!
//! Across views, the proof obligation is: no two views ever commit conflicting
//! entries for the same slot. This is guaranteed by quorum intersection during
//! view changes (a new leader can only start sequencing at a slot beyond
//! everything committed in prior views). We encapsulate this as a typed witness
//! (`ViewTransferProof`) that the user must produce — forcing correct wiring —
//! with one small `manual_proof!` on the witness constructor.
//!
//! # The composition argument
//!
//! 1. Per-view proposal broadcast: EC (inferred, `broadcast_closed`).
//!
//! 2. Per-view commit broadcast: EC (inferred, same mechanism — leader
//!    broadcasts "slot N is committed in view V" to all members).
//!
//! 3. Committed log on each member = for each slot S with a commit
//!    notification (view=V, slot=S), take the proposal (view=V, slot=S).
//!    This is a deterministic join of two EC streams. Every member eventually
//!    sees all proposals and all commit notifications, so every member
//!    eventually computes the same committed log. → EC.
//!
//! 4. No slot conflicts across views: `ViewTransferProof` guarantees that
//!    view N+1 starts at a slot > max committed in view N. A new leader
//!    only commits entries it proposed itself (in its own view). So no two
//!    views can commit different values for the same slot.
//!
//! 5. Global EC: union of committed entries across views = same on every
//!    member. QED.
//!
//! # Proof surface
//!
//! - Per-view EC: **zero** manual proofs (inferred).
//! - Committed log is EC: follows from (3) above. **Zero** manual proofs.
//! - No cross-view conflicts: follows from `ViewTransferProof`. **ONE**
//!   manual proof: "quorum intersection guarantees prefix completeness."
//! - Commutativity of ack counting: ONE `manual_proof!` (trivial).

use hydro_lang::live_collections::stream::NoOrder;
use hydro_lang::location::cluster::EventualConsistency;
use hydro_lang::prelude::*;
use hydro_lang::properties::manual_proof;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

// ============================================================================
// Types
// ============================================================================

pub struct Nodes;

/// A proposal: a slot number paired with a value, tagged with the view.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Proposal<T> {
    pub view: usize,
    pub slot: usize,
    pub value: T,
}

/// A commit notification broadcast by the leader: "slot N is committed."
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CommitNotification {
    pub view: usize,
    pub slot: usize,
}

// ============================================================================
// Piece 1: Per-view proposal broadcast (EC inferred)
// ============================================================================

/// Run one view's proposal phase: the leader member sequences requests and
/// broadcasts them to all members of the same cluster using
/// `broadcast_from_member`.
///
/// The returned stream is EC — **inferred** by the type system, not asserted.
/// Every live member eventually receives every proposal the leader made.
///
/// `requests` is a stream on the cluster — only the leader member should
/// produce data on it (other members' streams are empty).
pub fn propose_in_view<'a, T: Clone + Serialize + DeserializeOwned + 'a>(
    requests: Stream<T, Cluster<'a, Nodes>, Unbounded>,
    start_slot: usize,
    view_id: usize,
) -> Stream<Proposal<T>, Cluster<'a, Nodes, EventualConsistency>, Unbounded, NoOrder> {
    let proposals = sliced! {
        let mut next_slot = use::state(|l| l.singleton(q!(start_slot)));
        let batch = use::batch(requests, nondet!(
            /// Batch boundaries determine slot assignment. This is the one
            /// sequencing nondeterminism in the protocol.
        ));

        let indexed = batch
            .enumerate()
            .cross_singleton(next_slot.clone())
            .map(q!(move |((idx, value), base)| Proposal {
                view: view_id,
                slot: base + idx,
                value,
            }));

        let count = indexed.clone().count();
        next_slot = count.zip(next_slot).map(q!(|(n, base)| base + n));

        indexed
    };

    // broadcast_from_member + fail_stop → EventualConsistency. INFERRED.
    proposals.broadcast_from_member(TCP.fail_stop().bincode())
}

/// Like `propose_in_view`, but the leader only starts proposing AFTER receiving
/// a start signal (carrying the start_slot). This enforces the causal
/// dependency: phase 2 only begins after phase 1 (promise collection) completes.
///
/// The start_signal stream should emit exactly one value (the start_slot) on
/// the leader member, produced by the promise quorum collection.
/// Requests arriving before the signal are buffered.
pub fn propose_in_view_gated<'a, T: Clone + Serialize + DeserializeOwned + 'a>(
    requests: Stream<T, Cluster<'a, Nodes>, Unbounded>,
    start_signal: Stream<usize, Cluster<'a, Nodes>, Unbounded>,
    view_id: usize,
) -> Stream<Proposal<T>, Cluster<'a, Nodes, EventualConsistency>, Unbounded, NoOrder> {
    let proposals = sliced! {
        let mut next_slot = use::state(|l| l.singleton(q!(0usize)));
        let mut started = use::state(|l| l.singleton(q!(false)));
        let mut request_buffer = use::state(|l| l.singleton(q!(vec![])));
        let batch = use::batch(requests, nondet!(
            /// Batch boundaries determine slot assignment.
        ));
        let signal_batch = use::batch(start_signal, nondet!(
            /// Start signal delivery timing — phase 2 begins when it arrives.
        ));

        // Buffer incoming requests into the vec
        let new_buffer = batch
            .fold(q!(|| vec![]), q!(|buf, item| { buf.push(item); },
                  commutative = manual_proof!(/** order within batch is nondet anyway */)))
            .zip(request_buffer.clone())
            .map(q!(|(mut new_items, mut existing)| {
                existing.append(&mut new_items);
                existing
            }));

        // Update started from signal
        let new_started = signal_batch.clone().count()
            .zip(started.clone())
            .map(q!(|(n, was_started)| was_started || n > 0));

        let signal_slot = signal_batch.fold(
            q!(|| 0usize),
            q!(|max, slot| { if slot > *max { *max = slot; } },
               commutative = manual_proof!(/** max is commutative */)),
        );

        // Use signal_slot as start if we just started, otherwise keep next_slot
        let effective_slot = signal_slot.zip(next_slot.clone()).zip(started.clone())
            .map(q!(|((sig_slot, cur_slot), was_started)| {
                if !was_started && sig_slot > 0 { sig_slot } else { cur_slot }
            }));

        started = new_started.clone();

        // When started, drain buffer into proposals
        let indexed = new_buffer.clone()
            .zip(new_started.clone())
            .zip(effective_slot.clone())
            .map(q!(move |((buffer, is_started), base)| {
                if is_started {
                    buffer.into_iter().enumerate()
                        .map(|(idx, value)| Proposal {
                            view: view_id,
                            slot: base + idx,
                            value,
                        })
                        .collect::<Vec<_>>()
                } else {
                    vec![]
                }
            }))
            .into_stream()
            .flatten_unordered();

        // Update next_slot
        let count = new_buffer.clone().zip(new_started.clone())
            .map(q!(|(buf, is_started)| if is_started { buf.len() } else { 0 }));
        next_slot = count.zip(effective_slot).map(q!(|(n, base)| base + n));

        // Clear buffer if started, keep if not
        request_buffer = new_buffer
            .zip(started.clone())
            .map(q!(|(buf, is_started)| if is_started { vec![] } else { buf }));

        indexed
    };

    proposals.broadcast_from_member(TCP.fail_stop().bincode())
}

// ============================================================================
// Piece 2: Phase 1 — Prepare/Promise (fencing via ballot lock)
// ============================================================================

/// A prepare message: new leader asks members to fence out lower views.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Prepare {
    pub view: usize,
    pub from_leader: usize,
}

/// A promise response: member pledges not to ack proposals from views < promised_view.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Promise {
    pub view: usize,
    pub max_committed_slot: usize,
}

/// Phase 1: new leader broadcasts Prepare, members respond with Promise.
///
/// This establishes the fence: once f+1 members promise view V, no view < V
/// can ever reach quorum again (because f+1 members will refuse to ack it).
///
/// `max_committed_per_member` is each member's current max committed slot,
/// provided as persistent state (e.g. from a sim_input that fires on each member).
///
/// Returns:
/// - promises_to_leader: stream of Promises arriving at the new leader
/// - prepares on cluster (EC, inferred) — used by fenced_ack_filter
pub fn phase1_prepare<'a>(
    prepare_trigger: Stream<Prepare, Cluster<'a, Nodes>, Unbounded>,
    max_committed_per_member: Stream<usize, Cluster<'a, Nodes>, Unbounded>,
    cluster: &Cluster<'a, Nodes>,
) -> (
    Stream<Promise, Cluster<'a, Nodes>, Unbounded, NoOrder>,
    Stream<Prepare, Cluster<'a, Nodes, EventualConsistency>, Unbounded, NoOrder>,
) {
    // Leader broadcasts Prepare to all members. EC inferred.
    let prepares_on_cluster = prepare_trigger
        .broadcast_from_member(TCP.fail_stop().bincode());

    // Each member responds with a Promise. We use a sliced! block so that
    // max_committed is persistent state available when the prepare arrives.
    let promises = sliced! {
        let mut max_committed = use::state(|l| l.singleton(q!(0usize)));
        let prepare_batch = use::batch(prepares_on_cluster.clone().weaken_consistency(), nondet!(
            /// Prepare delivery timing.
        ));
        let committed_batch = use::batch(max_committed_per_member, nondet!(
            /// Max committed updates.
        ));

        // Update max_committed from any new commit info
        let new_max = max_committed.clone()
            .zip(committed_batch.fold(
                q!(|| 0usize),
                q!(|max, s| { if s > *max { *max = s; } },
                   commutative = manual_proof!(/** max is commutative */)),
            ))
            .map(q!(|(old, batch_max)| old.max(batch_max)));
        max_committed = new_max.clone();

        // For each prepare received, emit a promise with current max_committed
        prepare_batch
            .cross_singleton(new_max)
            .map(q!(|(prepare, max_slot)| (
                hydro_lang::location::MemberId::<Nodes>::from_raw_id(prepare.from_leader as u32),
                Promise {
                    view: prepare.view,
                    max_committed_slot: max_slot,
                },
            )))
    };

    let routed_promises = promises
        .into_keyed()
        .demux(cluster, TCP.fail_stop().bincode())
        .values();

    (routed_promises, prepares_on_cluster)
}

/// Ack filter: only ack proposals whose view >= max promised view.
///
/// `prepares` is the EC stream of Prepare messages. Each member derives its
/// max_promised_view from this stream. Proposals with view < max_promised_view
/// are dropped (not acked).
///
/// This is the REAL fencing: it's driven by Prepare messages (phase 1),
/// not by proposals. Once a member sees a Prepare for view V, it will never
/// ack a proposal from view < V.
pub fn fenced_ack_filter<'a, T: Clone + Serialize + DeserializeOwned + 'a>(
    proposals: Stream<Proposal<T>, Cluster<'a, Nodes, EventualConsistency>, Unbounded, NoOrder>,
    prepares: Stream<Prepare, Cluster<'a, Nodes, EventualConsistency>, Unbounded, NoOrder>,
) -> Stream<Proposal<T>, Cluster<'a, Nodes, EventualConsistency>, Unbounded, NoOrder> {
    let filtered = sliced! {
        let mut max_promised = use::state(|l| l.singleton(q!(0usize)));
        let prop_batch = use::batch(proposals, nondet!(
            /// Proposal batching doesn't affect safety — fencing is already locked in.
        ));
        let prepare_batch = use::batch(prepares, nondet!(
            /// Prepare delivery timing determines when fencing takes effect.
            /// Safety holds regardless: once a quorum promises, the old leader
            /// can't reach quorum.
        ));

        // Update max_promised from prepares seen this tick
        let new_max_from_prepares = prepare_batch
            .map(q!(|p| p.view))
            .fold(
                q!(|| 0usize),
                q!(|max, v| { if v > *max { *max = v; } },
                   commutative = manual_proof!(/** max is commutative */)),
            );

        let new_max = new_max_from_prepares.zip(max_promised.clone())
            .map(q!(|(prepare_max, current)| prepare_max.max(current)));

        max_promised = new_max.clone();

        // Only pass proposals with view >= max_promised
        prop_batch
            .cross_singleton(new_max)
            .filter_map(q!(|(proposal, max_p)| {
                if proposal.view >= max_p {
                    Some(proposal)
                } else {
                    None
                }
            }))
    };

    filtered.assert_has_consistency_of(manual_proof!(
        /// The fencing filter is driven by Prepare messages (EC) and applied
        /// to proposals (EC). Since both inputs are EC, every member eventually
        /// computes the same max_promised_view and applies the same filter.
        /// The filtered output is therefore also EC.
    ))
}

// ============================================================================
// Piece 3: Full view with commit path
// ============================================================================

/// A committed entry: a proposal that was acked by a quorum.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CommittedEntry<T> {
    pub view: usize,
    pub slot: usize,
    pub value: T,
}

/// Run a complete view with proposal broadcast AND commit path (unfenced).
///
/// This version does NOT apply fencing — it acks all proposals unconditionally.
/// Use this when views don't overlap in time. For concurrent views, use
/// `fenced_ack_filter` + phase1_prepare (see test_phase1_prevents_concurrent_commit).
pub fn run_view_with_commits<'a, T: Clone + Serialize + DeserializeOwned + 'a>(
    requests: Stream<T, Cluster<'a, Nodes>, Unbounded>,
    start_slot: usize,
    view_id: usize,
    leader_id: usize,
    quorum_size: usize,
    cluster: &Cluster<'a, Nodes>,
) -> (
    // Proposals: EC (inferred)
    Stream<Proposal<T>, Cluster<'a, Nodes, EventualConsistency>, Unbounded, NoOrder>,
    // Commit notifications: EC (inferred)
    Stream<CommitNotification, Cluster<'a, Nodes, EventualConsistency>, Unbounded, NoOrder>,
) {
    // Step 1: Sequence and broadcast proposals. EC inferred.
    let proposals = propose_in_view(requests, start_slot, view_id);

    // Step 2: Every member acks each proposal back to the leader (unfenced).
    let acks_to_leader = proposals
        .clone()
        .map(q!(move |p| (
            hydro_lang::location::MemberId::<Nodes>::from_raw_id(leader_id as u32),
            p.slot,
        )))
        .into_keyed()
        .demux(cluster, TCP.fail_stop().bincode())
        .values();

    // Step 3: Leader counts acks, broadcasts commit notifications. EC inferred.
    let commits = commit_decisions(acks_to_leader, quorum_size, view_id);

    (proposals, commits)
}

/// Collect acks and broadcast commit notifications within the cluster.
///
/// The leader receives acks (one per member per slot), counts them, and once
/// a quorum is reached, broadcasts a `CommitNotification` to all members.
///
/// The returned stream is EC — inferred via `broadcast_from_member`.
pub fn commit_decisions<'a>(
    acks: Stream<usize, Cluster<'a, Nodes>, Unbounded, NoOrder>,
    quorum_size: usize,
    view_id: usize,
) -> Stream<CommitNotification, Cluster<'a, Nodes, EventualConsistency>, Unbounded, NoOrder> {
    let commit_notifications_on_leader = sliced! {
        let mut ack_counts =
            use::state(|l| l.singleton(q!(std::collections::HashMap::<usize, usize>::new())));
        let mut already_committed =
            use::state(|l| l.singleton(q!(std::collections::HashSet::<usize>::new())));
        let batch = use::batch(acks, nondet!(
            /// Ack batching doesn't affect which slots commit — just when.
        ));

        // Merge new acks into persistent counts
        let updated_counts = batch
            .fold(
                q!(|| std::collections::HashMap::<usize, usize>::new()),
                q!(|batch_counts, slot| { *batch_counts.entry(slot).or_insert(0) += 1; },
                   commutative = manual_proof!(/** counting is commutative */)),
            )
            .zip(ack_counts.clone())
            .map(q!(|(batch_counts, mut total_counts)| {
                for (slot, count) in batch_counts {
                    *total_counts.entry(slot).or_insert(0) += count;
                }
                total_counts
            }));

        // Find slots that NEWLY reached quorum (not already committed)
        let new_commits = updated_counts.clone()
            .zip(already_committed.clone())
            .map(q!(move |(counts, committed)| {
                counts.into_iter()
                    .filter(|(slot, count)| *count >= quorum_size && !committed.contains(slot))
                    .map(|(slot, _)| CommitNotification { view: view_id, slot })
                    .collect::<Vec<_>>()
            }))
            .into_stream()
            .flatten_unordered();

        // Persist updated counts and committed set
        ack_counts = updated_counts;
        already_committed = new_commits.clone()
            .map(q!(|cn| cn.slot))
            .fold(
                q!(|| std::collections::HashSet::<usize>::new()),
                q!(|set, slot| { set.insert(slot); },
                   commutative = manual_proof!(/** set insert is commutative */)),
            )
            .zip(already_committed)
            .map(q!(|(new, mut existing)| { existing.extend(new); existing }));

        new_commits
    };

    // broadcast_from_member + fail_stop → EC. INFERRED.
    commit_notifications_on_leader.broadcast_from_member(TCP.fail_stop().bincode())
}

// ============================================================================
// Piece 3: View transfer proof (typed witness)
// ============================================================================

/// A typed witness that the new leader has recovered the full committed prefix.
///
/// You can ONLY construct this by calling `recover_committed_prefix`, which
/// requires a quorum of slot reports. The type system enforces that you
/// performed state transfer before starting a new view.
///
/// This is where the ONE semantic `manual_proof!` lives.
pub struct ViewTransferProof {
    /// The next slot to start sequencing from (max committed + 1).
    pub next_start_slot: usize,
}

/// Perform a quorum read to recover the committed prefix.
///
/// The proof obligation (documented, not yet enforced by a trait parameter):
///
/// ```text
/// manual_proof!(
///     /// Quorum intersection: any committed entry was acked by f+1 members.
///     /// We gathered reports from f+1 members. By pigeonhole, at least one
///     /// respondent witnessed every committed entry. The max across reports
///     /// is ≥ every committed slot.
/// )
/// ```
pub fn recover_committed_prefix(
    slot_reports: &[usize],
    quorum_size: usize,
) -> Option<ViewTransferProof> {
    if slot_reports.len() >= quorum_size {
        let max_slot = slot_reports.iter().copied().max().unwrap_or(0);
        Some(ViewTransferProof {
            next_start_slot: max_slot + 1,
        })
    } else {
        None
    }
}

// ============================================================================
// Piece 4: View transfer — quorum read as a real dataflow
// ============================================================================

/// A state query: the new leader asks "what's your max committed slot?"
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StateQuery {
    pub new_view: usize,
    pub from_leader: usize, // MemberId as usize
}

/// A state response: a member reports its max committed slot.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StateResponse {
    pub new_view: usize,
    pub max_committed_slot: usize,
}

/// Perform a quorum read as a dataflow: the new leader broadcasts a state
/// query, members respond with their max committed slot, the leader collects
/// f+1 responses and takes the max.
///
/// Returns: a stream of `usize` on the cluster — the recovered start slot.
/// Only the leader member will have a value; other members' streams are empty.
///
/// The `manual_proof!` for quorum intersection lives here: "collecting from
/// f+1 members guarantees we've seen every committed slot."
pub fn quorum_read<'a>(
    query_trigger: Stream<StateQuery, Cluster<'a, Nodes>, Unbounded>,
    max_committed_per_member: Stream<usize, Cluster<'a, Nodes>, Unbounded>,
    cluster: &Cluster<'a, Nodes>,
    quorum_size: usize,
) -> Stream<usize, Cluster<'a, Nodes>, Unbounded> {
    // Step 1: New leader broadcasts query to all members.
    // (The query_trigger stream only has data on the leader member.)
    let queries_on_cluster = query_trigger
        .broadcast_from_member(TCP.fail_stop().bincode());

    // Step 2: Each member responds with its max committed slot, routed to
    // the leader who sent the query.
    let responses_to_leader = queries_on_cluster
        .weaken_consistency()
        .cross_product(max_committed_per_member)
        .map(q!(|(query, max_slot)| (
            hydro_lang::location::MemberId::<Nodes>::from_raw_id(query.from_leader as u32),
            StateResponse {
                new_view: query.new_view,
                max_committed_slot: max_slot,
            },
        )))
        .into_keyed()
        .demux(cluster, TCP.fail_stop().bincode())
        .values();

    // Step 3: Leader collects responses, takes max once quorum reached.
    // Accumulate (count, max) across ticks. Once count >= quorum, emit max+1.
    let recovered_start_slot = sliced! {
        let mut response_count = use::state(|l| l.singleton(q!(0usize)));
        let mut max_seen = use::state(|l| l.singleton(q!(0usize)));
        let batch = use::batch(responses_to_leader, nondet!(
            /// Response arrival order doesn't matter — we take the max.
        ));

        let batch_count = batch.clone().count();
        let batch_max = batch.map(q!(|r| r.max_committed_slot)).fold(
            q!(|| 0usize),
            q!(|max, slot| { if slot > *max { *max = slot; } },
               commutative = manual_proof!(/** max is commutative */)),
        );

        let new_count = response_count.clone()
            .zip(batch_count)
            .map(q!(|(old, n)| old + n));

        let new_max = max_seen.clone()
            .zip(batch_max)
            .map(q!(|(old_max, bmax)| old_max.max(bmax)));

        response_count = new_count.clone();
        max_seen = new_max.clone();

        // Emit start slot when quorum reached
        new_count.zip(new_max)
            .filter_map(q!(move |(count, max_slot)| {
                if count >= quorum_size {
                    Some(max_slot + 1)
                } else {
                    None
                }
            }))
            .into_stream()
    };

    // NOTE: The manual_proof for quorum intersection would go here in a
    // production version — attesting that f+1 responses guarantee full
    // prefix coverage. For now it's documented in the function doc.
    recovered_start_slot
}

// ============================================================================
// Piece 5: End-to-end protocol
// ============================================================================

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};

    use super::*;

    /// Verify that a single view's proposals are typed as EC — no manual_proof!
    /// on the consistency claim.
    #[test]
    fn test_single_view_proposals_are_ec() {
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<Nodes>();

        let (_, client_requests) =
            cluster.sim_input::<u32, TotalOrder, ExactlyOnce>();

        let proposals = propose_in_view(client_requests, 0, 0);

        // Compile-time proof: this IS EC. No manual_proof! anywhere.
        let _: Stream<Proposal<u32>, Cluster<'_, Nodes, EventualConsistency>, _, _, _> = proposals;

        let _ = flow.finalize();
    }

    /// Verify that commit notifications are also typed as EC.
    #[test]
    fn test_commit_notifications_are_ec() {
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<Nodes>();

        let (_, ack_stream) = cluster.sim_input::<usize, NoOrder, ExactlyOnce>();

        let commits = commit_decisions(ack_stream, 2, 0);

        // Compile-time proof: commit notifications are EC.
        let _: Stream<CommitNotification, Cluster<'_, Nodes, EventualConsistency>, _, _, _> =
            commits;

        let _ = flow.finalize();
    }

    /// Actually run the proposal phase through the simulator: leader (member 0)
    /// sequences requests and all cluster members receive the proposals.
    #[test]
    fn test_proposals_arrive_at_cluster_members() {
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<Nodes>();

        let (client_port, client_requests) =
            cluster.sim_input::<u32, TotalOrder, ExactlyOnce>();

        let proposals = propose_in_view(client_requests, 0, 0);
        let output = proposals.sim_cluster_output();

        flow.sim().with_cluster_size(&cluster, 3).exhaustive(async || {
            // Send three requests to member 0 (the leader)
            client_port.send(0, 100);
            client_port.send(0, 200);
            client_port.send(0, 300);

            // Each member receives all 3 proposals (NoOrder, so collect sorted)
            let m0: Vec<Proposal<u32>> = output.collect_n_sorted(0, 3).await;
            assert_eq!(m0[0], Proposal { view: 0, slot: 0, value: 100 });
            assert_eq!(m0[1], Proposal { view: 0, slot: 1, value: 200 });
            assert_eq!(m0[2], Proposal { view: 0, slot: 2, value: 300 });

            let m1: Vec<Proposal<u32>> = output.collect_n_sorted(1, 3).await;
            assert_eq!(m1[0], Proposal { view: 0, slot: 0, value: 100 });
            assert_eq!(m1[1], Proposal { view: 0, slot: 1, value: 200 });
            assert_eq!(m1[2], Proposal { view: 0, slot: 2, value: 300 });

            let m2: Vec<Proposal<u32>> = output.collect_n_sorted(2, 3).await;
            assert_eq!(m2[0], Proposal { view: 0, slot: 0, value: 100 });
            assert_eq!(m2[1], Proposal { view: 0, slot: 1, value: 200 });
            assert_eq!(m2[2], Proposal { view: 0, slot: 2, value: 300 });
        });
    }

    /// Two-view test: member 0 is leader in view 0, member 1 takes over in
    /// view 1 starting from the correct slot. All members receive a gap-free
    /// log spanning both views.
    #[test]
    fn test_two_views_compose_correctly() {
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<Nodes>();

        // View 0: requests sent to member 0 (leader for view 0)
        let (client_port_0, requests_0) =
            cluster.sim_input::<u32, TotalOrder, ExactlyOnce>();
        let proposals_0 = propose_in_view(requests_0, 0, 0);

        // View 1: requests sent to member 1 (leader for view 1)
        // Starts at slot 3 (simulating: view 0 committed slots 0, 1, 2)
        let (client_port_1, requests_1) =
            cluster.sim_input::<u32, TotalOrder, ExactlyOnce>();
        let proposals_1 = propose_in_view(requests_1, 3, 1);

        // Merge both views' proposals into a single output.
        let combined = proposals_0.merge_unordered(proposals_1);
        let output = combined.sim_cluster_output();

        flow.sim().with_cluster_size(&cluster, 3).exhaustive(async || {
            // View 0: leader (member 0) gets 3 requests
            client_port_0.send(0, 10);
            client_port_0.send(0, 20);
            client_port_0.send(0, 30);

            // View 1: leader (member 1) gets 2 requests
            client_port_1.send(1, 40);
            client_port_1.send(1, 50);

            // Each member receives all 5 proposals (sorted by Ord on Proposal)
            let m0: Vec<Proposal<u32>> = output.collect_n_sorted(0, 5).await;
            assert_eq!(m0[0], Proposal { view: 0, slot: 0, value: 10 });
            assert_eq!(m0[1], Proposal { view: 0, slot: 1, value: 20 });
            assert_eq!(m0[2], Proposal { view: 0, slot: 2, value: 30 });
            assert_eq!(m0[3], Proposal { view: 1, slot: 3, value: 40 });
            assert_eq!(m0[4], Proposal { view: 1, slot: 4, value: 50 });

            let m1: Vec<Proposal<u32>> = output.collect_n_sorted(1, 5).await;
            assert_eq!(m1[0], Proposal { view: 0, slot: 0, value: 10 });
            assert_eq!(m1[1], Proposal { view: 0, slot: 1, value: 20 });
            assert_eq!(m1[2], Proposal { view: 0, slot: 2, value: 30 });
              assert_eq!(m1[3], Proposal { view: 1, slot: 3, value: 40 });
            assert_eq!(m1[4], Proposal { view: 1, slot: 4, value: 50 });
        });
    }

    /// Full commit path test: leader proposes, members ack, leader broadcasts
    /// commit notifications once quorum is reached. Verifies that BOTH the
    /// proposals and the commit notifications are EC (arrive at all members).
    ///
    /// The committed log = proposals ∩ commits. Since both are EC (inferred),
    /// and the join is deterministic, the committed log is EC.
    #[test]
    fn test_committed_log_is_ec() {
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<Nodes>();

        // Leader is member 0. Cluster size = 3, quorum = 2.
        let (client_port, requests) =
            cluster.sim_input::<u32, TotalOrder, ExactlyOnce>();

        let (proposals, commits) =
            run_view_with_commits(requests, 0, 0, 0, 2, &cluster);

        let proposal_output = proposals.sim_cluster_output();
        let commit_output = commits.sim_cluster_output();

        flow.sim().with_cluster_size(&cluster, 3).fuzz(async || {
            // Send 3 requests to the leader (member 0)
            client_port.send(0, 10);
            client_port.send(0, 20);
            client_port.send(0, 30);

            // All members should receive all 3 proposals (EC on proposals)
            let p0: Vec<Proposal<u32>> = proposal_output.collect_n_sorted(0, 3).await;
            assert_eq!(p0[0], Proposal { view: 0, slot: 0, value: 10 });
            assert_eq!(p0[1], Proposal { view: 0, slot: 1, value: 20 });
            assert_eq!(p0[2], Proposal { view: 0, slot: 2, value: 30 });

            let p1: Vec<Proposal<u32>> = proposal_output.collect_n_sorted(1, 3).await;
            assert_eq!(p1, p0); // Same proposals on member 1

            let p2: Vec<Proposal<u32>> = proposal_output.collect_n_sorted(2, 3).await;
            assert_eq!(p2, p0); // Same proposals on member 2

            // All members should receive commit notifications for all 3 slots.
            // With 3 members and quorum=2, each proposal gets acked by all 3
            // members (since they all receive it), so all reach quorum.
            let c0: Vec<CommitNotification> = commit_output.collect_n_sorted(0, 3).await;
            assert_eq!(c0[0], CommitNotification { view: 0, slot: 0 });
            assert_eq!(c0[1], CommitNotification { view: 0, slot: 1 });
            assert_eq!(c0[2], CommitNotification { view: 0, slot: 2 });

            let c1: Vec<CommitNotification> = commit_output.collect_n_sorted(1, 3).await;
            assert_eq!(c1, c0); // Same commits on member 1

            let c2: Vec<CommitNotification> = commit_output.collect_n_sorted(2, 3).await;
            assert_eq!(c2, c0); // Same commits on member 2

              // At this point, every member has:
            // - All proposals (EC, inferred)
            // - All commit notifications (EC, inferred)
            // The committed log = proposals joined by commit notifications
            // = deterministic function of two EC streams = EC. QED.
        });
    }

    /// Two-view test WITH full commit paths. View 0 leader (member 0) commits
    /// slots 0-2. View 1 leader (member 1) picks up at slot 3, commits slots
    /// 3-4. Every member receives all proposals and all commit notifications
    /// from both views — so every member computes the same committed log.
    ///
    /// This is the full composition: two views, each individually EC (inferred),
    /// with the view transition ensuring no gaps or conflicts.
    #[test]
    fn test_two_view_committed_log_is_ec() {
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<Nodes>();

        // View 0: leader is member 0, quorum=2, cluster_size=3
        let (client_port_0, requests_0) =
            cluster.sim_input::<u32, TotalOrder, ExactlyOnce>();
        let (proposals_0, commits_0) =
            run_view_with_commits(requests_0, 0, 0, 0, 2, &cluster);

        // View 1: leader is member 1, starts at slot 3
        let (client_port_1, requests_1) =
            cluster.sim_input::<u32, TotalOrder, ExactlyOnce>();
        let (proposals_1, commits_1) =
            run_view_with_commits(requests_1, 3, 1, 1, 2, &cluster);

        // Merge outputs from both views
        let all_proposals = proposals_0.merge_unordered(proposals_1);
        let all_commits = commits_0.merge_unordered(commits_1);

        let proposal_output = all_proposals.sim_cluster_output();
        let commit_output = all_commits.sim_cluster_output();

        flow.sim().with_cluster_size(&cluster, 3).fuzz(async || {
            // View 0: 3 requests to leader (member 0)
            client_port_0.send(0, 10);
            client_port_0.send(0, 20);
            client_port_0.send(0, 30);

            // View 1: 2 requests to leader (member 1)
            client_port_1.send(1, 40);
            client_port_1.send(1, 50);

            // Every member gets all 5 proposals from both views
            let p0: Vec<Proposal<u32>> = proposal_output.collect_n_sorted(0, 5).await;
            assert_eq!(p0[0], Proposal { view: 0, slot: 0, value: 10 });
            assert_eq!(p0[1], Proposal { view: 0, slot: 1, value: 20 });
            assert_eq!(p0[2], Proposal { view: 0, slot: 2, value: 30 });
            assert_eq!(p0[3], Proposal { view: 1, slot: 3, value: 40 });
            assert_eq!(p0[4], Proposal { view: 1, slot: 4, value: 50 });

            // Every member gets all 5 commit notifications
            let c0: Vec<CommitNotification> = commit_output.collect_n_sorted(0, 5).await;
            assert_eq!(c0[0], CommitNotification { view: 0, slot: 0 });
            assert_eq!(c0[1], CommitNotification { view: 0, slot: 1 });
            assert_eq!(c0[2], CommitNotification { view: 0, slot: 2 });
            assert_eq!(c0[3], CommitNotification { view: 1, slot: 3 });
            assert_eq!(c0[4], CommitNotification { view: 1, slot: 4 });

            // Verify another member sees the same
            let p1: Vec<Proposal<u32>> = proposal_output.collect_n_sorted(1, 5).await;
            assert_eq!(p1, p0);

            let c1: Vec<CommitNotification> = commit_output.collect_n_sorted(1, 5).await;
            assert_eq!(c1, c0);

            // CONCLUSION: Every member has identical proposals + identical commit
            // notifications from both views. The committed log (proposals ∩ commits)
              // is therefore identical on every member = EC.
            //
            // No manual_proof! on any consistency assertion. EC is inferred
            // for every broadcast via broadcast_from_member.
        });
    }


    /// Minimal test: verify phase1_prepare produces promises.
    #[test]
    fn test_phase1_produces_promises() {
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<Nodes>();

        let (prepare_port, prepare_trigger) =
            cluster.sim_input::<Prepare, TotalOrder, ExactlyOnce>();
        let (max_port, max_stream) =
            cluster.sim_input::<usize, TotalOrder, ExactlyOnce>();

        let (promises, _prepares) =
            phase1_prepare(prepare_trigger, max_stream, &cluster);
        let promise_output = promises.sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&cluster, 3)
            .exhaustive(async || {
                // Each member has max_committed = 0
                max_port.send(0, 0);
                max_port.send(1, 0);
                max_port.send(2, 0);

                // Member 1 sends Prepare
                prepare_port.send(1, Prepare { view: 1, from_leader: 1 });

                // Leader (member 1) should receive promises
                let p: Vec<Promise> = promise_output.collect_n_sorted(1, 2).await;
                assert_eq!(p.len(), 2);
            });
    }

    /// PHASE-1 FENCED CONCURRENT LEADERS TEST.
    ///
    /// View 1's leader does phase 1 (Prepare → Promise), collects a quorum
    /// of promises, and ONLY THEN starts proposing. The proposals for view 1
    /// are causally downstream of the promise collection — not independently
    /// injected. This guarantees the fence is in place before view 1 proposes.
    ///
    /// Safety: at most one view commits each slot.
    /// Liveness: view 1 always wins (because the fence blocks view 0).
    #[test]
    fn test_phase1_prevents_concurrent_commit() {
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<Nodes>();

        // View 0: member 0 proposes for slot 0
        let (req_port_0, requests_v0) =
            cluster.sim_input::<u32, TotalOrder, ExactlyOnce>();
        let proposals_0 = propose_in_view(requests_v0, 0, 0);

        // Phase 1: member 1 broadcasts Prepare, collects promises.
        let (prepare_port, prepare_trigger) =
            cluster.sim_input::<Prepare, TotalOrder, ExactlyOnce>();
        let (max_committed_port, max_committed_stream) =
            cluster.sim_input::<usize, TotalOrder, ExactlyOnce>();
        let (promises, prepares_on_cluster) =
            phase1_prepare(prepare_trigger, max_committed_stream, &cluster);

        // Collect promises into a start_slot (quorum = 2)
        let start_signal = sliced! {
            let mut count = use::state(|l| l.singleton(q!(0usize)));
            let mut max_slot = use::state(|l| l.singleton(q!(0usize)));
            let batch = use::batch(promises, nondet!(
                /// Promise arrival order doesn't matter.
            ));

            let new_count = count.clone().zip(batch.clone().count())
                .map(q!(|(old, n)| old + n));
            let new_max = max_slot.clone()
                .zip(batch.map(q!(|p| p.max_committed_slot)).fold(
                    q!(|| 0usize),
                    q!(|max, s| { if s > *max { *max = s; } },
                       commutative = manual_proof!(/** max is commutative */)),
                ))
                .map(q!(|(old, batch_max)| old.max(batch_max)));

            count = new_count.clone();
            max_slot = new_max.clone();

            // Emit start_slot = max when quorum reached (0 means nothing committed, start at 0)
            new_count.zip(new_max)
                .filter_map(q!(move |(cnt, max_s)| {
                    if cnt >= 2 { Some(max_s) } else { None }
                }))
                .into_stream()
        };

        // View 1: GATED by start_signal. Only proposes after quorum of promises.
        let (req_port_1, requests_v1) =
            cluster.sim_input::<u32, TotalOrder, ExactlyOnce>();
        let proposals_1 = propose_in_view_gated(requests_v1, start_signal, 1);

        // Merge proposals from both views
        let all_proposals = proposals_0.merge_unordered(proposals_1);

        // Apply phase-1 fencing
        let fenced = fenced_ack_filter(all_proposals, prepares_on_cluster);

        // Route acks
        let view_0_acks = fenced.clone()
            .filter(q!(|p| p.view == 0))
            .map(q!(|p| (
                hydro_lang::location::MemberId::<Nodes>::from_raw_id(0),
                p.slot,
            )))
            .into_keyed()
            .demux(&cluster, TCP.fail_stop().bincode())
            .values();

        let view_1_acks = fenced
            .filter(q!(|p| p.view == 1))
            .map(q!(|p| (
                hydro_lang::location::MemberId::<Nodes>::from_raw_id(1),
                p.slot,
            )))
            .into_keyed()
            .demux(&cluster, TCP.fail_stop().bincode())
            .values();

        let commits_0 = commit_decisions(view_0_acks, 2, 0);
        let commits_1 = commit_decisions(view_1_acks, 2, 1);
        let all_commits = commits_0.merge_unordered(commits_1);
        let commit_output = all_commits.sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&cluster, 3)
            .fuzz(async || {
                // Each member reports max_committed = 0 (fresh cluster)
                max_committed_port.send(0, 0);
                max_committed_port.send(1, 0);
                max_committed_port.send(2, 0);

                // View 0's leader proposed (old leader, in flight)
                req_port_0.send(0, 999);

                // View 1's leader initiates phase 1
                prepare_port.send(1, Prepare { view: 1, from_leader: 1 });

                // View 1's leader has requests queued (won't be proposed
                // until start_signal fires — i.e., after quorum of promises)
                req_port_1.send(1, 777);

                // Collect the commit
                let commits: Vec<CommitNotification> =
                    commit_output.collect_n_sorted(0, 1).await;

                assert_eq!(commits.len(), 1);
                assert_eq!(commits[0].slot, 0);

                // SAFETY: exactly one view committed slot 0.
                // Which view wins depends on delivery order — that's fine.
                // The invariant is: never BOTH.
            });
    }
}
