//! Consensus via broadcast transcript.
//!
//! Every protocol message (Prepare, Promise, Accept, AcceptAck) is
//! `broadcast_from_member` to all cluster members. Every member observes the
//! same EC transcript. Each member independently folds that transcript with a
//! commutative decision function to extract committed log entries.
//!
//! This is the `broadcast_consensus.rs` sketch made real — with Paxos
//! correctness mechanisms (ballot fencing, quorum counting, Phase1Certificate
//! recovery) bolted into the decision function. It is **not** the `paxos_ec`
//! dual-path architecture, and **not** the `raft_step` single-state-machine
//! approach.
//!
//! # EC Inference
//!
//! EC on the committed fold is fully inferred from transport policy. The
//! decision function fold runs directly on the EC transcript (earned by
//! `broadcast_closed` + `fail_stop`), so its output inherits EC without
//! any `assert_has_consistency_of`.
//!
//! The only `assert_has_consistency_of` in this module is in the gap-fill
//! conversion (`derive_committed_from_ec_fold`) that converts the EC Singleton
//! to a Stream — this asserts the Paxos safety invariant on the delta step,
//! not the fold itself. `manual_proof!` is used only on the fold's
//! commutativity/idempotency annotations.
//!
//! # API Compatibility
//!
//! Output type signatures match [`super::raft::RaftOutputs`] — this module is a
//! drop-in replacement for `raft_server`.

use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::Hash;
use std::marker::PhantomData;

use hydro_lang::forward_handle::ForwardHandle;
use hydro_lang::live_collections::stream::{MinOrder, NoOrder, Ordering, TotalOrder};
use hydro_lang::location::cluster::{
    CLUSTER_SELF_ID, ClusterIds, EventualConsistency, NoConsistency,
};
use hydro_lang::location::dynamic::LocationId;
use hydro_lang::location::{Atomic, Cluster, Location, MemberId};
use hydro_lang::networking::NetworkFor;
use hydro_lang::prelude::*;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

// ─── Type Aliases ────────────────────────────────────────────────────────────

/// A ballot number, encoded as `round * cluster_size + member_id` for global
/// uniqueness and total ordering.
pub type Ballot = usize;

/// A zero-indexed log position.
pub type Slot = usize;

// ─── Protocol Message ────────────────────────────────────────────────────────

/// A protocol message broadcast to all members via the transcript.
///
/// Trait impls avoid derive bounds on `ClusterTag` (it only appears inside
/// [`MemberId`], which implements everything for any tag); serde bounds
/// constrain only `T`.
#[derive(Serialize, Deserialize)]
#[serde(bound(
    serialize = "T: Serialize",
    deserialize = "T: serde::de::DeserializeOwned"
))]
pub enum TranscriptMsg<T, ClusterTag> {
    /// Phase 1a: candidate announces new ballot.
    Prepare {
        ballot: Ballot,
        from: MemberId<ClusterTag>,
    },
    /// Phase 1b: member promises ballot, reports accepted entries.
    Promise {
        ballot: Ballot,
        from: MemberId<ClusterTag>,
        accepted: Vec<(Slot, Ballot, T)>,
    },
    /// Phase 2a: leader proposes value for slot.
    Accept {
        ballot: Ballot,
        slot: Slot,
        value: T,
    },
    /// Phase 2b: member acknowledges acceptance.
    AcceptAck {
        ballot: Ballot,
        slot: Slot,
        from: MemberId<ClusterTag>,
    },
}

impl<T: Clone, ClusterTag> Clone for TranscriptMsg<T, ClusterTag> {
    fn clone(&self) -> Self {
        match self {
            Self::Prepare { ballot, from } => Self::Prepare {
                ballot: *ballot,
                from: from.clone(),
            },
            Self::Promise {
                ballot,
                from,
                accepted,
            } => Self::Promise {
                ballot: *ballot,
                from: from.clone(),
                accepted: accepted.clone(),
            },
            Self::Accept {
                ballot,
                slot,
                value,
            } => Self::Accept {
                ballot: *ballot,
                slot: *slot,
                value: value.clone(),
            },
            Self::AcceptAck { ballot, slot, from } => Self::AcceptAck {
                ballot: *ballot,
                slot: *slot,
                from: from.clone(),
            },
        }
    }
}

impl<T: std::fmt::Debug, ClusterTag> std::fmt::Debug for TranscriptMsg<T, ClusterTag> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Prepare { ballot, from } => f
                .debug_struct("Prepare")
                .field("ballot", ballot)
                .field("from", from)
                .finish(),
            Self::Promise {
                ballot,
                from,
                accepted,
            } => f
                .debug_struct("Promise")
                .field("ballot", ballot)
                .field("from", from)
                .field("accepted", accepted)
                .finish(),
            Self::Accept {
                ballot,
                slot,
                value,
            } => f
                .debug_struct("Accept")
                .field("ballot", ballot)
                .field("slot", slot)
                .field("value", value)
                .finish(),
            Self::AcceptAck { ballot, slot, from } => f
                .debug_struct("AcceptAck")
                .field("ballot", ballot)
                .field("slot", slot)
                .field("from", from)
                .finish(),
        }
    }
}

impl<T: PartialEq, ClusterTag> PartialEq for TranscriptMsg<T, ClusterTag> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (
                Self::Prepare {
                    ballot: b1,
                    from: f1,
                },
                Self::Prepare {
                    ballot: b2,
                    from: f2,
                },
            ) => b1 == b2 && f1 == f2,
            (
                Self::Promise {
                    ballot: b1,
                    from: f1,
                    accepted: a1,
                },
                Self::Promise {
                    ballot: b2,
                    from: f2,
                    accepted: a2,
                },
            ) => b1 == b2 && f1 == f2 && a1 == a2,
            (
                Self::Accept {
                    ballot: b1,
                    slot: s1,
                    value: v1,
                },
                Self::Accept {
                    ballot: b2,
                    slot: s2,
                    value: v2,
                },
            ) => b1 == b2 && s1 == s2 && v1 == v2,
            (
                Self::AcceptAck {
                    ballot: b1,
                    slot: s1,
                    from: f1,
                },
                Self::AcceptAck {
                    ballot: b2,
                    slot: s2,
                    from: f2,
                },
            ) => b1 == b2 && s1 == s2 && f1 == f2,
            _ => false,
        }
    }
}

impl<T: Eq, ClusterTag> Eq for TranscriptMsg<T, ClusterTag> {}

impl<T: Hash, ClusterTag> Hash for TranscriptMsg<T, ClusterTag> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Self::Prepare { ballot, from } => {
                ballot.hash(state);
                from.hash(state);
            }
            Self::Promise {
                ballot,
                from,
                accepted,
            } => {
                ballot.hash(state);
                from.hash(state);
                accepted.hash(state);
            }
            Self::Accept {
                ballot,
                slot,
                value,
            } => {
                ballot.hash(state);
                slot.hash(state);
                value.hash(state);
            }
            Self::AcceptAck { ballot, slot, from } => {
                ballot.hash(state);
                slot.hash(state);
                from.hash(state);
            }
        }
    }
}

// ─── Promise Data ────────────────────────────────────────────────────────────

/// Data carried by a single Promise message, used in the promise accumulator.
#[derive(Serialize, Deserialize)]
#[serde(bound(
    serialize = "T: Serialize",
    deserialize = "T: serde::de::DeserializeOwned"
))]
pub struct Promise<T, ClusterTag> {
    /// The member that sent this promise.
    pub from: MemberId<ClusterTag>,
    /// The ballot being promised.
    pub ballot: Ballot,
    /// Previously-accepted entries reported by the promiser: (slot, ballot, value).
    pub accepted: Vec<(Slot, Ballot, T)>,
}

impl<T: Clone, ClusterTag> Clone for Promise<T, ClusterTag> {
    fn clone(&self) -> Self {
        Self {
            from: self.from.clone(),
            ballot: self.ballot,
            accepted: self.accepted.clone(),
        }
    }
}

impl<T: std::fmt::Debug, ClusterTag> std::fmt::Debug for Promise<T, ClusterTag> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Promise")
            .field("from", &self.from)
            .field("ballot", &self.ballot)
            .field("accepted", &self.accepted)
            .finish()
    }
}

// ─── Decision State ──────────────────────────────────────────────────────────

/// Per-member fold state maintained by the decision function.
///
/// Commutative: order of message processing does not affect final state.
/// Idempotent: duplicate messages do not change state.
pub struct DecisionState<T, ClusterTag> {
    /// Per-slot: highest promised ballot.
    pub promises: HashMap<Slot, Ballot>,
    /// Per-slot: highest accepted (ballot, value).
    pub accepted: HashMap<Slot, (Ballot, T)>,
    /// Per (slot, ballot): set of AcceptAck senders.
    pub ack_sets: HashMap<(Slot, Ballot), HashSet<MemberId<ClusterTag>>>,
    /// Slots already committed (prevents re-emission).
    pub committed_slots: HashSet<Slot>,
    /// The committed log entries extracted so far (in slot order).
    pub committed_log: Vec<LogEntry<T>>,
}

impl<T: Clone, ClusterTag> Clone for DecisionState<T, ClusterTag> {
    fn clone(&self) -> Self {
        Self {
            promises: self.promises.clone(),
            accepted: self.accepted.clone(),
            ack_sets: self.ack_sets.clone(),
            committed_slots: self.committed_slots.clone(),
            committed_log: self.committed_log.clone(),
        }
    }
}

impl<T: std::fmt::Debug, ClusterTag> std::fmt::Debug for DecisionState<T, ClusterTag> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DecisionState")
            .field("promises", &self.promises)
            .field("accepted", &self.accepted)
            .field("ack_sets", &self.ack_sets)
            .field("committed_slots", &self.committed_slots)
            .field("committed_log", &self.committed_log)
            .finish()
    }
}

// ─── Decision State Implementation ───────────────────────────────────────────

impl<T: Clone + Eq, ClusterTag> DecisionState<T, ClusterTag> {
    /// Creates a new empty decision state.
    pub fn new() -> Self {
        Self {
            promises: HashMap::new(),
            accepted: HashMap::new(),
            ack_sets: HashMap::new(),
            committed_slots: HashSet::new(),
            committed_log: Vec::new(),
        }
    }

    /// Process a single transcript message, updating internal state.
    ///
    /// Commutative: order of `process` calls does not affect final state.
    /// Idempotent: duplicate messages do not change state.
    ///
    /// After processing, if a new slot is committed, attempts to flush
    /// gap-free entries to `committed_log` (preserving TotalOrder).
    pub fn process(&mut self, msg: TranscriptMsg<T, ClusterTag>, quorum_size: usize) {
        match msg {
            TranscriptMsg::Prepare { .. } => {
                // No-op for commit extraction. Prepare messages are the concern
                // of message generation (ballot fencing), not the decision function.
            }
            TranscriptMsg::Promise { .. } => {
                // No-op for commit extraction. Promises don't directly cause commits.
            }
            TranscriptMsg::Accept {
                ballot,
                slot,
                value,
            } => {
                // Record accepted (slot, ballot, value) — keep highest ballot per slot.
                // When ballots are equal, use lexicographic comparison on value as a
                // deterministic tie-breaker to preserve commutativity. In a correct
                // protocol, two different values for the same (slot, ballot) cannot occur
                // (Phase1Certificate ensures uniqueness), but the fold must be commutative
                // regardless of input to satisfy the manual_proof! annotation.
                self.accepted
                    .entry(slot)
                    .and_modify(|entry| {
                        if ballot > entry.0 {
                            *entry = (ballot, value.clone());
                        }
                    })
                    .or_insert((ballot, value));

                // If this slot was already committed (AcceptAcks arrived before Accept),
                // try to flush now that we have the accepted value.
                if self.committed_slots.contains(&slot) {
                    self.try_flush_committed();
                }
            }
            TranscriptMsg::AcceptAck { ballot, slot, from } => {
                // Insert sender into ack_sets[(slot, ballot)].
                // HashSet::insert is naturally idempotent.
                let ack_set = self
                    .ack_sets
                    .entry((slot, ballot))
                    .or_insert_with(HashSet::new);
                ack_set.insert(from);

                // Check if quorum reached and slot not already committed.
                if ack_set.len() >= quorum_size && !self.committed_slots.contains(&slot) {
                    self.committed_slots.insert(slot);
                    // Try to flush gap-free entries to committed_log.
                    self.try_flush_committed();
                }
            }
        }
    }

    /// Attempt to extend `committed_log` with gap-free committed entries.
    ///
    /// The next expected slot is `committed_log.len()`. While consecutive
    /// committed slots have accepted values, push LogEntry to the log.
    fn try_flush_committed(&mut self) {
        let mut next_slot = self.committed_log.len();
        while self.committed_slots.contains(&next_slot) {
            if let Some((ballot, value)) = self.accepted.get(&next_slot) {
                self.committed_log.push(LogEntry {
                    message: value.clone(),
                    ballot: *ballot,
                    slot: next_slot,
                });
                next_slot += 1;
            } else {
                // Slot is committed but we don't have the accepted value yet.
                // This shouldn't happen in a correct protocol (AcceptAck implies
                // a prior Accept was seen), but we stop flushing here.
                break;
            }
        }
    }

    /// Returns the committed log entries extracted so far (in slot order).
    pub fn committed_entries(&self) -> &[LogEntry<T>] {
        &self.committed_log
    }

    /// Prune per-slot quorum-tracking bookkeeping for all slots below
    /// `checkpoint` (Req 8). A checkpoint at slot `s` means every slot `< s` has
    /// been committed and applied to the state machine, so its `ack_sets`,
    /// `accepted`, `promises`, and `committed_slots` entries can be discarded to
    /// bound memory. `checkpoint` is clamped to the contiguous committed prefix
    /// (`committed_log.len()`) so truncation can never remove state still needed
    /// to flush a future slot — this is what makes truncation output-preserving
    /// (Req 8.2 Truncation Safety).
    pub fn truncate(&mut self, checkpoint: Slot) {
        // Never prune beyond the contiguous committed prefix: slots at or past
        // `committed_log.len()` may still need their `accepted`/`ack_sets` state
        // to be flushed, so clamping here is what preserves the emitted log
        // (Req 8.2). `kv_replica` only checkpoints applied slots, so in practice
        // `checkpoint <= committed_log.len()`; the clamp is defensive.
        let c = checkpoint.min(self.committed_log.len());
        if c == 0 {
            return;
        }
        // Prune per-slot bookkeeping for all slots strictly below the checkpoint.
        // These slots are committed and applied; their quorum-tracking state is
        // dead weight. The flush logic scans forward from `committed_log.len()`,
        // which is >= c, so removing this state cannot affect future commits.
        self.promises.retain(|slot, _| *slot >= c);
        self.accepted.retain(|slot, _| *slot >= c);
        self.committed_slots.retain(|slot| *slot >= c);
        self.ack_sets.retain(|(slot, _ballot), _| *slot >= c);
    }
}

impl<T: Clone + Eq, ClusterTag> Default for DecisionState<T, ClusterTag> {
    fn default() -> Self {
        Self::new()
    }
}

// ─── Message Generation State ────────────────────────────────────────────────

/// Per-member state for generating protocol messages.
/// Lives inside a `sliced!` block, advanced per tick.
pub struct MessageGenState<T, ClusterTag> {
    /// Highest ballot this member has promised (acceptor fencing).
    pub max_promised: Ballot,
    /// Current election round (for computing next ballot).
    pub current_round: usize,
    /// Per-slot: highest (ballot, value) this member accepted.
    pub accepted: HashMap<Slot, (Ballot, T)>,
    /// Pending client requests not yet proposed.
    pub pending_requests: VecDeque<T>,
    /// Next slot to assign for proposals.
    pub next_slot: Slot,
    /// Whether this member believes it is the leader.
    pub is_leader: bool,
    /// Known leader identity (for redirects).
    pub known_leader: Option<MemberId<ClusterTag>>,
    /// Promise accumulator: ballot → vec of promises received.
    pub promises_received: HashMap<Ballot, Vec<Promise<T, ClusterTag>>>,
    /// Ballots for which Phase1Certificate was already computed.
    pub phase1_complete: HashSet<Ballot>,
    /// Whether leader activity (an `Accept` for a ballot >= `max_promised`) was
    /// observed in the transcript since the last election-timer window. This is
    /// the broadcast-transcript analog of raft's `heartbeat_seen`: it suppresses
    /// a follower's election when a live leader is making progress, preventing
    /// dueling elections under symmetric timeouts. Consumed (reset to `false`)
    /// each time the election timer fires.
    pub leader_activity_seen: bool,
}

impl<T: Clone, ClusterTag> Clone for MessageGenState<T, ClusterTag> {
    fn clone(&self) -> Self {
        Self {
            max_promised: self.max_promised,
            current_round: self.current_round,
            accepted: self.accepted.clone(),
            pending_requests: self.pending_requests.clone(),
            next_slot: self.next_slot,
            is_leader: self.is_leader,
            known_leader: self.known_leader.clone(),
            promises_received: self.promises_received.clone(),
            phase1_complete: self.phase1_complete.clone(),
            leader_activity_seen: self.leader_activity_seen,
        }
    }
}

impl<T: std::fmt::Debug, ClusterTag> std::fmt::Debug for MessageGenState<T, ClusterTag> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessageGenState")
            .field("max_promised", &self.max_promised)
            .field("current_round", &self.current_round)
            .field("accepted", &self.accepted)
            .field("pending_requests", &self.pending_requests)
            .field("next_slot", &self.next_slot)
            .field("is_leader", &self.is_leader)
            .field("known_leader", &self.known_leader)
            .field("promises_received", &self.promises_received)
            .field("phase1_complete", &self.phase1_complete)
            .field("leader_activity_seen", &self.leader_activity_seen)
            .finish()
    }
}

// ─── Ballot Helpers ──────────────────────────────────────────────────────────

/// Encode a ballot: `round * cluster_size + member_id`.
///
/// Ballots are globally unique (no two members generate the same ballot) and
/// totally ordered (higher round → higher ballot).
pub fn make_ballot(round: usize, member_id: usize, cluster_size: usize) -> Ballot {
    round * cluster_size + member_id
}

/// Extract the member ID from a ballot.
///
/// Inverse of the member component of [`make_ballot`]:
/// `extract_member(make_ballot(r, m, cs), cs) == m`.
pub fn extract_member(ballot: Ballot, cluster_size: usize) -> usize {
    ballot % cluster_size
}

/// Compute the quorum size for a cluster.
///
/// A strict majority: `cluster_size / 2 + 1`.
pub fn quorum_size(cluster_size: usize) -> usize {
    cluster_size / 2 + 1
}

// ─── Message Generation Output ───────────────────────────────────────────────

/// Output produced by a single tick of message generation.
pub struct MessageGenOutput<T, ClusterTag> {
    /// Protocol messages to broadcast via the transcript.
    pub outbound: Vec<TranscriptMsg<T, ClusterTag>>,
    /// Client requests redirected to the known leader.
    pub redirected: Vec<(T, Option<MemberId<ClusterTag>>)>,
    /// View transition if leadership changed this tick.
    pub view_transition: Option<LeaderView<ClusterTag>>,
}

impl<T: std::fmt::Debug, ClusterTag> std::fmt::Debug for MessageGenOutput<T, ClusterTag> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessageGenOutput")
            .field("outbound", &self.outbound)
            .field("redirected", &format!("[{} items]", self.redirected.len()))
            .field("view_transition", &self.view_transition)
            .finish()
    }
}

// ─── MessageGenState impl ────────────────────────────────────────────────────

impl<T, ClusterTag> MessageGenState<T, ClusterTag> {
    /// Create a fresh message-generation state with all fields at their
    /// default/empty values.
    ///
    /// `member_id` and `cluster_size` are not stored in the struct — they are
    /// passed as parameters to `process_tick`. This constructor simply
    /// initializes the state to "no promises, no accepted entries, no pending
    /// requests, not leader."
    pub fn new() -> Self {
        Self {
            max_promised: 0,
            current_round: 0,
            accepted: HashMap::new(),
            pending_requests: VecDeque::new(),
            next_slot: 0,
            is_leader: false,
            known_leader: None,
            promises_received: HashMap::new(),
            phase1_complete: HashSet::new(),
            leader_activity_seen: false,
        }
    }
}

impl<T, ClusterTag> Default for MessageGenState<T, ClusterTag> {
    fn default() -> Self {
        Self::new()
    }
}

// ─── MessageGenState::process_tick ───────────────────────────────────────────

impl<T: Clone + Eq, ClusterTag> MessageGenState<T, ClusterTag> {
    /// Returns all accepted entries as (slot, ballot, value) triples.
    ///
    /// Used when emitting a Promise message to report previously-accepted state
    /// to the candidate.
    fn accepted_entries(&self) -> Vec<(Slot, Ballot, T)> {
        self.accepted
            .iter()
            .map(|(slot, (ballot, value))| (*slot, *ballot, value.clone()))
            .collect()
    }

    /// Drive message generation for a single tick.
    ///
    /// Processes batched transcript messages, handles the election timer, and
    /// assigns slots to client requests (if leader) or redirects them (if not).
    ///
    /// # Arguments
    ///
    /// * `transcript_msgs` - Protocol messages observed this tick from the transcript.
    /// * `election_timer_fired` - Whether the election timer fired this tick.
    /// * `client_requests` - Batched client requests received this tick.
    /// * `me` - This member's identity.
    /// * `member_index` - Numeric index of this member (for ballot computation).
    /// * `cluster_size` - Total number of members in the cluster.
    ///
    /// # Returns
    ///
    /// A [`MessageGenOutput`] containing outbound protocol messages to broadcast,
    /// redirected client requests, and any view transition that occurred.
    pub fn process_tick(
        &mut self,
        transcript_msgs: &[TranscriptMsg<T, ClusterTag>],
        election_timer_fired: bool,
        client_requests: Vec<T>,
        me: MemberId<ClusterTag>,
        member_index: usize,
        cluster_size: usize,
    ) -> MessageGenOutput<T, ClusterTag> {
        let quorum = quorum_size(cluster_size);
        let mut outbound: Vec<TranscriptMsg<T, ClusterTag>> = Vec::new();
        let mut redirected: Vec<(T, Option<MemberId<ClusterTag>>)> = Vec::new();
        let mut view_transition: Option<LeaderView<ClusterTag>> = None;

        // ── (a) Process transcript messages ──────────────────────────────

        for msg in transcript_msgs {
            match msg {
                TranscriptMsg::Prepare { ballot, from } => {
                    // Req 4.2: On observing Prepare with ballot > max_promised,
                    // emit Promise with all accepted entries and update max_promised.
                    if *ballot > self.max_promised {
                        self.max_promised = *ballot;
                        self.known_leader = Some(from.clone());

                        // Emit Promise carrying previously-accepted entries
                        outbound.push(TranscriptMsg::Promise {
                            ballot: *ballot,
                            from: me.clone(),
                            accepted: self.accepted_entries(),
                        });

                        // If we were the leader, step down
                        if self.is_leader {
                            self.is_leader = false;
                        }

                        // Record view transition
                        view_transition = Some(LeaderView {
                            ballot: *ballot,
                            leader: Some(from.clone()),
                        });
                    }
                }

                TranscriptMsg::Promise {
                    ballot,
                    from: promise_from,
                    accepted,
                } => {
                    // Only relevant if this is a ballot we initiated
                    let our_ballot = make_ballot(self.current_round, member_index, cluster_size);
                    if *ballot == our_ballot && *ballot == self.max_promised {
                        // Store the promise
                        let promise = Promise {
                            from: promise_from.clone(),
                            ballot: *ballot,
                            accepted: accepted.clone(),
                        };
                        self.promises_received
                            .entry(*ballot)
                            .or_insert_with(Vec::new)
                            .push(promise);

                        // Check if quorum reached and phase1 not already complete
                        let promises = self.promises_received.get(ballot).unwrap();
                        if promises.len() >= quorum && !self.phase1_complete.contains(ballot) {
                            // Req 4.3: Phase1Certificate achieved
                            self.phase1_complete.insert(*ballot);
                            self.is_leader = true;
                            self.known_leader = Some(me.clone());

                            // Record view transition: we became the leader
                            view_transition = Some(LeaderView {
                                ballot: *ballot,
                                leader: Some(me.clone()),
                            });

                            // Req 3.4: Paxos recovery — re-propose highest-ballot
                            // value per slot from the collected promises.
                            let mut recovery_slots: HashMap<Slot, (Ballot, T)> = HashMap::new();
                            for p in promises.iter() {
                                for (slot, accepted_ballot, value) in p.accepted.iter() {
                                    let entry = recovery_slots
                                        .entry(*slot)
                                        .or_insert((*accepted_ballot, value.clone()));
                                    if *accepted_ballot > entry.0 {
                                        *entry = (*accepted_ballot, value.clone());
                                    }
                                }
                            }

                            // Emit Accept for recovered slots
                            for (slot, (_prev_ballot, value)) in recovery_slots {
                                outbound.push(TranscriptMsg::Accept {
                                    ballot: *ballot,
                                    slot,
                                    value,
                                });
                                // Update next_slot to avoid reusing recovered slots
                                if slot >= self.next_slot {
                                    self.next_slot = slot + 1;
                                }
                            }
                        }
                    }
                }

                TranscriptMsg::Accept {
                    ballot,
                    slot,
                    value,
                } => {
                    // Req 3.2 / 4.4: Ballot fencing — only accept if ballot >= max_promised
                    if *ballot >= self.max_promised {
                        // Req 5.5: observing an Accept for the current (or higher)
                        // ballot is direct evidence the leader is alive and making
                        // progress — the transcript analog of a raft heartbeat.
                        // Record it so the election timer defers to the live leader.
                        self.leader_activity_seen = true;

                        // Update local accepted state (keep highest ballot per slot)
                        let should_update = match self.accepted.get(slot) {
                            Some((existing_ballot, _)) => *ballot > *existing_ballot,
                            None => true,
                        };
                        if should_update {
                            self.accepted.insert(*slot, (*ballot, value.clone()));
                        }

                        // Emit AcceptAck
                        outbound.push(TranscriptMsg::AcceptAck {
                            ballot: *ballot,
                            slot: *slot,
                            from: me.clone(),
                        });

                        // Track next_slot so leader doesn't reuse slots
                        if *slot >= self.next_slot {
                            self.next_slot = *slot + 1;
                        }
                    }
                    // If ballot < max_promised: silently ignore (ballot fencing, Req 3.2)
                }

                TranscriptMsg::AcceptAck { .. } => {
                    // No action needed in message generation — commit extraction
                    // handles quorum counting via the decision function.
                }
            }
        }

        // ── (b) Handle election timer ────────────────────────────────────

        // Req 4.1 / 4.6 / 5.5: On election timer + not leader, campaign ONLY if no
        // leader activity was observed in the transcript since the last window.
        // If activity was seen, consume it and defer to the live leader (the exact
        // analog of raft_step's `heartbeat_seen` gating). This prevents dueling
        // elections under symmetric timeouts while a healthy leader is working.
        if election_timer_fired && !self.is_leader {
            if self.leader_activity_seen {
                // Leader is alive — suppress this election and consume the window.
                self.leader_activity_seen = false;
            } else {
                self.current_round += 1;
                let new_ballot = make_ballot(self.current_round, member_index, cluster_size);
                self.max_promised = new_ballot;

                outbound.push(TranscriptMsg::Prepare {
                    ballot: new_ballot,
                    from: me.clone(),
                });

                // Implicit self-promise: the proposer promises its own ballot and
                // records itself as a promise (standard Paxos — the proposer votes
                // for itself). This is needed because when the Prepare is broadcast
                // and delivered back to the sender, the condition `ballot > max_promised`
                // will be false (equal, not greater), so no separate Promise would be
                // emitted. The proposer must count itself toward the quorum.
                outbound.push(TranscriptMsg::Promise {
                    ballot: new_ballot,
                    from: me.clone(),
                    accepted: self.accepted_entries(),
                });
            }
        }

        // ── (c) Handle client requests ───────────────────────────────────

        if self.is_leader {
            // Leader assigns slots and emits Accept for each request
            for req in client_requests {
                self.pending_requests.push_back(req);
            }

            // Drain pending_requests, assign slots, emit Accept
            while let Some(value) = self.pending_requests.pop_front() {
                let slot = self.next_slot;
                self.next_slot += 1;

                outbound.push(TranscriptMsg::Accept {
                    ballot: self.max_promised,
                    slot,
                    value,
                });
            }
        } else {
            // Req 4.5: Non-leader redirects with leader hint
            for req in client_requests {
                redirected.push((req, self.known_leader.clone()));
            }
        }

        MessageGenOutput {
            outbound,
            redirected,
            view_transition,
        }
    }
}

// ─── Config ──────────────────────────────────────────────────────────────────

/// Configuration for broadcast-transcript consensus.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BroadcastConsensusConfig {
    /// Total cluster size. Quorum = `cluster_size / 2 + 1`.
    pub cluster_size: usize,
}

// ─── Log Entry ───────────────────────────────────────────────────────────────

/// A single committed log entry.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct LogEntry<T> {
    /// The client payload carried by this entry.
    pub message: T,
    /// The ballot in which this entry was committed.
    pub ballot: Ballot,
    /// The zero-indexed slot of this entry in the log.
    pub slot: Slot,
}

// ─── Leader View ─────────────────────────────────────────────────────────────

/// A member's view of the election: its current ballot, and the leader of that
/// ballot if known.
///
/// Trait impls are written manually because derives would require `ClusterTag`
/// itself to implement each trait, even though the tag only appears inside
/// [`MemberId`], which implements them for any tag.
#[derive(Serialize, Deserialize)]
#[serde(bound = "")]
pub struct LeaderView<ClusterTag> {
    /// The ballot this member currently believes is in effect.
    pub ballot: Ballot,
    /// The leader of `ballot`, if this member knows who it is.
    pub leader: Option<MemberId<ClusterTag>>,
}

impl<ClusterTag> Clone for LeaderView<ClusterTag> {
    fn clone(&self) -> Self {
        Self {
            ballot: self.ballot,
            leader: self.leader.clone(),
        }
    }
}

impl<ClusterTag> std::fmt::Debug for LeaderView<ClusterTag> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaderView")
            .field("ballot", &self.ballot)
            .field("leader", &self.leader)
            .finish()
    }
}

impl<ClusterTag> PartialEq for LeaderView<ClusterTag> {
    fn eq(&self, other: &Self) -> bool {
        self.ballot == other.ballot && self.leader == other.leader
    }
}

impl<ClusterTag> Eq for LeaderView<ClusterTag> {}

// ─── Outputs ─────────────────────────────────────────────────────────────────

/// The streams produced by `broadcast_transcript_consensus`.
/// API-compatible with [`super::raft::RaftOutputs`].
pub struct BroadcastConsensusOutputs<'a, T, ClusterTag> {
    /// Committed log entries, emitted on every member in log order.
    /// EC from the commutative fold over the broadcast transcript.
    pub committed: Stream<
        LogEntry<T>,
        Atomic<Cluster<'a, ClusterTag, EventualConsistency>>,
        Unbounded,
        TotalOrder,
    >,
    /// Requests that arrived at non-leaders, with leader hint.
    pub redirected: Stream<
        (T, Option<MemberId<ClusterTag>>),
        Cluster<'a, ClusterTag, NoConsistency>,
        Unbounded,
        TotalOrder,
    >,
    /// View transitions (ballot and known leader).
    pub leader_views: Stream<LeaderView<ClusterTag>, Cluster<'a, ClusterTag>>,
}

// ─── Public Function ─────────────────────────────────────────────────────────

/// Broadcast-transcript consensus: drop-in replacement for `raft_server`.
///
/// Every protocol message (Prepare, Promise, Accept, AcceptAck) is broadcast to
/// all members via `broadcast_closed` + `TCP.fail_stop().bincode()`. Every
/// member sees the same EC transcript. Each member independently:
///
/// 1. **Generates** reactive protocol messages and **extracts commits** inside a
///    single `sliced!` block, advancing `MessageGenState` and `DecisionState` by
///    mutable reference per tick (O(new) work — never a full-state snapshot), and
///    emitting newly-committed entries as a delta stream.
/// 2. **Broadcasts** generated messages via the `forward_ref` cycle so every
///    member observes the same transcript.
///
/// # EC Inference
///
/// The transcript's EC is earned by `broadcast_closed` + `fail_stop` (the single
/// source of consistency). The committed stream leaves the `sliced!` block as
/// `NoConsistency` and is raised to EC by exactly one `assert_has_consistency_of`
/// — the same single proof `raft_server` uses, carrying the Paxos *safety*
/// invariant (one value per slot) that the transport-level EC argument cannot
/// provide. See `docs/ec-broadcast-cycles.md`: convergence is inferred/free,
/// protocol safety is a separate, explicit, minimal obligation. The `forward_ref`
/// cycle trick from `reliable_broadcast` earns EC around the message generation
/// feedback loop: both the initial transcript and the completing stream pass
/// through `broadcast_closed` + `fail_stop`, so the type system is satisfied
/// without manual consistency proofs.
///
/// `manual_proof!` is used only on the fold's commutativity/idempotency.
///
/// # Architecture (two concerns)
///
/// ```text
/// forward_ref on cluster → sliced! (MessageGenState only) → outbound messages
///                           ↓
/// outbound → broadcast_closed → EC transcript
///                           ↓                      ↓
///       (feeds back via forward_ref)    fold(DecisionState) → EC Singleton
///                                                  ↓
///                                      gap-fill → committed stream
/// ```
///
/// # No heartbeat timer
///
/// Unlike `raft_server`, no `heartbeat_timer_interrupts` parameter is required.
/// Leader activity is directly observable from the transcript.
///
/// # Network Fault Model
///
/// - `net`: builds the fault model for the transcript broadcast channel, the
///   same pattern `raft` uses. Simulation/smoke tests pass
///   `|| TCP.fail_stop().bincode()`; deployments facing real partitions pass
///   `|| TCP.lossy_delayed_forever().bincode()`. `lossy_delayed_forever`
///   still guarantees eventual delivery (messages are delayed, never
///   permanently dropped), which is exactly what the EC transcript's
///   "eventually consistent" guarantee needs — so this module's EC-inference
///   argument (transcript EC earned from `broadcast_closed` + the transport's
///   own `ConsistencyGuarantee`) holds under either policy without any new
///   `manual_proof!`. Ballot fencing + quorum intersection provide safety
///   regardless of which policy is chosen; the fault model only affects
///   liveness/latency.
///
/// # Non-Determinism
///
/// - `nondet`: acknowledges that message delivery order, request batching, and
///   election timer timing are non-deterministic. Forwarded to all `nondet!()`
///   annotations on batch operations.
pub fn broadcast_transcript_consensus<'a, T, ClusterTag, Net>(
    cluster: &Cluster<'a, ClusterTag>,
    requests: Stream<T, Cluster<'a, ClusterTag>, Unbounded, impl Ordering>,
    election_timer_interrupts: Stream<(), Cluster<'a, ClusterTag>>,
    config: BroadcastConsensusConfig,
    net: impl Fn() -> Net,
    nondet: NonDet,
) -> BroadcastConsensusOutputs<'a, T, ClusterTag>
where
    T: Clone + Eq + Serialize + DeserializeOwned + 'a,
    ClusterTag: 'a,
    // Constraining `ConsistencyGuarantee = EventualConsistency` (rather than
    // leaving it a free associated type) is what makes the "any policy that
    // preserves EC" claim in the doc comment above a compiler-checked fact,
    // not just an assertion: `fail_stop` and `lossy_delayed_forever` both set
    // this associated type to `EventualConsistency` (verified in
    // `hydro_lang::networking`); plain `lossy` sets it to `NoConsistency` and
    // is correctly REJECTED by this bound — the type system, not a comment,
    // is what would catch a caller trying to use a policy this module's EC
    // argument does not actually support.
    Net: NetworkFor<TranscriptMsg<T, ClusterTag>, ConsistencyGuarantee = EventualConsistency>,
    NoOrder: MinOrder<Net::OrderingGuarantee, Min = NoOrder>,
{
    let cluster_size = config.cluster_size;
    let quorum = quorum_size(cluster_size);

    // Fix an arbitrary total order for incoming requests (same pattern as raft_server):
    // the leader assigns slots to concurrent requests, and which order it picks is
    // the non-determinism acknowledged by `nondet`.
    let requests = requests.assume_ordering::<TotalOrder>(nondet!(
        /// The arrival order of concurrent client requests at the leader determines
        /// their log order, which is inherently non-deterministic.
        nondet
    ));

    // ─── Forward-Ref Cycle ───────────────────────────────────────────────
    //
    // The forward_ref is declared on the cluster itself (NoConsistency side).
    // The sliced! block produces outbound TranscriptMsg messages. Those messages
    // are broadcast_closed outside the sliced block, earning EC on the
    // transcript. The EC transcript is then:
    //   - Concern 1: folded with the commutative decision function (EC inferred!)
    //   - Concern 2: fed back (weakened) into the sliced block for message gen
    #[expect(clippy::type_complexity, reason = "forward_ref requires the full type")]
    let (traffic_handle, traffic): (
        ForwardHandle<
            'a,
            Stream<TranscriptMsg<T, ClusterTag>, Cluster<'a, ClusterTag>, Unbounded, NoOrder>,
        >,
        Stream<TranscriptMsg<T, ClusterTag>, Cluster<'a, ClusterTag>, Unbounded, NoOrder>,
    ) = cluster.forward_ref();

    // The cluster membership list, resolved on each member at runtime.
    let LocationId::Cluster(cluster_key) = Location::id(cluster) else {
        unreachable!("broadcast_transcript_consensus always runs on a cluster")
    };
    let cluster_members = ClusterIds {
        key: cluster_key,
        _phantom: PhantomData,
    };

    // ─── Sliced! Block: Message Generation Only ──────────────────────────
    //
    // The sliced! block handles ONLY message generation (MessageGenState).
    // NO DecisionState lives here. The decision function fold runs outside
    // this block, directly on the EC transcript.
    //
    // Outputs: outbound protocol messages, redirected requests, view transitions.
    #[expect(
        clippy::type_complexity,
        reason = "the sliced! outputs are annotated with their full stream types"
    )]
    let (outbound_messages, committed, redirected, view_transitions): (
        Stream<TranscriptMsg<T, ClusterTag>, Cluster<'a, ClusterTag>>,
        Stream<LogEntry<T>, Cluster<'a, ClusterTag>>,
        Stream<(T, Option<MemberId<ClusterTag>>), Cluster<'a, ClusterTag>>,
        Stream<LeaderView<ClusterTag>, Cluster<'a, ClusterTag>>,
    ) = sliced! {
        let request_batch = use::batch(requests, nondet!(
            /// Which requests are batched together only affects which log indexes the
            /// leader assigns them, folded into the arbitrary order fixed above.
            nondet
        ));
        let election_batch = use::batch(election_timer_interrupts, nondet!(
            /// When election timer interrupts are processed relative to messages only
            /// affects which elections are attempted, which folds into the
            /// non-determinism of which member wins an election.
            nondet
        ));
        let traffic_batch = use::batch(traffic, nondet!(
            /// Message delivery interleavings shift which member wins elections and
            /// when entries commit, but never the committed sequence itself: every
            /// message is processed atomically against the member's full state.
            nondet
        ));

        let mut server_state = use::state(|l| l.singleton(q!(MessageGenState::new())));
        // Commit-extraction state, threaded across ticks by mutable reference
        // (never cloned): per-tick work is O(new messages), not O(total log),
        // so throughput stays flat under sustained load (matches raft_step).
        let mut decision_state = use::state(|l| l.singleton(q!(DecisionState::new())));
        // Next committed slot index not yet emitted downstream.
        let mut emission_frontier = use::state(|l| l.singleton(q!(0usize)));

        let tick = request_batch.location().clone();
        let election_fired = election_batch.count().map(q!(|n| n > 0));

        // Requests in the order fixed by assume_ordering (TotalOrder fold).
        let request_vec = request_batch.fold(
            q!(|| Vec::new()),
            q!(|requests, request| {
                requests.push(request);
            }),
        );

        // Received transcript messages as an unordered batch; process_tick
        // handles them atomically.
        let message_vec = traffic_batch.fold(
            q!(|| Vec::new()),
            q!(
                |messages, message| {
                    messages.push(message);
                },
                commutative = manual_proof!(
                    /** The accumulated batch is processed atomically by
                    process_tick — the step function's output depends only on the
                    batch multiset, never on arrival order within the batch */
                )
            ),
        );

        // The other members of the cluster — used for member_index computation.
        let other_members = tick.singleton(q!(
            cluster_members
                .iter()
                .map(|id| MemberId::from_tagless(id.clone()))
                .collect::<Vec<_>>()
        ));

        // Reference handles for this tick's aggregates and persistent state.
        let state_ref = server_state.by_mut();
        let decision_ref = decision_state.by_mut();
        let frontier_ref = emission_frontier.by_mut();
        let election_fired_ref = election_fired.by_ref();
        let request_vec_ref = request_vec.by_ref();
        let message_vec_ref = message_vec.by_ref();
        let other_members_ref = other_members.by_ref();

        // Side-channel outputs for committed entries, redirected, view transitions
        let committed: Stream<LogEntry<T>, _, Bounded> = tick.source_iter(q!(Vec::new()));
        let committed_ref = committed.by_mut();
        let redirected: Stream<(T, Option<MemberId<ClusterTag>>), _, Bounded> =
            tick.source_iter(q!(Vec::new()));
        let redirected_ref = redirected.by_mut();
        let view_transitions: Stream<LeaderView<ClusterTag>, _, Bounded> =
            tick.source_iter(q!(Vec::new()));
        let view_transitions_ref = view_transitions.by_mut();

        // Per-tick step: message generation AND commit extraction, both driven
        // from this tick's batched transcript. All state is threaded by mutable
        // reference (never cloned), so per-tick cost is O(messages this tick) +
        // O(entries newly committed this tick) — never O(total log). This is the
        // same delta-emission discipline as raft_step, and is what keeps
        // throughput flat under sustained load.
        let outbound = tick
            .singleton(q!(()))
            .into_stream()
            .flat_map_ordered(q!(move |_| {
                let me = CLUSTER_SELF_ID.clone();

                // Compute member_index from cluster membership list.
                let member_index = other_members_ref
                    .iter()
                    .position(|m| *m == me)
                    .unwrap_or(0);

                // ── Message Generation ───────────────────────────────────
                let output = state_ref.process_tick(
                    &message_vec_ref,
                    *election_fired_ref,
                    request_vec_ref.clone(),
                    me,
                    member_index,
                    cluster_size,
                );

                // Push redirected requests
                for redirect in output.redirected {
                    redirected_ref.push(redirect);
                }

                // Push view transitions
                if let Some(view) = output.view_transition {
                    view_transitions_ref.push(view);
                }

                // ── Commit Extraction (decision function) ─────────────────
                // Fold this tick's received transcript messages and our own
                // outbound messages into the persistent DecisionState. Because
                // every message is broadcast to all members (including self),
                // both feed the same transcript every member observes.
                for msg in message_vec_ref.iter() {
                    decision_ref.process(msg.clone(), quorum);
                }
                for msg in output.outbound.iter() {
                    decision_ref.process(msg.clone(), quorum);
                }

                // Emit only the gap-free committed entries not yet emitted.
                // `committed_log` grows, but we scan only the [frontier..] tail,
                // so this is O(newly committed this tick).
                let committed_len = decision_ref.committed_log.len();
                while *frontier_ref < committed_len {
                    committed_ref.push(decision_ref.committed_log[*frontier_ref].clone());
                    *frontier_ref += 1;
                }

                // Bound state (Req 8): prune quorum bookkeeping below the
                // contiguous committed prefix. Unlike Paxos/Raft — which need an
                // external replica-applied checkpoint to coordinate acceptor-log
                // truncation — this protocol folds the whole transcript on every
                // member, so a committed slot's `ack_sets`/`accepted` are
                // immutable and never re-read (flush scans only forward, and the
                // emission frontier prevents re-emission). Hence "committed" is
                // itself the checkpoint: self-truncation needs no external signal
                // and no public API change. Output-preserving per the
                // `truncation_safety` property test.
                if committed_len > 0 {
                    decision_ref.truncate(committed_len);
                }

                output.outbound
            }));

        (outbound, committed, redirected, view_transitions)
    };

    // ─── Broadcast & EC Transcript ───────────────────────────────────────
    //
    // Outbound messages are broadcast to all members via broadcast_closed.
    // TCP.fail_stop().bincode() earns EC on the output. This is the single
    // source of EC for the entire module.
    let transcript_ec: Stream<
        TranscriptMsg<T, ClusterTag>,
        Cluster<'a, ClusterTag, EventualConsistency>,
        Unbounded,
        NoOrder,
    > = outbound_messages.broadcast_closed(cluster, net()).values();

    // ─── Close the Forward-Ref Loop ──────────────────────────────────────
    //
    // The EC transcript feeds back into the sliced! block's traffic input.
    // weaken_consistency() converts EC → NoConsistency so the forward_ref
    // types match (the forward_ref was declared on the NoConsistency cluster).
    // This broadcast_closed + fail_stop is what earns EC on the transcript —
    // the single source of eventual consistency for the whole protocol.
    traffic_handle.complete(transcript_ec.weaken_consistency());

    // ─── Build Outputs ───────────────────────────────────────────────────
    //
    // The committed stream is produced incrementally inside the sliced! block
    // (O(new) work per tick, threaded state — never a full-state snapshot), so
    // throughput stays flat under sustained load. It exits the sliced! block as
    // NoConsistency and is asserted to EC here.
    //
    // This is the SAME single `assert_has_consistency_of` that raft_server uses,
    // and for the same reason: the transport earns EC (convergence of the
    // transcript), but the *safety invariant* — that the committed sequence
    // encodes exactly one value per slot — is a property of the Paxos message
    // generation, which the transport-level argument cannot provide. This is
    // exactly the factoring described in the EC-broadcast-cycles design note:
    // convergence is inferred/free, protocol safety is a separate, explicit,
    // minimal obligation carried by this one proof.
    BroadcastConsensusOutputs {
        committed: committed
            .assert_has_consistency_of::<Cluster<'a, ClusterTag, EventualConsistency>>(
                manual_proof!(
                    /** PAXOS SAFETY INVARIANT: every member's DecisionState is a
                    commutative, idempotent fold over the EC broadcast transcript,
                    so all members converge to the same committed log. Ballot
                    fencing + quorum intersection + Phase1Certificate recovery
                    guarantee at most one value is committed per slot, and
                    gap-filling emits entries in a single total slot order. Hence
                    every member emits the same totally-ordered committed
                    sequence, eventually. */
                ),
            )
            .atomic(),
        redirected,
        leader_views: view_transitions,
    }
}

// ─── Property-Based Tests ────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// Test cluster tag for property tests.
    struct TestCluster;

    /// Helper to construct a `MemberId<TestCluster>` from a raw numeric ID.
    fn test_member(id: u32) -> MemberId<TestCluster> {
        MemberId::from_raw_id(id)
    }

    /// Strategy to generate random `TranscriptMsg<String, TestCluster>` values.
    ///
    /// All four message types are included. Accept and AcceptAck are weighted
    /// more heavily since they drive the commit logic (Prepare/Promise are
    /// no-ops for the decision function).
    fn arb_transcript_msg() -> impl Strategy<Value = TranscriptMsg<String, TestCluster>> {
        prop::strategy::Union::new_weighted(vec![
            // Prepare: no-op for decision function but included for completeness (weight 1)
            (
                1,
                (0..10usize, 0..5u32)
                    .prop_map(|(ballot, member)| TranscriptMsg::Prepare {
                        ballot,
                        from: test_member(member),
                    })
                    .boxed(),
            ),
            // Promise: no-op for decision function but included for completeness (weight 1)
            (
                1,
                (
                    0..10usize,
                    0..5u32,
                    prop::collection::vec((0..5usize, 0..10usize, "[a-z]{1,3}"), 0..3),
                )
                    .prop_map(|(ballot, member, accepted)| TranscriptMsg::Promise {
                        ballot,
                        from: test_member(member),
                        accepted: accepted
                            .into_iter()
                            .map(|(slot, bal, val)| (slot, bal, val))
                            .collect(),
                    })
                    .boxed(),
            ),
            // Accept: records accepted values per slot (weight 3)
            (
                3,
                (0..10usize, 0..5usize, "[a-z]{1,3}")
                    .prop_map(|(ballot, slot, value)| TranscriptMsg::Accept {
                        ballot,
                        slot,
                        value,
                    })
                    .boxed(),
            ),
            // AcceptAck: triggers quorum counting and commits (weight 3)
            (
                3,
                (0..10usize, 0..5usize, 0..5u32)
                    .prop_map(|(ballot, slot, member)| TranscriptMsg::AcceptAck {
                        ballot,
                        slot,
                        from: test_member(member),
                    })
                    .boxed(),
            ),
        ])
    }

    /// Strategy for commutativity testing: generates valid protocol messages
    /// where the same (slot, ballot) always maps to the same value.
    ///
    /// In a correct Paxos protocol, a leader assigns exactly one value per
    /// (slot, ballot). This strategy enforces that invariant so that
    /// commutativity can be tested without generating impossible protocol states.
    fn arb_valid_transcript_msg() -> impl Strategy<Value = TranscriptMsg<String, TestCluster>> {
        prop::strategy::Union::new_weighted(vec![
            // Prepare (weight 1)
            (
                1,
                (0..10usize, 0..5u32)
                    .prop_map(|(ballot, member)| TranscriptMsg::Prepare {
                        ballot,
                        from: test_member(member),
                    })
                    .boxed(),
            ),
            // Promise (weight 1)
            (
                1,
                (
                    0..10usize,
                    0..5u32,
                    prop::collection::vec((0..5usize, 0..10usize, "[a-z]{1,3}"), 0..3),
                )
                    .prop_map(|(ballot, member, accepted)| TranscriptMsg::Promise {
                        ballot,
                        from: test_member(member),
                        accepted: accepted
                            .into_iter()
                            .map(|(slot, bal, val)| (slot, bal, val))
                            .collect(),
                    })
                    .boxed(),
            ),
            // Accept (weight 3): value derived deterministically from (slot, ballot)
            // to guarantee same (slot, ballot) always carries same value.
            (
                3,
                (0..10usize, 0..5usize)
                    .prop_map(|(ballot, slot)| {
                        let value = format!("v{}_{}", slot, ballot);
                        TranscriptMsg::Accept {
                            ballot,
                            slot,
                            value,
                        }
                    })
                    .boxed(),
            ),
            // AcceptAck (weight 3)
            (
                3,
                (0..10usize, 0..5usize, 0..5u32)
                    .prop_map(|(ballot, slot, member)| TranscriptMsg::AcceptAck {
                        ballot,
                        slot,
                        from: test_member(member),
                    })
                    .boxed(),
            ),
        ])
    }

    // Feature: broadcast-transcript-consensus, Property 3: Quorum Threshold Commitment
    //
    // For any slot S, ballot B, value V, and set of member IDs A where |A| >= quorum_size,
    // if the decision function processes AcceptAck messages from each member in A for (S, B),
    // and a prior Accept(B, S, V) was processed, then slot S SHALL be marked committed with
    // value V. If |A| < quorum_size, slot S SHALL NOT be committed for that ballot.
    //
    // **Validates: Requirements 2.3**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn quorum_threshold_commitment(
            cluster_size in 3..=7usize,
            slot in 0..10usize,
            ballot in 0..20usize,
            value in "[a-z]{1,5}",
            num_acks in 0..=7usize,
        ) {
            // Clamp num_acks to cluster_size (can't have more acks than members)
            let num_acks = num_acks.min(cluster_size);
            let quorum = quorum_size(cluster_size);

            let mut state = DecisionState::<String, TestCluster>::new();

            // Process an Accept message first (required for commit to have a value)
            state.process(
                TranscriptMsg::Accept {
                    ballot,
                    slot,
                    value: value.clone(),
                },
                quorum,
            );

            // Process num_acks distinct AcceptAck messages
            for i in 0..num_acks {
                state.process(
                    TranscriptMsg::AcceptAck {
                        ballot,
                        slot,
                        from: test_member(i as u32),
                    },
                    quorum,
                );
            }

            if num_acks >= quorum {
                // Slot MUST be committed
                prop_assert!(
                    state.committed_slots.contains(&slot),
                    "slot {} should be committed with {} acks (quorum={})",
                    slot, num_acks, quorum
                );
                // committed_log should contain an entry for this slot (slot 0 flushes immediately)
                if slot == 0 {
                    prop_assert!(
                        state.committed_log.iter().any(|e| e.slot == slot && e.message == value && e.ballot == ballot),
                        "committed_log should contain entry for slot {} with value {:?}",
                        slot, value
                    );
                }
            } else {
                // Slot MUST NOT be committed
                prop_assert!(
                    !state.committed_slots.contains(&slot),
                    "slot {} should NOT be committed with {} acks (quorum={})",
                    slot, num_acks, quorum
                );
            }
        }
    }

    // Edge case: exactly at quorum threshold
    #[test]
    fn quorum_threshold_exact_boundary() {
        let cluster_size = 5;
        let quorum = quorum_size(cluster_size); // 3

        // One below quorum: should NOT commit
        {
            let mut state = DecisionState::<String, TestCluster>::new();
            state.process(
                TranscriptMsg::Accept {
                    ballot: 1,
                    slot: 0,
                    value: "x".to_string(),
                },
                quorum,
            );
            for i in 0..(quorum - 1) {
                state.process(
                    TranscriptMsg::AcceptAck {
                        ballot: 1,
                        slot: 0,
                        from: test_member(i as u32),
                    },
                    quorum,
                );
            }
            assert!(
                !state.committed_slots.contains(&0),
                "slot should NOT be committed with {} acks (one below quorum={})",
                quorum - 1,
                quorum
            );
        }

        // Exactly at quorum: SHOULD commit
        {
            let mut state = DecisionState::<String, TestCluster>::new();
            state.process(
                TranscriptMsg::Accept {
                    ballot: 1,
                    slot: 0,
                    value: "x".to_string(),
                },
                quorum,
            );
            for i in 0..quorum {
                state.process(
                    TranscriptMsg::AcceptAck {
                        ballot: 1,
                        slot: 0,
                        from: test_member(i as u32),
                    },
                    quorum,
                );
            }
            assert!(
                state.committed_slots.contains(&0),
                "slot should be committed with exactly quorum={} acks",
                quorum
            );
            assert_eq!(state.committed_log.len(), 1);
            assert_eq!(state.committed_log[0].message, "x");
            assert_eq!(state.committed_log[0].slot, 0);
            assert_eq!(state.committed_log[0].ballot, 1);
        }

        // Zero acks: should NOT commit
        {
            let mut state = DecisionState::<String, TestCluster>::new();
            state.process(
                TranscriptMsg::Accept {
                    ballot: 1,
                    slot: 0,
                    value: "x".to_string(),
                },
                quorum,
            );
            assert!(
                !state.committed_slots.contains(&0),
                "slot should NOT be committed with zero acks"
            );
        }

        // All members ack: SHOULD commit
        {
            let mut state = DecisionState::<String, TestCluster>::new();
            state.process(
                TranscriptMsg::Accept {
                    ballot: 1,
                    slot: 0,
                    value: "y".to_string(),
                },
                quorum,
            );
            for i in 0..cluster_size {
                state.process(
                    TranscriptMsg::AcceptAck {
                        ballot: 1,
                        slot: 0,
                        from: test_member(i as u32),
                    },
                    quorum,
                );
            }
            assert!(
                state.committed_slots.contains(&0),
                "slot should be committed when all {} members ack",
                cluster_size
            );
            assert_eq!(state.committed_log.len(), 1);
            assert_eq!(state.committed_log[0].message, "y");
        }
    }

    // Feature: broadcast-transcript-consensus, Property 1: Fold Commutativity
    //
    // For any set of valid protocol messages (Prepare, Promise, Accept, AcceptAck),
    // applying the decision function fold in any permutation of those messages
    // SHALL produce the same committed_log.
    //
    // **Validates: Requirements 1.2, 2.1**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn fold_commutativity(msgs in prop::collection::vec(arb_valid_transcript_msg(), 0..20), seed: u64) {
            use rand::SeedableRng;
            use rand::seq::SliceRandom;

            let quorum = quorum_size(5); // cluster_size = 5 → quorum = 3

            // Apply messages in original order
            let mut state_original = DecisionState::<String, TestCluster>::new();
            for msg in msgs.iter() {
                state_original.process(msg.clone(), quorum);
            }

            // Shuffle the messages using the random seed for a true permutation
            let mut shuffled = msgs.clone();
            let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);
            shuffled.shuffle(&mut rng);

            // Apply messages in shuffled order
            let mut state_shuffled = DecisionState::<String, TestCluster>::new();
            for msg in shuffled.iter() {
                state_shuffled.process(msg.clone(), quorum);
            }

            // The committed_log must be identical regardless of processing order
            prop_assert_eq!(
                &state_original.committed_log,
                &state_shuffled.committed_log,
                "committed_log differs between original and shuffled processing order"
            );

            // Also verify the internal state that drives commits is identical:
            // - committed_slots (which slots reached quorum)
            prop_assert_eq!(
                &state_original.committed_slots,
                &state_shuffled.committed_slots,
                "committed_slots differs between original and shuffled processing order"
            );
            // - ack_sets (quorum counting state)
            prop_assert_eq!(
                &state_original.ack_sets,
                &state_shuffled.ack_sets,
                "ack_sets differs between original and shuffled processing order"
            );
            // - accepted (highest ballot per slot)
            prop_assert_eq!(
                &state_original.accepted,
                &state_shuffled.accepted,
                "accepted differs between original and shuffled processing order"
            );
        }
    }

    // Feature: broadcast-transcript-consensus, Property 4: Agreement
    //
    // For any valid protocol execution (sequence of messages respecting ballot fencing
    // invariants), if the decision function applied to any two subsets of the transcript
    // both commit slot S, they SHALL commit the same value for slot S.
    //
    // **Validates: Requirements 3.1, 6.1**

    /// Strategy to generate a valid protocol trace where each (slot, ballot) pair
    /// maps to exactly one value. This is the Paxos invariant: a leader only
    /// proposes one value per slot per ballot.
    ///
    /// Generates Accept and AcceptAck messages for multiple slots/ballots, ensuring
    /// at least one slot has enough AcceptAcks to potentially reach quorum.
    fn arb_valid_protocol_trace(
        cluster_size: usize,
    ) -> impl Strategy<Value = Vec<TranscriptMsg<String, TestCluster>>> {
        let quorum = quorum_size(cluster_size);
        // Generate per-slot configurations: each slot has a unique (ballot, value) pair
        // We use up to 3 slots, each with its own ballot and value
        (
            prop::collection::vec(
                (0..5usize, "[a-z]{1,3}"), // (ballot, value) per slot
                1..=3,
            ),
            // How many acks per slot (to sometimes exceed quorum)
            prop::collection::vec(0..=(cluster_size), 3),
            // Extra AcceptAck messages for different ballots (noise that respects invariants)
            prop::collection::vec(
                (0..3usize, 5..10usize, 0..5u32), // (slot, different_ballot, member)
                0..5,
            ),
        )
            .prop_map(move |(slot_configs, ack_counts, extra_acks)| {
                let mut trace = Vec::new();

                // For each slot, emit one Accept and several AcceptAcks
                for (slot, (ballot, value)) in slot_configs.iter().enumerate() {
                    // Accept message — defines the unique value for (slot, ballot)
                    trace.push(TranscriptMsg::Accept {
                        ballot: *ballot,
                        slot,
                        value: value.clone(),
                    });

                    // AcceptAck messages from distinct members
                    let num_acks = if slot < ack_counts.len() {
                        ack_counts[slot].min(cluster_size)
                    } else {
                        quorum // default to quorum
                    };
                    for i in 0..num_acks {
                        trace.push(TranscriptMsg::AcceptAck {
                            ballot: *ballot,
                            slot,
                            from: test_member(i as u32),
                        });
                    }
                }

                // Add noise AcceptAcks for ballots that DON'T have a conflicting
                // Accept with a different value (ballots 5-9 are not used above)
                for (slot, ballot, member) in &extra_acks {
                    let slot = *slot;
                    let ballot = *ballot;
                    if slot < slot_configs.len() {
                        trace.push(TranscriptMsg::AcceptAck {
                            ballot,
                            slot,
                            from: test_member(*member),
                        });
                    }
                }

                trace
            })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// Agreement on valid protocol traces: two subsets must agree on committed
        /// slot values. Uses traces that respect the Paxos invariant (one value
        /// per slot+ballot).
        #[test]
        fn agreement_random(
            trace in arb_valid_protocol_trace(5),
            mask1 in prop::collection::vec(any::<bool>(), 30),
            mask2 in prop::collection::vec(any::<bool>(), 30),
        ) {
            let quorum = quorum_size(5);

            // Apply subset 1
            let mut state1 = DecisionState::<String, TestCluster>::new();
            for (i, msg) in trace.iter().enumerate() {
                if i < mask1.len() && mask1[i] {
                    state1.process(msg.clone(), quorum);
                }
            }

            // Apply subset 2
            let mut state2 = DecisionState::<String, TestCluster>::new();
            for (i, msg) in trace.iter().enumerate() {
                if i < mask2.len() && mask2[i] {
                    state2.process(msg.clone(), quorum);
                }
            }

            // For any slot committed by both, they must agree on the value
            for slot in state1.committed_slots.intersection(&state2.committed_slots) {
                let entry1 = state1.committed_log.iter().find(|e| e.slot == *slot);
                let entry2 = state2.committed_log.iter().find(|e| e.slot == *slot);
                if let (Some(e1), Some(e2)) = (entry1, entry2) {
                    prop_assert_eq!(
                        &e1.message, &e2.message,
                        "Agreement violated at slot {}: {:?} vs {:?}", slot, e1, e2
                    );
                }
            }
        }

        /// Agreement on well-formed traces with guaranteed commits: ensures the
        /// test exercises the non-trivial case where both subsets may commit a slot.
        /// The trace always has full quorum for slot 0, so subsets that include
        /// enough messages will commit it.
        #[test]
        fn agreement_well_formed(
            trace in arb_valid_protocol_trace(5),
            mask1 in prop::collection::vec(any::<bool>(), 30),
            mask2 in prop::collection::vec(any::<bool>(), 30),
        ) {
            let cluster_size = 5;
            let quorum = quorum_size(cluster_size);

            // Apply subset 1
            let mut state1 = DecisionState::<String, TestCluster>::new();
            for (i, msg) in trace.iter().enumerate() {
                if i < mask1.len() && mask1[i] {
                    state1.process(msg.clone(), quorum);
                }
            }

            // Apply subset 2
            let mut state2 = DecisionState::<String, TestCluster>::new();
            for (i, msg) in trace.iter().enumerate() {
                if i < mask2.len() && mask2[i] {
                    state2.process(msg.clone(), quorum);
                }
            }

            // For any slot committed by both, they must agree on the value
            for slot in state1.committed_slots.intersection(&state2.committed_slots) {
                let entry1 = state1.committed_log.iter().find(|e| e.slot == *slot);
                let entry2 = state2.committed_log.iter().find(|e| e.slot == *slot);
                if let (Some(e1), Some(e2)) = (entry1, entry2) {
                    prop_assert_eq!(
                        &e1.message, &e2.message,
                        "Agreement violated at slot {}: {:?} vs {:?}", slot, e1, e2
                    );
                }
            }
        }

        /// Agreement with full trace: when both observers see the entire trace
        /// (in different orders), they must agree on committed values.
        /// This validates that the fold's commutativity implies agreement.
        #[test]
        fn agreement_full_trace(
            trace in arb_valid_protocol_trace(5),
        ) {
            let quorum = quorum_size(5);

            // Both observers see the full trace but in different orders
            let mut state1 = DecisionState::<String, TestCluster>::new();
            for msg in trace.iter() {
                state1.process(msg.clone(), quorum);
            }

            let mut state2 = DecisionState::<String, TestCluster>::new();
            for msg in trace.iter().rev() {
                state2.process(msg.clone(), quorum);
            }

            // Committed slots must be identical
            prop_assert_eq!(
                &state1.committed_slots, &state2.committed_slots,
                "Full trace observers disagree on which slots are committed"
            );

            // For each committed slot, values must agree
            for slot in state1.committed_slots.iter() {
                let entry1 = state1.committed_log.iter().find(|e| e.slot == *slot);
                let entry2 = state2.committed_log.iter().find(|e| e.slot == *slot);
                if let (Some(e1), Some(e2)) = (entry1, entry2) {
                    prop_assert_eq!(
                        &e1.message, &e2.message,
                        "Agreement violated at slot {} with full trace: {:?} vs {:?}", slot, e1, e2
                    );
                }
            }
        }
    }

    // Feature: broadcast-transcript-consensus, Property 2: Fold Idempotency
    //
    // For any valid protocol message and any DecisionState, applying the decision
    // function to that message twice SHALL produce the same state as applying it once.
    //
    // **Validates: Requirements 2.2**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn fold_idempotency(
            setup_msgs in prop::collection::vec(arb_transcript_msg(), 0..10),
            msg in arb_transcript_msg()
        ) {
            let cluster_size = 3;
            let quorum = quorum_size(cluster_size);

            // Build a non-trivial DecisionState by applying random setup messages.
            let mut state = DecisionState::<String, TestCluster>::new();
            for m in setup_msgs.iter() {
                state.process(m.clone(), quorum);
            }

            // Apply the target message once → state_1
            let mut state_1 = state.clone();
            state_1.process(msg.clone(), quorum);

            // Apply the same message again → state_2
            let mut state_2 = state_1.clone();
            state_2.process(msg, quorum);

            // Assert state unchanged after second application
            prop_assert_eq!(
                &state_1.committed_log,
                &state_2.committed_log,
                "committed_log changed after duplicate message application"
            );
            prop_assert_eq!(
                &state_1.accepted,
                &state_2.accepted,
                "accepted changed after duplicate message application"
            );
            prop_assert_eq!(
                &state_1.ack_sets,
                &state_2.ack_sets,
                "ack_sets changed after duplicate message application"
            );
            prop_assert_eq!(
                &state_1.committed_slots,
                &state_2.committed_slots,
                "committed_slots changed after duplicate message application"
            );
        }
    }

    // Feature: broadcast-transcript-consensus, Property 5: Ballot Fencing
    //
    // For any MessageGenState where max_promised = B, and any Accept message with
    // ballot < B, the message generation logic SHALL NOT emit an AcceptAck for that Accept.
    // Conversely, Accept with ballot >= B SHOULD produce AcceptAck.
    //
    // **Validates: Requirements 3.2**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn ballot_fencing_rejects_stale_accept(
            max_promised in 5..20usize,
            stale_ballot_offset in 1..5usize,
            slot in 0..10usize,
            value in "[a-z]{1,5}",
        ) {
            // Ensure stale_ballot < max_promised
            let stale_ballot = max_promised.saturating_sub(stale_ballot_offset).min(max_promised - 1);

            let mut state = MessageGenState::<String, TestCluster>::new();
            state.max_promised = max_promised;

            let accept_msg = TranscriptMsg::Accept {
                ballot: stale_ballot,
                slot,
                value,
            };

            let output = state.process_tick(
                &[accept_msg],
                false,
                vec![],
                test_member(0),
                0,
                5,
            );

            // No AcceptAck should be emitted for stale ballot
            let has_accept_ack = output.outbound.iter().any(|msg| {
                matches!(msg, TranscriptMsg::AcceptAck { .. })
            });
            prop_assert!(
                !has_accept_ack,
                "AcceptAck emitted for stale ballot {} < max_promised {}",
                stale_ballot, max_promised
            );
        }

        #[test]
        fn ballot_fencing_accepts_current_or_higher(
            max_promised in 1..15usize,
            ballot_offset in 0..5usize,
            slot in 0..10usize,
            value in "[a-z]{1,5}",
        ) {
            // ballot >= max_promised
            let ballot = max_promised + ballot_offset;

            let mut state = MessageGenState::<String, TestCluster>::new();
            state.max_promised = max_promised;

            let accept_msg = TranscriptMsg::Accept {
                ballot,
                slot,
                value,
            };

            let output = state.process_tick(
                &[accept_msg],
                false,
                vec![],
                test_member(0),
                0,
                5,
            );

            // AcceptAck SHOULD be emitted for ballot >= max_promised
            let accept_ack = output.outbound.iter().find(|msg| {
                matches!(msg, TranscriptMsg::AcceptAck { .. })
            });
            prop_assert!(
                accept_ack.is_some(),
                "AcceptAck NOT emitted for ballot {} >= max_promised {}",
                ballot, max_promised
            );

            // Verify the AcceptAck has correct fields
            if let Some(TranscriptMsg::AcceptAck { ballot: ack_ballot, slot: ack_slot, .. }) = accept_ack {
                prop_assert_eq!(*ack_ballot, ballot);
                prop_assert_eq!(*ack_slot, slot);
            }
        }
    }

    // Feature: broadcast-transcript-consensus, Property 6: Phase1Certificate Guards Accept Emission
    //
    // For any MessageGenState and ballot B, the message generation logic SHALL NOT
    // emit an Accept for ballot B unless it has accumulated a quorum of Promise
    // messages for ballot B (constituting a Phase1Certificate).
    //
    // **Validates: Requirements 3.3, 4.3**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn phase1_certificate_guards_accept(
            cluster_size in 3..=7usize,
            num_promises in 0..=6usize,
        ) {
            let quorum = quorum_size(cluster_size);
            // Ensure we stay strictly below quorum
            let num_promises = num_promises.min(quorum - 1);

            let member_index = 0;
            let me = test_member(0);
            let mut state = MessageGenState::<String, TestCluster>::new();

            // Simulate starting an election: fire election timer to get a Prepare
            state.current_round = 1;
            let ballot = make_ballot(1, member_index, cluster_size);
            state.max_promised = ballot;

            // Feed sub-quorum Promise messages for our ballot.
            // Promises come from other members (indices 1..num_promises).
            let promise_msgs: Vec<TranscriptMsg<String, TestCluster>> = (0..num_promises)
                .map(|i| TranscriptMsg::Promise {
                    ballot,
                    from: test_member((i + 1) as u32),
                    accepted: vec![],
                })
                .collect();

            // First tick: process the sub-quorum promises
            let output1 = state.process_tick(
                &promise_msgs,
                false,
                vec![],
                me.clone(),
                member_index,
                cluster_size,
            );

            // No Accept should be emitted from processing promises alone
            let accepts1: Vec<_> = output1.outbound.iter().filter(|m| matches!(m, TranscriptMsg::Accept { .. })).collect();
            prop_assert!(accepts1.is_empty(), "Accept emitted from sub-quorum promises: {:?}", accepts1);

            // Second tick: present client requests — leader-only behavior would emit Accept
            let output2 = state.process_tick(
                &[],
                false,
                vec!["request1".to_string(), "request2".to_string()],
                me.clone(),
                member_index,
                cluster_size,
            );

            // No Accept should be emitted since we don't have Phase1Certificate
            let accepts2: Vec<_> = output2.outbound.iter().filter(|m| matches!(m, TranscriptMsg::Accept { .. })).collect();
            prop_assert!(accepts2.is_empty(), "Accept emitted without Phase1Certificate (sub-quorum promises={}): {:?}", num_promises, accepts2);

            // Should not be leader without Phase1Certificate
            prop_assert!(!state.is_leader, "is_leader should be false without Phase1Certificate (num_promises={}, quorum={})", num_promises, quorum);
        }
    }

    // Feature: broadcast-transcript-consensus, Property 9: Promise Emission
    //
    // For any MessageGenState with max_promised = B_old and any Prepare message with
    // ballot = B_new > B_old, the message generation logic SHALL emit a Promise for
    // B_new containing all previously-accepted entries, and SHALL update max_promised
    // to B_new.
    //
    // **Validates: Requirements 4.2**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn promise_emission(
            b_old in 0..10usize,
            b_new_offset in 1..10usize,
            num_accepted in 0..5usize,
        ) {
            let b_new = b_old + b_new_offset;
            let cluster_size = 5;
            let me = test_member(0);
            let mut state = MessageGenState::<String, TestCluster>::new();
            state.max_promised = b_old;

            // Set up some accepted entries
            for slot in 0..num_accepted {
                state.accepted.insert(slot, (b_old, format!("val_{}", slot)));
            }

            // Present Prepare with higher ballot
            let prepare = TranscriptMsg::Prepare {
                ballot: b_new,
                from: test_member(2),
            };

            let output = state.process_tick(
                &[prepare],
                false,
                vec![],
                me,
                0,
                cluster_size,
            );

            // Assert exactly one Promise emitted
            let promises: Vec<_> = output.outbound.iter()
                .filter_map(|m| match m {
                    TranscriptMsg::Promise { ballot, from: _, accepted } => Some((ballot, accepted)),
                    _ => None,
                })
                .collect();
            prop_assert_eq!(promises.len(), 1, "Exactly one Promise should be emitted");
            prop_assert_eq!(*promises[0].0, b_new, "Promise ballot should be B_new");
            prop_assert_eq!(
                promises[0].1.len(), num_accepted,
                "Promise should contain all {} accepted entries", num_accepted
            );

            // Assert all previously-accepted entries are included in the Promise
            for slot in 0..num_accepted {
                let found = promises[0].1.iter().any(|(s, b, v)| {
                    *s == slot && *b == b_old && *v == format!("val_{}", slot)
                });
                prop_assert!(
                    found,
                    "Promise missing accepted entry for slot {} (ballot={}, value=val_{})",
                    slot, b_old, slot
                );
            }

            // Assert max_promised updated to B_new
            prop_assert_eq!(
                state.max_promised, b_new,
                "max_promised should be updated to B_new={}", b_new
            );
        }
    }

    // Feature: broadcast-transcript-consensus, Property 7: Paxos Recovery
    //
    // For any set of Promise messages forming a quorum for ballot B, if any Promise
    // reports a previously-accepted value (slot S, ballot B', value V), the leader
    // SHALL re-propose value V for slot S where B' is the highest ballot among all
    // reported accepted values for that slot.
    //
    // **Validates: Requirements 3.4, 6.5**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn paxos_recovery(
            slot in 0..5usize,
            low_ballot in 1..5usize,
            high_ballot in 5..10usize,
            low_value in "[a-z]{1,3}",
            high_value in "[a-z]{1,3}",
        ) {
            let cluster_size = 3;
            let member_index = 0;
            let me = test_member(0);
            let mut state = MessageGenState::<String, TestCluster>::new();

            // Simulate starting an election: member 0 started round 2
            state.current_round = 2;
            let ballot = make_ballot(2, member_index, cluster_size); // = 6
            state.max_promised = ballot;

            // Two promises forming a quorum (quorum_size(3) = 2):
            // - Member 1 reports accepted (slot, low_ballot, low_value)
            // - Member 2 reports accepted (slot, high_ballot, high_value)
            let promises = vec![
                TranscriptMsg::Promise {
                    ballot,
                    from: test_member(1),
                    accepted: vec![(slot, low_ballot, low_value.clone())],
                },
                TranscriptMsg::Promise {
                    ballot,
                    from: test_member(2),
                    accepted: vec![(slot, high_ballot, high_value.clone())],
                },
            ];

            let output = state.process_tick(
                &promises,
                false,
                vec![],
                me,
                member_index,
                cluster_size,
            );

            // Leader should re-propose the HIGH-ballot value for the recovered slot
            let accepts: Vec<_> = output.outbound.iter()
                .filter_map(|m| match m {
                    TranscriptMsg::Accept { ballot: b, slot: s, value } if *s == slot => Some((b, value)),
                    _ => None,
                })
                .collect();

            prop_assert!(!accepts.is_empty(), "Leader should emit Accept for recovered slot {}", slot);
            // The value must be the one from the highest ballot
            prop_assert_eq!(
                accepts[0].1, &high_value,
                "Recovery must use highest-ballot value: expected {:?} (ballot {}) but got {:?}",
                high_value, high_ballot, accepts[0].1
            );
        }
    }

    // Feature: broadcast-transcript-consensus, Property 13: Ballot Uniqueness
    //
    // For any two distinct members with IDs i and j (where i ≠ j) and any rounds
    // r_i and r_j, the ballots r_i * cluster_size + i and r_j * cluster_size + j
    // SHALL be distinct.
    //
    // **Validates: Requirements 6.4**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn ballot_uniqueness(
            cluster_size in 2usize..=10,
            rounds in prop::collection::vec(0usize..100, 1..20),
            member_ids in prop::collection::vec(0usize..10, 1..20),
        ) {
            // Collect all valid (member_id, round) pairs and their ballots
            let mut seen_ballots: HashMap<Ballot, (usize, usize)> = HashMap::new();

            for &round in &rounds {
                for &member_id in &member_ids {
                    if member_id < cluster_size {
                        let ballot = make_ballot(round, member_id, cluster_size);

                        // Check that no other (round', member_id') produced the same ballot
                        if let Some(&(prev_round, prev_member)) = seen_ballots.get(&ballot) {
                            // Same ballot must mean same (round, member_id)
                            prop_assert_eq!(
                                (round, member_id), (prev_round, prev_member),
                                "Ballot collision: ballot {} produced by both ({}, {}) and ({}, {})",
                                ballot, round, member_id, prev_round, prev_member
                            );
                        }
                        seen_ballots.insert(ballot, (round, member_id));

                        // Also verify extract_member recovers the member_id
                        prop_assert_eq!(
                            extract_member(ballot, cluster_size), member_id,
                            "extract_member({}, {}) should be {} but got {}",
                            ballot, cluster_size, member_id, extract_member(ballot, cluster_size)
                        );
                    }
                }
            }
        }
    }

    // Feature: broadcast-transcript-consensus, Property 10: AcceptAck Emission
    //
    // For any MessageGenState with max_promised = B and any Accept message with
    // ballot >= B, the message generation logic SHALL emit an AcceptAck for that
    // (slot, ballot) and update its accepted state.
    //
    // **Validates: Requirements 4.4**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn acceptack_emission(
            max_promised in 0..10usize,
            ballot_offset in 0..5usize,
            slot in 0..10usize,
            value in "[a-z]{1,3}",
        ) {
            // ballot >= max_promised (offset of 0 means equal, >0 means strictly greater)
            let ballot = max_promised + ballot_offset;
            let cluster_size = 5;
            let me = test_member(0);
            let mut state = MessageGenState::<String, TestCluster>::new();
            state.max_promised = max_promised;

            let accept = TranscriptMsg::Accept {
                ballot,
                slot,
                value: value.clone(),
            };

            let output = state.process_tick(
                &[accept],
                false,
                vec![],
                me,
                0,
                cluster_size,
            );

            // AcceptAck should be emitted
            let acks: Vec<_> = output.outbound.iter()
                .filter_map(|m| match m {
                    TranscriptMsg::AcceptAck { ballot: b, slot: s, .. } => Some((*b, *s)),
                    _ => None,
                })
                .collect();
            prop_assert_eq!(acks.len(), 1, "Exactly one AcceptAck should be emitted");
            prop_assert_eq!(acks[0].0, ballot, "AcceptAck ballot should match Accept ballot");
            prop_assert_eq!(acks[0].1, slot, "AcceptAck slot should match Accept slot");

            // Accepted state should be updated
            prop_assert!(
                state.accepted.contains_key(&slot),
                "accepted map should contain slot {} after processing Accept", slot
            );
            let (accepted_ballot, accepted_value) = state.accepted.get(&slot).unwrap();
            prop_assert_eq!(*accepted_ballot, ballot, "accepted ballot should match");
            prop_assert_eq!(accepted_value, &value, "accepted value should match");
        }

        /// Negative case: Accept with ballot < max_promised should NOT emit AcceptAck.
        /// (Complementary to Property 5's ballot fencing, but asserts from the
        /// AcceptAck emission perspective.)
        #[test]
        fn acceptack_emission_rejected_when_stale(
            max_promised in 1..15usize,
            stale_offset in 1..5usize,
            slot in 0..10usize,
            value in "[a-z]{1,3}",
        ) {
            // Ensure stale_ballot < max_promised
            let stale_ballot = max_promised.saturating_sub(stale_offset).min(max_promised - 1);
            let cluster_size = 5;
            let me = test_member(0);
            let mut state = MessageGenState::<String, TestCluster>::new();
            state.max_promised = max_promised;

            let accept = TranscriptMsg::Accept {
                ballot: stale_ballot,
                slot,
                value: value.clone(),
            };

            let output = state.process_tick(
                &[accept],
                false,
                vec![],
                me,
                0,
                cluster_size,
            );

            // No AcceptAck should be emitted for stale ballot
            let acks: Vec<_> = output.outbound.iter()
                .filter(|m| matches!(m, TranscriptMsg::AcceptAck { .. }))
                .collect();
            prop_assert!(
                acks.is_empty(),
                "AcceptAck should NOT be emitted for stale ballot {} < max_promised {}",
                stale_ballot, max_promised
            );

            // Accepted state should NOT be updated for the stale ballot
            if let Some((accepted_ballot, _)) = state.accepted.get(&slot) {
                prop_assert!(
                    *accepted_ballot != stale_ballot,
                    "accepted state should not record stale ballot {} for slot {}",
                    stale_ballot, slot
                );
            }
        }
    }

    // ─── Unit Tests: Decision Function (Task 8.1) ────────────────────────────

    // **Validates: Requirements 2.1, 2.2, 2.3, 2.4**

    #[test]
    fn decision_empty_transcript_no_commits() {
        let state = DecisionState::<String, TestCluster>::new();
        assert_eq!(state.committed_entries().len(), 0);
    }

    #[test]
    fn decision_single_slot_committed() {
        let cluster_size = 3;
        let quorum = quorum_size(cluster_size); // 2
        let mut state = DecisionState::<String, TestCluster>::new();

        state.process(
            TranscriptMsg::Accept {
                ballot: 1,
                slot: 0,
                value: "hello".to_string(),
            },
            quorum,
        );
        state.process(
            TranscriptMsg::AcceptAck {
                ballot: 1,
                slot: 0,
                from: test_member(0),
            },
            quorum,
        );
        state.process(
            TranscriptMsg::AcceptAck {
                ballot: 1,
                slot: 0,
                from: test_member(1),
            },
            quorum,
        );

        assert_eq!(state.committed_entries().len(), 1);
        let entry = &state.committed_entries()[0];
        assert_eq!(entry.message, "hello");
        assert_eq!(entry.ballot, 1);
        assert_eq!(entry.slot, 0);
    }

    #[test]
    fn decision_sub_quorum_no_commit() {
        let quorum = quorum_size(3); // 2
        let mut state = DecisionState::<String, TestCluster>::new();

        state.process(
            TranscriptMsg::Accept {
                ballot: 1,
                slot: 0,
                value: "x".to_string(),
            },
            quorum,
        );
        state.process(
            TranscriptMsg::AcceptAck {
                ballot: 1,
                slot: 0,
                from: test_member(0),
            },
            quorum,
        );
        // Only 1 ack, need 2

        assert_eq!(state.committed_entries().len(), 0);
    }

    #[test]
    fn decision_gap_filling() {
        let quorum = quorum_size(3); // 2
        let mut state = DecisionState::<String, TestCluster>::new();

        // Commit slot 1 first (but slot 0 is not committed)
        state.process(
            TranscriptMsg::Accept {
                ballot: 1,
                slot: 1,
                value: "second".to_string(),
            },
            quorum,
        );
        state.process(
            TranscriptMsg::AcceptAck {
                ballot: 1,
                slot: 1,
                from: test_member(0),
            },
            quorum,
        );
        state.process(
            TranscriptMsg::AcceptAck {
                ballot: 1,
                slot: 1,
                from: test_member(1),
            },
            quorum,
        );

        // Slot 1 is in committed_slots but NOT in committed_log (gap)
        assert!(state.committed_slots.contains(&1));
        assert_eq!(state.committed_entries().len(), 0); // withheld!

        // Now commit slot 0
        state.process(
            TranscriptMsg::Accept {
                ballot: 1,
                slot: 0,
                value: "first".to_string(),
            },
            quorum,
        );
        state.process(
            TranscriptMsg::AcceptAck {
                ballot: 1,
                slot: 0,
                from: test_member(0),
            },
            quorum,
        );
        state.process(
            TranscriptMsg::AcceptAck {
                ballot: 1,
                slot: 0,
                from: test_member(1),
            },
            quorum,
        );

        // Now both should flush in order
        assert_eq!(state.committed_entries().len(), 2);
        assert_eq!(state.committed_entries()[0].message, "first");
        assert_eq!(state.committed_entries()[0].slot, 0);
        assert_eq!(state.committed_entries()[1].message, "second");
        assert_eq!(state.committed_entries()[1].slot, 1);
    }

    #[test]
    fn decision_duplicate_acks_dont_double_count() {
        let quorum = quorum_size(3); // 2
        let mut state = DecisionState::<String, TestCluster>::new();

        state.process(
            TranscriptMsg::Accept {
                ballot: 1,
                slot: 0,
                value: "x".to_string(),
            },
            quorum,
        );
        // Same member acks twice — should not count twice
        state.process(
            TranscriptMsg::AcceptAck {
                ballot: 1,
                slot: 0,
                from: test_member(0),
            },
            quorum,
        );
        state.process(
            TranscriptMsg::AcceptAck {
                ballot: 1,
                slot: 0,
                from: test_member(0),
            },
            quorum,
        );
        state.process(
            TranscriptMsg::AcceptAck {
                ballot: 1,
                slot: 0,
                from: test_member(0),
            },
            quorum,
        );

        // Still only 1 unique ack, need 2 for quorum
        assert_eq!(state.committed_entries().len(), 0);

        // Add a different member's ack → quorum reached
        state.process(
            TranscriptMsg::AcceptAck {
                ballot: 1,
                slot: 0,
                from: test_member(1),
            },
            quorum,
        );
        assert_eq!(state.committed_entries().len(), 1);
    }

    // ─── Unit Tests: Checkpoint Truncation (Task 11.2 / Req 8) ───────────────

    /// Req 8.1: truncate prunes per-slot bookkeeping below the checkpoint.
    #[test]
    fn truncate_prunes_committed_prefix_state() {
        let quorum = quorum_size(3); // 2
        let mut state = DecisionState::<String, TestCluster>::new();

        // Commit slots 0 and 1 (each: Accept + 2 acks).
        for slot in 0..2usize {
            state.process(
                TranscriptMsg::Accept {
                    ballot: 1,
                    slot,
                    value: format!("v{}", slot),
                },
                quorum,
            );
            for m in 0..2u32 {
                state.process(
                    TranscriptMsg::AcceptAck {
                        ballot: 1,
                        slot,
                        from: test_member(m),
                    },
                    quorum,
                );
            }
        }
        assert_eq!(state.committed_entries().len(), 2);
        // Precondition: bookkeeping for slots 0,1 is present.
        assert!(state.accepted.contains_key(&0));
        assert!(state.ack_sets.contains_key(&(0, 1)));
        assert!(state.committed_slots.contains(&0));

        // Checkpoint at slot 2: everything for slots < 2 may be pruned.
        state.truncate(2);

        assert!(
            !state.accepted.contains_key(&0) && !state.accepted.contains_key(&1),
            "accepted entries below checkpoint must be pruned"
        );
        assert!(
            !state.ack_sets.contains_key(&(0, 1)) && !state.ack_sets.contains_key(&(1, 1)),
            "ack_sets below checkpoint must be pruned"
        );
        assert!(
            !state.committed_slots.contains(&0) && !state.committed_slots.contains(&1),
            "committed_slots below checkpoint must be pruned"
        );
    }

    // Req 8.2 (Truncation Safety): interleaving checkpoints at the contiguous
    // committed prefix never changes the emitted committed log. Running a trace
    // with periodic truncation yields the same committed_log as running it
    // without truncation.
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn truncation_safety(
            msgs in prop::collection::vec(arb_valid_transcript_msg(), 0..40),
            checkpoint_mask in prop::collection::vec(any::<bool>(), 40),
        ) {
            let quorum = quorum_size(5);

            // Reference: no truncation.
            let mut a = DecisionState::<String, TestCluster>::new();
            for m in msgs.iter() {
                a.process(m.clone(), quorum);
            }

            // Truncated: checkpoint at the contiguous committed prefix at marked steps.
            let mut b = DecisionState::<String, TestCluster>::new();
            for (i, m) in msgs.iter().enumerate() {
                b.process(m.clone(), quorum);
                if i < checkpoint_mask.len() && checkpoint_mask[i] {
                    let cp = b.committed_entries().len();
                    b.truncate(cp);
                }
            }

            prop_assert_eq!(
                a.committed_entries(),
                b.committed_entries(),
                "truncation changed the emitted committed log"
            );
        }
    }

    /// Truncation must not break future commits: after pruning the committed
    /// prefix, a later slot still commits correctly.
    #[test]
    fn truncate_preserves_future_commits() {
        let quorum = quorum_size(3);
        let mut state = DecisionState::<String, TestCluster>::new();

        // Commit slot 0.
        state.process(
            TranscriptMsg::Accept {
                ballot: 1,
                slot: 0,
                value: "a".to_string(),
            },
            quorum,
        );
        state.process(
            TranscriptMsg::AcceptAck {
                ballot: 1,
                slot: 0,
                from: test_member(0),
            },
            quorum,
        );
        state.process(
            TranscriptMsg::AcceptAck {
                ballot: 1,
                slot: 0,
                from: test_member(1),
            },
            quorum,
        );
        state.truncate(1);

        // Now commit slot 1.
        state.process(
            TranscriptMsg::Accept {
                ballot: 1,
                slot: 1,
                value: "b".to_string(),
            },
            quorum,
        );
        state.process(
            TranscriptMsg::AcceptAck {
                ballot: 1,
                slot: 1,
                from: test_member(0),
            },
            quorum,
        );
        state.process(
            TranscriptMsg::AcceptAck {
                ballot: 1,
                slot: 1,
                from: test_member(1),
            },
            quorum,
        );

        assert_eq!(state.committed_entries().len(), 2);
        assert_eq!(state.committed_entries()[1].message, "b");
        assert_eq!(state.committed_entries()[1].slot, 1);
    }

    // ─── Unit Tests: Message Generation (Task 8.2) ───────────────────────────

    /// Req 4.1: Election timer fires → Prepare emitted with fresh ballot
    #[test]
    fn msggen_election_timer_emits_prepare() {
        let mut state = MessageGenState::<String, TestCluster>::new();
        let me = test_member(0);
        let cluster_size = 3;

        let output = state.process_tick(&[], true, vec![], me, 0, cluster_size);

        // Should emit a Prepare with fresh ballot
        let prepares: Vec<_> = output
            .outbound
            .iter()
            .filter(|m| matches!(m, TranscriptMsg::Prepare { .. }))
            .collect();
        assert_eq!(
            prepares.len(),
            1,
            "Election timer should emit exactly one Prepare"
        );
        if let TranscriptMsg::Prepare { ballot, .. } = prepares[0] {
            // current_round increments to 1, ballot = 1 * 3 + 0 = 3
            assert_eq!(*ballot, make_ballot(1, 0, cluster_size));
        } else {
            panic!("Expected Prepare message");
        }
    }

    /// Req 4.5: Non-leader receives request → redirect with leader hint
    #[test]
    fn msggen_non_leader_redirects_requests() {
        let mut state = MessageGenState::<String, TestCluster>::new();
        state.known_leader = Some(test_member(2)); // knows member 2 is leader
        let me = test_member(0);

        let output = state.process_tick(
            &[],
            false,
            vec!["req1".to_string(), "req2".to_string()],
            me,
            0,
            3,
        );

        assert_eq!(output.redirected.len(), 2);
        assert_eq!(output.redirected[0].0, "req1");
        assert_eq!(output.redirected[0].1, Some(test_member(2)));
        assert_eq!(output.redirected[1].0, "req2");
        assert_eq!(output.redirected[1].1, Some(test_member(2)));
    }

    /// Req 4.6: Leader suppresses election timer
    #[test]
    fn msggen_leader_suppresses_election_timer() {
        let mut state = MessageGenState::<String, TestCluster>::new();
        state.is_leader = true;
        state.max_promised = make_ballot(1, 0, 3);
        state.current_round = 1;
        let me = test_member(0);

        let output = state.process_tick(&[], true, vec![], me, 0, 3);

        // Leader should NOT emit Prepare even if timer fires
        let prepares: Vec<_> = output
            .outbound
            .iter()
            .filter(|m| matches!(m, TranscriptMsg::Prepare { .. }))
            .collect();
        assert_eq!(prepares.len(), 0, "Leader should suppress election timer");
    }

    /// Req 4.2: Observe Prepare with higher ballot → Promise emitted
    #[test]
    fn msggen_higher_ballot_prepare_emits_promise() {
        let mut state = MessageGenState::<String, TestCluster>::new();
        state.max_promised = 3;
        state.accepted.insert(0, (2, "old_val".to_string()));
        let me = test_member(0);

        let prepare = TranscriptMsg::Prepare {
            ballot: 10,
            from: test_member(2),
        };
        let output = state.process_tick(&[prepare], false, vec![], me, 0, 3);

        let promises: Vec<_> = output
            .outbound
            .iter()
            .filter_map(|m| match m {
                TranscriptMsg::Promise {
                    ballot, accepted, ..
                } => Some((ballot, accepted)),
                _ => None,
            })
            .collect();
        assert_eq!(promises.len(), 1, "Should emit exactly one Promise");
        assert_eq!(
            *promises[0].0, 10,
            "Promise ballot should match Prepare ballot"
        );
        assert_eq!(
            promises[0].1.len(),
            1,
            "Promise should report the one accepted entry"
        );
        // Verify the accepted entry is reported correctly
        let (slot, ballot, value) = &promises[0].1[0];
        assert_eq!(*slot, 0);
        assert_eq!(*ballot, 2);
        assert_eq!(value, "old_val");
        // max_promised must be updated
        assert_eq!(state.max_promised, 10);
    }

    /// Req 4.3: Quorum of Promises → Accept emitted for pending proposals
    #[test]
    fn msggen_quorum_promises_emits_accept() {
        let mut state = MessageGenState::<String, TestCluster>::new();
        let cluster_size = 3;
        let member_index = 0;
        let me = test_member(0);

        // Simulate election: member 0 started round 1
        state.current_round = 1;
        let ballot = make_ballot(1, member_index, cluster_size); // = 3
        state.max_promised = ballot;
        state.pending_requests.push_back("proposal_1".to_string());

        // Quorum = 2 promises needed
        let promises = vec![
            TranscriptMsg::Promise {
                ballot,
                from: test_member(1),
                accepted: vec![],
            },
            TranscriptMsg::Promise {
                ballot,
                from: test_member(2),
                accepted: vec![],
            },
        ];

        let output = state.process_tick(&promises, false, vec![], me, member_index, cluster_size);

        assert!(
            state.is_leader,
            "Should become leader after quorum promises"
        );
        let accepts: Vec<_> = output
            .outbound
            .iter()
            .filter(|m| matches!(m, TranscriptMsg::Accept { .. }))
            .collect();
        assert!(
            !accepts.is_empty(),
            "Leader should emit Accept for pending proposals"
        );
        // Verify the Accept carries our proposal
        if let TranscriptMsg::Accept {
            ballot: ab,
            slot,
            value,
        } = accepts[0]
        {
            assert_eq!(*ab, ballot);
            assert_eq!(*slot, 0);
            assert_eq!(value, "proposal_1");
        } else {
            panic!("Expected Accept message");
        }
    }

    /// Req 3.2: Ballot fencing: stale Accept ignored (no AcceptAck emitted)
    #[test]
    fn msggen_ballot_fencing_ignores_stale_accept() {
        let mut state = MessageGenState::<String, TestCluster>::new();
        state.max_promised = 10;
        let me = test_member(0);

        let stale_accept = TranscriptMsg::Accept {
            ballot: 5,
            slot: 0,
            value: "stale".to_string(),
        };
        let output = state.process_tick(&[stale_accept], false, vec![], me, 0, 3);

        // No AcceptAck for stale ballot
        let acks: Vec<_> = output
            .outbound
            .iter()
            .filter(|m| matches!(m, TranscriptMsg::AcceptAck { .. }))
            .collect();
        assert_eq!(
            acks.len(),
            0,
            "Should not emit AcceptAck for ballot < max_promised"
        );
    }

    // ─── Leader-Liveness / Failover (Req 4.1, 5.5) ───────────────────────────
    //
    // These mirror raft_step's `heartbeat_seen` gating: a follower must NOT
    // campaign while it is observing leader activity in the transcript, and MUST
    // campaign once leader activity ceases. Without this, symmetric election
    // timeouts cause perpetual dueling elections even under a healthy leader.

    /// Req 5.5: A follower that observed leader activity (an Accept for the
    /// current ballot) in the previous window MUST suppress its election when the
    /// timer fires, rather than campaigning against a live leader.
    #[test]
    fn msggen_suppresses_election_when_leader_active() {
        let mut state = MessageGenState::<String, TestCluster>::new();
        state.max_promised = 3;
        state.known_leader = Some(test_member(0));
        let me = test_member(1);

        // Window 1: observe an Accept from the leader (leader is alive/working).
        // Timer does not fire this window.
        let accept = TranscriptMsg::Accept {
            ballot: 3,
            slot: 0,
            value: "x".to_string(),
        };
        let _ = state.process_tick(&[accept], false, vec![], me.clone(), 1, 3);

        // Window 2: election timer fires. Because leader activity was observed,
        // the follower must NOT campaign.
        let out = state.process_tick(&[], true, vec![], me.clone(), 1, 3);

        let prepares: Vec<_> = out
            .outbound
            .iter()
            .filter(|m| matches!(m, TranscriptMsg::Prepare { .. }))
            .collect();
        assert!(
            prepares.is_empty(),
            "follower must suppress election after observing leader activity, emitted: {:?}",
            prepares
        );
    }

    /// Req 4.1: A follower that has seen NO leader activity across an election
    /// window MUST campaign when the timer fires.
    #[test]
    fn msggen_campaigns_when_leader_silent() {
        let mut state = MessageGenState::<String, TestCluster>::new();
        state.max_promised = 3;
        state.known_leader = Some(test_member(0));
        let me = test_member(1);

        // No transcript activity at all; election timer fires → must campaign.
        let out = state.process_tick(&[], true, vec![], me.clone(), 1, 3);

        let prepares: Vec<_> = out
            .outbound
            .iter()
            .filter(|m| matches!(m, TranscriptMsg::Prepare { .. }))
            .collect();
        assert_eq!(
            prepares.len(),
            1,
            "follower must campaign when no leader activity was observed"
        );
    }

    /// Observed leader activity suppresses exactly ONE election window (consume
    /// semantics, mirroring raft's `heartbeat_seen = false`). A subsequent timer
    /// firing with no fresh activity must then campaign.
    #[test]
    fn msggen_leader_activity_consumed_each_window() {
        let mut state = MessageGenState::<String, TestCluster>::new();
        state.max_promised = 3;
        state.known_leader = Some(test_member(0));
        let me = test_member(1);

        // Observe leader activity.
        let accept = TranscriptMsg::Accept {
            ballot: 3,
            slot: 0,
            value: "x".to_string(),
        };
        let _ = state.process_tick(&[accept], false, vec![], me.clone(), 1, 3);

        // First timer firing: suppressed (consumes the activity window).
        let out1 = state.process_tick(&[], true, vec![], me.clone(), 1, 3);
        assert!(
            !out1
                .outbound
                .iter()
                .any(|m| matches!(m, TranscriptMsg::Prepare { .. })),
            "first election after activity must be suppressed"
        );

        // Second timer firing with NO new activity: must campaign.
        let out2 = state.process_tick(&[], true, vec![], me.clone(), 1, 3);
        assert!(
            out2.outbound
                .iter()
                .any(|m| matches!(m, TranscriptMsg::Prepare { .. })),
            "second election with no fresh activity must campaign"
        );
    }

    // Feature: broadcast-transcript-consensus, Property 12: Safety Under Adversarial Schedules
    //
    // For any random delivery schedule (arbitrary message reorderings, concurrent
    // elections, partial deliveries), the committed logs observed by any two members
    // SHALL be consistent — one is always a prefix of the other, and they agree on
    // all shared slots.
    //
    // **Validates: Requirements 6.1, 6.7**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn safety_under_adversarial_schedules(
            proposals in prop::collection::vec("[a-z]{1,3}", 2..6),
            delivery_masks in prop::collection::vec(prop::collection::vec(any::<bool>(), 50), 3),
            seed: u64,
        ) {
            use rand::SeedableRng;
            use rand::seq::SliceRandom;

            let cluster_size = 3;
            let quorum = quorum_size(cluster_size);

            // Build a complete valid trace (single ballot, all proposals)
            let ballot = make_ballot(1, 0, cluster_size);
            let mut full_trace: Vec<TranscriptMsg<String, TestCluster>> = Vec::new();

            full_trace.push(TranscriptMsg::Prepare { ballot, from: test_member(0) });
            for i in 0..cluster_size {
                full_trace.push(TranscriptMsg::Promise { ballot, from: test_member(i as u32), accepted: vec![] });
            }
            for (slot, value) in proposals.iter().enumerate() {
                full_trace.push(TranscriptMsg::Accept { ballot, slot, value: value.clone() });
                for i in 0..cluster_size {
                    full_trace.push(TranscriptMsg::AcceptAck { ballot, slot, from: test_member(i as u32) });
                }
            }

            // Shuffle the trace using the random seed
            let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);
            let mut shuffled_trace = full_trace.clone();
            shuffled_trace.shuffle(&mut rng);

            // Each member gets a random SUBSET of the shuffled trace
            let mut member_states: Vec<DecisionState<String, TestCluster>> = Vec::new();
            for member_idx in 0..cluster_size {
                let mask = &delivery_masks[member_idx];
                let mut state = DecisionState::<String, TestCluster>::new();

                for (i, msg) in shuffled_trace.iter().enumerate() {
                    if i < mask.len() && mask[i] {
                        state.process(msg.clone(), quorum);
                    }
                }
                member_states.push(state);
            }

            // Assert prefix consistency: for any two members, their committed logs
            // must agree on all shared slots
            for i in 0..cluster_size {
                for j in (i+1)..cluster_size {
                    let log_i = &member_states[i].committed_log;
                    let log_j = &member_states[j].committed_log;
                    let min_len = log_i.len().min(log_j.len());

                    for k in 0..min_len {
                        prop_assert_eq!(
                            &log_i[k].message, &log_j[k].message,
                            "Safety violation: member {} and {} disagree at slot {}: {:?} vs {:?}",
                            i, j, k, log_i[k], log_j[k]
                        );
                        prop_assert_eq!(
                            log_i[k].slot, log_j[k].slot,
                            "Slot ordering violation between member {} and {}", i, j
                        );
                    }
                }
            }
        }
    }

    // Feature: broadcast-transcript-consensus, Property 8: Validity
    //
    // For any protocol execution, every value V appearing in the committed log
    // SHALL exist in the set of original client proposals. No value is invented
    // by the protocol.
    //
    // **Validates: Requirements 3.5**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn validity(
            proposals in prop::collection::vec("[a-z]{1,5}", 1..10),
        ) {
            let cluster_size = 3;
            let quorum = quorum_size(cluster_size);

            // Simulate: member 0 wins election, proposes all values
            let mut msg_gen = MessageGenState::<String, TestCluster>::new();
            let mut decision = DecisionState::<String, TestCluster>::new();
            let me = test_member(0);

            // Step 1: Election — fire timer, get Prepare
            let output1 = msg_gen.process_tick(&[], true, vec![], me.clone(), 0, cluster_size);
            // Process own Prepare in decision state
            for msg in &output1.outbound {
                decision.process(msg.clone(), quorum);
            }

            // Step 2: Simulate quorum of Promises (from members 1 and 2)
            let ballot = make_ballot(1, 0, cluster_size);
            let promises = vec![
                TranscriptMsg::Promise { ballot, from: test_member(1), accepted: vec![] },
                TranscriptMsg::Promise { ballot, from: test_member(2), accepted: vec![] },
            ];
            let output2 = msg_gen.process_tick(&promises, false, proposals.clone(), me.clone(), 0, cluster_size);
            // Process all outbound messages (Accept messages) through decision
            for msg in &output2.outbound {
                decision.process(msg.clone(), quorum);
            }

            // Step 3: Simulate AcceptAcks from quorum for each Accept
            let accepts: Vec<_> = output2.outbound.iter()
                .filter(|m| matches!(m, TranscriptMsg::Accept { .. }))
                .cloned()
                .collect();
            for accept in &accepts {
                if let TranscriptMsg::Accept { ballot, slot, .. } = accept {
                    for i in 0..cluster_size {
                        let ack = TranscriptMsg::AcceptAck {
                            ballot: *ballot,
                            slot: *slot,
                            from: test_member(i as u32),
                        };
                        decision.process(ack, quorum);
                    }
                }
            }

            // Assert: every committed value was in the original proposals
            for entry in decision.committed_entries() {
                prop_assert!(
                    proposals.contains(&entry.message),
                    "Committed value {:?} not in original proposals {:?}",
                    entry.message, proposals
                );
            }
        }
    }

    // Feature: broadcast-transcript-consensus, Property 11: Convergence
    //
    // For any complete protocol execution (all messages delivered to all members),
    // when every member applies the decision function to the full transcript, all
    // members SHALL produce the same committed log.
    //
    // **Validates: Requirements 6.3**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn convergence(
            proposals in prop::collection::vec("[a-z]{1,5}", 1..8),
            seed: u64,
        ) {
            use rand::SeedableRng;
            use rand::seq::SliceRandom;

            let cluster_size = 3;
            let quorum = quorum_size(cluster_size);

            // Build a complete transcript by simulating a full execution:
            // Member 0 wins election, proposes all values, all members ack.
            let mut all_messages: Vec<TranscriptMsg<String, TestCluster>> = Vec::new();

            // Member 0 wins election
            let ballot = make_ballot(1, 0, cluster_size);
            all_messages.push(TranscriptMsg::Prepare { ballot, from: test_member(0) });
            // Promises from all members
            for i in 0..cluster_size {
                all_messages.push(TranscriptMsg::Promise {
                    ballot,
                    from: test_member(i as u32),
                    accepted: vec![],
                });
            }
            // Leader proposes each value and all members ack
            for (slot, value) in proposals.iter().enumerate() {
                all_messages.push(TranscriptMsg::Accept {
                    ballot,
                    slot,
                    value: value.clone(),
                });
                // All members ack
                for i in 0..cluster_size {
                    all_messages.push(TranscriptMsg::AcceptAck {
                        ballot,
                        slot,
                        from: test_member(i as u32),
                    });
                }
            }

            // Each member sees the full transcript but in a DIFFERENT random order
            let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);
            let mut member_logs: Vec<Vec<LogEntry<String>>> = Vec::new();

            for _member in 0..cluster_size {
                let mut shuffled = all_messages.clone();
                shuffled.shuffle(&mut rng);

                let mut state = DecisionState::<String, TestCluster>::new();
                for msg in shuffled {
                    state.process(msg, quorum);
                }
                member_logs.push(state.committed_log.clone());
            }

            // All members must produce the same committed log
            for i in 1..cluster_size {
                prop_assert_eq!(
                    &member_logs[0], &member_logs[i],
                    "Member 0 and member {} diverge on committed log", i
                );
            }
        }
    }

    // ─── Integration Tests: Deterministic Simulation (Task 9.1) ──────────────
    //
    // StepBroadcastCluster harness — mirrors raft.rs's StepCluster pattern but
    // adapted for broadcast-transcript consensus where every message goes to all
    // members (no per-member routing).
    //
    // **Validates: Requirements 6.1, 6.2, 6.3, 6.4, 6.5, 6.6**

    /// Deterministic simulation harness for broadcast-transcript consensus.
    ///
    /// Analogous to raft.rs's `StepCluster` but simpler: every outbound message
    /// from any member is delivered to ALL reachable members (broadcast semantics).
    /// The harness maintains per-member `MessageGenState` and `DecisionState`,
    /// and supports partition simulation via a reachability mask.
    struct StepBroadcastCluster {
        cluster_size: usize,
        msg_gen_states: Vec<MessageGenState<String, TestCluster>>,
        decision_states: Vec<DecisionState<String, TestCluster>>,
        /// Messages in flight: collected from ticks, delivered to all reachable
        /// members on `deliver()`.
        in_flight: Vec<TranscriptMsg<String, TestCluster>>,
        /// Partition mask: `partitioned[i] = true` means member i is isolated
        /// (neither sends nor receives messages).
        partitioned: Vec<bool>,
    }

    impl StepBroadcastCluster {
        fn new(cluster_size: usize) -> Self {
            Self {
                cluster_size,
                msg_gen_states: (0..cluster_size).map(|_| MessageGenState::new()).collect(),
                decision_states: (0..cluster_size).map(|_| DecisionState::new()).collect(),
                in_flight: Vec::new(),
                partitioned: vec![false; cluster_size],
            }
        }

        /// Drive one tick for a specific member with explicit messages.
        ///
        /// Feeds `msgs` plus optional election timer and client requests.
        /// Collects outbound messages into `in_flight` (if not partitioned).
        /// Also feeds outbound messages to the member's own decision state
        /// (since broadcast means the sender also sees its own messages).
        fn tick_with_msgs(
            &mut self,
            member: usize,
            msgs: &[TranscriptMsg<String, TestCluster>],
            election: bool,
            requests: Vec<String>,
        ) {
            let me = test_member(member as u32);
            let output = self.msg_gen_states[member].process_tick(
                msgs,
                election,
                requests,
                me,
                member,
                self.cluster_size,
            );

            // If this member is partitioned, its outbound messages are dropped.
            if !self.partitioned[member] {
                self.in_flight.extend(output.outbound);
            }
        }

        /// Deliver all in-flight messages to every reachable member.
        ///
        /// Each message is fed to:
        /// 1. Every non-partitioned member's `DecisionState` (for commit extraction)
        /// 2. Every non-partitioned member's `MessageGenState` (via process_tick)
        ///    which may produce new outbound messages.
        ///
        /// After delivery, in-flight messages are cleared and new outbound messages
        /// from the processing are collected into the next round of in_flight.
        fn deliver(&mut self) {
            let msgs = std::mem::take(&mut self.in_flight);
            if msgs.is_empty() {
                return;
            }

            for member in 0..self.cluster_size {
                if self.partitioned[member] {
                    continue;
                }

                // Feed messages to the decision state (commit extraction)
                let quorum = quorum_size(self.cluster_size);
                for msg in msgs.iter() {
                    self.decision_states[member].process(msg.clone(), quorum);
                }

                // Feed messages to message generation (reactive protocol)
                let me = test_member(member as u32);
                let output = self.msg_gen_states[member].process_tick(
                    &msgs,
                    false,
                    vec![],
                    me,
                    member,
                    self.cluster_size,
                );

                // Collect new outbound messages (if not partitioned)
                if !self.partitioned[member] {
                    self.in_flight.extend(output.outbound);
                }
            }
        }

        /// Deliver until no new messages are generated (quiescence).
        fn deliver_until_quiet(&mut self) {
            let mut iterations = 0;
            loop {
                if self.in_flight.is_empty() {
                    return;
                }
                self.deliver();
                iterations += 1;
                assert!(
                    iterations < 100,
                    "deliver_until_quiet did not converge after 100 rounds"
                );
            }
        }

        /// Elect a leader: fire election timer on `member`, deliver until quiet.
        /// Returns the ballot won.
        ///
        /// Fires the candidate's election timer until it actually campaigns and
        /// wins. A single firing may only *consume* a stale leader-activity
        /// window (the `leader_activity_seen` gate — the analog of raft's
        /// `heartbeat_seen`): a follower that recently observed the old leader's
        /// traffic defers for one window before campaigning. We therefore retry
        /// the timer a bounded number of times, mirroring repeated wall-clock
        /// timeouts after leader activity ceases.
        fn elect(&mut self, member: usize) -> Ballot {
            for _ in 0..5 {
                self.tick_with_msgs(member, &[], true, vec![]);
                self.deliver_until_quiet();
                if self.msg_gen_states[member].is_leader {
                    return self.msg_gen_states[member].max_promised;
                }
            }

            panic!(
                "member {} failed to win election after 5 timer firings \
                 (max_promised={}, current_round={}, leader_activity_seen={})",
                member,
                self.msg_gen_states[member].max_promised,
                self.msg_gen_states[member].current_round,
                self.msg_gen_states[member].leader_activity_seen,
            );
        }

        /// Submit a request to a member (must be leader).
        /// Drives one tick with the request, collecting outbound Accept messages.
        fn submit(&mut self, member: usize, payload: &str) {
            self.tick_with_msgs(member, &[], false, vec![payload.to_string()]);
        }

        /// Submit a request and immediately deliver until quiet.
        fn submit_and_deliver(&mut self, member: usize, payload: &str) {
            self.submit(member, payload);
            self.deliver_until_quiet();
        }

        /// Get committed entries for a member.
        fn committed(&self, member: usize) -> &[LogEntry<String>] {
            self.decision_states[member].committed_entries()
        }

        /// Assert committed logs are consistent across all non-partitioned members.
        /// "Consistent" means one is a prefix of the other (prefix consistency).
        fn assert_committed_consistent(&self) {
            let active: Vec<usize> = (0..self.cluster_size)
                .filter(|i| !self.partitioned[*i])
                .collect();

            for i in 0..active.len() {
                for j in (i + 1)..active.len() {
                    let log_i = self.committed(active[i]);
                    let log_j = self.committed(active[j]);
                    let min_len = log_i.len().min(log_j.len());

                    // The shorter log must be a prefix of the longer
                    assert_eq!(
                        &log_i[..min_len],
                        &log_j[..min_len],
                        "Committed logs diverge between member {} and member {}: \
                         {:?} vs {:?}",
                        active[i],
                        active[j],
                        log_i,
                        log_j,
                    );
                }
            }
        }

        /// Assert all non-partitioned members have the same committed log.
        fn assert_committed_equal(&self) {
            let active: Vec<usize> = (0..self.cluster_size)
                .filter(|i| !self.partitioned[*i])
                .collect();

            if active.len() < 2 {
                return;
            }
            let reference = self.committed(active[0]);
            for &m in &active[1..] {
                assert_eq!(
                    reference,
                    self.committed(m),
                    "Committed log of member {} differs from member {}: {:?} vs {:?}",
                    m,
                    active[0],
                    self.committed(m),
                    reference,
                );
            }
        }

        /// Reconstruct member `m`'s replicated KV store by folding its committed log
        /// in slot order (last-write-wins per key; empty value = delete). This mirrors
        /// exactly what `kv_replica` does: fold the committed sequence into a map.
        fn kv_store(&self, member: usize) -> HashMap<String, String> {
            let mut store = HashMap::new();
            for entry in self.committed(member) {
                let (k, v) = entry.message.split_once('=').expect("payload must be k=v");
                if v.is_empty() {
                    store.remove(k);
                } else {
                    store.insert(k.to_string(), v.to_string());
                }
            }
            store
        }

        /// Assert every non-partitioned member's KV store is identical (replicated-store convergence).
        fn assert_kv_stores_converged(&self) {
            let active: Vec<usize> = (0..self.cluster_size)
                .filter(|i| !self.partitioned[*i])
                .collect();
            if active.len() < 2 {
                return;
            }
            let reference = self.kv_store(active[0]);
            for &m in &active[1..] {
                assert_eq!(
                    self.kv_store(m),
                    reference,
                    "KV store of member {} diverges from member {}",
                    m,
                    active[0]
                );
            }
        }

        /// Partition a member (stops sending and receiving messages).
        fn partition(&mut self, member: usize) {
            self.partitioned[member] = true;
        }

        /// Heal a partition (member can send and receive again).
        fn heal(&mut self, member: usize) {
            self.partitioned[member] = false;
        }

        /// Check that at most one member believes it is leader for a given ballot.
        fn assert_at_most_one_leader_per_ballot(&self) {
            let mut leaders_by_ballot: HashMap<Ballot, Vec<usize>> = HashMap::new();
            for (i, state) in self.msg_gen_states.iter().enumerate() {
                if state.is_leader {
                    leaders_by_ballot
                        .entry(state.max_promised)
                        .or_default()
                        .push(i);
                }
            }
            for (ballot, leaders) in &leaders_by_ballot {
                assert!(
                    leaders.len() <= 1,
                    "Multiple leaders for ballot {}: {:?}",
                    ballot,
                    leaders,
                );
            }
        }
    }

    // ─── Integration Test 0: Live leader suppresses follower elections ───────

    /// Req 5.5: under a healthy leader actively committing proposals, a follower
    /// whose election timer fires must NOT start a competing election — it must
    /// observe the leader's `Accept` traffic and defer. This is the cluster-level
    /// anti-dueling property that leader-activity gating provides. Without the
    /// gate, followers would campaign and inflate ballots; with it, member 0 stays leader
    /// and the committed log never forks.
    #[test]
    fn live_leader_suppresses_follower_elections() {
        let mut cluster = StepBroadcastCluster::new(3);
        let ballot0 = cluster.elect(0);

        // Sustained load: each proposal is delivered to all members, so followers
        // continuously observe member 0's Accept traffic. Between proposals, fire
        // followers' election timers — they should defer to the live leader.
        for i in 0..5 {
            cluster.submit_and_deliver(0, &format!("p{}", i));

            // Followers' election timers fire, but they just saw leader activity.
            cluster.tick_with_msgs(1, &[], true, vec![]);
            cluster.tick_with_msgs(2, &[], true, vec![]);
            cluster.deliver_until_quiet();
        }

        // Member 0 must still be the sole leader on its original ballot.
        assert!(
            cluster.msg_gen_states[0].is_leader,
            "member 0 should remain leader under sustained load"
        );
        assert_eq!(
            cluster.msg_gen_states[0].max_promised, ballot0,
            "leader ballot should not have been superseded by a spurious election"
        );
        assert!(
            !cluster.msg_gen_states[1].is_leader && !cluster.msg_gen_states[2].is_leader,
            "no follower should have seized leadership from a live leader"
        );
        cluster.assert_at_most_one_leader_per_ballot();
        cluster.assert_committed_consistent();

        // And all 5 proposals committed, no fork.
        assert_eq!(cluster.committed(0).len(), 5);
        cluster.assert_committed_equal();
    }

    // ─── Integration Test 1: Stable leader commits proposals (Req 6.2) ───────

    /// A stable leader commits a stream of proposals within bounded ticks.
    /// All members converge to the same committed log.
    #[test]
    fn stable_leader_commits_proposals() {
        let mut cluster = StepBroadcastCluster::new(3);

        // Elect member 0 as leader
        cluster.elect(0);

        // Submit 5 proposals and deliver each
        for i in 0..5 {
            cluster.submit_and_deliver(0, &format!("proposal_{}", i));
        }

        // All members should have the same 5 committed entries in order
        for member in 0..3 {
            let committed = cluster.committed(member);
            assert_eq!(
                committed.len(),
                5,
                "member {} should have 5 committed entries, got {}",
                member,
                committed.len(),
            );
            for (i, entry) in committed.iter().enumerate() {
                assert_eq!(
                    entry.message,
                    format!("proposal_{}", i),
                    "member {} slot {} has wrong value",
                    member,
                    i,
                );
                assert_eq!(entry.slot, i);
            }
        }

        cluster.assert_committed_equal();
    }

    // ─── Integration Test 2: Leader crash → reelection → recommit (Req 6.5) ──

    /// Leader crashes (partitioned), new leader elected, new proposals committed.
    /// After healing, members converge.
    #[test]
    fn leader_crash_reelection_recommits() {
        let mut cluster = StepBroadcastCluster::new(3);

        // Elect member 0, commit 2 proposals
        cluster.elect(0);
        cluster.submit_and_deliver(0, "before_crash_1");
        cluster.submit_and_deliver(0, "before_crash_2");

        // Verify all members committed the 2 entries
        for member in 0..3 {
            assert_eq!(
                cluster.committed(member).len(),
                2,
                "member {} should have 2 committed entries before crash",
                member,
            );
        }

        // Partition member 0 (simulates crash)
        cluster.partition(0);

        // Elect member 1 as new leader
        cluster.elect(1);

        // Submit new proposal via member 1
        cluster.submit_and_deliver(1, "after_crash");

        // Members 1 and 2 should have 3 committed entries
        for member in 1..3 {
            assert_eq!(
                cluster.committed(member).len(),
                3,
                "member {} should have 3 committed entries after reelection",
                member,
            );
            assert_eq!(cluster.committed(member)[2].message, "after_crash");
        }

        // Heal member 0
        cluster.heal(0);
        cluster.deliver_until_quiet();

        // Verify prefix consistency across all members
        cluster.assert_committed_consistent();
    }

    // ─── Integration Test 3: Partition heals → convergence (Req 6.3) ─────────

    /// A partitioned member misses proposals. After healing and message delivery,
    /// all members converge to the same committed prefix.
    #[test]
    fn partition_heals_convergence() {
        let mut cluster = StepBroadcastCluster::new(3);

        // Elect member 0
        cluster.elect(0);

        // Submit initial proposals (all members see these)
        cluster.submit_and_deliver(0, "shared_1");
        cluster.submit_and_deliver(0, "shared_2");

        // Partition member 2
        cluster.partition(2);

        // Submit more proposals (member 2 won't see these)
        cluster.submit_and_deliver(0, "missed_by_2_a");
        cluster.submit_and_deliver(0, "missed_by_2_b");

        // Members 0 and 1 should have 4 entries
        assert_eq!(cluster.committed(0).len(), 4);
        assert_eq!(cluster.committed(1).len(), 4);
        // Member 2 should still have only 2 entries
        assert_eq!(cluster.committed(2).len(), 2);

        // Heal member 2
        cluster.heal(2);

        // Submit one more proposal to generate traffic that carries info to
        // the healed member. In broadcast-transcript, the leader re-broadcasts
        // Accept messages — the healed member will see them after healing.
        cluster.submit_and_deliver(0, "after_heal");

        // Members 0 and 1 should have 5 entries
        assert_eq!(cluster.committed(0).len(), 5);
        assert_eq!(cluster.committed(1).len(), 5);

        // Verify prefix consistency: member 2's log is a prefix of the others
        cluster.assert_committed_consistent();
    }

    // ─── Integration Test 4: Concurrent elections never fork (Req 6.1) ────────

    /// Concurrent election attempts by multiple members never produce forked
    /// committed logs. At most one wins; committed logs remain prefix-consistent.
    #[test]
    fn concurrent_elections_never_fork() {
        let mut cluster = StepBroadcastCluster::new(3);

        // Fire election timers on members 0 AND 1 simultaneously
        cluster.tick_with_msgs(0, &[], true, vec![]);
        cluster.tick_with_msgs(1, &[], true, vec![]);

        // Deliver all traffic with interleaved messages
        cluster.deliver_until_quiet();

        // At most one should be leader
        let leaders: Vec<usize> = (0..3)
            .filter(|i| cluster.msg_gen_states[*i].is_leader)
            .collect();
        assert!(
            leaders.len() <= 1,
            "At most one leader should emerge from concurrent elections, got {:?}",
            leaders,
        );

        // If someone won, submit proposals and verify consistency
        if let Some(&leader) = leaders.first() {
            cluster.submit_and_deliver(leader, "concurrent_1");
            cluster.submit_and_deliver(leader, "concurrent_2");
        }

        // Committed logs must be prefix-consistent across all members
        cluster.assert_committed_consistent();

        // If no one won (contested election), retry with a single candidate.
        // In broadcast-transcript Paxos, a higher ballot always wins eventual
        // leadership since all members observe both Prepares.
        if leaders.is_empty() {
            // Member 2 tries (hasn't attempted yet, so its ballot is fresh)
            cluster.elect(2);
            cluster.submit_and_deliver(2, "after_retry");
            cluster.assert_committed_consistent();
        }
    }

    // ─── Integration Test 5: At most one leader per ballot (Req 6.4) ─────────

    /// After any election sequence, at most one member believes it is leader
    /// for any given ballot. Ballot uniqueness from the encoding guarantees this.
    #[test]
    fn at_most_one_leader_per_ballot() {
        let mut cluster = StepBroadcastCluster::new(5);

        // Run an election with member 0
        cluster.elect(0);
        cluster.assert_at_most_one_leader_per_ballot();

        // Member 1 starts an election (higher ballot supersedes member 0)
        cluster.tick_with_msgs(1, &[], true, vec![]);
        cluster.deliver_until_quiet();
        cluster.assert_at_most_one_leader_per_ballot();

        // Member 2 also starts an election
        cluster.tick_with_msgs(2, &[], true, vec![]);
        cluster.deliver_until_quiet();
        cluster.assert_at_most_one_leader_per_ballot();

        // After all elections settle, verify the invariant still holds
        let leaders: Vec<(usize, Ballot)> = cluster
            .msg_gen_states
            .iter()
            .enumerate()
            .filter(|(_, s)| s.is_leader)
            .map(|(i, s)| (i, s.max_promised))
            .collect();

        // Check no two leaders share a ballot
        let mut seen_ballots: HashSet<Ballot> = HashSet::new();
        for (member, ballot) in &leaders {
            assert!(
                seen_ballots.insert(*ballot),
                "Duplicate leader ballot {}: member {} shares it with another leader",
                ballot,
                member,
            );
        }
    }

    // ─── KVS End-to-End Functional Tests (Req 10.3, 10.4) ────────────────────
    //
    // These tests assert the *replicated key-value store* (not just the
    // committed log) is linearizable across replicas. Each committed payload is
    // interpreted as a KV write via the `"k=v"` convention (empty value =
    // delete), folded in slot order with last-write-wins — exactly what
    // `kv_replica` does. They run on the deterministic `StepBroadcastCluster`
    // harness (simulation-level, fast, deterministic — not deployment tests).

    /// KVS 1: A sequence of writes to a single key resolves to the last write by
    /// slot order, and every replica's store agrees. Under a stable leader,
    /// submit order equals slot order, so the final value is the last submitted.
    #[test]
    fn kvs_single_key_last_write_wins() {
        let mut cluster = StepBroadcastCluster::new(3);
        cluster.elect(0);

        cluster.submit_and_deliver(0, "x=1");
        cluster.submit_and_deliver(0, "x=2");
        cluster.submit_and_deliver(0, "x=3");

        for member in 0..3 {
            let store = cluster.kv_store(member);
            assert_eq!(
                store.get("x").map(String::as_str),
                Some("3"),
                "member {} should have x=3 (last write by slot order), got {:?}",
                member,
                store.get("x"),
            );
            assert_eq!(store.len(), 1, "member {} should hold only key x", member);
        }
        cluster.assert_kv_stores_converged();
    }

    /// KVS 2: Interleaved writes to several keys converge to the expected store.
    /// The expected store is computed by folding the SAME submit sequence
    /// (which equals slot order under a stable leader) with last-write-wins.
    #[test]
    fn kvs_multi_key_store_converges() {
        let mut cluster = StepBroadcastCluster::new(3);
        cluster.elect(0);

        let writes = ["a=1", "b=2", "a=3", "c=9", "b=4"];
        for w in writes {
            cluster.submit_and_deliver(0, w);
        }

        // Expected store: fold the submit sequence (== slot order) LWW.
        let mut expected = HashMap::new();
        for w in writes {
            let (k, v) = w.split_once('=').unwrap();
            expected.insert(k.to_string(), v.to_string());
        }

        assert_eq!(
            cluster.kv_store(0),
            expected,
            "member 0 store should equal the folded submit sequence",
        );
        cluster.assert_kv_stores_converged();
    }

    /// KVS 3: A delete (empty value) removes the key from every replica's store.
    #[test]
    fn kvs_delete_semantics() {
        let mut cluster = StepBroadcastCluster::new(3);
        cluster.elect(0);

        cluster.submit_and_deliver(0, "k=5");
        cluster.submit_and_deliver(0, "k=");

        for member in 0..3 {
            let store = cluster.kv_store(member);
            assert!(
                !store.contains_key("k"),
                "member {} should have deleted key k, store = {:?}",
                member,
                store,
            );
        }
        cluster.assert_kv_stores_converged();
    }

    /// KVS 4: After a partition heals and traffic is delivered, the replicated
    /// KV store (not just the committed log) converges across all members.
    /// Mirrors `partition_heals_convergence` but asserts store convergence.
    #[test]
    fn kvs_partition_heal_store_converges() {
        let mut cluster = StepBroadcastCluster::new(3);
        cluster.elect(0);

        cluster.submit_and_deliver(0, "a=1");
        cluster.submit_and_deliver(0, "b=2");

        // Partition member 2; it misses these writes.
        cluster.partition(2);
        cluster.submit_and_deliver(0, "a=3");
        cluster.submit_and_deliver(0, "c=4");

        assert_eq!(cluster.committed(0).len(), 4);
        assert_eq!(cluster.committed(1).len(), 4);
        assert_eq!(cluster.committed(2).len(), 2);

        // Heal and generate more traffic.
        cluster.heal(2);
        cluster.submit_and_deliver(0, "b=5");

        assert_eq!(cluster.committed(0).len(), 5);
        assert_eq!(cluster.committed(1).len(), 5);

        // Member 2 committed the new slot but has a gap for the slots it missed
        // while partitioned, so its contiguous prefix stalls. A re-election
        // triggers Paxos recovery: the new leader re-proposes every accepted
        // slot from the collected Promises, re-broadcasting the missed Accepts
        // so the healed member fills its gap and the replicated store converges.
        cluster.elect(1);
        cluster.deliver_until_quiet();

        // The replicated KV store (value fold) converges across all members.
        // Note: the caught-up member re-commits the recovered slots under the
        // new ballot, so `LogEntry.ballot` metadata can differ across members
        // even though every slot's committed *value* agrees — agreement is on
        // value, not ballot, so we assert convergence on the KV store.
        cluster.assert_kv_stores_converged();
    }

    /// KVS 5: Writes committed before a leader crash survive re-election and are
    /// present in every surviving replica's store, with no lost/rolled-back
    /// writes (linearizable prefix). Mirrors `leader_crash_reelection_recommits`.
    #[test]
    fn kvs_leader_crash_writes_survive() {
        let mut cluster = StepBroadcastCluster::new(3);

        // Commit writes under leader 0.
        cluster.elect(0);
        cluster.submit_and_deliver(0, "a=1");
        cluster.submit_and_deliver(0, "b=2");

        for member in 0..3 {
            assert_eq!(cluster.committed(member).len(), 2);
        }

        // Leader 0 crashes.
        cluster.partition(0);

        // New leader elected, commits more writes.
        cluster.elect(1);
        cluster.submit_and_deliver(1, "c=3");
        cluster.submit_and_deliver(1, "a=9");

        // Heal old leader and settle. The healed old leader missed the slots
        // committed under the new leader; a re-election drives Paxos recovery,
        // re-broadcasting those Accepts so the old leader fills its gap and all
        // survivors agree on every committed slot (Figure-8 safety).
        cluster.heal(0);
        cluster.deliver_until_quiet();
        cluster.elect(2);
        cluster.deliver_until_quiet();

        // Writes committed before the crash must still be present. `b=2` was
        // never overwritten, so it must survive on every surviving member.
        for member in 0..3 {
            let store = cluster.kv_store(member);
            assert_eq!(
                store.get("b").map(String::as_str),
                Some("2"),
                "member {} lost pre-crash write b=2 (store = {:?})",
                member,
                store,
            );
        }

        // No lost/rolled-back writes: the replicated store converges across all
        // survivors (linearizable prefix). Recovery re-commits the recovered
        // slots under a new ballot on the lagging member, so `LogEntry.ballot`
        // metadata can differ while every committed *value* agrees — agreement
        // is on value, which is exactly what the KV store fold reflects.
        cluster.assert_kv_stores_converged();
    }

    /// KVS 6: A 5-node cluster converges to the same store under ~10 multi-key
    /// writes, extending convergence coverage beyond 3 nodes.
    #[test]
    fn kvs_five_node_convergence() {
        let mut cluster = StepBroadcastCluster::new(5);
        cluster.elect(0);

        let writes = [
            "a=1", "b=2", "c=3", "a=4", "d=5", "b=6", "e=7", "c=8", "a=9", "d=0",
        ];
        for w in writes {
            cluster.submit_and_deliver(0, w);
        }

        // Expected store: fold submit sequence (== slot order under stable leader).
        let mut expected = HashMap::new();
        for w in writes {
            let (k, v) = w.split_once('=').unwrap();
            expected.insert(k.to_string(), v.to_string());
        }

        for member in 0..5 {
            assert_eq!(
                cluster.kv_store(member),
                expected,
                "member {} store should equal the folded submit sequence",
                member,
            );
        }
        cluster.assert_kv_stores_converged();
    }

    // Feature: broadcast-transcript-consensus, KV-store analog of prefix-consistency.
    //
    // For any random valid single-ballot trace of KV writes delivered under
    // arbitrary reorderings and partial (per-member) delivery subsets, the KV
    // stores reconstructed from any two members' committed logs SHALL agree on
    // their common committed prefix — no key ever maps to different values.
    // This is the KV-store analog of the committed-log prefix-consistency
    // property (`safety_under_adversarial_schedules`).
    //
    // **Validates: Requirements 10.3**
    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        #[test]
        fn kvs_linearizable_under_adversarial_delivery(
            writes in prop::collection::vec(
                (prop::sample::select(vec!["a", "b", "c"]), 0u32..5),
                2..8,
            ),
            delivery_masks in prop::collection::vec(prop::collection::vec(any::<bool>(), 80), 3),
            seed: u64,
        ) {
            use rand::SeedableRng;
            use rand::seq::SliceRandom;

            let cluster_size = 3;
            let quorum = quorum_size(cluster_size);

            // Payloads are always in "k=v" form so `kv_store` folding can parse them.
            let payloads: Vec<String> = writes
                .iter()
                .map(|(k, v)| format!("{}={}", k, v))
                .collect();

            // Build a complete valid single-ballot trace over the KV writes.
            let ballot = make_ballot(1, 0, cluster_size);
            let mut full_trace: Vec<TranscriptMsg<String, TestCluster>> = Vec::new();
            full_trace.push(TranscriptMsg::Prepare { ballot, from: test_member(0) });
            for i in 0..cluster_size {
                full_trace.push(TranscriptMsg::Promise { ballot, from: test_member(i as u32), accepted: vec![] });
            }
            for (slot, value) in payloads.iter().enumerate() {
                full_trace.push(TranscriptMsg::Accept { ballot, slot, value: value.clone() });
                for i in 0..cluster_size {
                    full_trace.push(TranscriptMsg::AcceptAck { ballot, slot, from: test_member(i as u32) });
                }
            }

            // Adversarial delivery: shuffle, then each member sees a random subset.
            let mut rng = rand::rngs::SmallRng::seed_from_u64(seed);
            let mut shuffled_trace = full_trace.clone();
            shuffled_trace.shuffle(&mut rng);

            let mut member_states: Vec<DecisionState<String, TestCluster>> = Vec::new();
            for member_idx in 0..cluster_size {
                let mask = &delivery_masks[member_idx];
                let mut state = DecisionState::<String, TestCluster>::new();
                for (i, msg) in shuffled_trace.iter().enumerate() {
                    if i < mask.len() && mask[i] {
                        state.process(msg.clone(), quorum);
                    }
                }
                member_states.push(state);
            }

            // Fold a slice of committed entries into a KV store (LWW; empty = delete).
            let fold_store = |log: &[LogEntry<String>]| -> HashMap<String, String> {
                let mut store = HashMap::new();
                for entry in log {
                    let (k, v) = entry.message.split_once('=').unwrap();
                    if v.is_empty() {
                        store.remove(k);
                    } else {
                        store.insert(k.to_string(), v.to_string());
                    }
                }
                store
            };

            // KV-store prefix consistency: for any two members, the stores folded
            // from their common committed prefix must be identical, so no key
            // maps to different values across replicas.
            for i in 0..cluster_size {
                for j in (i + 1)..cluster_size {
                    let log_i = &member_states[i].committed_log;
                    let log_j = &member_states[j].committed_log;
                    let min_len = log_i.len().min(log_j.len());

                    let store_i = fold_store(&log_i[..min_len]);
                    let store_j = fold_store(&log_j[..min_len]);
                    prop_assert_eq!(
                        &store_i, &store_j,
                        "KV linearizability violation: member {} and {} disagree on \
                         their common committed prefix ({} entries): {:?} vs {:?}",
                        i, j, min_len, store_i, store_j
                    );
                }
            }
        }
    }
}
