//! Wire protocol message types for the lego replication protocol.
//!
//! All messages use opaque byte payloads — the protocol layer never
//! knows the concrete command type. Only the application adapter
//! serializes/deserializes commands.

use hydro_lang::location::MemberId;
use serde::{Deserialize, Serialize};

/// Cluster marker type for replicas.
#[derive(Serialize, Deserialize, Clone)]
pub struct TransparentReplica {}

/// Cluster marker type for the router process.
pub struct Router;

/// Cluster marker for benchmark clients.
pub struct BenchClient;

/// Process marker for benchmark aggregator.
pub struct BenchAggregator;

/// Current view of the cluster membership.
///
/// `members[0]` is the primary for this view. Views are ordered by `view_num`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct View {
    pub view_num: u64,
    pub members: Vec<u32>,
}

impl Ord for View {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.view_num.cmp(&other.view_num)
    }
}

impl PartialOrd for View {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl View {
    /// Returns the primary replica's ID (`members[0]`).
    pub fn primary(&self) -> u32 {
        self.members[0]
    }

    /// Returns `true` if the given member ID is part of this view.
    pub fn contains(&self, member_id: u32) -> bool {
        self.members.contains(&member_id)
    }
}

/// Replicate message: primary → all backups. Carries opaque payload bytes.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Replicate {
    pub view_num: u64,
    pub seq: usize,
    pub payload: Vec<u8>,
    pub sender: MemberId<TransparentReplica>,
}

/// Acknowledgment: backup → primary. Confirms receipt of a sequence number.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Ack {
    pub view_num: u64,
    pub seq: usize,
    pub sender: MemberId<TransparentReplica>,
}

/// Periodic commit notification from primary to all replicas.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct CommitNotification {
    pub view_num: u64,
    pub committed_seqs: Vec<usize>,
    pub pending_acks: Vec<(usize, Vec<u32>)>,
}

/// State transfer request from new primary to surviving backups.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct StateTransferRequest {
    pub view_num: u64,
    pub requester: MemberId<TransparentReplica>,
}
