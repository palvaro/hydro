//! The [`ReplicableService`] trait definition.
//!
//! Defines the core abstraction that users implement to make their service
//! transparently replicated. The trait captures the four operations needed by
//! the primary/backup protocol: command application, read-only classification,
//! snapshotting, and state restoration.

use serde::{Serialize, de::DeserializeOwned};
use std::fmt::Debug;

/// A service that can be transparently replicated via primary/backup replication.
///
/// Implementors wrap a replication-oblivious service and expose it through this
/// trait. The replication layer handles sequencing, quorum acknowledgment, view
/// changes, and state transfer — all invisible to the service implementation.
///
/// # Observational Determinism Requirement
///
/// The [`apply`](ReplicableService::apply) method **must be observationally
/// deterministic**: given the same sequence of commands starting from the same
/// initial state, it must produce the same sequence of responses on every replica.
pub trait ReplicableService: Send + Default + 'static {
    /// The command type sent by clients and replicated across the cluster.
    type Command: Serialize + DeserializeOwned + Clone + Debug + Send;

    /// The response type returned to clients after a command is committed and applied.
    type Response: Serialize + DeserializeOwned + Clone + Debug + Send;

    /// Apply a command to the service, returning a response.
    /// Must be deterministic.
    fn apply(&mut self, command: Self::Command) -> Self::Response;

    /// Returns `true` if the given command is read-only (has no side effects).
    fn is_read_only(command: &Self::Command) -> bool;

    /// Serialize the entire service state to a byte vector.
    fn snapshot(&self) -> Vec<u8>;

    /// Restore service state from a previously-taken snapshot.
    fn restore(&mut self, data: &[u8]);
}
