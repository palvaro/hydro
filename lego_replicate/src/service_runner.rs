//! Standalone service runner: receives committed commands from the Hydro
//! dataflow via TCP, applies them to a persistent backend, serves reads.
//!
//! This separates the Hydro replication plumbing from the state machine.
//! The service process:
//! - Owns the backend (redb/fjall/rusqlite) independently of Hydro's lifecycle
//! - Persists max_applied_seq alongside the data
//! - On restart, resumes from where it left off (idempotent replay)
//! - Serves reads directly without going through the dataflow

use std::collections::HashMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// A standalone service that receives (seq, payload) from the dataflow
/// and applies them to a backend. Tracks max_applied_seq for idempotent replay.
pub struct ServiceRunner<S: crate::ReplicableService> {
    service: S,
    max_applied: usize,
}

impl<S: crate::ReplicableService> ServiceRunner<S>
where
    S::Command: serde::de::DeserializeOwned,
    S::Response: serde::Serialize,
{
    /// Create a new runner. If the backend has persisted state, pass the
    /// max_applied_seq from the last run. Otherwise pass 0.
    pub fn new(service: S, resume_from: usize) -> Self {
        Self { service, max_applied: resume_from }
    }

    /// Create from a fresh default service.
    pub fn fresh() -> Self {
        Self { service: S::default(), max_applied: 0 }
    }

    /// The sequence number to resume from (tell the dataflow to send from here).
    pub fn resume_seq(&self) -> usize {
        self.max_applied
    }

    /// Apply a single committed command. Idempotent — skips if already applied.
    /// Returns Some(response) if applied, None if skipped.
    pub fn apply(&mut self, seq: usize, payload: &[u8]) -> Option<S::Response> {
        if seq < self.max_applied {
            return None; // already applied
        }
        let cmd: S::Command = bincode::deserialize(payload).expect("deserialize command");
        let resp = self.service.apply(cmd);
        self.max_applied = seq + 1;
        Some(resp)
    }

    /// Apply a read-only command directly (no sequencing needed).
    pub fn apply_read(&mut self, payload: &[u8]) -> S::Response {
        let cmd: S::Command = bincode::deserialize(payload).expect("deserialize command");
        self.service.apply(cmd)
    }

    /// Get a snapshot of the service state.
    pub fn snapshot(&self) -> Vec<u8> {
        self.service.snapshot()
    }

    /// Restore from a snapshot.
    pub fn restore(&mut self, data: &[u8]) {
        self.service.restore(data);
    }

    /// Get the underlying service (for direct access).
    pub fn service(&self) -> &S {
        &self.service
    }

    /// Get the underlying service mutably.
    pub fn service_mut(&mut self) -> &mut S {
        &mut self.service
    }
}

/// Wire format for messages between the dataflow and the service runner.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub enum ServiceMessage {
    /// A committed command to apply. (seq, payload)
    Commit(usize, Vec<u8>),
    /// A read-only command to apply directly. (request_id, payload)
    Read(u64, Vec<u8>),
    /// Query: what's your max_applied_seq?
    ResumeQuery,
}

/// Wire format for responses from the service runner.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub enum ServiceResponse {
    /// Response to a committed command. (seq, response_bytes)
    CommitResponse(usize, Vec<u8>),
    /// Response to a read. (request_id, response_bytes)
    ReadResponse(u64, Vec<u8>),
    /// Answer to ResumeQuery.
    ResumeAnswer(usize),
}

/// Run the service as a TCP server. Receives ServiceMessages, applies them,
/// sends back ServiceResponses.
///
/// This is the standalone process that owns the backend.
pub async fn run_service_server<S: crate::ReplicableService>(
    bind_addr: &str,
    mut runner: ServiceRunner<S>,
) -> std::io::Result<()>
where
    S::Command: serde::de::DeserializeOwned + Send,
    S::Response: serde::Serialize + Send,
{
    let listener = TcpListener::bind(bind_addr).await?;
    println!("[SERVICE] Listening on {}, resume_seq={}", bind_addr, runner.resume_seq());

    loop {
        let (mut stream, addr) = listener.accept().await?;
        println!("[SERVICE] Connection from {}", addr);

        loop {
            // Read length-prefixed message
            let len = match stream.read_u32().await {
                Ok(n) => n as usize,
                Err(_) => break, // connection closed
            };
            let mut buf = vec![0u8; len];
            if stream.read_exact(&mut buf).await.is_err() { break; }

            let msg: ServiceMessage = bincode::deserialize(&buf).expect("deserialize msg");
            let response = match msg {
                ServiceMessage::Commit(seq, payload) => {
                    match runner.apply(seq, &payload) {
                        Some(resp) => {
                            let resp_bytes = bincode::serialize(&resp).unwrap();
                            ServiceResponse::CommitResponse(seq, resp_bytes)
                        }
                        None => ServiceResponse::CommitResponse(seq, vec![]), // already applied
                    }
                }
                ServiceMessage::Read(req_id, payload) => {
                    let resp = runner.apply_read(&payload);
                    let resp_bytes = bincode::serialize(&resp).unwrap();
                    ServiceResponse::ReadResponse(req_id, resp_bytes)
                }
                ServiceMessage::ResumeQuery => {
                    ServiceResponse::ResumeAnswer(runner.resume_seq())
                }
            };

            let resp_bytes = bincode::serialize(&response).unwrap();
            let _ = stream.write_u32(resp_bytes.len() as u32).await;
            let _ = stream.write_all(&resp_bytes).await;
        }
    }
}

/// Convenience: create a ServiceRunner for the redb backend that resumes
/// from persisted state.
/// Convenience: create a fresh ServiceRunner for the redb backend.
#[cfg(feature = "backend_redb")]
pub fn redb_runner() -> ServiceRunner<crate::backends::redb::RedbService> {
    ServiceRunner::fresh()
}
