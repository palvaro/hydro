#[cfg(feature = "tokio")]
pub mod broadcast_transcript_consensus;
#[cfg(feature = "tokio")]
pub mod compartmentalized_paxos;
#[cfg(feature = "tokio")]
pub mod compute_pi;
#[cfg(feature = "tokio")]
pub mod consensus_bench;
pub mod kv_replica;
pub mod many_to_many;
pub mod map_reduce;
#[cfg(feature = "tokio")]
pub mod paxos;
#[cfg(feature = "tokio")]
pub mod paxos_bench;
#[cfg(feature = "tokio")]
pub mod paxos_log_bench;
#[cfg(feature = "tokio")]
pub mod paxos_with_client;
pub mod raft;
pub mod simple_cluster;
#[cfg(feature = "tokio")]
pub mod two_pc;
#[cfg(feature = "tokio")]
pub mod two_pc_bench;

// WIP: the following modules were written against an older hydro_lang API
// (they call `broadcast_from_member`, which has since been renamed/removed,
// and `typed_consensus` has a `T`/`usize` type mismatch at ~line 360).
// The source is preserved in git; re-enable these `mod` declarations after
// porting them to the current API (see `broadcast_closed`).
// #[cfg(feature = "tokio")]
// pub mod paxos_ec;
// #[cfg(feature = "tokio")]
// pub mod typed_consensus;
// #[cfg(feature = "tokio")]
// pub mod state_space_comparison;
