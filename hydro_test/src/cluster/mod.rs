#[cfg(feature = "tokio")]
pub mod compartmentalized_paxos;
#[cfg(feature = "tokio")]
pub mod compute_pi;
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

// hydrofrac (`EcCheck`) targets and checks.
pub mod reliable_broadcast;
pub mod two_pc_ctp;

pub mod ec_hydrofrac;
pub mod ec_hydrofrac_classes;
#[cfg(test)]
mod ec_two_pc_hydrofrac;
#[cfg(test)]
mod hydrofrac_provenance_tests;

#[cfg(test)]
mod ec_demos_hydrofrac;

#[cfg(test)]
mod ec_bruteforce;

#[cfg(test)]
mod ec_races_targets;

#[cfg(test)]
mod ec_quorum_trust;

#[cfg(test)]
mod ec_implies_checks;
#[cfg(test)]
mod ec_implies_teaching;
#[cfg(test)]
mod ec_reliable_send;
#[cfg(test)]
mod ec_teaching;

#[cfg(test)]
mod ec_teaching_checks;
