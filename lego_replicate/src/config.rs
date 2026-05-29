//! Configuration types for the replication protocol.

/// Configuration for the Paxos consensus subsystem used in view changes.
#[derive(Clone, Copy, Debug)]
pub struct PaxosConfig {
    pub f: usize,
    pub i_am_leader_send_timeout: u64,
    pub i_am_leader_check_timeout: u64,
    pub i_am_leader_check_timeout_delay_multiplier: usize,
}

/// Top-level configuration for the transparent replication protocol.
#[derive(Clone, Debug)]
pub struct ReplicateConfig {
    pub initial_members: Vec<u32>,
    pub f: usize,
    pub commit_timeout_ms: u64,
    pub notification_interval_ms: u64,
    pub backup_apply: bool,
    pub paxos_config: PaxosConfig,
}

impl Default for ReplicateConfig {
    fn default() -> Self {
        Self {
            initial_members: vec![0, 1, 2],
            f: 1,
            commit_timeout_ms: 5000,
            notification_interval_ms: 1666,
            backup_apply: true,
            paxos_config: PaxosConfig {
                f: 1,
                i_am_leader_send_timeout: 5,
                i_am_leader_check_timeout: 10,
                i_am_leader_check_timeout_delay_multiplier: 15,
            },
        }
    }
}
