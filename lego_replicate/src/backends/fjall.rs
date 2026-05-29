//! fjall backend implementation of `ReplicableService`.

use crate::ReplicableService;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

pub struct FjallService {
    #[allow(dead_code)]
    keyspace: Arc<fjall::Keyspace>,
    partition: fjall::PartitionHandle,
    _tempdir: Option<Arc<tempfile::TempDir>>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum FjallCommand {
    Insert { key: Vec<u8>, value: Vec<u8> },
    Get { key: Vec<u8> },
    Remove { key: Vec<u8> },
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum FjallResponse {
    Ok,
    Value(Option<Vec<u8>>),
    Removed(bool),
}

impl Default for FjallService {
    fn default() -> Self {
        let tempdir = tempfile::tempdir().expect("failed to create temp dir for fjall");
        let keyspace = fjall::Config::new(tempdir.path())
            .open()
            .expect("failed to open fjall keyspace");
        let partition = keyspace
            .open_partition("data", fjall::PartitionCreateOptions::default())
            .expect("failed to open fjall partition");
        Self {
            keyspace: Arc::new(keyspace),
            partition,
            _tempdir: Some(Arc::new(tempdir)),
        }
    }
}

impl ReplicableService for FjallService {
    type Command = FjallCommand;
    type Response = FjallResponse;

    fn apply(&mut self, command: Self::Command) -> Self::Response {
        match command {
            FjallCommand::Insert { key, value } => {
                self.partition.insert(key, value).expect("fjall insert failed");
                FjallResponse::Ok
            }
            FjallCommand::Get { key } => {
                let value = self.partition.get(key).expect("fjall get failed").map(|s| s.to_vec());
                FjallResponse::Value(value)
            }
            FjallCommand::Remove { key } => {
                let existed = self.partition.get(&key).expect("fjall get failed").is_some();
                self.partition.remove(&key).expect("fjall remove failed");
                FjallResponse::Removed(existed)
            }
        }
    }

    fn is_read_only(command: &Self::Command) -> bool {
        matches!(command, FjallCommand::Get { .. })
    }

    fn snapshot(&self) -> Vec<u8> {
        let pairs: Vec<(Vec<u8>, Vec<u8>)> = self
            .partition
            .iter()
            .map(|entry| {
                let entry = entry.expect("fjall iter entry failed");
                (entry.0.to_vec(), entry.1.to_vec())
            })
            .collect();
        bincode::serialize(&pairs).expect("serialize failed")
    }

    fn restore(&mut self, data: &[u8]) {
        let pairs: Vec<(Vec<u8>, Vec<u8>)> =
            bincode::deserialize(data).expect("deserialize failed");
        let keys: Vec<Vec<u8>> = self
            .partition
            .iter()
            .map(|entry| entry.expect("fjall iter entry failed").0.to_vec())
            .collect();
        for key in keys {
            self.partition.remove(key).expect("fjall remove failed");
        }
        for (key, value) in pairs {
            self.partition.insert(key, value).expect("fjall insert failed");
        }
    }
}
