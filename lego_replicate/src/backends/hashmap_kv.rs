//! In-memory HashMap KV backend for benchmarking.
//! Same semantics as the Hydro Paxos demo's KV store.

use crate::ReplicableService;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum KvCommand {
    Put { key: String, value: String },
    Get { key: String },
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum KvResponse {
    Ok,
    Value(Option<String>),
}

#[derive(Default)]
pub struct HashMapKv {
    store: HashMap<String, String>,
}

impl ReplicableService for HashMapKv {
    type Command = KvCommand;
    type Response = KvResponse;

    fn apply(&mut self, command: KvCommand) -> KvResponse {
        match command {
            KvCommand::Put { key, value } => {
                self.store.insert(key, value);
                KvResponse::Ok
            }
            KvCommand::Get { key } => {
                KvResponse::Value(self.store.get(&key).cloned())
            }
        }
    }

    fn is_read_only(command: &KvCommand) -> bool {
        matches!(command, KvCommand::Get { .. })
    }

    fn snapshot(&self) -> Vec<u8> {
        bincode::serialize(&self.store).unwrap()
    }

    fn restore(&mut self, data: &[u8]) {
        self.store = bincode::deserialize(data).unwrap();
    }
}
