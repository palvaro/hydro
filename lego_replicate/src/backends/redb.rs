//! redb backend implementation of `ReplicableService`.

use crate::ReplicableService;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use redb::{Database, ReadableTable, TableDefinition, TableError};

const TABLE: TableDefinition<'static, &[u8], &[u8]> = TableDefinition::new("kv");

pub struct RedbService {
    db: Arc<Database>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum RedbCommand {
    Put { key: Vec<u8>, value: Vec<u8> },
    Get { key: Vec<u8> },
    Delete { key: Vec<u8> },
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum RedbResponse {
    Ok,
    Value(Option<Vec<u8>>),
    Deleted(bool),
}

impl Default for RedbService {
    fn default() -> Self {
        let tmp = tempfile::NamedTempFile::new().expect("failed to create temp file for redb");
        let db = Database::create(tmp.path()).expect("failed to create redb database");
        Self { db: Arc::new(db) }
    }
}

impl ReplicableService for RedbService {
    type Command = RedbCommand;
    type Response = RedbResponse;

    fn apply(&mut self, command: Self::Command) -> Self::Response {
        match command {
            RedbCommand::Put { key, value } => {
                let write_txn = self.db.begin_write().expect("begin_write failed");
                {
                    let mut table = write_txn.open_table(TABLE).expect("open_table failed");
                    table.insert(key.as_slice(), value.as_slice()).expect("insert failed");
                }
                write_txn.commit().expect("commit failed");
                RedbResponse::Ok
            }
            RedbCommand::Get { key } => {
                let read_txn = self.db.begin_read().expect("begin_read failed");
                let table = match read_txn.open_table(TABLE) {
                    Ok(t) => t,
                    Err(TableError::TableDoesNotExist(_)) => return RedbResponse::Value(None),
                    Err(e) => panic!("open_table failed: {e}"),
                };
                let value = table
                    .get(key.as_slice())
                    .expect("get failed")
                    .map(|v| v.value().to_vec());
                RedbResponse::Value(value)
            }
            RedbCommand::Delete { key } => {
                let write_txn = self.db.begin_write().expect("begin_write failed");
                let existed = {
                    let mut table = write_txn.open_table(TABLE).expect("open_table failed");
                    table.remove(key.as_slice()).expect("remove failed").is_some()
                };
                write_txn.commit().expect("commit failed");
                RedbResponse::Deleted(existed)
            }
        }
    }

    fn is_read_only(command: &Self::Command) -> bool {
        matches!(command, RedbCommand::Get { .. })
    }

    fn snapshot(&self) -> Vec<u8> {
        let read_txn = self.db.begin_read().expect("begin_read failed");
        let table = match read_txn.open_table(TABLE) {
            Ok(t) => t,
            Err(TableError::TableDoesNotExist(_)) => {
                let pairs: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
                return bincode::serialize(&pairs).expect("serialize failed");
            }
            Err(e) => panic!("open_table failed: {e}"),
        };
        let pairs: Vec<(Vec<u8>, Vec<u8>)> = table
            .iter()
            .expect("iter failed")
            .map(|entry| {
                let entry = entry.expect("entry failed");
                (entry.0.value().to_vec(), entry.1.value().to_vec())
            })
            .collect();
        bincode::serialize(&pairs).expect("serialize failed")
    }

    fn restore(&mut self, data: &[u8]) {
        let pairs: Vec<(Vec<u8>, Vec<u8>)> =
            bincode::deserialize(data).expect("deserialize failed");
        let write_txn = self.db.begin_write().expect("begin_write failed");
        {
            let mut table = write_txn.open_table(TABLE).expect("open_table failed");
            let keys: Vec<Vec<u8>> = table
                .iter()
                .expect("iter failed")
                .map(|entry| entry.expect("entry failed").0.value().to_vec())
                .collect();
            for key in keys {
                table.remove(key.as_slice()).expect("remove failed");
            }
            for (key, value) in pairs {
                table.insert(key.as_slice(), value.as_slice()).expect("insert failed");
            }
        }
        write_txn.commit().expect("commit failed");
    }
}
