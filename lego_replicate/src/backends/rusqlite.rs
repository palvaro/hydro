//! rusqlite backend implementation of `ReplicableService`.

use crate::ReplicableService;
use serde::{Deserialize, Serialize};

use rusqlite::Connection;
use std::io::Write;

pub struct RusqliteService {
    conn: Connection,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SqlCommand(pub String);

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum SqlResponse {
    Execute { rows_affected: usize },
    Query { columns: Vec<String>, rows: Vec<Vec<Option<String>>> },
    Error(String),
}

impl Default for RusqliteService {
    fn default() -> Self {
        let conn = Connection::open_in_memory().expect("failed to open in-memory SQLite database");
        Self { conn }
    }
}

fn is_sql_read_only(sql: &str) -> bool {
    let upper = sql.trim().to_uppercase();
    upper.starts_with("SELECT") || upper.starts_with("PRAGMA") || upper.starts_with("EXPLAIN")
}

impl ReplicableService for RusqliteService {
    type Command = SqlCommand;
    type Response = SqlResponse;

    fn apply(&mut self, command: Self::Command) -> Self::Response {
        let sql = &command.0;
        if is_sql_read_only(sql) {
            match self.execute_query(sql) {
                Ok(resp) => resp,
                Err(e) => SqlResponse::Error(e.to_string()),
            }
        } else {
            match self.execute_mutate(sql) {
                Ok(resp) => resp,
                Err(e) => SqlResponse::Error(e.to_string()),
            }
        }
    }

    fn is_read_only(command: &Self::Command) -> bool {
        is_sql_read_only(&command.0)
    }

    fn snapshot(&self) -> Vec<u8> {
        let tmp = tempfile::NamedTempFile::new().expect("failed to create temp file for SQLite snapshot");
        let tmp_path = tmp.path().to_path_buf();
        let mut dst = Connection::open(&tmp_path).expect("failed to open destination for SQLite backup");
        {
            let backup = rusqlite::backup::Backup::new(&self.conn, &mut dst)
                .expect("failed to create backup for snapshot");
            backup
                .run_to_completion(100, std::time::Duration::from_millis(0), None)
                .expect("backup snapshot failed");
        }
        drop(dst);
        std::fs::read(&tmp_path).expect("failed to read SQLite snapshot file")
    }

    fn restore(&mut self, data: &[u8]) {
        let mut tmp = tempfile::NamedTempFile::new().expect("failed to create temp file for SQLite restore");
        tmp.write_all(data).expect("failed to write SQLite snapshot to temp file");
        tmp.flush().expect("failed to flush temp file");
        let tmp_path = tmp.path().to_path_buf();
        let src = Connection::open(&tmp_path).expect("failed to open SQLite snapshot for restore");
        let mut new_conn = Connection::open_in_memory().expect("failed to open new in-memory SQLite database");
        {
            let backup = rusqlite::backup::Backup::new(&src, &mut new_conn)
                .expect("failed to create backup for restore");
            backup
                .run_to_completion(100, std::time::Duration::from_millis(0), None)
                .expect("backup restore failed");
        }
        self.conn = new_conn;
    }
}

impl RusqliteService {
    fn execute_query(&self, sql: &str) -> Result<SqlResponse, rusqlite::Error> {
        let mut stmt = self.conn.prepare(sql)?;
        let column_count = stmt.column_count();
        let columns: Vec<String> = (0..column_count)
            .map(|i| stmt.column_name(i).unwrap_or("?").to_string())
            .collect();
        let rows: Vec<Vec<Option<String>>> = stmt
            .query_map([], |row| {
                let mut values = Vec::with_capacity(column_count);
                for i in 0..column_count {
                    let value: Option<String> = row.get::<_, Option<String>>(i).unwrap_or(None);
                    values.push(value);
                }
                Ok(values)
            })?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(SqlResponse::Query { columns, rows })
    }

    fn execute_mutate(&mut self, sql: &str) -> Result<SqlResponse, rusqlite::Error> {
        let rows_affected = self.conn.execute(sql, [])?;
        Ok(SqlResponse::Execute { rows_affected })
    }
}
