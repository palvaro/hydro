//! Concrete applier for use inside Hydro dataflow `q!()` closures.
//!
//! Command format: `PUT:<key>:<value>:<nonce>` or `GET:<key>:<nonce>`
//! Response format: `OK nonce=<n> seq=<s> PUT <k>=<v>` or `VALUE nonce=<n> GET <k>=<v>`

/// State for the redb applier inside a Hydro dataflow scan.
#[derive(Clone)]
pub struct RedbApplierState {
    #[cfg(feature = "backend_redb")]
    db: std::sync::Arc<redb::Database>,
    #[cfg(feature = "backend_redb")]
    _tmp: std::sync::Arc<tempfile::NamedTempFile>,
    #[cfg(not(feature = "backend_redb"))]
    _phantom: (),
}

impl RedbApplierState {
    pub fn new() -> Self {
        #[cfg(feature = "backend_redb")]
        {
            let tmp = tempfile::NamedTempFile::new().expect("failed to create tempfile");
            let db = redb::Database::create(tmp.path()).expect("failed to create redb");
            Self { db: std::sync::Arc::new(db), _tmp: std::sync::Arc::new(tmp) }
        }
        #[cfg(not(feature = "backend_redb"))]
        { panic!("backend_redb feature required") }
    }

    pub fn apply_command(&self, seq: usize, cmd: &str) -> String {
        #[cfg(feature = "backend_redb")]
        {
            use redb::ReadableTable;
            const TABLE: redb::TableDefinition<'static, &[u8], &[u8]> = redb::TableDefinition::new("kv");

            let parts: Vec<&str> = cmd.splitn(4, ':').collect();
            match parts[0] {
                "PUT" if parts.len() >= 3 => {
                    let (key, value) = (parts[1], parts[2]);
                    let nonce: u64 = parts.get(3).and_then(|s| s.parse().ok()).unwrap_or(0);
                    let txn = self.db.begin_write().expect("begin_write");
                    { let mut t = txn.open_table(TABLE).expect("open_table"); t.insert(key.as_bytes(), value.as_bytes()).expect("insert"); }
                    txn.commit().expect("commit");
                    format!("OK nonce={} seq={} PUT {}={}", nonce, seq, key, value)
                }
                "GET" if parts.len() >= 2 => {
                    let key = parts[1];
                    let nonce: u64 = parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(0);
                    let txn = self.db.begin_read().expect("begin_read");
                    let value = match txn.open_table(TABLE) {
                        Ok(t) => t.get(key.as_bytes()).expect("get")
                            .map(|v| String::from_utf8_lossy(v.value()).to_string())
                            .unwrap_or("(nil)".to_string()),
                        Err(_) => "(nil)".to_string(),
                    };
                    format!("VALUE nonce={} GET {}={}", nonce, key, value)
                }
                _ => format!("ERROR seq={} unknown command", seq),
            }
        }
        #[cfg(not(feature = "backend_redb"))]
        { let _ = (seq, cmd); panic!("backend_redb feature required") }
    }
}

impl Default for RedbApplierState {
    fn default() -> Self { Self::new() }
}

/// Fjall-backed applier state. Same command format as RedbApplierState.
#[derive(Clone)]
pub struct FjallApplierState {
    #[cfg(feature = "backend_fjall")]
    partition: fjall::PartitionHandle,
    #[cfg(feature = "backend_fjall")]
    _keyspace: std::sync::Arc<fjall::Keyspace>,
    #[cfg(feature = "backend_fjall")]
    _tempdir: std::sync::Arc<tempfile::TempDir>,
    #[cfg(not(feature = "backend_fjall"))]
    _phantom: (),
}

impl FjallApplierState {
    pub fn new() -> Self {
        #[cfg(feature = "backend_fjall")]
        {
            let tempdir = tempfile::tempdir().expect("tempdir");
            let keyspace = fjall::Config::new(tempdir.path()).open().expect("fjall open");
            let partition = keyspace.open_partition("kv", fjall::PartitionCreateOptions::default()).expect("partition");
            Self { partition, _keyspace: std::sync::Arc::new(keyspace), _tempdir: std::sync::Arc::new(tempdir) }
        }
        #[cfg(not(feature = "backend_fjall"))]
        { panic!("backend_fjall feature required") }
    }

    pub fn apply_command(&self, seq: usize, cmd: &str) -> String {
        #[cfg(feature = "backend_fjall")]
        {
            let parts: Vec<&str> = cmd.splitn(4, ':').collect();
            match parts[0] {
                "PUT" if parts.len() >= 3 => {
                    let nonce: u64 = parts.get(3).and_then(|s| s.parse().ok()).unwrap_or(0);
                    self.partition.insert(parts[1].as_bytes(), parts[2].as_bytes()).expect("insert");
                    format!("OK nonce={} seq={} PUT {}={}", nonce, seq, parts[1], parts[2])
                }
                "GET" if parts.len() >= 2 => {
                    let nonce: u64 = parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(0);
                    let value = self.partition.get(parts[1].as_bytes()).expect("get")
                        .map(|v| String::from_utf8_lossy(&v).to_string())
                        .unwrap_or("(nil)".to_string());
                    format!("VALUE nonce={} GET {}={}", nonce, parts[1], value)
                }
                _ => format!("ERROR seq={} unknown", seq),
            }
        }
        #[cfg(not(feature = "backend_fjall"))]
        { let _ = (seq, cmd); panic!("backend_fjall feature required") }
    }
}

impl Default for FjallApplierState {
    fn default() -> Self { Self::new() }
}

/// Rusqlite-backed applier state. Same command format as RedbApplierState.
#[derive(Clone)]
pub struct RusqliteApplierState {
    #[cfg(feature = "backend_rusqlite")]
    conn: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    #[cfg(not(feature = "backend_rusqlite"))]
    _phantom: (),
}

impl RusqliteApplierState {
    pub fn new() -> Self {
        #[cfg(feature = "backend_rusqlite")]
        {
            let conn = rusqlite::Connection::open_in_memory().expect("sqlite open");
            conn.execute("CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT)", []).expect("create table");
            Self { conn: std::sync::Arc::new(std::sync::Mutex::new(conn)) }
        }
        #[cfg(not(feature = "backend_rusqlite"))]
        { panic!("backend_rusqlite feature required") }
    }

    pub fn apply_command(&self, seq: usize, cmd: &str) -> String {
        #[cfg(feature = "backend_rusqlite")]
        {
            let parts: Vec<&str> = cmd.splitn(4, ':').collect();
            let conn = self.conn.lock().unwrap();
            match parts[0] {
                "PUT" if parts.len() >= 3 => {
                    let nonce: u64 = parts.get(3).and_then(|s| s.parse().ok()).unwrap_or(0);
                    conn.execute("INSERT OR REPLACE INTO kv (key, value) VALUES (?1, ?2)", [parts[1], parts[2]]).expect("insert");
                    format!("OK nonce={} seq={} PUT {}={}", nonce, seq, parts[1], parts[2])
                }
                "GET" if parts.len() >= 2 => {
                    let nonce: u64 = parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(0);
                    let value: String = conn.query_row("SELECT value FROM kv WHERE key = ?1", [parts[1]], |row| row.get(0))
                        .unwrap_or("(nil)".to_string());
                    format!("VALUE nonce={} GET {}={}", nonce, parts[1], value)
                }
                _ => format!("ERROR seq={} unknown", seq),
            }
        }
        #[cfg(not(feature = "backend_rusqlite"))]
        { let _ = (seq, cmd); panic!("backend_rusqlite feature required") }
    }
}

impl Default for RusqliteApplierState {
    fn default() -> Self { Self::new() }
}
