//! Demo: replicated SQL database with failover.
//!
//! Run with: cargo run -p lego_replicate --features backend_rusqlite --example demo_rusqlite

use lego_replicate::backends::rusqlite::{RusqliteService, SqlCommand, SqlResponse};
use lego_replicate::ReplicableService;

struct Cluster {
    replicas: Vec<Option<RusqliteService>>,
    primary_idx: usize,
}

impl Cluster {
    fn new(n: usize) -> Self {
        Self { replicas: (0..n).map(|_| Some(RusqliteService::default())).collect(), primary_idx: 0 }
    }

    fn execute(&mut self, sql: &str) -> SqlResponse {
        let cmd = SqlCommand(sql.to_string());
        let mut resp = SqlResponse::Error("no replicas".into());
        for r in self.replicas.iter_mut().flatten() {
            resp = r.apply(cmd.clone());
        }
        resp
    }

    fn query(&mut self, sql: &str) -> SqlResponse {
        let cmd = SqlCommand(sql.to_string());
        self.replicas[self.primary_idx].as_mut().unwrap().apply(cmd)
    }

    fn crash_primary(&mut self) { self.replicas[self.primary_idx] = None; }

    fn failover(&mut self) {
        let new = self.replicas.iter().position(|r| r.is_some()).unwrap();
        let snapshot = self.replicas[new].as_ref().unwrap().snapshot();
        let mut fresh = RusqliteService::default();
        fresh.restore(&snapshot);
        self.replicas[new] = Some(fresh);
        self.primary_idx = new;
    }
}

fn main() {
    let mut cluster = Cluster::new(3);

    println!("── Create table and insert data ─────────────────────────────");
    cluster.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)");
    cluster.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')");
    cluster.execute("INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')");
    cluster.execute("INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com')");
    println!("  ✅ Table created, 3 rows inserted");

    println!("\n── Query before crash ──────────────────────────────────────");
    match cluster.query("SELECT * FROM users ORDER BY id") {
        SqlResponse::Query { columns, rows } => {
            println!("  Columns: {:?}", columns);
            for row in &rows { println!("  {:?}", row); }
        }
        other => println!("  Error: {:?}", other),
    }

    println!("\n💥 Crashing primary...");
    cluster.crash_primary();

    println!("🔄 Failover...");
    cluster.failover();

    println!("\n── Query after failover ────────────────────────────────────");
    match cluster.query("SELECT name, email FROM users WHERE id = 2") {
        SqlResponse::Query { rows, .. } => {
            println!("  Bob's record: {:?}", rows[0]);
            assert_eq!(rows[0][0], Some("Bob".to_string()));
            assert_eq!(rows[0][1], Some("bob@example.com".to_string()));
            println!("  ✅ Data survived failover");
        }
        other => println!("  ❌ Error: {:?}", other),
    }
}
