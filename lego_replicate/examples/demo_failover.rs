//! Interactive demo: primary/backup replication with redb backend.
//!
//! Simulates a 3-replica cluster using the ReplicableService trait.
//! Demonstrates write durability across primary failover.
//!
//! Run with: cargo run -p lego_replicate --features backend_redb --example demo_failover

use lego_replicate::backends::redb::{RedbCommand, RedbResponse, RedbService};
use lego_replicate::ReplicableService;

struct Replica {
    id: usize,
    service: RedbService,
    alive: bool,
}

struct Cluster {
    replicas: Vec<Replica>,
    primary_idx: usize,
}

impl Cluster {
    fn new() -> Self {
        let replicas = (0..3)
            .map(|id| Replica { id, service: RedbService::default(), alive: true })
            .collect();
        println!("╔══════════════════════════════════════════════════════════╗");
        println!("║  Lego Replication Demo (redb backend)                   ║");
        println!("║  3 replicas: [0]=Primary, [1]=Backup, [2]=Backup       ║");
        println!("╚══════════════════════════════════════════════════════════╝");
        println!();
        Self { replicas, primary_idx: 0 }
    }

    fn status(&self) {
        for r in &self.replicas {
            let role = if r.id == self.primary_idx { "PRIMARY" } else { "backup " };
            let state = if r.alive { "alive" } else { "DEAD " };
            println!("  Replica {}: [{}] {}", r.id, state, role);
        }
        println!();
    }

    fn put(&mut self, key: &str, value: &str) {
        let cmd = RedbCommand::Put { key: key.as_bytes().to_vec(), value: value.as_bytes().to_vec() };
        print!("  PUT {}={} → ", key, value);
        if !self.replicas[self.primary_idx].alive {
            println!("ERROR: primary is dead!");
            return;
        }
        for r in self.replicas.iter_mut().filter(|r| r.alive) {
            r.service.apply(cmd.clone());
        }
        println!("committed (replicated to {} replicas)", self.replicas.iter().filter(|r| r.alive).count());
    }

    fn get(&mut self, key: &str) {
        let cmd = RedbCommand::Get { key: key.as_bytes().to_vec() };
        let reader_idx = if self.replicas[self.primary_idx].alive {
            self.primary_idx
        } else {
            self.replicas.iter().position(|r| r.alive).unwrap_or(0)
        };
        let resp = self.replicas[reader_idx].service.apply(cmd);
        match resp {
            RedbResponse::Value(Some(v)) => println!("  GET {} → \"{}\" (from replica {})", key, String::from_utf8_lossy(&v), reader_idx),
            RedbResponse::Value(None) => println!("  GET {} → (not found) (from replica {})", key, reader_idx),
            other => println!("  GET {} → unexpected: {:?}", key, other),
        }
    }

    fn crash_primary(&mut self) {
        println!("  💥 CRASH: Replica {} (primary) is dead!", self.primary_idx);
        self.replicas[self.primary_idx].alive = false;
        println!();
    }

    fn failover(&mut self) {
        let new_primary = self.replicas.iter()
            .position(|r| r.alive && r.id != self.primary_idx)
            .expect("no alive replicas!");
        println!("  🔄 FAILOVER: Electing replica {} as new primary...", new_primary);
        let snapshot = self.replicas[new_primary].service.snapshot();
        println!("     Snapshot taken ({} bytes)", snapshot.len());
        let mut fresh = RedbService::default();
        fresh.restore(&snapshot);
        self.replicas[new_primary].service = fresh;
        self.primary_idx = new_primary;
        println!("  ✅ Replica {} is now PRIMARY", new_primary);
        println!();
    }
}

fn main() {
    let mut cluster = Cluster::new();
    cluster.status();

    println!("── Step 1: Write x=1 and y=7 ──────────────────────────────");
    cluster.put("x", "1");
    cluster.put("y", "7");
    println!();

    println!("── Step 2: Read y ─────────────────────────────────────────");
    cluster.get("y");
    println!();

    println!("── Step 3: Crash the primary ──────────────────────────────");
    cluster.crash_primary();
    cluster.status();

    println!("── Step 4: Failover ───────────────────────────────────────");
    cluster.failover();
    cluster.status();

    println!("── Step 5: Read x and y (from new primary) ────────────────");
    cluster.get("x");
    cluster.get("y");
    println!();

    println!("── Done! Writes survived primary failure. ─────────────────");
}
