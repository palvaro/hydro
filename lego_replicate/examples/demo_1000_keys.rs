//! Demo: write 1000 keys, crash primary, read back and verify all data survived.
//!
//! Run with: cargo run -p lego_replicate --features backend_redb --example demo_1000_keys

use lego_replicate::backends::redb::{RedbCommand, RedbResponse, RedbService};
use lego_replicate::ReplicableService;
use std::collections::HashMap;

struct Cluster {
    replicas: Vec<Option<RedbService>>,
    primary_idx: usize,
}

impl Cluster {
    fn new(n: usize) -> Self {
        Self { replicas: (0..n).map(|_| Some(RedbService::default())).collect(), primary_idx: 0 }
    }

    fn put(&mut self, key: &[u8], value: &[u8]) {
        let cmd = RedbCommand::Put { key: key.to_vec(), value: value.to_vec() };
        for r in self.replicas.iter_mut().flatten() {
            r.apply(cmd.clone());
        }
    }

    fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        let cmd = RedbCommand::Get { key: key.to_vec() };
        match self.replicas[self.primary_idx].as_mut().unwrap().apply(cmd) {
            RedbResponse::Value(v) => v,
            _ => panic!("unexpected response"),
        }
    }

    fn crash_primary(&mut self) { self.replicas[self.primary_idx] = None; }

    fn failover(&mut self) {
        let new = self.replicas.iter().position(|r| r.is_some()).unwrap();
        let snapshot = self.replicas[new].as_ref().unwrap().snapshot();
        let mut fresh = RedbService::default();
        fresh.restore(&snapshot);
        self.replicas[new] = Some(fresh);
        self.primary_idx = new;
    }
}

fn main() {
    let mut rng_state: u64 = 0xdeadbeef;
    let mut expected: HashMap<String, String> = HashMap::new();
    let mut cluster = Cluster::new(3);

    println!("Writing 1000 keys...");
    for i in 0..1000 {
        rng_state = rng_state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let key = format!("key_{:04}", i);
        let value = format!("val_{}", rng_state & 0xFFFF);
        cluster.put(key.as_bytes(), value.as_bytes());
        expected.insert(key, value);
    }
    println!("  ✅ 1000 keys written and replicated");

    println!("\n💥 Crashing primary (replica 0)...");
    cluster.crash_primary();

    println!("🔄 Failover to replica 1...");
    cluster.failover();

    println!("Reading back all 1000 keys from new primary...");
    let mut mismatches = 0;
    for (key, expected_val) in &expected {
        let got = cluster.get(key.as_bytes());
        match got {
            Some(v) if String::from_utf8_lossy(&v) == expected_val.as_str() => {}
            Some(v) => {
                println!("  MISMATCH: {} expected={} got={}", key, expected_val, String::from_utf8_lossy(&v));
                mismatches += 1;
            }
            None => {
                println!("  MISSING: {} expected={}", key, expected_val);
                mismatches += 1;
            }
        }
    }

    if mismatches == 0 {
        println!("  ✅ All 1000 keys verified correct after failover");
    } else {
        println!("  ❌ {} mismatches found!", mismatches);
        std::process::exit(1);
    }
}
