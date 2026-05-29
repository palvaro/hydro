# Lego Replicate: Comprehensive Report

## Summary

`lego_replicate` reproduces `hydro_transparent_replicate`'s functionality by composing
pre-built consensus primitives rather than hand-writing protocol logic.

## LOC Comparison

| Component | transparent_replicate | lego_replicate | Reduction |
|-----------|----------------------|----------------|-----------|
| Total src/ | 3521 | 1619 | 54% |
| Protocol core | 1735 | 558 | 68% |
| Primitives (reusable) | 0 (inline) | 251 | — |
| Backends | 540 | 345 | 36% |
| Messages | 169 | 80 | 53% |
| Config | 67 | 39 | 42% |

## Nondeterminism & Proof Obligations

| Metric | transparent_replicate | lego_replicate | Reduction |
|--------|----------------------|----------------|-----------|
| `nondet!` annotations | 63 | 36 | 43% |
| `manual_proof!` obligations | 12 | 5 | 58% |

The reduction comes from:
- Primitives encapsulate their own nondet (decide, ordered_deliver handle batching internally)
- Opaque payload design eliminates nondet around command-type-specific logic
- No notification broadcaster or per-replica failure detector (coordinator-driven FD is simpler)

## Architecture Comparison

### transparent_replicate (hand-written)
- `core_replication_module`: slot assignment + broadcast + quorum in one 288-line function
- `apply_commands`: 130-line scan with BTreeMap buffering
- `notification_broadcaster`: 70 lines
- `failure_detector`: 110 lines (per-replica, heartbeat-based)
- `view_manager`: 50 lines
- `state_transfer`: 100 lines
- `replicate_service`: 170-line top-level composition with forward_refs
- `coordinator_failure_detector`: 180 lines (Process-based scan)
- `cluster_coordinator_failure_detector`: 170 lines (Cluster-based)

### lego_replicate (composed from primitives)
- `compose_protocol`: 200 lines — wires primitives together
- `primitives::decide::join_confirmed`: 43 lines (from consensus-zoo)
- `primitives::ordered_deliver::deliver_in_order`: 66 lines (from consensus-zoo)
- `primitives::liveness::silence_detector`: 53 lines (from consensus-zoo)
- `primitives::discover::state_transfer`: 79 lines (from consensus-zoo)
- `process_router_failure_detector`: 160 lines (adapted from transparent_replicate)
- `hydro_std::quorum::collect_dynamic_quorum`: 40 lines (new primitive)

## What Worked

1. **Opaque payload design**: The protocol sequences `Vec<u8>` without knowing the command type.
   Only the application adapter deserializes. This eliminated all generic type parameter issues
   with stageleft's `q!()` closures.

2. **Primitive composition**: `decide` and `ordered_deliver` are genuinely reusable — they work
   for any `(slot, payload)` stream regardless of what the payload contains.

3. **Dynamic quorum**: `collect_dynamic_quorum` (added to hydro_std) cleanly handles the
   view-change case where quorum size changes at runtime.

4. **Paxos reuse**: hydro_test's `CorePaxos` works unchanged for view change sequencing.

5. **Replicated stream for hot standby**: Exposing `output.replicated` (fires on ALL replicas)
   enables backups to maintain state without additional protocol logic.

## What Did NOT Work

1. **consensus-zoo as a direct dependency**: The consensus-zoo crate is pinned to a different
   hydro rev with incompatible API (`merge_unordered`, `broadcast_closed` don't exist in
   current hydro). Had to adapt the primitive patterns locally instead of importing.

2. **consensus-zoo primitives are typed to KvCommand**: `slots::single_sequencer`,
   `accumulate::star`, `decide::join_confirmed` all hardcode `KvCommand`/`SlottedCmd`.
   Generic versions were added to consensus-zoo but couldn't be used due to (1).

3. **Process vs Cluster topology mismatch**: consensus-zoo primitives assume
   `Process<Coord>` + `Cluster<Acceptor>`. transparent_replicate uses a single
   `Cluster<TransparentReplica>` with dynamic primary. Had to write cluster-topology
   versions of the accumulate pattern.

4. **Silence detector is too simple for the FD**: The generic `silence_detector` primitive
   (fires on timeout) isn't sufficient for the full FD state machine (arm on command,
   ping on timeout, propose from alive set, re-propose on failure). The Process-based FD
   ended up being a 160-line scan — same complexity as transparent_replicate's.

5. **EC2 instance restart ≠ process restart**: Hydro's deploy framework does not auto-restart
   processes on restarted EC2 instances. The "stop primary, restart primary, stop backup"
   scenario fails because the restarted VM has no running process. This is a framework
   limitation shared with transparent_replicate.

## Primitives Used

| Primitive | Source | Role |
|-----------|--------|------|
| `decide::join_confirmed` | consensus-zoo pattern | Join confirmed slots with payloads |
| `ordered_deliver::deliver_in_order` | consensus-zoo pattern | Buffer + emit in slot order |
| `liveness::silence_detector` | consensus-zoo pattern | Timeout detection |
| `discover::state_transfer` | consensus-zoo pattern | Max-seq reconciliation on view change |
| `quorum::collect_dynamic_quorum` | new (hydro_std) | Runtime-variable quorum threshold |
| `CorePaxos` | hydro_test | View change sequencing |

## Tests

| Test | Status | What it verifies |
|------|--------|-----------------|
| `basic_protocol_test` | ✅ PASS | IR compiles (structural correctness) |
| `deploy_test` | ✅ PASS | PUT/GET end-to-end on localhost |
| `local_pipeline_test` | ✅ PASS | 100 PUTs + 5 GETs correct |
| `failover_e2e_test` | ✅ PASS | PUT, kill primary, GET returns value |
| `backend_properties` | ✅ PASS | 9 tests: CRUD + snapshot/restore for all backends |
| `ec2_autotest` Phase 1-3 | ✅ PASS | PUT, EC2 stop, failover recovery |
| `ec2_autotest` Phase 4-6 | ❌ FAIL | EC2 restart + kill another (framework limitation) |

## Conclusion

Building from primitives reduced code by 54% and proof obligations by 58%. The protocol
is cleaner and more modular. However, the failure detector remains complex (it's inherently
stateful) and the consensus-zoo API incompatibility forced local adaptation rather than
direct reuse. The EC2 restart limitation is shared with transparent_replicate and is a
framework issue, not a protocol issue.
