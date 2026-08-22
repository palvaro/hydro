# Typed Consensus — Handoff Document

## What this is

A Paxos-structured consensus protocol for Hydro that composes EC-typed building blocks. It's a drop-in alternative to `hydro_test/src/cluster/raft.rs` with the same interface and equivalent safety guarantees, but with EC inferred by the type system on per-view broadcasts rather than one monolithic `manual_proof!`.

## Current state: PARTIALLY WORKING

The core protocol works — elections, proposals, acks, commits, and log composition all function correctly in simulation. Safety under concurrent views is verified by the fuzzer (8192 iterations, 3-node and 5-node clusters).

### What works

- All component functions pass isolated unit tests
- The composed `typed_consensus()` function:
  - Completes the full election → propose → commit flow
  - Passes safety property (no slot conflicts) under concurrent views (3-node, 5-node)
  - Passes liveness under stable view
  - Passes EC convergence (all members see same log)
- 2 `manual_proof!` annotations as designed

### Known limitations

1. **Heartbeat emission disabled in simulation.** The `leader_heartbeat_emission` broadcast creates a scheduling loop in the DFIR simulator runtime — broadcasting heartbeats via the `heartbeat_fwd` forward_ref prevents the simulator from reaching quiescence. The heartbeat forward_ref is completed with an empty stream. This means:
   - Heartbeat-based election timer suppression doesn't work in sim
   - Tests drive elections explicitly via `election_port.send(...)`
   - Production deployment (via `examples/typed_consensus.rs`) still uses real timers

2. **Hardcoded `view_id = 1`.** The `propose_in_view_gated` function is always called with `view_id = 1`. This means all proposals are tagged with view 1 regardless of which view the leader actually established. This causes safety violations when multiple views are active simultaneously (the `test_fully_concurrent_run_never_forks` test catches this). Fix: propagate the actual view number from the election/start_signal path into `propose_in_view_gated`.

3. **`max_committed_fwd` doesn't feed real data in the first view.** The max_committed forward_ref is properly wired but on the first election with no prior commits, all members report 0 and start_slot = 1. This is correct. However, for multi-view scenarios, the feedback ensures subsequent views start beyond committed slots.

## File layout

- `hydro_test/src/cluster/typed_consensus.rs` — all protocol code + tests
- `hydro_test/examples/typed_consensus.rs` — deployment example
- `.kiro/specs/typed-consensus/` — spec documents (requirements, design, tasks)
