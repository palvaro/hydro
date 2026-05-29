# Code Walkthrough: How Lego-Replicate Uses Patterns

## The Pipeline

```
client → router → replicas (slot assign → broadcast → quorum → decide → deliver) → applier → router → client
```

The protocol lives in `src/protocol.rs::compose_protocol`. Here's how each stage
maps to a pattern from consensus-zoo.

---

## Stage 1: View Manager (Paxos)

```rust
let paxos = CorePaxos { proposers, acceptors, paxos_config };
let committed_views = paxos.build(...);
```

**Pattern used:** `hydro_test::CorePaxos` — unchanged, called as a library.
This is the only part that uses full Paxos. It sequences view change proposals.
The data path never touches Paxos.

---

## Stage 2: Slot Assignment

```rust
let batch = client_payloads.batch(&tick, nondet!(/** batch boundaries */));
let primary_batch = batch.filter_if_some(is_primary);
let indexed = primary_batch.enumerate().cross_singleton(next_slot).map(...);
```

**Pattern used:** consensus-zoo's `slots::single_sequencer` pattern.
Batch → enumerate → add base offset → update next_slot via cycle.

**Why not call the primitive directly:** The consensus-zoo `single_sequencer`
takes a `KeyedStream<MemberId<Client>, Cmd>` (client provenance). Our design
has opaque `Vec<u8>` payloads arriving at the cluster without client tagging.
The pattern is the same (3 lines), just without the keyed stream wrapper.

---

## Stage 3: Broadcast + Quorum (Accumulate)

```rust
let replicates = indexed.cross_singleton(view_in_tick)
    .map(|((seq, payload), v)| Replicate { view_num, seq, payload, sender })
    .all_ticks()
    .broadcast(replicas, TCP.fail_stop().bincode(), nondet!(...))
    .values();

let acks = valid.map(|(r)| (r.sender, (r.seq, Ok(()))))
    .all_ticks()
    .demux(replicas, TCP.fail_stop().bincode())
    .values();

let confirmed = collect_dynamic_quorum(acks, quorum_size);
```

**Pattern used:** consensus-zoo's `accumulate::star` pattern.
Broadcast proposals → collect acks → quorum threshold → emit confirmed slots.

**What's different:** We use `collect_dynamic_quorum` (new primitive in hydro_std)
instead of `collect_quorum` because the view size changes at runtime. We also
validate `view_num` on receivers (reject stale-view messages) — this is the
view-guard logic that consensus-zoo's `accumulate::star_ballot` does with ballots.

**Why not call `star` directly:** It takes `Process<Coord>` (single coordinator
topology). We're in a `Cluster` with dynamic primary. Same pattern, different
location types.

---

## Stage 4: Decide

```rust
let committed = crate::primitives::decide::join_confirmed(slot_payloads, confirmed_slots);
```

**Pattern used:** consensus-zoo's `decide::join_confirmed` — **called directly**.
This is a genuine reuse. The primitive buffers `(slot, payload)` pairs and
confirmed slot numbers across ticks, emitting when both arrive for a slot.

43 lines. Zero modification from the consensus-zoo pattern.

---

## Stage 5: Ordered Deliver

```rust
let committed_in_order = crate::primitives::ordered_deliver::deliver_in_order(committed, replicas);
```

**Pattern used:** consensus-zoo's `ordered_deliver::deliver_in_order` — **called directly**.
Buffers out-of-order commits, sorts, emits the contiguous prefix, buffers the rest.

66 lines. Zero modification from the consensus-zoo pattern.

---

## Stage 6: State Transfer (on view change)

```rust
let reconciled_seq = crate::primitives::discover::state_transfer(replicas, current_view, max_seq_ref);
```

**Pattern used:** consensus-zoo's `discover::read_quorum` pattern.
Detect primary change → broadcast query → collect responses → reduce (take max).

**What's different:** We query for `max_replicated_seq` (a single usize) instead
of accepted ballot entries. The shape is identical: broadcast request, collect
responses, reduce. No ballot logic needed because our data path doesn't use ballots.

---

## Stage 7: Failure Detection

```rust
let fd_proposals = process_router_failure_detector(router, replicas, commands, responses, ...);
```

**Pattern used:** consensus-zoo's `liveness::silence_detector` is the core
timeout primitive. But the full FD is a 160-line scan that implements:
- Arm on command send
- Fire on response timeout
- Ping all replicas
- Propose view from alive set
- Re-propose until confirmed

**Why the primitive isn't enough:** `silence_detector` just fires `()` on timeout.
The FD needs stateful logic: track armed_at, pinging phase, ping replies, pending
proposals, warmed_up flag. This state machine doesn't decompose into smaller
primitives — it's inherently a single scan with multiple event types.

This is the one place where we couldn't use a primitive and had to write
application-specific logic. It's the same complexity as transparent-replicate's FD.

---

## Where We Use Patterns (directly callable)

| Stage | Primitive | LOC | Called directly? |
|-------|-----------|-----|-----------------|
| View manager | `CorePaxos` | (library) | ✅ Yes |
| Decide | `join_confirmed` | 43 | ✅ Yes |
| Ordered deliver | `deliver_in_order` | 66 | ✅ Yes |
| Dynamic quorum | `collect_dynamic_quorum` | 40 | ✅ Yes |

## Where We Use Patterns (same shape, adapted)

| Stage | Pattern from | Why adapted |
|-------|-------------|-------------|
| Slot assignment | `slots::single_sequencer` | No client provenance key, opaque payloads |
| Accumulate | `accumulate::star` | Cluster topology (not Process), dynamic quorum |
| State transfer | `discover::read_quorum` | Simpler (max_seq, no ballots) |

## Where We Can't Use Patterns

| Stage | Why |
|-------|-----|
| Failure detector | Stateful scan with 4 event types, application-specific logic |
| Applier (in ec2_demo2) | Application-specific command parsing + redb/fjall/rusqlite |

---

## The Glue

The glue in `compose_protocol` (200 lines) does:
1. Wire Paxos output → view fold (10 lines)
2. Wire state transfer forward_ref cycle (5 lines)
3. Slot assignment on primary (15 lines)
4. Broadcast + view_num validation + ack routing (20 lines)
5. Dynamic quorum wiring (5 lines)
6. Call decide + deliver primitives (5 lines)
7. Track max_replicated_seq for state transfer (10 lines)
8. Expose outputs (5 lines)

Total meaningful glue: ~75 lines. The rest of protocol.rs is the FD (160 lines)
and `replicate_service` wrapper (30 lines).

---

## Verdict

**4 primitives called directly** (decide, deliver, dynamic_quorum, Paxos).
**3 patterns adapted** (slots, accumulate, discover).
**1 thing we couldn't pattern-ize** (failure detector).

The protocol core is 75 lines of glue + 4 primitive calls. Compare to
transparent-replicate's 1735-line hand-written protocol.rs that inlines
all of this logic without separation.
