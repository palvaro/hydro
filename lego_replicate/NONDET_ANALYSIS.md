# Nondeterminism Analysis

## Goal

Once the primary (1) chooses a batch and (2) assigns slots, all work done by
backups should be **completely deterministic**. Let's evaluate each `nondet!`
against this property.

## Classification

### ESSENTIAL: Primary's two choices (the only "real" nondeterminism)

These are the two sources of nondeterminism that define the protocol's behavior.
Everything downstream should be deterministic given these choices.

1. **`batch boundaries`** (protocol.rs:136) — which client payloads land in each tick.
   This is the primary's batching decision.

2. **Slot assignment order** — within a batch, `enumerate()` assigns slots. The order
   depends on arrival order of payloads within the tick. This is implicit (no explicit
   nondet) but is the second source.

### CONTROL PATH: View changes (inherently nondeterministic)

These are nondeterministic because failure detection is wall-clock-based and
view changes are triggered by real-world events (node crashes). They don't
affect the data path's determinism property.

3. **`Paxos leader election`** (protocol.rs:85) — which proposer becomes Paxos leader.
4. **`Paxos commit ordering`** (protocol.rs:86) — Paxos internal commit order.
5. **`view proposals to proposers`** (protocol.rs:61) — membership snapshot for broadcast.
6. **`committed views to replicas`** (protocol.rs:91) — membership snapshot for broadcast.
7. **`FD scan order doesn't affect correctness`** (protocol.rs:289, 407) — FD event ordering.
8. **`FD proposals`** / **`FD proposals to replicas`** (protocol.rs:325, 501) — membership.
9. **`send view to router`** (protocol.rs:357) — sample timing.
10. **`periodic ping`** / **`ping`** / **`ping broadcast`** (protocol.rs:268, 270, 381, 383) — timer.
11. **`fd heartbeat`** / **`fd heartbeat tick`** (protocol.rs:376, 377) — timer.
12. **`scan heartbeat`** (protocol.rs:394) — timer.
13. **`batch`** (protocol.rs:404) — FD batch boundaries.

### DATA PATH: Should be deterministic but currently marked nondet

These are on the backup's data path. Given the primary's batch+slot decisions,
these SHOULD be deterministic. Each one is a candidate for elimination.

14. **`replicate broadcast`** (protocol.rs:171) — primary broadcasts to replicas.
    *Why nondet:* membership snapshot (which replicas are in the cluster).
    *Can eliminate?* YES — if we use the view's member list (which is deterministic
    after view change commits) instead of dynamic cluster membership. The primary
    knows exactly who to send to from the view.

15. **`replicate batch`** (protocol.rs:176) — backups batch incoming replicates.
    *Why nondet:* which replicates land in which tick on the backup.
    *Can eliminate?* NO — network delivery timing is inherently nondeterministic.
    But this doesn't affect correctness because backups validate view_num and the
    quorum logic is order-independent. The RESULT is deterministic (same set of
    acks) even if the batching differs.

16. **`stale view ok`** (protocol.rs:130) — snapshot of current_view into tick.
    *Why nondet:* the view singleton might be stale (from a previous tick).
    *Can eliminate?* NO — this is the fundamental tension between ticks and
    unbounded singletons. But staleness only causes dropped messages (wrong
    view_num rejected), never incorrect commits.

17. **`reconciled base`** (protocol.rs:140) — snapshot of reconciled_seq into tick.
    *Why nondet:* state transfer result might not be available yet.
    *Can eliminate?* Same as above — only matters during view change transition.

18. **`heartbeat`** / **`heartbeat tick`** (protocol.rs:126, 127) — keeps tick firing.
    *Why nondet:* timer source is wall-clock.
    *Can eliminate?* YES — if we guarantee the tick fires on every incoming message.
    The heartbeat is a workaround for Hydro's tick model (ticks only fire when
    input arrives). If we restructure to use the replicate messages themselves
    as the tick trigger, no heartbeat needed.

19. **`apply batch`** (protocol.rs:542) — batching committed commands for application.
    *Why nondet:* which committed commands land in which application tick.
    *Can eliminate?* YES — committed commands arrive in TotalOrder from
    `deliver_in_order`. The batch boundary doesn't matter because the scan
    processes them sequentially regardless. This nondet is vacuous.

20. **`assume_ordering TotalOrder`** (protocol.rs:543) — for the apply scan.
    *Why nondet:* asserting that `deliver_in_order` output is ordered.
    *Can eliminate?* YES — `deliver_in_order` already returns a TotalOrder stream.
    This is a type-system artifact, not real nondeterminism.

### PRIMITIVES: Internal nondeterminism

21. **`commands arrive as assigned`** (decide.rs:27) — persistence of pending commands.
    *Can eliminate?* NO — the sliced! block needs to persist state across ticks.
    But this is deterministic given the same inputs.

22. **`confirmations arrive after quorum`** (decide.rs:28) — same as above.
    *Can eliminate?* Same — deterministic given same inputs.

23. **`Commits arrive out of order`** (ordered_deliver.rs:32) — batch boundary.
    *Can eliminate?* NO — commits arrive from the network in arbitrary order.
    But the primitive's whole job is to reorder them, so the OUTPUT is deterministic.

24. **`view changes are infrequent`** (discover.rs:24) — view singleton persistence.
    *Can eliminate?* NO — same tick/singleton tension as #16.

25. **`state transfer request`** (discover.rs:48) — membership for broadcast.
    *Can eliminate?* Same as #14 — could use view member list.

26. **`batch`** (discover.rs:55) — state transfer request batching.
    *Can eliminate?* Vacuous — reduce takes any one request.

27. **`stale ok`** (discover.rs:61) — max_seq snapshot.
    *Can eliminate?* NO — same tick/singleton tension.

28. **`timer is wall-clock driven`** (liveness.rs:26) — silence detector timer.
    *Can eliminate?* NO — failure detection is inherently wall-clock.

29. **`assume_ordering TotalOrder`** (liveness.rs:33) — event/timer merge.
    *Can eliminate?* NO — interleaving of events and timer is nondeterministic.

## Summary: Backup determinism

Given the primary's batch + slot assignment:

| Category | Count | Deterministic for backups? |
|----------|-------|---------------------------|
| Primary's choices | 2 | N/A (these ARE the choices) |
| Control path (FD/Paxos) | 13 | N/A (failure path only) |
| Data path — eliminable | 4 | Could be made deterministic |
| Data path — inherent | 5 | Already deterministic in outcome |
| Primitives — internal | 9 | Deterministic given same inputs |

**The backup data path is already effectively deterministic in outcome.** The nondet
annotations on the backup path (#15, #16, #17, #23) don't affect the result — they
only affect timing (which tick processes what). The same set of commands gets acked,
the same quorum is reached, the same commits are delivered in the same order.

## How to eliminate the 4 eliminable nondets

1. **#14 (replicate broadcast membership):** Replace dynamic `source_cluster_members`
   with the view's member list. Broadcast only to `view.members`, not to whoever
   Hydro thinks is in the cluster. Requires a `demux` with explicit member IDs
   instead of `broadcast`.

2. **#18 (heartbeat):** Remove the heartbeat. Instead, ensure the tick fires on
   every incoming replicate message (which it already does — `replicates.batch(&tick)`
   triggers the tick). The heartbeat is only needed for the primary when idle.

3. **#19 (apply batch):** Remove — the stream is already TotalOrder from
   `deliver_in_order`. The batch is vacuous.

4. **#20 (assume_ordering):** Change `deliver_in_order` to return a stream that's
   already typed as TotalOrder (it is — the `all_ticks()` at the end preserves order).

After these eliminations: **0 nondet on the backup data path** (given primary's choices).
The only remaining nondets are the primary's 2 choices and the control path (FD/Paxos).
