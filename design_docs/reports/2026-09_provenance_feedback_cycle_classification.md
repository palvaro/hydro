# Provenance-Based Classification of Feedback Cycles: Outcome

Status: research checkpoint, sim-time tool with ground-truth validation. Not a repository-wide
audit, not a runtime monitor, not a collapse verdict.

Companion to `2026-09_dfir_feedback_cycle_analysis_outcome.md`, which explains why aggregate
stage/window telemetry could not separate productive from operationally reactivated cycles. This
report describes the approach that can, what it produced on the ground-truth programs, and where
it stops.

## The idea in one paragraph

The earlier attempt was observational: infer *why* a message was sent from counters aggregated
over stages and wall-clock windows. Provenance cannot be recovered from aggregates, which is what
every one of that report's six blockers amounts to. The simulator, however, is eidetic in the
Arnold sense: every execution is a replayable byte string of nondeterministic decisions, and the
program is compiled from IR that we control. So we replay with heavyweight, passive
instrumentation instead: every item carries the set of source events it descends from, every
physical emission is logged with that lineage, and "why did this message happen" becomes a fact
read off the log rather than a guess. Driving the program through quiescence barriers makes each
stimulus the sole cause of the work between two quiet points, so no counterfactual reasoning is
needed.

## What was built

All in `hydro_lang`, behind `SimFlow::with_provenance()`; the untagged path is untouched.

- `sim::provenance` (runtime): `Tagged<T>` wraps every item with a `BTreeSet<Tag>` and a
  `coarse` flag. `Eq`/`Hash`/`Ord` delegate to the value, so `unique`, `sort`, `difference`,
  `multiset_delta` and every simulator hook behave exactly as on the untagged program, and no
  nondeterministic decision is added or removed: a recorded fuzz execution replays identically.
  Tags are `(kind, port, member, seq)`; `kind` is `Data` or `Operational`, declared at the port
  with `sim_input_operational()`. Network payloads carry tags in a side frame so byte counts are
  those of the real program. Emissions are logged in the dylib and drained by the host through an
  exported symbol (`take_emissions()`).
- `sim::provenance_ir` (compiler pass, run before `emit`): rewrites every IR node so items flow as
  `Tagged<T>` (or `(K, Tagged<V>)` for keyed collections, which the simulator's keyed hooks
  destructure). `map`/`filter` keep tags, `join`/`cross` union the two sides, `fold`/`reduce`
  accumulate, `anti_join`/`enumerate` get reshaping pre/post maps. Closures that read state
  through `by_ref`/`by_mut` inherit everything that state has accumulated and mark their outputs
  `coarse`; mutable `Vec`/`Optional` references are mirrored and written back after each call.
- `classify()`: a ~60-line pure function over the log. Per emission, on the channel (sender
  member, destination, recipient member):

  | data lineage | operational tag | label |
  |---|---|---|
  | none | yes | `FixedOperational` |
  | not dominated by any single earlier emission on this channel | – | `Productive` |
  | dominated | yes | `Reactivated` |
  | dominated | no | `Redundant` |

  Runs of identical *coarse* lineage are one causal unit. `attribute()` sums messages/bytes/data
  tags per (emission point, operational tag).
- `EmissionRecord::payload_hash`: content identity, explicitly not lineage, for counting distinct
  payloads where lineage is too coarse (see limits).
- Receipts: every network edge also logs an `EmissionPointKind::Receive` at the deserialize side,
  on the same channel as the send. Network sends are judged against what the recipient has
  *received*, and only receipts advance network history. Lossless: no change. Under network
  loss: the retry of a message that never arrived is `Productive`.
- `timeout_retry_lossy_with_timers` with a `LossPolicy`: deterministic program-level faults
  (the simulator's network is reliable) — drop the first arrival of odd-id requests at the
  service, and/or black-hole odd-id responses at the client.

Tests: `hydro_test/src/cluster/provenance_ground_truth.rs` (9 tests). `timeout_retry` gained a
`timeout_retry_with_timers` variant that takes its two timers as inputs, the convention `raft.rs`
already used; the deployment-facing API is unchanged.

## Results

Every number below is asserted by a passing test across several random simulator schedules.

**Heartbeat** (3 members, timer as operational input): 9 messages per tick, all
`FixedOperational`, zero data lineage; gain per stimulus = cluster size.

**Timeout/retry** (ground truth), N ∈ {1, 4, 9}, barrier-separated:

| phase | on `requests` | on `responses` |
|---|---|---|
| A: N requests, quiesce | N `Productive` | – |
| B: one retry tick | N `Reactivated`, one operational tag each, N data tags in total | – |
| C: one service pulse | – | 1 `Productive` |
| D: second retry tick | N−1 messages | – |

Raw classifier output for N=4 (`D0.k` = k-th request, `T1.k` = k-th retry tick, `T2.0` = service
pulse):

```
A  Productive    requests   {D0.0,D0.1,D0.2}               coarse 18B   (×3, one tick)
   Productive    requests   {D0.0,D0.1,D0.2,D0.3}          coarse 18B   (second tick)
B  Reactivated   requests   {D0.0..D0.3,T1.0}              coarse 18B   ×4
C  Productive    responses  {D0.0..D0.3,T1.0,T2.0}         coarse 18B
D  Reactivated   requests   {D0.0..D0.3,T1.0,T1.1,T2.0}    coarse 18B   ×3
```

Loop gain, N=4, k retry ticks before the service drains: physical responses N(k+1) = 8, 12, 16;
distinct payloads N = 4; waste N·k = 4, 8, 12. The program's own `unique()` output agrees (4
completions). Reactivated input at the service costs it capacity that delivers nothing new.

**Retry under a response black hole** (ground truth: futile, unbounded): after the even
requests complete, each of 3 retry ticks re-sends exactly the 2 black-holed requests, all
`Reactivated`, and costs the service 2 pulses whose responses are discarded. Per-tick waste
(2, 2, 2): constant and never drains. Nothing further ever completes.

**Retry under request loss** (ground truth: necessary retry) — a negative result. The service
discards the first arrival of odd-id requests; all four retries are labelled `Reactivated`
although two were the first copy the service ever processed. The discard happens inside the
service *after* the network delivered, so receipt-history advances and the opaque `by_mut` queue
hides it. All four complete only after the retries. See lesson 6.

**Transitive closure** (ground truth), edges admitted one per input in reverse topological order,
steps as operational input: 6 facts out, all `Productive`, exact lineage; derived facts carry
combinations of edge tags plus a step tag; no emission once the frontier is exhausted;
re-admitting the graph emits nothing.

**G-Set gossip** (no ground truth; observations): N updates at one member broadcast once →
N·3 `Productive`. First pump: 3 messages, `Productive` (see limits). Second pump: 3
`Reactivated`. Messages per pump constant across N ∈ {2, 8}; bytes grow. Loop gain: a member
emits 3 messages after one pump whether it received 0, 1 or 3 pumps beforehand.

**Raft** (`raft_server`), N ∈ {1, 5}: election tick → 2 RequestVote, `FixedOperational` (no
data yet). N requests → no traffic. Heartbeat → 2 AppendEntries carrying the suffix,
`Productive`, 160 B → 384 B. Second heartbeat → 2 AppendEntries, 104 B for both N; labelled
`Reactivated` (coarse), payload fixed-size.

## What was learned

1. **Novelty is dominance, not "an unseen tag".** A transitive-closure fact (a,c) from edges ab
   and bc carries only tags already emitted, yet no single earlier emission carried both. An
   emission is non-novel only if its data lineage is a subset of one earlier emission on the
   same channel. This is what makes derivation count as progress and re-emission not.
2. **History is per sender→recipient channel, not per operator.** Gossip's echo edge has no
   history of its own, but the recipients already received that state on the initial-broadcast
   edge. "Already told" is a property of the pair, not of the edge.
3. **Lineage cannot distinguish a join from a fold.** Gossip's first pump {D0..Dn} is a new
   combination of separately-received elements — exactly the shape of a TC fact. What separates
   gossip from TC is recurrence: the second pump re-emits identical lineage with no new data. The
   cycle-level verdict must rest on recurrence after data stops, which is also what the original
   definition says ("repeatedly turns retained state back into work").
4. **Coarse lineage is the method's boundary, and it is where `sliced!` programs live.** The
   retry service's queue is a `VecDeque` behind `by_mut`; every response inherits the whole
   queue's lineage, so after the first response every one — originals included — is dominated and
   labelled `Reactivated`. Raft's entire server state is one such struct. Downstream of opaque
   state, labels degrade to "first productive, rest reactivated", and the loop gain had to be
   measured by content identity plus the program's own `unique()` gate. This is precisely the
   place where a static analysis would also go blind.
5. **Closed loop vs open loop is measurable.** With retry, reactivated input at a node makes that
   node waste capacity (responses N(k+1) for N distinct). With gossip, a member's output is
   independent of how many pumps it received. Whether the second pattern is safe is not
   established — gossip is unlabelled — but the probe separates the two mechanisms.
6. **Coarseness can produce a wrong answer with no fallback.** Under request loss inside the
   service, the necessary retries are labelled `Reactivated`, and neither bytes nor payload
   identity corrects it (a retry is byte-identical to its original). This is the first case where
   opaque state costs the answer, not just the route to it, and it motivates either
   network-level loss in the simulator (drops before the deserialize wrapper, which receipts
   would see) or finer lineage through the common queue/outstanding-set shapes behind `by_mut`.
7. **Program-level findings from driving the ground truth through barriers.** The TC program
   drops its frontier if admissions straddle ticks without a step, and joins only the current
   frontier on the left against base edges, so incremental admission discovers the closure only
   in reverse topological order.

## Toward a cycle-level classifier

Per-emission labels are the primitive, not the verdict. Four classes fall out of two experiments
that need no new instrumentation, run through quiescence barriers:

- **Fixed**: timer-funded emissions carry no data lineage (heartbeat, RequestVote). Cost bounded
  by membership.
- **Draining**: no recurrence after data stops (TC; Raft replication with acking followers).
- **Open-loop reactivation**: recurs; a recipient's output does not depend on how much
  reactivated input it received (gossip *as measured*). Cost ∝ retained state.
- **Closed-loop reactivation**: recurs; per-firing gain grows with backlog *and* the recipient's
  wasted output grows with the number of firings (retry). Collapse candidate; actual collapse is
  a rates-and-capacity question this tool does not answer.

Ground truth now covers retry (lossless), retry under a black hole (sustained closed-loop
waste, never drains), retry under request loss (the method's negative result), and TC. Gossip
and Raft remain observations. The classes should be treated as a hypothesis with a few anchors,
not a result.

## Limits and non-goals

- Sim-time only. Wall-clock `source_interval` cannot be simulated; timers must be `sim_input`s.
- Coarse lineage through `by_ref`/`by_mut` closures (above). Mutable `Vec` mirrors do not
  reflect in-place edits of pre-existing items.
- Unsupported nodes panic with `provenance: unsupported ...`: `resolve_futures*`,
  `flat_map_stream_blocking`, `scan_async_blocking`, `reduce_keyed_watermark`, versioned
  networks, raw-bytes channels and external ports.
- The simulator's network is reliable; "lossy" channels only relax the liveness check. Loss
  must be modelled in the program, where it lands inside opaque state (lesson 6). Network-level
  loss in the simulator is the natural next step.
- Interleaved schedules (a timer firing while a response is in flight) are not driven here;
  barriers serialize phases. Lineage stays exact under interleaving; only the experimental
  protocol changes.
- Not a runtime monitor. The cheap production proxy suggested by this work is per-channel
  content-hash repetition; the simulator is where such a proxy could be calibrated.

## Reproducing

```
SDKROOT=/Library/Developer/CommandLineTools/SDKs/MacOSX26.5.sdk \
  cargo test -p hydro_test --lib provenance_ground_truth
```

The classified logs quoted above are written to `hydro_test/target/provenance_dump.txt` (the fuzz
harness captures stderr). On this machine the default SDK (27.0) ships `.tbd` files that the
toolchain's `rust-lld` rejects; the 26.5 SDK works.
