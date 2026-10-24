# Fragile tick-level steps per `EventualConsistency` label (corpus of §4.9)

Read-only analysis of `58d0b3bcbf`. All paths are relative to the repo root. `rb.rs` means
`hydro_std/src/ec_inference_demos/reliable_broadcast.rs`, and `q.rs` means
`hydro_std/src/ec_inference_demos/quorum.rs`. The design doc's `quorum.rs:135/:144` are this
file's `use::batch` at q.rs:136 and its `fold` at q.rs:144.

Step categories: **T** set-preserving · **D** dedup against own history · **P** this-tick trigger
× persisted/snapshot side · **PP** both sides cumulative · **B2** two this-tick batches · **N**
negation other than D · **F** fold/count in a tick · **O** order-dependent op · **R** closure over
state by reference.

"Same faults" means the same crash points (positional `crash after msg`) and the same omissions.
"Existence fragile" means a fact's presence at a correct member can differ between two schedules
with the same faults.

## Summary

| target | fragile step on the label's path? | harness input pattern | existence fragile *in the harness*? | brute-forceable? |
|---|---|---|---|---|
| URB closed | no. The mint is F+R over cumulative `pending`, with a monotone threshold, plus D | one process input, 2 sends, no other external input | **no** | no (>240k schedules in the probe) |
| G-Set closed/live | P (`snapshot × pump`) | updates → quiesce → 3×(ticks → quiesce) | **no** for the element set (barriers). The *values* of echo records vary | no (sampled 8) |
| multi-Paxos | R+N (acceptor fence), P/PP+R (leader), F+GC (covering) | lead → quiesce → 2 commands | **no** (1 proposer, 1 ballot) | no |
| ABD (both) | P (`waiting∪batch × snapshot`), F (covering max over the first-quorum batch), N/GC | 1 write, no quiesce | **no** (the covering is always `None`) | no (probe >21k) |
| 2PC-CTP, 2 parts, quiesced | P (`decided × queried`), P+N (`uncertain∖decided × timeout`) | txn → quiesce → timeouts | **no** (the barrier puts the decision before every query) | sampled |
| 2PC-CTP, 3 parts, quiesced | same | same | **yes** (p2's query vs p0's `Decided` at p1) | sampled 64 (>104k under 1 crash) |
| 2PC-CTP, 2 parts, racing timers | same | txn ∥ timeouts, **no quiesce** | **yes** | sampled 64, 1 fault |
| hydro_test broadcast (plain/echo) | D only | 1 payload, no quiesce | **no** | plain yes (exhaustive, ≤2 faults); echo no (>160k) |
| `ask_once` | P (`known × queried`), N (`count()==0` emptiness) | value to m0 ∥ timeout to m2, **no quiesce** | **yes** | **yes**: 3 members, 2 inputs, ≤2 faults = 8 sets, 132 schedules |

Pattern: the fragile cases are exactly a **P whose trigger side is not persisted** (a query or
timeout consumed once) and whose state side is filled by a message that can race it. The race
must be exposed by an external input or timer sent without a quiesce before it, or by a
peer-generated trigger inside one quiesce window (CTP-3). Monotone-threshold F over cumulative
state, D, and PP are never fragile here.

---

## 1. Uniform reliable broadcast closed

Definition: `uniform_broadcast.rs:50-80`. Harness: `hydro_test/src/cluster/ec_demos_hydrofrac.rs:93-131,368`.
N=3, thresholds 1 and 2, `sample_schedules(8)`. Inputs: `send(1); send(2)`, with no quiesce and no
other external input.

| label | chain (input → label) | cat |
|---|---|---|
| `initial` | `source.broadcast_closed` rb.rs:130 | T |
| `echo` | `initial.merge_unordered(fwd)` rb.rs:146 → `.unique()` rb.rs:149 (top-level) → `broadcast_closed` rb.rs:154-156 | T, D (top-level `unique`) |
| `distinct_acks` | `echoes.entries().map` uniform_broadcast.rs:78 → `acks.unique()` q.rs:133 | T |
| `certified` | `use::batch(distinct_acks)` q.rs:136 → `fold` to Vec q.rs:144 (commutative, consumer order-insensitive) → `flat_map_unordered` closure with `pending.by_mut()`, `fired.by_mut()` q.rs:151-171: insert attestor into persisted `pending[fact]`, emit once at `len() >= threshold`, `fired` blocks re-emission | T, F (vec materialise), R, F-threshold **monotone over cumulative** `pending`, D (`fired`) |
| output | `quorum(..).map(into_fact)` uniform_broadcast.rs:79 | T |

**Judgment: not fragile.** The certificate exists iff ≥threshold distinct echoes arrive. That
count is cumulative across ticks (`pending` persists, and `fired` removes only the facts that have
already fired), so batching changes only when it fires. The echoes' existence comes from
choice-free top-level dataflow. Nothing sends after the mint, so there is no U2. Caveat for a
checker: the threshold and D are inside one R closure, so a structural classifier sees only "R" and
must treat it as opaque or recognise the idiom.

## 2. G-Set gossip (closed `crdt_gossip.rs:30-91`, live `:110-187`)

Harness: ec_demos_hydrofrac.rs:137-170. N=3, `sample_schedules(8)`. Inputs: `update.send(m, 10+m)`
for every m, then **quiesce**, then 3 rounds of {tick to all 3; **quiesce**}.

| label | chain | cat |
|---|---|---|
| `initial` | closed: `broadcast_closed(..).values()` :36-38. Live: `fan_out` = top-level `source.cross_product(members.ids)` fan_out.rs:229-234, with membership from `source_cluster_membership_stream` fan_out.rs:135-149 | T. Live: **PP** (both cumulative, top-level) |
| `all_elements` | `initial.merge_unordered(gossip.flatten_unordered())` :54 / :138 | T |
| `gset_state` | top-level `fold` (commutative + idempotent) :57 / :141. The scalar is covered by `all_elements` | F, top-level over cumulative (lattice) |
| `echo` | `use::snapshot(gset_state)` :70-73 (persisted side) and `use::batch(gossip_ticks)` :74-77 (trigger) → `snapshot.filter_if(pump.first().is_some())` :78 → `broadcast_closed`/`fan_out` :82-84 / :166-171 | **P** (snapshot persisted, pump trigger). `first()` is O on a TotalOrder batch but only `is_some` is used |
| `fan_out` (live) | as `initial`, over `sampled` | PP |

**Judgment: element existence not fragile in this harness. Echo-record values are.** Each pump
snapshots a set, and whether a round-r pump at m comes before or after the peers' round-r echoes
reach m changes the *content* of m's echo record (a `BTreeSet`). With these barriers every element
present at a correct member when a round starts is pumped in that round, and elements enter only
before the first quiesce. So under the admissible crashes, an element that a correct member has
reaches every correct member one round later whichever way the in-round race goes. Fragility could
only appear if an element first reached a member through an in-round race in the **last** round. I
found no ≤2-crash pattern that does that with 3 rounds.

## 3. Multi-Paxos (`hydro_std/src/ec_inference_demos/multi_paxos.rs`)

Harness: ec_demos_hydrofrac.rs:213-242. 1 proposer, 3 acceptors, 3 learners, `max_crashes(1)`,
`sample_schedules(8)`. Inputs: `lead.send(0,1)`, then **quiesce**, then `command 7; command 8`
with no quiesce. Labels: `prepares` :280-282, `accepts_fwd` :286, `accepts` :546-550,
`learned_at_proposers` :566-570, `initial` :580-583, `learned` :588, `echo` :589-592, `facts`
:601-616, `log` :618, plus the quorum's `distinct_acks`/`certified` (q.rs:133/136).

Chain to `learned`:

| step | site | cat |
|---|---|---|
| prepares broadcast | :280 | T |
| acceptor tick: `batch(prepares)`, `batch(accepts_fwd)` :303-310; `fold` to Vec :318-331; closure over `max_promised`/`accepted` (`by_mut`) :333-386. Accept iff `b >= mp0` (the promise at tick start), and the reply carries a clone of `accepted` | B2 (two this-tick batches interact through `mp0`), F, R, **N** (refusal by own higher promise; not GC) |
| demux → `accepted_acks` filter_map | :390-410 | T |
| `covering_quorum_slotted` (promises) q.rs:529-, batch q.rs:549: count + max over `pending ∪ batch`, `newly = reached.anti_join(fired)`, `pending = current.anti_join(reached)` | F (max over the batch that crosses the threshold: **not** monotone), D, N=GC |
| leader tick: `batch(covered)`, `batch(commands)`, `batch(learned_slots_fwd)` :437-452; `fold` (cmd fold is ordered: O on TotalOrder) :466-483; closure over `cur`/`learned`/`pending` :500-541. Commands wait in persisted `pending` until `cur` is set; `plan.start` = learned prefix *at that tick* | F, O, R, PP-like (both sides persisted), P (learned snapshot → `start`) |
| `accepts` broadcast | :546 | T |
| `quorum(majority, accepted_acks)` | :557 | as §1 (monotone) |
| `chosen` → `initial` broadcast → merge + `unique` → `echo` | :580-592 | T, D (top-level) |
| `facts` flat_map → `log` (`splice_epoch_log` fold, epoch_splice.rs:177) | :606, :618 | T, F top-level |

**Judgment: not fragile in this harness. Fragile in general.** One proposer with one ballot means
no prepare ever exceeds an accept, so the `mp0` refusal never fires. The quiesce after `lead`
establishes the covering (all acceptors empty, so it is deterministic) before any command arrives.
Commands are TotalOrder, so slots 7→0 and 8→1 come out under any batching. The accept sends come
out in the same sequence whether 7 and 8 share a tick or not, so there is no U2. With competing
proposers or ballots, the acceptor's tick-start fence (N/B2) and the covering's max over the
threshold batch decide which `(b, slot, v)` exists.

## 4. ABD (`hydro_std/src/ec_inference_demos/abd.rs`)

Harness: `ec_abd_hydrofrac.rs:25-56`. 3 replicas, 1 client, majority 2, `sample_schedules(8)`.
Input: `w_send.send(0,(1,10))` only, with no quiesce. Labels: `queries` :373-379, `phase2_fwd`
:385, `phase2` :471-477, `register` :395 / `abd_register_state` fold :147 (covered by its input).
The reliable variant adds `initial`/`new_messages`/`echo` (rb.rs:267/275/277).

| step | site | cat |
|---|---|---|
| queries: reads ∪ writes `map` → `broadcast_closed` | :373-379 | T |
| replica tick: `snapshot(register)` :208, `batch(phase2)` :217, `batch(queries)` :225 | — |
| acks: `(waiting ∪ new_writes) cross_singleton reg_now` → filter `applied >= ts`; unacked go back to `waiting` | :238-258 | **P** with the trigger side persisted (`waiting`), so cumulative. The `waiting` rewrite is N=GC |
| resps: `query_batch cross_singleton reg_now` | :260-266 | **P** (trigger = the query, consumed once; snapshot persisted). Every query gets a response, so existence is safe, but the **value** depends on the snapshot |
| client: `covering_quorum` q.rs:334-394 | :428 | F: `max` over `pending ∪ batch` at the threshold, **batch-dependent**; D (`fired`); N=GC (late responses dropped, q.rs:382) |
| `covered.join(operations)` (top-level) → `from_covering` picks `ts.round = max+1` | :441-444 | PP, T. The record **value** (ts) inherits the covering's max |
| phase2 `broadcast_closed`, or reliable broadcast (top-level `unique`) | :472-476 | T (D) |
| `register` top-level max fold | :147 | F lattice |

**Judgment: not fragile in this harness, fragile in general.** With a single write and nothing
written beforehand, every query response is `None`. The covering's max is then `None` in every
schedule, `ts = (1, client0)` is fixed, and the phase-2 record set is a function of the faults.
With concurrent writes, the snapshot behind `resps` and the covering max (F over the first-quorum
batch) decide which `(ts, value)` phase-2 record exists.

## 5. 2PC with cooperative termination (`hydro_test/src/cluster/two_pc_ctp.rs`)

Labels: `prepares` :83, `commits` :88-89, `peer_in` :92-95, `peer` :162-166, `committed`
:204 (`newly_decided`).

| step | site | cat |
|---|---|---|
| `prepare_batch`, `commit_batch`, `peer_batch`, `timeout_batch` | :106-109 | — |
| votes = prepare_batch | :118 | T |
| `newly_decided = commit_batch ∪ peer Decided → unique → filter_not_in(decided)`; `decided = decided ∪ newly` | :121-130 | T, **D** |
| `uncertain_now = (uncertain ∪ votes).filter_not_in(decided_now)` | :133-136 | N (negative side is own `decided`: "not yet decided", a retraction-GC of uncertainty, but existence-relevant downstream) |
| `timed_out = timeout_batch.count() > 0` | :142-143 | F on a this-tick batch (an "any" test) |
| `queries = uncertain_now.filter_if(timed_out)` | :147-149 | **P** (persisted `uncertain`, one-shot timeout trigger) + the N above |
| `answers = decided_now.join(queried)` | :150-157 | **P** (persisted `decided`; trigger = the peer's Query, **not persisted**) |
| coordinator: `all_votes` persisted → keyed `fold` count `>= n` → `filter_not_in(committed)` | :172-201 | F monotone over cumulative, D |

Harnesses:

| harness | site | inputs | members | judgment |
|---|---|---|---|---|
| `two_pc_with_cooperative_termination_survives_the_coordinators_crash` | ec_two_pc_hydrofrac.rs:17-57,141 | `Txn(1)` → **quiesce** → timeouts to all | coordinator + 2, 8 samples | **not fragile.** The barrier means p0 has decided (or nobody has) before any query is sent, and the `answers` join always finds `decided`. A participant crash leaves one correct participant |
| 3 participants, quiesced | :175 (256 samples), ec_bruteforce.rs:317-371 (64 samples, ≤2 faults) | same | coordinator + 3 | **fragile.** With the coordinator crashed after commit→p0, and p0 crashed after its `Decided`→p1 (answers are broadcast to all), p2's one-shot Query reaches p1 either before p1 decides (no answer, ever) or after (answer). The timers are sent together after the barrier, so the two peer messages race inside one quiesce window. The P at :154-157 decides existence |
| `ctp2_racing_timers_*` | :318-351 (cross_check 64 samples, 1 fault), :373-469 (seeded injection) | `Txn(1)` ∥ timeouts, **no quiesce before the timers** | coordinator + 2 | **fragile.** With the coordinator crashed after commit→p0, p1's single timeout fires either (a) before the prepare (empty `uncertain`, so no query ever) or (b) after the prepare. In (b) p1's query reaches p0 either before p0's commit (no answer) or after (answer). Both P sites (:147, :154) decide existence |

## 6. hydro_test reliable broadcast (`hydro_test/src/cluster/reliable_broadcast.rs`)

Labels: `initial` :53, `echoes` :57-60, the echo broadcast :89-92, `deliveries` :95.
Harnesses: ec_hydrofrac.rs:131-151 (one `Payload(42)`, no quiesce, 3 members; plain exhaustive,
echo 64 samples) and ec_bruteforce.rs:61-97 (plain, exhaustive, ≤2 faults).

| step | site | cat |
|---|---|---|
| `batch(initial)`, `batch(echoes)` | :68-69 | — |
| `fresh = (from_broadcaster ∪ from_members).unique().filter_not_in(seen)`; `seen ∪= fresh` | :75-77 | T, **D** |
| `echoed = fresh` (echo) or `filter(false)` (plain) | :81-85 | T |
| echo `broadcast_closed` | :91 | T |

**Judgment: not fragile.** D only changes which arrival is "first", which is lineage, not
existence. With one payload, each member sends at most one echo fan-out, in member-id order, so
crash points do not move (no U2). With several payloads in one batch, the order of the echo fan-out
could vary, which is U2 only.

## 7. `ask_once` (`hydro_test/src/cluster/ec_bruteforce.rs:135-199`)

Harness: :217-246. 3 members, `value_send.send(0,7)` ∥ `timeout_send.send(2,())`, **no
quiesce**. Every schedule of every ≤2-fault set: 8 sets, 132 schedules (:251). Label: `known`
(:195), which is `newly`.

| step | site | cat |
|---|---|---|
| `batch(values)`, `batch(net_in)`, `batch(timeouts)` | :154-156 | — |
| `newly = (own ∪ heard).unique().filter_not_in(known)`; `known ∪= newly` | :161-165 | T, **D** |
| `announce = own.flat_map_ordered → m1, m2` | :168-171 | T (O: the send order is fixed by `flat_map_ordered`) |
| `ask = timeout_batch.filter_if(known_now.count() == 0)` | :173-177 | F + **N** (emptiness of own state; non-monotone) with a one-shot timeout trigger |
| `answers = known_now.filter_if(net_batch has Query)` | :179-185 | **P** (persisted `known`, trigger = the query, **not persisted**) |
| `demux` over `fail_stop` | :190-192 | T |

**Judgment: fragile.** m0 crashes after announcing to m1 and before m2. m2's timer fires (and
`count()==0` holds, since m2 knows nothing), so it asks m1 once. If the query reaches m1 before the
announcement, the P at :183-185 yields nothing and m2's `known` stays empty. If it arrives after,
m2 learns 7. The faults are the same in both cases and only the schedule differs.

## Notes for the checker

- Every existence-fragile case is **P with a non-persisted trigger** (a one-shot query or timeout)
  whose state side is filled by a message that can race the trigger. The `N` emptiness tests
  (`count()==0`, `filter_not_in(decided)`) widen the window but are not fragile on their own.
- These steps are robust and could be tagged non-fragile: D (`x.filter_not_in(own outputs)`), PP,
  top-level lattice folds, and an in-tick threshold over **persisted cumulative** state (the
  quorum mint, 2PC's coordinator).
- F at a threshold over **pending ∪ batch with GC after firing** (`covering_quorum`) is
  value-fragile. Its *existence* is safe, but downstream records whose identity carries the
  aggregate (ABD's `ts`) inherit the fragility.
- The URB and multi-Paxos mints hide the threshold and D inside an **R** closure (`by_mut`). A
  structural classifier must treat R as opaque/fragile unless it recognises the closure.
- Barrier sensitivity: the same 2PC program goes from not fragile to fragile when the timers move
  from after `quiesce()` to before it (CTP-2), or when a third participant introduces peer-generated
  triggers inside one window (CTP-3).
