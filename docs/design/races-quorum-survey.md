# Quorum sites in URB, multi-Paxos and ABD, and the trusted-combinator question

Read-only analysis of `hydrofrac-races` @ `9f9f7ff2e4`. `q.rs` is
`hydro_std/src/ec_inference_demos/quorum.rs`, `mp.rs` is `multi_paxos.rs`, `ub.rs` is
`uniform_broadcast.rs`, and `rb.rs` is `reliable_broadcast.rs`, all in the same directory.
`abd.rs` is the ABD file there too.

Evidence marks: **D** = demonstrated (I ran it, and the output is quoted). **A** = argued from the
code, with file:line. **G** = guessed.

## Terms

- **Response set of key k.** Every response for k that reaches the observer during the whole run.
- **Existence of an output.** Whether the operator emits a record for k at all.
- **Value of an output.** The record's content apart from k: an aggregate, a ballot, a timestamp.
- **Set-determined.** The output records over a whole run (existence *and* value) are a function
  of the response sets, however they were batched or ordered. This is the property Lemma R needs
  (`hydrofrac-races.md` §5): the record must be equal by content in every schedule.
- **Crossing tick.** The tick in which k's count first reaches the threshold.
- **First-arrived subset.** The responses for k that had arrived by the crossing tick: the
  threshold or more, chosen by the schedule.
- **`quorum_once`.** The proposed trusted operator: "emit k once, as soon as n distinct responders
  have answered k." It is set-determined by definition. The lineage of k's record is the n
  responses.
- **Harness fact.** Something true only because of the inputs the test sends: one proposer, one
  write, a `quiesce()`. Races sees only lineage and fragile tags, never harness facts.

## 1. The five mints in `q.rs`, and the older `hydro_std::quorum`

| function | computes | existence | value | expressible as `quorum_once` without changing behaviour? |
|---|---|---|---|---|
| `quorum` q.rs:121-185 | `(fact, attestor)` → `Durable<fact>`, once, when the number of distinct attestors reaches `threshold` | set-determined (A: `unique` q.rs:133; persisted `pending` q.rs:142,164-165; `fired` q.rs:143,161,168) | none: the record is the key | **Yes. It *is* `quorum_once`** with key = fact. It needs no rewrite, only the checker's trust |
| `quorum_retiring` q.rs:195-281 | `quorum` plus metadata from an `active` stream, with per-request state discarded after firing | set-determined **only under the caller contract** of one outstanding request (see §2) | the metadata, taken from the activation record: a function of k | Yes: `quorum_once(acks)` joined at top level with `active` on k. Under the contract the output set and the messages are the same. Memory is unbounded instead of retired. It is used only in `abd_kv_register` (abd.rs:812), which is not a corpus target |
| `covering_quorum` q.rs:334-394 | `(k, (responder, Option<(Ts,X)>))` → `(k, Covering<max>)`, once | set-determined (A: q.rs:358-384, same persist/fire structure) | **max over the first-arrived subset**: everything in `pending ∪ batch` at the crossing tick (q.rs:358-378). This is **order-sensitive** (D, §4) | Existence yes. **Value no.** Making the value set-determined would mean waiting for *all* responders, which loses liveness under a crash |
| `covering_quorum_retiring` q.rs:402-497 | same, plus metadata and retiring | as `quorum_retiring` | max over the responders in `pending` at the crossing tick (q.rs:473-483). Order-sensitive | Existence only. It is used only by `abd_kv_register` (abd.rs:741) |
| `covering_quorum_slotted` q.rs:529-598 | `(ballot, (acceptor, SlotMap))` → `(ballot, Covering<BTreeMap<slot,(Ts,X)>>)`, once | set-determined | slot-wise max over the first-arrived subset of promises (q.rs:561-577). Order-sensitive. The promises' contents are themselves schedule-dependent (§3) | Existence only |

So the trusted mint already exists in the corpus: `quorum`, whose unforgeable output is
`Durable` (q.rs:95). The "covering" mints are a different operator, "existence-robust,
value-fragile". The module says so itself: "certificate existence is deterministic, certificate
content is honest nondeterminism" (q.rs:289-291, 521-523).

**A second shape: `hydro_std/src/quorum.rs`.** This older library is not used by the corpus
targets. It is used by `hydro_test` Paxos and 2PC (`paxos.rs:528,741`, `two_pc.rs:40,61`).
- **`collect_quorum`** (:110-171) counts `Ok` *messages*, not distinct responders. It removes a
  key's state once `max` responses have arrived. It is set-determined only if each key gets at most
  `max` responses (A).
- **`collect_quorum_with_response`** (:7-108) emits **every** `Ok` response of a key that reached
  `min`, including late ones, up to `max`. Its output set is therefore set-determined, values
  included (A: the doc at :24-31). It avoids first-majority sensitivity by giving the choice to the
  consumer. A consumer that must pick one value (a max ballot "now") reintroduces the choice
  downstream.

## 2. A side finding: `quorum_retiring` existence is order-sensitive outside its contract

At every activation, `pending_ref.retain` (q.rs:239-247) drops buffered acks for keys that are
neither active nor activated in this tick. Here is a run that loses an output:
1. The acks for r2 arrive before r2's activation is released.
2. An activation for r3 runs. This drops the acks for r2.
3. r2 activates later and never reaches the threshold.

ABD-KV's one-outstanding-operation contract rules this out (A: abd.rs:329-331). The contract is a
caller fact, so races cannot see it. `covering_quorum_retiring` has the same code (q.rs:445-453).
D: see §4.

## 3. What is order-sensitive and no combinator removes

1. **Paxos acceptor fence** (mp.rs:343-386). An accept is answered iff `b >= mp0`, the promise *at
   tick start* (:350). A promise is given iff `b > mp0` (:374).
   - **Scenario** (A): two proposers. P1 has covered with ballot (1,P1) and sends
     `accept((1,P1), slot 0, v1)`. P2 sends `prepare((2,P2))`.
   - At acceptor a0, if the accept is released first, `Accepted((1,P1),0,v1)` exists. If the
     prepare is released first, it is refused and the record does not exist in that run.
   - The faults are the same in both runs. Whether `(1, start, 0, v1)` is chosen depends on this.
     Refusal is the point of Paxos (safety), so no set-determined operator can replace it.
2. **Paxos covering value** (`covering_quorum_slotted`, then `EpochPlan::new`, mp.rs:162-176 and
   515-519). The plan's `horizon` is the highest slot in the first-arrived maps.
   - **Scenario** (A): an old leader's accept for slot 5 reached only a2. A covering that includes
     a2 re-proposes slots `start..=5` and gives new commands slots 6 and up. A covering of {a0,a1}
     proposes nothing and gives new commands slot `start` and up.
   - So different `(b, start, slot, v)` proposals, and therefore chosen facts, exist. By Paxos
     safety, *chosen* slots get the same value under every covering, but the tuples (round, start,
     slot) and the unchosen slots differ.
3. **Paxos declared start** (mp.rs:504-512, 516). `start` = the learned prefix at the covering
   tick. This is a learned-slots batch meeting persisted state ("P" in races-target-steps.md).
   `start` is a field of every `learned` tuple (mp.rs:559,608-615), so it changes the facts'
   identity, not only their timing.
4. **Paxos leader kernel** (mp.rs:500-541). Commands wait in `pending` until `cur` is set. A higher
   covering replaces `cur` (:522-528). Which epoch sequences a command depends on the order in
   which coverings and commands are released. Leader election itself is outside this code: the
   rounds are harness input `leads`.
5. **ABD replica snapshot** (abd.rs:208, 260-261). A query's response carries the register *at the
   tick its batch is released*.
   - **Scenario** (A): client B's phase 2 for `(1,B)` reaches r0 only. Client A queries. If A's
     first-arrived subset includes r0 after r0 applied B's write, A's max is `(1,B)` and A writes
     at `ts=(2,A)` (abd.rs:300). If A's subset is {r1,r2}, A writes at `(1,A)`.
   - Both runs are failure-free, with different phase-2 records. This is what a linearizable
     register does: a concurrent write may or may not be observed.
6. **ABD covering value** (`covering_quorum`, abd.rs:428). This is the max over the first-arrived
   subset (D, §4), and it feeds the timestamp `ts` of every phase-2 record.

Items 1, 2, 5 and 6 are the protocols' *specified* nondeterminism. Item 3 is a design choice
(`start` could be removed from the fact's identity). Item 4 is the authored sequencing.

## 4. Demonstrations (scratch tests, not committed)

I ran three scratch tests, added temporarily to `q.rs`'s test module, using
`cargo test -p hydro_std --lib -- ec_inference_demos::quorum::tests::scratch --nocapture`:

| test | input | output over all schedules | shows |
|---|---|---|---|
| `scratch_covering_value_varies_by_schedule` | `covering_quorum(2)`, key 5, responses `None`, `(2,·)`, `(3,·)` | 13 schedules: `{Some(round 2): 3, Some(round 3): 10}`. Exactly one covering every time | **D**: existence is set-determined. The value is the max of the first-arrived subset and differs by schedule |
| `scratch_quorum_set_is_schedule_independent` | `quorum(2)`. Fact 7 from members 0,1,2. Fact 9 from member 0 twice | 192 schedules: `{[7]: 192}` | **D**: the output set is the same in every schedule, and a duplicate does not forge a quorum |
| `scratch_quorum_retiring_concurrent` | `quorum_retiring(2)`. Activations r2 and r3 sent together, two acks for r2 | 104 schedules: `{[2]: 92, []: 12}` | **D**: outside the one-outstanding contract, r2's certificate is lost in 12 schedules (§2) |

## 5. Per label, if the corpus used a trusted `quorum_once` wherever that preserves behaviour

"Robust?" means that no fragile step remains on any derivation path, so the label's pairs could
prune fault sets.

### Uniform reliable broadcast (`ub.rs:50-80`, rb.rs:102-160)

| label | path | robust? |
|---|---|---|
| `initial` rb.rs:130 | `broadcast_closed` at top level | **yes** (already) |
| `echo` rb.rs:146-156 | merge, top-level `unique`, `broadcast_closed` | **yes** (already) |
| `distinct_acks` q.rs:133 | `entries().map`, then `unique` | **yes** (already) |
| `certified` q.rs:135, output ub.rs:79 | `quorum` | **YES**, once `quorum` is trusted. No other step is fragile |

With `quorum` trusted, URB has **no fragile step at all**:
- Every other operator is top-level and set-preserving. Its cross product is outside a tick, and
  membership is not a fragile source (A: fragile.rs:168-175, 184).
- So it would be *fragile-free* (`hydrofrac-races.md` §4.1). It would get a one-schedule probe, and
  Claim B, not only Claim A.
- This is the case where the combinator pays off in full: today a 3-member probe exceeds 215,000
  schedules (§11).
- No rewrite of the program is needed. The checker must recognise `quorum`'s lowering strictly (as
  `fanout::mark` does for `broadcast_closed`), or the operator must be given its own IR marker.

**Lineage needs care** (A + G). The rewrite gives a closure over `by_mut` state the lineage of
*everything the state absorbed* (provenance/mod.rs:14-15).
- `pending` holds all facts, so a certificate for fact 2 would carry fact 1's echoes as well. That
  is sound but coarse, and every extra delivery is one more way to kill the derivation.
- The trusted version should give `Durable(f)` the lineage of exactly the attestations of `f`.
  The minimal choice is the first `threshold` of them.

### Multi-Paxos (`mp.rs`)

| label | path | robust? | remaining fragile step |
|---|---|---|---|
| `prepares` :280-282 | `leads → map → broadcast_closed` | **yes** (already) | — |
| `accepts_fwd` :286, `accepts` :546-550 | leader kernel ← `covered`, `commands`, `learned_slots` | **no** | leader closure over state :503 (R); in-tick folds :466-483; `covering_quorum_slotted` fold_keyed and anti_join q.rs:561,584,586; upstream, the acceptor closure :343 |
| `distinct_acks`/`certified` of `chosen` :557 | `quorum(majority, accepted_acks)`. Already `quorum_once` | **no** | `accepted_acks` come out of the acceptor closure :343 (fence :350) |
| `learned_at_proposers` :566-570, `initial` :580-583, `learned` :588, `echo` :589-592, `facts` :601-616, `log` :618 | everything downstream of `chosen` is top-level and set-preserving | **no** | same as `chosen`, plus `start` from the learned-prefix batch :447,504-512 |

In the harness (`ec_demos_hydrofrac.rs:213-242`: one proposer, one lead, a `quiesce()`, then two
commands), the fence never fires, every covering is empty, and `start = 0`. So the facts are in
fact schedule-independent (races-target-steps.md §3). All three are harness facts and none appears
in lineage: the tags at :343 and :503 are set in every run.

### ABD (`abd.rs`, harness `ec_abd_hydrofrac.rs:25-56`)

| label | path | robust? | remaining fragile step |
|---|---|---|---|
| `queries` :374-379 | harness reads/writes, `map`, merge, `broadcast_closed` | **yes** (already) | — |
| `phase2_fwd` :385, `phase2` :470-477, `register` :395 (checked at its input) | `query_resps` ← replica `cross_singleton(snapshot)` :208,260-261; then `covering_quorum` :428; `join` :441; `ts` from the max :300 | **no** | the snapshot :208 and cross_singleton :261 (the response *value*); `covering_quorum`'s in-tick fold and anti_join q.rs:360,380,382 (the max over the first-arrived subset). Existence alone could be trusted, but the record carries `ts` |
| reliable variant `initial`/`new_messages`/`echo` (rb.rs:267-277) | the same phase-2 records, echoed | **no** | same |

The `quorum` at :481 (write acks → `certified`) is already `quorum_once`. It feeds only `write_done`
and `read_result`, which carry no label. Their upstream (acks, :237-250) is a `cross_singleton`
with a snapshot whose existence is in fact cumulative: `waiting` persists and the register is
monotone. But the step is still tagged.

In the one-write harness every response is `None`, so the max is `None` and `ts = (1, client0)` in
every schedule. That is a harness fact again: the tags remain.

## 6. Bottom line

- **URB delivered output: becomes certifiable. YES.** The program already uses the audited
  combinator (`quorum`). Trusting it makes URB fragile-free. Remaining work is in the checker: a
  strict recognition of `quorum`'s IR, and per-fact lineage.
- **Paxos `learned`: NO.** The quorum on its path (`chosen`) is already `quorum_once`. What stays
  fragile is upstream of it and is the protocol itself:
  - the acceptor's promise fence;
  - the first-majority covering;
  - the leader's sequencing and `start` snapshot.
- **ABD `phase2`/`register`: NO.** Every phase-2 record carries a timestamp computed from the
  first-arrived majority's register snapshots. That is order-sensitive by specification. A
  set-determined replacement would have to wait for every replica, which changes behaviour.
- **So one trusted `quorum_once` buys URB only.** For Paxos and ABD the benefit is limited to labels
  that are robust already (`prepares`, `queries`). Their important labels stay at brute-force cost
  unless the checker gains a different capability, for example:
  - v2's ordering premises discharged by barriers (§8.1);
  - "existence-robust, value-fragile" tags plus a check that the value is the same in every
    schedule of the harness. That is a harness-level fact, which lineage alone cannot give.
