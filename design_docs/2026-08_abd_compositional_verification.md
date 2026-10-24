# ABD compositional verification plan

2026-08. Companion code:
`hydro_std/src/ec_inference_demos/abd.rs` and
`hydro_std/src/ec_inference_demos/quorum.rs`.

## Status and claim discipline

This document separates three different kinds of evidence which must not be
collapsed into the slogan “the components were tested, therefore the protocol
is correct”:

1. **A mathematical composition argument** derives ABD safety and progress
   from explicit component contracts and environmental assumptions.
2. **Bounded exhaustive simulation** checks the Hydro implementations of the
   small stateful components over named finite input domains and every schedule
   the simulator represents for those inputs.
3. **An end-to-end smoke test** remains a regression sentinel for wiring and
   compilation. It is not part of the linearizability proof.

Exhaustive simulation is exhaustive over schedules for the supplied finite
inputs; it is not quantification over arbitrary values, histories, cluster
sizes, or executions. The mathematical proof therefore does not follow from
those tests. Conversely, the proof is about the contracts below, so its
application to the running Hydro program depends on the implementation-to-
contract mapping in §7. A future mechanically checked refinement could close
that last gap; this plan records rather than hides it.

The intended evidence level is consequently: **manual theorem over explicit
contracts, bounded exhaustive audits of each local implementation, and one
assembled wiring sentinel**. End-to-end fuzzing is not needed as evidence for
the theorem once all obligations below are discharged, although it remains a
useful independent bug-finding tool.

## 1. Specification and assumptions

The object is a multi-writer, multi-reader atomic register initially empty.
A completed `write(v)` returns no value. A completed `read()` returns empty if
no write precedes it in the chosen linearization, otherwise the value of the
latest preceding write. Linearizability requires a legal sequential history
which preserves the real-time order of non-overlapping operations.

The theorem assumes:

- Static membership of `N` replicas and quorum threshold `Q` with `2Q > N`.
- Fail-stop failures only; no Byzantine member or forged member identity.
- At least `Q` live replicas for progress (safety does not require progress).
- Between live members, messages selected for progress are eventually
  delivered and enabled local work is eventually scheduled.
- Request ids are unique per client member across reads and writes.
- Each client member has at most one operation outstanding.
- `Ts = (round, writer)` is ordered lexicographically; distinct client members
  have distinct `writer` ids.
- The imported Hydro primitives satisfy the contracts named in §6.

The intentional freedom is which `Q` responses form a covering. The schedule
may choose any such subset; safety must hold for all choices.

## 2. Component contracts

Each contract is both a proof premise and the oracle for its component tests.
Contracts are written independently of batching and tick boundaries.

### C1. Timestamp and client planning

For a write by member `w`, covering maximum `m`, and request id `rid`:

- The planned timestamp is `(1, w)` when `m` is empty, and
  `(m.ts.round + 1, w)` otherwise; hence it is strictly greater than `m`.
- Distinct writers cannot mint equal timestamps.
- Under the one-outstanding-operation rule, one writer cannot associate two
  values with the same timestamp.
- Phase 2 is emitted only for the request whose covering fired.

For a read:

- An empty covering returns empty and emits no phase-2 write.
- A nonempty covering `(ts, v)` emits exactly that pair as write-back: reads do
  not mint or change timestamps and cannot invent values.
- A nonempty result is returned only after phase 2 for that request has a
  `Durable` certificate.

For both operation kinds, request correlation is exact: a covering or durable
certificate for one `rid` cannot advance or complete another `rid`.

### C2. Replica max register

For each replica register:

- State is empty or a single `(ts, value)` pair.
- Applying a phase-2 pair replaces state exactly when its timestamp is larger.
- State is monotonically nondecreasing by timestamp.
- Subject to C1's timestamp/value uniqueness, applying a finite multiset of
  phase-2 pairs is independent of message order, duplicates, and batching and
  yields its timestamp maximum.

The existing top-level fold and its explicit EC type annotation mechanically
pin the additional label claim: all live replica folds over the same EC input
converge. That label is useful evidence for the implementation but the safety
proof below needs only each replica's local monotonicity.

### C3. Replica query and ack service

- A replica emits an ack for phase-2 `(rid, ts, value)` only after a register
  snapshot is at least `ts` (the write is installed or superseded).
- An unacknowledgeable request remains pending across ticks; once a snapshot is
  at least `ts`, fair scheduling eventually emits its ack.
- A query response is a current register snapshot.
- Therefore, after a replica has acknowledged `ts`, all later query responses
  from that replica carry a timestamp at least `ts`.
- A replica emits at most one applicable response per exactly-once request.

The `>=`, rather than equality, condition is essential: a newer value may
legitimately supersede a pending write before its ack is emitted.

### C4. Durable quorum mint

For `quorum(Q, attestations)`:

- A `Durable(f)` certificate is emitted only after `Q` distinct replica
  identities attest the same fact `f`.
- Repeated attestations by one identity count once.
- A fact certifies at most once.

The threshold's relationship to `N` is deliberately a caller obligation, not
something the mint can infer.

### C5. Covering quorum mint

For each request key, `covering_quorum(Q, responses)`:

- It emits only after responses from `Q` distinct replicas.
- It emits exactly once for that key.
- Its aggregate is the timestamp maximum of exactly the schedule-chosen
  threshold-reaching set (empty only if every member of that set reports
  empty).
- Keys do not contaminate one another and later responses cannot revise a
  fired covering.

This contract permits different legal aggregates from different schedules.
An oracle must check membership in the set of legal first-covering results,
not require the maximum of every response that will eventually arrive.

## 3. Bounded exhaustive test plan and status

The implemented audit currently covers T1 and T6 with finite ordinary
enumeration, T2/T3 with two exhaustive simulations of the exact production
fold/service functions, and T4/T5 with seven exhaustive mint simulations. The
original one-outstanding-operation RED witness and assembled ABD fuzz/crash
tests remain as independent bug-finding evidence. The additional red mutants
listed below are future strengthening, not prerequisites silently claimed as
already implemented.

Every simulator test exercises at least one execution; representative stateful
cases report 11 schedules for ack-then-query and 1,461 schedules for the
two-write max/superseded-ack scenario. Exhaustiveness is always relative to the
named finite input multiset.

### T1. Timestamp/planner finite enumeration

The implemented table test enumerates request ids `{0,1}`, writer ids
`{0,1}`, covered rounds `{empty,0,1,2}`, and both read outcomes. It checks
empty/nonempty write stamping, strict domination, cross-writer inequality,
read write-back identity, empty-read behavior, and preservation of the request
id through the planner. Equal-key-only correlation itself is an imported keyed
`join` contract (§6), not established by this pure test. The existing
assembled RED test retains the overlapping-write counterexample: one writer
can mint equal timestamps for different values when it violates the
outstanding-operation rule.

### T2. Replica max fold, exhaustive schedules

Directly inject the exact production fold/service functions at one replica,
avoiding network round trips. The implemented finite case supplies two ordered
timestamps as one unordered input bag, so exhaustive scheduling varies their
admission order and batch boundaries; after both acknowledgements, a query
must return the higher pair. The same check establishes that the lower write
may be superseded before acknowledgement. Duplicate and three-level bags are
useful future matrix extensions.

For every explored schedule, final state is the supplied multiset maximum and
never regresses. A future red non-max combiner should yield a counterexample.

### T3. Ack gate/query service, exhaustive schedules

The implemented exhaustive traces cover max/superseded ack and the ordering
"observe ack, then issue query", which directly audits the proof's persistence
premise and exactly-once acknowledgement for those inputs. Delayed coverage
arises among the explored snapshot/batch schedules. Explicit multi-tick delay
and query-before-ack cases are useful future matrix extensions. Future red
variants removing the gate and changing `>=` to `==` should respectively
expose a premature ack and a permanently stuck superseded write.

### T4. Durable mint, exhaustive schedules

For a small fixed threshold, cover fewer than threshold distinct attestors,
exactly threshold, duplicates, more than threshold, and two interleaved facts.
Assert soundness, distinctness, per-fact isolation, and fired-once.

### T5. Covering mint, exhaustive schedules

For `Q = 2`, the implemented tests cover exactly two responders, duplicate
responder, three responders, empty plus nonempty, two interleaved request keys,
and a late third response. For three responders, the oracle accepts only
aggregates produced by one legal threshold-reaching pair. A future red
duplicate-counting variant should mint a false covering.

### T6. Finite quorum-arithmetic audit

For `N = 1..=7`, enumerate subsets and check that all subsets of size at least
`Q` intersect when `2Q > N`. For a broken threshold, record disjoint witnesses.
This audits examples; the general intersection lemma below is elementary
mathematics, not established by this finite test.

### Integration sentinel

The current suite retains all assembled tests as defense in depth, including
the write-then-read wiring sentinel. Their purpose is to catch gross miswiring,
missing completion, crashes, or a code/proof mapping accidentally bypassed;
they are not cited as proving linearizability.

## 4. Safety argument

Write `reg_r(t)` for replica `r`'s register timestamp at a point in an
execution, with empty as bottom.

### L1. Ack persistence

If replica `r` acknowledges timestamp `t`, then from that ack onward
`reg_r >= t`.

By C3, the ack is gated on a snapshot at least `t`; by C2, the register never
regresses.

### L2. Completed nonempty operations leave a durable set

If a write or nonempty read completes with timestamp `t`, there exists a set
`D` of at least `Q` distinct replicas whose registers remain at least `t`.

By C1, completion follows a `Durable` phase-2 certificate. By C4, the
certificate has `Q` distinct replica acknowledgements. Apply L1 to each.

### L3. Later coverings dominate completed operations

If nonempty operation `a` completes before operation `b` begins, and `a` has
timestamp `t_a`, then `b`'s covering maximum is at least `t_a`.

L2 supplies `D`, `|D| >= Q`. C5 supplies the covering responder set `C`,
`|C| >= Q`. Since `2Q > N`, `C ∩ D` is nonempty. A member in the intersection
responds at least `t_a` by L1 and C3; C5's maximum is therefore at least
`t_a`.

### L4. Real-time timestamp order

If `a` completes before `b` begins, then a nonempty read `b` adopts a timestamp
at least `a`'s, while a write `b` mints one strictly greater than `a`'s.

This is L3 plus C1. After any completed nonempty operation, a later covering
cannot be empty, so the empty-read case cannot violate this lemma.

### L5. Timestamp/value integrity

Every non-bottom timestamp denotes one written value. Distinct writers cannot
tie, one writer cannot tie under the one-outstanding-operation assumption,
and reads only retransmit existing pairs (C1). Thus max merge never chooses
between different values at an equal timestamp.

### L6. Read stabilization and incomplete writes

A nonempty read returns the value associated with its adopted timestamp and,
before returning, makes that pair durable (C1 and L2). It may adopt a write
whose invocation has not completed. Linearizability permits completing that
pending write immediately before the read in the completed extension. Read
write-back prevents new-old inversion: any later covering dominates the
returned read by L3.

### Theorem. ABD safety

Extend the history by completing each pending write whose timestamp/value is
returned by a completed read; discard other incomplete operations. Order
writes by timestamp. Place every nonempty read with timestamp `t` after the
write of `t` and before the first greater-timestamp write, ordering reads with
the same `t` in any real-time-consistent order. Place empty reads before every
write they do not overlap.

L5 makes the write order and returned values unambiguous. L4 preserves
real-time order between non-overlapping operations. L6 ensures every nonempty
read returns the latest preceding write in the constructed order and that a
later non-overlapping operation cannot regress. Empty-read placement is legal
because a completed preceding nonempty operation would make its covering
nonempty by L3. The constructed sequential history therefore implements the
atomic-register specification and respects real time: the completed history
is linearizable.

## 5. Progress argument

Consider an operation invoked by a live client while at least `Q` replicas
remain live. Under eventual delivery and fair scheduling:

1. Phase-1 broadcast reaches at least `Q` live replicas.
2. C3 eventually yields their query responses; C5 yields a covering.
3. An empty read completes at this point.
4. A write or nonempty read broadcasts phase 2 to the replicas.
5. C2 installs or supersedes it at each responding live replica; C3 eventually
   acknowledges it.
6. C4 yields a durable certificate after `Q` distinct acks, and C1 completes
   the operation.

This proves conditional progress, not wait-freedom in an unfair execution and
not completion for a client which itself crashes.

## 6. Imported Hydro contracts

The proof treats these as trusted-base imports (S8 in the trust accounting),
not ABD-local test obligations:

- `broadcast_closed` fans an element to every fixed destination member with
  source identity supplied by channel keying.
- `demux` routes to the keyed requester and supplies sender identity on the
  receiving keyed stream.
- `TCP.fail_stop()` and the simulator's crash model match the delivery/failure
  assumptions stated in §1.
- `ExactlyOnce` supplies the documented retry semantics.
- Keyed `join` correlates equal keys only.
- `forward_ref` denotes the declared feedback edge without changing element
  values.

The `Durable` and `Covering` constructors remain convention-sealed (S7) while
staged code prevents true Rust privacy. The proof assumes certificates are
created only by their named mints; promoting the mints into `hydro_lang` would
shrink this assumption.

## 7. Implementation-to-obligation map

No safety-relevant line is allowed to be dismissed as “glue”; every region is
assigned a contract or imported premise.

| implementation region | obligation |
|---|---|
| `ToReplica` / `ToClient` variants and channel keying | request kind integrity; imported transport/authentication contracts |
| phase-1 merged query broadcast | imported `broadcast_closed`; unique `rid` premise |
| phase-2 `forward_ref` | imported feedback semantics |
| `abd_register_state` top-level max fold | C2 and C1 timestamp/value uniqueness |
| `abd_replica`: `reg_now`, `waiting`, candidates and ack filters | C3, relying on C2 monotonicity |
| `abd_replica` query response branch | C3 |
| response `demux` and variant split | imported routing plus request-kind integrity |
| `covering_quorum` call | C5 and `2Q > N` caller premise |
| `AbdPlan::from_covering`, its keyed join and variant splits | C1 timestamp choice, read repair, and request correlation |
| phase-2 broadcast and completion of forward ref | imported broadcast/feedback semantics |
| `quorum` call | C4 |
| certified/result joins | C1 request correlation |

A refactor which changes this map must update this document and the relevant
local contract tests. Strong ABD-specific wrapper types may make request kind
and phase correlation structural in the future; until then those joins are a
manual refinement obligation.

## 8. What may and may not be claimed

After T1–T6 and the mapping audit pass, the defensible claim is:

> ABD safety and conditional progress are derived compositionally from C1–C5
> and the imported Hydro contracts. Bounded exhaustive component simulations
> audit the local implementations against those contracts for named finite
> cases. The assembled smoke test is a wiring regression sentinel, not a proof.

It is **not** defensible to claim that exhaustive component tests alone prove
arbitrary ABD executions, or that no integration bug is possible. Literal
mechanical elimination of the integration/refinement gap would require a
proof assistant/model checker over unbounded contracts, refinement of the
Hydro IR to that model, or types/certificates which make every wiring step
constructionally valid. This document deliberately leaves that residual trust
visible.
