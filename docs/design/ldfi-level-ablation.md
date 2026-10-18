## Raft

Harness: `hydro_test/src/cluster/ldfi_ablation_raft.rs` (`level_ablation_raft`, gated on
`LDFI_ABLATION_OUT`), a copy of `raft_ldfi.rs`'s durability harness with the configuration
shrunk so that the sixteen-cell grid fits in about half an hour (the Raft half took 19.5
minutes, the Paxos half 8): **2 rounds** before the end of
finite failures instead of 3, **1 sampled schedule per hypothesis** instead of 4, and a
hypothesis cap of **64** (the original searches run 90–235 hypotheses × 4 schedules and take
30–68 minutes each). Three members, crash budget 1, `TCP.lossy_delayed_forever()` on the
intra-cluster channel, heal phase unchanged (2 cycles × 3 heartbeat rounds). The program is
compiled once per variant with `with_provenance()` and all four levels run on it. Fuzz
schedules draw OS entropy, so repeated runs differ; wall times were taken on an 8-core host with
one other search running concurrently.

Cells: `verdict / hypotheses / schedules / mean derivation size / seconds`. Mean derivation size
is the mean number of `Dependency` leaves over the derivations of goal-held schedules.

| goal | config | L0 exec | L1 slice | L2 record | L3 replay |
|---|---|---|---|---|---|
| durability of a committed entry | n=3 cr=1 rounds=2 heal=2 sched=1 max=64, bug = election restriction ignores term | certificate / 59 / 59 / 46.4 / 19s | certificate / 59 / 59 / 46.4 / 19s | budget / 64 / 64 / 10.5 / 244s | budget / 64 / 64 / 10.4 / 253s |
| durability of a committed entry | n=3 cr=1 rounds=2 heal=2 sched=1 max=64, correct | certificate / 59 / 59 / 46.4 / 19s | certificate / 59 / 59 / 46.4 / 19s | budget / 64 / 64 / 10.5 / 306s | budget / 64 / 64 / 10.1 / 255s |
| durability of a committed entry | correct, as above but max=160 (L2 only) | – | – | certificate / 107 / 107 / 10.3 / 403s | – |

Replays / replay fallbacks per cell:

| config | L0 exec | L1 slice | L2 record | L3 replay |
|---|---|---|---|---|
| bug | 1,984 / 0 | 1,984 / 0 | 16,510 / 0 | 18,732 / 0 |
| correct | 1,984 / 0 | 1,984 / 0 | 16,286 / 0 | 18,741 / 0 |
| correct, max=160 | – | – | 27,594 / 0 | – |

An earlier pass over the same grid (same configuration, different schedule draws, heavier
machine load) gave the same L0/L1 cells to the digit and L2/L3 cells of `budget / 64 / 64 /
10.5–10.9 / 310–373s`.

### Observations

- **L2 vs L1 derivation size: 4.4× smaller** (46.4 → 10.1–10.5 dependencies per derivation).
  Raft is one `raft_step` closure per tick over a `RaftServerState`, so at L0/L1 the static
  slice is a single opaque sink and every output depends on every delivery into the member;
  L0 and L1 are identical cells (same hypotheses, same derivations, same wall time), as
  `hydrofrac.md` predicts for a closure-encoded protocol.
- **L3 vs L2: no difference.** Under the provenance rewrite the step's outputs are
  `Prov::OPAQUE`, so L2 already replays through the step; L3 adds replay of the dataflow-style
  parts (the observer, the channel plumbing) and lands within 1% on derivation size and within
  10% on replay count. On this harness L2 *is* counterfactual lineage.
- **Fewer dependencies did not mean fewer hypotheses.** L0/L1 certify in 59 hypotheses; L2 was
  still running at 64 and, given 160, certified in 107 (on the correct variant). The
  over-approximate clause ("everything delivered to the member") is hit by nearly any fault
  set, and successive runs saturate the fault universe quickly; the precise clauses each name a
  different path (an original `AppendEntries`, its heartbeat retransmission, a vote) and the
  solver has to defeat each one separately. Wall time is dominated by replay either way:
  ~4–5 s per hypothesis at L2/L3 (260–290 replays per hypothesis) against ~0.3 s at L0/L1, so
  under this budget L2/L3 cost ~13× the wall time of L1 for a verdict that did not arrive.
- **Replays at L0/L1 are not zero** (1,984 per search). The specification is hand-written, and
  `spec_view` derives a hand-written view's final value by counterfactual replay *of the
  observer* at every level (unioned with the recorded derivation); only the protocol nodes'
  lineage follows the configured level. None of the Raft cells recorded a replay fallback; the
  replays that panicked inside the generated crate ("index out of bounds: the len is 1 but the
  index is 1", the known leader-indexed-past-its-log case) are counted as "target not produced"
  and keep the delivery, not as fallbacks.
- **Verdict agreement.** No cell contradicts another: the correct variant is certified at
  every level that finished (L0, L1, L2@160). The bug variant is certified at L0/L1 and
  unresolved (`budget`) at L2/L3. The injected bug *is* reachable in the original harness
  (3 rounds, 4 schedules: counterexample after 90 hypotheses at L2); whether it is reachable
  in 2 rounds with 1 schedule is not known, so the L0/L1 certificates on the bug variant are
  either correct for the shrunken schedule or a sampled-schedule miss, and this grid cannot tell
  which. **Not flagged as a soundness bug**, but worth a longer L2 run on `raft-bug` with
  `LDFI_ABLATION_MAX_HYP=256` (≈20 min) before quoting the L0/L1 cells as evidence.
- **Operators blocking L2: none.** Raft compiles under the provenance rewrite (the state-carrying
  closure is wrapped by `wrap_opaque`), so no cell is `n/a`.

## Paxos (core, for reference)

Same harness file, a copy of `paxos_ldfi.rs`'s core harness (two proposers, three acceptors,
`LeaderTiming::Driven`, lossy acceptor links, crash budget 1), with 1 sampled schedule per
hypothesis instead of 4 and a hypothesis cap of 256 (the original certificate search ran 372
hypotheses × 4 schedules in about 30 minutes). With one schedule the failure-free run is
vacuous (`PRE`, "proposer 0 chose `x`", does not hold) in roughly a third of the draws; the
harness redraws up to four times and reports the count.

| goal | config | L0 exec | L1 slice | L2 record | L3 replay |
|---|---|---|---|---|---|
| agreement across a leader change | p=2 a=3 cr=1 sched=1 max=256, bug = commit quorum off by one | violated / 44 / 44 / 20.6 / 3s | violated / 43 / 43 / 22.5 / 3s | violated / 67 / 67 / 15.0 / 15s | violated / 82 / 82 / 15.9 / 24s |
| agreement across a leader change | p=2 a=3 cr=1 sched=1 max=256, correct | budget / 256 / 256 / 21.1 / 87s | budget / 256 / 256 / 21.6 / 96s | certificate / 195 / 195 / 18.1 / 70s | certificate / 236 / 236 / 19.5 / 160s |

Replays / replay fallbacks / vacuous redraws of the failure-free run:

| config | L0 exec | L1 slice | L2 record | L3 replay |
|---|---|---|---|---|
| bug | 147 / 0 / 3 | 187 / 0 / 2 | 259 / 0 / 2 | 638 / 0 / 0 |
| correct | 746 / 0 / 0 | 760 / 0 / 2 | 594 / 0 / 0 | 1,348 / 0 / 0 |

Two earlier passes (before the vacuous redraw existed) are informative about variance: with
cap 64 the bug was found at L1 in 38 and at L2 in 28 hypotheses, and L0/L3 hit the cap; with
cap 2048, L0 certified the correct variant in 180 hypotheses (46s) and L1 in 151 (31s), while
L2 drew a vacuous schedule and L3 had not finished after 25 minutes and was killed. Counts at
one schedule per hypothesis move by ±50% between passes; the table above is a single pass.

### Observations

- **L2 vs L1 derivation size: ~1.3–1.5× smaller** (22.5 → 15.0 on the bug variant, 21.6 → 18.1
  on the correct one): Paxos is dataflow-style, so the slice already separates P1/P2 traffic
  and record provenance removes the rest of the over-approximation within an execution.
- **L3 vs L2:** derivations of the same size (15.9 vs 15.0, 19.5 vs 18.1), 2–2.5× the replays,
  1.6–2.3× the wall time. Replay sees nothing the rewrite missed here.
- **Hypotheses:** as on Raft, smaller derivations did not shorten the search for the bug
  (L0/L1 43–44, L2 67, L3 82); the counterexamples differ by level (`p2a` omissions to two
  acceptors at L0/L1, a `p1b` omission at L2/L3), so the levels walk different paths through
  the fault lattice. On the correct variant L2 and L3 certified within 256 where L0 and L1 did
  not; the earlier pass had L0/L1 certifying in 151–180, so this is within schedule variance.
- **Verdict agreement:** every level that reached a verdict agrees (bug → `violated` at all
  four; correct → `certificate` or `budget`). No soundness disagreement.
- **Operators blocking L2: none**; core Paxos compiles under the rewrite.
