## ISR replication

Harness: `isr_replication::harness` (`hydro_test`), 3 replicas, 4 rounds, 1 write; the
hand-written durability specification (`PRE`: the client was acknowledged for `Write(1)` and
ZK's final view names a leader; `POST`: that leader's log holds `Write(1)`); replicas crashable,
omissions on the lossy replication channel within the first round. Two encodings of the same
protocol: **closure** (one opaque step function per tick) and **dataflow** (per-message-type
streams). *unclean* is the buggy variant (election from outside the ISR), *clean* the correct one.

Every cell searches the **same provenance-compiled program** (`with_provenance()`), so the levels
differ only in which lineage the backward closure follows. `fuzz k` = `Exploration::Fuzz
{ instances: k }` (k = 8 is what the unit tests use; k = 1 is the measurement grid's one schedule
per hypothesis). Hypothesis budget 256 (the default). Harness: `ldfi_ablation_isr.rs`, test
`level_ablation_isr` (gated on `LDFI_ABLATION_OUT`); one run, debug build, ~5 min total.

Cell = `verdict / hypotheses / schedules / mean derivation size / wall secs`; mean derivation size
is the mean number of dependencies per derivation over goal-held schedules. Replay counts are in
the notes below rather than the cells.

| encoding | config | L0 exec | L1 slice | L2 record | L3 replay |
|---|---|---|---|---|---|
| closure | unclean cr=1 fuzz 8 | **cert** / 14 / 112 / 7.4 / 6.9s | **cert** / 14 / 112 / 7.4 / 6.5s | bug / 9 / 72 / 3.8 / 36.3s | **cert** / 6 / 48 / 1.7 / 9.7s |
| closure | unclean cr=1 fuzz 1 | **cert** / 14 / 14 / 7.4 / 0.5s | **cert** / 14 / 14 / 7.4 / 0.5s | bug / 9 / 9 / 3.9 / 2.9s | **cert** / 6 / 6 / 1.7 / 0.7s |
| closure | unclean cr=0 fuzz 8 | cert / 4 / 32 / 8.8 / 1.9s | cert / 4 / 32 / 8.8 / 1.2s | cert / 1 / 8 / 4.0 / 4.2s | cert / 1 / 8 / 1.0 / 2.3s |
| closure | clean cr=1 fuzz 8 | cert / 14 / 112 / 7.4 / 3.2s | cert / 14 / 112 / 7.4 / 3.6s | cert / 12 / 96 / 4.0 / 23.7s | cert / 6 / 48 / 1.7 / 5.8s |
| closure | clean cr=1 fuzz 1 | cert / 14 / 14 / 7.4 / 0.5s | cert / 14 / 14 / 7.4 / 0.5s | cert / 12 / 12 / 3.6 / 2.9s | cert / 6 / 6 / 1.7 / 0.7s |
| closure | clean cr=0 fuzz 8 | cert / 4 / 32 / 8.8 / 1.1s | cert / 4 / 32 / 8.8 / 1.1s | cert / 1 / 8 / 4.0 / 3.4s | cert / 1 / 8 / 1.0 / 1.8s |
| dataflow | unclean cr=1 fuzz 8 | **cert** / 14 / 112 / 7.4 / 3.8s | **cert** / 14 / 112 / 7.4 / 3.6s | bug / 9 / 72 / 4.1 / 2.7s | **cert** / 6 / 48 / 1.7 / 3.0s |
| dataflow | unclean cr=1 fuzz 1 | **cert** / 14 / 14 / 7.4 / 0.6s | **cert** / 14 / 14 / 7.4 / 0.6s | bug / 9 / 9 / 4.1 / 0.4s | **cert** / 6 / 6 / 1.7 / 0.4s |
| dataflow | unclean cr=0 fuzz 8 | cert / 4 / 32 / 8.8 / 1.3s | cert / 4 / 32 / 8.8 / 1.3s | cert / 1 / 8 / 4.0 / 0.4s | cert / 1 / 8 / 1.0 / 0.9s |
| dataflow | clean cr=1 fuzz 8 | cert / 14 / 112 / 7.4 / 3.5s | cert / 14 / 112 / 7.4 / 3.5s | cert / 13 / 104 / 4.3 / 3.4s | cert / 6 / 48 / 1.7 / 3.0s |
| dataflow | clean cr=1 fuzz 1 | cert / 14 / 14 / 7.4 / 0.6s | cert / 14 / 14 / 7.4 / 0.6s | cert / 13 / 13 / 4.3 / 0.5s | cert / 6 / 6 / 1.7 / 0.4s |
| dataflow | clean cr=0 fuzz 8 | cert / 4 / 32 / 8.8 / 1.2s | cert / 4 / 32 / 8.8 / 1.2s | cert / 1 / 8 / 4.0 / 0.4s | cert / 1 / 8 / 1.0 / 0.9s |

Bold = the verdict is wrong (a certificate on the buggy variant). Replay fallbacks were 0 in
every cell. Replays per cell (fuzz 8, cr=1): closure L0/L1 640, L2 ≈7100–7200, L3 ≈1700;
dataflow L0/L1 640, L2 512–624, L3 680. Replays occur at L0/L1 too because the hand-written
specification's final-view derivation (`spec_view`) is always taken by counterfactual replay and
unioned with the recorded one, independently of the level.

### Observations

- **VERDICT DISAGREEMENT ACROSS LEVELS — false certificates on the buggy variant.** On *unclean*
  (the known unclean-election bug: one lost replication plus the leader's crash after it
  acknowledges) only **L2 finds the bug**; **L0, L1 and L3 certify it**, on both encodings, under
  both fuzz 1 and fuzz 8 (so this is not sampling noise: same hypothesis sequence every time).
  Confirmed independently of this harness: the repository's own
  `ldfi_finds_the_unclean_election_bug{,_dataflow}` tests fail with `LDFI_NO_PROVENANCE=1`
  (L1: certificate in 14 hypotheses) and with `LDFI_LEVEL=replay` (L3: certificate in 6).
  Diagnosis from the hypothesis traces (`level_ablation_isr_diagnose`, dataflow, fuzz 1):
  - The counterexample is `{omit(repl[0→1]), crash(replica@0 after client-ack)}`. At L2 the
    failure-free derivation is small (`hb[0→]#3`, `hb[1→]`, `hb[2→]`, `observed[0→]#2`), so the
    only crash that kills it on replica 0 is the one after its send #2, the client ack; the
    second hypothesis is already that crash, the third adds the omission, done in 9.
  - At L0/L1 the failure-free derivation names 11 deliveries including `repl[0→1]#0`,
    `repl[0→2]#1`, `client-ack#2`, `hb#3`. The hitting-set search prefers the crashes after
    `repl[0→1]` / `repl[0→2]` (each kills more of the clause) and never tries the weaker crash
    after the ack. Every hypothesis combining the omission with one of those earlier crashes is
    **vacuous** (the leader dies before acknowledging, so `PRE` fails and `goal_held = !pre ||
    post` is true); vacuous runs add no clause, the hitting sets exhaust, and the search
    certifies in 14. The coarser lineage lets a *stronger* crash shadow the weaker crash the bug
    needs, and vacuity is read as coverage.
  - At L3 the failure-free derivation is a single dependency, `observed[0→]#2` (the leader's own
    log output). Replay judges the leader's heartbeat `hb[0→]#3` unnecessary because removing it
    makes ZK's final view leaderless, which fails `PRE` and so leaves the goal *held*. The
    spec view is not monotone in the facts (a last-value view; cf. `history-hydrofrac-negation.md`)
    and the 1-minimal sufficient set under a `!pre || post` goal drops exactly the delivery whose
    loss the bug exploits. Omission is never considered (`observed` is not lossy): 6 hypotheses,
    all crashes, certificate.
- **L2 vs L1 hypothesis reduction.** 14 → 9 (bug, both encodings), 14 → 12 (closure clean),
  14 → 13 (dataflow clean), 4 → 1 (cr=0). Mean derivation size roughly halves (7.4 → 3.6–4.3;
  8.8 → 4.0 at cr=0). L0 and L1 are identical in every cell: the static slice buys nothing over
  the execution closure on this program, in either encoding.
- **L3 vs L2.** 9/12–13 → 6 hypotheses with derivations of 1.7 deliveries on average, but see
  above: on *unclean* the reduction is bought with a false certificate. On *clean* the certificates
  agree. On the closure encoding L2 is the slowest level (≈7000 replays per search, 24–36 s at
  fuzz 8) because the opaque step is replayed per record (`Prov::OPAQUE`); L3 replays ≈1700 and is
  3–4× faster than L2 there. On the dataflow encoding all levels are within a few seconds.
- **The 14/27 → 6/14 claim** (closure, L2 → L3, bug/certificate hypotheses) does **not**
  reproduce. Measured today, closure: L2 **9**/12, L3 6/6 — but the L3 "6" on the buggy variant is
  a certificate, not a counterexample, and the clean-variant certificate is 6 rather than 14. The
  dataflow L2 figure of 10/18 is now 9/13. The harness has changed since those numbers were
  taken (hand-written `witness_final` goal → `Spec` with `observe_sequenced(views).max()`; the
  client acknowledgements and views are now fault points), which is the likely cause of both the
  count changes and the L3 verdict flip; it was not re-measured on the old harness.
- cr=0 is a degenerate row: with no crashes the only faults are first-round omissions, and L2/L3
  certify from the failure-free run alone (1 hypothesis) because no omission kills the recorded
  derivation; L0/L1 need 4.
