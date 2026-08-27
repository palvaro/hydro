# SESSION CHECKPOINT — the quorum→consensus ladder, rungs 0–3

2026-08. Written at the close of the session that built the ladder. Nothing
is in flight; everything below is implemented, test-verified, committed, and
pushed to `palvaro/hydro` branch `eventual_consistency`.

**Read `2026-08_quorum_certificates.md` first** — it is the ladder's home
and carries all the session's determinations. This file is only the state
snapshot and resume pointers.

## Landed and verified this session

- **Rung 0/0.5, the mints** (`hydro_std/src/ec_inference_demos/quorum.rs`):
  `quorum` → sealed `Durable` (distinct-attestor counting, fired-once);
  `covering_quorum` → sealed `Covering` (count + max-by-`Ts` in one fold).
  `Ts` (timestamp = ballot) lives here. Durable's mint asserts EC-through-
  the-slice; Covering's deliberately does NOT (content is schedule-authored)
  — the asymmetry is a finding (ladder doc §3b/§4).
- **Rung 1, URB** (`uniform_broadcast.rs`): five-line client of RB's
  exported echo stream (`reliable_broadcast_closed_with_echoes`).
  Uniformity red/green pair under crashable sender + crashable member.
- **Rung 2, ABD** (`abd.rs`): cluster of clients; replica register is a
  top-level max-lattice fold, **EC inferred and compiler-pinned** (explicit
  type annotation, zero consistency assertions in the file); ack gate on a
  monotone register snapshot. Tests: smoke, cross-client real-time order,
  progress + latest-read under replica crash, ts-monotone reads under
  client crash (the incomplete-write case).
- **Rung 3, synod** (`synod.rs`): phase 1 = Covering mint, phase 2 =
  Durable mint, ballot = Ts, all unchanged; the one new thing is
  adopt-highest. The acceptor is a tick-serialized refusal kernel, no EC
  label, CONFIRMING the acceptor-inverts-ABD hypothesis. Tests: smoke;
  agreement under concurrently dueling proposers ±acceptor crash; RED
  no-adoption variant (two values chosen); RED sub-majority quorum (two
  values chosen — the first mechanical audit of a mint's `manual_proof!`);
  progress under the Ω discipline.
- **Suite state**: hydro_std 61/61 (incl. the two contract red tests);
  hydro_lang sim 151/151 and raft 16/16 re-verified at session close.
- **Portfolio table** (research agenda §3): gained URB, ABD, and synod
  rows; RB's row records its uniformity hole. The progress column across
  leader_merge ✗ / member-leader ✗ / ABD ✓ (no leader) / synod ✓ (Ω only) /
  Raft ✓ is the "succession is the cost of a log" + FLP-tax story, all
  cells mechanical.

## The stack (pop in roughly this order)

1. **Rung 4 — multi-decree**: epoch-keyed log of synod into the existing M1
   splice reader (`epoch_splice.rs`); in-protocol ballot management and the
   learning-dissemination EC story (chosen certificates are stable facts —
   broadcast-shaped, where EC legitimately reappears on consensus output).
2. **Static FT refutation from location types** (ladder doc §3c): capability
   -set cardinality as a compile-time necessary condition. Tier 1 is an
   afternoon-sized IR pass; calibration = reproduce the table's ✗ cells and
   none of its ✓ cells (member-leader is the discriminating case).
3. **Linearizability history checker at quiescence** (ladder doc §3, rung-2
   proof-sketch bullet): Wing–Gong/Porcupine over per-client op intervals —
   M-check-family sim oracle.
4. Pre-existing agenda items: Tier-1 restructure (completeness premise into
   hydro_lang), M-check, `live()` oracle substrate, `collect_quorum`
   cleanup (distinctness/re-fire/TODO-proof issues, ladder doc §4),
   `quorum_round` plumbing helper, dynamic-membership leader_merge variant,
   `reliable_broadcast_live` crash test.

## For the next agent: picking up rung 4

Read, in this order:
1. This file (state + stack).
2. `2026-08_quorum_certificates.md` — the ladder's home. Everything you
   need is in §3 (the rung-4 entry and the DAG), §3a (determination/
   commitment), §3b (the joint cuts you will consume), §3c (the Herlihy
   dictionary — rung 4 IS the universal construction; learning is where EC
   re-enters), §3d (deferred static-FT idea, do not build), §4 (findings,
   incl. `collect_quorum`'s issues — do NOT build on it).
3. `2026-08_epoch_keyed_consensus_splice.md` and
   `2026-08_slices_finalize_not_combine.md` — the splice invariant and the
   maxim; then `hydro_std/src/ec_inference_demos/epoch_splice.rs` (the M1
   splice reader — built, tested, consumed by nothing yet: rung 4 is its
   first consumer).
4. The joints you compose: `quorum.rs` (both mints + `Ts` = ballot),
   `synod.rs` (rung 4 = an epoch-keyed log of this; note the acceptor's
   batch-serialization safety argument in its module docs), `abd.rs` (the
   covering-consumption pattern), `reliable_broadcast.rs` (learning
   dissemination — chosen certificates are stable facts; `Durable` is
   deliberately not `Serialize`, so shipping certificates to learners
   forces the transportable-certificate design decision).
5. `2026-08_crash_injection_sim.md` §3 — the driver discipline for
   progress tests (Ω as input, per-round quiesce barriers).

Working rules that will save you a day: fuzz first, exhaustive only when
small (the gossip-hang lesson); run tests under a watchdog
(`perl -e 'alarm N; exec @ARGV' cargo test ...`); never name host generics
inside `q!` (inference carries them); `sliced!` cannot parse multi-line
turbofish; q! closures cannot capture host `bool`s (branch outside);
red/green pairs are the house test style — every claimed guarantee gets a
deliberately broken variant the search must refute. Update the portfolio
table row and the ladder doc's rung-4 entry when done; commit conventional
style; push to origin eventual_consistency.

## Known debts (documented, deliberate)

- One-outstanding-op / distinct-rounds caller contracts are now
  SIM-WITNESSED as load-bearing (red tests in abd.rs / synod.rs); still
  structurally unenforced.
- `Durable`/`Covering` unforgeability is by `#[doc(hidden)]` convention
  (staged code cannot see module privacy) until the mints are promoted into
  `hydro_lang`.
- Certificates are not `Serialize` on purpose (wire-crossing = forgeable);
  transportable certificates need their own design.
- `urb_delivers_to_all` is the suite's slowest test (~140 s, exhaustive).
- `hydro_test/src/cluster/snapshots/typed_consensus_ir.snap.new` remains
  unreviewed and uncommitted in the working copy.

## Earlier in this same working period (previous checkpoint's content)

Crash-fault injection in the sim, the gossip behavior tests, and two sim
bug fixes (multiset_delta `Hash` requirement → BTreeSet state; the
TopLevelFoldHook empty-batch scan-kill) — all landed, committed, and
described in `2026-08_crash_injection_sim.md` and the git history
(`feb1da6e4a`..`a2c33c94e8` spans the ladder; earlier commits cover the
gossip/crash work).
