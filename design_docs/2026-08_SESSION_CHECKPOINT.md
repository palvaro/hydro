# SESSION CHECKPOINT — the quorum→consensus ladder, rungs 0–3

2026-08. Written at the close of the session that built the ladder. Nothing
is in flight; everything below is implemented, test-verified, committed, and
pushed to `palvaro/hydro` branch `eventual_consistency` (HEAD `a2c33c94e8`).

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
- **Suite state**: hydro_std 59/59. hydro_lang sim (151/151) and raft
  (16/16) last ran before the ladder work — the ladder only added to
  hydro_std, but re-run cross-suite before the next big change.
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

## Known debts (documented, deliberate)

- One-outstanding-op / distinct-rounds are UNENFORCED caller contracts, and
  they are load-bearing for the ABD linearizability proof (ladder doc §3).
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
