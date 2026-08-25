# SESSION CHECKPOINT — crash injection / dynamic membership / demo portfolio

2026-08. Updated at the end of the follow-up session that verified the gossip
behavior tests. Nothing is in flight; everything below is implemented,
test-verified, and synced to this working copy.

## Landed and verified (all suites green at time of sync)

- **Crash-fault injection in the simulator** (`hydro_lang/src/sim/`):
  staged sends + `CrashHook` forking crash-vs-flush at send boundaries only,
  per-recipient prefix cuts, halted locations skipped but kept alive.
  APIs: `with_crashable_process(&p)`, `with_crashable_cluster(&c, F)`
  (untargeted; budget shared across members). A scripted-crash API was built
  and then deliberately removed (wrong quantifier). Crash-free flows compile
  byte-identically. Full description: `2026-08_crash_injection_sim.md`.
- **Demo suite** (all passing):
  - `sim_crashable_sender_explores_partial_broadcasts` (hydro_lang, exhaustive)
  - `broadcast_closed_violates_agreement_under_sender_crash` /
    `reliable_broadcast_closed_agreement_under_sender_crash` (hydro_std, exhaustive)
  - `leader_merge_dissemination_hole_leader_crash_diverges_replicas`,
    `leader_merge_plus_reliable_broadcast_agrees_but_blocks_at_f1`,
    `member_leader_single_crash_can_block_progress` (hydro_std)
  - `any_single_crash_cannot_block_progress` (hydro_test raft, fuzz; crash-
    agnostic rotating driver with per-round quiesce barriers — un-barriered
    drivers livelock, see crash doc §3)
  - `fan_out_ec_mint_refuted_under_source_crash` (hydro_std, exhaustive):
    premise 2 of the fan_out EC mint refuted under source-member crash.
- **CRDT gossip behavior tests** (formerly in flight, now verified —
  `hydro_std/src/ec_inference_demos/crdt_gossip.rs`):
  - `g_set_gossip_converges_static` (fuzz, n=2): every member converges to the
    union.
  - `g_set_gossip_live_late_joiner_converges` (fuzz, n=3, dynamic membership).
  - `g_set_gossip_survivors_converge_under_member_crash` (fuzz 8192, n=3,
    `with_crashable_cluster(_, 1)`, bounded pump rounds, survivor-agnostic
    convergence check) — fills the gossip safety cell in the portfolio table.
  - `g_set_gossip_live_n1_own_element_reaches_own_state` (exhaustive, n=1):
    minimal regression net for the fold-hook bug below.
  - `fan_out_live_self_delivery_under_dynamic_membership` (fan_out.rs,
    exhaustive, n=1): self-delivery is not masked by peers' echoes.
  - Fuzz, not exhaustive, for the n≥2 gossip tests: the snapshot hook inside
    the `sliced!` pump forks on every state change and the echo cycle keeps
    re-offering, so exhaustive search does not terminate even at n=2
    (verified >5 min; this — not a livelock — was the previous session's
    "hang").
- **Two bugs found and fixed while verifying the gossip tests:**
  1. **Sim codegen required `Hash` payloads through live fan-out.** A
     top-level join compiles to `join_multiset -> multiset_delta()`, whose
     delta map is keyed by the item — so fanning out a `HashSet` (not `Hash`)
     failed to compile at sim-build time, invisibly until the first test run.
     Gossip state is now a `BTreeSet` (which is `Hash`); rationale in the
     module doc.
  2. **`TopLevelFoldHook` empty batches permanently killed the fold**
     (`hydro_lang/src/sim/runtime.rs` + `compile/ir/mod.rs`). The hook sent
     `vec![]` on trivial (empty) decisions; the generated `scan` wrapper
     returned `None` for an empty batch, which `scan` interprets as
     terminating the accumulator — every later element silently dropped.
     Symptom: a gossip member never absorbed its own element if its fold hook
     was serviced before its input arrived; empty releases are unlogged, so
     traces looked clean. Fix: the hook no longer sends empty batches, and the
     generated wrapper now panics loudly if one ever arrives. Diagnosed via an
     n=1 exhaustive repro plus temporary quiescence-state instrumentation in
     `sim/compiled.rs` (since removed).
- **Docs** (all in design_docs/): `2026-08_crash_injection_sim.md`,
  `2026-08_completeness_vs_consistency.md`, the rewritten
  `2026-08_research_agenda.md` (portfolio table in §2; gossip safety cell now
  green), resolution addendum in `2026-08_membership_hook_blowup_findings.md`.
- Verified at last full run: hydro_lang sim suite 151/151 (`--features sim`),
  hydro_std 45/45, hydro_test raft 16/16.

## Pending (user-requested, not started)

- Dynamic-membership leader_merge variant + tests.
- Portfolio-table blank cells: `reliable_broadcast_live` crash test;
  member-leader agreement assertion.
- The Tier-1 restructure (agenda §4) remains the headline next work item.
- `hydro_test/src/cluster/snapshots/typed_consensus_ir.snap.new` is an
  unreviewed insta snapshot sitting in the working copy — review and accept
  (or delete) before it goes stale.
