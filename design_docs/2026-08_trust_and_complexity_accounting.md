# Trust and Complexity Accounting for Hydro Programs

2026-08

**Status:** design. Defines the measurement framework the research agenda
(`2026-08_research_agenda.md` §1) needs in order to say whether its goals are
being met, and the protocol for applying it to every consensus artifact in
this repo. Companion to `2026-08_nondet_vs_manual_proof.md` (the taxonomy of
trust tokens this doc quantifies over).

## 1. Why: the goals have no yardstick

The agenda's two goals are:

1. **Inference** — consistency labels on outputs derived by the compiler,
   zero consistency assertions in the protocol body.
2. **Residue localization** — irreducible obligations concentrated in single,
   named, greppable seams instead of smeared through a protocol.

Both are *comparative* claims — this factoring is better than that one — and
we currently evaluate them with adjectives plus three half-measures already
in the corpus:

- grep counts (LOC, `manual_proof!` sites, `nondet!` sites) in doc prose;
- `typed_consensus.rs`'s self-pin test (asserts "exactly 2
  `assert_has_consistency_of` in production code, inside these two named
  functions, with non-empty proof text" — a residue-localization check as a
  unit test, `typed_consensus.rs:2707`);
- `state_space_comparison.rs` (exhaustive-search tractability of
  typed_consensus components vs. the Raft monolith).

These are measures, but they mislead in specific, demonstrated ways:

**Counting rewards smearing.** Raw seam counts across the consensus
artifacts (protocol bodies only, tests excluded):

| artifact | body LOC | consistency asserts | algebra proofs |
|---|---|---|---|
| `raft.rs` | 1151 | **1** | 1 |
| `typed_consensus.rs` | 1339 | 3 | 15 |
| `paxos_ec.rs` | 1471 | 7 | 4 |

By count, Raft is the *least* trusting artifact. In reality its one
assertion is the worst in the repo: `raft.rs:1044` blesses the committed-log
output with a proof text bundling "election restriction, log-matching, and
majority-commit" — three whole-protocol invariants in one unchecked claim
whose verification requires reading essentially the entire 1151-line body.
A metric under which one enormous proof scores better than fifteen atomic
ones is anti-correlated with goal 2. **The unit of account must be the scope
of each obligation, not the number of obligation tokens.**

**Counts miss obligations with no token.** Three kinds of trust have zero
grep footprint:

- *Forwarded obligations*: `leader_merge`'s protocol bodies contain zero
  `nondet!` invocations — because the authored-order obligation is a `NonDet`
  **parameter** discharged by callers. `paxos.rs` forwards 8 such parameters;
  the entire quorum ladder forwards 0. Same obligations, different visibility.
- *Caller contracts in prose*: ABD's one-outstanding-op rule, synod's
  distinct-rounds rule, multi_paxos's globally-distinct-rounds rule. Two of
  these are sim-witnessed as load-bearing (violating them mechanically breaks
  agreement); the third has no witness at all. None appear in any count.
- *Convention seals*: the `#[doc(hidden)]` certificate constructors, and
  multi_paxos's "the learning channel is trusted" decision. Pure prose.

**Counts conflate choices that matter with choices that don't.** A `nondet!`
guarding a batch boundary whose effect is provably erased downstream is a
different object from a `nondet!` that authors the log order the system
commits to. Today both count as 1.

This doc defines the accounting that fixes these three failures, states
crisp goal-attainment criteria in its terms, and records first measurements.

**Terminology, defined once.** A **seam** is any single site where the
compiler accepts an unchecked human claim (the full taxonomy is §2). A
**mint** is a seam that *creates* a guarantee others inherit — either a
consistency label (`assert_has_consistency_of`) or a sealed certificate
value (the `quorum.rs` constructors). An obligation is **load-bearing** for
a guarantee when falsifying it demonstrably breaks that guarantee (the
red-test discipline makes this a mechanical fact, not a judgment).

## 2. The objects of measurement

### 2.1 Claims

Everything is accounted *per output guarantee*, not per file. For a given
spec (consensus: agreement / prefix-consistency; Ω-conditional progress;
label claims on outputs) each implementation exposes a set of **claims**:

- **Typed claims**: a consistency/ordering label on an output edge
  (e.g. "the committed log at every member is `TotalOrder, EC`").
- **Untyped claims**: properties no label can carry (agreement,
  linearizability, non-blockingness), enforced by protocol and attacked by
  the simulator.

The spec also declares its **freedoms**: outcomes it deliberately leaves
open (which value wins a consensus race, which majority answers a covering
read, the order a leader authors). Freedoms matter in §4: nondeterminism
that lands inside a declared freedom is not a defect.

### 2.2 Seams (the full taxonomy)

Refining `2026-08_nondet_vs_manual_proof.md` into accounting categories:

| kind | token | what accepting it means |
|---|---|---|
| S1 consistency mint | `assert_has_consistency_of(manual_proof!)` | a label asserted; if false, replicas diverge and nothing records it (the `unsafe` of Hydro) |
| S2 algebra proof | `commutative =` / `idempotent = manual_proof!` | an unpinned choice claimed unable to affect the result |
| S3 assumer | `assume_ordering` / `assume_retries` + `nondet!` | a label strengthened; the choice *does* matter and is on the books |
| S4 introducer | `batch`/`snapshot`/`sample_every`/`timeout` + `nondet!` | environment timing enters as an explicit choice |
| S5 forwarded obligation | `NonDet` parameter in a signature | an S3/S4 obligation published to callers |
| S6 caller contract | prose in module docs | a usage rule the dataflow does not enforce |
| S7 convention seal | `#[doc(hidden)]` constructor; "authority is the channel" | unforgeability/authenticity by agreement, not mechanism |
| S8 trusted-base import | `*_trusted` sites inside `hydro_lang` (`broadcast_closed` etc.) | shared audited capital, amortized across all users |

S6 and S7 are first-class citizens of the accounting *because* they have no
token: any metric that omits them reports the ladder's multi_paxos as
strictly safer than it is (its single largest soft spot, the learning
channel's authority, is an S7).

### 2.3 The assumption graph

Seams are not independent; they justify each other, and risk composes along
the edges:

- an S4 batch nondet is harmless *because* a downstream S2 fold proof says
  order cannot matter;
- that S2 proof ("max by `Ts` is commutative — writers never tie") is true
  *because* of an S6 contract (one outstanding op per client) enforced
  nowhere;
- the S6 contract's violation is *witnessed* by a red test — or it isn't.

The **assumption graph** has seams and contracts as nodes and
"justified-by" edges. A claim's real trust bill is the transitive closure of
the seams its derivation touches. This is the structure raw counts flatten:
ABD's fold proof is one line of text whose truth lives partly at another
location and partly in prose — and the repo's own history validates the
point, because exactly that dependency chain is what the
`abd_violating_one_outstanding_op_forges_timestamps` red test falsifies.

## 3. The two scope measures (the core of the accounting)

For every seam `s`, two regions of the compile-time dataflow graph — which
Hydro, uniquely, has in hand statically:

**Antecedent scope A(s) — what must be read to *check* the claim.** The
smallest program region whose contents determine whether the seam's
proposition is true: the backward slice from the seam to the nearest edges
whose relevant property is mechanically established, **plus every contract
(S6) and seal (S7) the proposition's truth cites**. This is "how much code
the proof blesses." Until the IR pass exists (§10), classify ordinally:

- **A0** — the combiner closure alone (e.g. "set insert is commutative").
- **A1** — one combinator/slice body (the quorum mint's fired-once argument:
  check the one `sliced!` block).
- **A2** — one phase/subgraph within one location (the acceptor's
  batch-serialization argument: the acceptor slice plus its two input
  streams' properties).
- **A3** — spans locations (Raft's committed-log assert: election +
  replication + commit, i.e. the protocol).
- **A4** — spans locations *and* cites unenforced contracts (ABD's
  commutativity proof: the client's stamping logic + the one-outstanding-op
  contract; multi_paxos's splice-fold premise: globally-distinct rounds).

The bucket ordering is a risk ordering: A0 is checkable in one sitting by
reading one closure; A4 cannot be checked by reading *any* amount of code,
because part of the proposition is a promise about callers. Note that A4
seams can have one-line proof *text* — textual size and antecedent scope are
uncorrelated, which is precisely why counting tokens fails.

**Consequent scope C(s) — what breaks if the claim is false.** The forward
slice from the seam's edge until each path reaches an output or a
**re-establishing operator** — a point that makes the property true again
regardless of upstream damage. Consistency has genuine re-establishers (an
EC-earning broadcast mints its label from the network policy, absorbing
upstream label damage — "EC is re-earnable," ladder doc §3a); determinism
claims generally do not. Report C(s) as the set of spec claims whose
derivation cone contains `s`: the seam's blast list.

**Per-claim bill of materials.** Inverting C gives the headline artifact:
for each spec claim, the exact set of seams in its derivation, each tagged
with its A-bucket and evidence grade (§5). This is the "what must be true
and who checks it" table — the mechanical version of a trust ledger, per
guarantee, comparable across implementations of the same spec.

## 4. Nondeterminism accounting: resolved is a spectrum, not a bit

`2026-08_nondet_vs_manual_proof.md` §5 established that "locally resolved"
is prose, not a mechanism. The accounting therefore grades every S3/S4/S5
seam by **what actually backs the resolution claim**:

- **N0 — erased, mechanically confirmed.** The choice provably does not
  reach any output, *and an oracle confirms it*: exhaustive exploration plus
  a test asserting identical outputs across all explored schedules (static
  RB's `count == 1` + exact-delivery assert is the exemplar). Note the
  standing caveat: nearly every sim test today runs
  `skip_consistency_assertions`, so label-level determinism claims currently
  have **no** runtime oracle — N0 status for most seams is blocked on the
  M-check (agenda §5), and the accounting should say so rather than round up.
- **N1 — erased per prose.** The guard's doc comment cites downstream
  structure (a dedup, a fired-once set, a commutative fold) that the type
  system neither knows nor checks. The erasure is *conditional on S2 proofs*
  — an assumption-graph edge, not a discharge. Most of the ladder's batch
  nondets are N1.
- **N2 — escapes, spec-sanctioned.** The choice is visible in outputs and
  lands inside a declared spec freedom (the leader's authored order; which
  covering forms; which value wins). N2 is not risk; it is the spec's own
  slack — but every N2 must be *matched* to a named freedom. An unmatched
  escape is not N2.
- **N3 — escapes, unaccounted.** Neither erased nor matched to a freedom.
  Required to be zero; any N3 is a finding.

Two consequences worth making explicit. First, **forwarding (S5) is
ownership, not resolution**: `paxos.rs`'s 8 forwarded parameters and the
ladder's 0 are the *same obligations* at different visibility — the
forwarded form is arguably more honest (it is in the signature; the ladder's
equivalent is S6 prose), and the accounting must not reward hiding. Second,
**erased nondets still cost**: every N0/N1 seam is a fork dimension for the
simulator and a coordination point at runtime (`sliced` cuts are waiting,
per `2026-08_slices_finalize_not_combine.md`). They belong on the complexity
axis (§6.2) even when they carry no risk.

## 5. Evidence grades: who checks each seam

Every seam and contract gets the strongest mechanical check that actually
touches it:

- **E4 — type-refused alternative.** The compiler *forces* the seam to
  exist: deleting it does not compile (a consumed consistency assert; a
  fold's commutativity annotation on an unordered input). The obligation
  cannot silently vanish.
- **E3 — adversarially exercised.** The simulator actively explores against
  the claim: it permutes fold inputs *despite* commutativity proofs
  (`sim/runtime.rs:1705`), forks batch boundaries, injects crashes. E3 is
  only as strong as the oracle downstream of it — permutation under
  `skip_consistency_assertions` with no output assertion checks nothing.
  Effective grade = min(seam's grade, strength of the tests covering its
  blast list).
- **E2 — red-tested.** A deliberately falsified variant exists and the
  search witnesses observable failure (the house red/green style). This is
  the only grade that demonstrates an obligation is load-bearing rather
  than decorative.
- **E1 — green-tested only.** Consistent with explored executions;
  sub-graded exhaustive (E1e) vs. fuzz (E1f).
- **E0 — prose only.** Nothing mechanical touches it. Every S7 seal in the
  repo is E0 today; so is multi_paxos's globally-distinct-rounds contract.

The grade vocabulary makes the earlier "three ledgers" conversation
mechanical: goal-level risk is concentrated exactly in the high-C, high-A,
low-E corner, and the accounting's job is to make that corner visible and
small.

## 6. The metrics

Deliberately a profile, not a single scalar — a composite index would invite
gaming and hide exactly the structure we built the accounting to expose.
Risk and complexity are **separate axes**: a program can be large and
low-trust (the ladder) or small and high-trust (Raft), and conflating the
axes re-creates the LOC fallacy.

### 6.1 Risk axis (per spec claim, then aggregated)

1. **Inference ratio** — fraction of the spec's *typed* claims whose bill of
   materials contains zero local S1 seams (S8 imports allowed, listed).
   Goal 1's dial.
2. **Worst gap** — max A-bucket over all local S1/S2 seams; and the **gap
   distribution** (how many seams at each bucket). Goal 2's dial. Raft's
   worst gap is A3 with one seam; that single cell captures what the seam
   *count* inverted.
3. **Trusted surface** — total antecedent scope, deduplicated, as a fraction
   of protocol operators: how much of the program a skeptic must read as
   prose-backed rather than compiler-backed.
4. **Contract ledger** — count of S6/S7 obligations, each with its evidence
   grade; headline sub-metric: fraction at E2+ (witnessed load-bearing).
5. **Nondet census** — N0/N1/N2/N3 counts, N2s matched to named freedoms,
   N3 required zero.
6. **Red coverage** — fraction of all seams+contracts whose falsification
   has a witnessing test (E2+). §8 mechanizes this.
7. **Amortization split** — local seams vs. S8 imports. Shared mints
   (`quorum.rs`, `broadcast_closed`) are audited once and reused; a local
   seam is paid per-program. The split is what should make a
   mint-and-compose factoring measurably cheaper than a monolith — and the
   accounting must also police the laundering move (promoting a dubious
   local seam into "trusted base" to get it off the books requires the
   promotion review the ladder docs already demand for `quorum.rs`).

### 6.2 Complexity axis (spec-independent)

1. **Graph size** — operators, edges, locations, message types.
2. **Kernel mass** — count and total operator size of order-sensitive
   `sliced!` blocks: the region where inference is structurally unavailable
   (the determination kernels). The un-inferable core, measured.
3. **Cut count** — batch/snapshot hooks: simultaneously the coordination
   points (each cut is waiting) and the simulator's fork dimensions.
4. **Cycle count** — `forward_ref`s.
5. **Behavioral volume** — executions explored under a standardized driver
   at fixed scale (n=3, one command, exhaustive where it terminates,
   decision-point count where it does not). `state_space_comparison.rs` is
   the precedent; static RB's "exactly 1 execution" is the floor. This is
   the one complexity metric that is already fully mechanical.

Performance (throughput/latency) is a third axis, out of scope here — the
benches exist (`paxos_bench`, `consensus_bench`) and are already principled.
A 2026-08 audit nevertheless exposed a complexity/performance coupling worth
recording: the ladder's `state.zip(batch).map(...)` + reassignment/clone style
moves growing state through the graph every tick, whereas Raft's `by_mut`
style mutates it in place. On the three-node/100-client consensus bench the
old multi-Paxos path managed about 1,200 req/s while p50 rose from roughly
12 to 97 ms. The completed refactor converts the acceptor, leader, wrapper,
and quorum hot state to `by_mut`, makes splice-state snapshots structurally
shared, and carries accepted proposal payloads through the quorum certificate
instead of retaining an unbounded proposal join. On the same three-node,
100-client localhost bench, multi-Paxos now sustains a steady median of about
22,400 req/s with p50 flat at roughly 4.1–4.4 ms; the same-day Raft baseline
is about 63,200 req/s at roughly 1.9 ms. The old zip/map/reassign discipline
therefore had a measured asymptotic cost, and the remaining 2.8× constant
factor is consistent with the ladder's extra quorum/echo traffic rather than
history replay. The benchmark's pinned leader uses a two-second election
period: the former 200 ms period mistook saturated progress for a stall and
injected redo elections, which was a benchmark-driver artifact rather than a
consensus cost.

## 7. Goal-attainment criteria, stated so they can fail

The agenda's goals, restated as falsifiable conditions on the metrics:

- **Goal 1 (inference) is met for a claim** iff its bill of materials
  contains zero local S1 seams. **Met for a program** iff true of every
  typed claim in its spec. *Approach* is the inference ratio rising across
  the portfolio table's rows.
- **Goal 2 (localization) is met for a program** iff every remaining local
  seam has antecedent A1 or tighter, antecedent regions are pairwise
  disjoint, and every A4 dependency (contract-citing proof) has its contract
  at E2+ — i.e. what a proof cannot see, a red test must witness.
- **Honesty floor (both goals)**: N3 = 0, and every S6/S7 at E0 is listed in
  the artifact's own docs as unwitnessed. Trust may be unavoidable;
  unacknowledged trust is a defect.

By these criteria, today: no consensus artifact meets goal 1 outright
(multi_paxos comes closest — zero local S1 — but its untyped agreement claim
rests on E0 items); none meets goal 2 (every quorum-family program has A4
seams, and multi_paxos's contracts sit at E0). That the criteria return
"not yet, and here is the exact deficit" for our best artifact is evidence
they measure something real.

## 8. Trust mutation testing: mechanizing "load-bearing"

The red/green house style generalizes into a coverage discipline. For each
seam kind, a mutation operator:

| seam | mutation |
|---|---|
| S1 consistency mint | delete the assert (if it still compiles, the label was never consumed — **dead trust**, a finding by itself) |
| S2 algebra proof | swap the proof door for the assume door (`assume_ordering + nondet!`) and let the sim shuffle in earnest |
| S3/S4 | widen the admitted choice (coarser batches, reordered release) |
| S6 contract | a violating driver (exactly the existing `*_violating_*` tests) |
| threshold parameters | off-by-one (the sub-majority reds, generalized) |
| S7 seal | forge at the constructor / inject on the trusted channel |

Run each mutant under the standard driver; require the search to find an
observable violation. Three outcomes, all informative: **refused** (does not
compile — the obligation is compiler-enforced, E4), **killed** (the search
witnesses failure — load-bearing, E2), **survived** (either the obligation
is decorative and should be deleted, or the test oracle is too weak to see
its violation — both findings). **Red coverage** = killed+refused over
total. The five hand-written red tests in the ladder are this table executed
manually for five cells; the mutation harness is the same idea run to
completion, and it converts "load-bearing" from an adjective in doc prose
into a per-seam bit computed by the toolchain.

## 9. First measurements (Tier 0: scripted census + manual classification)

Protocol bodies only (first `#[cfg(test)]` split; `raft.rs` and
`leader_merge.rs` splits hand-corrected). Columns: S1 = consistency
asserts, S2 = algebra proofs, S3/S4 = assumers + introducer nondets,
S5 = forwarded `NonDet` params, K = `sliced!` kernels, cyc = `forward_ref`s.

| artifact | body LOC | S1 | S2 | S3/S4 | S5 | K | cyc |
|---|---|---|---|---|---|---|---|
| `raft.rs` | 1151 | 1 | 1 | 2 + 6 | 2 | 3 | 3 |
| `paxos.rs` | 891 | 0 | 3 | 1 + 26 | 8 | 2 | 13 |
| `paxos_ec.rs` | 1471 | 7 | 4 | 0 + 5 | 0 | 8 | 12 |
| `typed_consensus.rs` | 1339 | 3 | 15 | 5 + 22 | 0 | 9 | 6 |
| `broadcast_transcript_consensus.rs` | 1326 | 5 | 1 | 2 + 5 | 1 | 11 | 9 |
| `leader_merge.rs` | 260 | 0 | 0 | 0 | 3 | 0 | 0 |
| `abd.rs` | 333 | 0 | 2 | 0 + 3 | 0 | 1 | 3 |
| `synod.rs` | 288 | 0 | 2 | 0 + 2 | 0 | 1 | 2 |
| `multi_paxos.rs` | 607 | 0 | 4 | 0 + 5 | 0 | 2 | 6 |
| shared: `quorum.rs` + `epoch_splice.rs` (S8 for the ladder) | 539 | 2 | 4 | 0 + 3 | 0 | 3 | 0 |

Deep classification, done so far only for the artifacts whose seams this
session has actually read (a fairness rule this doc adopts: A-buckets and
evidence grades are assigned only with file/line evidence in hand, so a
reader can audit the judgment):

- **`raft.rs`**: 1 × S1 at **A3** (`raft.rs:1044`; the proof text names three
  protocol-wide invariants), C = every downstream consumer of the committed
  log. Evidence: E1f green (prefix-consistency + progress fuzz) — no red
  test attacks the assert itself. The single highest-risk seam measured:
  worst gap, widest blast, weakest audit.
- **`abd.rs`**: 2 × S2, of which the register fold's commutativity is **A4**
  (cites `Ts` stamping at the client location + the one-outstanding-op S6);
  that S6 is **E2** (red-tested). Batch/snapshot nondets N1. Zero S1: its
  typed claim (register EC) is inferred — goal 1 holds for the typed claim;
  linearizability is untyped, E1f + E2 on two contracts.
- **`synod.rs`**: 2 × S2 at A0–A1; S6 distinct-rounds at **E2**;
  the imported mints' intersection premise attacked by the sub-majority red
  (**E2**, the "first mechanical audit of a mint"). Cleanest profile in the
  table.
- **`multi_paxos.rs`**: 4 × S2 (A0–A1, except the splice-fold premise
  inherited via `epoch_splice`, which is **A4** citing globally-distinct
  rounds — **E0**, no red test); learning-channel S7 at **E0**; adopt-highest
  and sub-majority at **E2**. Inference ratio 1 on typed claims; the E0
  cells are its exact deficit (and were called out in the sober assessment
  that prompted this doc — the accounting reproduces that assessment
  mechanically, which is the point).
- **`paxos.rs`**: zero S1 because it claims no output label at all — risk
  lives in 8 forwarded S5 obligations and 26 local nondets. An honest but
  unlabeled artifact: the accounting distinguishes "inferred" from
  "never claimed," which the portfolio table's Consistency column already
  does informally.
- **`paxos_ec.rs`, `typed_consensus.rs`, `broadcast_transcript_consensus.rs`**:
  A-buckets **TBD pending a reading pass** — 15 S1 seams to classify with
  evidence in hand. By count they look worse than Raft; the entire
  prediction of this framework is that their gap distributions are better.
  Confirming or refuting that is the first real test of the metric.

**Measurement protocol for the full pass** (next session): for each S1/S2
seam, record file:line, proposition (quote the proof text), A-bucket with
the citation that justifies it, blast list, evidence grade with the test
name that grants it; for each artifact, the S6/S7 ledger from module docs;
then the per-claim bills of materials and §6.1 headlines. Estimated one
session for the three unread artifacts.

## 10. Mechanization tiers

- **Tier 0 (exists, above)**: scripted census + evidence-cited manual
  classification. Sufficient for the cross-artifact comparison the agenda
  needs this quarter; the classification is auditable but human.
- **Tier 1 — IR provenance pass**: the compile-time graph already exists
  (the IR snapshots in `hydro_test` prove it is walkable). Annotate every
  edge's label with its **provenance set** (which S1/S8 sites its derivation
  traversed) → inference ratio and blast lists become compiler outputs.
  Backward reachability from each seam to mechanically-labeled ancestors →
  A-buckets computed, not judged (contracts still enter by declaration —
  give S6 a token: a `contract!(...)` marker with a name, so citing one in a
  `manual_proof!` becomes greppable structure instead of prose). This is the
  same afternoon-sized IR-pass shape as the ladder doc §3d's capability-set
  analysis, and the two passes share the graph walk.
- **Tier 2 — sim integration**: the M-check (agenda §5) upgrades N1→N0
  claims from prose to oracle; taint propagation through explored executions
  classifies escapes (N2/N3) empirically; the §8 mutation harness computes
  red coverage. Tier 2 is where "the sim distrusts the human witnesses"
  stops being a design principle and becomes a coverage report.

## 11. Limits, and one warning

- The metrics are proxies. A0 proofs can be wrong; E2 shows falsifiability
  under one driver, not validity; exhaustive-at-n=3 is not n.
- Antecedent buckets are judgment until Tier 1 — hence the evidence-citation
  rule in §9, which keeps the judgment auditable.
- **Goodhart**: the moment these numbers matter, factoring decisions will
  chase them. Two known gaming vectors are priced in — laundering local
  seams into "trusted base" (blocked by the promotion-review rule in §6.1),
  and converting greppable seams into prose contracts (blocked by counting
  S6/S7 as seams with the *worst* default grade). A third is inherent:
  splitting one A3 proof into many A1 proofs whose *conjunction* is not
  audited. The per-claim bill of materials is the defense — the claim still
  lists every piece — but composition of many small trusted claims is a real
  residual risk the accounting exposes without eliminating.
- The measured artifacts and this framework share an author (including
  agent sessions). The citation rules exist so the numbers survive a
  skeptical human audit; they are not a substitute for one.



