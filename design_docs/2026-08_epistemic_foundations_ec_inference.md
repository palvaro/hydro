# Epistemic Foundations for EC Inference: Knowledge Theory, the Fan-Out Rule, and the Orchestrated-Membership Post-Mortem

2026-08

**Status:** brainstorm record + prototype landed (`hydro_std::fan_out`, commit
`391c6df8e6`) + post-mortem of the orchestrated-membership attempt
(`756c3bee90`). Sources: Halpern & Moses, *Knowledge and Common Knowledge in a
Distributed Environment* (JACM 37(3), 1990) ["JACM"]; Halpern, *Using Reasoning
About Knowledge to Analyze Distributed Systems* (Ann. Rev. Comput. Sci. 2, 1987)
["Survey"].

## 1. The correspondence: EC is eventual common knowledge

Halpern & Moses define a hierarchy of group-knowledge states and pair each with
the strongest coordination it supports. The two facts that matter for Hydro:

- **Common knowledge C** (everyone knows, everyone knows that everyone knows,
  … ad infinitum) is required for *simultaneous* coordination and is
  **unattainable** in any practical system (JACM Thm 5: unreliable channels
  cannot create it; Thm 8: even temporal imprecision alone forbids it).
- **Eventual common knowledge C^◇**, the greatest fixed point of
  `X ≡ E^◇(φ ∧ X)` ("everyone will eventually have known both φ and this very
  fact"), is exactly what *eventually-coordinated* actions require and support
  (JACM §11) — and it **is attainable**, iff every message is eventually
  delivered (reliable/eventual delivery). Over unguaranteed channels even
  eventual coordination is impossible (JACM Prop 10).

Hydro's `EventualConsistency` label — "every live member eventually converges
to the same value" — is the coordination property whose epistemic
characterization is C^◇, with two precisifications:

1. **Indexical group.** It is C^◇ *among the live members* (the group N varies
   by run/point) — the same move JACM makes for C_N among nonfaulty processors
   in Byzantine agreement (Survey §6). `fail_stop`'s "consistency guarantees
   only apply to live members" (networking/mod.rs) encodes exactly this.
2. **Stable facts.** The C^◇ machinery works for facts that, once true, stay
   true. "Element e ∈ collection" on a grow-only collection is stable;
   snapshot-valued facts are not. Stable facts are the epistemic twin of
   monotone collections — this is *why* EC propagates through monotone
   operators and mechanically dies at `batch`/`snapshot`/`sample_every`.

Immediate corollaries that ground existing design decisions as theorems rather
than choices:

- `lossy → NoConsistency` is JACM Prop 10, not a policy call.
- Static `ClusterIds` is common knowledge *for free*: it is a model-level fact
  (true in all runs), and the unattainability theorems only forbid attaining CK
  of *run-contingent* facts. A `Joined` event is run-contingent, so dynamic
  membership can never be CK over any realistic channel — at best C^◇. This is
  precisely why `broadcast_live`'s member stream is `NoConsistency` and EC is
  re-earned at delivery.
- The type system is the **ledger of model-level common knowledge**: everything
  compile-time (protocol structure, failure policy, static membership) is CK
  because every member runs the same artifact. EC inference = deriving which
  run-contingent facts get upgraded to C^◇ by riding on that ledger.
- C^◇ is a **greatest fixed point**, strictly stronger than any finite
  iteration of "everyone eventually knows" (JACM §11 — E^◇ is not downward
  continuous). Coordination needs the self-referential fixed point, which is
  established *coinductively* via the **induction rule** (JACM C2): if the
  system validates `φ ⊃ E^◇(φ ∧ ψ)`, then `φ ⊃ C^◇ψ`. This is structurally the
  same proof shape as inferring EC around a `forward_ref` cycle — cycles
  carrying EC are legitimate gfp claims, not circular reasoning.

## 2. The load-bearing insight: CK of membership was never required

Inspecting what the epistemic argument for broadcast actually consumes: the
infinite nesting comes from the induction rule applied to a **model-level
invariant** ("whoever holds element e eventually sends it to every member it
eventually learns of") — and model-level facts are CK for free. The
extensional membership facts only need to supply the **first level**: every
join fact eventually reaches every data-holder. That is E^◇ (eventual
delivery), not C^◇, and not C.

So the static case was *over-provisioned*: `broadcast_closed` consumed CK of
the member set because it was lying around, but soundness needs only:

1. **Membership completeness** — every `Joined` fact is eventually delivered to
   every holder of data. A property of the membership *source* (oracle
   contract or static `ClusterIds`), not of any protocol. NOT a consistency
   property: observers may see joins at different times and in different
   orders forever (joins are stable facts; the view is a monotone lower bound
   of the true relation).
2. **Holder persistence** — some holder of e retains it and keeps
   participating (append-only data; both join sides retained). See §5 for the
   crash-fault strengthening.
3. **Channel eventual delivery** — the failure policy delivers every sent
   message to every live member (`fail_stop` / `lossy_delayed_forever`;
   tracked by `NetworkFor::ConsistencyGuarantee`).

Break any one premise and EC is unsound; keep all three and the induction rule
mints C^◇. An oracle providing membership at "only" C^◇/E^◇ grade is therefore
*not* a weaker-than-required foundation — it is exactly sufficient.

## 3. The prototype: `hydro_std::fan_out` (commit `391c6df8e6`)

The three premises, made into types:

- `Completeness` type-state on membership views: **`EventuallyComplete`**
  (premise 1) vs **`Sampled`** (a snapshot; deliberately has no fan-out —
  "never fan out over a membership snapshot" is unrepresentable rather than
  documented).
- `MembershipView` with the trusted mints of `EventuallyComplete`:
  `live()` (runtime membership oracle), `static_members()` (deploy-time
  `ClusterIds` — the degenerate, common-knowledge case), and a
  `from_stream_asserted()` escape hatch (named obligation for custom oracles).
- **`fan_out` / `fan_out_from_process`: the single EC-minting rule.** Data ×
  eventually-complete members → demux; output consistency =
  `NetworkFor::ConsistencyGuarantee`; holder persistence from the retained
  symmetric-hash join. The one `manual_proof!` citing all three premises lives
  here and nowhere else.

Consequences, all verified:

- `broadcast_live` / `broadcast_live_from_process` became three-line thin
  clients; their bespoke `manual_proof!`s are deleted. All prior behavior
  tests (static delivery, dynamic membership, late-joiner catch-up,
  `reliable_broadcast_live` fuzz) pass unchanged through the new path.
- `g_set_gossip_live` (crdt_gossip.rs): gossip over *dynamic* membership with
  EC fully inferred — zero consistency assertions; the only `manual_proof!`s
  are the fold's ACI obligations. Verified negatively: swapping `fail_stop`
  for plain `lossy` fails to compile at exactly the cycle-close
  (`complete(echo)`) and return sites.

Known deltas from the endgame: the rule belongs in `hydro_lang` using
`assert_has_consistency_of_trusted` (with `broadcast_closed` becoming an
instance via `static_members`), and completeness deserves its own
`CompletenessProof` trait rather than reusing `ConsistencyProof`.

## 4. The pattern language and its (conjectured) completeness

Four patterns, all type-checked:

- **A (mint):** fan out persistent data over an `EventuallyComplete` view via
  an EC-preserving policy.
- **B (coinduct):** declare `forward_ref` on the EC location; close the cycle
  only with EC streams (the broadcast-anchored cycle).
- **C (preserve):** ACI folds / monotone ops carry EC (stable facts).
- **D (re-earn):** nondet cut (`sample_every` …) downgrades; a fresh mint
  re-earns.

**Completeness conjecture.** Any protocol guaranteeing eventual convergence
necessarily establishes C^◇ (induction rule), and C^◇ of a data fact can only
arise via message chains from persistent holders to every member, guaranteed by
model-level structure (Survey §5, Chandy–Misra: knowledge is gained only via
message chains). Hence every sound EC protocol factors through pattern A plus
cycles — the pattern language is exhaustive *for the C^◇ tier*, and "the types
rejected it" means "not EC," not "library gap." To be stress-tested with abuse
cases (client-relayed delivery, hierarchical clusters).

**The tier map** (what to tell a developer whose protocol the types reject):

| Knowledge tier | Coordination | Hydro artifact | Status |
|---|---|---|---|
| C^◇ (eventual CK) | eventual convergence | `EventualConsistency` via fan_out + cycles | today |
| C^L (likely CK) | probabilistic convergence | pull-gossip / epidemic protocols; random peer selection is a `nondet!` cut, correctly refused EC. Either a future probabilistic label, or add a deterministic completeness backstop to re-enter C^◇ | future label |
| C^T (timestamped CK) | per-epoch agreement | views/epochs; retraction, pruning, current-set queries (M3) | future label |
| C (true CK) | simultaneity | should have **no** expressible type — unattainability as API design | never |

Boundary results the theory fixes:

- **Joins vs. leaves are not symmetric.** `joined(p)` is stable → C^◇-mintable.
  `¬member(p)` is a fact about the run's future — unknowable in async systems
  (the failure-detector impossibility in epistemic clothes). Exactly two doors
  out: (a) reclassify leave as crash — the indexical group C^◇_N already
  quantifies over survivors, zero new machinery; (b) epochs/view synchrony =
  C^T. There is no third door. (Corollary: incarnation ids work because they
  make departure a *stable* fact — "this id never returns" — re-entering the
  C^◇ tier by renaming.)
- **Pruning shrinks the indexical group, not the tier.** A bounded-retention
  log still gives full C^◇ but among "members joined before the prune
  horizon." The sub-EC tier should be a **group parameter** on the label
  ("C^◇ among N"), not a new rung: N = live members (today), never-leavers
  (crash semantics), pre-horizon joiners (pruned logs), view-k members
  (epochs).
- **Consensus safety is not a knowledge-tier fact.** It is an invariant about
  message *generation*, not delivery. The transcript-consensus finding (one
  residual assertion for safety) is principled and permanent: the EC label
  covers convergence; safety belongs to the fold + simulator.
- **Division of labor:** the types assume the completeness/delivery axioms and
  do the coinduction (unbounded); the simulator is the **test oracle for the
  axioms** (M0 membership hook; convergence-at-quiescence checking) and
  refutes finite counterexamples. Neither can do the other's job.

## 5. Post-mortem: the orchestrated-membership attempt (`756c3bee90`)

Verdict: the attempt correctly identified every ingredient and placed the
trusted label on the one object the theory says cannot carry it.

**What panned out (already load-bearing):**

- The `MembershipHook` order-insensitivity fix (fork on join *timing*, not
  order) — killed the exhaustive-search blowup; landed and tested.
- The **convergence theorem** premises (a)–(d) map one-for-one onto §2/§3:
  (a) ACI accumulation = fold obligations; (b) eventual transitive flow = the
  cycle; (c) envelope coverage = `EventuallyComplete`; (d) per-offer delivery
  = `ConsistencyGuarantee`. Tier 1 (replicate-cycle rule signed once) = the
  generic rule; Tier 2 (named obligation) = `from_stream_asserted`. Two
  independent derivations of the same factoring — evidence it is real.
- The **monotone envelope** insight: fan-out needs a monotone
  *over-approximation* whose only obligation is "eventually contains V∞" —
  i.e., the completeness property, discovered from the systems side.
- The **sender-crash finding** (see correction below).
- M-check (convergence-at-quiescence in sim) — the right axiom oracle.

**Why `orchestrator_view` was a dead end (each failure predicted):**

1. **The EC label had no consumers.** The design doc proves it itself: the
   pipeline is EC-view → envelope (`NoConsistency`, necessarily) → re-earned
   EC at delivery. Every consumer drops the label at the next step. The
   property fan-out consumes is *coverage* — a completeness property, not a
   consistency property. The attempt minted the wrong type.
2. **"The current set is EC" is out-of-tier.** `member(p)` is not a stable
   fact; the axiom had to reach for "once churn stops" — a quiescence/limit
   assumption the C^◇ machinery cannot support. The doc's own pinned escape
   hatches are exactly the theory's two doors: incarnation ids (stable-fact
   renaming) and ordered view changes (C^T). Note the orchestrator (ZK)
   internally *has* a total order; consuming only the unordered eventual-view
   guarantee discarded precisely the C^T structure that would make current-set
   claims sound.
3. **The trust story couldn't tighten.** Axiom in `hydro_std` behind
   user-callable asserts (the doc's own complaint), satisfied *by
   construction* in the sim (circular), deploy backing never wired.

**Correction the attempt makes to §3:** premise 2 tacitly assumed the holder
does not crash. Single-hop fan-out yields EC only if the sender is correct;
under crash faults, "every element reaches every member that ever joins" is
what the *echo cycle* adds (classical RB agreement). Epistemically: one
delivery gives K_i(m); the echo invariant "every correct receiver re-sends to
all" is what makes the holder-set self-perpetuating so the induction rule
closes under crashes. In the current fault model (sim does not crash senders;
`fail_stop` models recipient failure) `fan_out`'s axiom is exactly as strong
as `broadcast_closed`'s — status-quo parity. Endgame: premise 2 hardens to
"some **correct** holder persists," which is structural only in the
replicate-cycle — so the crash-fault-honest EC mint attaches to the cycle
(Tier 1), with `fan_out` demoted to a mechanical primitive.

## 6. Salvage plan / next steps

1. Drop `orchestrator_view`'s EC claim; re-home `member_envelope` as a third
   mint: `MembershipView::orchestrated(...) → EventuallyComplete`, its trust
   obligation now the honest, minimal coverage axiom.
2. Keep a current-set product only for protocols needing the agreed set
   (quorums), tagged as epoch/C^T-tier (M3) — or ride incarnation ids.
3. Promote the replicate-cycle rule into `hydro_lang` as the crash-fault-honest
   mint (subsuming `fan_out`'s EC under crash faults); `reliable_broadcast_live`
   becomes its test instance. Move `fan_out` to `hydro_lang` with
   `assert_has_consistency_of_trusted`; make `broadcast_closed` an instance;
   introduce `CompletenessProof`.
4. Write the completeness conjecture's abuse cases
   (crdt_gossip_soundness.rs-style).
5. Implement convergence-at-quiescence checking in the sim (replaces
   `skip_consistency_assertions` for these constructs).

## 7. Reading map (code ↔ theory)

| Artifact | Grounding |
|---|---|
| `EventualConsistency` label semantics | JACM §11 (C^◇ gfp), §10 (fixed points) |
| `fan_out`'s single `manual_proof!` | JACM §6 axiom C1 / rule C2 (induction rule), applied coinductively |
| `lossy → NoConsistency` | JACM §8 Thm 5, §11 Prop 10 |
| "among live members" | indexical-group knowledge C_N, Survey §6 |
| monotone ops preserve EC; `batch`/`snapshot` drop it | stable facts, JACM §11 caveats; Survey §5 (knowledge moves only via message chains) |
| joins-vs-leaves asymmetry; M3 | stable facts + failure-detector impossibility; C^T (JACM §12) for epochs |
| `manual_proof!` soundness criterion | internal knowledge consistency, JACM §13 |
| static `ClusterIds` needs no proof | model-level validity ⇒ CK at time 0 |
