# Quorum Certificates: the Monotone Half of Consensus

2026-08. Companion code: `hydro_std/src/ec_inference_demos/quorum.rs` (rung 0),
`uniform_broadcast.rs` (rung 1).

## 1. The boundary this program respects: types vs. proof

EC inference works because EC is a type-shaped property: value-abstract (never
depends on what the data is), local (every rule mentions one operator's edge),
and symmetric (the same claim at every member, with eventuality absorbing all
scheduling). The checker propagates a label by structural induction over the
compile-time dataflow graph.

A quorum argument violates all three at once. It depends on values (distinct
member identities inside the payloads), it does arithmetic over runtime
cardinalities against the deployment size, and its power is relational across
instants — the intersection lemma relates every pair of certificates across
ballots and needs an existential witness ("some member is in both quorums")
over runtime sets. That is induction over traces, not over syntax: proof
territory, not type territory.

The consequence is that quorum facts land on the **residue localization**
success criterion, not the **inference** one — the same place `fan_out` lives.
The type system cannot derive a quorum claim, but it can make the certificate
**unforgeable** (one audited constructor) and **propagable** (a certificate is
a stable fact: monotone, EC-friendly, the same category as `Joined`). The
trusted residue is one mint; the simulator attacks it with crashes.

## 2. A quorum is a stability certificate

A fact attested by k distinct members survives any crash pattern that kills
fewer than k of them. `Durable` names that upgrade: with k = F + 1 under the
cluster's fault budget F, a certified fact cannot die with its holder(s).

The observation that organizes the whole ladder: **the quorum machinery of
consensus is monotone.** "k distinct members have attested f" is a monotone
threshold over a monotone count of stable facts — once true, it stays true.
Paxos's phase-2 certificates (`Durable`: a majority accepted (b, v)) and
phase-1 certificates (`Covering`: a majority was read, so every past `Durable`
fact intersects it) both live in this monotone fragment. The only non-monotone
move in Paxos is epoch succession, which is already quarantined in the splice
invariant (`2026-08_epoch_keyed_consensus_splice.md`). Consensus safety
factors as: **Durable ∘ Covering ∘ Splice** — two monotone mints plus one
quarantined non-monotone rule, each with its own crash-sim oracle.

## 3. The ladder

Per rung: what is typed (propagated), minted (trusted, one audited proof), and
sim-tested (the oracle). Static membership throughout — "a majority of a set
that is still growing" is not well-posed, which makes this ladder the concrete
forcing function for versioned membership views (agenda §4 item 4).

- **Rung 0 — the `quorum` mint** (DONE). Distinct-attestor counting
  (deduplicated `(fact, attestor)` pairs), threshold gate, `Durable`
  certificate emitted exactly once at the crossing. Two obligations at the
  mint: the fault-model pigeonhole (k > F leaves a correct attestor) carried
  by the sealed type, and determinism-through-the-slice (certificate set is a
  deterministic monotone function of the attestation set) carried by one
  `assert_has_consistency_of`. Known gaps, documented in the module: staged
  code cannot see module privacy, so unforgeability is by `#[doc(hidden)]`
  convention until the mint is promoted into `hydro_lang`; and `Durable` is
  deliberately not `Serialize` (a wire-crossing certificate would be a
  deserializable forgery — rung 2 must design transport).
- **Rung 1 — uniform reliable broadcast** (DONE). RB's echo cycle with
  delivery gated on a `Durable` certificate at threshold F + 1: deliver only
  facts that are already crash-proof. Zero assertions in the protocol body.
  The separation pair, both fuzz at n=3 under crashable sender + crashable
  cluster (F = 1): regular RB has an execution in which a member delivers and
  the fact dies with it (fewer than N − F ever deliver) — the deliver-then-
  crash hole, found by the fuzzer; URB never exhibits it in any explored
  execution. Uniformity is precisely a safety claim that covers *faulty*
  members' outputs, a distinction the portfolio table now records.
- **Rung 2 — ABD register** (DONE, `abd.rs`). Multi-writer: clients are a
  cluster (symmetric logic replicated by the language; requester and replica
  identity carried by channel keying, unforgeable). Write = covering read →
  stamp `(max_round + 1, CLUSTER_SELF_ID)` (the authored choice,
  `leader_merge`'s `nondet!` seam, now per-client) → phase 2 → rung-0
  `Durable` certificate. Read = covering read → adopt max → write it back
  through the same phase-2 path → certificate → return. Findings:
  - **The replica register is EC, inferred and compiler-pinned**: a top-level
    max-lattice fold over the EC write stream (gossip's pattern), zero
    consistency assertions in the file. The price: acks are *gated* on a
    register snapshot (`ts_applied ≥ ts_written`) instead of riding tick
    atomicity — sound because snapshots of a monotone singleton are monotone.
  - **Client-side streams are indexical, correctly un-EC**: quorum
    request/response traffic does not converge across members and shouldn't.
    Broadcast-shaped protocols are EC-shaped; quorum protocols are not.
  - The covering read is inline (count + running max in ONE fold, so the
    certificate's content is atomic with its trigger; fired once per rid).
    Its `nondet!` is load-bearing and justified: which majority answers picks
    which covering read this is; any majority dominates every completed
    write. A general `Covering` mint = Durable's counting + a caller lattice
    aggregation + exactly this nondet seam.
  - **Linearizability is untyped** — enforced by protocol, attacked by sim
    only. Whether an atomic register's output deserves a label (per-client
    ts-monotone reads is close to the per-sender-TO machinery) is a rung-3+
    taxonomy question.
  - Sim results (all fuzz): write/read smoke; sequential cross-client ops
    respect real time; **progress + latest-read under an untargeted replica
    crash** (the portfolio row — no leader, no dead state at F = 1); and
    reads stay ts-monotone under an untargeted *client* crash mid-phase-2
    (the classic incomplete-write scenario, expressible only because clients
    are a crashable cluster).
  - Honest residue: one-outstanding-op-per-client is an unenforced caller
    contract (violating it can mint duplicate timestamps, invalidating the
    max-merge tie argument); `Ts` carries `TaglessMemberId` so writers never
    tie.
- **Rung 3 — single-decree synod** (DESIGNED, not yet built). One slot of
  Paxos: proposers (a cluster, ≥ 2 so they duel) race to get one value chosen
  by acceptors. Construction claim: **synod = ABD's skeleton + one new
  rule.** Ballot = `Ts` unchanged; phase 1 = the covering pattern unchanged
  (promise carries highest accepted proposal; covering at majority); phase 2
  = the rung-0 `Durable` mint unchanged (accepted-acks at majority = chosen).
  The one new thing is **adopt-highest**: ABD's client may write its own
  value over a covering (overwriting is legal register semantics); the synod
  proposer must propose the covering's max-ballot value if one exists. That
  conditional is the entire difference between a register and consensus, and
  it is the splice invariant in miniature (ballot = epoch, adopt = splice).
  - **The acceptor inverts ABD's typing story (hypothesis to verify).**
    ABD's replica was a lattice (order-insensitive max-merge), hence a
    top-level fold, hence EC inferred. The synod acceptor cannot be:
    `promise(b)` exists to *refuse* future lower-ballot accepts, so
    `(max_promised, accepted)` is order-sensitive — the same message set in
    different orders ends in different states. Refusal is the mechanism, and
    refusal does not commute. So the acceptor is a tick-serialized slice
    with no EC label, *correctly*: the EC-inference boundary and the
    needs-succession boundary appear to land in the same place. Whether
    that is a theorem or a coincidence is a question this rung should
    answer in prose.
  - Ledger: zero new mints expected (same two combiner proofs as ABD); if a
    third obligation appears, its location defines the `Covering` mint.
  - Sim plan: smoke; agreement under dueling proposers (∀, fuzz, ±acceptor
    crash); **RED: adopt-highest replaced by own-value must yield divergent
    choices** (the splice rule is load-bearing — "the echo cycle is
    load-bearing," one rung up); **RED: quorum size ⌊N/2⌋ must yield
    divergent choices** — the first test that directly attacks the rung-0
    mint's fault-model `manual_proof!` rather than a protocol; progress
    under the rotating-oracle Ω discipline (driver owns ballot escalation,
    like Raft's timer inputs — no in-protocol retry at this rung).
  - Scope guards: no NACKs, no leases, no separate learners, single decree;
    one-outstanding-attempt-per-proposer remains a caller contract.
- **Rung 4 — multi-decree.** Epoch-keyed log of rung 3, consumed by the
  existing M1 splice reader; in-protocol ballot management arrives here.

## 3a. Taint is not transitive: the monotone shell / non-monotone kernel

A worry the acceptor hypothesis raises: if consensus's heart is un-EC, was
the EC machinery bathwater? No — because **EC is re-earnable**. A label is a
per-stream claim, not a purity discipline: `broadcast_closed` mints EC at
delivery regardless of input consistency (the RB cycle types with exactly
this), and a chosen certificate is a *stable fact* that re-enters the
monotone world the moment it exists. Synod's shape is monotone shell
(dissemination in), non-monotone kernel (the acceptor's conditional — a few
dozen lines), monotone shell (certificates and learning out). By volume the
protocol is mostly small EC pieces; the compiler draws the boundary around
the part that isn't, and that boundary is a *map of where coordination is
purchased*, not a defect report. Separately: a quorum-certified fact is
arguably *stronger* than EC (agreed, F-independent) — `Durable` as a future
label tier, per the fault-dependency thread in the ordering/consistency
taxonomy, is the natural home for that observation.

## 3b. Reuse scorecard, and where to cut the joints

After three rungs: **mints reuse; protocol bodies do not.** `quorum` has
three unmodified consumers. But URB restates RB's echo cycle and synod will
restate ABD's phases, because sibling protocols differ in their *middles*
and functions compose at their *edges* (RB exports deliveries, URB needed
the echoes; ABD exports completions, synod interposes adopt-highest between
covering and phase 2). Protocol-as-subroutine is the wrong joint for
siblings; the right joints, read off from the duplication:

1. **Extract `covering`** (threshold + caller lattice merge with proofs
   passed through + the "any majority" nondet seam); rung-0 `quorum` becomes
   its payload-free instance. Three shapes exist to generalize from.
2. **RB should export its echo stream** — return (deliveries, echoes); URB
   collapses to RB + `quorum(f+1, echoes)`, true in code not just prose.
3. **A `quorum_round` helper** (request → member-keyed responses →
   certificate → rid-keyed join back to continuation): the shape appears
   four times across ABD and synod.
4. **Do not** force synod to call ABD. Vertical reuse — consuming a
   guarantee (RSM over any TO,EC log; rung 4 consuming the splice reader) —
   is a separate, still-untested axis.

Plan amendment: build rung 3 *through* the extraction — `covering` first,
ABD ported onto it (suite as regression oracle), synod as its second
consumer, plus the RB echo-export refactor.

## 4. Findings so far

- The uniformity tests require observing a *crashed* member's pre-crash
  deliveries; `sim_cluster_output` provides this (external outputs are not
  staged by the crash hook), and the deliver-then-crash interleaving is real:
  the delivery escapes in the same tick in which the echo sends are cut.
- `hydro_std`'s pre-existing `collect_quorum` counts responses, not distinct
  members (a duplicating attestor can forge its threshold), can re-fire a key
  when min == max and late responses re-accumulate, and its consistency
  assert is a literal `manual_proof!(/** TODO */)`. The rung-0 mint is the
  hardened replacement for certificate purposes; migrating `collect_quorum`'s
  callers is future cleanup.
- The quorum slice's batch hook forks the exhaustive search heavily: URB's
  crash-free delivers-to-all takes ~140 s exhaustive at n=3 (it passes). The
  crash tests are fuzz (8192), matching the Raft standard.
