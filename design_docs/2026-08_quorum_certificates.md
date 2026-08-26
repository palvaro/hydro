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
- **Rung 3 — single-decree synod.** ABD's skeleton + the splice rule: propose
  only after adopting the max-ballot value from a read certificate (the
  `Covering` snapshot feeding the splice invariant). Ideally zero new mints.
  Sim: dueling proposers for safety; progress under the rotating-oracle Ω
  discipline already used by the Raft tests.
- **Rung 4 — multi-decree.** Epoch-keyed log of rung 3, consumed by the
  existing M1 splice reader.

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
