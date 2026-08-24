# Research Agenda: Inferring Distributed Consistency Types

2026-08

**Status:** agenda and index. States the goals, records the concrete progress so
far, notes the new understanding that has come out of it, and points to the code
that constitutes evidence. The design docs it references contain a great deal of
speculation; this doc deliberately restricts itself to what we have actually
been able to build and to the ideas that are load-bearing for that work.

## 1. Goals

Hydro claims to do for distributed systems what Rust did for memory safety and
concurrency: make the hard cases tractable by pushing their invariants into the
type system. For that claim to hold, the type system must **make it hard to
write the wrong program** and give **compile-time confidence** that a correct
one is correct — without the author reaching for escape hatches at every step.

The escape hatches are `assert_has_consistency_of` (assert a consistency label
by hand) and `manual_proof!` (discharge a fact by human argument). They are the
`unsafe` of Hydro. Some uses are legitimate and irreducible; the problem is
their frequency and placement. The current ecosystem leans on them heavily —
the reference Paxos (`hydro_test/src/cluster/paxos.rs`) asserts its consistency
rather than deriving it, and there are few examples of **inferred**
EventualConsistency (EC).

The goal is to move that boundary: derive consistency labels from how a protocol
is built, and shrink the manual obligations down to the places where a genuine,
non-inferable fact actually lives. Two success criteria, both mechanical:

1. **Inference** — EC appears on a protocol's output because the compiler
   derived it from the composition, with zero consistency assertions in the
   protocol body.
2. **Residue localization** — where an obligation is genuinely irreducible, it
   is a single, named, greppable seam rather than a proof smeared through the
   protocol.

The work targets two protocol classes: **EC, NoOrder** systems (broadcast and
gossip, where agreement is on a set/bag) and **EC, TotalOrder** systems (agreed
logs, where positions must be assigned). Each is studied under both static and
dynamic cluster membership.

## 2. Concrete progress

These are the things we have actually been able to do with the type system.
They are small but real: EC (and, where noted, total order) appears on the
output because the compiler derived it, not because we asserted it.

- **Static reliable broadcast** (`hydro_std/src/ec_inference_demos/reliable_broadcast.rs`)
  — echo-based reliable broadcast over a static cluster. EC is inferred around
  the re-broadcast cycle, with zero consistency assertions in the protocol.

- **CRDT (G-Set) gossip** (`.../crdt_gossip.rs`) — state-based grow-only-set
  gossip over a static cluster. EC is inferred around the folded-state cycle;
  the only `manual_proof!`s are the fold's associativity/commutativity/
  idempotence obligations, which are honest lattice-merge facts, not consistency
  assertions. A companion soundness file (`crdt_gossip_soundness.rs`) pins the
  negative cases: swapping `fail_stop` for a lossy policy must fail to compile.

- **Leader/merge "fake consensus"** (`.../leader_merge.rs`) — multi-writer,
  total-order, EC output produced by a single merging leader (primary/backup),
  with **no consensus machinery at all**. This is the most instructive result:
  it shows that a large part of what is usually reached for as "consensus" is
  not needed to *produce* a multi-writer total order — a single author suffices,
  and the type system infers TO,EC on the broadcast of that author's chosen
  order. It also pins where the type system *refuses* the naive port (a cluster
  member as leader), which surfaces the exact spot the guarantee is load-bearing.

Supporting infrastructure that also landed:

- **Taxonomy pins** (`hydro_std/src/taxonomy_tests.rs`) — the ordering ×
  consistency corners recorded as compile-time type facts.
- **Simulator dynamic-membership support** — join-timing exploration in the sim
  (`52fe248857`), with behavior tests for reliable broadcast and late-joiner
  catch-up. A state-space blowup under dynamic membership was diagnosed to root
  cause (`membership_hook_blowup_findings.md`).

The division of labor these rest on: **the types assume the delivery axioms and
do the induction; the simulator is the test oracle that refutes finite
counterexamples.** One oracle we deliberately do not yet have is crash
injection — the sim explores message, batch, and join timing but never kills a
process — so any guarantee that turns on process failure currently lives only in
the types and prose, not in the sim.

## 3. New understanding

The concrete work above produced two pieces of clarity worth recording
separately from the code.

### 3.1 The Halpern connection: EC is eventual common knowledge

Hydro's `EventualConsistency` label — "every live member eventually converges to
the same value" — lines up cleanly with **eventual common knowledge C^◇** from
Halpern & Moses (*Knowledge and Common Knowledge in a Distributed Environment*,
JACM 1990). This is not decoration; it explains and grounds design choices we
had otherwise made by feel:

- True common knowledge C (needed for *simultaneous* coordination) is
  unattainable over realistic channels; C^◇ (needed for *eventual* coordination)
  is attainable exactly when every message is eventually delivered. So
  `lossy → NoConsistency` is a theorem, not a policy call.
- C^◇ holds only for **stable facts** (once true, stays true). This is why EC
  propagates through monotone operators and mechanically dies at
  `batch`/`snapshot`/`sample_every`.
- The type system is the ledger of **model-level common knowledge**: everything
  compile-time (protocol structure, failure policy, static membership) is common
  knowledge for free because every member runs the same artifact. EC inference
  is deriving which run-contingent facts get upgraded to C^◇ by riding on that
  ledger. This is why inferring EC around a `forward_ref` cycle is legitimate
  coinduction rather than circular reasoning — it is Halpern & Moses's induction
  rule.
- It fixes the boundary between joins and leaves: a `Joined` event is a stable
  fact (C^◇-mintable), but a leave/"not a member" is a claim about the run's
  future, which is unknowable in an asynchronous system (the failure-detector
  impossibility in epistemic clothes). This is *why* the append-only,
  joins-only case is the tractable one.

Fuller treatment: `2026-08_epistemic_foundations_ec_inference.md`.

### 3.2 The orchestrator factors the dynamic-membership problem

The other piece of clarity is practical. Any real deployment has an
**orchestrator** — Kubernetes, ECS, ZooKeeper, a service mesh, etc. — that is
already in the business of tracking membership. Recognizing this factors the
dynamic-membership problem cleanly in two:

1. **The common case: an oracle hands the cluster its members.** In a real
   deployment the orchestrator provides membership. From Hydro's point of view
   this is an oracle delivering an EC view of the member set — and once
   membership arrives as an EC input, the dynamic case reduces to the machinery
   we already have: fan out over that view, re-earn EC at delivery. This is the
   case that matters for anything anyone would actually ship, and it is the one
   to build out.

2. **Inventing the universe: bootstrapping the oracle in bare Hydro.** The other
   branch is building such a membership oracle *from scratch* inside Hydro, with
   no external orchestrator — deriving eventual agreement on the member set from
   first principles. This is genuinely interesting and touches the hardest parts
   of the theory (agreeing on a mutating set is not a stable-fact problem), but
   it is **not urgent**: in practice the orchestrator already exists, so this is
   a "fun, later" branch rather than a blocker.

This factoring is what earlier attempts (the deleted orchestrated-membership
experiment) were groping toward without stating cleanly. The lesson from that
attempt — recorded in the epistemic doc §5 — is that "the current set of members
is EC" is not a stable fact and cannot be minted the way join events can; taking
the member set as an EC input *from an oracle* sidesteps exactly that problem.

## 4. Where this leaves consensus, and other speculation

Much of the content of the referenced design docs is speculation and should be
read as such. In particular:

- The **EC, TotalOrder / multi-writer** story beyond leader/merge — decomposing
  consensus into a convergence part and an author-succession part, a typed
  fault-dependency parameter `F`, and an epoch-keyed "consensus as a splice
  invariant" construction (`succeed_key`) — is exploratory. The only *concrete*
  TO,EC result so far is the single-writer leader/merge above; everything about
  fault-tolerant multi-writer consensus remains a sketch on paper
  (`2026-08_ordering_consistency_taxonomy.md`, `2026-08_epoch_keyed_consensus_splice.md`).
- `paxos_ec.rs` is a partial data point in that direction (it infers EC on the
  committed log and isolates slot-safety to one assertion) but is not a finished
  result and should not be over-claimed.

These are recorded to preserve the thinking, not because they are done.

## 5. Near-term direction

- Build out **case (1)** of §3.2: dynamic membership via an oracle-supplied EC
  member view, with reliable broadcast and gossip running over it and their EC
  inferred. This is the practical payoff and the natural next step from the
  static demos.
- Finish the simulator's dynamic-membership testing path so late-joiner
  convergence is exercised through the real compiled dataflow, not just under
  static membership.

Deferred (interesting, not urgent): bootstrapping a membership oracle in bare
Hydro (§3.2 case 2); the fault-tolerant multi-writer consensus program (§4).
