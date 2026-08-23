# The Ordering × Consistency Taxonomy, Multi-Writer Logs, and Fault Dependencies

2026-08

**Status:** brainstorm record. Companion to
`2026-08_epistemic_foundations_ec_inference.md` (which covers the EC ⟺ C^◇
correspondence, the fan-out rule, and the tier map). This doc records the
second session: how ordering composes with consistency, where multi-writer
lives in the type system, the leader-merge construction (compile-verified),
and the fault-dependency parameter the EC label is currently missing.

## 1. The taxonomy: O is intra-member, Con is cross-member

The two parameters answer different questions about different scopes:

- **Ordering `O`** (per collection): is *my local* eventual value a sequence
  (`TotalOrder`) or a bag (`NoOrder`)? Order-sensitive consumption demands
  `TotalOrder`; `NoOrder` permits only bag-dependence (or commutativity
  proofs).
- **Consistency `Con`** (on `Cluster` locations): do all live members converge
  to the same eventual value?

They are not independent in *meaning*: **`O` determines the equivalence
relation that `Con` quantifies over.** EC on a `NoOrder` stream = all members
eventually hold the same *bag*. EC on a `TotalOrder` stream = the same
*sequence*. But all four corners are inhabited:

| | `NoConsistency` | `EventualConsistency` |
|---|---|---|
| `NoOrder` | lossy broadcast output; merged cluster-to-cluster receipts | broadcast/gossip outputs: same bag, per-member arrival orders differ |
| `TotalOrder` | **member-local inputs** (each member's own client session, timers, its shard of a demuxed queue); anything touching `CLUSTER_SELF_ID` | an agreed log |

TO,NC — the corner that prompted this doc — is the *most common type in the
system*: every `sim_input` on a cluster is exactly
`Stream<T, Cluster<_, NoConsistency>, Unbounded, TotalOrder, ExactlyOnce>`.
Sharding (demux of an ordered stream over TCP) is TO,NC par excellence: each
member gets its subsequence in FIFO order, members hold disjoint data.

## 2. TO,EC is graded by writer count, not intrinsically hard

- **Single writer: TO,EC is free today.** `TCP.fail_stop()` carries
  `OrderingGuarantee = TotalOrder` *and* `ConsistencyGuarantee = EC`
  (verified in `networking/mod.rs`), and `broadcast_closed`'s output ordering
  is `MinOrder<O, N::OrderingGuarantee>`. A Process→Cluster broadcast of a
  `TotalOrder` stream over `fail_stop` is TO,EC: same elements, same order,
  every member. (`lossy_delayed_forever` is EC but `NoOrder`: a delayed
  message re-arrives out of position — preserves the bag, not the sequence.)
- **Multi-writer: TO,EC = consensus.** NoOrder,EC agreement is on the
  set-lattice ("e ∈ X" is a stable fact needing no placement decision).
  TO,EC agreement is on the prefix-lattice: the stable facts are
  position-indexed ("entry i is e"), and positions must be *assigned*. One
  writer assigns positions locally; distributed writers must agree on
  assignment — and "at most one value per slot" is exactly the safety
  residue the transcript-consensus work already isolated as non-inferable.

Since prefixes are stable facts and appends are monotone, TO,EC is still
C^◇-tier: the fan-out/cycle machinery applies; the *only* new obligation is
position-assignment safety, which lives outside the consistency label.

## 3. Where multi-writer shows up in the type system

**(a) Writer identity is the key parameter.** Process-source network ops
return plain `Stream`s; cluster-source ops return
`KeyedStream<MemberId<Writer>, …>` — per-writer substreams, each FIFO-ordered
(per-key `TotalOrder` from the transport), with no order across keys. The
type system never hands you "the merged stream from many writers" as a
primitive.

**(b) The flattening operators enforce a uniform law.** The doors out of
keyed-by-writer (signatures from `keyed_stream/mod.rs`, `stream/mod.rs`):

```rust
fn values(self)  -> Stream<V, L, B, NoOrder, R>                    // EC kept, order surrendered
fn entries(self) -> Stream<(K,V), L, B, NoOrder, R>                // same
fn entries_partially_ordered(self, _: NonDet)
    -> Stream<(K,V), L::DropConsistency, B, TotalOrder, R>         // order kept, consistency stripped
fn interleave…(self, other, _: NonDet)
    -> Stream<T, L::DropConsistency, B, TotalOrder, _>             // same law, two-stream form
```

Keep EC and lose the order, or keep an order and lose EC — never both. The
`nondet!` + `DropConsistency` pair encodes the epistemic fact: per-writer
prefixes are stable facts someone's history determines (cheap C^◇ via FIFO
broadcast); **the cross-writer interleaving is a fact that does not exist
until someone manufactures it** — each member merging independently makes
divergent choices. No message chain can inform you of something nobody
generated.

**(c) Hence multi-writer TO,EC is an unreachable corner of the safe algebra —
a missing morphism.** There is no safe-operator path from
`KeyedStream<MemberId<W>, T, ClusterEC, per-key TO>` to
`Stream<T, ClusterEC, TotalOrder>`. "Consensus" names the missing arrow. Its
two known implementations are the two ways to manufacture the interleaving
fact: **collapse the key space to one** (leadership; all of Raft's difficulty
is that the collapse isn't stable across leader changes), or **make the order
data** (agree on the NoOrder,EC bag of `(slot, value)` facts — already
inferable — then recover the sequence locally by dense-prefix extraction,
which is monotone, hence EC-preserving; residue = slot uniqueness).

## 4. The leader-merge construction (compile-verified)

Multi-writer TO,EC **without consensus** is expressible today and
type-checks (pinned as a compile-time test:
`hydro_std/src/taxonomy_tests.rs::multi_writer_leader_merge_is_total_order_ec_with_untracked_spof`):

```rust
let at_leader = writer_stream.send(&leader, TCP.fail_stop().bincode());
//   KeyedStream<MemberId<W>, T, Process<Leader>, _, per-key TotalOrder, _>
let merged = at_leader.entries_partially_ordered(nondet!(/* leader dictates */));
//   Stream<(MemberId<W>, T), Process<Leader>, _, TotalOrder, _>
let log = merged.broadcast_closed(&replicas, TCP.fail_stop().bincode());
//   Stream<(MemberId<W>, T), Cluster<R, EventualConsistency>, _, TotalOrder, _>
```

Why it type-checks, and why that is *correct*: `entries_partially_ordered`
returns `L::DropConsistency`, but on a `Process`, `DropConsistency = Self`.
Consistency is a cross-replica property; a single node has no replicas to
disagree with. The interleaving is still `nondet!` (run-contingent choice),
but within one execution the choice, once made, is data — and FIFO broadcast
of data from one node is TO,EC by the single-writer row. This is
primary-backup, a legitimate and ubiquitous pattern.

**Consequence: consensus is not needed to *produce* multi-writer total
order.** What consensus adds is *fault tolerance of the merge decision* —
that the leader's interleaving choices survive the leader.

## 5. The unrecorded assumption: EC is three-place

The leader-merge EC label is unsound the moment the leader can crash
mid-broadcast: replica A holds prefix p, replica B holds q ≠ p, both live,
the choice died with the leader, nothing reconciles them. It is rescued today
only by the fault model (the sim does not crash processes; `broadcast_closed`'s
trusted axiom silently assumes sender correctness). **The label cannot
distinguish primary-backup from Raft** — same output type, wildly different
guarantee.

The full signature of the guarantee is three-place:

> In every run where the failure pattern is tolerable per **F** *(an
> assumption — restricts which runs the promise covers)*, every member of
> **N** that stays live eventually holds the same value *(the quantifier
> inside the promise)*.

- **N — beneficiaries.** Who is promised to converge. Indexical (varies by
  run): today hardwired to "live members of the output cluster." A member of
  N crashing shrinks the group; the promise persists for the rest. The sub-EC
  tiers are all "same guarantee, smaller N": never-leavers (Left-as-crash),
  pre-horizon joiners (pruned logs), view-k members (epochs).
- **F — fault dependency.** Which failure patterns *void* the promise,
  including for blameless members of N. Not in general a set of named
  locations: the honest type is a failure predicate (quorum system /
  adversary structure). Named-set S is the degenerate case ("any failure set
  not containing the leader"); threshold-f is Paxos's case.

## 6. Convergence vs. progress — do not conflate them in F

"Paxos needs a quorum" conflates two properties with different dependencies:

- **Progress** (new entries keep committing) needs a live quorum — but
  progress is a *liveness* property and does not belong in a convergence
  label. A stalled log identical at every live replica is converged.
- **Convergence** (live replicas agree on what was decided): Paxos's
  dependency here is genuinely **∅**, *provided decided facts are relayed
  holder-to-holder* (learns forwarded, RB-style — the echo cycle again).
  Any commit any live member knows eventually reaches every live member
  under arbitrary crash patterns; commits nobody live holds are absent for
  everyone equally — still agreement. Quorum death stalls the log
  identically everywhere; it does not fork it.

## 7. The unifying invariant: holders-at-first-visibility

Why is leader-merge F = {leader} while RB and (relayed) Paxos are F = ∅?
One number:

> **How many locations hold a decision at the moment it first becomes
> visible to a beneficiary?**

- Leader-merge: visible at first delivery, holder count **1**. Lose the
  holder mid-fan-out → half-published, irrecoverable choice → fork.
- Reliable broadcast: re-broadcast *before* delivering — you become a holder
  before the fact is visible through you; holder count grows with visibility.
- Paxos: visible (committed) only after **f+1** acceptors hold it — phase 2
  *is* the rule "act only after the decision is distributed knowledge in
  every survivable configuration"; recovery is fact-discovery, not fact-loss.

**F is the residual dependency left by acting on under-replicated
decisions.** "Act only once the decision is held widely enough to survive
your failure model" is the single F-clearing mechanism; leader-merge, the
echo, and phase-2 quorums are the same rule at replication degrees 1,
growing, and f+1. Consensus's fault-tolerance contribution is enforcing this
invariant for *order-decisions*, with fencing to preserve slot-safety across
leader changes. Type-theoretically: **consensus is the arrow
`EC<F = {leader}> → EC<F = ∅>`** — a fault-dependency eraser.

## 8. Implementation sketches

- **Cheap (witness genus):** alongside `nondet!` (run-contingent *choices*)
  and `manual_proof!` (human-discharged *facts*), a third witness for assumed
  *correctness of locations* — e.g. `fault_dependency!(leader, /** … */)` —
  demanded at every process→cluster EC mint. Greppable, reviewable.
- **Full (typed F):** a fault-dependency parameter on the label (named-set
  first; thresholds later), unioned by operators, entering at process-source
  mints and feedback-through-a-location, cleared only by the consensus
  combinator (or an act-after-replication rule) with its proof obligation.
- **Sim oracle:** crash injection. Kill the leader mid-broadcast across
  explored schedules and check for divergence — the differential test for
  exactly the axiom the label assumes. (Complements the existing division of
  labor: types assume, sim refutes.)
- **Refusal:** progress/quorum-availability must not be encoded in F — it
  belongs to the liveness axis (Bounded/Unbounded territory), not the
  consistency label.

## 9. Open questions

- Does N × F subsume the sub-EC tier design (epistemic doc §4) entirely?
  (N handles retention/epochs; F handles fault assumptions; the label is
  `C^◇ among N given F`.)
- The holders-at-visibility invariant wants a static story: can "visibility
  only after k-fold holding" be a typed discipline (an `act_after_replication`
  combinator), making F-clearing inferable the way the echo cycle makes
  coverage inferable?
- Where does per-decision durability (client-facing "committed") attach? It
  is neither N nor F — it is a promise to an external observer, likely an
  `ExternalBincodeSink`-side obligation.
- ~~Recording the leader-merge pattern as a pinned type test~~ Done:
  `hydro_std/src/taxonomy_tests.rs` pins both the single-writer TO,EC fact
  and the leader-merge construction. When F lands, extend the test to assert
  the dependency is *tracked* (`EC<F = {leader}>`).
