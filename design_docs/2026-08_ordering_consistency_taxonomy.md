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
`hydro_std/src/ec_inference_demos/leader_merge.rs::tests::multi_writer_leader_merge_is_total_order_ec_with_untracked_spof`,
backing the `leader_merge_broadcast` demo function in that module):

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

## 8. Why the two F-clearings cost differently — and where the cost actually lives

There is an obvious parallel: plain broadcast passes the type system as EC but
is not meaningfully fault tolerant, and reliable broadcast strengthens it
against sender failure cheaply — one echo cycle, EC still inferred. Leader-merge
(§4) passes the type system as TO,EC and is not fault tolerant *in exactly the
same way* (one holder at first visibility, §7). Yet its usual repair is the
entire consensus apparatus. If the F-clearing mechanism is the same rule at
different replication degrees, why is the second repair so much more expensive?

Short answer: **it isn't — for what F measures.** Worked through carefully, the
convergence hole in leader-merge is repairable at RB cost, and everything
actually expensive in consensus turns out to sit on the liveness axis, which §6
already ordered out of F. The long answer follows.

**Two kinds of facts.** Distinguish provenance:

- **Observed facts** exist before dissemination. RB's message *m* existed the
  instant the sender created it; RB's only job is delivery. Any single holder is
  a complete, authoritative copy, and copies cannot conflict. This is the
  single-writer row of §2: stable facts someone's history determines.
- **Authored facts** do not exist until someone manufactures them. Per §3b, the
  cross-writer interleaving is exactly this — the leader is not relaying a
  pre-existing order, it is the sole author of a brand-new one, and the
  `nondet!` in the leader-merge program marks the run-contingent *choice*. A
  choice has no oracle: if it is lost before anyone else holds it, there is no
  ground truth to recover. This is §5's fork.

**But §4 already noted the escape: once made, the choice is data.** An authored
fact is only unrecoverable *before it is disseminated*; the instant the leader
has chosen, the choice is an ordinary observed fact from everyone else's
perspective — and observed facts are what RB moves cheaply. Concretely, apply
§3c's "make the order data" route in its degenerate single-author case:

1. the leader enumerates its merged TO stream into `(slot, value)` facts —
   the order now rides in the slots, not in transit order;
2. reliable-broadcast the facts to the replicas as a NoOrder,EC bag (the echo:
   each replica re-broadcasts before delivering);
3. each replica recovers the sequence locally by dense-prefix extraction
   (monotone, hence EC-preserving, per §3c).

Now rerun §5's fork scenario. The leader dies mid-fan-out; replica A received
slot 5, replica B did not. The echo carries slot 5 to B: whatever *any* live
member holds reaches *every* live member (§6's relay argument). The bags
converge. A single author cannot assign one slot twice, so §3c's residue — slot
uniqueness — is satisfied for free. Dense-prefix extraction is deterministic on
the converged bag, so the delivered *sequences* converge, stalling at the same
gap everywhere if some decision died with the leader — which §6 explicitly
blesses: a stalled log identical at every live replica is converged.

That is multi-writer TO,EC with **convergence-F = ∅, at RB cost: no quorum
round, no ballots, no fencing.** And it is emphatically *not consensus* —
nothing is agreed anywhere; slot assignment is unilateral; it is reliable
broadcast applied to authored data. (The slot-shipping step — minus the echo —
is now pinned in code:
`ec_inference_demos/leader_merge.rs::leader_merge_slots_from_member`, with a
distinguished cluster member as the author. That variant also pins a
type-system fact: the *naive* member-leader port is refused — on a `Cluster`,
`DropConsistency` genuinely bites and member-locality is invisible — so the
Process version's clean TO,EC typing is revealed as load-bearing on the
location kind. Making order data is what routes around the missing morphism.)

**So what does consensus actually buy?** The hardened log has one property
nobody would ship: when the author dies, it freezes *forever*. Converged,
consistent, and dead. Under §6's own discipline — progress is liveness and does
not belong in F — the hardened primary/backup and relayed Paxos have the **same
three-place signature**: same O, same Con, same N, same F = ∅. Every remaining
difference between them lives on the liveness axis: Paxos's log keeps accepting
entries after the author dies. That requires **author succession**, and
succession is where the entire cost of consensus sits:

1. a successor must be able to take over authorship (the liveness demand);
2. admitting a successor admits *rival* authors, and invention — unlike copying
   — is not idempotent: two authors can assign the same slot differently (the
   safety shadow of the liveness demand);
3. hence ballots/fencing and quorum intersection. This is also why phase 2
   *waits for f+1 acks before visibility*, where RB's echo is fire-and-forget
   (send, then deliver — no acks; one surviving copy suffices because copies
   cannot conflict). The quorum round is not a convergence cost. It is the down
   payment on **recoverable authorship**: a successor running phase 1 must be
   able to *discover* every decision that might have become visible, and
   quorum-held-before-visible is what makes discovery complete.

**The factoring.** §7's arrow `EC<F = {leader}> → EC<F = ∅>` factors into two
maps with wildly different price tags:

> (convergence-eraser: the echo — cheap, identical to RB's)
> ∘ (author-succession: quorums + fencing — expensive, liveness-motivated,
> safety-constrained)

The opening question dissolves: hardening the *convergence* of an authored
order costs the same as hardening a broadcast, by the same mechanism. Consensus
is more expensive because it is asked for more — nobody wants a log that stops.
The hard part was never "get the order everywhere despite crashes"; it is "keep
*making* order after the order-maker dies, without letting two order-makers
fork a slot."

**Ledger evidence from the repo's own programs.** The decomposition is visible
witness-by-witness:

- The authored-order `nondet!` is present and identical in all of them:
  leader-merge's `entries_partially_ordered(nondet!(/** leader dictates the
  interleaving */))`, and `raft.rs` / `broadcast_transcript_consensus.rs`'s
  `requests.assume_ordering::<TotalOrder>(nondet!(/** arrival order ... is
  inherently non-deterministic */))`. Consensus does not remove the choice's
  witness — a choice has no oracle, so this witness is irreducible.
- Naive leader-merge's EC rests on `broadcast_closed`'s trusted axiom,
  publishing the choice at holder-count 1. `paxos_ec`'s *consistency label* is
  inferred with no consistency assertion at all — the choice is never visible
  below quorum. (No contradiction with the footnote's observation that
  transcript-Paxos carries a `manual_proof!`: that proof discharges the
  slot-safety residue, which §2 places *outside* the consistency label.)
- The one witness the log-that-stops variants never need: Paxos/Raft's election
  `nondet!` ("which member wins an election is inherently non-deterministic").
  That is the succession machinery — the liveness purchase — showing up in the
  ledger.

**Corollary for typed F.** The `fault_dependency!` witness (§9) tracks *choices
visible below survivable replication*: naive leader-merge incurs it; echoing
the decisions clears it. But note the honest limit this section implies: typed
F will distinguish naive primary/backup from Paxos, and will **never**
distinguish echo-hardened primary/backup from Paxos — by §9's own refusal rule,
their difference (progress after author death) is not F's to record. That is
the label being *right*, not weak: F measures convergence, and a
converged-but-frozen log genuinely converges. It also adds a third program to
the footnote's indistinguishability observation, and pins where the residual
distinction must eventually live: the liveness axis (Bounded/Unbounded
territory), not the consistency label.

## 9. Implementation sketches

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

## 10. Open questions

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
- ~~Recording the leader-merge pattern as a pinned type test~~ Done, and since
  promoted to a real demo function: `hydro_std/src/ec_inference_demos/leader_merge.rs`
  exposes `leader_merge_broadcast` and pins its type-fact test, grouped with the
  other EC-inference demos (reliable broadcast, CRDT gossip). The single-writer
  TO,EC fact stays pinned in `hydro_std/src/taxonomy_tests.rs`. When F lands,
  extend the leader-merge test to assert the dependency is *tracked*
  (`EC<F = {leader}>`).

## Footnote: how invisible the primary/backup ↔ Paxos distinction currently is

Three closing observations on §5's "the label cannot distinguish
primary-backup from Raft":

1. **The visible trust ledger is inverted.** The primary/backup program (§4)
   carries *zero* consistency proofs — one `nondet!` — while transcript-Paxos
   carries a `manual_proof!` for slot-safety. To an auditor of witness
   obligations, the SPOF program looks *cleaner* than the fault-tolerant one.
   The ledger measures safety-under-contention (a problem primary/backup does
   not have — it breaks instead of racing); the fault-tolerance axis is
   simply unmeasured.

2. **Today even fault injection cannot reveal the difference.** The sim
   explores message timing, batching, and join timing — it never kills a
   process. So the distinction currently lives nowhere mechanical: not in
   the types, not in the sim; only in prose. And this is the label being
   *honest about its run set*, not a soundness bug: a fault model is a
   closure property on the set of runs (Halpern), and over the runs the
   current model contains, the two programs are genuinely equivalent — they
   diverge only on runs outside it (leader crashes). The failure mode is
   Halpern & Moses's internal-knowledge-consistency criterion breaking at
   the model boundary: acting as if the leader never crashes is safe exactly
   until an observable history contradicts it, and a production leader crash
   is that observation.

3. **But the distinction is statically derivable — it need not wait for
   fault injection.** The difference between the two programs is a
   structural fact about the dataflow graph: *does a quorum-ack edge precede
   the commit-visibility edge?* Holders-at-first-visibility = 1 vs. f+1
   (§7) is readable off the wiring without executing a single fault. That is
   why typed F (§9) is possible at all: fault injection is the *oracle* for
   the property, but the property itself is derivable from the graph — the
   same way the echo cycle makes coverage derivable. The endgame is the
   usual division of labor pushed one axis further out: F in the types,
   crash injection in the sim attacking the axioms F rests on.
