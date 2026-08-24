# Consensus as a Splice Invariant over an Epoch-Keyed Stream

2026-08

**Status:** design sketch, pre-implementation. Companion to
`2026-08_ordering_consistency_taxonomy.md` (§3c, §7, §8, §10) and successor to
the leader-merge demo ladder in `hydro_std/src/ec_inference_demos/leader_merge.rs`.
Records the construction plan for an inductively-built consensus protocol whose
safety residue is localized at a single typed seam. The protocol family is not
new (completed, it lands in Paxos / Viewstamped Replication / Raft territory);
the **factoring** is the contribution: everything intra-key is machine-inferred,
and the entire manual obligation concentrates at key transitions.

## 1. The inductive idea

- **Base case (done, pinned).** One live key = one author = the order.
  `leader_merge_keyed_from_member` produces
  `KeyedStream<MemberId<L>, (writer, value), Cluster<EC>, per-key TotalOrder>`
  with EC *inferred* — zero consistency assertions. With a single author,
  everything rides the single-writer row (taxonomy §2) for free. Failure-free
  consensus is: that one key **is** the log.
- **Inductive step.** If progress stops (the author is suspected dead), some
  other member becomes the author — *a new key*. The old key's substream ends;
  the new key's begins. Safety is an invariant relating the keys.

Taxonomy §3c described consensus as "collapse the key space to one," with all
the difficulty in the collapse being unstable across leader changes. This
design takes the dual: **do not collapse. Let the key space grow monotonically
— one key per epoch — keep per-key TO,EC free, and reduce consensus to the
cross-key splice invariant.**

## 2. The log type

```text
KeyedStream<Epoch, (slot, value), Cluster<L, EventualConsistency>, per-key TotalOrder>
```

- **Keys must be epochs, not member ids.** "Agreement on the prefix" across
  keys is not even *stateable* without a total order on keys (member 1 then
  member 0 then member 1 again is unorderable). `Epoch` is a monotone counter,
  optionally `(epoch, MemberId)` with a deterministic epoch↦member map
  (round-robin, as in ballot schemes). The key space **is** the ballot space.
- **Per-key content:** the substream of key *e* is epoch-*e*'s author's chosen
  order — single writer, so per-key TotalOrder + EC are inferred exactly as in
  the base demo.
- The signature is succession-native: a leader change is a new live key, with
  no type change. (This was the observation that motivated the honest-type
  variant: the cross-key question the type forces consumers to confront is
  precisely the safety residue.)

## 3. The invariant (sharpened)

Naive statement: "all keys agree on the prefix." The correct statement is about
**committed** prefixes:

> **Splice invariant.** For epochs e < e′: every entry *committed* under e
> appears, at the same slot, in the log under e′.

Sharpenings that make it implementable:

- Each new key **declares a start slot** s_{e′}: the position from which its
  author continues the log.
- Entries of older keys at slots ≥ s_{e′} are **dead** — this is log
  truncation, and it is unavoidable: a deposed author may have half-published
  entries that were never committed, and nobody is obligated to keep them.
  ("All keys agree on the prefix" without the committed-qualifier would forbid
  truncation and is unachievable.)
- **The splice reader:** fold keys in epoch order, taking key e's entries at
  slots `[s_e, s_{e′})` where e′ is the next live epoch. Equivalently: slot *i*
  is owned by the largest declared epoch whose start is ≤ *i*; read slots
  ascending from the owner, stalling at the first miss. Deterministic on the
  fact bags — which is what EC needs: converged bags ⇒ converged logs. **The
  raw splice is deliberately non-monotone**: a newly declared epoch retracts a
  dead tail (truncation), so an already-derived suffix can shrink. Monotone
  emission is exactly what the commit rule (M2) buys — restricted to committed
  entries, the splice only grows. (Implemented and pinned:
  `ec_inference_demos/epoch_splice.rs`, including a test that pins the
  non-monotonicity itself.)
- The invariant then reduces to: **no committed entry ever lies at or beyond
  any successor's declared start slot.**

## 4. The two forced components

The user-level sketch is: "to become leader, prove you hold a prefix at least
as recent as a quorum's; carry the proof in an EC stream." That is necessary
but not sufficient. Two components are *forced*, each by a counterexample, and
they are exactly taxonomy §8's two axes run forward as a construction.

**(a) Commit rule — quorum-ack before visibility (§7, §8 axis 1).**
Counterexample if absent: epoch-0's author appends entry 7; replica A delivers
it to a client; the author dies. The epoch-1 candidate reads a quorum that
excludes A; no quorum member holds entry 7; the candidate's "proven" max-prefix
stops at 6; it declares s_1 = 7 and overwrites a client-visible entry. Fix: an
entry is **committed** (client-visible, splice-durable) only once f+1 members
hold it. Then every election quorum intersects every commit quorum, and the
candidate's max-prefix read provably covers all committed entries. The election
proof and the commit rule are two halves of one quorum-intersection argument —
neither works alone.

**(b) Fencing — endorsements end keys (§8 axis 2).**
Counterexample if absent: the epoch-1 candidate completes its quorum read; the
epoch-0 author, alive but slow, appends entry 8 afterward; some member accepts
it. Key 0 grew after s_1 was computed; the splice is violated. Fix: a quorum
member's endorsement of epoch e′ is simultaneously a **promise** to accept no
further appends under any epoch < e′, enforced at the member. This is what
makes a key's substream *end* (rather than pause), i.e., what makes "the key
space grows monotonically and old keys are dead" true rather than aspirational.

With (a) and (b), the sketch is complete and safe. Liveness (when to suspect
the author, how elections avoid livelock) is a separate axis — timeouts are
member-local TO,NC inputs (§1), racing elections must be *safe* under (b)
regardless of timing, and election liveness is explicitly out of scope for the
safety design (taxonomy §6 discipline).

## 5. The residue ledger — what is inferred, what is owed

| piece | status |
|---|---|
| per-key order + consistency | **inferred** (single-writer row per key) — pinned in `leader_merge_keyed_from_member` |
| dissemination of certificates: "member m holds prefix P", "m endorses epoch e′", per-entry acks | observed facts once uttered → NoOrder,EC **bags**, inferred via `broadcast_closed` / reliable-broadcast (§8: authored facts become observed the instant they exist) |
| quorum counting over those bags | `hydro_std::quorum::collect_quorum` (its consistency obligations are today `manual_proof!(/** TODO */)` — to be discharged or absorbed, see §6) |
| splice reader | deterministic + monotone → EC-preserving, inferable |
| commit rule + fencing + start-slot correctness | **the entire consensus residue**, concentrated at the key transition |

Everything above the last row is machine-checked or mechanically inferable
with existing machinery. The last row is the single human obligation.

## 6. `succeed_key`: the one combinator, the one proof

Package the residue as one combinator owning the key transition:

```text
succeed_key(log, certificates, epoch e′)  — contract:
  (i)   commit-gating: an entry is visible/splice-durable only when
        quorum-acked            (holders-at-first-visibility = f+1, §7)
  (ii)  endorsement-is-promise: members endorsing e′ refuse appends under
        epochs < e′             (fencing)
  (iii) intersection: election quorums ∩ commit quorums ≠ ∅
        (threshold f over a closed cluster gives this for free)
```

Its one `manual_proof!` is the quorum-intersection argument — the same
slot-safety residue transcript-consensus isolated as non-inferable (§2), but
now attached to *one named seam* instead of smeared through a protocol body.
`succeed_key` is where `EC<F = ∅>` is minted for the spliced log: it is the
`act_after_replication` typed discipline that taxonomy §10 lists as an open
question, made concrete. In §8's terms: the combinator *is* the
author-succession factor of the F-eraser arrow, priced separately from the
convergence factor (which the echo already handles).

Framing sentence: **consensus as a monotone fold over an epoch-keyed stream,
where safety is a splice invariant at key boundaries and everything intra-key
is inference.**

## 7. Relation to existing implementations in this repo

`typed_consensus`, `paxos_ec`, and `broadcast_transcript_consensus` already
demonstrate "EC inferred for dissemination + manual proof for slot safety."
The differences here:

1. **The log's type is the protocol's state.** The epoch-keyed stream is not
   an encoding detail; the key structure carries the ballot discipline, and
   the type system's own refusal to flatten keyed streams (the missing
   morphism, §3c) is what forces the residue to surface at key boundaries.
2. **Residue localization.** One combinator, one proof, greppable; everything
   else inferred. The existing implementations spread the safety argument
   across fences, folds, and commit paths.
3. **Inductive buildability.** Each rung of the ladder below is independently
   a pinned, testable demo — the protocol is the *last* rung, not the first.

## 8. Build ladder

- **M1 — splice reader** (small). *Done:* `ec_inference_demos/epoch_splice.rs`.
  Pure `SpliceState` (ownership/truncation semantics, unit-tested including the
  deliberate non-monotonicity) + `splice_epoch_log` dataflow fold (EC preserved
  via one commutativity `manual_proof!` — ACI genus, no consistency assertion)
  + sim behavior test: two epochs, one dead tail, every member converges to the
  same truncated log.
- **M2 — commit certificates** (small). Per-entry acks as an EC bag +
  `collect_quorum`; gate visibility on quorum. Pin: the commit rule as a
  mechanical gate. This alone upgrades the single-key demo to §8's hardened
  variant with quorum-grade (rather than echo-grade) convergence.
- **M3 — election read** (medium). Candidate folds the certificate bag for a
  quorum max-prefix; mints key e+1 with declared s_{e+1}. Deliberately unsafe
  alone; if expressible, pin the §4(a) counterexample as a failing-property
  sim test (the sim explores message orderings and concurrent timer
  interrupts, so racing elections are explorable *without* crash injection).
- **M4 — fencing** (hard). Endorsement-as-promise state at members; appends
  under stale epochs refused; key substreams provably end. Expected fight: the
  refusal is inherently stateful-per-member (`sliced!` + `use::state`), and
  keeping the fenced stream's EC label honest through that state is exactly
  the kind of place the type system may demand a new assertion — if so, that
  assertion belongs inside `succeed_key`, not in user code.
- **M5 — `succeed_key`** (capstone). Package M2–M4 behind the contract of §6
  with its single `manual_proof!`; splice via M1; pin the three-place claim:
  spliced log is TO,EC with F = ∅ among live members of the closed cluster.

## 9. Honest limits and open questions

- **Crash injection still doesn't exist.** The sim can race elections (timer
  interrupts are ordinary inputs) but cannot kill the epoch-0 author, so
  "progress resumes after author death" is untestable today; what *is*
  testable is safety under concurrent authors, which is the hard part. Same
  division of labor as the taxonomy footnote: types + sim attack safety;
  crash-liveness waits for the fault-injection oracle.
- **Closed cluster only.** Quorum intersection assumes fixed membership;
  composing with the dynamic-membership machinery (`fan_out`,
  `broadcast_live`) is out of scope and interacts with epochs in known-hard
  ways (view changes ≈ membership epochs; see the orchestrated-membership
  post-mortem).
- **`collect_quorum`'s TODO proofs** become load-bearing at M2; discharging
  them (or absorbing them into `succeed_key`'s axiom) is part of the work.
- **Does the splice reader subsume dense-prefix extraction?** The slot route's
  reader is the one-key special case; if M1 is written well, the slot-route
  demo becomes its degenerate instance.
- **Liveness of election** (livelock avoidance, timeout tuning) is
  deliberately unaddressed — the liveness axis again.
