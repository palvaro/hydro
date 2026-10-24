# `nondet!` and `manual_proof!`: Two Doors Out of the Same Gates

2026-08

**Status:** conceptual note, claims verified against the code (file/line refs
inline). Clarifies the relationship between Hydro's two witness macros —
`nondet!` (`hydro_lang/src/nondet.rs`) and `manual_proof!`
(`hydro_lang/src/properties/mod.rs`).

The short version, all of it verified below:

- The ordering/retries labels (`NoOrder`, `AtLeastOnce`) are **not**
  nondeterminism; they record what is *unpinned* (arrival order, duplication)
  while safe operators preserve eventual determinism of the values.
- Nondeterminism of output values enters only through `nondet!`-guarded
  operators. `manual_proof!` (in its algebra face) is demanded where an
  unpinned input meets a sensitive consumer, to certify determinism is
  *preserved*.
- At such a gate the two macros are **duals — the two doors out**: prove the
  choice cannot matter (`manual_proof!`, stay deterministic) or accept that it
  does (`assume_* + nondet!`, admit nondeterminism). The compiler's own
  diagnostics present exactly this pair.
- Only `nondet!` can be forwarded to callers (checked-exception-like);
  `manual_proof!` always discharges locally. And `manual_proof!` has a second,
  unrelated face — the consistency mint — which is the `unsafe`-like one.
- "Locally resolving" a `nondet!` is **prose, not a mechanism**. Nothing
  consumes a guard; the doc comment is an informal proof, and the simulator is
  what attacks it.

## 1. Mechanically, both are unchecked tokens

Both macros parse a doc comment, discard it, and construct a marker value
(`nondet.rs:83`, `properties/mod.rs:108`):

- `nondet!(/** why acceptable */ [forwarded_guards…])` → `NonDet`
- `manual_proof!(/** why the property holds */)` → `ManualProof`

Neither checks anything. The prose is for the reviewer; the compiler sees only
the token. Both are trust points. The differences are in *where the type system
demands each one* and *what accepting it means*.

## 2. What the labels track: unpinned, not nondeterministic

Every safe Hydro API guarantees **eventual determinism**
(`docs/correctness/nondet.md`): however messages are delayed or interleaved,
outputs settle to the same final value. The `Order` and `Retries` type
parameters record what that guarantee does *not* pin down:

- `NoOrder` — the eventual **bag** of elements is deterministic; the arrival
  order is not. Minted structurally by operators whose output genuinely has no
  order, none of which take a witness: `merge`/`interleave`, `flat_map`/
  `flatten_unordered`, `KeyedStream::{values, entries, keys}`,
  `resolve_futures`, `weaken_ordering`, and unordered network receive paths
  (all in `live_collections/`).
- `AtLeastOnce` — the set of *distinct* deliveries is deterministic; how many
  times each is delivered is not.

Crucially, a `NoOrder` stream is not yet a nondeterministic *value*. It becomes
one only if something order-sensitive consumes it. The labels are the type
system's memory of unpinned choices; the gates below are where that memory
gets confronted.

## 3. `nondet!`: the doors nondeterminism enters through

The design principle (`docs/correctness/nondet.md`): *"all non-determinism in a
Hydro program originates at a `nondet!` invocation."* (With the docs' own
caveat: closures handed to `map`/`filter` can smuggle unchecked nondeterminism —
RNG, wall clock, hash-map iteration. The guarantee covers the dataflow API, not
arbitrary Rust.)

The guarded operators come in two kinds, verified in `stream/mod.rs`:

**(a) Introducers** — operators whose output values genuinely vary run to run:
`batch` (boundaries, :2152), `sample_every` (timer, :2054), `timeout` (:2081),
`snapshot`, timers. Note that these *also* weaken their output types where the
damage is expressible: `sample_every` returns
`Stream<T, L::DropConsistency, Unbounded, O, AtLeastOnce>` — consistency
dropped, retries weakened, in the type. The `nondet!` guard covers only the
residue the labels cannot express (*which* elements, *which* boundaries).

**(b) Assumers** — `assume_ordering` (:2220) and `assume_retries` (:2330),
which **strengthen a label unchecked**: `NoOrder → TotalOrder`,
`AtLeastOnce → ExactlyOnce`. Nothing changes at runtime; the program simply
proceeds as if the unpinned choice were pinned, which is exactly how
order/duplication-contingency leaks into values. This is the nondet-side
analog of the consistency mint in §6.

Because `NonDet` is a **value parameter**, a guard supports three dispositions
(`docs/correctness/nondet.md`):

1. **Resolve locally** — bare `nondet!(/** explanation */)`; callers never see
   it (see §5 for what this really means).
2. **Forward** — take a `nondet_` parameter and cite it:
   `nondet!(/** how inner maps to outer */ nondet_param)`. The obligation
   becomes part of your signature and the caller inherits it, recursively —
   the checked-exception shape. (`paxos_core`'s `nondet_leader` /
   `nondet_commit` are the worked example.)
3. **Discharge at the root** — no caller remains; the justification cites the
   service-level contract.

## 4. The gates, and `manual_proof!` as the other door

Where an unpinned input meets a sensitive consumer, the type system erects a
gate. `fold`/`reduce` carry the bounds (`stream/mod.rs:1449, 1510`):

```rust
C: ValidCommutativityFor<O>,   // order-sensitivity gate
I: ValidIdempotenceFor<R>,     // duplication-sensitivity gate
```

with (`properties/mod.rs:292–309`):

```rust
impl ValidCommutativityFor<TotalOrder> for NotProved {}   // ordered: no proof needed
impl<O: Ordering> ValidCommutativityFor<O> for Proved {}  // any order: ok WITH proof
impl ValidIdempotenceFor<ExactlyOnce> for NotProved {}    // exactly-once: no proof
impl<R: Retries> ValidIdempotenceFor<R> for Proved {}     // any retries: ok WITH proof
```

On a `NoOrder` (resp. `AtLeastOnce`) input, `NotProved` does not satisfy the
bound: the fold **will not compile** bare. The compiler's `on_unimplemented`
diagnostic then names both doors (`properties/mod.rs:286–289`):

> Because the input stream has ordering `{O}`, the closure must demonstrate
> commutativity with a `commutative = ...` annotation. […] To intentionally
> process the stream by observing a non-deterministic (shuffled) order of
> elements, use `.assume_ordering`. **This introduces non-determinism** so
> avoid unless necessary.

So at the gate you choose:

- **Door 1 — `commutative = manual_proof!(…)`**: claim the unpinned choice
  *cannot affect the result*. Determinism is preserved; no nondeterminism ever
  enters; the claim is trusted. (`Proved` is what `ManualProof` supplies via
  `CommutativeProof`.)
- **Door 2 — `.assume_ordering::<TotalOrder>(nondet!(…))`**: accept that the
  choice *does* affect the result. Nondeterminism officially enters the
  program, guarded, forwardable.

The two macros are **duals at the same gate**, asserting opposite things:
"the choice doesn't matter" vs. "the choice matters and here is why that is
acceptable." (An earlier draft of this note called them "sibling witnesses
certifying the same thing" — wrong; the diagnostic above is the corrective.)

Some sensitive operators offer **no proof door at all**: `first()` and `last()`
require `O: IsOrdered` (`stream/mod.rs:1623, 1655`; `KeyedStream::first` also
demands `R: IsExactlyOnce`, `keyed_stream/mod.rs:1835`). On an unordered input
they simply do not exist — the only way through is `assume_ordering`. There is
no commutativity story that could make "first" well-defined on a bag, so the
type system refuses rather than asks for a proof.

(Library internals use `_trusted` variants — `assume_ordering_trusted`,
`assume_retries_trusted` — with internal `nondet!`s, e.g. `last()`'s
`nondet!(/** last is idempotent */)`. Marked "only for internal APIs that have
been carefully vetted," `keyed_stream/mod.rs:586`. Same sequestration pattern
as `assert_has_consistency_of_trusted` in §6.)

## 5. "Locally resolved" is prose, not a mechanism

Nothing ever consumes, matches, or neutralizes a `NonDet` guard. When the docs
say a nondeterminism is "resolved locally" they mean one thing: **it is not
observable in the function's outputs** — and the *only* artifact of that claim
is the doc comment inside the `nondet!`, which is an informal proof citing the
downstream structure that makes it true. From the docs' own examples:

- `sample_every(…, nondet!(/** …`.last()` eventually resolves to the final
  value of the input singleton */))` — the justification cites a downstream
  `.last()`;
- `assume_retries::<ExactlyOnce>(nondet!(/** retried requests carry the same
  ID, and we only count each unique ID once (via `first()`) */))` — cites a
  downstream dedup.

Note what the cited operators are *not*: they are not resolvers the type
system recognizes. `.last()` does not discharge anything; the type system's
entire contribution to `.last()` is refusing it on unordered inputs (§4). The
connection between the guard and the mechanism it cites lives wholly in prose.
If someone deletes the `first()`, the program still compiles and the stale
comment is all a reviewer has (the docs say this explicitly: the explanation
"gives a reviewer a fighting chance").

What actually checks these claims is the **simulator**: it mechanically
explores the choices `nondet!` admits — batch boundaries, sample timing,
orderings — and it deliberately does **not** trust `manual_proof!` either
(`sim/runtime.rs:1705`: elements are permuted "even if commutativity is
claimed"). The division of labor: the types force every trust point to be
written down; the sim attacks what was written.

## 6. The other face of `manual_proof!`: the consistency mint

Everything above is the determinism/algebra face. `manual_proof!` also
satisfies `ConsistencyProof`, which is what `assert_has_consistency_of` demands
(`stream/mod.rs:497`):

```rust
committed_entries                                   // Cluster<_, NoConsistency>
    .assert_has_consistency_of::<Cluster<_, EventualConsistency>>(manual_proof!(/** … */))
                                                    // Cluster<_, EventualConsistency>
```

This is a different beast from the gates of §4. There is no unpinned-choice
story and no dual `nondet!` door; the proof simply mints a stronger label, and
the output type carries no record that it rests on an unchecked argument. A
downstream consumer cannot distinguish an inferred `EventualConsistency` from a
`/** TODO */`-asserted one. This is the `unsafe` of Hydro.

It is *syntactically* similar to §3(b)'s assumers (`assume_ordering`,
`assume_retries`) — both strengthen a label the compiler cannot check — but
semantically they differ in kind. The assumer's label is **true in every run**:
the stream really is consumed in some order; only *which* order varies run to
run, and that variation is recorded on the `nondet!` ledger, forwardable to
callers, and explored by the simulator. The mint's label is a **fact claim that
may hold in no run**: if the proof is wrong, members genuinely diverge and
nothing anywhere records the breakage. Recorded arbitrary choice vs.
potentially-false unrecorded claim — only the second is `unsafe`. (The same
line separates a justified `assume_ordering` from a *false* `commutative =`
proof: both produce cross-run variation, but the first is on the books and the
second cooks them — which is exactly why the sim permutes regardless of the
claim.)

(`hydro_lang` reserves `assert_has_consistency_of_trusted` for its own audited
minting sites — `broadcast_closed`, the interval sources — so user-facing
consistency trust concentrates in a few reviewed places.)

## 7. Summary

| | `nondet!` | `manual_proof!` (algebra face) | `manual_proof!` (consistency face) |
|---|---|---|---|
| demanded at | introducers (`batch`, `sample_every`, …) and assumers (`assume_ordering`, `assume_retries`) | gates where unpinned input meets sensitive consumer (`ValidCommutativityFor`, `ValidIdempotenceFor`) | `assert_has_consistency_of` |
| what accepting it means | nondeterminism **enters**, justified | determinism **preserved**, the unpinned choice provably can't matter | a stronger consistency label, asserted |
| position in signature | value parameter | trait-bound satisfier | trait-bound satisfier |
| can publish to callers? | yes — forwardable, checked-exception-like | no | no |
| relation to the other | dual door at the §4 gates; the compiler's diagnostic offers both | dual door at the §4 gates | no dual; structurally akin to the *assumers* (unchecked label strengthening) |
| what checks it | simulator explores the admitted choices | simulator permutes anyway (`sim/runtime.rs:1705`) | nothing mechanical today (the inference agenda's target) |

Three corrections this note bakes in, against earlier drafts of itself:

1. `NoOrder` is minted structurally (merge, flat_map, keyed flattening,
   channels), **not** published by a `nondet!`.
2. At the fold gates, the macros are **duals** (prove-it-can't-matter vs.
   accept-that-it-does), not siblings certifying the same thing.
3. Nothing "resolves" a `nondet!` — not a downstream operator, not a token.
   "Locally resolved" is an informal proof in the guard's doc comment that may
   *cite* downstream operators (`.last()`, dedup via `first()`), but the type
   system neither knows nor checks the connection; the simulator does.
