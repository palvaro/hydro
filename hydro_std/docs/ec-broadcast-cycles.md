# Inferring EventualConsistency across broadcast cycles

## The claim

A class of cluster protocols — reliable broadcast, CRDT gossip, and (we
conjecture) broadcast-transcript consensus — can carry an
`EventualConsistency` (EC) type on their output **inferred entirely by the
type system**, with zero `assert_has_consistency_of` and zero `manual_proof!`
on the consistency label itself.

This is stronger than it sounds. These protocols contain *feedback cycles*
(a member's output depends on what it received from others), and cycles are
exactly where naive consistency reasoning breaks. We show the inference is
**always sound** because it rests on two facts the type system already
enforces, plus one structural discipline.

## The two facts the type system already gives us

1. **`broadcast_closed` + `fail_stop` earns EC from the transport, not the
   input.** Its return type is `Cluster<'a, L, N::ConsistencyGuarantee>`,
   where `ConsistencyGuarantee` comes from the *network failure policy*
   ([`NetworkFor`]). `fail_stop` → `EventualConsistency`; `lossy` →
   `NoConsistency`. Crucially, the consistency of the *input* stream is
   irrelevant — a `NoConsistency` input broadcast over `fail_stop` yields an
   `EC` output. EC is *earned fresh* at every broadcast.

2. **Location types are invariant in the consistency parameter, and every
   non-broadcast operator preserves the location `L` unchanged.**
   `merge_unordered`, `unique`, `fold`, `clone` all have signatures
   `Stream<T, L, …> → …<T, L, …>`. Any operator that introduces per-member
   nondeterminism (`batch`, `sample_every`, …) returns `L::DropConsistency`
   instead — mechanically downgrading to `NoConsistency`. You cannot
   accidentally keep an EC label across a nondeterministic step; the types
   forbid it.

## The trick

A feedback cycle in Hydro is built with `forward_ref`: you obtain a handle
and a placeholder stream, use the placeholder, then `complete` the handle
with the real stream later. The placeholder's **location type is fixed at
declaration** — including its consistency parameter.

The trick is to declare the `forward_ref` on the **EC location produced by an
initial `broadcast_closed`**, rather than on the bare cluster (which defaults
to `NoConsistency`):

```rust
let initial = source.broadcast_closed(cluster, TCP.fail_stop().bincode()); // EC
let (handle, cycle) = initial.location().forward_ref::<Stream<_, _, …>>();  // EC placeholder
```

Now the placeholder `cycle` is EC. To `complete` the handle, you must supply
an EC stream — and the type checker enforces this. The completing stream is
produced by *another* `broadcast_closed`, which (fact 1) independently earns
EC. The cycle closes with matching types.

## Why this is *always* sound

The soundness argument has exactly three clauses, and all three are checked
by the compiler — none are assumed:

1. **`initial` is genuinely EC.** It is the output of `broadcast_closed` +
   `fail_stop`. This is the one place the *library* (not user code) uses a
   trusted assertion, justified once inside `hydro_lang`: a fail-stop network
   delivers the same messages to every live member. User code cannot forge
   this — `broadcast_closed`'s EC output is the only entry point.

2. **Only EC streams are ever connected to the cycle.** The `forward_ref`
   placeholder is EC, so `merge_unordered` (fact 2) forces its other operand
   to share the EC location. The completing `echo` stream must be EC or
   `complete` will not type-check. There is no way to smuggle a
   `NoConsistency` stream into the cycle.

3. **Nothing between the endpoints can downgrade and survive.** Every
   in-cycle operator either preserves `L` (fact 2, e.g. `merge_unordered`,
   `unique`, `fold`) or returns `L::DropConsistency`. If it downgrades, the
   result is `NoConsistency`, and it will fail to unify with the EC
   `forward_ref` or the EC `merge` — a compile error. The only way a program
   type-checks is if EC is preserved end-to-end.

The subtle case is deliberate downgrade-then-re-earn. `crdt_gossip` samples
its folded state (`sample_every` → `NoConsistency`) and then re-broadcasts it
(`broadcast_closed` → EC again). This is sound for the same reason as clause
1: the sampling nondeterminism happens *before* a broadcast, and the
broadcast earns EC fresh regardless of its input. The re-earned EC is what
completes the cycle.

**Consequence:** if the program compiles, the output is EC. There is no
trusted user-level annotation on the consistency label. The trust boundary is
exactly `broadcast_closed`'s single library-internal assertion — the same one
every use of `broadcast_closed` already relies on.

## What EC does and does not guarantee here

EC means *all live members converge to the same value*. It does **not** mean
the value is deterministic across executions, nor that it satisfies an
application-level safety invariant (e.g. "no two values committed per slot").

- **Reliable broadcast** and **CRDT gossip** need nothing more: convergence
  *is* the specification. Reliable broadcast additionally deduplicates
  (`unique`); CRDT gossip relies on the fold's merge being ACI, which is a
  separate `manual_proof!` on *commutativity/idempotency* — orthogonal to the
  EC label.
- **Consensus** needs EC *plus* a safety invariant that the transport-level
  argument does not provide. EC guarantees everyone agrees on the transcript;
  it does not guarantee the transcript encodes a single value per slot. That
  remains a property of message generation, tested separately.

So the pattern cleanly *factors* the proof surface: EC (convergence) is
inferred and free; algebraic properties (ACI) or protocol safety are separate,
minimal, clearly-scoped obligations.

## Code

All on branch [`eventual_consistency`](https://github.com/palvaro/hydro/tree/eventual_consistency)
of `palvaro/hydro`.

- **Reliable broadcast** —
  [`hydro_std/src/reliable_broadcast.rs`](https://github.com/palvaro/hydro/blob/eventual_consistency/hydro_std/src/reliable_broadcast.rs).
  The cycle: `broadcast_closed` (line 92) → `forward_ref` on the EC location
  (line 103) → `merge_unordered` (107) → `unique` (110) → re-`broadcast_closed`
  (116) → `complete` (120).

- **CRDT gossip (state-based G-Set)** —
  [`hydro_std/src/crdt_gossip.rs`](https://github.com/palvaro/hydro/blob/eventual_consistency/hydro_std/src/crdt_gossip.rs).
  Same skeleton, but folds into a `HashSet` (line 47), then
  `sample_every` (58) downgrades and `broadcast_closed` (62) re-earns EC to
  close the cycle.

- **Soundness notes / abuse cases** —
  [`hydro_std/src/crdt_gossip_soundness.rs`](https://github.com/palvaro/hydro/blob/eventual_consistency/hydro_std/src/crdt_gossip_soundness.rs).

[`NetworkFor`]: https://github.com/palvaro/hydro/blob/eventual_consistency/hydro_lang/src/networking/mod.rs
