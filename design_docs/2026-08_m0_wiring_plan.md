# M0 wiring plan: dynamic membership in the simulator

Plain-language plan for wiring the `MembershipHook` (already built, in
`sim/runtime.rs`) into the simulator. This is the "pieces #2–#4" from the
proposal's M0 milestone.

## The problem, restated

When the simulator sets up a cluster, it hands the program every "member X
joined" event **all at once, before the program starts doing anything**.
Concretely: membership is compiled to `source_stream(iter([Joined, Joined, …]))`,
an eager list the scheduler drains up front. So every member appears
simultaneously, and there's no way to test "a member joins late, after messages
are already flowing."

## The fix, in one sentence

Feed those "joined" events through the `MembershipHook` instead of an eager list,
so the simulator releases them **one at a time, at moments it chooses** — turning
join timing into something the exhaustive search explores.

## What has to change (three small edits, one opt-in)

1. **An opt-in switch.** Add `SimFlow::with_dynamic_membership(&cluster)`. It just
   records the cluster's key in a `HashSet`. If a cluster isn't in that set,
   nothing changes — it keeps the old eager behavior. This keeps every existing
   simulator test untouched (they don't opt in, so they don't fork on join
   timing and blow up their state space).

2. **Carry that set into the code generator.** The simulator's DFIR builder needs
   to know which clusters are "dynamic." Thread the `HashSet` from `SimFlow` into
   the builder.

3. **Emit the hook instead of the eager list, for dynamic clusters.** Today the
   membership source always compiles to `source_stream(eager_list) -> tee()`.
   Change it so: if the target cluster is marked dynamic, instead emit a
   hook-backed source — create the channel, register a
   `MembershipHook` pre-seeded with that cluster's members (each as a `Joined`
   event), and read from the hook's output. The member list is already computed
   in `sim/flow.rs` (`cluster_member_ids`), so seeding is free.

That's it. No scheduler changes (it already forks on hook decisions). No changes
to other deployment backends. No `Deploy`-trait changes.

## The one asymmetry to keep in mind

The simulator's existing hooks all sit *between* two dataflow pieces (something
feeds them, they feed something). The membership hook is different: **nothing
feeds it** — its events are pre-seeded from the known member set. So we emit only
the "downstream" half of the usual hook wiring (read from the hook), not the
"upstream" half. That's why it needs its own small emit path rather than reusing
the existing one — it's a simplification of the existing pattern, not a new
mechanism.

## How we'll know it works

An end-to-end simulator test: a cluster with `with_dynamic_membership`, a
broadcast running, and an assertion that a member which joins *after* the first
messages still ends up converged. That's the delta-on-`member` path the current
simulator can't reach — and the payoff that unblocks M2's late-joiner test.

## Scope / risk

- The hook itself: done.
- Edits 1 and 2: mechanical.
- Edit 3 is the real work — it's the codegen path, and the part that wants care.
- Deliberately out of scope: `Left` (members leaving). It's non-monotone and
  belongs to M3.
