# hydrofrac: one rule, `implies`

Status: design for review (phase 1), against `hydrofrac-races` at `57bd3b20f0`. Section numbers `§n` refer to
`docs/design/hydrofrac-soundness.md`. Claims are marked **proved** (argued here), **checked** (code enforces it),
**to test**, or **guessed**.

## 1. The rule, its settings, the API

**The rule.** For collections `A` and `B` and key functions `ka`, `kb`: *if a record `r` is in `A` (at a member
chosen by the A-side), then some record `r'` with `kb(r') == ka(r)` is in `B` (at the members chosen by the
B-side)*. It is read on the end state of each run (after the heal and the recovery phase), as today.

- **A-side** chooses which `A`-records are *obligations*:
  - `Correct`: those held by members that never crash in the run;
  - `Any`: those held by any member, a crashed one counting what its tap emitted before the crash fired.
- **B-side** chooses which `B`-members must hold a match, for each obligation key `κ = ka(r)`:
  - `AllCorrect`: every member of `B`'s location that never crashes;
  - `AtLeast(k)`: at least `k` members of `B`'s location held a match at some point (a crashed member counts what it
    held before crashing, as durability on crash-stop replicas is usually stated);
  - `Some`: `AtLeast(1)`.

**API** (`hydrofrac::Implies`, an extension trait on top-level `Stream`s, any location, not in a tick):

```rust
use hydrofrac::{ASide, BSide, Implies};
deliveries.implies("uniform", &deliveries, q!(|m| m), q!(|m| m), ASide::Any, BSide::AllCorrect);
acks.implies("durable", &stored, q!(|(id, _)| id), q!(|(id, _)| id), ASide::Correct, BSide::AtLeast(2));
...
results.label("uniform").assert_holds();
```

It adds, beside the user's dataflow, `a.clone().map(ka)` and `b.clone().map(kb)`, each tagged with the rule's name,
side and setting, into an empty `for_each`. The keys must be `Serialize` (a fact is its key's `bincode`, A4). The
planner finds the two tagged nodes and taps them like labels; the program itself is unchanged. This is not
`assert_has_consistency_of` (a type change on one collection), because a rule relates two collections that may live
on different locations; a name is needed because a rule has no binding of its own to name it by. Two rules with one
name are refused when the program is compiled. A `map` of a known-pure closure is robust and no choice (§3), so a
rule adds no schedules; an impure key closure costs schedules, as anywhere else.

## 2. `EventualConsistency` is one instance

A collection labelled `EventualConsistency` becomes the rule `A = B =` that collection, `ka = kb =` identity, `Correct`
→ `AllCorrect`: "every fact a correct member holds, every correct member holds". That is exactly today's POST *plus*
today's no-divergence check (§0), read in one go: today's POST covers the probe's facts and the divergence check the
others. So each run's verdict is unchanged. The *self-agreement* instance keeps its bounds (`|members| − 2` member
crashes, §10) and its `Vacuous(SingleMember)` shortcut; reports still call a missing fact outside the probe's
obligations *divergent*. The API of `EcCheck` and `Results` is unchanged; `Results` gains one entry per `implies` rule,
after the labels.

## 3. The search

- **Obligations are read from each run.** Every run (every schedule of every set the search or the oracles execute)
  is judged against the `A`-records it actually holds, never against the probe's.
- **Pairs.** For each obligation key `κ` the probe showed, and each member `b` of `B`'s location, a pair `(b, κ)`;
  its clauses are the derivations of the `B`-records with key `κ` at `b` (one per tap emission, as today).
- **Groups with a threshold.** The pairs of `κ` form a group with a *need*: `AllCorrect` needs 1 broken pair, `AtLeast(k)`
  needs `|B| − k + 1`. A set is admissible for a group if it breaks at least *need* of its pairs and the group is not
  exempt. The program becomes `Σ_{p∈G} z_p ≥ need_G · y_G`, `Σ_G y_G ≥ 1`. With need 1 it is today's program.
- **Breaking a pair.** For `AllCorrect`, as today: kill every robust clause, and do not excuse (a start crash of `b`
  excuses). For `AtLeast`, a crashed member's later holdings do not count, and Lemma R only says "holds or crashed",
  so *any* crash of `b` (start, crash point, `Unseen`) breaks `(b, κ)`, besides killing its robust clauses.
- **Exemptions** (a group with no obligation needs nothing): a dead set for `κ` (§8, re-proved in §5 below), or a
  set that start-crashes every member of `A`'s location (it emits nothing). The second is added only when it is
  within the bounds, so the EC program is unchanged.
- **No preserve rule.** Molly may skip a set that kills every known derivation of `PRE`, because its provenance graph
  holds *every* derivation of the failure-free run, and in a monotone program faults only remove derivations.
  hydrofrac's lineage holds one derivation per tap emission: a record reached by two paths and deduplicated (`unique`,
  the dedup idiom) shows one. Killing that path does not remove the record, so skipping the set would be a false
  certificate (**to test**: a two-path relay with `unique`, where the skipped set breaks the rule). Only proved absence
  exempts: start crashes and dead sets. Preserve terms could still order proposals (Molly's heuristic scope), but no
  instance needs it yet, so they are left out.
- **Pruning needs obligation completeness.** A set may be skipped (or an exploration stopped early) only if every
  obligation it could create is among the pairs. That is **proved** for fragile-free and choice-free programs (§5) and
  otherwise checked by today's Claim B sweep: every set within the bounds, every schedule, no pruning. So "Claim A"
  and "Claim B" become one claim, the rule; the second sweep is the *completeness sweep*.

## 4. Observation

The fault hook drops only what a node emits *after* its crash fires (`hf/fault/mod.rs`, `on_output`; oracle 2's hook
the same); what it emitted before is in the log. So `Any` and `AtLeast` need no new taps: the evaluation reads every
member's outputs instead of only the correct members'. A crash fires right after its send (A11), and the node emits
nothing more in that execution: that is the crash-stop semantics both oracles share. **Checked** by reading; **to
test** with a rule on a member that delivers and then crashes.

## 5. The soundness argument, claim by claim

- **§0, §11 (specification).** Replaced by the rule. The *domain* survives only to choose pairs and to word reports.
- **§1 (certificate theorem), case 1.** Fix `H` and a schedule. If `H°` is admissible for no group, take an
  obligation `κ` of the run. By completeness (below) `κ` is a probe obligation, so it has a group. Either the group is
  exempt, so `κ` is no obligation (contradiction), or fewer than *need* of its pairs are broken. Unbroken under
  `AllCorrect`: some robust clause survives, so by Lemma R `b` holds `κ` or crashed: every correct `b` holds it.
  Unbroken under `AtLeast`: `b` is not crashed in `H` and a robust clause survives, so `b` holds `κ`: at least `k`
  members hold it. **Proved.** Case 2 (tried sets) is unchanged.
- **§1.1 (free literals).** The new terms (the crash of `b` breaking a pair, thresholds over `z`) are over faults
  only, so every free literal still helps every row it is in. **Proved.**
- **§2 (Lemma R), §3–§7, §9 (completion).** Unchanged: they are about derivations, which now serve `B`. Exemptions use
  start crashes (always known, §9.2) and dead sets.
- **§8 (dead sets).** `κ` is dead under `S` if `S`'s one run shows no obligation `κ`. Preconditions: as today, but the
  crash points allowed are of nodes outside **`A`'s** location. Proof: an obligation `κ` at `a` under `H ⊇ S` has a
  robust derivation `D` (fragile-free); no fault of `S` kills `D` (as in §8); `a` is not crashed under `S` (a start
  crash of `a` in `S` is in `H`, where `a` then emits nothing); by Lemma R `a` holds `κ` under `S`. **Proved.**
- **§10 (bounds).** Self-agreement: as today. Any other rule: at most one crash per node, `max_crashes`, `nodes − 1`;
  no cluster cap, since a single survivor proves nothing there (uniform broadcast over two members needs a member
  crash). This widens coverage, never narrows it.
- **§13 (completeness, was Claim B).** *Can faults create `A`-records?* Yes: a retry after a timeout, a failover, a
  view change. Each needs a fragile step (a timer is a fragile source, §3) or a schedule choice. In a fragile-free
  program every `A`-record at `a` under `H` has a robust derivation, which survives the empty set, so by Lemma R it is
  at `a` in the probe, crashed or not: an `Any` obligation too. In a choice-free program faults only remove records
  (A13). **Proved.** Otherwise the completeness sweep runs every set within the bounds in every schedule, without
  early stops, so each obligation is re-read in a run that exists. A set pruned in the first sweep is run again
  there. **Proved** (§13's argument with groups for pairs). An empty probe with completeness proved is
  `Vacuous(no obligations)`; otherwise the completeness sweep runs, as today (§11).
- **§14 (one-schedule policy).** "Every schedule of `S` shows the same facts" was about end states. `Any` and
  `AtLeast` also read what a member emitted *before* its crash point fired, which may depend on the schedule (two
  arrivals in either order around the crashing send). So a set with a crash point on a member of `A`'s location (for
  `Any`) or `B`'s (for `AtLeast`) is explored over every schedule. **Proved** need; the restriction is **checked**.
- **§18 (panics).** Unchanged: a pruned set never hides a panic, by the same two cases.

## 6. Cost and tests

**Cost (guessed).** EC: the same program (need 1, no new rows), the same sets and schedules; the evaluation reads one
view where it read two. Within 1.1x. `implies` rules: `|B| × |obligations|` pairs, as a label of `B` with that many
facts; `AtLeast` adds a threshold row per group. A rule in a program with timers pays the completeness sweep, as EC
does today.

**Tests.**
- `hf/spec.rs` unit tests for each setting, including a crashed member's pre-crash records.
- Every existing test unchanged (verdicts, and the brute-force agreement of every cross-check).
- **Brute-force cross-checks** (`ec_implies_checks.rs`), for each instance: regular RB (`Correct → AllCorrect`) holds
  and uniform RB (`Any → AllCorrect`) breaks on a deliver-then-forward broadcast; URB holds both; validity; no
  creation (`Some`); acked ⇒ durable, broken by acking after one store, holding by acking after all (`AllCorrect`) or a
  majority (`AtLeast(2)` of 3); the two-path `unique` relay of §3.
- **Two independent oracles.** Brute force gets its own evaluation of the rule (no longer `hf/spec.rs`); oracle 2 finds
  the tagged nodes on its own fork, taps them, and evaluates the rule with its own code. Both are compared with the
  search by rule name.
- **Teaching examples** (`hydro_test/src/cluster/ec_implies_teaching.rs`): (a) a broadcast that passes regular RB and
  fails uniform RB; (b) the repo's URB passing both; (c) acked ⇒ durable, broken and fixed.
