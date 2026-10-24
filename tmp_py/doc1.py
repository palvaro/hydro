p='docs/design/hydrofrac-soundness.md'; s=open(p).read()
def rep(o,n):
    global s
    assert s.count(o)==1,o[:90]
    s=s.replace(o,n)

# --- §3 structure
rep("""- a closure that reads Hydro state by reference is fragile before any arm (`closure_over_state`).""",
"""- a closure that reads Hydro state by reference is fragile before any arm (`closure_over_state`);
- so is a closure, or a source expression, that names outside or shared state (`impure_closure`, A3): `static`,
  `thread_local`, `Cell`/`RefCell`/`OnceCell`/`Lazy*`, `Mutex`/`RwLock`, `Atomic*`, `unsafe`, randomness, `Instant`,
  `SystemTime`, `tokio`, and `std::`/`core::` `env`, `fs`, `io`, `net`, `process`, `ptr`, `thread`, `time`
  (`hf/provenance/fragile.rs`, `names_impure`). The guard is textual: it cannot see into functions the closure calls,
  and it flags some pure closures, which only costs schedules. Such a closure is also a schedule choice (§3.1).""")

# --- §3 table rows
rep("""| `Tee`, `Reference`, `BeginAtomic`, `EndAtomic`, `AssertIsConsistent`, `Counter`, `UnboundSingleton` | robust | pass-through, in the same execution |""",
"""| `Tee`, `Reference`, `EndAtomic`, `AssertIsConsistent`, `Counter`, `UnboundSingleton` | robust | pass-through, in the same execution |""")
rep("""| `DeferTick` | **fragile**, except the dedup idiom's state edge | a deferred record exists only if a later tick runs, and a tick runs only when a hook releases (`hydro_lang/src/sim/compiled.rs:2022`). The dedup edge only feeds a negation's negative side, whose tags the rewrite drops (`hf/provenance/rewrite.rs:1142`) |""",
"""| `DeferTick` | **fragile**, except the dedup idiom's state edge | a deferred record exists only if a later tick runs, and a tick runs only when a hook releases (`hydro_lang/src/sim/compiled.rs:2022`). The dedup edge only feeds a negation's negative side, whose tags the rewrite drops (`hf/provenance/rewrite.rs:1142`), provided nothing else reads the idiom's state untagged (below) |""")
rep("""| `Batch` of a stream | robust | decides when a record enters a tick, never whether: the hook can always release pending input, and quiescence runs it |
| `Batch` of a singleton, optional or keyed singleton (a *snapshot*) | **fragile** | the value seen depends on how much has been folded |
| `Source::Iter`, `ClusterMembers`, `ExternalNetwork`; `SingletonSource`; `ExternalInput` | robust | fixed values, the static membership, the harness's inputs (A8) |""",
"""| `Batch` or `BeginAtomic` of a stream, into a tick that snapshots no singleton | robust | decides when a record enters a tick, never whether: the hook can always release pending input, and quiescence runs it (A6). The simulator lowers `atomic` to a batch (`hydro_lang/src/sim/builder.rs:818`) |
| `Batch` or `BeginAtomic` of a stream, into a tick that also snapshots a singleton | **fragile** (`batch_beside_snapshot`) | the simulator runs that tick only once the singleton has a value (`SingletonHook::is_ready`, `hydro_lang/src/sim/runtime.rs:591`). If what feeds the singleton never arrives, the tick never runs, and the batched records never exist (A6) |
| `Batch` or `BeginAtomic` of a singleton, optional or keyed singleton (a *snapshot*) | **fragile** | the value seen depends on how much has been folded |
| `Source::Iter` and `SingletonSource` at top level; `ClusterMembers`, `ExternalNetwork`; `ExternalInput` | robust | fixed values emitted once, the static membership (A14), the harness's inputs (A8) |
| `Source::Iter` and `SingletonSource` inside a tick (`tick.singleton`, `optional_first_tick`) | **fragile** (`tick_constant`), except a dedup idiom's initial state | emitted in every tick that runs, and only if one runs; whether (and how often) the tick runs depends on its deliveries |""")

# --- exemptions
rep("""- **Constant inputs** (`hf/provenance/fragile.rs:299`, `View::constant`). A non-allowlisted step whose every input is
  built from iterator and singleton sources by stateless operators computes the same records in every schedule.
  Sources are excluded from this rule, since a node with no inputs would pass it vacuously: they decide in their own
  arm.""",
"""- **Constant inputs** (`hf/provenance/fragile.rs`, `View::constant`). A non-allowlisted step whose every input is
  built from iterator and singleton sources by stateless operators computes the same records from the same inputs.
  Inside a tick those sources are tagged themselves (`tick_constant`), so their tags still reach the step's output.
  Sources are excluded from this rule, since a node with no inputs would pass it vacuously: they decide in their own
  arm. `DeferTick` is excluded too: its records exist only if a later tick runs.""")
rep("""- **The dedup idiom** (`hf/provenance/fragile.rs:323`, `View::dedup_edge`): `fresh = arrived.filter_not_in(seen);
  seen = seen.chain(fresh)`, with `seen` starting from a constant. Its output over the run is every record of
  `arrived` not in the initial state. Proof:
  - a record `r` of `arrived` is emitted in the first tick it is in, unless it is already in `seen`;
  - `seen` holds only the initial state and earlier outputs;
  - so each record not in the initial state is emitted exactly once, in whatever tick it arrives.""",
"""- **The dedup idiom** (`hf/provenance/fragile.rs`, `View::dedup_idiom`): `fresh = arrived.filter_not_in(seen);
  seen = seen.chain(fresh)`, with `seen` starting from a constant. Its output over the run is every record of
  `arrived` not in the initial state. Proof:
  - a record `r` of `arrived` is emitted in the first tick it is in, unless it is already in `seen`;
  - `seen` holds only the initial state and earlier outputs;
  - so each record not in the initial state is emitted exactly once, in whatever tick it arrives.

  The exemption covers the `Difference`, the `DeferTick` carrying `seen`, and the initial state's nodes. It applies
  only if their records reach nothing else untagged: `seen` is read only by the negation and the cycle's sink, the
  sink only by its `CycleSink` root, the cycle source only by the `DeferTick`, which only `seen` reads, and the initial
  state's nodes only by each other and `seen`. A reader is also allowed if every path from it to a root passes a node
  that tags whatever it emits (a fold, reduce or scan, a `cross_singleton`, `enumerate`, `chain_first`, or a join in a
  tick, outside a trusted quorum: `View::covered`). Another reader (`seen.clone().all_ticks()`) would emit records
  that exist only if a later tick runs, untagged.""")
rep("""- `hf/check/brute_tests.rs` `a_top_level_scan_over_a_chosen_order_is_fragile` and `deferred_records_are_fragile`
  (the two regressions of §17);""",
"""- `hf/check/brute_tests.rs` `a_top_level_scan_over_a_chosen_order_is_fragile`, `deferred_records_are_fragile`,
  `tick_constants_are_fragile`, `a_dedup_state_read_elsewhere_is_fragile`, `review_batch_beside_an_unready_snapshot`
  and `impure_closures_are_fragile_choices` (the regressions of §17);""")

# --- §3.1
rep("""special rule names it. A `batch`, a snapshot, an untrusted `observe_nondet`, a fold over an unordered input, an
unordered external input, an ordered lossy network, any source but iterators, membership and external networks, and
every unlisted kind are choices.""",
"""special rule names it. A `batch`, a snapshot, an untrusted `observe_nondet`, a fold over an unordered input, an
unordered external input, an ordered lossy network, any source but iterators, membership and external networks, a
closure that names outside state (`impure_closure`, §3), and every unlisted kind are choices.""")
open(p,'w').write(s)
print('ok')
