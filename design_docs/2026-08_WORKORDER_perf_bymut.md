# WORK ORDER: fix the multi_paxos performance collapse

2026-08. Self-contained brief for a fresh session. Read this file, then the
references in §5, then start. Branch: **`eventual_consistency`** (continue
the existing line of work; the sibling work order
`2026-08_WORKORDER_consensus_gauntlet.md` uses its own branch and must not
be touched by this session).

## 1. The problem, with numbers

`multi_paxos_live` (the quorum ladder's rung 4 + liveness wrapper,
`hydro_std/src/ec_inference_demos/multi_paxos{,_live}.rs`) is functionally
complete and green (79/79 hydro_std tests, including colocated
whole-node-crash progress at raft's own standard). Under the
apples-to-apples bench (`hydro_test/src/cluster/consensus_bench.rs`,
`multi_paxos_some_throughput` vs `raft_some_throughput`, localhost, 3
nodes, 100 virtual clients):

- **Raft: ~63,700 req/s steady, p50 ~1.9 ms, flat.**
- **multi_paxos: ~1,200 req/s, p50 climbing 12→97 ms over a ~30 s run.**

53x slower AND O(history) per-tick growth. Raft is flat on the identical
substrate, so this is our code, not the platform.

## 2. Root cause (diagnosed, verified by reading, not yet fixed)

Two slice-state disciplines coexist in the codebase:

- **Raft's (flat):** one `use::state` singleton accessed via **`by_mut()`**
  inside a single staged closure per tick (`raft.rs:933-1000`). Batches are
  pre-folded to `Vec` singletons read via `by_ref()`; side outputs are
  `by_mut` Vec streams. State is mutated **in place**: O(batch) work per
  tick, zero clones, regardless of how large the log grows.
- **The ladder's (grows):** the synod-era `state.zip(batch).map(closure)`
  + reassignment + `.clone()` tee pattern. The whole state value is moved
  through the dataflow every tick, and every tee clones it. Growing values
  therefore cost O(size) per tick: **quadratic overall.**

The growing values on the bench's hot path:

1. **The acceptor's `accepted` map** (`multi_paxos.rs`, acceptor slice):
   `BTreeMap<slot, (Ts, Option<V>)>`, one entry per request ever, teed
   every tick (`accepted = new_accepted.clone()` + the promise
   `cross_singleton`). The leading suspect for the growth curve.
2. **The leader kernel's `learned` set** (`multi_paxos.rs`):
   `BTreeSet<usize>` of all chosen slots ever, same zip/clone pattern at
   every proposer.
3. Already fixed for reference: the wrapper's `ElectionState`
   (`multi_paxos_live.rs`) had the same bug with unbounded
   submitted/completed sets; it now retires completed values (state bounded
   by outstanding work). Its remaining zip-pattern tees are bounded, but
   converting it to `by_mut` too is cheap and consistent.
4. **Possibly the quorum mints** (`quorum.rs`): `fired` (a persisted
   stream) grows forever and is re-`chain`ed/reassigned per tick; whether
   that is O(|fired|)/tick depends on DFIR persistence internals — do NOT
   assume; measure after fixing 1-3.

## 3. The work

Rewrite the acceptor slice and the leader kernel (and optionally the
wrapper kernel) in raft's `by_mut` discipline. **Semantics must not
change**: the batch serializations are documented and load-bearing —

- Acceptor: accepts are checked against **tick-start** `max_promised`;
  promises are checked against tick-start `max_promised` but report the
  **post-batch** accepted map; `max_promised` then advances by the batch's
  max prepare. (Original argument in `synod.rs` module docs, "Batch
  semantics at the acceptor".) In a sequential closure: snapshot `mp0`,
  process all accepts against `mp0`, then all promises (cloning the map
  only per prepare — rare), then advance `max_promised`.
- Leader kernel: apply coverings (establishments) before sequencing that
  tick's commands; slot assignment order within a tick is the authored
  choice. Keep `EpochPlan::new` as the pure planning step.
- Batch→Vec pre-folds on NoOrder inputs need `commutative =` proofs whose
  justification is "the consumer is order-insensitive / sorts" — copy the
  proof-text style from `raft.rs:949-961` and `lin_kv.rs`'s committed_vec.
- Two-output slices (leader kernel emits proposals AND establishment
  events; wrapper emits leads AND releases): either raft's side-channel
  pattern (`by_mut` on a `tick.source_iter(q!(Vec::new()))` stream,
  `raft.rs:984-991`) or a per-tick tuple singleton split by two maps —
  side-channel is the proven one.

**Do not change any public signature** of `multi_paxos` /
`multi_paxos_live` / `MultiPaxosOutputs`: the gauntlet work order codes
against them concurrently. Internals only.

## 4. Verification protocol (in order; all must pass)

1. `cargo test -p hydro_std` — 79/79. The sim suite is the safety net for
   this refactor; that is what it exists for. Run under a watchdog
   (`perl -e 'alarm N; exec @ARGV' cargo test ...`).
2. `cargo test -p hydro_test multi_paxos_some_throughput -- --nocapture`:
   the latency curve must be **flat** (p50 not growing across windows).
   That is the acceptance bar for the growth fix.
3. `cargo test -p hydro_test raft_some_throughput -- --nocapture` re-run
   the same day for a fair baseline; report both.
4. If throughput is still far below raft after flattening, profile the
   constant factor next; known intrinsic costs, in likely order: the
   O(n²) learner echo (consider: proposer→learner direct + echo only as
   anti-entropy), per-request splice-fact fan-out (two facts per decree),
   the response-path `unique()`, mint slice overheads (§2 item 4). Fix →
   re-measure → commit, one variable at a time.
5. Update the perf numbers in the rung-4 commit's claims wherever they are
   quoted (the bench commit message is history; the accounting doc §6.2
   and portfolio table get the new numbers).

## 5. Read before starting

- `raft.rs:920-1010` — the by_mut discipline (the existence proof).
- `multi_paxos.rs` — acceptor slice + leader kernel (the rewrite targets);
  module docs for the safety arguments that must survive.
- `multi_paxos_live.rs` — the wrapper kernel; `ElectionState` docs (the
  retirement fix already applied — the pattern to preserve).
- `synod.rs` module docs — the acceptor batch-serialization argument.
- `design_docs/2026-08_trust_and_complexity_accounting.md` §6.2 — where
  the measured cost story lands.
- Working rules that save a day: never name host generics inside `q!`;
  `sliced!` requires all `use::` declarations before other statements;
  staged code cannot call free functions (associated fns on pub types
  work); wire types referenced in `q!` must be `pub` (deploy-mode staging).

## 6. Recording the finding

Whatever the outcome, the result belongs in the accounting doc: the
zip/map/reassign pattern is the house style of the entire ladder
(synod, ABD, mints), and it now has a measured asymptotic cost that the
by_mut style avoids. One honest paragraph in
`2026-08_trust_and_complexity_accounting.md` §6.2 (complexity axis) with
the before/after numbers, plus a line in the ladder doc's rung-4 entry if
the acceptor rewrite changes its "tick-serialized slice" description.
