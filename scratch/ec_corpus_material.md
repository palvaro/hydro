# EC corpus material (branch `docs/hydrofrac-inventory` @ c28c36d24d)

Collected read-only from `git archive docs/hydrofrac-inventory` (extracted to `$TMPDIR/hf`; `git worktree add` failed because `.git` is read-only).
All paths below are relative to the repo root **on that branch**. Gallery = `docs/design/molly-viz-samples/ec/`; transcripts = `docs/design/molly-viz-samples/ec/output/`.

## 0. Provenance / staleness

- Code on the branch (`hydro_lang`, `hydro_test`, `hydro_std`) was last touched by **1538508755** (2026-10-21 02:12, "unbounded crash budget by default, with vacuity constraints"). Nothing after it changes code.
- Every transcript, gallery dir and `PRINT.md` was added by **e048756fcd** (2026-10-21 16:40, "import inventory draft…"); `PRINT.md` re-touched in 8ac5c6c1d4 (17:02). So by commit date **none predates** 1538508755.
- By content, they are post-crash-budget too: every transcript prints the new `ldfi: crash bounds: crashes unbounded; not all N nodes at once; at most c of the checked cluster's m crashable member(s)` line (multi-Paxos: `at most 1 crash(es)` = its explicit `.crash_budget(1)`), and the counts match the "Unbounded" column of `docs/design/ldfi-level-ablation.md` § "Unbounded crash budget with vacuity constraints" (also written in 1538508755).
- **Current HEAD of the user's checkout (bb8842cf85 sandbox) does NOT contain 1538508755**; the code under test only exists on the branch.
- Could not re-run anything: the `ldfi` feature needs `highs-sys` → **cmake + libclang, neither installed** on this host (no cached highs build in `target/` either). All numbers below are from committed files.
- The `ec/output/*.txt` files carry no total wall time; the `test <unknown> ... run time: X ms` lines are per-simulator-instance, not per test. Wall seconds come from the ablation doc table (measured on 1538508755, "one run each, warm build, --test-threads=1", 8×Xeon 8375C, debug build, ±30 % noise per that doc's header).

## 1. How to run / render

- Single test (each transcript's first line is the exact command):
  `cargo test -p hydro_test --lib cluster::<module>::tests::<test> -- --nocapture --test-threads=1 --exact`
  (ABD: `cargo test -p hydro_std --lib ec_inference_demos::abd::hydrofrac_tests::all_labels_reports_the_register_as_not_a_stream -- --nocapture --test-threads=1 --exact`).
  Needs cmake + libclang (HiGHS). None of the EC tests is `#[ignore]`d (the only ignored tests in those files are `isr_replication::tests::ldfi_hunt_false_certificate` and one in `two_pc_ldfi.rs:824`, not EC).
- Diagrams: only ONE committed gallery test produces an EC diagram: `cluster::ec_ldfi::tests::gallery_ec_broadcast_closed_all_labels` (no-op unless `MOLLY_VIZ_OUT` set):
  ```sh
  MOLLY_VIZ_OUT=/tmp/gallery cargo test -p hydro_test --lib cluster::ec_ldfi::tests::gallery_ec_broadcast_closed_all_labels -- --test-threads=1 --exact
  # all gallery tests: MOLLY_VIZ_OUT=/tmp/gallery cargo test -p hydro_test gallery_ -- --test-threads=1
  for f in /tmp/gallery/*/*.dot; do
    if grep -q layout=neato "$f"; then neato -n2 -Tpng -Gdpi=110 "$f" -o "${f%.dot}.png"
    else dot -Tpng -Gdpi=96 "$f" -o "${f%.dot}.png"; fi
  done
  ```
  Template (from `ec_ldfi.rs:317`): run the search with `.with_traces().with_provenance_graphs()`, pick `results.searched().find(|(_, o)| o.is_counterexample())`, call `ldfi_viz::write_gallery(&dir, "<title>", Level::Precise, outcome)` (`gallery_dir` / `write_gallery` in `hydro_test/src/cluster/ldfi_viz.rs:223,237`). The other 14 `ec/` entries have **no committed test**; regenerating needs one per entry on that template.
- All 15 `failing.dot` are `layout=neato`. I verified: re-rendering with `neato -n2 -Gdpi=110` reproduces the committed pixel size for ISR/2PC/multi-Paxos entries; **`ec_broadcast_closed_all_labels/failing.png` (758×499) was rendered at 96 dpi**, not 110 (minor inconsistency with PRINT.md's "110 dpi").
- Print re-render (`failing-print.png`, exists for the 3 ISR + 5 multi-Paxos entries): font ×1.6, arrows ×1.4, penwidth 1.5, `neato -n2 -Gdpi=160` — recipe in `ec/PRINT.md`.
- Each `ec/<entry>/` has `failing.dot`, `failing.png`, `report.md` (Mermaid sequence diagram + "Found after N hypothesis run(s), S schedule(s) explored").

## 2. Per-example material

Legend: **cmd** = run the single test; **out** = committed transcript; **dia** = Lamport diagram dir under `docs/design/molly-viz-samples/ec/`; **t(unb)** = wall seconds under the current (unbounded, post-1538508755) bounds and **t(1cr)** = the former one-crash budget (PRE-1538508755 numbers), both from `ldfi-level-ablation.md` § "Unbounded crash budget…" table; per-test totals (compile excluded? the doc says "warm build"; first test of each run includes the dylib prebuild). Naive fault space: see §3.

Shared helper shapes: every `hydro_test` EC search is
`Ldfi::new().level(Level::Precise)[.exploration(..)].crashable(..).crashable_cluster(..)[.crash_budget(k)].spec(&spec).search(..)|.search_all_labels(..)`
on `flow.sim().skip_consistency_assertions().with_cluster_size(..).with_provenance().compiled()`.

### 2.1 `broadcast_closed` process → cluster, all labels (gallery template)
- **test** `ec_broadcast_closed_all_labels` (+ gallery twin `gallery_ec_broadcast_closed_all_labels`), `hydro_test/src/cluster/ec_ldfi.rs:298-345` (helper `broadcast_closed_all_labels`, :260-295).
```rust
let source = flow.process::<Source>();  let members = flow.cluster::<Member>();   // 3 members
let (send, items) = source.sim_input::<Item, TotalOrder, ExactlyOnce>();
let at_members = items.broadcast_closed(&members, TCP.fail_stop().name("bcast").bincode()); // ec_ldfi.rs:270
let _out = at_members.weaken_consistency().sim_cluster_output();
let spec = Spec::all_labels(&mut flow);
Ldfi::new().level(Level::Precise)
    .crashable("source", &source).crashable_cluster("member", &members, 0..3)
    .spec(&spec)
    .search_all_labels(&compiled, async |_run| {
        send.send(Item(1)); send.send(Item(2)); hydro_lang::sim::quiesce().await;
    })
```
- **cmd** `cargo test -p hydro_test --lib cluster::ec_ldfi::tests::ec_broadcast_closed_all_labels -- --nocapture --test-threads=1 --exact`; diagram: `MOLLY_VIZ_OUT=/tmp/gallery cargo test -p hydro_test --lib cluster::ec_ldfi::tests::gallery_ec_broadcast_closed_all_labels -- --test-threads=1 --exact`
- **out** `ec/output/ec_broadcast_closed_all_labels.txt`
  `EventualConsistency at ec_ldfi.rs:270 (Cluster(loc2v1), AssertIsConsistent, minted and delivered): VIOLATED by {crash(source after bcast[→1]:Item(2))}, 5 hypotheses` (exhaustive, 1 schedule/hypothesis)
- **dia** `ec_broadcast_closed_all_labels/failing.{dot,png}`, report.md: 5 hyps, 5 schedules.
- **t** unb 5 hyps, 6.2 s (1cr: n/a, new test).

### 2.2 `broadcast_closed` process → cluster, hand spec (`fail_stop`)
- **test** `ec_broadcast_closed_fail_stop_breaks_under_a_sender_crash`, `ec_ldfi.rs:205` → `process_to_cluster(Transport::FailStop)` (:64-118): same flow but `Spec::new` + `spec.eventual_consistency(spec.observe_cluster(at_members.weaken_consistency()), spec.correct_members(&members, 0..3))`, `.search(..)` same body.
- **cmd** `cargo test -p hydro_test --lib cluster::ec_ldfi::tests::ec_broadcast_closed_fail_stop_breaks_under_a_sender_crash -- --nocapture --test-threads=1 --exact`
- **out** `ec/output/ec_broadcast_closed_fail_stop_breaks_under_a_sender_crash.txt`: `LDFI: goal violated after 5 hypothesis run(s) by {crash(source after bcast[→1]:Item(2))}`
- **dia** none of its own (same run as 2.1). **t** unb 5 / 43.7 s (includes dylib prebuild); 1cr 5 / 72.6 s (prebuild).

### 2.3 `broadcast_closed` cluster → cluster
- **test** `ec_broadcast_closed_cluster_to_cluster_breaks_under_a_source_crash`, `ec_ldfi.rs:234` → `cluster_to_cluster()` (:120-153)
```rust
let sources = flow.cluster::<Source>();  /* 2 */  let members = flow.cluster::<Member>(); /* 3 */
let (send, items) = sources.sim_input::<Item, TotalOrder, ExactlyOnce>();
let at_members = items.broadcast_closed(&members, TCP.fail_stop().name("bcast").bincode()).values();
let mut spec = Spec::new(&mut flow);
let correct = spec.correct_members(&members, 0..3);
let observed = spec.observe_cluster(at_members.weaken_consistency());
spec.eventual_consistency(observed, correct);
Ldfi::new().level(Level::Precise)
    .crashable_cluster("source", &sources, 0..2).crashable_cluster("member", &members, 0..3)
    .spec(&spec)
    .search(&compiled, async |_run| { send.send(0, Item(10)); send.send(1, Item(20)); hydro_lang::sim::quiesce().await; })
```
- **cmd** `cargo test -p hydro_test --lib cluster::ec_ldfi::tests::ec_broadcast_closed_cluster_to_cluster_breaks_under_a_source_crash -- --nocapture --test-threads=1 --exact`
- **out** `ec/output/ec_broadcast_closed_cluster_to_cluster_breaks_under_a_source_crash.txt`: `LDFI: goal violated after 5 hypothesis run(s) by {crash(source@0 after bcast[0→1]:Item(10))}`
- **dia** `ec_broadcast_closed_cluster_to_cluster/` (5 hyps, 5 sched). NB: produced from this `.search` test, not an `all_labels` test; no committed gallery test.
- **t** unb 5 / 5.6 s; 1cr 5 / 5.5 s.

### 2.4 `broadcast_closed` over `lossy_delayed_forever`
- **test** `ec_broadcast_closed_lossy_delayed_forever_breaks_under_one_omission`, `ec_ldfi.rs:223` → `process_to_cluster(Transport::LossyDelayedForever)`: channel `TCP.lossy_delayed_forever().name("bcast").bincode()`, sim additionally `.test_safety_only()`; same builder/body as 2.2.
- **cmd** `cargo test -p hydro_test --lib cluster::ec_ldfi::tests::ec_broadcast_closed_lossy_delayed_forever_breaks_under_one_omission -- --nocapture --test-threads=1 --exact`
- **out** `ec/output/ec_broadcast_closed_lossy_delayed_forever_breaks_under_one_omission.txt`: `LDFI: goal violated after 2 hypothesis run(s) by {omit(bcast[→0]:Item(1))}`
- **dia** `ec_broadcast_closed_lossy/` (2 hyps, 2 sched). **t** unb 2 / 2.3 s; 1cr ("was budget 0") 2 / 2.2 s.

### 2.5 Plain vs echo broadcast under the generated spec
- **test** `ec_generated_spec_separates_plain_from_reliable_broadcast`, `ec_ldfi.rs:248` → `echo_broadcast(echo)` (:155-190): `broadcast(&members, source, echo, nondet!(hook=initial_hook), nondet!(hook=echo_hook))` from `cluster/reliable_broadcast.rs`; `Spec::new` + `eventual_consistency`; `.crashable("broadcaster", ..).crashable_cluster("member", .., 0..3)`; body `source_send.send(Payload(42)); quiesce`.
- **cmd** `cargo test -p hydro_test --lib cluster::ec_ldfi::tests::ec_generated_spec_separates_plain_from_reliable_broadcast -- --nocapture --test-threads=1 --exact`
- **out** `ec/output/ec_generated_spec_separates_plain_from_reliable_broadcast.txt`: plain `LDFI: goal violated after 2 hypothesis run(s) by {crash(broadcaster after initial[→1]:Payload(42))}`; echo `LDFI: no fault set within bounds violates the goal under any schedule (21 hypothesis run(s))`
- **dia** none in `ec/` (the hand-spec version is `molly-viz-samples/plain_broadcast/`, from `gallery_plain_broadcast` in `cluster/reliable_broadcast.rs`).
- **t** unb 2 / 21, 24.0 s; 1cr 2 / 10, 20.5 s.

### 2.6 `broadcast_live_from_process` (hydro_std)
- **test** `ec_demo_broadcast_live_breaks_under_a_sender_crash`, `hydro_test/src/cluster/ec_demos_ldfi.rs:311` → `broadcast_search(Broadcast::Live)` (:66-121):
```rust
let delivered = broadcast_live_from_process(items, &members, TCP.fail_stop().name("bcast").bincode()).weaken_consistency();
// Spec::new + eventual_consistency(observe_cluster(delivered), correct_members(&members, 0..3))
Ldfi::new().level(Level::Precise).exploration(Exploration::Exhaustive)
    .crashable("source", &source).crashable_cluster("member", &members, 0..3).spec(&spec)
    .search(&compiled, async |_run| { send.send(1); send.send(2); hydro_lang::sim::quiesce().await; })
```
- **cmd** `cargo test -p hydro_test --lib cluster::ec_demos_ldfi::tests::ec_demo_broadcast_live_breaks_under_a_sender_crash -- --nocapture --test-threads=1 --exact`
- **out** `…/ec_demo_broadcast_live_breaks_under_a_sender_crash.txt`: `violated after 5 … {crash(source after bcast[→1]:2)}`. **dia** none. **t** unb 5 / 5.9 s; 1cr 5 / 5.9 s.

### 2.7 `leader_merge_broadcast`
- **test** `ec_demo_leader_merge_breaks_under_a_leader_crash`, `ec_demos_ldfi.rs:364` → `leader_merge_search()` (:183-221): 2 writers → `writes.send(&leader, TCP.fail_stop().name("to_leader"))` → `leader_merge_broadcast(at_leader, &replicas, TCP.fail_stop().name("merged"), nondet!(..))`; compiled **without provenance** (rewrite can't handle `entries_partially_ordered`); `.crashable("leader", ..).crashable_cluster("replica", .., 0..3)`; body `write.send(0,100); write.send(1,200); quiesce`.
- **cmd** `cargo test -p hydro_test --lib cluster::ec_demos_ldfi::tests::ec_demo_leader_merge_breaks_under_a_leader_crash -- --nocapture --test-threads=1 --exact`
- **out** `…/ec_demo_leader_merge_breaks_under_a_leader_crash.txt`: `violated after 2 … {crash(leader after merged[→1]:(MemberId::<…Writer>(0), 100))}`. **dia** none. **t** unb 2 / 5.1 s; 1cr 2 / 6.6 s.

### 2.8 Reliable broadcast, all labels (inner hops violated, delivered set certified)
- **test** `ec_demo_reliable_broadcast_all_labels`, `ec_demos_ldfi.rs:394-455`
```rust
let delivered = reliable_broadcast_closed(items, &members);          // hydro_std reliable_broadcast.rs:130 / :156 / :149
let _out = delivered.weaken_consistency().sim_cluster_output();
let spec = Spec::all_labels(&mut flow);
let results = Ldfi::new().level(Level::Precise)
    .crashable("source", &source).crashable_cluster("member", &members, 0..3).spec(&spec)
    .search_all_labels(&compiled, async |_run| { send.send(1); send.send(2); hydro_lang::sim::quiesce().await; });
// asserts: results.at("reliable_broadcast.rs:149") is Certificate with runs.len()==36; :130 violated by source crash
```
- **cmd** `cargo test -p hydro_test --lib cluster::ec_demos_ldfi::tests::ec_demo_reliable_broadcast_all_labels -- --nocapture --test-threads=1 --exact`
- **out** `…/ec_demo_reliable_broadcast_all_labels.txt`:
  - `:130 (…, minted): VIOLATED by {crash(source after reliable_broadcast.rs:130[→1]:2)}, 5 hypotheses`
  - `:156 (…, minted): VIOLATED by {crash(member@0 after reliable_broadcast.rs:156[0→1]:2)}, 3 hypotheses`
  - `:149 (…, Unique, delivered): CERTIFICATE, 36 hypotheses, exhaustive`
- **dia** `ec_reliable_broadcast_inner_source_hop/` (:130, 5 hyps/5 sched), `ec_reliable_broadcast_inner_echo/` (:156, 3/3). Dataflow figure `molly-viz-samples/figures/reliable_broadcast_regions.dot` (hand-written, `dot -Tpng -Gdpi=150`).
- **t** unb 5/3/36, 7.9 s; 1cr 5/3/10, 7.3 s.

### 2.9 Reliable broadcast closed + live, hand spec
- **test** `ec_demo_reliable_broadcast_certifies`, `ec_demos_ldfi.rs:324` → `broadcast_search(ReliableClosed)` and `(ReliableLive)` (exhaustive).
- **cmd** `cargo test -p hydro_test --lib cluster::ec_demos_ldfi::tests::ec_demo_reliable_broadcast_certifies -- --nocapture --test-threads=1 --exact`
- **out** `…/ec_demo_reliable_broadcast_certifies.txt`: twice `no fault set within bounds violates the goal under any schedule (36 hypothesis run(s))`. **dia** none. **t** unb 36+36, 12.2 s; 1cr 10+10, 11.6 s.

### 2.10 Uniform reliable broadcast (thresholds 1, 2)
- **test** `ec_demo_uniform_broadcast`, `ec_demos_ldfi.rs:339` → `broadcast_search(Uniform(t))`: `uniform_reliable_broadcast_closed(items, &members, t)`, `Exploration::Fuzz { instances: 8 }` + `.unit_test_fuzz_iterations(8)` (exhaustive "ran past 240,000 interleavings").
- **cmd** `cargo test -p hydro_test --lib cluster::ec_demos_ldfi::tests::ec_demo_uniform_broadcast -- --nocapture --test-threads=1 --exact`
- **out** `…/ec_demo_uniform_broadcast.txt`: `uniform(1)/(2): … no fault set within bounds violated the goal in 8 sampled schedule(s) per hypothesis (105 hypothesis run(s)); not a proof`. **dia** none. **t** unb 105+105, 22.1 s; 1cr 28+28, 14.8 s.

### 2.11 G-Set gossip (closed + live)
- **test** `ec_demo_g_set_gossip_certifies`, `ec_demos_ldfi.rs:356` → `gossip_search(live)` (:127-180): 3 members, `g_set_gossip[_live](&members, updates, ticks)`, observed via member tick `snapshot(..).into_stream().flat_map_unordered(..).all_ticks()`; `Fuzz{8}`; `.crashable_cluster("member", .., 0..3)` only; body: `update.send(m, 10+m)` ∀m, quiesce, then 3× `tick.send_many(all members)`, quiesce.
- **cmd** `cargo test -p hydro_test --lib cluster::ec_demos_ldfi::tests::ec_demo_g_set_gossip_certifies -- --nocapture --test-threads=1 --exact`
- **out** `…/ec_demo_g_set_gossip_certifies.txt`: twice `no fault set … violated the goal in 8 sampled schedule(s) per hypothesis (13 hypothesis run(s)); not a proof`. **dia** none. **t** unb 13+13, 26.0 s; 1cr 13+13, 29.2 s.

### 2.12 Multi-Paxos `learned`, hand spec, `.crash_budget(1)`
- **test** `ec_demo_multi_paxos_learned_under_one_crash`, `ec_demos_ldfi.rs:379` → `multi_paxos_search()` (:229-276):
```rust
let outputs = multi_paxos(&acceptors, &learners, 2, leads, commands);   // 1 proposer, 3 acceptors, 3 learners
let learned = outputs.learned.weaken_consistency();
// Spec::new + eventual_consistency(observe_cluster(learned), correct_members(&learners, 0..3))
Ldfi::new().level(Level::Precise).exploration(Exploration::Fuzz { instances: 8 })
    .crashable_cluster("proposer", &proposers, 0..1)
    .crashable_cluster("acceptor", &acceptors, 0..3)
    .crashable_cluster("learner", &learners, 0..3)
    .crash_budget(1)
    .spec(&spec)
    .search(&compiled, async |_run| {
        lead.send(0, 1); hydro_lang::sim::quiesce().await;
        command.send(0, 7); command.send(0, 8); hydro_lang::sim::quiesce().await;
    })
```
- **cmd** `cargo test -p hydro_test --lib cluster::ec_demos_ldfi::tests::ec_demo_multi_paxos_learned_under_one_crash -- --nocapture --test-threads=1 --exact`
- **out** `…/ec_demo_multi_paxos_learned_under_one_crash.txt`: `no fault set … in 8 sampled schedule(s) per hypothesis (25 hypothesis run(s)); not a proof`. **dia** none.
- **t** budget(1) after fix: 25 hyps, 15.0 s. 1cr (pre-fix): 24 / 15.0 s. **Unbounded (no budget): Budget 256 at 211.9 s; with max_hypotheses(2000): no verdict after 659 hyps, >30 min** (1/24/101/299/161/73 hyps of 0…5 crashes).

### 2.13 Multi-Paxos, all labels, `.crash_budget(1)`
- **test** `ec_demo_multi_paxos_all_labels`, `ec_demos_ldfi.rs:457-515`: same flow with `let _learned = outputs.learned.weaken_consistency().sim_cluster_output(); let spec = Spec::all_labels(&mut flow);`, same builder (`Fuzz{8}`, budget 1), `.search_all_labels(..)` same body. Asserts `:588` exit certifies and no `Budget`.
- **cmd** `cargo test -p hydro_test --lib cluster::ec_demos_ldfi::tests::ec_demo_multi_paxos_all_labels -- --nocapture --test-threads=1 --exact`
- **out** `…/ec_demo_multi_paxos_all_labels.txt` (434 lines):
  - `:281` acceptors, minted+delivered: VIOLATED `crash(proposer@0 after multi_paxos.rs:281[0→1]:Ts{round:1,..})`, 5
  - `:286` CycleSource delivered: VIOLATED `crash(proposer@0 after multi_paxos.rs:549[0→1]:(Ts{..},0,1,Some(8)))`, 8
  - `:549` minted: VIOLATED (same crash), 8
  - `:569` proposers minted: CERTIFICATE, 6, 8 sampled
  - `:570` Map delivered: CERTIFICATE, 6, 8 sampled
  - `:582` learners minted: VIOLATED `crash(proposer@0 after multi_paxos.rs:582[0→1]:(1,0,0,Some(7)))`, 11
  - `:588` Unique delivered: CERTIFICATE, 25, 8 sampled
  - `:591` minted: VIOLATED `crash(learner@0 after multi_paxos.rs:591[0→1]:(1,0,0,Some(7)))`, 9
  - `:608` FlatMap delivered: CERTIFICATE, 26, 8 sampled
  - `epoch_splice.rs:190`: not checked: Singleton
- **dia** `ec_multi_paxos_labels_281_prepares/` (5 hyps, 40 sched), `_286_accepts_fwd/` (8, 64), `_549_accepts/` (8, 64), `_582_learner_hop/` (**10**, 80 — transcript says 11; Fuzz variance), `_591_learner_echo/` (9, 72). All have `failing-print.png`.
- **t** budget(1) after fix: 32.5 s total (doc paragraph). 1cr pre-fix: 5/8/8/18/18/10/10/25/25, 33.5 s. Unbounded: 5/8/8/23/23/11/8/**Budget 256**/**Budget 256**, 445.8 s.

### 2.14 Label inside a tick (class boundary)
- **test** `all_labels_reports_a_label_inside_a_tick`, `hydro_test/src/cluster/ec_all_labels_classes.rs:22-94`
```rust
let at_members = items.broadcast_closed(&members, TCP.fail_stop().name("bcast").bincode());       // :31
let tick = members.tick();
let in_tick = at_members.batch(&tick, nondet!(/** test */))
    .assert_has_consistency_of::<Tick<Cluster<'_, Member, EventualConsistency>>>(manual_proof!(..)) // :36
    .map(q!(|x| x * 10));
let _out = in_tick.all_ticks().weaken_consistency().sim_cluster_output();                             // :41
let spec = Spec::all_labels(&mut flow);
// builder: .crashable("source").crashable_cluster("member", 0..3); body send 1, 2; quiesce
```
- **cmd** `cargo test -p hydro_test --lib cluster::ec_all_labels_classes::tests::all_labels_reports_a_label_inside_a_tick -- --nocapture --test-threads=1 --exact`
- **out** `…/all_labels_reports_a_label_inside_a_tick.txt`: `:31 VIOLATED by {crash(source after bcast[→1]:2)}, 5`; `:36 not checked: inside a tick`; `:41 (YieldConcat, delivered) VIOLATED (same), 5`. **dia** none. **t** unb 14.2 s; 1cr 14.5 s.

### 2.15 ISR replication, all labels, senders crashable
- **test** `ldfi_all_labels_break_under_a_sender_crash`, `hydro_test/src/cluster/isr_replication.rs:1205` → `harness::compile_all_labels(false, Encoding::Dataflow, DEFAULT_SHAPE /*3 replicas, 4 rounds, 1 write*/, true, 8)` (:934-1001) + `configure(None).exploration(Exploration::Fuzz { instances: 8 }).search_all_labels(&compiled, async |run| body(&h, run).await)`.
  `compile_all_labels`: `isr_replication(&client,&zk,&replicas,..)`; outputs to `sim_output`/`sim_cluster_output`; `Spec::all_labels`; sim `.test_safety_only()`; `Ldfi::new().level(Precise).crashable_cluster("replica", .., 0..3)` + (senders_crashable) `.crashable("client").crashable("zk")`. Channels: `write`, `view`, `ack`, `hb`, `shrink`, `client-ack` = `fail_stop`; **`repl` = `lossy_delayed_forever`**.
  `body` (:906-925): `write_send.send(Write(w))` ∀w; quiesce; for round in 0..4 { replica_timer.send_many(all); quiesce; zk_timer.send(()); quiesce; if round==0 { run.end_of_finite_failures() } }.
- **cmd** `cargo test -p hydro_test --lib cluster::isr_replication::tests::ldfi_all_labels_break_under_a_sender_crash -- --nocapture --test-threads=1 --exact`
- **out** `…/ldfi_all_labels_break_under_a_sender_crash.txt`: `:343 VIOLATED {crash(client after write[→1]:Write(1))}, 2`; `:360 (CycleSource) VIOLATED {crash(zk after view[→1]:View{leader:Some(0),isr:[0,1,2]})}, 8`; `:564 VIOLATED (same zk crash), 8`.
- **dia** `ec_isr_all_labels_343_write/` (2 hyps, 16 sched), `_360_replication_fwd/` (8, 64), `_564_view/` (8, 64); all have `failing-print.png`.
- **t** unb 9.4 s; 1cr 9.3 s.

### 2.16 ISR, all labels, harness fault model (client + ZK correct)
- **test** `ldfi_all_labels_with_the_harness_fault_model`, `isr_replication.rs:1177` (`compile_all_labels(.., senders_crashable=false, 8)`).
- **cmd** `cargo test -p hydro_test --lib cluster::isr_replication::tests::ldfi_all_labels_with_the_harness_fault_model -- --nocapture --test-threads=1 --exact`
- **out** ⚠ **no transcript**: `ec/output/ldfi_all_labels_with_the_harness_fault_model.txt` is the **2PC** test of the same name (its header line is `cluster::two_pc_ldfi::…`). Numbers (no violation 4 / 7 / 7) only exist in the ablation table. **t** unb 19.7 s; 1cr 19.8 s. The corpus table's "EFF moved to just after the write" row (V 2 / 5 / 5) is from an earlier body; no test/transcript.

### 2.17 2PC with cooperative termination, all labels, coordinator crashable
- **test** `ldfi_all_labels_with_the_harness_fault_model`, `hydro_test/src/cluster/two_pc_ldfi.rs:610` → `harness::compile_all_labels(2, /*ctp*/true, /*coordinator_crashable*/true, 8)` (:358-425) + `configure(None).search_all_labels(&compiled, async |run| body(&h, run).await)` (exhaustive).
  `compile_all_labels`: `two_pc(&coordinator,&participants,2,txns,timeouts,ctp, nondet!(hook=prepare_hook), …commit, peer, vote, timeout hooks)`; `committed.sim_cluster_output()`; `Spec::all_labels`; `Ldfi::new().level(Precise).crashable_cluster("participant", .., 0..2)` + `.crashable("coordinator", ..)`. `body` (:341): `txn_send.send(Txn(1))`; quiesce; `timeout_send.send_many(all participants)`; quiesce.
- **cmd** `cargo test -p hydro_test --lib cluster::two_pc_ldfi::tests::ldfi_all_labels_with_the_harness_fault_model -- --nocapture --test-threads=1 --exact`
- **out** `…/ldfi_all_labels_with_the_harness_fault_model.txt`: `:86 VIOLATED {crash(coordinator after prepare[→0]:Txn(1))}, 2`; `:92 VIOLATED {crash(coordinator after commit[→0]:Txn(1))}, 2`; `:98` and `:161` `VACUOUS (nothing held in the failure-free run)`.
- **dia** `ec_two_pc_all_labels_86_prepare/` (2 hyps, 20 sched), `ec_two_pc_all_labels_92_commit/` (2, 64).
- **t** unb 12.5 s; 1cr 12.4 s.

### 2.18 2PC, all labels, coordinator correct
- **test** `ldfi_all_labels_with_the_coordinator_correct`, `two_pc_ldfi.rs:640` (`compile_all_labels(2, true, false, 8)`).
- **cmd** `cargo test -p hydro_test --lib cluster::two_pc_ldfi::tests::ldfi_all_labels_with_the_coordinator_correct -- --nocapture --test-threads=1 --exact`
- **out** `…/ldfi_all_labels_with_the_coordinator_correct.txt`: `:86`/`:92 CERTIFICATE, 1 hypotheses, exhaustive`; `:98`/`:161` VACUOUS. **dia** none. **t** unb 1/1, 4.4 s; 1cr 3/5, 4.7 s.

### 2.19 ABD register, all labels
- **test** `all_labels_reports_the_register_as_not_a_stream`, `hydro_std/src/ec_inference_demos/abd.rs:622-680` (module `hydrofrac_tests`)
```rust
let replicas = flow.cluster::<Replica>(); let clients = flow.cluster::<Client>();   // 3 replicas, 1 client
let outs = abd_register(&replicas, 3/2 + 1, writes, reads);
let _done = outs.write_done.sim_cluster_output(); let _read = outs.read_result.sim_cluster_output();
let spec = Spec::all_labels(&mut flow);
Ldfi::new().level(Level::Precise).exploration(Exploration::Fuzz { instances: 8 })
    .crashable_cluster("replica", &replicas, 0..3).crashable_cluster("client", &clients, 0..1)
    .spec(&spec)
    .search_all_labels(&compiled, async |_run| { w_send.send(0, (1, 10u32)); hydro_lang::sim::quiesce().await; })
```
- **cmd** `cargo test -p hydro_std --lib ec_inference_demos::abd::hydrofrac_tests::all_labels_reports_the_register_as_not_a_stream -- --nocapture --test-threads=1 --exact`
- **out** `…/all_labels_reports_the_register_as_not_a_stream.txt`: `:141 VIOLATED {crash(client@0 after abd.rs:141[0→1]:Query{rid:1})}, 2`; `:150`, `:161`, `:314` VIOLATED `{crash(client@0 after abd.rs:314[0→1]:Write{rid:1,..,value:10})}`, 2 each; `:165 not checked: Singleton`. **dia** none. **t** unb 32.1 s; 1cr did not compile (`Level::Record`).

All source line references cited in the corpus doc (`ec_ldfi.rs:270`, `two_pc_ldfi.rs:86/92/98/161`, `isr_replication.rs:343/360/564`, `abd.rs:141/150/161/165/314`, `multi_paxos.rs:281…608`, `epoch_splice.rs:190`, `reliable_broadcast.rs:130/149/156`, `ec_all_labels_classes.rs:31/36/41`) were checked against the branch source and point at the right operators.

## 3. Iterations vs brute force (item e)

### 3.1 What exists for `Strategy::Blind`
**No `Strategy::Blind` run of any EC-corpus test (generated `EventualConsistency` spec) exists**, before or after 1538508755. `Strategy::Blind` (`hydro_lang/src/sim/ldfi.rs:4424`) = same loop, no cover clauses: it enumerates every untried admissible fault set over `observed` (candidate faults of *every observed delivery*, ⊇ the lineage universe), weakest first, honouring the same crash bounds and vacuous-superset pruning, and ends in `Outcome::Exhausted` instead of a certificate. The `ldfi_ablation_*.rs` harnesses (`LDFI_ABLATION_OUT=<md>`) cover 2PC, ISR, broadcast/delivery, Raft with hand-written specs and compare *levels*, not Blind; Blind comparisons come from `compare_search_strategies` (`two_pc_ldfi.rs`, `isr_replication.rs:1259`) and `ldfi_grid.rs`. Existing Blind numbers on the *same protocols* (all **PRE-1538508755**, explicit crash budgets, hand-written specs):

| protocol / spec | LDFI hyps | Blind hyps | source (commit date) |
|---|---|---|---|
| echo reliable broadcast n=3, agreement, 1 crash, exhaustive | 10 (cert) | 13, exhausted (12 crash points) | `ldfi-level-ablation.md` § Diagnostic … broadcast (0bbf22d080, 2026-10-19) |
| same, 2 crashes | 32 (cert) | 58, exhausted (67 admissible sets − 9 vacuous supersets) | same |
| plain 2PC n=2 cr=1 | 2 (viol) | 2 | `ldfi-on-sim-hooks.md` (9d0ec39c92, ≤2026-10-18) |
| 2PC+CTP n=2 cr=1 | 7 (cert) | 13 exhausted | same |
| 2PC+CTP n=2 cr=2 | 8 (viol) | 14 | same |
| 2PC+CTP n=3 cr=1 / cr=2 | 10 cert / 13 viol | 34 exhausted / cancelled >37 runs (~10 min) | same |
| 2PC+CTP cr=1 certificate, n=2..6 (`ldfi/1`) | 7,10,13,16,19 | 13,25,37,81,139 exhausted | same, measurement grid |
| 2PC+CTP cr=2 violation, n=2..6 (instances) | 8,18,21,82,119 (median) | 14,48,96,168,275 | same |
| ISR unclean cr=1 (closure / dataflow) | 19 / 15 | 25 / 25 | same |
| ISR clean cr=1 certificate (closure / dataflow) | 42 / 28 | 88 / 88 exhausted | same |
| Raft (staged body, withdrawn) | 25 | 160 | same (comparison later withdrawn in history doc) |

Docs' own summary: "blind enumeration ties LDFI wherever one crash suffices, because the weakest-first order alone finds it"; "lineage beats blind … by 2–7× on certificates". Every EC-corpus counterexample is one fault, so the EC violations are exactly the regime where Blind ≈ LDFI.

### 3.2 Naive fault-space size per EC search (derived here, from the committed transcripts)
Method (`$TMPDIR/work/space.py`): take the last `ldfi: universe {…}` line of each search (the lineage universe; Blind's universe is ⊇ this, so these are **lower bounds** for brute force), count crash points per node and omissions, and count admissible fault sets = at most one crash point per node, any subset of omissions, subject to the printed bounds (crash budget if any; not all N nodes; ≤ cap crashes in the checked cluster). Includes the empty set; does not subtract vacuous supersets (Blind would). "Singletons" = admissible one-fault sets; Blind tries all of them (in its fixed order) before any pair, so for a 1-fault counterexample Blind needs between 2 and 1+singletons hypotheses.

| test / label | universe (crash pts per node, omissions) | bounds | singletons | naive admissible sets | LDFI hyps (verdict) |
|---|---|---|---|---|---|
| bcast_closed all_labels `:270` / fail_stop hand / broadcast_live / classes `:31`,`:41` / RB `:130` | source 6, member 1×3 | unb., N=4, cap 1 | 9 | 28 | 5 (V) |
| bcast_closed cluster→cluster | source@0 3, source@1 3, member 1×3 | unb., N=5, cap 1 | 9 | 64 | 5 (V) |
| bcast_closed lossy | source 6, member 1×3, **6 omissions** | unb., cap 1 | 15 | 1,792 | 2 (V) |
| plain broadcast (generated spec) | broadcaster 3, member 1×3 | unb. | 6 | 16 | 2 (V) |
| echo broadcast (generated spec) | broadcaster 3, members 3/2/1 | unb. | 9 | 28 | 21 (C) |
| leader_merge | leader 7, replica 1×3 | unb., N=6 | 10 | 32 | 2 (V) |
| RB all_labels `:156` echo | source 6, member 7×3 | unb., cap 1 | 27 | 154 | 3 (V) |
| RB `:149` delivered (all_labels) / RB closed, live (hand) | source 6, members 5/3/1 | unb., cap 1 | 15 | 70 | 36 (C) each |
| uniform(1), uniform(2) | source 6, member 7×3 | unb., cap 1 | 27 | 154 | 105 (no viol., Fuzz 8) |
| g_set_gossip / _live | member 6/7/8 ; 6/8/10 | unb., N=3, cap 1 | 21 ; 24 | 22 ; 25 | 13 ; 13 (no viol., Fuzz 8) |
| multi-Paxos `:281` | proposer 3, acceptor 1×3 | budget 1 | 6 | 7 | 5 (V) |
| multi-Paxos `:286`, `:549` | proposer 10, acceptor 3×3 | budget 1 | 19 | 20 | 8 (V) |
| multi-Paxos `:569`, `:570` | proposer 14 (cap 0 ⇒ excluded), acceptor 4×3 | budget 1 | 12 | 13 | 6 (C) |
| multi-Paxos `:582` | proposer 18, acceptor 4×3, learner 3×3 | budget 1 | 39 | 40 | 11 (V) |
| multi-Paxos `:591` | proposer 18, acceptor 4×3, learner 7×3 | budget 1 | 51 | 52 | 9 (V) |
| multi-Paxos `:588`, `:608` / learned (hand) | proposer 18, acceptor 4×3, learner 7/3/3 | budget 1 | 43 | 44 | 25, 26 / 25 (C / no viol.) |
| multi-Paxos learned, **unbounded** (what the budget avoids) | same 43 points, 7 nodes | unb., N=7, cap 1 | 43 | **33,250** = 19 (proposer) × 5³ (acceptors) × 14 (≤1 learner); the N−1 cut removes nothing here | Budget 256 at 212 s; 659 hyps no verdict >30 min |
| ISR `:360`, `:564` | zk 3, replica 2×3 | unb., N=5, cap 1 | 9 | 28 | 8 (V) |
| ISR `:343` | client 3, replica 1×3 | unb., cap 1 | 6 | 16 | 2 (V) |
| 2PC `:86` (coord. crashable) | coordinator 2 (participants cap 0) | unb., N=3, cap 0 | 2 | 3 | 2 (V) |
| 2PC `:92` (coord. crashable) | coordinator 4 | cap 0 | 4 | 5 | 2 (V) |
| 2PC `:86`/`:92` coord. correct | participants only, cap 0 | — | 0 | 1 | 1 (C) |
| ABD `:150`/`:161`/`:314` | client@0 6, replica 2×3 | unb., N=4, cap 1 | 12 | 49 | 2 (V) |
| ABD `:141` | client@0 3, replica 1×3 | unb., cap 1 | 6 | 16 | 2 (V) |

Reading: for certificates (where brute force must exhaust the space) LDFI ran 36/70 (RB), 21/28 (echo), 105/154 (uniform), 13/22–25 (gossip), 25/44 (Paxos learned, budget 1), 6/13 (Paxos `:569`) of the naive admissible sets — i.e. ~0.5–0.7×, consistent with the docs' 2–7× being measured on larger universes / bigger budgets. For the single-fault violations LDFI's count (2–11) vs Blind's bound (≤ 1+singletons = 3–52).
Caveat: lineage universes grow during the search; for early counterexamples the printed universe may undercount. These are estimates, not measurements; an actual Blind run needs `.strategy(Strategy::Blind)` added to each EC search (`Outcome::Exhausted`/`Counterexample` + `runs().len()`), which I could not build (no cmake/libclang).

## 4. Mismatches / staleness findings
1. **Name collision**: `ec/output/ldfi_all_labels_with_the_harness_fault_model.txt` is the 2PC test; the ISR test of the same name (`isr_replication.rs:1177`, cited in the corpus' ISR companion table, 4/7/7) has **no committed transcript**.
2. Gallery vs transcript: `ec_multi_paxos_labels_582_learner_hop/report.md` = 10 hyps, transcript = 11 (Fuzz variance; corpus doc already notes it). All other gallery report.md counts match the transcripts.
3. 14 of 15 `ec/` diagrams have no committed generating test (only `gallery_ec_broadcast_closed_all_labels`); `ec_broadcast_closed_cluster_to_cluster` and `ec_broadcast_closed_lossy` come from `.search` (Spec::new) tests, not all_labels.
4. `ec_broadcast_closed_all_labels/failing.png` rendered at 96 dpi; the rest at 110 dpi (PRINT.md says 110).
5. `ec_demos_ldfi.rs` comment says unbounded multi-Paxos "does not finish within 2,000 hypotheses"; the ablation doc says Budget 256 at 211.9 s and 659 hyps in >30 min with `max_hypotheses(2000)` (stopped, not exhausted). `ec_demo_multi_paxos_all_labels` doc comment points at "section 6 of docs/design/hydrofrac.md", which has no numbered sections now (results live in `hydrofrac-ec-corpus.md`).
6. Corpus ISR "EFF moved to just after the write" row has no test/transcript (older body).
7. Ablation unbounded table lists multi-Paxos `:591` as V 8 (unbounded run) vs 9 in the budget-1 transcript — different configurations, not an error.
8. All committed outputs postdate 1538508755 and match its unbounded semantics; the "one crash" column of the ablation table and every Blind number are PRE-1538508755.
9. User's working checkout HEAD does not contain 1538508755; the tests/outputs above only reproduce on `docs/hydrofrac-inventory`.
