# Work spec: hydrofrac checks every `EventualConsistency` label automatically

Status: done (see `hydrofrac.md` §4.5–§6 and `Spec::all_labels`). Branch built on: `sandbox-ce19e56b-0571-4476-8bca-61900a6a4ef4`
in `/local/home/palvarox/hydro`, at or after commit `8d6e7e053e`. Read
`docs/design/hydrofrac.md` first (short); it describes what exists. Section 4.5 of it is the
one-paragraph version of this spec.

## 1. Goal

Today a hydrofrac test checks one `EventualConsistency`-labelled collection: the test author
passes it to `spec.observe_cluster(c)` and calls `spec.eventual_consistency(..)`. The
specification is a function of the collection alone, and the labelled collections are
enumerable from the program's IR. So the test author should not have to name anything.

After this work, a test supplies only the execution (flow, cluster sizes, crashable nodes,
crash budget, body) and hydrofrac returns one verdict per labelled collection in the program:

```
EventualConsistency at multi_paxos.rs:590 (Cluster(loc3v1)): CERTIFICATE, 24 hypotheses, 8 sampled schedules
EventualConsistency at multi_paxos.rs:281 (Cluster(loc2v1)): VIOLATED by {crash(acceptor@1 after ...)}, 7 hypotheses
EventualConsistency at multi_paxos.rs:142 (Cluster(loc2v1)): not checked: local inputs only
EventualConsistency at abd.rs:158 (Cluster(loc1v1)):         not checked: Singleton (not a set-valued collection)
```

The existing manual path (`observe_cluster` + `eventual_consistency`) stays, unchanged in
behaviour. The per-search report of unchecked labels (`undischarged_ec_report` in
`hydro_lang/src/sim/ldfi.rs`) is deleted; the results table replaces it.

## 2. What exists that this builds on

- **Enumeration of labelled collections**: `hydro_lang/src/sim/slice.rs`, `compute()`, the
  block that fills `Slices::ec_collections: Vec<EcCollection { label, location,
  fed_by_network }>`. A "collection" is a node whose `metadata().consistency ==
  Some(EventualConsistency)`, whose location root is a `Cluster`, and none of whose inputs is
  labelled (the point where the label is minted). Deduplicated by (source line, location).
  `fed_by_network` follows cycles via `cycle_sinks`. This runs *after* the provenance rewrite
  today; you will need it *before* (see 4.1), so factor it into a function that takes `&[HydroRoot]`
  and returns the list, and have both callers use it. Keep node identity across passes by
  attaching a tag (`metadata.tag`) or by doing the rewrite in the same pass that enumerates.
- **The generated specification view**: `hydro_lang/src/spec_views.rs::eventual_consistency`
  (POST only, no PRE) and `Spec::eventual_consistency` in `hydro_lang/src/sim/spec.rs`, which
  wires the fact-domain feed (`FactFeed`) and calls `self.post(..)`. The fact element type `T`
  there is generic; here it will be `Vec<u8>` (bincode of the element).
- **Feeds from the search into the observer before each run**: `CorrectFeed` (members not
  crashed, per cluster) and `FactFeed` (the domain learned from the probe), both in
  `spec.rs`; installed by `Run::install` (`ldfi.rs` ~line 886); the probe logic is in
  `Ldfi::run` (~line 2890: first failure-free run with empty domain → `learn_fact_domain` →
  `continue`; empty → `Outcome::Vacuous`). `fuzz_faults_with` (~line 3268) probes too.
- **IR rewriting with synthesized closures and types**: `hydro_lang/src/sim/provenance.rs`
  (`rewrite`, `element_type`, `wrap_map`, ...) shows how to build `HydroNode`s with closures
  made from `syn` tokens and element types taken from `CollectionKind`. It runs in
  `SimFlow::compiled()` (`hydro_lang/src/sim/flow.rs` ~line 213) before `slice::compute`.
- **How a network send is built in the IR**: `Stream::send` in
  `hydro_lang/src/live_collections/stream/networking.rs` (~line 200): `HydroNode::Network {
  name, networking_info, serialize, deserialize, instantiate_fn, input, metadata }`. A
  cluster→process send delivers `(MemberId<C>, T)` at the receiver (`.entries()` on the
  received keyed stream). How `Tee` is created: `Stream::clone` in
  `live_collections/stream/mod.rs` ~line 393.
- **Harnesses to validate against**: `hydro_test/src/cluster/ec_ldfi.rs`,
  `ec_demos_ldfi.rs` (the demos in `hydro_std/src/ec_inference_demos/`),
  `isr_replication.rs`, `two_pc_ldfi.rs`.

## 3. User-facing API (target)

```rust
let mut flow = FlowBuilder::new();
// ... build the program; nothing about hydrofrac in it ...

let spec = Spec::all_labels(&mut flow);          // NEW: creates the observer; names nothing

let compiled = flow.sim()
    .skip_consistency_assertions()
    .with_cluster_size(&members, 3)
    .with_provenance()
    .compiled();

let results: LabelResults = Ldfi::new()
    .level(Level::Precise)
    .crashable("source", &source)
    .crashable_cluster("member", &members, 0..3)
    .crash_budget(1)
    .spec(&spec)
    .search_all_labels(&compiled, async |_run| { ...shared body... });

for (site, verdict) in &results.verdicts { ... }
println!("{results}");                           // the table in section 1
assert!(results.all_certified_or_not_checked()); // or finer assertions per site
```

`LabelResults` holds, per labelled collection (in `ec_collections` order): its label (source
site), location, and a `LabelVerdict`:

```rust
pub enum LabelVerdict {
    Searched(Outcome),              // Certificate / Counterexample / Budget / Vacuous
    NotChecked(NotCheckedReason),   // see 4.3
}
```

`Outcome::Certificate` keeps `Grade::Generated`.

## 4. Design

### 4.1 Observation by IR rewrite

In `SimFlow::compiled()`, when the flow carries a `Spec` built with `all_labels` (see 4.2),
**before** `provenance::rewrite`:

1. Enumerate labelled collections (factored from `slice.rs`, section 2).
2. For each one that is **in class** (4.3), tap it:
   - Turn the node `N` into a `Tee` if it is not one (as `Stream::clone` does) and take a
     second branch.
   - `Map` the branch to `(idx: usize, bytes: Vec<u8>)` with a synthesized closure
     `|x: <ElemType>| (idx, bincode::serialize(&x).unwrap())`, `ElemType` from
     `element_type(&metadata.collection_kind)` (plain, pre-provenance type).
   - `Network` the result to the observer process with the same serialize/deserialize shape
     `Stream::send(.., TCP.fail_stop().name("observe").bincode())` produces for a
     cluster→process send, so the observer receives `(MemberId<C>, (idx, bytes))`. Use a
     tagless member id on the observer side (`TaglessMemberId` / raw `u32`); there is no `C`
     type available. The channel name must be `observe` or the slice code's
     `channel_ec_sources` / observer detection needs updating; check how `spec.rs` and
     `slice.rs` identify observer channels.
3. Union all taps and splice them into the **observation slot** (4.2).

Insert the tap *before* the provenance rewrite so that the tap is rewritten like any other
operator and `Level::Precise` lineage flows through it. The tap must not change the program's
own behaviour: a `Tee` branch to the observer is invisible to the rest of the program.

Note on `fed_by_network` and in-class checks: compute them on the pre-rewrite IR.

### 4.2 The observer side

`Spec::all_labels(&mut flow)`:

- Creates the observer process (as `Spec::new` does).
- Creates one **observation slot**: a placeholder source node at the observer, tagged (e.g.
  `metadata.tag = Some("hydrofrac:observe-slot")`), typed as
  `Stream<(TaglessMemberId, (usize, Vec<u8>)), Process<Observer>, Unbounded, NoOrder,
  ExactlyOnce>`. The rewrite in 4.1 replaces this placeholder with the union of the taps.
  (Alternative: a `forward_ref` cycle whose `CycleSink` the rewrite supplies; pick whichever
  is simpler given how `FlowBuilder` finalizes — a placeholder that would compile to an empty
  source if never replaced is the safer default.)
- Creates two inputs fed by the search before each run, alongside the existing feeds:
  - `current: usize` — which collection this search is about (one `sim_input`, accumulated
    like `correct_members`);
  - `correct: Stream<u32>` — the members of *that collection's cluster* that the hypothesis
    does not crash. Generalize `CorrectFeed` so the search can feed the correct set of a
    cluster chosen per search (the cluster is `ec_collections[idx].location`; its size comes
    from `with_cluster_size`; crashed members from the hypothesis).
- Builds the view: filter the slot by `idx == current`, map to `(member, bytes)`, and apply
  `spec_views::eventual_consistency` with `T = Vec<u8>`; wire `FactFeed` exactly as
  `Spec::eventual_consistency` does. The view's facts are already bytes, so `FactFeed::send`
  is the identity on bytes.

`SpecParts` gains what the search needs: `slot: Option<...>` / a flag that this is an
all-labels spec, the `current` feed, and the per-cluster correct feed.

### 4.3 Which collections are in class

Check per enumerated collection, in this order, and record the first failing reason:

| Reason (`NotCheckedReason`) | Test |
|---|---|
| `LocalInputsOnly` | `!fed_by_network` — no fault in the model can make members disagree |
| `NotAStream(kind)` | `collection_kind` is `Singleton`, `Optional`, `KeyedSingleton` — the set-based spec does not apply (lattice values need an equality variant; out of scope here) |
| `InsideTick` | `metadata.location_id` is a `Tick(..)`, not the cluster root — bounded per-tick contents are not "the collection's contents" |
| `KeyedStream` | treat as a stream of `(key, value)` pairs **if** easy (tap with a map to the pair); otherwise `NotAStream` |

Everything else (an `Unbounded` `Stream` at a cluster root, fed by the network) is checked.

Element types must be `serde::Serialize` for the tap to compile. Labelled cluster collections
are nearly always built from network sends and so are. If a program has one that is not, the
generated crate fails to compile with a clear error; provide `Spec::all_labels_except(&mut
flow, &[&str /* source-site substrings */])` so the user can exclude it, and document this in
the limitations.

### 4.4 The search loop

`Ldfi::search_all_labels(&compiled, body) -> LabelResults`:

```
let labels = compiled.slices().ec_collections  (with in-class info from the rewrite)
for (idx, label) in labels:
    if not in class: verdicts.push(NotChecked(reason)); continue
    set the spec's `current` = idx and the correct-feed cluster = label.location
    clear the fact domain
    outcome = self.run(..)           // existing loop: probe → learn domain → search
    verdicts.push(Searched(outcome))
```

Reuse `Ldfi::run` as is; the only per-collection state is the two feeds and the domain. Print
one summary line per collection as each finishes (the table in section 1) and the full
`Outcome` display for counterexamples. `Exploration` and `Level` apply to every collection.

Delete `undischarged_ec_report` and its call site; delete `Slices::channel_ec_sources` if
nothing else uses it.

### 4.5 Doc

Update `docs/design/hydrofrac.md`: §4.5 becomes the description of automatic coverage (short);
§5's example becomes the `all_labels` form, with the manual form mentioned in one sentence;
§6 gains the per-label results for multi-Paxos, ISR, 2PC (section 6 below); §8 drops the
"report is a report" bullet and adds the `Serialize` requirement and the not-in-class kinds;
§9 drops item 1. Keep the document in the same register: describe what exists, no narrative.

## 5. Constraints

- Do not change the verdicts of the existing EC tests (`ec_ldfi.rs`, `ec_demos_ldfi.rs`):
  the manual path must produce the same outcomes and hypothesis counts as before.
- Do not change the program under test's behaviour: the tap is a `Tee` branch; verify by
  checking that a program's own `sim_output`s are identical with and without the spec.
- Soundness rule from `hydrofrac.md` §2 and §4.2 must hold for each per-collection search:
  it does, if each search is an unchanged `Ldfi::run`.
- `hydro_lang/src/spec_views.rs` must stay outside `sim` (its `q!` closures are staged).

## 6. Acceptance

1. `Spec::all_labels` on the `reliable_broadcast_closed` demo (3 members, source + members
   crashable, budget 1): the `delivered` label certifies in 10 hypotheses (same as the manual
   test); the other labels get verdicts; no label is silently skipped.
2. On `multi_paxos` (1 proposer, 3 acceptors, 3 learners, budget 1, `Fuzz { instances: 8 }`):
   13 verdicts; `learned` (site `multi_paxos.rs:590`) certifies with the same count as the
   manual test (24 at commit `8d6e7e053e`). Record the other 12 verdicts in §6 of the doc,
   whatever they are, with one sentence each on why (an acceptor-local fold is "local inputs
   only"; a proposer→acceptor broadcast violated by a proposer crash is the first row of the
   table again; etc.). Do not tune the body to make anything pass.
3. On the ISR harness program (`isr_replication.rs`) and the 2PC program (`two_pc_ldfi.rs`),
   with the senders those harnesses declare non-crashable made crashable: the labelled
   `broadcast_closed`s are violated by a sender crash between two fan-out sends. With the
   harnesses' own crashable sets: certified or vacuous, as the case may be; report what
   happens.
4. A program with a labelled `Singleton` (`abd`) and one with a labelled collection inside a
   tick produce `NotChecked` verdicts with the right reasons and still compile and run.
5. All existing suites pass: `cargo test -p hydro_test --lib -- ec_ ldfi quorum_ldfi paxos
   reliable_broadcast two_pc raft::tests` (53 passed / 13 ignored at `8d6e7e053e`);
   `cargo test -p hydro_lang --features sim,build,ldfi,deploy --lib` (286 passed);
   `cargo test -p hydro_std --lib` (12 passed). `cargo fmt`; `cargo clippy -p hydro_lang -p
   hydro_test -p hydro_std --features hydro_lang/ldfi,hydro_lang/build,hydro_lang/sim --tests`
   clean apart from the pre-existing style lints inside `hydro_std/src/ec_inference_demos/`
   (verbatim copies; leave them).
6. `undischarged_ec_report` is gone; the doc is updated as in 4.5.

## 7. Environment notes (this machine)

- The sandbox's `$TMPDIR` is periodically wiped. The Rust build needs `cmake` and `libclang`
  that are not on the system path; rebuild them with:
  `cd $TMPDIR && python3 -m venv venv && ./venv/bin/pip install -q cmake libclang` and then
  `export PATH="$TMPDIR/venv/bin:$PATH"; export LIBCLANG_PATH="$TMPDIR/venv/lib/python3.7/site-packages/clang/native"`
  before any `cargo` command. If `cargo` fails on `highs-sys`, this is why.
- Never edit source while a `cargo test` is running (the trybuild-based simulator detects
  "unexpected recompilation" and fails spuriously).
- The simulator compiles each test's flow into a dylib; test output of passing tests is not
  shown without `--nocapture`, and parallel tests interleave stderr, so for reading LDFI
  output use `--nocapture --test-threads=1` on a filtered set.
- Long suites: run them with `nohup ... > $TMPDIR/x.log 2>&1 &` and poll; the EC suite is
  about 1.5 minutes, the full hydro_test LDFI set about 7, hydro_lang about 7.
- Insta snapshots (`hydro_test/src/cluster/snapshots/*.snap`) change if the IR of the
  Paxos benchmark changes; this work should not touch them (the tap is only inserted when a
  `Spec::all_labels` exists).

## 8. Out of scope

- The lattice/`Singleton` variant of the specification (observe the final value per member,
  POST is equality). Record such collections as `NotChecked(NotAStream)`.
- Convergence witnesses, decision-trace replay, dynamic membership.
- Making the absence of a certificate for some label fail a test automatically beyond what
  `LabelResults` lets the test assert.
