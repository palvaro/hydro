## Reliable broadcast

Harness: `hydro_test/src/cluster/ldfi_ablation_broadcast.rs` (`LDFI_ABLATION_OUT=… cargo test -p hydro_test level_ablation_broadcast -- --test-threads=1 --nocapture`), the search of `reliable_broadcast.rs`'s unit tests: the broadcaster and every member crashable (`fail_stop` links, so crashes are the only faults), specification `PRE` = some member delivered `m`, `POST` = every correct member delivered `m` (written positively, or through `filter_not_in` as "negated"). One program per configuration, compiled with provenance and shared by all four levels; exhaustive schedule exploration; one message. Wall times were measured on a loaded machine (sibling ablations running concurrently) and are indicative only.

| config | L0/exec | L1/slice | L2/record | L3/replay |
|---|---|---|---|---|
| plain, n=3, 1 crash, POST positive | violated / 2 / 8 / 6.00 / 0.1s (r54 f0) | violated / 2 / 8 / 6.00 / 0.1s (r54 f0) | violated / 2 / 8 / 6.00 / 0.1s (r54 f0) | violated / 2 / 8 / 6.00 / 0.2s (r148 f0) |
| reliable (echo), n=3, 1 crash, POST positive | certificate / 15 / 3289 / 6.81 / 28.6s (r21168 f0) | certificate / 15 / 3289 / 6.81 / 30.2s (r21168 f0) | certificate / 10 / 2641 / 4.55 / 23.4s (r17280 f0) | certificate / 10 / 2641 / 4.55 / 64.1s (r65388 f0) |
| reliable (echo), n=3, 2 crashes, POST positive | certificate / 73 / 10845 / 4.77 / 76.0s (r54872 f0) | certificate / 73 / 10845 / 4.77 / 107.1s (r54872 f0) | certificate / 32 / 7271 / 3.38 / 52.6s (r38664 f0) | certificate / 32 / 7271 / 3.38 / 186.2s (r142059 f0) |
| plain, n=3, 1 crash, POST negated | violated / 2 / 8 / 6.00 / 0.1s (r54 f0) | violated / 2 / 8 / 6.00 / 0.1s (r54 f0) | violated / 2 / 8 / 6.00 / 0.1s (r54 f0) | violated / 2 / 8 / 6.00 / 0.3s (r148 f0) |
| reliable (echo), n=3, 1 crash, POST negated | certificate / 15 / 3289 / 6.81 / 32.2s (r21168 f0) | certificate / 15 / 3289 / 6.81 / 39.8s (r21168 f0) | certificate / 10 / 2641 / 4.55 / 30.9s (r17280 f0) | certificate / 10 / 2641 / 4.55 / 85.1s (r65388 f0) |
| plain, n=4, 1 crash, POST positive | violated / 2 / 30 / 8.00 / 0.7s (r264 f0) | violated / 2 / 30 / 8.00 / 0.7s (r264 f0) | violated / 2 / 30 / 8.00 / 0.7s (r264 f0) | violated / 2 / 30 / 8.00 / 1.5s (r732 f0) |
| reliable (echo), n=4, 1 crash | not run: 92,160 schedules per hypothesis exhaustively (~5 min each), at every level | | | |

Cells: `verdict / hypotheses / schedules / mean derivation size / wall secs`; `(rN fM)` = counterfactual replays and replay fallbacks. Mean derivation size is the mean number of `Dependency` leaves over the derivations of goal-held schedules; a member's delivery counts as two leaves (the protocol message it rests on and the member's observation send to the specification's observer), so `6.00` at n=3 and `8.00` at n=4 are the n deliveries of plain broadcast.

### Observations

- **Verdicts agree across all four levels in every configuration** (violated for plain broadcast, certificate for reliable broadcast). No soundness disagreement.
- **L0 = L1.** Execution and slice lineage produce identical hypotheses, schedules, and derivations in every row. Each member tick has two inputs (the broadcaster's batch and the echo batch) and both feed the deliveries, so the static slice narrows nothing here. (Wall-time differences between the two columns are machine noise.)
- **L2 vs L1 (plain broadcast): no change.** Plain broadcast's derivation is already minimal at L0 — each delivery rests on exactly one fan-out send — and the search is two hypotheses at every level.
- **L2 vs L1 (reliable broadcast): fewer, smaller derivations and fewer hypotheses.** At 1 crash: 15 → 10 hypotheses (−33%), 3289 → 2641 schedules (−20%), mean derivation 6.81 → 4.55 leaves (−33%). At 2 crashes: 73 → 32 hypotheses (−56%), 10845 → 7271 schedules (−33%), 4.77 → 3.38 leaves (−29%). The execution-level lineage of a member tick charges a delivery to *every* message in the batch (the broadcaster's send and every echo that arrived in the same tick); record provenance charges it to the one it was deduplicated from, so fewer crash points become candidates and fewer hypotheses have to be refuted. The L2 run took 20–30% less wall time than L1 despite the provenance bookkeeping.
- **L3 vs L2: identical derivations, 2.7–3.5× the wall time.** Counterfactual replay reproduces L2's hypotheses, schedules, and mean derivation size exactly in every row (zero fallbacks), with 3.8× as many replays (e.g. 17,280 → 65,388 at 1 crash). Record provenance already sees everything replay can see in this program (no closure-encoded state the rewrite misses).
- **Replays at L0–L2 are not zero.** The `rN` counts at L0–L2 are the counterfactual derivation of the *hand-written specification views* at the observer, which the search runs at every level (`ldfi.rs`, "For a hand-written view, two derivations are taken together…"); they scale with schedules (≈6.5 replays per schedule). L3 adds the per-node replays on top.
- **Positive vs negated `POST`: identical at every level.** The observer-side counterfactual makes the two formulations indistinguishable (same hypotheses, schedules, derivations), as the unit tests claim.
- **Size axis.** At n=4 the plain-broadcast counterexample is still found in 2 hypotheses at every level (30 schedules, 8 leaves). Reliable broadcast at n=4 was not run: exhaustive exploration enumerates 92,160 schedules per hypothesis (~310 s each on this machine), and the lineage level does not change the per-hypothesis schedule count, only the number of hypotheses.

## Reliable delivery

Harness: the same file, test `level_ablation_broadcast_and_delivery` (`LDFI_ABLATION_ONLY=delivery` runs this half alone), the search of `reliable_delivery.rs`'s unit tests: `msg` and `ack` channels `lossy_delayed_forever` (omission is the only fault), `eot` timer rounds with omissions admitted in the first `eff`, goal = the receiver received the message by the end of the horizon. "hooks" binds the receiver's message batch and the sender's ack batch to scripted hooks. Compiled with provenance; exhaustive schedule exploration (which is one schedule per hypothesis here).

| config | L0/exec | L1/slice | L2/record | L3/replay |
|---|---|---|---|---|
| give up after 2, eot=4 eff=2, no hooks | violated / 3 / 3 / 1.00 / 0.1s | violated / 3 / 3 / 1.00 / 0.0s | violated / 3 / 3 / 1.00 / 0.0s | violated / 3 / 3 / 1.00 / 0.0s |
| give up after 2, eot=4 eff=2, hooks | violated / 3 / 3 / 1.00 / 0.0s | violated / 3 / 3 / 1.00 / 0.0s | violated / 3 / 3 / 1.00 / 0.0s | violated / 3 / 3 / 1.00 / 0.0s (r6 f2) |
| retry until ack, eot=4 eff=2, no hooks | certificate / 3 / 3 / 0.67 / 0.0s | certificate / 3 / 3 / 0.67 / 0.0s | certificate / 3 / 3 / 0.67 / 0.0s | certificate / 3 / 3 / 0.67 / 0.0s |
| retry until ack, eot=4 eff=2, hooks | certificate / 3 / 3 / 0.67 / 0.0s | certificate / 3 / 3 / 0.67 / 0.0s | certificate / 3 / 3 / 0.67 / 0.0s | certificate / 3 / 3 / 0.67 / 0.0s (r9 f3) |
| give up after 3, eot=6 eff=3, hooks | violated / 4 / 4 / 1.00 / 0.0s | violated / 4 / 4 / 1.00 / 0.1s | violated / 4 / 4 / 1.00 / 0.1s | violated / 4 / 4 / 1.00 / 0.1s (r9 f3) |
| retry until ack, eot=6 eff=3, hooks | certificate / 4 / 4 / 0.75 / 0.0s | certificate / 4 / 4 / 0.75 / 0.0s | certificate / 4 / 4 / 0.75 / 0.0s | certificate / 4 / 4 / 0.75 / 0.1s (r12 f4) |

Cells as above. A mean below 1.00 for the retry-until-ack certificates is the last hypothesis's empty derivation (the delivery that got through was sent after `EFF`, outside the fault vocabulary).

### Observations

- **Verdicts agree across all four levels in every configuration.** No soundness disagreement.
- **The levels are indistinguishable on this protocol.** Every row has the same hypotheses (`{}`, `{msg_1}`, `{msg_1, msg_2}`, … one per pre-`EFF` round), one schedule per hypothesis, and a derivation of exactly one leaf (the one `msg` delivery the goal rests on; acks never appear). The goal is a single external output with a single-delivery derivation, so there is nothing for L2 or L3 to narrow below L0/L1: the program's per-execution lineage is already exact.
- **Hooks change nothing in the verdict or the search**, as the unit tests claim; they only change where lineage is read (traced hook releases instead of receives).
- **L3 replays fall back on every step when the batches are hooked** (`r6 f2`, `r9 f3`, `r12 f4`: one fallback per goal-held schedule, i.e. every replayed derivation step), and runs no replays at all without hooks (there is no traced execution to replay; the lineage comes from the receives). With a hooked batch the node evidently does not reproduce its output under solo replay with everything kept, so the closure falls back to the recorded (L2) lineage — the same one-leaf derivation. The outcome is unaffected; worth a look at why a hooked batch does not replay, but not a soundness issue in these results.
- **Workload axis (eot=6, eff=3):** hypotheses grow by one per extra pre-`EFF` round at every level; nothing else changes.
