# Blind test: the frozen checker on programs it had never seen

The seven programs under `hydro_test/src/cluster/witnesses/blind/` were written and labeled in a fresh session that was given only `design_docs/2026-09_blind_witness_task.md` and the repository. The checker (`hydro_lang::sim::amplification::check`, verdict rule and `CheckConfig::new(240)` unchanged from the corpus runs) was then run on them by an operator who had not read the programs. This document records the verdicts first, as they were produced, and only afterwards compares them with the author's labels.

## How the firewall held, and where it leaked

The operator read, for each program, the public function signature (parameter list and return type), the public struct and field names, and the steady-state input its own harness feeds per round (the constants and the `send` calls inside the harness's `run` function). The operator did not read any module doc comment, doc table, label, or program body before recording the verdicts below. The wiring is in `hydro_test/src/cluster/witnesses/blind_check.rs`.

Two leaks happened and are recorded here. First, the grep pattern used to find the harness's `run` function also matched the names of the test functions, so the operator saw names such as `..._exhibit_persistent_collapse`, `..._causes_collapse`, `burst_obeys_the_closed_form_and_recovers`, and `closed_form_holds_across_schedules` for every program before wiring. Those names imply the author's labels. Second, the `run` function of `log_catchup.rs` contains the author's hand-computed expectation as a comment, and the operator read it while extracting the input. Neither leak can affect the checker's output, because the verdict rule, hold grid, and counting were frozen before the blind programs existed and the operator changed nothing in them; the leaks bear only on the operator's independence, which is what a blind test is meant to protect, and so they are stated rather than hidden. Struct field names such as `retry`, `redelivery`, and `prepare_timeout_ticks` were also visible and hint at mechanism; they are part of the public signature and were needed for nothing, but they were seen.

## Phase 1: verdicts recorded before reading labels

All seven checks ran in 73.6 s of test time (116 s wall clock including the build), `cargo test -p hydro_test --lib blind_check -- --nocapture --test-threads=1`. Each program ran 240 rounds of its harness's steady-state input with no trigger; every batch and snapshot hook that ever had buffered input was held from round 20 for 0, 5, 10, 20, 40, 60, 80, and 100 rounds. The number after each hook is the extra admitted records over the unheld run at each of those hold lengths.

### `batch_flush`, verdict hazardous

Input: 2 items, one batcher clock element, one sink clock element per round; `batch_size 8`, `flush_after_ticks 4`, sink `max_per_tick 8`. Baseline 1920 admitted records and 960 network messages over 5 hooks.

- `batch_flush.rs:161:42#0 [FlushCopy]`: 0, 60, 35444, 38180, 40342, 41622, 42814, 43908. Gain 43908, first at k = 5. This is the named location.
- `batch_flush.rs:161:42#1 [()]`: 0, 28, 27824, 35516, 38474, 39250, 40170, 41216.
- `batch_flush.rs:102:43#0 [u64]`: 0, 4, 31552, 35710, 38550, 39304, 40198, 41220.
- `batch_flush.rs:102:43#1` and `#2 [(usize, ())]`: 0, ±8, 64, 30448, 28108, 25136, 20756, 16732.

Under the winning hold the hooks that admitted more were `161:42#0 [FlushCopy]` (+43388) and `102:43#0 [u64]` (+520).

### `bounded_fanout`, verdict benign

Input: 2 publications per round to a cluster of 3 subscribers, `subscribers_per_publication 3`. Baseline 1920 admitted and 1440 network over 2 hooks. Both hooks (`bounded_fanout.rs:110:31#0 [Delivery]` and `88:15#0 [(usize, u64)]`) read 0 at every hold length; both ticks parked for the whole hold (single-hook ticks, never asked), so the holds took effect by parking.

### `credit_flow`, verdict benign

Input: 2 jobs, one producer clock, one consumer clock per round; `producer_capacity 4`, `consumer_capacity 4`, `credit_window 16`. Baseline 1920 admitted and 960 network over 5 hooks. Every hook (`credit_flow.rs:124:48#0..#2`, `178:56#0 [Job]`, `178:56#1 [()]`) read 0 at every hold length.

### `log_catchup`, verdict hazardous

Input: 2 appends, one follower clock, one leader clock per round, no unrelated work; `poll_every_ticks 6`, `retry_after_ticks 4`, `chunk_size 12`, `leader_capacity 10`. Baseline 1470 admitted and 510 network over 4 hooks.

- `log_catchup.rs:218:28#2 [Fetch]`: 0, 0, 26, 468, 1512, 1327, 1135, 933. Gain 1512, first at k = 10. This is the named location.
- `log_catchup.rs:125:53#0 [RecordReply]`: 0, 0, 26, 130, 377, 585, 897, 1326. Gain 1326.
- `log_catchup.rs:218:28#3 [()]`: 0, 0, 26, 143, 351, 572, 819, 1027. Gain 1027.
- `log_catchup.rs:218:28#0 [()]`: 0, −10, −10, −16, −48, −67, −86, −118 admitted, but a gain of 72 in sends from `Process(loc1v1)`.

Under the winning hold the hooks that admitted more were `125:53#0 [RecordReply]` (+1368) and `218:28#2 [Fetch]` (+144).

### `token_bucket`, verdict benign

Input: 3 applications per round, one refill clock element; `refill_tokens 5`, `bucket_capacity 10`. Baseline 960 admitted and 720 network over 2 hooks. Both hooks (`token_bucket.rs:80:37#0 [Request]`, `80:37#1 [(usize, ())]`) read 0 at every hold length.

### `two_phase_commit`, verdict hazardous

Input: 1 transaction, one coordinator timer, one participant timer per round; `prepare_timeout_ticks 4`, `participant_capacity 5`. Baseline 2160 admitted and 1440 network over 5 hooks.

- `two_phase_commit.rs:240:42#1 [()]`: 0, 18, 6095, 7440, 8520, 9037, 9461, 9845. Gain 9845, first at k = 5. This is the named location.
- `two_phase_commit.rs:240:42#0 [ParticipantMessage]`: 0, 9, 5982, 7417, 8514, 9034, 9458, 9842.
- `two_phase_commit.rs:201:58#1 [Vote]`: 0, 1, 36, 5845, 7418, 8170, 8574, 8974.
- `two_phase_commit.rs:201:58#0 [(usize, ())]`: 0, −3, 4730, 5907, 5961, 5549, 4769, 4023.
- `two_phase_commit.rs:201:58#2 [(usize, ())]`: 0 throughout.

Under the winning hold the hooks that admitted more were `240:42#0 [ParticipantMessage]` (+9687) and `201:58#1 [Vote]` (+158).

### `visibility_queue`, verdict hazardous

Input: 2 submissions, one broker clock, one worker clock per round; `visibility_ticks 6`, worker `max_per_tick 5`. Baseline 1920 admitted and 960 network over 5 hooks.

- `visibility_queue.rs:195:44#1 [Delivery]`: 0, 0, 46, 3910, 5424, 5936, 6254, 6560. Gain 6560, first at k = 10. This is the named location.
- `visibility_queue.rs:195:44#0 [(usize, ())]`: 0, 0, 20, 108, 3757, 5025, 5590, 5914.
- `visibility_queue.rs:110:37#1 [u64]`: 0, 0, 16, 96, 3961, 5062, 5598, 5910.
- `visibility_queue.rs:110:37#2 [(usize, u64)]`: 0, 0, 0, 52, 3220, 3373, 3205, 2758.
- `visibility_queue.rs:110:37#0 [(usize, ())]`: 0 throughout.

Under the winning hold the hooks that admitted more were `195:44#1 [Delivery]` (+6400) and `110:37#1 [u64]` (+160).

### Summary of Phase 1

Four programs were found hazardous (`batch_flush`, `log_catchup`, `two_phase_commit`, `visibility_queue`) and three benign (`bounded_fanout`, `credit_flow`, `token_bucket`). Every hazardous verdict came from admitted records rising; in every hazardous case the curve rises steeply between a 5- and a 20-round hold and keeps rising or plateaus, and in every benign case every curve is flat at zero.

## Phase 2: comparison

Only after the section above was written did the operator read `blind/mod.rs`, each program's module docs, and the code at the named source locations.

| program | author's label | basis | checker verdict | agree | named hook | author's mechanism, and whether the hook is where it acts |
|---|---|---|---|---|---|---|
| `batch_flush` | hazardous | exhibited collapse (tail rounds 600..800: 18 completions, 225,506 copies sent, sink FIFO 240,682 to 463,658) | hazardous | yes | `batch_flush.rs:161:42#0 [FlushCopy]`, the sink's arrivals of flushed copies | A flush does not remove items from the open batch; only acknowledgements do, so a late acknowledgement lets the next clock element send another copy, and copies consume sink capacity. Holding the sink's arrivals is what makes acknowledgements late. The hook is the point of action. |
| `bounded_fanout` | benign | assurance argument: `wire = processed = 3P` (burst run 2,140 inputs, 6,420 wire, 6,420 processed) | benign | yes | none | Each publication is delivered once to each of three subscribers and there is no acknowledgement or retry. Both hooks read zero under every hold. |
| `credit_flow` | benign | assurance argument: four records per input (burst run 520 inputs, exactly 2,080 work records) | benign | yes | none | Credit-based flow control: the producer sends only while it holds credits, the consumer acknowledges each job, and a late acknowledgement stalls the producer rather than making it resend. Holding the `Ack` hook produced no extra records. |
| `log_catchup` | hazardous | exhibited collapse (tail: 1 completion, 192 retries, leader backlog 1,624 to 1,940) | hazardous | yes | `log_catchup.rs:218:28#2 [Fetch]`, the leader's arrivals of fetch requests | A fetch unanswered for `retry_after_ticks` is re-sent every follower tick; each request expands to up to `chunk_size` record reads at the leader, which share the leader's capacity. Holding fetches at the leader makes replies late, so the follower re-sends and the leader re-reads. The hook is the point of action; the follower's `RecordReply` hook (the reply edge) rose second, to 1326. |
| `token_bucket` | benign | assurance argument: one send and one serve per request (burst run exactly 1,200 sends and 1,200 serves) | benign | yes | none | A rate limiter: requests wait for tokens and are served once; there is no timeout or retry. Both hooks read zero. |
| `two_phase_commit` | hazardous | exhibited collapse (tail: 8 completions, 56,742 retries, participant backlog 73,335 to 129,260) | hazardous | yes | `two_phase_commit.rs:240:42#1 [()]`, the participant's timer (its service quantum), with the participant's arrivals hook `#0 [ParticipantMessage]` at 9842 nearly tied | The coordinator re-sends a missing prepare every `prepare_timeout_ticks`, and participants have no duplicate suppression, so every repeat consumes participant capacity and produces another vote. Holding the participant's timer withholds capacity and lengthens FIFO wait past the timeout; holding its arrivals does the same. Both hooks are in the participant's service block, which is where the author's mechanism acts. |
| `visibility_queue` | hazardous | exhibited collapse (tail: 63 first and 937 repeated processings, worker backlog 50,216 to 85,465) | hazardous | yes | `visibility_queue.rs:195:44#1 [Delivery]`, the worker's arrivals of deliveries | A delivery outstanding for `visibility_ticks` is re-delivered; the worker processes repeats as real work from the same FIFO. Holding the worker's arrivals makes completions late enough to expire visibility. The hook is the point of action. |

In every hazardous case the checker named the arrivals hook (or, for two-phase commit, the service timer of the same block) of the process whose queueing delay drives the resend, which is the same shape the corpus runs found for `rpc_retry` (server arrivals first, response edge second). The one nuance is that within a `sliced!` block the `#index` in a hook's identity is not declaration order: in `two_phase_commit.rs:240` the participant's `clocks` hook is `#1 [()]` and its `arrivals` hook is `#0 [ParticipantMessage]`, so the item type, not the index, is what tells them apart.

## Summary

The author supplied seven ground-truth labels: four hazardous by exhibited collapse and three benign by assurance argument. The frozen checker agreed with all seven, and on each of the four hazardous programs it named the hook at which the author's own description places the mechanism. No benign program produced a positive gain on any hook at any hold length, so there was no false hazardous; no hazardous program went unfound, so there was no miss. The verdict rule, hold grid, and counts were not changed between the corpus runs and this run.

Two things bear on how much this is worth. The seven programs are drawn from the same family as the corpus in one respect that matters to the method: every hazardous one is a timeout-driven resend whose redundant work is records crossing a network edge, and every benign one does no resending at all. The programs differ from the corpus in protocol (batching with retained items, chunked log catch-up, two-phase commit, visibility timeouts, credit flow, a token bucket, fan-out), which is what the brief asked for, and the checker's rule was not adjusted for them. But a hazard whose redundant work does not manifest as records or messages, or a program that resends without being timeout-driven, has still not been tested. And the operator's independence was imperfect, as recorded above: test-function names implying the labels were seen before wiring, and one author comment was read. The verdicts could not have been affected by that, since nothing in the checker was touched; the comparison above is the honest report of what a frozen tool produced on programs its developers had not seen.


## Postscript: the checker's hold sequence and location rule changed after this run

The verdicts above were recorded under the checker's fixed grid of hold lengths. The grid has since been replaced by a doubling sequence of holds up to the run horizon, and the location rule by a ranking on the shortest hold length that provokes a reaction. Rerun at the default horizon of 1000 rounds, all seven verdicts are unchanged. Three of the four hazardous programs name the same decision point as before (`batch_flush` sink arrivals, `two_phase_commit` participant timer, `visibility_queue` worker arrivals). `log_catchup` now names a leader timer whose first reaction is one extra message at a hold of 8 rounds, ahead of the `Fetch` arrivals hook, which first reacts at 16 rounds with 1759 extra admitted records; both are edges of the same catch-up loop, and the report shows both curves. Details are in `2026-09_metastable_3_status.md`.
