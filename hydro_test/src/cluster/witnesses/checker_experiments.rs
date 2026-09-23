//! Checker experiments, in three steps: can random schedule exploration alone recover the label of
//! `rpc_retry` (first experiment); can holding one edge at a time recover every corpus label at
//! the witnesses' own parameters (second); and can the same sweep take its verdict from counts
//! the simulator collects itself, with nothing read from the program (third)?
//!
//! ## First experiment: random schedules, no simulator changes
//!
//! The question the checker must answer is whether there exists a schedule under which a fixed
//! input costs the program more work than the input requires, and more of it the longer delivery
//! is delayed. This module asks that question of [`super::super::rpc_retry`] using only the
//! existing `flow.sim().fuzz(..)` API: the fuzzer chooses, at every tick of every slice, how many
//! buffered items each `use::batch` releases. The harness offers the witness's baseline workload
//! (two requests per round against a capacity of five) and never a trigger. For every schedule
//! the fuzzer draws, the harness records the total work (client sends and server serves), the
//! number of distinct requests, and the longest reply delay any request saw, measured in client
//! clock ticks from first send to completion. Extra work is total work minus what the distinct
//! requests require, bucketed by that longest delay.
//!
//! # How a random schedule reaches a delay
//!
//! A reply can only be delayed relative to the client's logical clock while clock elements are
//! released and the reply is held. Under the round-based harness, a tick whose only loaded hook is
//! the replies hook is forced to release, so a reply cannot survive past the round's last clock
//! element. The harness therefore offers several clock elements per round, and a schedule that
//! releases them one at a time while holding a reply advances the client's clock past the timeout.
//! Because every release count is a uniform random draw, the chance that a reply survives `k`
//! consecutive clock releases falls geometrically in `k`, so the timeout has to be small for a
//! random search to reach it. The program is unchanged; timeout and clock density are harness
//! parameters, as they are for the bounded-queue hold experiment in
//! [`super::bounded_queue_rejection`].
//!
//! # Hand-computed expectation, written before measuring
//!
//! Twenty-four rounds, two requests per round (48 distinct requests), six clock elements per
//! round, timeout 2 ticks, capacity 5 per server tick, 512 schedules per configuration.
//!
//! - Under the prompt schedule, every reply is observed before the next clock element, latency is
//!   0, and the run costs 48 sends and 48 serves: extra work 0.
//! - With `max_attempts = 3`, a reply held while two clock elements are released makes the client
//!   re-send that id and the server, whose queue is otherwise empty, serve it a second time; held
//!   while four are released, a third time. So every schedule whose longest delay is below 2 has
//!   extra work exactly 0; schedules whose longest delay is 2 or 3 have positive mean extra work;
//!   schedules whose longest delay is 4 or more have a higher mean than those, and no request is
//!   sent more than three times, so extra sends never exceed 96. The curve rises with delay and
//!   plateaus at the attempt cap.
//! - With `max_attempts = 1`, the client never re-sends, so extra work is exactly 0 in every
//!   delay bucket, although the delay buckets themselves are populated the same way.
//!
//! The relation is statistical rather than exact because a clock jump and the reply can land in
//! the same client tick, which gives a long latency with no re-send.
//!
//! # Measured
//!
//! 512 schedules per configuration, about 10 s of wall clock per 512 once the staged crate is
//! built (the first configuration in a process takes about 25 s). Extra work is sends minus the 48
//! distinct requests; extra serves were equal to extra sends in every schedule, so the server redid
//! exactly the work the client re-sent.
//!
//! | configuration | schedules with extra work | extra sends by longest observed delay (mean over schedules; count) | mean / max extra sends |
//! |---|---|---|---|
//! | `max_attempts = 3`, timeout 2 | 512 of 512 | delay 4: 25.3 (7); delay 5: 23.1 (75); delay 6: 25.3 (430) | 24.96 / 55 |
//! | `max_attempts = 1`, timeout 2 | 0 of 512 | delay 1: 0 (8); 2: 0 (45); 3: 0 (110); 4: 0 (128); 5: 0 (140); 6: 0 (81) | 0 / 0 |
//! | `max_attempts = 3`, timeout 4 | 512 of 512 | delay 4: 4.8 (5); delay 5: 5.8 (69); delay 6: 8.1 (438) | 7.73 / 22 |
//! | `max_attempts = 3`, timeout 6 | 416 of 512 | delay 4: 0 (7); delay 5: 0 (45); delay 6: 2.5 (460) | 2.28 / 9 |
//!
//! Both halves of the expectation held: with three attempts every schedule cost more than the
//! input, with one attempt none did, and the delays a schedule imposed were the same either way
//! (1 to 6 ticks). One thing the expectation got wrong: with retries on, no schedule had a longest
//! delay below 4, so the low buckets of the timeout-2 run are empty and its curve is flat at the
//! attempt cap. The re-sends themselves lengthen the schedule: each re-send produces another reply,
//! which keeps the client tick runnable for more sub-ticks in the round, each of which can release
//! another clock element while a reply is held. The rising part of the curve shows once the
//! timeout is raised so that only the longest delays reach it: at timeout 4 extra sends rise from
//! 4.8 to 8.1 across the delay buckets and no request is sent more than twice; at timeout 6 only
//! the delay-6 schedules re-send. Across the three timeouts the mean falls from 25.1 to 7.7 to 2.3,
//! which is the same curve read the other way: extra work rises with the delay a schedule can impose
//! relative to the timeout.
//!
//! # What this does and does not give the checker
//!
//! Random schedule exploration recovers the label of `rpc_retry` at one attempt and three
//! attempts without a load trigger and without touching the simulator, at a cost of about 10 s per
//! 512 schedules. It does so only because the timeout was set small enough for a random draw to
//! reach it; at the witness's own timeout of 40 the same search would need a reply to survive 40
//! consecutive single-element clock releases, which a uniform draw will not produce. A checker that
//! uses the fuzzer as its search needs a way to bias or direct the draws toward holding one edge,
//! or a harness that meters time so that a hold of a chosen length is a single decision.
//!
//! On localization: the simulator's per-tick trace (`run_with_scheduler_and_logger`) prints, for
//! every decision, the `use::batch` source location and the items released, and `fuzz_repro` replays
//! a saved schedule with that trace. So for any one schedule the information to say which batch
//! decision held the reply is available. Inside `fuzz`, however, the bytes that identify the
//! current schedule are not exposed to the harness unless the run fails, so a passing schedule
//! that showed extra work cannot be replayed with the trace on. Making the fuzzer save the
//! schedules the harness flags would close that gap.
//!
//! # Second experiment: hold one edge, at the witness's own parameters
//!
//! The limit above is answered by [`hydro_lang::sim::hold_one_hook`]: a driver that follows
//! the prompt schedule everywhere except at one named `use::batch` hook, which releases nothing
//! while the harness says so. "Delay everything arriving at this edge for `k` rounds" becomes one
//! decision, so the timeout of 40 is reached as easily as a timeout of 2. The
//! [`hold_sweep`] module below runs each witness under its own baseline workload with no trigger,
//! discovers the hooks that ever have something buffered, holds each in turn for `k` in a grid,
//! and reads extra work (total work minus what the distinct inputs require) from the witness's own
//! outputs. The verdict rule: a configuration is hazardous if for some holdable hook some hold
//! length adds work over the unheld run (a positive *gain*); benign if no hold adds any work. A
//! hold can only postpone work, never pull work into the run from beyond its end, so any
//! positive gain is work the hold created. The curve need not rise monotonically: a program the
//! hold tips into a persistently degraded state does a constant amount of extra work per
//! remaining round, so its total falls as the hold grows and leaves fewer rounds after it. Hooks
//! the scheduler refuses to hold (the only loaded hook on its tick is forced to release) are
//! reported separately and excluded from the verdict.
//!
//! Two things had to change in the simulator, both inert unless a hold is set. Hooks are
//! identified to the driver through a thread-local the scheduler sets around each decision
//! (`SimHook::hook_location`, `hook_item_type`, `hold_one_hook::with_current_hook`); every
//! `use::batch` in one `sliced!` block reports the block's location, so a hook is named
//! `location#index-within-tick [item type]`. And `SimTick::can_run` ignores a held hook when
//! deciding whether a tick is runnable, so a tick whose only loaded hook is held parks until other
//! input arrives. Without that clause every hold was refused: in a round-based run the held items
//! are, by the time the tick is next considered, its only buffered input, and `run_hooks` forces
//! the tick to release something. This is the smallest form of the empty-tick policy the previous
//! effort needed.
//!
//! ## Hand-computed expectation for `rpc_retry`, written before measuring
//!
//! Baseline: one clock element and two requests per round, capacity 5, timeout 40, hold from
//! round 20, grid 0/5/10/20/40/60/80/100, 240 rounds. Holding the client's `responses` hook for
//! `k` rounds: a request first sent at round `r` during the hold is judged every tick and re-sent
//! at `r + 40` if its response is still held, so requests from the first `k − 40` hold rounds are
//! re-sent once (`2(k − 40)` re-sends) and those from the first `k − 80` twice. The server serves
//! every re-send, so extra serves equal extra sends. Expected extra work (sends plus serves over
//! the requirement of two per distinct request): 0 for `k ≤ 40`; 40 at `k = 60`; 160 at
//! `k = 80`; 320 at `k = 100`. With `max_attempts = 1` the same holds produce 0 at every `k`.
//! Holding the `clock` hook stops time, so no timeout can fire: 0 at every `k`. Holding
//! `new_requests` delays first sends and then dumps `2k` requests on the server at once; the
//! server drains three per round, so for `k = 100` the last of them wait about 65 rounds and
//! are re-sent once: 0 up to `k = 60` or so, then rising. The server's `arrivals` hook sits on a
//! tick with no other input and cannot be held. The metrics-window hooks buffer only metrics
//! inputs and change no work: 0 at every `k`.
//!
//! ## Measured on `rpc_retry`
//!
//! Seven hooks had buffered input; none was refused. Extra work by hold length
//! 0/5/10/20/40/60/80/100 (client sends plus server serves, minus two per distinct request):
//!
//! | configuration | hook held | curve | gain |
//! |---|---|---|---|
//! | 3 attempts | client tick, `Response` (responses) | 0 0 0 0 0 80 160 320 | 320 |
//! | 3 attempts | server tick, `Request` (arrivals) | 0 0 0 0 4 136 498 588 | 588 |
//! | 3 attempts | client tick, `(usize, u64)` (new requests) | 0 0 0 0 0 0 0 4 | 4 |
//! | 3 attempts | clock and the four metrics-window hooks | all 0 | 0 |
//! | 1 attempt | every hook | all 0 | 0 |
//!
//! The responses curve is the hand computation exactly. The largest gain is at the server's
//! arrivals hook, the edge the witness's load trigger stresses: held requests time out and are
//! re-sent behind the held originals, and the dump on release keeps the queue above the timeout
//! for many more rounds. Verdicts: hazardous at three attempts, localized to the arrivals and
//! responses edges; benign at one attempt. About 2 s per 240-round execution; 115 s and 136 s per
//! configuration for 7 hooks × 7 holds plus the discovery run.
//!
//! ## Measured across the corpus
//!
//! Every configuration ran its own baseline workload for 240 rounds (60 for `crdt_gossip`, whose
//! cost is quadratic in the set size) with no trigger, hold from round 20, the grid above (2, 5,
//! 10, 20 for `crdt_gossip`). No hold was refused anywhere. The whole run takes about 5 minutes
//! of wall clock for the 20 configurations outside `rpc_retry` with the tests in parallel, plus
//! about 4 minutes for `rpc_retry`; all 22 together finish in 5 minutes 54 seconds.
//!
//! | configuration | label | verdict | agree | hook with the largest gain | curve (extra work by hold length) |
//! |---|---|---|---|---|---|
//! | rpc_retry, 3 attempts | hazardous | hazardous | yes | server `Request` arrivals | 0 0 0 0 4 136 498 588 |
//! | rpc_retry, 1 attempt | benign | benign | yes | none | all 0 |
//! | backoff_retry, backoff on | no ground truth; expected hazardous | hazardous | yes | server `Request` arrivals | 0 0 0 0 0 90 242 448 |
//! | backoff_retry, backoff off | hazardous | hazardous | yes | server `Request` arrivals | 0 0 0 0 4 136 498 588 |
//! | bounded_queue, `Some(100)` | no ground truth; expected hazardous | hazardous | yes | server `Request` arrivals | 0 0 0 0 4 204 400 634 |
//! | bounded_queue, `None` | hazardous | hazardous | yes | server `Request` arrivals | 0 0 0 0 4 136 498 588 |
//! | cache, request-dated, no coalescing | hazardous | hazardous | yes | cache `Fill` (fills from origin) | 336 388 468 2520 2668 2668 2668 2668 |
//! | cache, fill-dated, no coalescing | no ground truth; expected hazardous | hazardous | yes | origin clock `()` | 248 320 400 544 856 1152 1448 1748 |
//! | cache, coalescing | benign | benign | yes | none (every curve flat or falling) | 240 240 240 240 220 200 180 160 |
//! | gossip_resend, ack timeout 3 | hazardous | hazardous | yes | `Ack` hook | 0 100 7063 7065 7065 7065 7065 7065 |
//! | gossip_resend, ack timeout 0 | benign | benign | yes | none | all 0 |
//! | election, uniform timeouts | hazardous | hazardous | yes | `Msg` inbox | 0 0 45 125 245 385 530 655 |
//! | election, spread 3 | hazardous | hazardous | yes | `Msg` inbox | 0 0 5 40 100 155 220 265 |
//! | rebalancing, no cooldown | hazardous, recovers | hazardous | yes | `u64` task arrivals | 0 74 112 60 120 185 240 300 |
//! | rebalancing, cooldown 8 | no ground truth; expected hazardous | hazardous | yes | `u64` task arrivals | 0 18 42 60 120 180 240 300 |
//! | compaction, reserve 0 | hazardous | hazardous | yes | `Op` arrivals | 0 5136 9979 9281 7961 6741 5621 4601 |
//! | compaction, reserve 8 | no ground truth; expected hazardous | benign | **no** | none | all 0 |
//! | lease, re-send after 4 | hazardous | hazardous | yes | client `Ack` hook | 0 12 3345 3495 3495 3495 3495 3495 |
//! | lease, one outstanding | benign | benign | yes | none | all 0 |
//! | crdt_gossip | benign | benign | yes | none (pump hold only postpones merges) | 16113 15744 15123 13908 10803 |
//! | pure_heartbeat | benign | benign | yes | none (no batch hook at all) | 2160 |
//! | transitive_closure | benign | benign | yes | none (step hold only postpones) | 288 288 282 270 246 222 198 174 |
//!
//! Twenty-one of twenty-two agree. The one disagreement is `compaction_falls_behind` with
//! `compaction_reserve = 8`. Its label rests on the read-cost coupling being present in the code
//! and on the witness's own load trigger (40 gets per round against a budget of 30) making the
//! store fall behind. A hold on either of its two hooks adds no work: holding the clock starves
//! the tick of budget and the released elements arrive as one tick whose reserve compacts the
//! whole segment before any get is costed; holding the operations dumps them into a store whose
//! segment the reserve keeps at zero. Under this program the reserve removes the delay-triggered
//! path entirely, and what remains is a load-triggered one that no schedule of a fixed input
//! reaches. Whether the label or the verdict is right is a question about the definition, not
//! the measurement: the hazard needs offered load above capacity, which the scheduler cannot
//! manufacture.
//!
//! Three further observations. First, the same edge localizes the hazard in every request and
//! response program: the server's arrivals hook, with the client's response hook second. Second,
//! the two rebalancing configurations are found hazardous through the same hook and with nearly
//! the same curve, so the sweep detects "a delayed dump of tasks causes migrations" but does not
//! separate the ping-pong the cooldown removes from the legitimate moves it leaves; the label
//! agrees but for a different reason than the author's. Third, the fixed-horizon shape appears
//! wherever a hold tips a program into a degraded state it does not leave (compaction, the
//! gossip `Delta` and timer hooks): a large jump at small `k` and then a decline proportional to
//! the rounds remaining, which is why the rule reads the peak and not the endpoint.
//!
//! # Third experiment: the same sweep with no counters from the program
//!
//! The two sweeps above read work from counters the witness authors exposed (sends, serves,
//! fetches, migrations). A tool pointed at an arbitrary crate has none of those. The
//! [`generic_measure`] module runs the same 22 sweeps (same inputs, same grid, same hold plan)
//! and takes the verdict from [`hydro_lang::sim::work_counts`], which the simulator collects
//! passively: records released by every `use::batch` hook (counted host-side in `run_hooks`),
//! network messages (the deploy path's `_network_metrics()` pass-through placed in the sim's send
//! pipelines and read from DFIR metrics), DFIR handoff items, and DFIR subgraph runs, each broken
//! down by hook, sending location, and cluster member. Nothing about scheduling changed, and the
//! simulator's own 165 sim tests pass.
//!
//! ## Reasoning about the candidates, written before measuring
//!
//! A hold delays records; it cannot create them. So a count is fit for the verdict only if a
//! delay cannot raise it without the program deriving something new. Records released by batch
//! hooks and network messages both have that property: over a run each is what arrived minus
//! what is still buffered, batch sizes do not enter, and postponement past the end lowers them.
//! DFIR handoff items do not: they include persisted state re-read every tick, so a program that
//! keeps a table of outstanding requests re-reads more rows for longer under a hold with no
//! record re-derived. Subgraph runs move with tick count, which a hold changes directly. For
//! `rpc_retry` the expectation was that network messages reproduce the hand count exactly
//! (sends plus serves are the messages in both directions) and that admitted records reproduce
//! its shape at a larger scale (a re-sent request enters the server's arrivals tick, the
//! client's response tick, and the witness's two metrics ticks). For `compaction_falls_behind`
//! the expectation was a miss: its extra work is a cost field in a payload and a `u64` segment
//! length, not records.
//!
//! ## What the measurement added to the rule
//!
//! Two configurations forced a choice the reasoning above had not made. A plain total of
//! admitted records (or of messages) called `election spread=3` benign: the hand count rises
//! 0 0 5 40 100 155 220 265, but the cluster's message total *falls* (960 to 944), because the
//! vote requests the followers add are outnumbered by the heartbeats the leader stops sending
//! while the cluster is leaderless. The finest breakdown, every hook on every member, caught
//! that (member 1's sends rise by 824) but called `rpc_retry` at one attempt hazardous: with one
//! attempt, a request whose reply is held past the timeout is *abandoned* rather than completed,
//! so the client's `abandoned` hook admits 202 more records while its `completed` hook admits
//! 202 fewer, a substitution with no work added. The two views fail in opposite ways: a total is
//! safe against substitution within a location but masked by displacement between members; the
//! finest view unmasks displacement but flags substitution. What separates the cases is what
//! competes for capacity. A message sent to another location consumes that location's capacity
//! and is counted *per sender*, where a quiet leader cannot hide a busy follower. Records a
//! location admits into its own ticks consume its own capacity and are counted *in total*, where
//! a completion turned abandonment nets to zero. The rule ([`hold_sweep::checker_verdict`]):
//! hazardous if some hold raises any member's outgoing messages or the program's total admitted
//! records above the unheld run; benign otherwise.
//!
//! ## Measured across the corpus
//!
//! Every configuration under the same input and grid as the second experiment. "hand" is the
//! verdict from the witness's own counters; "total" is a plain total of admitted records; "rule"
//! is the checker's rule above. The gain column is the rule's largest rise and what rose. The 22
//! tests pass together in 377 s of test time against 354 s for the hand-counted sweep, so
//! counting costs about six percent.
//!
//! | configuration | hand | total | rule | what rose, by how much | held hook (rule) |
//! |---|---|---|---|---|---|
//! | rpc_retry, 3 attempts | hazardous | hazardous | hazardous | admitted +1470 (network +588, the hand count exactly) | server `Request` arrivals |
//! | rpc_retry, 1 attempt | benign | benign | benign | none (finest view: `abandoned` hook +202, a substitution) | none |
//! | backoff_retry, on | hazardous | hazardous | hazardous | admitted +448 | server `Request` arrivals |
//! | backoff_retry, off | hazardous | hazardous | hazardous | admitted +588 | server `Request` arrivals |
//! | bounded_queue, `Some(100)` | hazardous | hazardous | hazardous | admitted +634 | server `Request` arrivals |
//! | bounded_queue, `None` | hazardous | hazardous | hazardous | admitted +588 | server `Request` arrivals |
//! | cache, request-dated, no coalescing | hazardous | hazardous | hazardous | admitted +2332 | cache `Fill` |
//! | cache, fill-dated, no coalescing | hazardous | hazardous | hazardous | admitted +1500 | origin clock |
//! | cache, coalescing | benign | benign | benign | none | none |
//! | gossip_resend, ack timeout 3 | hazardous | hazardous | hazardous | admitted +5435 | `Ack` hook |
//! | gossip_resend, ack timeout 0 | benign | benign | benign | none | none |
//! | election, uniform timeouts | hazardous | hazardous (+108) | hazardous | member 1 sends +840 | timer hook (the `Msg` inbox also rises) |
//! | election, spread 3 | hazardous | **benign** (total falls) | hazardous | member 1 sends +824 | `Msg` inbox |
//! | rebalancing, no cooldown | hazardous | hazardous | hazardous | admitted +300 | `u64` task arrivals |
//! | rebalancing, cooldown 8 | hazardous | hazardous | hazardous | admitted +300 | `u64` task arrivals |
//! | compaction, reserve 0 | hazardous | **benign** | **benign** | none (handoffs +164460: the backlog re-costed every tick) | none |
//! | compaction, reserve 8 | benign | benign | benign | none | none |
//! | lease, re-send after 4 | hazardous | hazardous | hazardous | admitted +3221 | client `Ack` hook |
//! | lease, one outstanding | benign | benign | benign | none | none |
//! | crdt_gossip | benign | benign | benign | none (merges fall under every hold) | none |
//! | pure_heartbeat | benign | benign | benign | none (no batch hook to hold) | none |
//! | transitive_closure | benign | benign | benign | none | none |
//!
//! Twenty-one of twenty-two agree with the hand-counted verdict, and in every agreement the rule
//! names the same held hook, except `election spread=0`, where the hand count's largest gain was
//! at the `Msg` inbox and the rule's is at the timer hook one line away in the same `sliced!`
//! block (both holds delay heartbeats; both rise under both measures). The rule's second
//! localization signal, the hooks that admitted more under the winning hold, names the edges the
//! extra records crossed: for `rpc_retry` the server's arrivals, the client's responses, and the
//! two metrics ticks; for the election, the followers' `Msg` inboxes.
//!
//! The disagreement is `compaction_falls_behind` with `compaction_reserve = 0`, as predicted. Its
//! hold curve under the hand count is 0 5136 9979 9281 7961 6741 5621 4601 units; under admitted
//! records it is flat at 1440, because the store admits the same 1200 operations and 240 clock
//! elements whatever the schedule, and the units it "spends" are arithmetic on a `u64`. The only
//! record-level trace of its collapse is the backlog it re-chains through `cross_singleton` and
//! `scan` every tick, which handoff items do see (6478 to 170938). But handoff items also call
//! four of the seven benign configurations hazardous (rpc_retry one attempt +70062, coalescing
//! cache +124589, gossip_resend off +3282560, lease one outstanding +42591), all through the same
//! mechanism: state retained longer under a hold is re-read more times. A count that cannot
//! tell re-reading from re-deriving is not usable for the verdict, so the compaction miss stands.
//! A witness that materialized its scan as records (a `cross_product` of gets with segment
//! records rather than a cost field) would be visible to the rule; this one is not, and the
//! witness was not modified.
//!
//! ## What is generic now and what is not
//!
//! The verdict and both localization signals now come from the simulator alone: the harness
//! supplies the program, the workload, and the hold plan, and reads nothing from the program's
//! outputs. What is still supplied by hand is the workload itself and the per-program wiring
//! (which inputs to feed each round). Hooks are perturbed one at a time; `use::snapshot` hooks
//! are not perturbed and their releases are not counted, since a snapshot is one value per tick
//! and tick count is a scheduling artifact.
//!
//! # Fourth experiment: the checker as a library function
//!
//! Everything the third experiment did is now one call,
//! `hydro_lang::sim::amplification::check`: it takes a `hydro_lang::sim::flow::SimFlow`, a
//! `hydro_lang::sim::amplification::CheckConfig` (rounds, the round holds begin, the hold
//! grid), and a closure that sends one round of the fixed workload, and returns a
//! `hydro_lang::sim::amplification::Report` with the verdict, one curve per hook, and, when
//! hazardous, the held hook with the largest gain, its source location, what rose, the hold
//! length at which the gain first appeared, and the hooks that admitted more records under that
//! hold. The verdict rule is the third experiment's rule unchanged. The function compiles the
//! simulation once and reuses the compiled artifact for the discovery run and every hold, where
//! the harnesses above rebuilt the dylib on every run.
//!
//! The `library_check` module wires every one of the 22 configurations exactly as the
//! harnesses above do (same program parameters, same per-round input, same rounds and grid) and
//! asserts that the function returns the verdict and the held hook that `generic_measure`
//! recorded. All 22 pass and every gain matches to the unit: `rpc_retry` 1470 at the server's
//! `Request` arrivals, backoff 448 and 588, bounded queue 634 and 588, cache 2332 at `Fill` and
//! 1500 at the origin clock, gossip resend 5435 at `Ack`, election 840 and 824 in member 1's
//! sends, rebalancing 300 at the `u64` task arrivals, lease 3221 at `Ack`, and zero gain on the
//! seven benign configurations and the two compaction configurations. Where two hooks tie (the
//! request-dated cache's `Fill` hook and origin clock both gain 2332) the first in identity
//! order wins, as in `hold_sweep::checker_verdict`. The 22 tests run together in 44 s of wall
//! clock against 377 s for the third experiment's, because compilation happens once per
//! configuration instead of once per run.
//!
//! One practical note for callers: a type named inside a `q!` closure in the caller's wiring
//! must be imported at module level, because the staged crate does not see function-local `use`
//! statements.

#[cfg(test)]
mod sim_tests {
    use std::collections::{BTreeMap, HashSet};
    use std::time::{Duration, Instant};

    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::quiesce;

    use crate::cluster::rpc_retry::{
        Client, RetryPolicy, Server, ServerConfig, rpc_with_retries,
    };

    const ROUNDS: usize = 24;
    const REQUESTS_PER_ROUND: usize = 2;
    const CLOCKS_PER_ROUND: usize = 6;
    const SCHEDULES: usize = 512;
    const TIMEOUT_TICKS: u64 = 2;

    /// One fuzzed schedule's outcome.
    #[derive(Debug, Clone, Copy)]
    struct RunOutcome {
        longest_delay: u64,
        distinct: u64,
        sends: u64,
        serves: u64,
    }

    // Free functions rather than methods: `impl` blocks inside this module are dropped when the
    // module is staged into the trybuild crate.
    fn extra_sends(r: &RunOutcome) -> u64 {
        r.sends - r.distinct
    }
    fn extra_serves(r: &RunOutcome) -> u64 {
        r.serves - r.distinct
    }

    /// Runs `SCHEDULES` fuzzed schedules of the baseline workload and returns one outcome per
    /// schedule plus the wall-clock time.
    fn fuzz_baseline(max_attempts: u32, timeout_ticks: u64) -> (Vec<RunOutcome>, Duration) {
        let policy = RetryPolicy {
            timeout_ticks,
            max_attempts,
        };
        let server_config = ServerConfig {
            max_per_tick: 5,
            service_time: Duration::ZERO,
        };

        let mut flow = FlowBuilder::new();
        let client = flow.process::<Client>();
        let server = flow.process::<Server>();
        let (request_send, requests) = client.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let (clock_send, client_clock) = client.sim_input::<(), TotalOrder, ExactlyOnce>();
        // Metrics windows are a deployment concern; these never fire.
        let (_client_report_send, client_report_tick) =
            client.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (_server_report_send, server_report_tick) =
            server.sim_input::<(), TotalOrder, ExactlyOnce>();
        let outputs = rpc_with_retries(
            &client,
            &server,
            requests,
            client_clock,
            client_report_tick,
            server_report_tick,
            policy,
            server_config,
        );
        let completed = outputs
            .completed
            .map(q!(|c| (c.id, c.latency_ticks)))
            .sim_output();
        let outgoing = outputs.outgoing.sim_output();
        let processed = outputs.processed.sim_output();

        let mut runs: Vec<RunOutcome> = Vec::new();
        let runs_ref = &mut runs;
        let started = Instant::now();

        flow.sim()
            .unit_test_fuzz_iterations(SCHEDULES)
            .fuzz(async || {
                let mut sent_ids: HashSet<u64> = HashSet::new();
                let mut sends = 0u64;
                let mut serves = 0u64;
                let mut longest_delay = 0u64;
                for tick in 0..ROUNDS as u64 {
                    for _ in 0..CLOCKS_PER_ROUND {
                        clock_send.send(());
                    }
                    for _ in 0..REQUESTS_PER_ROUND {
                        request_send.send(tick);
                    }
                    quiesce().await;
                    for (_, latency) in completed.collect_sorted::<Vec<_>>().await {
                        longest_delay = longest_delay.max(latency);
                    }
                    while let Some(req) = outgoing.try_next().await {
                        sent_ids.insert(req.id);
                        sends += 1;
                    }
                    while processed.try_next().await.is_some() {
                        serves += 1;
                    }
                }
                runs_ref.push(RunOutcome {
                    longest_delay,
                    distinct: sent_ids.len() as u64,
                    sends,
                    serves,
                });
            });

        (runs, started.elapsed())
    }

    /// Mean extra sends over the schedules whose longest delay lies in `[lo, hi)`, with the count.
    fn bucket(runs: &[RunOutcome], lo: u64, hi: u64) -> (usize, f64) {
        let sel: Vec<&RunOutcome> = runs
            .iter()
            .filter(|r| r.longest_delay >= lo && r.longest_delay < hi)
            .collect();
        let mean = sel.iter().map(|r| extra_sends(r)).sum::<u64>() as f64 / sel.len().max(1) as f64;
        (sel.len(), mean)
    }

    fn report(label: &str, runs: &[RunOutcome], elapsed: Duration) {
        report_with_timeout(label, runs, elapsed, TIMEOUT_TICKS)
    }

    fn report_with_timeout(label: &str, runs: &[RunOutcome], elapsed: Duration, timeout: u64) {
        let with_extra = runs.iter().filter(|r| extra_sends(r) > 0).count();
        let max_sends = runs.iter().map(|r| extra_sends(r)).max().unwrap_or(0);
        let max_serves = runs.iter().map(|r| extra_serves(r)).max().unwrap_or(0);
        println!(
            "[{label}] {} schedules in {:.1} s; {} with extra work; max extra sends {max_sends}, max extra serves {max_serves}",
            runs.len(),
            elapsed.as_secs_f64(),
            with_extra
        );
        let mut by_delay: BTreeMap<u64, (usize, u64, u64)> = BTreeMap::new();
        for r in runs {
            let e = by_delay.entry(r.longest_delay).or_default();
            e.0 += 1;
            e.1 += extra_sends(r);
            e.2 += extra_serves(r);
        }
        for (delay, (n, sends, serves)) in &by_delay {
            println!(
                "[{label}] longest delay {delay}: {n} schedules, mean extra sends {:.2}, mean extra serves {:.2}",
                *sends as f64 / *n as f64,
                *serves as f64 / *n as f64
            );
        }
        let (n0, m0) = bucket(runs, 0, timeout);
        let (n1, m1) = bucket(runs, timeout, 2 * timeout);
        let (n2, m2) = bucket(runs, 2 * timeout, u64::MAX);
        println!(
            "[{label}] buckets: below timeout {n0} schedules mean {m0:.2}; one to two timeouts {n1} schedules mean {m1:.2}; two timeouts or more {n2} schedules mean {m2:.2}"
        );
    }

    /// With three attempts, extra work appears only when some reply was delayed past the timeout
    /// and grows with the longest delay a schedule imposed.
    #[test]
    fn fuzzed_schedules_recover_the_hazardous_label_of_rpc_retry() {
        let (runs, elapsed) = fuzz_baseline(3, TIMEOUT_TICKS);
        report("max_attempts = 3", &runs, elapsed);

        assert!(runs.iter().all(|r| r.distinct == (ROUNDS * REQUESTS_PER_ROUND) as u64));
        // No delay, no redundant work: the mechanism is delay-driven.
        assert!(
            runs.iter()
                .filter(|r| r.longest_delay < TIMEOUT_TICKS)
                .all(|r| extra_sends(r) == 0 && extra_serves(r) == 0),
            "a schedule that never delayed a reply past the timeout should not have re-sent or re-served"
        );
        // Some schedule makes the same 48 requests cost more than 48 sends and 48 serves.
        assert!(runs.iter().any(|r| extra_sends(r) > 0), "the fuzzer should find a schedule that re-sends");
        assert!(runs.iter().any(|r| extra_serves(r) > 0), "a re-send should make the server serve an id again");
        // Never more than the attempt cap.
        assert!(runs.iter().all(|r| extra_sends(r) <= 2 * r.distinct));
        // Extra work grows with delay.
        let (n1, m1) = bucket(&runs, TIMEOUT_TICKS, 2 * TIMEOUT_TICKS);
        let (n2, m2) = bucket(&runs, 2 * TIMEOUT_TICKS, u64::MAX);
        if n1 > 0 && n2 > 0 {
            assert!(m2 >= m1, "extra sends should grow with the delay the schedule imposed");
        }
    }

    /// With one attempt, the same schedules produce the same delays and no extra work at all.
    #[test]
    fn fuzzed_schedules_find_no_extra_work_without_retries() {
        let (runs, elapsed) = fuzz_baseline(1, TIMEOUT_TICKS);
        report("max_attempts = 1", &runs, elapsed);

        assert!(runs.iter().all(|r| r.distinct == (ROUNDS * REQUESTS_PER_ROUND) as u64));
        assert!(
            runs.iter().all(|r| extra_sends(r) == 0 && extra_serves(r) == 0),
            "without retries no schedule can add work"
        );
        // The delays are still there; only the response to them is gone.
        assert!(
            runs.iter().any(|r| r.longest_delay >= TIMEOUT_TICKS),
            "the fuzzer should still find schedules that delay a reply past the timeout"
        );
    }

    /// The delay a schedule can impose is bounded by the clock density (six elements per round),
    /// so raising the timeout leaves fewer schedules able to reach it, and past half the clock
    /// density no schedule can reach it twice. Expected before measuring: mean extra sends fall
    /// from timeout 2 to timeout 4 to timeout 6, and at timeout 4 and 6 no request is sent more
    /// than twice (extra sends at most 48).
    #[test]
    fn extra_work_falls_as_the_timeout_rises_past_the_reachable_delay() {
        let mut means = Vec::new();
        for timeout in [2u64, 4, 6] {
            let (runs, elapsed) = fuzz_baseline(3, timeout);
            report_with_timeout(&format!("max_attempts = 3, timeout = {timeout}"), &runs, elapsed, timeout);
            let mean = runs.iter().map(extra_sends).sum::<u64>() as f64 / runs.len() as f64;
            let max = runs.iter().map(extra_sends).max().unwrap_or(0);
            println!("[timeout = {timeout}] mean extra sends {mean:.2}, max {max}");
            if timeout >= 4 {
                assert!(max <= (ROUNDS * REQUESTS_PER_ROUND) as u64, "a delay of at most 6 can trigger at most one re-send when the timeout is 4 or more");
            }
            means.push(mean);
        }
        assert!(means[0] > means[1] && means[1] > means[2], "extra work should fall as the timeout rises: {means:?}");
    }
}

/// Shared machinery for the hold-one-edge sweep. Free functions and plain structs only, because
/// `impl` blocks are dropped when the module is staged. The module is private on purpose: the
/// staging step copies a private module's items inline, whereas a public module's items become
/// re-exports from the real crate, where a `cfg(test)` module does not exist.
#[cfg(test)]
mod hold_sweep {
    use std::collections::BTreeMap;
    use std::time::Instant;

    use hydro_lang::sim::hold_one_hook::HoldHandle;
    use hydro_lang::sim::work_counts::WorkCounts;

    /// Hold lengths, in harness rounds, tried for every hook.
    pub const GRID: &[usize] = &[0, 5, 10, 20, 40, 60, 80, 100];
    /// The round at which a hold begins; everything before it is warm-up under the prompt
    /// schedule.
    pub const HOLD_START: usize = 20;

    /// One execution's outcome, in the witness's own units of work.
    #[derive(Debug, Clone, Default)]
    pub struct Outcome {
        /// Work the distinct inputs require (for example two per request: one send, one serve).
        pub required: u64,
        /// Work actually done.
        pub total: u64,
        /// Times the held hook was forced to release (nonzero means the hold did not take).
        pub forced: u64,
        /// Hooks that had something buffered at least once (from the driver's discovery).
        pub hooks: Vec<String>,
        /// The simulator's own, program-independent counts for the same execution.
        pub counts: WorkCounts,
    }

    /// The curve for one hook.
    #[derive(Debug, Clone)]
    pub struct HookCurve {
        pub hook: String,
        /// `(k, extra work)` for each grid point, in the witness's own units.
        pub extra: Vec<(usize, i64)>,
        /// `k` values at which the hold was refused (forced releases).
        pub refused_at: Vec<usize>,
        /// `(k, counts)` for each grid point, from the simulator.
        pub counts: Vec<(usize, WorkCounts)>,
    }

    #[derive(Debug, Clone)]
    pub struct SweepResult {
        pub label: String,
        pub baseline_extra: i64,
        pub baseline_counts: WorkCounts,
        pub curves: Vec<HookCurve>,
        pub seconds: f64,
    }

    /// The gain of a curve: the largest extra work at any hold length, over the unheld run. A
    /// hold can only postpone work, never pull work into the run from beyond its end, so any
    /// positive gain is work the hold created. The curve need not be monotone in `k`: a program
    /// the hold tips into a persistently degraded state does a constant amount of extra work per
    /// remaining round, so its total falls as the hold grows and leaves fewer rounds after it.
    pub fn gain(extra: &[(usize, i64)]) -> i64 {
        let Some(&(_, first)) = extra.first() else {
            return 0;
        };
        extra.iter().map(|(_, e)| *e).max().unwrap_or(first) - first
    }

    /// Whether the curve rises to its peak without dipping (reported alongside the gain).
    pub fn monotone_to_peak(extra: &[(usize, i64)]) -> bool {
        let peak = extra.iter().map(|(_, e)| *e).max().unwrap_or(0);
        let mut prev = i64::MIN;
        for &(_, e) in extra {
            if e < prev {
                return false;
            }
            prev = e;
            if e == peak {
                return true;
            }
        }
        true
    }

    /// Whether the hold took at every nonzero `k`.
    pub fn holdable(curve: &HookCurve) -> bool {
        curve.refused_at.is_empty()
    }

    /// The verdict and the hook with the largest gain. Hazardous if some holdable hook has a
    /// positive gain; benign if no hold added any work.
    pub fn verdict(result: &SweepResult) -> (&'static str, Option<&HookCurve>) {
        let mut best: Option<(&HookCurve, i64)> = None;
        for c in result.curves.iter().filter(|c| holdable(c)) {
            let g = gain(&c.extra);
            if g > 0 && best.is_none_or(|(_, bg)| g > bg) {
                best = Some((c, g));
            }
        }
        match best {
            Some((c, _)) => ("hazardous", Some(c)),
            None => ("benign", None),
        }
    }

    /// Runs the sweep. `run(target, k)` executes the witness once under the hold driver,
    /// holding `target` (a hook location) from [`HOLD_START`] for `k` rounds, and returns the
    /// outcome. `run(None, 0)` is the discovery run.
    pub fn sweep(
        label: &str,
        grid: &[usize],
        run: &dyn Fn(Option<&str>, usize) -> Outcome,
    ) -> SweepResult {
        let started = Instant::now();
        let base = run(None, 0);
        let baseline_extra = base.total as i64 - base.required as i64;
        println!(
            "[{label}] baseline: required {} total {} extra {baseline_extra}; {} hooks with buffered input; admitted {} network {} handoffs {} runs {}",
            base.required,
            base.total,
            base.hooks.len(),
            base.counts.admitted(),
            base.counts.network(),
            base.counts.handoffs(),
            base.counts.runs(),
        );
        let mut curves = Vec::new();
        for hook in &base.hooks {
            let mut extra = vec![(0usize, baseline_extra)];
            let mut counts = vec![(0usize, base.counts.clone())];
            let mut refused_at = Vec::new();
            for &k in grid.iter().filter(|k| **k > 0) {
                let o = run(Some(hook), k);
                let e = o.total as i64 - o.required as i64;
                if o.forced > 0 {
                    refused_at.push(k);
                }
                extra.push((k, e));
                counts.push((k, o.counts));
            }
            let curve = HookCurve {
                hook: hook.clone(),
                extra,
                refused_at,
                counts,
            };
            println!(
                "[{label}] hold {}: extra {:?} gain {}{}{}",
                curve.hook,
                curve.extra.iter().map(|(_, e)| *e).collect::<Vec<_>>(),
                gain(&curve.extra),
                if monotone_to_peak(&curve.extra) { "" } else { " (dips)" },
                if holdable(&curve) {
                    String::new()
                } else {
                    format!(" (refused at k = {:?})", curve.refused_at)
                }
            );
            for (name, measure) in MEASURES {
                let series = measure_series(&curve, *measure);
                println!(
                    "[{label}]   {name}: {:?} gain {}",
                    series.iter().map(|(_, v)| *v).collect::<Vec<_>>(),
                    gain(&series)
                );
            }
            curves.push(curve);
        }
        let result = SweepResult {
            label: label.to_owned(),
            baseline_extra,
            baseline_counts: base.counts,
            curves,
            seconds: started.elapsed().as_secs_f64(),
        };
        let (v, best) = verdict(&result);
        println!(
            "[{label}] verdict {v}{} in {:.1} s",
            best.map(|c| format!(", largest gain {} at {}", gain(&c.extra), c.hook))
                .unwrap_or_default(),
            result.seconds
        );
        for (name, measure) in MEASURES {
            let (v, best) = generic_verdict(&result, *measure);
            println!(
                "[{label}] verdict by {name}: {v}{}",
                best.map(|c| format!(
                    ", largest gain {} at {}",
                    gain(&measure_series(c, *measure)),
                    c.hook
                ))
                .unwrap_or_default(),
            );
        }
        if let (_, Some(c)) = generic_verdict(&result, Measure::Admitted) {
            println!(
                "[{label}] admitted records rose at (peak vs unheld): {}",
                admitted_rises(&result.baseline_counts, c)
            );
        }
        let (cv, cbest) = checker_verdict(&result);
        println!(
            "[{label}] verdict by checker rule: {cv}{}",
            cbest
                .as_ref()
                .map(|(c, what, g)| format!(", largest gain {g} in {what} when holding {}", c.hook))
                .unwrap_or_default()
        );
        if let Some((c, _, _)) = cbest {
            println!(
                "[{label}] hooks that admitted more under that hold: {}",
                hook_rises(&result.baseline_counts, c)
            );
        }
        result
    }

    /// A program-independent work measure read from [`WorkCounts`].
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Measure {
        /// Records released into ticks by `use::batch` hooks (the checker's measure).
        Admitted,
        /// Network messages sent.
        Network,
        /// Items read out of DFIR handoffs.
        Handoffs,
        /// DFIR subgraph runs.
        Runs,
    }

    /// Every measure with its report name, in report order.
    pub const MEASURES: &[(&str, Measure)] = &[
        ("admitted", Measure::Admitted),
        ("network", Measure::Network),
        ("handoffs", Measure::Handoffs),
        ("runs", Measure::Runs),
    ];

    pub fn measure_value(counts: &WorkCounts, measure: Measure) -> i64 {
        (match measure {
            Measure::Admitted => counts.admitted(),
            Measure::Network => counts.network(),
            Measure::Handoffs => counts.handoffs(),
            Measure::Runs => counts.runs(),
        }) as i64
    }

    /// The curve of one measure for one held hook, as `(k, value)`.
    pub fn measure_series(curve: &HookCurve, measure: Measure) -> Vec<(usize, i64)> {
        curve
            .counts
            .iter()
            .map(|(k, c)| (*k, measure_value(c, measure)))
            .collect()
    }

    /// The verdict under a program-independent measure, with the hook whose hold produced the
    /// largest gain. Same rule as [`verdict`]: hazardous if some holdable hook's hold raises the
    /// measure above the unheld run, benign otherwise. No requirement is subtracted because the
    /// input, and so the requirement, is the same at every grid point.
    pub fn generic_verdict(
        result: &SweepResult,
        measure: Measure,
    ) -> (&'static str, Option<&HookCurve>) {
        let mut best: Option<(&HookCurve, i64)> = None;
        for c in result.curves.iter().filter(|c| holdable(c)) {
            let g = gain(&measure_series(c, measure));
            if g > 0 && best.is_none_or(|(_, bg)| g > bg) {
                best = Some((c, g));
            }
        }
        match best {
            Some((c, _)) => ("hazardous", Some(c)),
            None => ("benign", None),
        }
    }

    /// Which hooks admitted more records at the curve's peak than in the unheld run, and by how
    /// much: the second localization signal, naming the edges the extra records crossed.
    pub fn admitted_rises(baseline: &WorkCounts, curve: &HookCurve) -> String {
        let series = measure_series(curve, Measure::Admitted);
        let peak_k = series
            .iter()
            .max_by_key(|(_, v)| *v)
            .map(|(k, _)| *k)
            .unwrap_or(0);
        let Some((_, peak)) = curve.counts.iter().find(|(k, _)| *k == peak_k) else {
            return String::new();
        };
        let mut rises: BTreeMap<i64, Vec<String>> = BTreeMap::new();
        for (hook, n) in &peak.hook_releases {
            let before = baseline.hook_releases.get(hook).copied().unwrap_or(0) as i64;
            let d = *n as i64 - before;
            if d > 0 {
                rises.entry(-d).or_default().push(hook.clone());
            }
        }
        rises
            .into_iter()
            .flat_map(|(neg, hooks)| hooks.into_iter().map(move |h| format!("{h} +{}", -neg)))
            .collect::<Vec<_>>()
            .join("; ")
    }

    /// The checker's rule. For one held hook, the gain is the largest rise over the unheld run,
    /// at any hold length, of either the program's total admitted records or any one member's
    /// outgoing network messages. Messages are read per sender because a total can hide a
    /// reaction when another sender goes quiet (a leader that stops sending heartbeats while
    /// followers start sending vote requests); admitted records are read in total because within
    /// a location a reaction can replace one record with another (a completion becomes an
    /// abandonment) without adding work. Returns the gain and the name of what rose.
    pub fn checker_gain(baseline: &WorkCounts, curve: &HookCurve) -> (i64, Option<String>) {
        let base_sends = baseline.sends_by_member();
        let base_admitted = baseline.admitted() as i64;
        let mut best: (i64, Option<String>) = (0, None);
        for (_, counts) in &curve.counts {
            let d = counts.admitted() as i64 - base_admitted;
            if d > best.0 {
                best = (d, Some("admitted records".to_owned()));
            }
            for (place, n) in counts.sends_by_member() {
                let before = base_sends.get(&place).copied().unwrap_or(0) as i64;
                let d = n as i64 - before;
                if d > best.0 {
                    best = (d, Some(place));
                }
            }
        }
        best
    }

    /// The verdict under the checker's rule, with the held hook that produced the largest gain
    /// and the name of what rose.
    pub fn checker_verdict(
        result: &SweepResult,
    ) -> (&'static str, Option<(&HookCurve, String, i64)>) {
        let mut best: Option<(&HookCurve, String, i64)> = None;
        for c in result.curves.iter().filter(|c| holdable(c)) {
            let (g, what) = checker_gain(&result.baseline_counts, c);
            if g > 0 && best.as_ref().is_none_or(|(_, _, bg)| g > *bg) {
                best = Some((c, what.unwrap_or_default(), g));
            }
        }
        match best {
            Some(b) => ("hazardous", Some(b)),
            None => ("benign", None),
        }
    }

    /// Which hooks on which members admitted more records at any hold length than in the unheld
    /// run: reported for localization only (see [`WorkCounts::hooks_by_member`]).
    pub fn hook_rises(baseline: &WorkCounts, curve: &HookCurve) -> String {
        let base = baseline.hooks_by_member();
        let mut best: BTreeMap<String, i64> = BTreeMap::new();
        for (_, counts) in &curve.counts {
            for (place, n) in counts.hooks_by_member() {
                let d = n as i64 - base.get(&place).copied().unwrap_or(0) as i64;
                if d > 0 {
                    let e = best.entry(place).or_default();
                    *e = (*e).max(d);
                }
            }
        }
        let mut v: Vec<(String, i64)> = best.into_iter().collect();
        v.sort_by_key(|(_, d)| -*d);
        v.into_iter()
            .map(|(p, d)| format!("{p} +{d}"))
            .collect::<Vec<_>>()
            .join("; ")
    }

    /// Applies the hold plan inside a round loop: call once per round before sending inputs.
    pub fn apply_hold(handle: &HoldHandle, target: Option<&str>, k: usize, round: usize) {
        if let Some(t) = target {
            if round == HOLD_START && k > 0 {
                handle.begin_hold(t);
            }
            if round == HOLD_START + k {
                handle.end_hold();
            }
        }
    }
}

/// Hold-one-edge sweep over `rpc_retry` at its own parameters (timeout 40, capacity 5).
#[cfg(test)]
mod hold_rpc_retry {
    use std::collections::HashSet;
    use std::time::Duration;

    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::hold_one_hook::HoldOneHookDriver;
    use hydro_lang::sim::quiesce;

    use crate::cluster::rpc_retry::{Client, RetryPolicy, Server, ServerConfig, rpc_with_retries};
    use crate::cluster::witnesses::checker_experiments::hold_sweep::{self, Outcome, apply_hold};

    const ROUNDS: usize = 240;
    const REQUESTS_PER_ROUND: usize = 2;

    pub(super) fn run(max_attempts: u32, target: Option<&str>, k: usize) -> Outcome {
        let policy = RetryPolicy {
            timeout_ticks: 40,
            max_attempts,
        };
        let server_config = ServerConfig {
            max_per_tick: 5,
            service_time: Duration::ZERO,
        };
        let mut flow = FlowBuilder::new();
        let client = flow.process::<Client>();
        let server = flow.process::<Server>();
        let (request_send, requests) = client.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let (clock_send, client_clock) = client.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (_crt, client_report_tick) = client.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (_srt, server_report_tick) = server.sim_input::<(), TotalOrder, ExactlyOnce>();
        let outputs = rpc_with_retries(
            &client,
            &server,
            requests,
            client_clock,
            client_report_tick,
            server_report_tick,
            policy,
            server_config,
        );
        let completed = outputs.completed.map(q!(|c| c.id)).sim_output();
        let outgoing = outputs.outgoing.map(q!(|r| r.id)).sim_output();
        let processed = outputs.processed.map(q!(|r| r.id)).sim_output();

        let (driver, handle) = HoldOneHookDriver::new();
        hydro_lang::sim::work_counts::enable();
        let mut out = Outcome::default();
        let out_ref = &mut out;
        let handle_ref = &handle;
        flow.sim().run_with_driver(driver, async || {
            let mut sent_ids: HashSet<u64> = HashSet::new();
            let mut sends = 0u64;
            let mut serves = 0u64;
            for round in 0..ROUNDS {
                apply_hold(handle_ref, target, k, round);
                clock_send.send(());
                for _ in 0..REQUESTS_PER_ROUND {
                    request_send.send(round as u64);
                }
                quiesce().await;
                let _ = completed.collect_sorted::<Vec<_>>().await;
                while let Some(id) = outgoing.try_next().await {
                    sent_ids.insert(id);
                    sends += 1;
                }
                while processed.try_next().await.is_some() {
                    serves += 1;
                }
            }
            out_ref.required = 2 * sent_ids.len() as u64;
            out_ref.total = sends + serves;
        });
        out.forced = handle.forced_releases();
        out.counts = hydro_lang::sim::work_counts::take();
        out.hooks = handle.hooks_seen();
        out
    }

    #[test]
    fn hold_sweep_rpc_retry_three_attempts() {
        let r = hold_sweep::sweep("rpc_retry max_attempts=3", hold_sweep::GRID, &|t, k| run(3, t, k));
        let (v, best) = hold_sweep::verdict(&r);
        assert_eq!(v, "hazardous");
        assert!(best.unwrap().hook.contains("rpc_retry.rs"));
    }

    #[test]
    fn hold_sweep_rpc_retry_one_attempt() {
        let r = hold_sweep::sweep("rpc_retry max_attempts=1", hold_sweep::GRID, &|t, k| run(1, t, k));
        let (v, _) = hold_sweep::verdict(&r);
        assert_eq!(v, "benign");
    }
}

/// Hold sweep over `backoff_retry` (base timeout 40, capacity 5), with and without backoff.
#[cfg(test)]
mod hold_backoff_retry {
    use std::collections::HashSet;

    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::hold_one_hook::HoldOneHookDriver;
    use hydro_lang::sim::quiesce;

    use crate::cluster::witnesses::backoff_retry::{
        Client, RetryPolicy, Server, ServerConfig, rpc_with_backoff,
    };
    use crate::cluster::witnesses::checker_experiments::hold_sweep::{self, Outcome, apply_hold};

    const ROUNDS: usize = 240;

    pub(super) fn run(backoff: bool, target: Option<&str>, k: usize) -> Outcome {
        let policy = RetryPolicy {
            base_timeout_ticks: 40,
            max_attempts: 3,
            backoff,
        };
        let mut flow = FlowBuilder::new();
        let client = flow.process::<Client>();
        let server = flow.process::<Server>();
        let (request_send, requests) = client.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let (clock_send, client_clock) = client.sim_input::<(), TotalOrder, ExactlyOnce>();
        let outputs = rpc_with_backoff(
            &client,
            &server,
            requests,
            client_clock,
            policy,
            ServerConfig { max_per_tick: 5 },
        );
        let completed = outputs.completed.map(q!(|c| c.id)).sim_output();
        let outgoing = outputs.outgoing.map(q!(|r| r.id)).sim_output();
        let processed = outputs.processed.map(q!(|r| r.id)).sim_output();

        let (driver, handle) = HoldOneHookDriver::new();
        hydro_lang::sim::work_counts::enable();
        let mut out = Outcome::default();
        let out_ref = &mut out;
        let handle_ref = &handle;
        flow.sim().run_with_driver(driver, async || {
            let mut sent_ids: HashSet<u64> = HashSet::new();
            let mut sends = 0u64;
            let mut serves = 0u64;
            for round in 0..ROUNDS {
                apply_hold(handle_ref, target, k, round);
                clock_send.send(());
                for _ in 0..2 {
                    request_send.send(round as u64);
                }
                quiesce().await;
                let _ = completed.collect_sorted::<Vec<_>>().await;
                while let Some(id) = outgoing.try_next().await {
                    sent_ids.insert(id);
                    sends += 1;
                }
                while processed.try_next().await.is_some() {
                    serves += 1;
                }
            }
            out_ref.required = 2 * sent_ids.len() as u64;
            out_ref.total = sends + serves;
        });
        out.forced = handle.forced_releases();
        out.counts = hydro_lang::sim::work_counts::take();
        out.hooks = handle.hooks_seen();
        out
    }

    #[test]
    fn hold_sweep_backoff_retry_backoff_on() {
        let r = hold_sweep::sweep("backoff_retry backoff=true", hold_sweep::GRID, &|t, k| run(true, t, k));
        assert_eq!(hold_sweep::verdict(&r).0, "hazardous");
    }

    #[test]
    fn hold_sweep_backoff_retry_backoff_off() {
        let r = hold_sweep::sweep("backoff_retry backoff=false", hold_sweep::GRID, &|t, k| run(false, t, k));
        assert_eq!(hold_sweep::verdict(&r).0, "hazardous");
    }
}

/// Hold sweep over `bounded_queue_rejection` (timeout 40, capacity 5), bounded and unbounded.
#[cfg(test)]
mod hold_bounded_queue {
    use std::collections::HashSet;

    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::hold_one_hook::HoldOneHookDriver;
    use hydro_lang::sim::quiesce;

    use crate::cluster::witnesses::bounded_queue_rejection::{
        Client, RetryPolicy, Server, ServerConfig, rpc_with_bounded_queue,
    };
    use crate::cluster::witnesses::checker_experiments::hold_sweep::{self, Outcome, apply_hold};

    const ROUNDS: usize = 240;

    pub(super) fn run(max_backlog: Option<usize>, target: Option<&str>, k: usize) -> Outcome {
        let policy = RetryPolicy {
            timeout_ticks: 40,
            max_attempts: 3,
            reject_backoff_ticks: 10,
        };
        let mut flow = FlowBuilder::new();
        let client = flow.process::<Client>();
        let server = flow.process::<Server>();
        let (request_send, requests) = client.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let (clock_send, client_clock) = client.sim_input::<(), TotalOrder, ExactlyOnce>();
        let outputs = rpc_with_bounded_queue(
            &client,
            &server,
            requests,
            client_clock,
            policy,
            ServerConfig {
                max_per_tick: 5,
                max_backlog,
            },
        );
        let completed = outputs.completed.map(q!(|c| c.id)).sim_output();
        let outgoing = outputs.outgoing.map(q!(|r| r.id)).sim_output();
        let processed = outputs.processed.map(q!(|r| r.id)).sim_output();
        let rejected = outputs.rejected.map(q!(|r| r.id)).sim_output();

        let (driver, handle) = HoldOneHookDriver::new();
        hydro_lang::sim::work_counts::enable();
        let mut out = Outcome::default();
        let out_ref = &mut out;
        let handle_ref = &handle;
        flow.sim().run_with_driver(driver, async || {
            let mut sent_ids: HashSet<u64> = HashSet::new();
            let mut work = 0u64;
            for round in 0..ROUNDS {
                apply_hold(handle_ref, target, k, round);
                clock_send.send(());
                for _ in 0..2 {
                    request_send.send(round as u64);
                }
                quiesce().await;
                let _ = completed.collect_sorted::<Vec<_>>().await;
                while let Some(id) = outgoing.try_next().await {
                    sent_ids.insert(id);
                    work += 1;
                }
                while processed.try_next().await.is_some() {
                    work += 1;
                }
                while rejected.try_next().await.is_some() {
                    work += 1;
                }
            }
            out_ref.required = 2 * sent_ids.len() as u64;
            out_ref.total = work;
        });
        out.forced = handle.forced_releases();
        out.counts = hydro_lang::sim::work_counts::take();
        out.hooks = handle.hooks_seen();
        out
    }

    #[test]
    fn hold_sweep_bounded_queue_some_100() {
        let r = hold_sweep::sweep("bounded_queue max_backlog=Some(100)", hold_sweep::GRID, &|t, k| {
            run(Some(100), t, k)
        });
        assert_eq!(hold_sweep::verdict(&r).0, "hazardous");
    }

    #[test]
    fn hold_sweep_bounded_queue_none() {
        let r = hold_sweep::sweep("bounded_queue max_backlog=None", hold_sweep::GRID, &|t, k| run(None, t, k));
        assert_eq!(hold_sweep::verdict(&r).0, "hazardous");
    }
}

/// Hold sweep over `cache_thundering_herd` (TTL 20, origin capacity 4), three configurations.
/// Work is fetches issued plus fetches processed at the origin; the requirement is taken as
/// zero, so the curve is total work and the verdict reads its shape.
#[cfg(test)]
mod hold_cache {
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::hold_one_hook::HoldOneHookDriver;
    use hydro_lang::sim::quiesce;

    use crate::cluster::witnesses::cache_thundering_herd::{
        Cache, CacheConfig, Key, Origin, OriginConfig, cache_with_expiry,
    };
    use crate::cluster::witnesses::checker_experiments::hold_sweep::{self, Outcome, apply_hold};

    const ROUNDS: usize = 240;
    const HOT_KEYS: u64 = 10;
    const HOT_PER_ROUND: u64 = 8;

    pub(super) fn run(coalesce: bool, request_dated: bool, target: Option<&str>, k: usize) -> Outcome {
        let mut flow = FlowBuilder::new();
        let cache = flow.process::<Cache>();
        let origin = flow.process::<Origin>();
        let (lookup_send, lookups) = cache.sim_input::<Key, TotalOrder, ExactlyOnce>();
        let (cache_clock_send, cache_clock) = cache.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (origin_clock_send, origin_clock) = origin.sim_input::<(), TotalOrder, ExactlyOnce>();
        let outputs = cache_with_expiry(
            &cache,
            &origin,
            lookups,
            cache_clock,
            origin_clock,
            CacheConfig {
                ttl_ticks: 20,
                coalesce,
                request_dated,
            },
            OriginConfig { max_fetch_per_tick: 4 },
        );
        let completed = outputs.completed.map(q!(|c| c.id)).sim_output();
        let fetches = outputs.fetches.map(q!(|f| f.lookup_id)).sim_output();
        let processed = outputs.processed.map(q!(|f| f.lookup_id)).sim_output();
        let applied = outputs.fills_applied.map(q!(|f| f.lookup_id)).sim_output();
        let redundant = outputs.fills_redundant.map(q!(|f| f.lookup_id)).sim_output();
        let stale = outputs.fills_stale.map(q!(|f| f.lookup_id)).sim_output();

        let (driver, handle) = HoldOneHookDriver::new();
        hydro_lang::sim::work_counts::enable();
        let mut out = Outcome::default();
        let out_ref = &mut out;
        let handle_ref = &handle;
        flow.sim().run_with_driver(driver, async || {
            let mut work = 0u64;
            for round in 0..ROUNDS as u64 {
                apply_hold(handle_ref, target, k, round as usize);
                cache_clock_send.send(());
                origin_clock_send.send(());
                for i in 0..HOT_PER_ROUND {
                    lookup_send.send(((round * HOT_PER_ROUND + i) % HOT_KEYS) as Key);
                }
                quiesce().await;
                let _ = completed.collect_sorted::<Vec<_>>().await;
                let _ = applied.collect_sorted::<Vec<_>>().await;
                let _ = redundant.collect_sorted::<Vec<_>>().await;
                let _ = stale.collect_sorted::<Vec<_>>().await;
                while fetches.try_next().await.is_some() {
                    work += 1;
                }
                while processed.try_next().await.is_some() {
                    work += 1;
                }
            }
            out_ref.required = 0;
            out_ref.total = work;
        });
        out.forced = handle.forced_releases();
        out.counts = hydro_lang::sim::work_counts::take();
        out.hooks = handle.hooks_seen();
        out
    }

    #[test]
    fn hold_sweep_cache_request_dated_no_coalesce() {
        let r = hold_sweep::sweep("cache request_dated, no coalesce", hold_sweep::GRID, &|t, k| {
            run(false, true, t, k)
        });
        assert_eq!(hold_sweep::verdict(&r).0, "hazardous");
    }

    #[test]
    fn hold_sweep_cache_fill_dated_no_coalesce() {
        let r = hold_sweep::sweep("cache fill_dated, no coalesce", hold_sweep::GRID, &|t, k| {
            run(false, false, t, k)
        });
        assert_eq!(hold_sweep::verdict(&r).0, "hazardous");
    }

    #[test]
    fn hold_sweep_cache_coalesce() {
        let r = hold_sweep::sweep("cache coalesce", hold_sweep::GRID, &|t, k| run(true, true, t, k));
        assert_eq!(hold_sweep::verdict(&r).0, "benign");
    }
}

/// Hold sweep over `gossip_resend` (5 members, 5 merges per tick, ack timeout 3 or off). Extra
/// work is re-sends plus repeated merges.
#[cfg(test)]
mod hold_gossip_resend {
    use std::collections::HashSet;

    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::hold_one_hook::HoldOneHookDriver;
    use hydro_lang::sim::quiesce;

    use crate::cluster::witnesses::checker_experiments::hold_sweep::{self, Outcome, apply_hold};
    use crate::cluster::witnesses::gossip_resend::{GossipConfig, Node, gossip_with_resend};

    const MEMBERS: u32 = 5;
    const ROUNDS: usize = 240;

    pub(super) fn run(ack_timeout_ticks: u64, target: Option<&str>, k: usize) -> Outcome {
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<Node>();
        let (timer_send, timer) = cluster.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (update_send, updates) = cluster.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let outputs = gossip_with_resend(
            &cluster,
            timer,
            updates,
            GossipConfig {
                max_merges_per_tick: 5,
                ack_timeout_ticks,
            },
        );
        let wire = outputs.wire.map(q!(|(to, d)| (to, d.from, d.seq))).sim_cluster_output();
        let merged = outputs.merged.map(q!(|d| (d.from, d.seq))).sim_cluster_output();
        let acks = outputs.acks_sent.sim_cluster_output();
        let inbox = outputs.inbox_depth.sim_cluster_output();
        let outstanding = outputs.outstanding_depth.sim_cluster_output();

        let (driver, handle) = HoldOneHookDriver::new();
        hydro_lang::sim::work_counts::enable();
        let mut out = Outcome::default();
        let out_ref = &mut out;
        let handle_ref = &handle;
        flow.sim().with_cluster_size(&cluster, MEMBERS as usize).run_with_driver(driver, async || {
            let mut sent: HashSet<(u32, u32, u64)> = HashSet::new();
            let mut merged_ids: HashSet<(u32, u32, u64)> = HashSet::new();
            let mut first = 0u64;
            let mut total = 0u64;
            let mut next_value = 0u64;
            for round in 0..ROUNDS {
                apply_hold(handle_ref, target, k, round);
                for m in 0..MEMBERS {
                    timer_send.send(m, ());
                    update_send.send(m, next_value);
                    next_value += 1;
                }
                quiesce().await;
                for m in 0..MEMBERS {
                    while let Some(x) = wire.try_next(m).await {
                        total += 1;
                        if sent.insert(x) {
                            first += 1;
                        }
                    }
                    while let Some((from, seq)) = merged.try_next(m).await {
                        total += 1;
                        if merged_ids.insert((m, from, seq)) {
                            first += 1;
                        }
                    }
                    while acks.try_next(m).await.is_some() {}
                    while inbox.try_next(m).await.is_some() {}
                    while outstanding.try_next(m).await.is_some() {}
                }
            }
            out_ref.required = first;
            out_ref.total = total;
        });
        out.forced = handle.forced_releases();
        out.counts = hydro_lang::sim::work_counts::take();
        out.hooks = handle.hooks_seen();
        out
    }

    #[test]
    fn hold_sweep_gossip_resend_on() {
        let r = hold_sweep::sweep("gossip_resend ack_timeout=3", hold_sweep::GRID, &|t, k| run(3, t, k));
        assert_eq!(hold_sweep::verdict(&r).0, "hazardous");
    }

    #[test]
    fn hold_sweep_gossip_resend_off() {
        let r = hold_sweep::sweep("gossip_resend ack_timeout=0", hold_sweep::GRID, &|t, k| run(0, t, k));
        assert_eq!(hold_sweep::verdict(&r).0, "benign");
    }
}

/// Hold sweep over `election_stampede` (5 members, timeout 6, budget 3). Work is vote requests
/// sent plus elections started; the requirement is zero because a stable leader needs neither.
#[cfg(test)]
mod hold_election {
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::hold_one_hook::HoldOneHookDriver;
    use hydro_lang::sim::quiesce;

    use crate::cluster::witnesses::checker_experiments::hold_sweep::{self, Outcome, apply_hold};
    use crate::cluster::witnesses::election_stampede::{ElectionConfig, Msg, Node, election};

    const MEMBERS: u32 = 5;
    const ROUNDS: usize = 240;

    pub(super) fn run(timeout_spread_ticks: u64, target: Option<&str>, k: usize) -> Outcome {
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<Node>();
        let (timer_send, timer) = cluster.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (client_send, clients) = cluster.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let outputs = election(
            &cluster,
            timer,
            clients,
            ElectionConfig {
                election_timeout_ticks: 6,
                timeout_spread_ticks,
                budget_per_tick: 3,
            },
        );
        let vote_requests = outputs
            .wire
            .filter_map(q!(|(_, msg)| match msg {
                Msg::VoteRequest { .. } => Some(()),
                _ => None,
            }))
            .sim_cluster_output();
        let processed = outputs.processed.sim_cluster_output();
        let inbox = outputs.inbox_depth.sim_cluster_output();
        let state = outputs.state_trace.sim_cluster_output();
        let elections = outputs.elections.sim_cluster_output();

        let (driver, handle) = HoldOneHookDriver::new();
        hydro_lang::sim::work_counts::enable();
        let mut out = Outcome::default();
        let out_ref = &mut out;
        let handle_ref = &handle;
        flow.sim().with_cluster_size(&cluster, MEMBERS as usize).run_with_driver(driver, async || {
            let mut work = 0u64;
            let mut next_client = 0u64;
            for round in 0..ROUNDS {
                apply_hold(handle_ref, target, k, round);
                for m in 0..MEMBERS {
                    timer_send.send(m, ());
                    client_send.send(m, next_client);
                    next_client += 1;
                }
                quiesce().await;
                for m in 0..MEMBERS {
                    while vote_requests.try_next(m).await.is_some() {
                        work += 1;
                    }
                    while elections.try_next(m).await.is_some() {
                        work += 1;
                    }
                    while processed.try_next(m).await.is_some() {}
                    while inbox.try_next(m).await.is_some() {}
                    while state.try_next(m).await.is_some() {}
                }
            }
            out_ref.required = 0;
            out_ref.total = work;
        });
        out.forced = handle.forced_releases();
        out.counts = hydro_lang::sim::work_counts::take();
        out.hooks = handle.hooks_seen();
        out
    }

    #[test]
    fn hold_sweep_election_uniform_timeouts() {
        let r = hold_sweep::sweep("election spread=0", hold_sweep::GRID, &|t, k| run(0, t, k));
        assert_eq!(hold_sweep::verdict(&r).0, "hazardous");
    }

    #[test]
    fn hold_sweep_election_spread_3() {
        let r = hold_sweep::sweep("election spread=3", hold_sweep::GRID, &|t, k| run(3, t, k));
        assert_eq!(hold_sweep::verdict(&r).0, "hazardous");
    }
}

/// Hold sweep over `rebalancing_ping_pong` (2 workers, reports every 4 rounds). Work is
/// migrations; the requirement is zero.
#[cfg(test)]
mod hold_rebalancing {
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::hold_one_hook::HoldOneHookDriver;
    use hydro_lang::sim::quiesce;

    use crate::cluster::witnesses::checker_experiments::hold_sweep::{self, Outcome, apply_hold};
    use crate::cluster::witnesses::rebalancing_ping_pong::{
        RebalanceConfig, Worker, rebalancing_workers,
    };

    const N: u32 = 2;
    const ROUNDS: usize = 240;

    pub(super) fn run(cooldown_ticks: u64, target: Option<&str>, k: usize) -> Outcome {
        let mut flow = FlowBuilder::new();
        let workers = flow.cluster::<Worker>();
        let (task_send, tasks) = workers.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let (clock_send, clock) = workers.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (report_send, report_tick) = workers.sim_input::<(), TotalOrder, ExactlyOnce>();
        let outputs = rebalancing_workers(
            &workers,
            tasks,
            clock,
            report_tick,
            RebalanceConfig {
                work_per_tick: 5,
                threshold: 10,
                cooldown_ticks,
            },
        );
        let completed = outputs.completed.map(q!(|t| t.id)).sim_cluster_output();
        let migrated = outputs.migrated.map(q!(|(_, t)| t.id)).sim_cluster_output();
        let ticks = outputs.ticks.map(q!(|t| t.queue_len)).sim_cluster_output();

        let (driver, handle) = HoldOneHookDriver::new();
        hydro_lang::sim::work_counts::enable();
        let mut out = Outcome::default();
        let out_ref = &mut out;
        let handle_ref = &handle;
        flow.sim().with_cluster_size(&workers, N as usize).run_with_driver(driver, async || {
            let mut work = 0u64;
            let mut next_id = 0u64;
            for round in 0..ROUNDS {
                apply_hold(handle_ref, target, k, round);
                for w in 0..N {
                    clock_send.send(w, ());
                    if round % 4 == 0 {
                        report_send.send(w, ());
                    }
                    for _ in 0..3 {
                        task_send.send(w, next_id);
                        next_id += 1;
                    }
                }
                quiesce().await;
                for w in 0..N {
                    while completed.try_next(w).await.is_some() {}
                    while migrated.try_next(w).await.is_some() {
                        work += 1;
                    }
                    while ticks.try_next(w).await.is_some() {}
                }
            }
            out_ref.required = 0;
            out_ref.total = work;
        });
        out.forced = handle.forced_releases();
        out.counts = hydro_lang::sim::work_counts::take();
        out.hooks = handle.hooks_seen();
        out
    }

    #[test]
    fn hold_sweep_rebalancing_no_cooldown() {
        let r = hold_sweep::sweep("rebalancing cooldown=0", hold_sweep::GRID, &|t, k| run(0, t, k));
        assert_eq!(hold_sweep::verdict(&r).0, "hazardous");
    }

    #[test]
    fn hold_sweep_rebalancing_cooldown_8() {
        let r = hold_sweep::sweep("rebalancing cooldown=8", hold_sweep::GRID, &|t, k| run(8, t, k));
        assert_eq!(hold_sweep::verdict(&r).0, "hazardous");
    }
}

/// Hold sweep over `compaction_falls_behind` (budget 30, 1 put and 4 gets per round). Extra work
/// is serve units beyond one per operation served.
#[cfg(test)]
mod hold_compaction {
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::hold_one_hook::HoldOneHookDriver;
    use hydro_lang::sim::quiesce;

    use crate::cluster::witnesses::checker_experiments::hold_sweep::{self, Outcome, apply_hold};
    use crate::cluster::witnesses::compaction_falls_behind::{
        Op, Store, StoreConfig, log_with_compaction,
    };

    const ROUNDS: usize = 240;

    pub(super) fn run(compaction_reserve: u64, target: Option<&str>, k: usize) -> Outcome {
        let mut flow = FlowBuilder::new();
        let store = flow.process::<Store>();
        let (op_send, ops) = store.sim_input::<Op, TotalOrder, ExactlyOnce>();
        let (clock_send, clock) = store.sim_input::<(), TotalOrder, ExactlyOnce>();
        let outputs = log_with_compaction(
            &store,
            ops,
            clock,
            StoreConfig {
                budget_per_tick: 30,
                compaction_reserve,
            },
        );
        let served = outputs.served.map(q!(|s| s.id)).sim_output();
        let ticks = outputs.ticks.map(q!(|t| t.serve_units)).sim_output();

        let (driver, handle) = HoldOneHookDriver::new();
        hydro_lang::sim::work_counts::enable();
        let mut out = Outcome::default();
        let out_ref = &mut out;
        let handle_ref = &handle;
        flow.sim().run_with_driver(driver, async || {
            let mut served_ops = 0u64;
            let mut units = 0u64;
            for round in 0..ROUNDS {
                apply_hold(handle_ref, target, k, round);
                clock_send.send(());
                op_send.send(Op::Put);
                for _ in 0..4 {
                    op_send.send(Op::Get);
                }
                quiesce().await;
                while served.try_next().await.is_some() {
                    served_ops += 1;
                }
                while let Some(u) = ticks.try_next().await {
                    units += u;
                }
            }
            out_ref.required = served_ops;
            out_ref.total = units;
        });
        out.forced = handle.forced_releases();
        out.counts = hydro_lang::sim::work_counts::take();
        out.hooks = handle.hooks_seen();
        out
    }

    #[test]
    fn hold_sweep_compaction_reserve_0() {
        let r = hold_sweep::sweep("compaction reserve=0", hold_sweep::GRID, &|t, k| run(0, t, k));
        assert_eq!(hold_sweep::verdict(&r).0, "hazardous");
    }

    /// The corpus records this configuration as having no ground truth but expects the tool to find
    /// amplification. The sweep finds no hold that adds work under the corpus input (see the
    /// module docs), so this test records the verdict instead of asserting the expectation.
    #[test]
    fn hold_sweep_compaction_reserve_8() {
        let r = hold_sweep::sweep("compaction reserve=8", hold_sweep::GRID, &|t, k| run(8, t, k));
        println!("[compaction reserve=8] verdict {}", hold_sweep::verdict(&r).0);
    }
}

/// Hold sweep over `lease_renewal_storm` (20 clients, period 10, server capacity 5). Extra work
/// is renewals re-sent plus re-sends served.
#[cfg(test)]
mod hold_lease {
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::hold_one_hook::HoldOneHookDriver;
    use hydro_lang::sim::quiesce;

    use crate::cluster::witnesses::checker_experiments::hold_sweep::{self, Outcome, apply_hold};
    use crate::cluster::witnesses::lease_renewal_storm::{
        ClientPolicy, Job, LeaseClient, LeaseServer, ServerConfig, lease_renewal,
    };

    const N: u32 = 20;
    const ROUNDS: usize = 240;

    pub(super) fn run(resend_after_ticks: Option<u64>, target: Option<&str>, k: usize) -> Outcome {
        let mut flow = FlowBuilder::new();
        let clients = flow.cluster::<LeaseClient>();
        let server = flow.process::<LeaseServer>();
        let (client_clock_send, client_clock) = clients.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (server_clock_send, server_clock) = server.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (_other_send, other_work) = server.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let outputs = lease_renewal(
            &clients,
            &server,
            client_clock,
            server_clock,
            other_work,
            ClientPolicy {
                renew_every_ticks: 10,
                lease_ticks: 30,
                resend_after_ticks,
            },
            ServerConfig { max_per_tick: 5 },
        );
        let sent = outputs.sent.map(q!(|r| r.resend as u64)).sim_cluster_output();
        let acked = outputs.acked.map(q!(|a| a.seq)).sim_cluster_output();
        let superseded = outputs.superseded.sim_cluster_output();
        let lease_trace = outputs.lease_trace.sim_cluster_output();
        let served = outputs
            .served
            .map(q!(|j| match j {
                Job::Renewal { resend, .. } => resend as u64,
                Job::Other(_) => 0,
            }))
            .sim_output();
        let backlog = outputs.backlog_trace.sim_output();

        let (driver, handle) = HoldOneHookDriver::new();
        hydro_lang::sim::work_counts::enable();
        let mut out = Outcome::default();
        let out_ref = &mut out;
        let handle_ref = &handle;
        flow.sim().with_cluster_size(&clients, N as usize).run_with_driver(driver, async || {
            let mut first = 0u64;
            let mut total = 0u64;
            for round in 0..ROUNDS {
                apply_hold(handle_ref, target, k, round);
                for m in 0..N {
                    client_clock_send.send(m, ());
                }
                server_clock_send.send(());
                quiesce().await;
                for m in 0..N {
                    while let Some(resend) = sent.try_next(m).await {
                        total += 1;
                        if resend == 0 {
                            first += 1;
                        }
                    }
                    let _ = acked.collect_sorted::<Vec<_>>(m).await;
                    let _ = superseded.collect_sorted::<Vec<_>>(m).await;
                    while lease_trace.try_next(m).await.is_some() {}
                }
                while let Some(resend) = served.try_next().await {
                    total += 1;
                    if resend == 0 {
                        first += 1;
                    }
                }
                while backlog.try_next().await.is_some() {}
            }
            out_ref.required = first;
            out_ref.total = total;
        });
        out.forced = handle.forced_releases();
        out.counts = hydro_lang::sim::work_counts::take();
        out.hooks = handle.hooks_seen();
        out
    }

    #[test]
    fn hold_sweep_lease_resend_after_4() {
        let r = hold_sweep::sweep("lease resend_after=Some(4)", hold_sweep::GRID, &|t, k| run(Some(4), t, k));
        assert_eq!(hold_sweep::verdict(&r).0, "hazardous");
    }

    #[test]
    fn hold_sweep_lease_one_outstanding() {
        let r = hold_sweep::sweep("lease resend_after=None", hold_sweep::GRID, &|t, k| run(None, t, k));
        assert_eq!(hold_sweep::verdict(&r).0, "benign");
    }
}

/// Hold sweep over the `g_set_gossip` load harness (3 members, one update per round). Work is
/// the merges observed through snapshots; the run is short because the simulator's cost grows
/// with the square of the set size.
#[cfg(test)]
mod hold_crdt_gossip {
    use std::collections::BTreeSet;

    use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::hold_one_hook::HoldOneHookDriver;
    use hydro_lang::sim::quiesce;
    use hydro_std::ec_inference_demos::crdt_gossip::g_set_gossip;

    use crate::cluster::witnesses::checker_experiments::hold_sweep::{self, Outcome, apply_hold};

    const N: u32 = 3;
    const ROUNDS: usize = 60;
    pub(super) const GRID: &[usize] = &[0, 2, 5, 10, 20];

    pub(super) fn run(target: Option<&str>, k: usize) -> Outcome {
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<()>();
        let (update_send, updates) = cluster.sim_input::<u32, NoOrder, ExactlyOnce>();
        let (pump_send, pumps) = cluster.sim_input::<(), TotalOrder, ExactlyOnce>();
        let state = g_set_gossip(&cluster, updates, pumps);
        let obs_tick = cluster.tick();
        let observed = state
            .snapshot(&obs_tick, nondet!(/** harness observation */))
            .all_ticks()
            .sim_cluster_output();

        let (driver, handle) = HoldOneHookDriver::new();
        hydro_lang::sim::work_counts::enable();
        let mut out = Outcome::default();
        let out_ref = &mut out;
        let handle_ref = &handle;
        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&cluster, N as usize)
            .run_with_driver(driver, async || {
                let mut work = 0u64;
                for round in 0..ROUNDS {
                    apply_hold(handle_ref, target, k, round);
                    for m in 0..N {
                        pump_send.send(m, ());
                    }
                    update_send.send_many_unordered([((round as u32) % N, round as u32)]);
                    quiesce().await;
                    for m in 0..N {
                        let snapshots: Vec<BTreeSet<u32>> = observed.collect(m).await;
                        work += snapshots.len() as u64;
                    }
                }
                out_ref.required = 0;
                out_ref.total = work;
            });
        out.forced = handle.forced_releases();
        out.counts = hydro_lang::sim::work_counts::take();
        out.hooks = handle.hooks_seen();
        out
    }

    #[test]
    fn hold_sweep_crdt_gossip() {
        let r = hold_sweep::sweep("crdt_gossip", GRID, &|t, k| run(t, k));
        assert_eq!(hold_sweep::verdict(&r).0, "benign");
    }
}

/// Hold sweep over `pure_heartbeat` (3 members). Work is heartbeats received; the requirement
/// is `n` per timer element.
#[cfg(test)]
mod hold_pure_heartbeat {
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::location::MemberId;
    use hydro_lang::prelude::*;
    use hydro_lang::sim::hold_one_hook::HoldOneHookDriver;
    use hydro_lang::sim::quiesce;

    use crate::cluster::pure_heartbeat::{Heartbeat, pure_heartbeat};
    use crate::cluster::witnesses::checker_experiments::hold_sweep::{self, Outcome, apply_hold};

    const N: u32 = 3;
    const ROUNDS: usize = 240;

    pub(super) fn run(target: Option<&str>, k: usize) -> Outcome {
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<()>();
        let (timer_send, timer) = cluster.sim_input::<(), TotalOrder, ExactlyOnce>();
        let received = pure_heartbeat(&cluster, timer).entries().sim_cluster_output();

        let (driver, handle) = HoldOneHookDriver::new();
        hydro_lang::sim::work_counts::enable();
        let mut out = Outcome::default();
        let out_ref = &mut out;
        let handle_ref = &handle;
        flow.sim().with_cluster_size(&cluster, N as usize).run_with_driver(driver, async || {
            let mut pulses = 0u64;
            let mut got = 0u64;
            for round in 0..ROUNDS {
                apply_hold(handle_ref, target, k, round);
                for m in 0..N {
                    timer_send.send(m, ());
                    pulses += 1;
                }
                quiesce().await;
                for m in 0..N {
                    got += received
                        .collect_sorted::<Vec<(MemberId<()>, Heartbeat)>>(m)
                        .await
                        .len() as u64;
                }
            }
            out_ref.required = pulses * N as u64;
            out_ref.total = got;
        });
        out.forced = handle.forced_releases();
        out.counts = hydro_lang::sim::work_counts::take();
        out.hooks = handle.hooks_seen();
        out
    }

    #[test]
    fn hold_sweep_pure_heartbeat() {
        let r = hold_sweep::sweep("pure_heartbeat", hold_sweep::GRID, &|t, k| run(t, k));
        assert_eq!(hold_sweep::verdict(&r).0, "benign");
    }
}

/// Hold sweep over `productive_tc` (a 4-edge chain every 5 rounds). Work is join candidates
/// generated; the requirement is zero.
#[cfg(test)]
mod hold_transitive_closure {
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::hold_one_hook::HoldOneHookDriver;
    use hydro_lang::sim::quiesce;

    use crate::cluster::witnesses::checker_experiments::hold_sweep::{self, Outcome, apply_hold};
    use crate::local::productive_tc::{TcTrace, productive_transitive_closure};

    const ROUNDS: usize = 240;

    pub(super) fn run(target: Option<&str>, k: usize) -> Outcome {
        let mut flow = FlowBuilder::new();
        let process = flow.process::<()>();
        let (graph_send, graphs) = process.sim_input::<Vec<(u32, u32)>, TotalOrder, ExactlyOnce>();
        let (step_send, steps) = process.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (facts, traces) = productive_transitive_closure(graphs, steps);
        let facts = facts
            .assume_ordering::<TotalOrder>(nondet!(/** the harness only counts facts */))
            .sim_output();
        let traces = traces.sim_output();

        let (driver, handle) = HoldOneHookDriver::new();
        hydro_lang::sim::work_counts::enable();
        let mut out = Outcome::default();
        let out_ref = &mut out;
        let handle_ref = &handle;
        flow.sim().run_with_driver(driver, async || {
            let mut work = 0u64;
            let mut graphs_admitted = 0u32;
            for round in 0..ROUNDS {
                apply_hold(handle_ref, target, k, round);
                if round % 5 == 0 {
                    let base = graphs_admitted * 1000;
                    graph_send.send((0..4).map(|i| (base + i, base + i + 1)).collect());
                    graphs_admitted += 1;
                }
                step_send.send(());
                quiesce().await;
                while let Some(t) = traces.try_next().await {
                    let t: TcTrace = t;
                    work += t.candidates_generated as u64;
                }
                while facts.try_next().await.is_some() {}
            }
            out_ref.required = 0;
            out_ref.total = work;
        });
        out.forced = handle.forced_releases();
        out.counts = hydro_lang::sim::work_counts::take();
        out.hooks = handle.hooks_seen();
        out
    }

    #[test]
    fn hold_sweep_transitive_closure() {
        let r = hold_sweep::sweep("transitive_closure", hold_sweep::GRID, &|t, k| run(t, k));
        assert_eq!(hold_sweep::verdict(&r).0, "benign");
    }
}

/// The same hold sweeps read through the simulator's own counts (`hydro_lang::sim::work_counts`)
/// instead of the witnesses' output counters. See the module docs, "Third experiment".
///
/// Each test runs one configuration exactly as the `hold_*` module above does (same input, same
/// grid, same hold plan), takes the verdict under the checker's rule (any member's outgoing
/// messages or the program's total admitted records rise), prints the comparison with the
/// hand-counted verdict and with a plain total, and asserts the verdict the module docs record.
#[cfg(test)]
mod generic_measure {
    use super::hold_sweep::{self, Measure, SweepResult};

    /// Prints one comparison row and returns the generic verdict (per-place rule) and the held
    /// hook that produced it.
    fn compare(r: &SweepResult) -> (&'static str, Option<String>) {
        let (hand_v, hand_best) = hold_sweep::verdict(r);
        let (total_v, total_best) = hold_sweep::generic_verdict(r, Measure::Admitted);
        let (place_v, place_best) = hold_sweep::checker_verdict(r);
        let series = |c: &hold_sweep::HookCurve| {
            hold_sweep::measure_series(c, Measure::Admitted)
                .iter()
                .map(|(_, v)| *v)
                .collect::<Vec<_>>()
        };
        println!(
            "[{}] COMPARE hand: {hand_v} at {} | admitted total: {total_v} at {} curve {:?} | checker rule: {place_v} at {} | agree {}",
            r.label,
            hand_best.map(|c| c.hook.clone()).unwrap_or_else(|| "none".into()),
            total_best.map(|c| c.hook.clone()).unwrap_or_else(|| "none".into()),
            total_best.map(series).unwrap_or_default(),
            place_best
                .as_ref()
                .map(|(c, what, g)| format!("{} ({what} +{g})", c.hook))
                .unwrap_or_else(|| "none".into()),
            hand_v == place_v,
        );
        (place_v, place_best.map(|(c, _, _)| c.hook.clone()))
    }

    #[test]
    fn generic_rpc_retry_three_attempts() {
        let r = hold_sweep::sweep("generic rpc_retry max_attempts=3", hold_sweep::GRID, &|t, k| {
            super::hold_rpc_retry::run(3, t, k)
        });
        let (v, hook) = compare(&r);
        assert_eq!(v, "hazardous");
        assert!(hook.unwrap().contains("rpc_retry.rs"));
    }

    #[test]
    fn generic_rpc_retry_one_attempt() {
        let r = hold_sweep::sweep("generic rpc_retry max_attempts=1", hold_sweep::GRID, &|t, k| {
            super::hold_rpc_retry::run(1, t, k)
        });
        assert_eq!(compare(&r).0, "benign");
    }

    #[test]
    fn generic_backoff_retry_backoff_on() {
        let r = hold_sweep::sweep("generic backoff_retry backoff=true", hold_sweep::GRID, &|t, k| {
            super::hold_backoff_retry::run(true, t, k)
        });
        assert_eq!(compare(&r).0, "hazardous");
    }

    #[test]
    fn generic_backoff_retry_backoff_off() {
        let r = hold_sweep::sweep("generic backoff_retry backoff=false", hold_sweep::GRID, &|t, k| {
            super::hold_backoff_retry::run(false, t, k)
        });
        assert_eq!(compare(&r).0, "hazardous");
    }

    #[test]
    fn generic_bounded_queue_some_100() {
        let r = hold_sweep::sweep("generic bounded_queue max_backlog=Some(100)", hold_sweep::GRID, &|t, k| {
            super::hold_bounded_queue::run(Some(100), t, k)
        });
        assert_eq!(compare(&r).0, "hazardous");
    }

    #[test]
    fn generic_bounded_queue_none() {
        let r = hold_sweep::sweep("generic bounded_queue max_backlog=None", hold_sweep::GRID, &|t, k| {
            super::hold_bounded_queue::run(None, t, k)
        });
        assert_eq!(compare(&r).0, "hazardous");
    }

    #[test]
    fn generic_cache_request_dated_no_coalesce() {
        let r = hold_sweep::sweep("generic cache request_dated, no coalesce", hold_sweep::GRID, &|t, k| {
            super::hold_cache::run(false, true, t, k)
        });
        assert_eq!(compare(&r).0, "hazardous");
    }

    #[test]
    fn generic_cache_fill_dated_no_coalesce() {
        let r = hold_sweep::sweep("generic cache fill_dated, no coalesce", hold_sweep::GRID, &|t, k| {
            super::hold_cache::run(false, false, t, k)
        });
        assert_eq!(compare(&r).0, "hazardous");
    }

    #[test]
    fn generic_cache_coalesce() {
        let r = hold_sweep::sweep("generic cache coalesce", hold_sweep::GRID, &|t, k| {
            super::hold_cache::run(true, true, t, k)
        });
        assert_eq!(compare(&r).0, "benign");
    }

    #[test]
    fn generic_gossip_resend_on() {
        let r = hold_sweep::sweep("generic gossip_resend ack_timeout=3", hold_sweep::GRID, &|t, k| {
            super::hold_gossip_resend::run(3, t, k)
        });
        assert_eq!(compare(&r).0, "hazardous");
    }

    #[test]
    fn generic_gossip_resend_off() {
        let r = hold_sweep::sweep("generic gossip_resend ack_timeout=0", hold_sweep::GRID, &|t, k| {
            super::hold_gossip_resend::run(0, t, k)
        });
        assert_eq!(compare(&r).0, "benign");
    }

    #[test]
    fn generic_election_uniform_timeouts() {
        let r = hold_sweep::sweep("generic election spread=0", hold_sweep::GRID, &|t, k| {
            super::hold_election::run(0, t, k)
        });
        assert_eq!(compare(&r).0, "hazardous");
    }

    #[test]
    fn generic_election_spread_3() {
        let r = hold_sweep::sweep("generic election spread=3", hold_sweep::GRID, &|t, k| {
            super::hold_election::run(3, t, k)
        });
        assert_eq!(compare(&r).0, "hazardous");
    }

    #[test]
    fn generic_rebalancing_no_cooldown() {
        let r = hold_sweep::sweep("generic rebalancing cooldown=0", hold_sweep::GRID, &|t, k| {
            super::hold_rebalancing::run(0, t, k)
        });
        assert_eq!(compare(&r).0, "hazardous");
    }

    #[test]
    fn generic_rebalancing_cooldown_8() {
        let r = hold_sweep::sweep("generic rebalancing cooldown=8", hold_sweep::GRID, &|t, k| {
            super::hold_rebalancing::run(8, t, k)
        });
        assert_eq!(compare(&r).0, "hazardous");
    }

    /// Predicted before measuring: benign under admitted records, disagreeing with the
    /// hand-counted verdict. The store's extra work is a cost field in a payload and a `u64`
    /// segment length, not records; the only records it moves are the operations and clock
    /// elements themselves, whose totals a hold cannot raise.
    #[test]
    fn generic_compaction_reserve_0() {
        let r = hold_sweep::sweep("generic compaction reserve=0", hold_sweep::GRID, &|t, k| {
            super::hold_compaction::run(0, t, k)
        });
        assert_eq!(compare(&r).0, "benign");
    }

    #[test]
    fn generic_compaction_reserve_8() {
        let r = hold_sweep::sweep("generic compaction reserve=8", hold_sweep::GRID, &|t, k| {
            super::hold_compaction::run(8, t, k)
        });
        assert_eq!(compare(&r).0, "benign");
    }

    #[test]
    fn generic_lease_resend_after_4() {
        let r = hold_sweep::sweep("generic lease resend_after=Some(4)", hold_sweep::GRID, &|t, k| {
            super::hold_lease::run(Some(4), t, k)
        });
        assert_eq!(compare(&r).0, "hazardous");
    }

    #[test]
    fn generic_lease_one_outstanding() {
        let r = hold_sweep::sweep("generic lease resend_after=None", hold_sweep::GRID, &|t, k| {
            super::hold_lease::run(None, t, k)
        });
        assert_eq!(compare(&r).0, "benign");
    }

    #[test]
    fn generic_crdt_gossip() {
        let r = hold_sweep::sweep("generic crdt_gossip", super::hold_crdt_gossip::GRID, &|t, k| {
            super::hold_crdt_gossip::run(t, k)
        });
        assert_eq!(compare(&r).0, "benign");
    }

    #[test]
    fn generic_pure_heartbeat() {
        let r = hold_sweep::sweep("generic pure_heartbeat", hold_sweep::GRID, &|t, k| {
            super::hold_pure_heartbeat::run(t, k)
        });
        assert_eq!(compare(&r).0, "benign");
    }

    #[test]
    fn generic_transitive_closure() {
        let r = hold_sweep::sweep("generic transitive_closure", hold_sweep::GRID, &|t, k| {
            super::hold_transitive_closure::run(t, k)
        });
        assert_eq!(compare(&r).0, "benign");
    }
}

/// The packaged checker, [`hydro_lang::sim::amplification::check`], run on every corpus
/// configuration with the same wiring, workload, rounds and grid as [`generic_measure`], and
/// asserted to return the verdict and held hook that module recorded. See the module docs,
/// "Fourth experiment".
#[cfg(test)]
mod library_check {
    use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::amplification::{CheckConfig, Report, Verdict, check};

    // Types named inside `q!` closures must be imported at module level; the staged crate does
    // not see function-local `use` statements.
    use crate::cluster::witnesses::election_stampede::Msg;
    use crate::cluster::witnesses::lease_renewal_storm::Job;

    const ROUNDS: usize = 240;

    fn config() -> CheckConfig {
        CheckConfig::new(ROUNDS)
    }

    fn expect_hazardous(label: &str, r: &Report, hook_contains: &[&str]) {
        println!("[library {label}]\n{r}");
        assert_eq!(r.verdict, Verdict::Hazardous, "{label}");
        let loc = r.location.as_ref().expect(label);
        for s in hook_contains {
            assert!(
                loc.hook.contains(s),
                "{label}: expected held hook to contain {s:?}, got {}",
                loc.hook
            );
        }
    }

    fn expect_benign(label: &str, r: &Report) {
        println!("[library {label}]\n{r}");
        assert_eq!(r.verdict, Verdict::Benign, "{label}");
        assert!(r.location.is_none(), "{label}");
    }

    // rpc_retry: timeout 40, capacity 5, 2 requests per round.

    fn rpc_retry(max_attempts: u32) -> Report {
        use crate::cluster::rpc_retry::{Client, RetryPolicy, Server, ServerConfig, rpc_with_retries};
        let mut flow = FlowBuilder::new();
        let client = flow.process::<Client>();
        let server = flow.process::<Server>();
        let (request_send, requests) = client.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let (clock_send, client_clock) = client.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (_crt, client_report_tick) = client.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (_srt, server_report_tick) = server.sim_input::<(), TotalOrder, ExactlyOnce>();
        let outputs = rpc_with_retries(
            &client,
            &server,
            requests,
            client_clock,
            client_report_tick,
            server_report_tick,
            RetryPolicy {
                timeout_ticks: 40,
                max_attempts,
            },
            ServerConfig {
                max_per_tick: 5,
                service_time: std::time::Duration::ZERO,
            },
        );
        let _completed = outputs.completed.map(q!(|c| c.id)).sim_output();
        let _outgoing = outputs.outgoing.map(q!(|r| r.id)).sim_output();
        let _processed = outputs.processed.map(q!(|r| r.id)).sim_output();
        check(flow.sim(), &config(), async |round| {
            clock_send.send(());
            for _ in 0..2 {
                request_send.send(round as u64);
            }
        })
    }

    /// Expected before running: hazardous, held hook the server's `Request` arrivals in
    /// `rpc_retry.rs`, gain in admitted records (1470 under `generic_measure`).
    #[test]
    fn library_rpc_retry_three_attempts() {
        let r = rpc_retry(3);
        expect_hazardous("rpc_retry max_attempts=3", &r, &["rpc_retry.rs", "Request"]);
        assert_eq!(r.location.as_ref().unwrap().rose, "admitted records");
    }

    /// Expected: benign, every curve flat.
    #[test]
    fn library_rpc_retry_one_attempt() {
        expect_benign("rpc_retry max_attempts=1", &rpc_retry(1));
    }

    // backoff_retry: base timeout 40, 3 attempts, capacity 5, 2 requests per round.

    fn backoff_retry(backoff: bool) -> Report {
        use crate::cluster::witnesses::backoff_retry::{
            Client, RetryPolicy, Server, ServerConfig, rpc_with_backoff,
        };
        let mut flow = FlowBuilder::new();
        let client = flow.process::<Client>();
        let server = flow.process::<Server>();
        let (request_send, requests) = client.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let (clock_send, client_clock) = client.sim_input::<(), TotalOrder, ExactlyOnce>();
        let outputs = rpc_with_backoff(
            &client,
            &server,
            requests,
            client_clock,
            RetryPolicy {
                base_timeout_ticks: 40,
                max_attempts: 3,
                backoff,
            },
            ServerConfig { max_per_tick: 5 },
        );
        let _completed = outputs.completed.map(q!(|c| c.id)).sim_output();
        let _outgoing = outputs.outgoing.map(q!(|r| r.id)).sim_output();
        let _processed = outputs.processed.map(q!(|r| r.id)).sim_output();
        check(flow.sim(), &config(), async |round| {
            clock_send.send(());
            for _ in 0..2 {
                request_send.send(round as u64);
            }
        })
    }

    /// Expected: hazardous at the server's `Request` arrivals (admitted +448 recorded).
    #[test]
    fn library_backoff_retry_backoff_on() {
        expect_hazardous("backoff_retry backoff=true", &backoff_retry(true), &["backoff_retry.rs", "Request"]);
    }

    /// Expected: hazardous at the server's `Request` arrivals (admitted +588 recorded).
    #[test]
    fn library_backoff_retry_backoff_off() {
        expect_hazardous("backoff_retry backoff=false", &backoff_retry(false), &["backoff_retry.rs", "Request"]);
    }

    // bounded_queue_rejection: timeout 40, 3 attempts, reject backoff 10, capacity 5.

    fn bounded_queue(max_backlog: Option<usize>) -> Report {
        use crate::cluster::witnesses::bounded_queue_rejection::{
            Client, RetryPolicy, Server, ServerConfig, rpc_with_bounded_queue,
        };
        let mut flow = FlowBuilder::new();
        let client = flow.process::<Client>();
        let server = flow.process::<Server>();
        let (request_send, requests) = client.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let (clock_send, client_clock) = client.sim_input::<(), TotalOrder, ExactlyOnce>();
        let outputs = rpc_with_bounded_queue(
            &client,
            &server,
            requests,
            client_clock,
            RetryPolicy {
                timeout_ticks: 40,
                max_attempts: 3,
                reject_backoff_ticks: 10,
            },
            ServerConfig {
                max_per_tick: 5,
                max_backlog,
            },
        );
        let _completed = outputs.completed.map(q!(|c| c.id)).sim_output();
        let _outgoing = outputs.outgoing.map(q!(|r| r.id)).sim_output();
        let _processed = outputs.processed.map(q!(|r| r.id)).sim_output();
        let _rejected = outputs.rejected.map(q!(|r| r.id)).sim_output();
        check(flow.sim(), &config(), async |round| {
            clock_send.send(());
            for _ in 0..2 {
                request_send.send(round as u64);
            }
        })
    }

    /// Expected: hazardous at the server's `Request` arrivals (admitted +634 recorded).
    #[test]
    fn library_bounded_queue_some_100() {
        expect_hazardous(
            "bounded_queue max_backlog=Some(100)",
            &bounded_queue(Some(100)),
            &["bounded_queue_rejection.rs", "Request"],
        );
    }

    /// Expected: hazardous at the server's `Request` arrivals (admitted +588 recorded).
    #[test]
    fn library_bounded_queue_none() {
        expect_hazardous(
            "bounded_queue max_backlog=None",
            &bounded_queue(None),
            &["bounded_queue_rejection.rs", "Request"],
        );
    }

    // cache_thundering_herd: TTL 20, origin capacity 4, 10 hot keys at 8 lookups per round.

    fn cache(coalesce: bool, request_dated: bool) -> Report {
        use crate::cluster::witnesses::cache_thundering_herd::{
            Cache, CacheConfig, Key, Origin, OriginConfig, cache_with_expiry,
        };
        const HOT_KEYS: u64 = 10;
        const HOT_PER_ROUND: u64 = 8;
        let mut flow = FlowBuilder::new();
        let cache = flow.process::<Cache>();
        let origin = flow.process::<Origin>();
        let (lookup_send, lookups) = cache.sim_input::<Key, TotalOrder, ExactlyOnce>();
        let (cache_clock_send, cache_clock) = cache.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (origin_clock_send, origin_clock) = origin.sim_input::<(), TotalOrder, ExactlyOnce>();
        let outputs = cache_with_expiry(
            &cache,
            &origin,
            lookups,
            cache_clock,
            origin_clock,
            CacheConfig {
                ttl_ticks: 20,
                coalesce,
                request_dated,
            },
            OriginConfig { max_fetch_per_tick: 4 },
        );
        let _completed = outputs.completed.map(q!(|c| c.id)).sim_output();
        let _fetches = outputs.fetches.map(q!(|f| f.lookup_id)).sim_output();
        let _processed = outputs.processed.map(q!(|f| f.lookup_id)).sim_output();
        let _applied = outputs.fills_applied.map(q!(|f| f.lookup_id)).sim_output();
        let _redundant = outputs.fills_redundant.map(q!(|f| f.lookup_id)).sim_output();
        let _stale = outputs.fills_stale.map(q!(|f| f.lookup_id)).sim_output();
        check(flow.sim(), &config(), async |round| {
            let round = round as u64;
            cache_clock_send.send(());
            origin_clock_send.send(());
            for i in 0..HOT_PER_ROUND {
                lookup_send.send(((round * HOT_PER_ROUND + i) % HOT_KEYS) as Key);
            }
        })
    }

    /// Expected: hazardous at the cache's `Fill` hook (admitted +2332 recorded).
    #[test]
    fn library_cache_request_dated_no_coalesce() {
        expect_hazardous("cache request_dated, no coalesce", &cache(false, true), &["Fill"]);
    }

    /// Expected: hazardous at the origin's clock hook (admitted +1500 recorded).
    #[test]
    fn library_cache_fill_dated_no_coalesce() {
        expect_hazardous(
            "cache fill_dated, no coalesce",
            &cache(false, false),
            &["cache_thundering_herd.rs", "()"],
        );
    }

    /// Expected: benign.
    #[test]
    fn library_cache_coalesce() {
        expect_benign("cache coalesce", &cache(true, true));
    }

    // gossip_resend: 5 members, 5 merges per tick, one update per member per round.

    fn gossip_resend(ack_timeout_ticks: u64) -> Report {
        use crate::cluster::witnesses::gossip_resend::{GossipConfig, Node, gossip_with_resend};
        const MEMBERS: u32 = 5;
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<Node>();
        let (timer_send, timer) = cluster.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (update_send, updates) = cluster.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let outputs = gossip_with_resend(
            &cluster,
            timer,
            updates,
            GossipConfig {
                max_merges_per_tick: 5,
                ack_timeout_ticks,
            },
        );
        let _wire = outputs.wire.map(q!(|(to, d)| (to, d.from, d.seq))).sim_cluster_output();
        let _merged = outputs.merged.map(q!(|d| (d.from, d.seq))).sim_cluster_output();
        let _acks = outputs.acks_sent.sim_cluster_output();
        let _inbox = outputs.inbox_depth.sim_cluster_output();
        let _outstanding = outputs.outstanding_depth.sim_cluster_output();
        let mut next_value = 0u64;
        check(
            flow.sim().with_cluster_size(&cluster, MEMBERS as usize),
            &config(),
            async |round| {
                if round == 0 {
                    next_value = 0;
                }
                for m in 0..MEMBERS {
                    timer_send.send(m, ());
                    update_send.send(m, next_value);
                    next_value += 1;
                }
            },
        )
    }

    /// Expected: hazardous at the `Ack` hook (admitted +5435 recorded).
    #[test]
    fn library_gossip_resend_on() {
        expect_hazardous("gossip_resend ack_timeout=3", &gossip_resend(3), &["Ack"]);
    }

    /// Expected: benign.
    #[test]
    fn library_gossip_resend_off() {
        expect_benign("gossip_resend ack_timeout=0", &gossip_resend(0));
    }

    // election_stampede: 5 members, timeout 6, budget 3, one client request per member per round.

    fn election(timeout_spread_ticks: u64) -> Report {
        use crate::cluster::witnesses::election_stampede::{ElectionConfig, Node, election};
        const MEMBERS: u32 = 5;
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<Node>();
        let (timer_send, timer) = cluster.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (client_send, clients) = cluster.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let outputs = election(
            &cluster,
            timer,
            clients,
            ElectionConfig {
                election_timeout_ticks: 6,
                timeout_spread_ticks,
                budget_per_tick: 3,
            },
        );
        let _vote_requests = outputs
            .wire
            .filter_map(q!(|(_, msg)| match msg {
                Msg::VoteRequest { .. } => Some(()),
                _ => None,
            }))
            .sim_cluster_output();
        let _processed = outputs.processed.sim_cluster_output();
        let _inbox = outputs.inbox_depth.sim_cluster_output();
        let _state = outputs.state_trace.sim_cluster_output();
        let _elections = outputs.elections.sim_cluster_output();
        let mut next_client = 0u64;
        check(
            flow.sim().with_cluster_size(&cluster, MEMBERS as usize),
            &config(),
            async |round| {
                if round == 0 {
                    next_client = 0;
                }
                for m in 0..MEMBERS {
                    timer_send.send(m, ());
                    client_send.send(m, next_client);
                    next_client += 1;
                }
            },
        )
    }

    /// Expected: hazardous, gain in one member's sends (member 1 +840 recorded), held hook the
    /// timer hook in `election_stampede.rs`.
    #[test]
    fn library_election_uniform_timeouts() {
        let r = election(0);
        expect_hazardous("election spread=0", &r, &["election_stampede.rs"]);
        assert!(r.location.as_ref().unwrap().rose.starts_with("sends from"));
    }

    /// Expected: hazardous, gain in one member's sends (member 1 +824 recorded), held hook the
    /// `Msg` inbox.
    #[test]
    fn library_election_spread_3() {
        let r = election(3);
        expect_hazardous("election spread=3", &r, &["election_stampede.rs", "Msg"]);
        assert!(r.location.as_ref().unwrap().rose.starts_with("sends from"));
    }

    // rebalancing_ping_pong: 2 workers, 5 units per tick, threshold 10, reports every 4 rounds,
    // 3 tasks per worker per round.

    fn rebalancing(cooldown_ticks: u64) -> Report {
        use crate::cluster::witnesses::rebalancing_ping_pong::{
            RebalanceConfig, Worker, rebalancing_workers,
        };
        const N: u32 = 2;
        let mut flow = FlowBuilder::new();
        let workers = flow.cluster::<Worker>();
        let (task_send, tasks) = workers.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let (clock_send, clock) = workers.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (report_send, report_tick) = workers.sim_input::<(), TotalOrder, ExactlyOnce>();
        let outputs = rebalancing_workers(
            &workers,
            tasks,
            clock,
            report_tick,
            RebalanceConfig {
                work_per_tick: 5,
                threshold: 10,
                cooldown_ticks,
            },
        );
        let _completed = outputs.completed.map(q!(|t| t.id)).sim_cluster_output();
        let _migrated = outputs.migrated.map(q!(|(_, t)| t.id)).sim_cluster_output();
        let _ticks = outputs.ticks.map(q!(|t| t.queue_len)).sim_cluster_output();
        let mut next_id = 0u64;
        check(
            flow.sim().with_cluster_size(&workers, N as usize),
            &config(),
            async |round| {
                if round == 0 {
                    next_id = 0;
                }
                for w in 0..N {
                    clock_send.send(w, ());
                    if round % 4 == 0 {
                        report_send.send(w, ());
                    }
                    for _ in 0..3 {
                        task_send.send(w, next_id);
                        next_id += 1;
                    }
                }
            },
        )
    }

    /// Expected: hazardous at the `u64` task arrivals (admitted +300 recorded).
    #[test]
    fn library_rebalancing_no_cooldown() {
        expect_hazardous("rebalancing cooldown=0", &rebalancing(0), &["rebalancing_ping_pong.rs", "u64"]);
    }

    /// Expected: hazardous at the `u64` task arrivals (admitted +300 recorded).
    #[test]
    fn library_rebalancing_cooldown_8() {
        expect_hazardous("rebalancing cooldown=8", &rebalancing(8), &["rebalancing_ping_pong.rs", "u64"]);
    }

    // compaction_falls_behind: budget 30, 1 put and 4 gets per round. Excluded from the
    // scorecard (its work is a modeled counter); the recorded generic verdict is benign for both.

    fn compaction(compaction_reserve: u64) -> Report {
        use crate::cluster::witnesses::compaction_falls_behind::{
            Op, Store, StoreConfig, log_with_compaction,
        };
        let mut flow = FlowBuilder::new();
        let store = flow.process::<Store>();
        let (op_send, ops) = store.sim_input::<Op, TotalOrder, ExactlyOnce>();
        let (clock_send, clock) = store.sim_input::<(), TotalOrder, ExactlyOnce>();
        let outputs = log_with_compaction(
            &store,
            ops,
            clock,
            StoreConfig {
                budget_per_tick: 30,
                compaction_reserve,
            },
        );
        let _served = outputs.served.map(q!(|s| s.id)).sim_output();
        let _ticks = outputs.ticks.map(q!(|t| t.serve_units)).sim_output();
        check(flow.sim(), &config(), async |_round| {
            clock_send.send(());
            op_send.send(Op::Put);
            for _ in 0..4 {
                op_send.send(Op::Get);
            }
        })
    }

    /// Expected: benign under record counts, as `generic_measure` recorded.
    #[test]
    fn library_compaction_reserve_0() {
        expect_benign("compaction reserve=0", &compaction(0));
    }

    /// Expected: benign.
    #[test]
    fn library_compaction_reserve_8() {
        expect_benign("compaction reserve=8", &compaction(8));
    }

    // lease_renewal_storm: 20 clients renewing every 10 ticks, server capacity 5.

    fn lease(resend_after_ticks: Option<u64>) -> Report {
        use crate::cluster::witnesses::lease_renewal_storm::{
            ClientPolicy, LeaseClient, LeaseServer, ServerConfig, lease_renewal,
        };
        const N: u32 = 20;
        let mut flow = FlowBuilder::new();
        let clients = flow.cluster::<LeaseClient>();
        let server = flow.process::<LeaseServer>();
        let (client_clock_send, client_clock) = clients.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (server_clock_send, server_clock) = server.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (_other_send, other_work) = server.sim_input::<u64, TotalOrder, ExactlyOnce>();
        let outputs = lease_renewal(
            &clients,
            &server,
            client_clock,
            server_clock,
            other_work,
            ClientPolicy {
                renew_every_ticks: 10,
                lease_ticks: 30,
                resend_after_ticks,
            },
            ServerConfig { max_per_tick: 5 },
        );
        let _sent = outputs.sent.map(q!(|r| r.resend as u64)).sim_cluster_output();
        let _acked = outputs.acked.map(q!(|a| a.seq)).sim_cluster_output();
        let _superseded = outputs.superseded.sim_cluster_output();
        let _lease_trace = outputs.lease_trace.sim_cluster_output();
        let _served = outputs
            .served
            .map(q!(|j| match j {
                Job::Renewal { resend, .. } => resend as u64,
                Job::Other(_) => 0,
            }))
            .sim_output();
        let _backlog = outputs.backlog_trace.sim_output();
        check(
            flow.sim().with_cluster_size(&clients, N as usize),
            &config(),
            async |_round| {
                for m in 0..N {
                    client_clock_send.send(m, ());
                }
                server_clock_send.send(());
            },
        )
    }

    /// Expected: hazardous at the client's `Ack` hook (admitted +3221 recorded).
    #[test]
    fn library_lease_resend_after_4() {
        expect_hazardous("lease resend_after=Some(4)", &lease(Some(4)), &["Ack"]);
    }

    /// Expected: benign.
    #[test]
    fn library_lease_one_outstanding() {
        expect_benign("lease resend_after=None", &lease(None));
    }

    // crdt_gossip_load: 3 members, one update per round, pump every member every round; 60 rounds
    // and a short grid because the simulator's cost is quadratic in the set size.

    /// Expected: benign (merges fall under every hold).
    #[test]
    fn library_crdt_gossip() {
        use hydro_std::ec_inference_demos::crdt_gossip::g_set_gossip;
        const N: u32 = 3;
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<()>();
        let (update_send, updates) = cluster.sim_input::<u32, NoOrder, ExactlyOnce>();
        let (pump_send, pumps) = cluster.sim_input::<(), TotalOrder, ExactlyOnce>();
        let state = g_set_gossip(&cluster, updates, pumps);
        let obs_tick = cluster.tick();
        let _observed = state
            .snapshot(&obs_tick, nondet!(/** harness observation */))
            .all_ticks()
            .sim_cluster_output();
        let cfg = CheckConfig::new(60).with_grid(&[0, 2, 5, 10, 20]);
        let r = check(
            flow.sim()
                .skip_consistency_assertions()
                .with_cluster_size(&cluster, N as usize),
            &cfg,
            async |round| {
                for m in 0..N {
                    pump_send.send(m, ());
                }
                update_send.send_many_unordered([((round as u32) % N, round as u32)]);
            },
        );
        expect_benign("crdt_gossip", &r);
    }

    // pure_heartbeat_load: 3 members, one timer element per member per round.

    /// Expected: benign, and no hook to hold at all.
    #[test]
    fn library_pure_heartbeat() {
        use crate::cluster::pure_heartbeat::pure_heartbeat;
        const N: u32 = 3;
        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<()>();
        let (timer_send, timer) = cluster.sim_input::<(), TotalOrder, ExactlyOnce>();
        let _received = pure_heartbeat(&cluster, timer).entries().sim_cluster_output();
        let r = check(
            flow.sim().with_cluster_size(&cluster, N as usize),
            &config(),
            async |_round| {
                for m in 0..N {
                    timer_send.send(m, ());
                }
            },
        );
        expect_benign("pure_heartbeat", &r);
        assert!(r.curves.is_empty(), "pure_heartbeat has no batch hook");
    }

    // transitive_closure_load: a 4-edge chain every 5 rounds, one step per round.

    /// Expected: benign.
    #[test]
    fn library_transitive_closure() {
        use crate::local::productive_tc::productive_transitive_closure;
        let mut flow = FlowBuilder::new();
        let process = flow.process::<()>();
        let (graph_send, graphs) = process.sim_input::<Vec<(u32, u32)>, TotalOrder, ExactlyOnce>();
        let (step_send, steps) = process.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (facts, traces) = productive_transitive_closure(graphs, steps);
        let _facts = facts
            .assume_ordering::<TotalOrder>(nondet!(/** the harness only counts facts */))
            .sim_output();
        let _traces = traces.sim_output();
        let mut graphs_admitted = 0u32;
        let r = check(flow.sim(), &config(), async |round| {
            if round == 0 {
                graphs_admitted = 0;
            }
            if round % 5 == 0 {
                let base = graphs_admitted * 1000;
                graph_send.send((0..4).map(|i| (base + i, base + i + 1)).collect());
                graphs_admitted += 1;
            }
            step_send.send(());
        });
        expect_benign("transitive_closure", &r);
    }
}
