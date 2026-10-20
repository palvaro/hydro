Hydrofrac went through five rounds, and most of what the first three built is gone. Some doors below were ones LDFI's authors had already looked behind (clock-indexed crash points, De Morgan why-not provenance); others exist only because Hydro is a Rust dataflow with a simulator rather than a Datalog with a fixed schedule (the hook as fault point, the step closure as a lineage wall, the dylib boundary). Nearly all are dead ends; nearly every one fixed a limit worth knowing. Sources: `history-hydrofrac-development.md` (H §n), `history-hydrofrac-negation.md` (N §n), `ldfi-on-sim-hooks.md` (S, by section title), and commits on `ldfi-hydrofrac`.

### Consumer-side hooks as the fault point
- **Tried:** Round 1 (squashed into `cd6cae7fc6`; S "Prototype status"). Standing `OmissionPolicy` / `PassthroughPolicy` on the receiving `BatchHook`, channels registered by the harness. Hypothesis: the scripted-hook layer already interposes on every tick input and crosses the dylib boundary, so faults there are free. The first cut, scripting `release_values` + `pause()` from the harness, died earlier still: a schedule, not a policy, so the harness had to predict arrival order; and hold is not drop (S "Why that is the wrong fault point").
- **Closed because:** a test seam, not a model of the network (H §11). A `tee`d channel omitted at one consumer was delivered at the others; hook-free paths (`recv.map(f).send(..)`, `sim_output`) could not be faulted; crashes needed a separate mechanism in the send half; every faultable batch was a hook parameter in the protocol's signature; and every certificate rested on the author having registered every channel. Replaced by the sender-side filter in `ab3ad7fe82`.
- **What it bought us:** the filter on every `Network` send with the transport type as the fault model and every channel a fault point by construction (H §5); the distinction between interposition and schedule control, of which LDFI needs only the second. Hooks survive as optional lineage precision and as what Level 3 replay feeds.
- **Reopen only if:** never — the sender is where Molly defines omission and crash.

### Crash points named by send count
- **Tried:** Round 1: `crash(node after k sends)` (S "Two soundness problems", 1; H §5.3).
- **Closed because:** under another hypothesis the node's send sequence shifts, `send #7` names a different message, the clause's literals mean something else in the bug's world, and 2PC at load 2 was certified with the bug present.
- **What it bought us:** `crash(node after sending m)`, `m` identified by `(channel, sender, destination, bytes)`; a node whose `m` is never sent survives; clauses carry the run's send sequences so the kill relation is evaluated in the run that produced them (H §7).
- **Reopen only if:** never — Molly's logical clock is the stable identity, and the message is the closest thing the simulator has to one.

### Message identity by occurrence counting
- **Tried:** S "Message identity and time": `(channel, from, to, payload, k)`, retransmissions distinct faults.
- **Closed because:** ordinals are unstable across hypotheses for the same reason send counts are. The built identity is the bytes; a byte-identical retransmission is omitted too while the fault stands (H §5.2), and a different payload (Raft's heartbeat with a higher `leader_commit`) is a different fault.
- **What it bought us:** stable omission identity with no clock. [NEEDS AUTHOR REASONING: was "an identical retransmission is the same fault" accepted deliberately? A protocol retrying with identical bytes can only be tested for "all copies lost", never "first lost, retry delivered".]
- **Reopen only if:** a target retransmits byte-identical payloads and the bug needs one copy lost and another delivered.

### Crashes admissible at any send
- **Tried:** Round 1 proposed crash points for every send of the run; `EFF` was implemented by swapping every hook's policy to `deliver_all()` (H §5.4, §11.1).
- **Closed because:** the swap became a switch on the filter; post-`EFF` crashes "violated" durability on *correct* Raft — the leader died in the heal phase after the last timer had fired, and a leaderless cluster was blamed for an entry it had no timer left to replicate. A crash inside the convergence window needs a second window the body cannot supply without knowing where the crash fell.
- **What it bought us:** one convergence window; crash points proposed only for pre-`EFF` sends, and every post-`EFF` dependency killed by the crashes that kill its sender's last pre-`EFF` send. (Sibling work, below, found the simulator still *fired* post-`EFF` points and closed that too.)
- **Reopen only if:** the body can express a recovery phase triggered by the crash itself.

### Round-1 lineage shortcuts: "everything delivered", byte-only matching
- **Tried:** Level 0 as "the whole delivered set is the derivation" (S "Level 0"); hook releases matched to sends by bytes alone.
- **Closed because:** Level 0 put reliable delivery's acknowledgements into every support although nothing read them; byte matching attributed 2PC's `commit` to the `prepare` with the same payload, a false certificate (H §6.1).
- **What it bought us:** `Level::Execution`, still the fallback at every level; channel-scoped matching for hooked releases; delivery ids minted at the send and carried on the wire, so receive-side attribution is exact (H §11.4).
- **Reopen only if:** never.

### The static slice as the precision instrument
- **Tried:** Round 1, `sim/slice.rs`: a per-sink backward slice of the IR to narrow "every output depends on every input of every earlier execution". Two completeness bugs each manufactured a false certificate (a `forward_ref`-fed hook with no channel; ISR's `Reference`-captured state invisible to the walk).
- **Closed because:** a step closure over a state struct (`raft_step`, `replica_step`) is one IR node, so the slice is Level 0 again; and even on dataflow-style ISR, Levels 0 and 1 coincide at every load — the slice cannot separate records that share a hook (S "Ablation"). A correctness requirement for Level 1, not a search improvement.
- **What it bought us:** the diagnosis that ISR's hypothesis count was half coarse lineage and half the goal's dependence on liveness signals (every heartbeat kept the leader), which no precision removes; and that record provenance, not slicing, keeps counts flat under load (2PC load 8: 11 vs 73).
- **Reopen only if:** never as a level (removed outright on the sibling branch).

### Every observed send as a crash candidate
- **Tried:** S "Two soundness problems", 2. Level 2 certified ISR at load 2 because the leader's crash *between* two acknowledgements was not in the goal's lineage — the goal was conditioned on the acknowledgement, not derived from it. First fix: admit every observed send.
- **Closed because:** it made the number of singleton hypotheses the number of sends, independent of lineage (2PC load 8: 46 instead of 11).
- **What it bought us:** Molly's answer — the precondition's lineage widens the universe: the crash right after each send in a `PRE` derivation is a *preserve point*, and universe = clause candidates ∪ preserve points (H §7 step 5).
- **Reopen only if:** never.

### Hand-named goals: `goal`, `witness`, `witness_final`, `require`
- **Tried:** Rounds 1–2: the property stated twice, a Rust verdict in the body and named output facts for lineage (`f0dad9a851`). `witness_final` was added when ISR's final view, derived as an "ever" fact, certified the bug at load 2 (S "Two kinds of witness").
- **Closed because:** the two encodings were never cross-checked and disagreed: a forgotten `require`, an unregistered channel, or an "ever" that should have been "final" was a wrong certificate with no warning (H §8.4, §10).
- **What it bought us:** `Spec`: `PRE`/`POST` as Hydro views on an observer, so the verdict and its lineage are one object and the body only drives (H §6.5). The old API is compiled and unused. [NEEDS AUTHOR REASONING: kept as an escape hatch on purpose, or should it be deleted so no new harness is written on it?]
- **Reopen only if:** never for harnesses.

### Exploring the observer's schedule
- **Tried:** The observer as an ordinary location whose ticks the explorer interleaved.
- **Closed because:** 64× more schedules on broadcast for an interleaving the program cannot observe (H §6.5).
- **What it bought us:** eager observer ticks outside the schedule choice.
- **Reopen only if:** never.

### Record provenance alone for the observer's views
- **Tried:** Round 3: derive `POST` from the tags its value carried.
- **Closed because:** a view that holds by *absence* (`filter_not_in`, an empty `count`) has no contributing records; the negated agreement `POST` had derivation `{}` and certified plain broadcast after one run (N §1; `e3928ed9fb`). Molly's De Morgan rewrite was considered and rejected: the simulator is a counterfactual oracle and need not know how the view was computed (N §2–3).
- **What it bought us:** `Lineage::spec_view`, the observer replayed with facts removed one at a time; positive and negated agreement get identical derivations.
- **Reopen only if:** never for hand-written views.

### Counterfactual lineage alone at the observer
- **Tried:** Immediately after: `spec_view` took only the counterfactual derivation.
- **Closed because:** ISR's durability `POST` reads the final leader off `observe_sequenced(views).max()`. Deleting views latest-first never changes the value, so the sufficient set was the write plus the *initial* view, with no heartbeat in its lineage; the breaking hypothesis (lose a replication, crash the leader after the acknowledgement, ZK promotes the stale follower) kills neither — it *adds* a view. Six hypotheses, certificate, bug present (N §4; `2dfc50feff`).
- **What it bought us:** the union of counterfactual and recorded derivations, sound where either is; and the general statement that the hitting-set inference holds only for views monotone in their intervenable facts, final-value views being the everyday counterexample (H §10).
- **Reopen only if:** the view is monotone by construction — which the generated specification is.

### The syntactic polarity check
- **Tried:** Round 4, `polarity.rs` (`c55d449a20`; N §5 (b)): a static walk grading each view `Monotone` / `NonMonotone` / `Unknown` to label the certificate and license the counterfactual-only derivation.
- **Closed because:** removed in `09618ccd96`. It could only approximate (the positive agreements of broadcast, 2PC and Raft graded `Unknown`; `first()` is an unclassifiable `scan`), and it existed only because hand-written views can have any shape. The generated specification — a universal over a fact domain and a member domain the hypothesis fixes — makes "derivation intact ⇒ view holds" true by its form, leaving nothing to classify (H §1.1, §6.5).
- **What it bought us:** the grades survive as words: *within the fault model* for the generated specification, *RELATIVE, no soundness claim* for every hand-written one; N §4 is the written reason. The control case (integrity, true, non-monotone, certified after one run with nothing checked) is the example for anyone who asks why.
- **Reopen only if:** hand-written specifications become a product rather than an escape hatch; even then a decision-trace oracle does not repair the inference for non-monotone views (N §5 (d)).

### Agreement as the Raft property
- **Tried:** Rounds 1–3: "no index holds two committed entries" (S "A Raft bug"; `881605b6c0`).
- **Closed because:** agreement is the *absence* of a conflict; a hypothesis breaks it by making a member commit something else, removing no recorded fact. The staged bug was found only because the same omission also displaced member 1's recorded commit (H §6.5).
- **What it bought us:** durability (`PRE`: some member committed `(i, r)`; `POST`: every correct member did), and the division of labour — agreement belongs to the simulator's own tests and Maelstrom (H §1.1).
- **Reopen only if:** never for LDFI; outside the class by definition.

### The staged Raft body and the "17 hypotheses" result
- **Tried:** Rounds 1–4 reported the election-restriction bug in 25, 20, then 17 hypotheses with a body that elected member 1, raced member 0's timer against a request, ordered requests and heartbeats, and fired member 1's timer twice after `EFF` (H §9).
- **Closed because:** everything the bug needs except the two lost messages was scripted by someone who knew it; the Maelstrom comparison credited lineage for conditions the body arranged; the author reported the number without saying so. Withdrawn with the 25-vs-160-vs-random-0/5 comparison.
- **What it bought us:** a generic body shaped only by what *correct* Raft needs, never touched in response to the buggy protocol; the bug in 90 hypotheses with a four-omission, no-crash, schedule-independent counterexample the author had not written down; correct Raft certified in 235. Blind and random on this body are unmeasured (a day of compute each).
- **Reopen only if:** never — the body is part of the claim.

### Tuning the body until correct Raft converged
- **Tried:** Round 5: four bodies "violated" durability on correct Raft (no post-`EFF` timer; no request of the new term; a timer spent on a re-armed suppression; too few heartbeats for the linear back-off), each lengthened; the fourth fix also changed the protocol (§5.3's conflict hint, because the long heal phase made a hypothesis an hour).
- **Closed because:** a specification error treated as a scheduling problem. `PRE` = "some member committed" is true at the first commit, so a run too short to catch up read as a bug (H §1.1).
- **What it bought us:** `PRE` must be a convergence witness, so an unconverged run is vacuous; `Outcome::Vacuous` when no hypothesis converges. The Raft harness is not yet on this pattern (needs the latest-heartbeat acknowledgement as an output; H §12 item 0).
- **Reopen only if:** never — fix `PRE`, not the horizon.

### Silent certification when `PRE` never held
- **Tried:** Rounds 1–4: a failure-free run with `PRE` false contributed `{}`, the ILP was infeasible, outcome `Certificate` (H §7, §9). The ISR false certificate of round 1 had the same shape (S "ISR replication", lesson 1).
- **Closed because:** the first generic Raft body sent requests to non-leaders, which this Raft drops; it certified in one run.
- **What it bought us:** `Outcome::Vacuous`, a verdict about the harness. The no-superset-of-a-vacuous-set constraint remains a heuristic (vacuity is not monotone in the fault set).
- **Reopen only if:** never.

### Letting a counterfactual replay panic
- **Tried:** Found, not tried: replay feeds recorded inputs to a member whose state has diverged; Raft's leader indexed past its log on an acknowledgement for an entry it never appended in the replayed world, and the panic unwound across the dylib boundary and aborted the process (H §6.4, §9).
- **Closed because:** a replay that cannot run is no evidence, and the abort took the search with it.
- **What it bought us:** `sim::runtime::erase_guarded`; a real-run panic is a test failure; a replay panic is inconclusive and keeps the delivery in the derivation (weaker clause, never a dropped one). Hit 624 times in 365,000 Raft replays.
- **Reopen only if:** never.

### Replay shortcuts: earlier-only candidates; unminimized post-`EFF` deliveries
- **Tried:** Round 2: minimize only over deliveries before the output's execution (S "Counterfactual lineage"). Round 5: skip minimizing post-`EFF` deliveries, which are not intervenable, to cut Raft replay cost (H §12 item 4).
- **Closed because:** a later echo can substitute for the original, so the first gave an empty derivation and a false certificate on reliable broadcast; the second made recursion worse, since minimization is also what prunes redundant retransmissions.
- **What it bought us:** candidates include later deliveries; a replayed tick may have nothing to do; a node that cannot reproduce its output with everything kept falls back to recorded lineage (H §6.4). Replay cost is in the recursion, not the per-replay time; caching across shared script prefixes is the open item.
- **Reopen only if:** a variant skips minimization *and* does not recurse through what it keeps.

### Maelstrom and partition nemeses for the election-restriction bug
- **Tried:** Round 3 (`e25bfbd2b1`; H §9.1): a Raft-backed `lin-kv` workload, twelve runs, 29 minutes, partitions and latency nemeses, ~1,900 elections, an oracle for an overwritten committed index.
- **Closed because:** the bug needs *selective* loss — `r0` to both followers, `r1` to the old leader, everything else delivered, so logs stay the same length while contents diverge. A partition drops everything on a link; the isolated node falls behind and the index-only check correctly denies it. And `EventualConsistency` + Maelstrom does not type-check: a premise is discharged by a completeness claim, not by "ran 29 minutes, no anomaly" (H §1.1).
- **What it bought us:** the sharp statement of what hydrofrac is for (its output shape, not a race on a bug); a real bug in Hydro's Maelstrom backend; and the honest weak form of the bug-finding claim.
- **Reopen only if:** a Maelstrom nemesis drops individual message identities, which narrows the second point only.

### Measurement mistakes: schedule accounting and the lucky baseline
- **Tried:** Round 1's tables charged LDFI 4–8 schedules per hypothesis against one instance per random fault set; the `bern` baseline visited nodes in registration order (S "Measurement grid").
- **Closed because:** hooks under a policy draw no entropy, so one schedule per hypothesis gave every verdict of eight on protocols without schedule-sensitive bugs; and registration order spent the crash budget on the coordinator / replica 0 half the time — the critical node in both protocols. Lucky, not uninformed.
- **What it bought us:** the corrected grid (lineage beats blind everywhere, 2–7× on certificates; beats uninformed random on 2PC at every size; ISR is "not yet a haystack") and the retraction of "random wins instances-to-bug". Raft keeps four schedules because its outcome does depend on the interleaving (H §5.5).
- **Reopen only if:** never — but re-measure before quoting a Raft speed-up.

### The undischarged-premise lint
- **Tried:** Round 5 (`09618ccd96`): every search printed every `EventualConsistency` source and whether the specification observed it; the first version printed forty lines for `multi_paxos`.
- **Closed because:** deleted in `aaa0ef79c8`. A report cannot fail a test, and making it one needed "every search over this program" (H §1.2). The direct route was to search every label.
- **What it bought us:** `Spec::all_labels` / `search_all_labels`: one verdict per labelled collection, minted and exit nodes both (they disagree on reliable broadcast), `NotChecked(reason)` for the rest.
- **Reopen only if:** never; `LabelResults` is the lint.

### A lattice-aware generated specification for `Singleton` outputs
- **Tried:** Proposed (H §1.2, §12) for ABD's register: observe the final value per correct member, `POST` is equality.
- **Closed because:** `7d58ad20e0`: checking the value re-proves the type system's conclusion. A labelled scalar is a function of its input stream, already vouched for; the only premise left is that every correct member gets the same inputs, so the tap goes on the input stream.
- **What it bought us:** a cleaner class boundary — dissemination, never computation. Scalars are `NotChecked(NotAStream)` until the input tap is built.
- **Reopen only if:** a scalar is labelled over an unlabelled input whose convergence nothing vouches for.

### The earlier proposal (`lfdi-on-sim-hooks.md`, `f32054f9`)
- **Tried:** Ordering cuts as "the easy 80%" of faults, a `ChannelHook` on the script-as-schedule model, a `CrashHook` fired `.at_quiescence()`, provenance as "a one-line codegen addition" (S "Appendix").
- **Closed because:** orderings are the fuzzer's dimension and Molly fixed them; crash at a phase barrier cannot express "after prepare to A, before B"; the "one line" is the Level 2 rewrite (26 operators, `wrap_opaque` for `Reference` captures; H §6.3).
- **What it bought us:** composition with the fuzzer — a hypothesis is a fault set, the explorer searches schedules under it, a counterexample is a pair (H §3).
- **Reopen only if:** never.

### Unmerged sibling work (`sandbox-6479bd3c…` and relatives, 19 Oct; not on `ldfi-hydrofrac`)
Three further doors, with their own record in `docs/design/ldfi-level-ablation.md` on that branch. The author should confirm which are canonical.
- **`Level::Slice` and `Level::Counterfactual` removed** (`4ddea33f19`): `Slice` matched `Execution` and `Counterfactual` matched `Record` on every protocol measured, the latter at 2–10× the replays; `Record` (renamed `Precise`, `fafcd70fe3`, which also undid a wrong downgrade for programs without provenance) is the default. The same commit made the simulator enforce "every failure ≤ `EFF`" after `4c43956b4b` found it had been firing post-`EFF` crash points the solver never proposed, one of two clause-soundness gaps that certified ISR's bug at three of four levels. [NEEDS AUTHOR REASONING: H §10 says `EFF` "should not shape the design further"; the sibling branch made it the simulator's fault model. Which stands?]
- **Disjunctive record provenance** (`73a5db9c05`, flag, default off): all witnesses per record at dedups. Verdicts identical; counts unchanged on 2PC and ISR, *higher* on echo broadcast (10→11, 32→39) — alternatives prune two hypotheses and mint three new crash points. The lazily grown universe already did most of Molly's pruning. Reopen only as "DNF clauses over the single-derivation universe".
- **Aggregate-local witnesses** ("any `k` of `n`" at a threshold fold): measurement incomplete at the branch tip. Open, not closed.

### Doors we never opened
- **Delay and reordering.** Expressible at the filter as hold-instead-of-drop (H §5.6); reordering is the schedule explorer's dimension.
- **Partitions as a primitive.** Omission sets already express them, and partitions miss selective-loss bugs (H §9.1).
- **Byzantine behaviour, duplication.** Outside Molly's vocabulary; duplication has no simulator hook (H §1.2).
- **`cut_after(n)` on `fail_stop`.** Nothing has needed a reliable channel to fail other than by its sender's crash.
- **The filter under `deploy`.** One OS-chosen interleaving per run, no determinism, replay or barriers (H §11.3): Molly's fixed schedule or worse.
- **Receive-side source for hook-free replay.** Would free lineage from `BatchHook` (H §11.3); deferred, since replay is confined to opaque records.
- **Decision-trace replay.** The general oracle (N §3); worth building only if the strengthened certificates are the ones users want, and it does not repair non-monotone views.
- **Why-not provenance by rewrite.** Molly's route (N §5 (c)); the oracle answers without knowing how the view was computed.
- **Exhaustive schedules for Raft; Raft's `committed` under the generated specification; blind/random on the generic Raft body; a coverage-guided fuzzer baseline; Raft under load.** Each an hour to a day of compute not yet spent (H §9, §10, §12).
- **Membership change.** Nothing to run it on: static membership in the simulator; `dyn_raft` is on `main` (H §12).
