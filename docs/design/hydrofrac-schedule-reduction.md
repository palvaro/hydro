# Hydrofrac: sent-set crashes and one schedule per hypothesis

This note explains three additions to `hydro_lang::sim::ldfi` and records the measurements
behind them: crash points named by the *set* of messages a node got out, a static check for
non-deterministic operators upstream of a checked collection (`sim::order`), and a mode
that runs one schedule per hypothesis when those two together make the extra schedules
redundant.

## The argument

A fault hypothesis is run under every schedule (`Exploration::Exhaustive`) or under a
sample of them (`Fuzz`), because its outcome can depend on the schedule. With an omission
named by message identity, a send's fate depends only on the send itself. Now take a
computation upstream of the checked collection that is order-insensitive, with no operator
that takes a `nondet!` guard anywhere in its causal cone. Then each node's received set, and
so its final state, is the same in every schedule, and one schedule decides the hypothesis.

A positional crash, `crash(n after m)`, breaks this: what it kills depends on the order in
which the schedule has `n` attempt its sends. One measurement shows it: changing the
emission order of one fan-out (`flat_map` versus `cross_product`) took reliable broadcast's
certificate from 36 to 49 hypotheses, with the same verdict. A crash named by the set `S` of
messages the node got out restores the argument. The node delivers a send iff the send's
identity is in `S`, whatever the order.

## Sent-set crashes (`CrashModel::SentSet`)

`Fault::Crash { node, at: CrashAt::Sent(S) }`, installed as `CrashPoint::Sent`:

- A protocol send whose identity is in `S` is delivered (subject to omissions). Any other
  send is dropped.
- The node is dead once every message of `S` is out, or at `EFF`, whichever comes first.
  After that every send is dropped, and its outputs and observations are suppressed. An
  empty `S` is a crash before any send.
- Kill relation (`Dependency::killed_by`): a delivery is killed if it is not in `S`, if it
  was sent after `EFF`, or if it is a copy attempted after the whole set was out. A
  dependency on an observation is killed by any crash of its node, which is conservative.
- Hypothesis space: to kill a send `d`, the candidate is the set of distinct messages the
  node got out before `d` in the run the clause came from. That is the positional "crash
  right before `d`", named by identity, and it is realizable by construction because a real
  run sent exactly that prefix. Preserve points (keeping a pre-condition's send) are the
  prefix up to and including it.
- Any subset of one execution's fan-out would also be realizable, since the fan-out's
  order is emission order, not causality. Proposing every subset took reliable broadcast
  past the 256-hypothesis budget, so only prefixes are proposed.

### Where sent sets are weaker than positional crashes

A set minted in one run cannot do two things:

1. Let through a message the node first sends only under another hypothesis. ISR's leader,
   once a replica's copy is lost, must shrink the in-sync set (`zk Shrink([1])`) before it
   acknowledges. No earlier run sent that message, so no proposed set contains it. The
   hypothesis then goes vacuous instead of exposing the unclean-election bug.
2. Cut a byte-identical retransmission. A set cannot say "the first heartbeat but not the
   later ones"; only the node's death once the set is complete cuts later copies.

A positional crash does both. So `SentSet` is not the default. `CrashModel::Auto` uses sent
sets only where they make the schedule reduction sound (see below) and positional crash
points everywhere else.

## The static check (`sim::order`)

The non-deterministic operators in the IR are the ones whose API takes a `NonDet` guard:

- `Batch` (`batch`, `snapshot` and their atomic forms; the `sample_*` operators contain one);
- untrusted `ObserveNonDet` (`assume_ordering`, `assume_retries`);
- `MergeOrdered`;
- timer sources (`source_interval`, `Instant::now`).

`Cones::nondet_upstream` walks the causal cone of a collection. It follows inputs, each
`Tee`'s or reference's shared node, each `forward_ref` back to its sink, each captured state
cell to its writers, and each network channel back to its sender.

- `LabelPlan::upstream_nondet` lists the sites per labelled collection, and
  `order_insensitive_upstream()` is true when the list is empty.
- `CompiledSim::nondet_upstream_of(location)` lists the sites for everything sent to a
  location: for a specification's observer, what it observes.

The check is conservative. A `batch` whose consumer is in fact insensitive to batch
boundaries is still a site. `Ldfi::trust_order_insensitive([...])` (or
`LDFI_TRUST_ORDER_INSENSITIVE`) names such sites, and the certificate then states the
assumption.

## One schedule per hypothesis (`Ldfi::schedule_reduction`)

The search replaces the configured exploration with `Exploration::FirstSchedule` when:

- the crash model is `SentSet`, or `Auto` resolving to it;
- the goal is a generated `EventualConsistency` specification, which reads only the final
  contents at correct members;
- no untrusted non-deterministic operator lies upstream of the checked collection.

`FirstSchedule` is the deterministic first schedule of the exhaustive explorer, every choice
at its first alternative. It also applies to the failure-free probe that learns the fact
domain. The certificate carries `reduction: Some(ScheduleReduction { trusted })`.
`LDFI_SCHEDULE_REDUCTION=0` forces exploration, and `LDFI_EXPLORATION=exhaustive|fuzz:N|first`
overrides the configured exploration. The harness body is assumed not to branch on what it
observes of the program.

## Memory

Each explored schedule is now folded into the search state as soon as its execution
finishes: `CompiledSim::{exhaustive,fuzz_bounded,first_schedule}_then` call back between
executions, outside the instance. Its record (the full event log) is dropped right away.

`Hypothesis::schedules` keeps only the schedules that contributed something: the first, a
failing one, and any with a new derivation or pre-condition derivation.
`Hypothesis::explored` and `Hypothesis::totals` count and sum over all of them.
`Ldfi::retain_schedules()` (used by the ablation harnesses, whose tables sum per-schedule
statistics) or `with_traces()` keeps every schedule.

## Measurements

These are `LDFI_RECORD` rows over every hydrofrac test in hydro_test, hydro_std and
hydro_lang (121 searches; ignored tests are listed separately below).

- **Old tree vs new tree, Positional:** every Exhaustive search is identical. The Fuzz
  searches differ only by sampling noise (hypothesis counts, and which Paxos omissions
  appear in the counterexample), with the same verdicts.
- **Positional vs SentSet (schedule reduction off):**
  - Verdicts change only on ISR: the unclean-election bug becomes a certificate, in four
    tests plus `compare_search_strategies`. The cause is the first limitation above.
  - Hypothesis counts grow: reliable broadcast 49→77, uniform broadcast 105→132, multi-Paxos
    `learned` 25→70, G-Set 13→32. A set minted in one run is a different fault from the one
    minted in another run, where the positional crash point named by the same message would
    have been shared.
  - Several tests that assert the positional shape (`CrashAt::After(..)`) or exact
    hypothesis counts fail. That is expected.
- **Positional vs Auto (schedule reduction on):** every verdict agrees. The searches the
  reduction applies to ran one schedule per hypothesis:

  | search | before | after |
  |---|---|---|
  | `ec_demo_reliable_broadcast_certifies` | 49 hypotheses, Exhaustive | 77 hypotheses, 77 schedules |
  | multi-Paxos `multi_paxos.rs:281` | 40 schedules | 5 |
  | `all_labels_reports_a_label_inside_a_tick` | 948 schedules | 5 |
  | the 2PC and ABD all-labels rows | 16–20 schedules each | 1–2 |

  The only test failure is `ec_demo_reliable_broadcast_all_labels`, which asserts 49
  hypotheses.
- **Classification:**
  - Order-insensitive: the reliable-broadcast family (closed, live, cluster-to-cluster), the
    single-hop `broadcast_closed` labels, multi-Paxos `:281`, ISR `:343`, 2PC `:86`, and ABD
    `:141`.
  - Racy, so their schedules are still explored:
    - multi-Paxos `learned` and the rest of its labels (batches at `multi_paxos.rs:299`,
      `:436`, `quorum.rs:135`);
    - G-Set gossip (batch at `crdt_gossip.rs:70`/`:154`, `assume_ordering` at `:57`/`:141`);
    - `leader_merge` (`assume_ordering`);
    - ISR `:360`/`:564`;
    - the hydro_test `reliable_broadcast` harness (a batch at `reliable_broadcast.rs:70`);
    - uniform broadcast (`quorum.rs:135`);
    - every hand-written specification (Raft, Paxos, 2PC/CTP, ISR election), which the
      reduction does not cover.
- **Uniform broadcast:** its only site is the quorum's `batch` (`quorum.rs:135`), whose
  `nondet!` justification argues it is boundary-insensitive. Trusting that site
  (`LDFI_TRUST_ORDER_INSENSITIVE=quorum.rs:135`, `LDFI_CRASH_MODEL=auto`):
  - `ec_demo_uniform_broadcast` (thresholds 1 and 2) and
    `uniform_agreement_uniform_broadcast_generated_spec` each certify in 132 hypotheses and
    132 schedules, about 4.3 s each, with a peak RSS of about 250 MB (whole test process).
  - Before, they took 105 hypotheses and 840 sampled schedules (Fuzz 8, not a proof). An
    Exhaustive run was infeasible: more than 359k schedules in the probe alone, 10 GB.
  - `uniform_agreement_uniform_broadcast` has a hand-written specification, so the reduction
    does not apply. Forced to one schedule per hypothesis anyway (`LDFI_CRASH_MODEL=sent-set
    LDFI_EXPLORATION=first`), it certifies in 306 hypotheses and 306 schedules, 42 s. That is
    a sample of one, not a proof. With Positional and Fuzz 8 it took 276 hypotheses, 2208
    schedules and 52 s.
- **Memory:** Positional Exhaustive on `ec_demo_uniform_broadcast`
  (`LDFI_EXPLORATION=exhaustive LDFI_SCHEDULE_REDUCTION=0`) was still in the failure-free
  probe after 40 minutes, and its resident set never exceeded 241 MB. The same probe used to
  retain one record per schedule, which reached 10 GB by 359k schedules.
- **Ignored tests:** I ran these with Positional and with SentSet at two tests at a time,
  under an 80-minute cap.
  - With Positional: `ldfi_certifies_paxos_agreement_under_one_crash` certified (377
    hypotheses, 18 min). `ldfi_certifies_paxos_kv_prefix_agreement_under_one_crash`
    certified (344, 22 min). `ldfi_finds_the_election_restriction_bug` found its
    counterexample (88, 25 min). `ldfi_ctp_three_participants_two_crashes` found its
    counterexample (7 hypotheses, 179k schedules, 50 min).
  - With SentSet, neither Paxos search finished within the cap, which means more
    hypotheses. These are hand-written specifications, so `Auto` keeps them positional.
