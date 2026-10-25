# DFIR Activation Evidence: Heartbeat vs. Timeout/Retry

Status: research evidence checkpoint

## Hypothesis

DFIR block activations can distinguish:

1. **fixed operational work**, whose activation and gain are determined by an exogenous source such as a timer; and
2. **feedback-amplified work**, where an operational activation reaches a block that reads retained data and emits new physical work from that data.

This is a dynamic hypothesis about block activations and data crossing block boundaries. It is not a static proof of safety and does not require tuple-level lineage.

**Scope (recovery pass).** This hypothesis is scoped to distinguishing the two
programs measured here, and only as a *telemetry shape*, not as a stability
verdict. Reusable "smell" predicates that once accompanied this evidence were
unsound and have been quarantined; see
`2026-09_hydro_feedback_smell_inventory.md` for the refined, falsifiable
statement (H1) and its known counterexamples (queue drainage, byte growth from
larger state, multi-window finite work, completion-authorized work).

## What is measured

DFIR already records, per interval:

- block run and poll counts;
- time spent polling;
- items drained from each input handoff;
- items present at handoffs.

The runtime retains the DFIR graph using the same IDs as these counters. The graph supplies:

- operators fused into each block;
- producer and consumer handoffs;
- delayed feedback handoffs;
- references to retained state, including mutability;
- compiler tags that correlate lowered operators with Hydro IR.

The spike adds a read-only stage view joining these existing measurements and graph facts. It also preserves `source_interval` origin automatically through Hydro IR and lowering as an `interval__<id>` tag.

No user annotation identifies heartbeat, retry, hazard, or safety.

## Heartbeat control

The isolated heartbeat program is:

```text
interval -> fixed heartbeat -> broadcast
```

It has no acknowledgement input, retry, outstanding-work state, or response path.

### Structural observation

The optimized block containing the interval also contains the network sink. It has:

- no input handoff;
- no delayed feedback handoff;
- no retained-state read or write.

### Dynamic observation

In actual deployed 100 ms windows, the interval/network block activated repeatedly. A representative run observed 5–10 block runs per window. The sidecar directly records nonzero serialized network message and payload-byte totals for this block.

A separate exhaustive simulator test establishes cardinality: each supplied timer event emits exactly one fixed-size heartbeat to every member. Therefore gain is fixed and membership-bounded.

## Timeout/retry

The retry program contains:

```text
organic request -> outstanding state -> attempt -> service
                       ^                       |
interval --------------+<------ response -----+
```

### Structural observation

The optimized client graph contains exactly one interval-dependent block with a mutable retained-state reference. The graph also contains the outbound request path.

### Dynamic observation

Actual deployed stage windows showed two relevant blocks:

1. an interval block with approximately seven activations/outputs per observed window; and
2. a retained-state block with three reads, one write, and physical output despite zero ordinary handoff input in that window.

The stage sidecar now reports exact serialized network message and payload-byte counts for internally serialized Hydro channels. Counting occurs after serialization and immediately before the transport sink; keyed sends extract the byte payload from `(destination, bytes)` without counting the destination wrapper. Transport framing is excluded.

Representative retained-state output counts across successive windows were 12, 0, and 1 while interval activity continued. The exact values are schedule-dependent; the significant observation is that the stateful block can emit work with no new ordinary handoff input, funded by retained data and operational activation. Subsequent trace tests directly observed nonzero serialized messages and bytes on this path.

The existing ground-truth deployment independently confirms that these emissions are physical retry attempts and that a finite overload produces work amplification.

## Gossip boundary result

Applying the same trace to state-based G-Set gossip demonstrates why direct byte accounting matters. Two real deployments used identical three-member membership, pump rate, and duration, but retained 4 versus 400 elements. Filtering to interval-origin pump stages excluded initial organic update dissemination.

Observed pump-only totals:

| Retained elements | Messages | Serialized payload bytes | Poll time |
|---:|---:|---:|---:|
| 4 | 63 | 1,512 | 3,891 us |
| 400 | 63 | 101,304 | 29,602 us |

Message gain was exactly fixed, while byte volume grew about 67x and poll time about 7.6x. Thus stage item/message counts alone would classify gossip as constant work even though retained state substantially increases physical work. Gossip remains a potentially hazardous feedback pattern; whether it becomes unstable requires a capacity/workload experiment.

## Conclusion

The evidence supports the feasibility of the activation-based approach for these two programs:

| Observation | Pure heartbeat | Timeout/retry |
|---|---:|---:|
| Operational interval block activates | yes | yes |
| Work path reads retained data | no | yes |
| Work path mutates retained data | no | yes |
| Delayed/feedback dependency governs emission | no | yes |
| Fixed bounded gain established | yes | no |
| Work emitted without new ordinary handoff input | no retained-data mechanism | observed |

The distinction is visible at DFIR block granularity. Tuple-level lineage is not required for this initial separation.

## Important limitations

- Network sinks may be fused into a block, so handoff output counts alone do not measure all physical work. Block runs and operator topology must be interpreted together.
- Retained-state references establish dependency, not field-level causality. They do not prove which retained item caused an output.
- Item count is not a universal work metric. Gossip payload bytes may grow while message count remains fixed.
- This evidence distinguishes the isolated heartbeat control from retry. It is not yet a general classifier.
- Productive recursive computation such as TC has not yet been evaluated with this activation trace. No static `anti_join` proof is needed for that future dynamic comparison.

## Next research step

Apply the same trace to state-based CRDT gossip and ask separately whether activation amplification appears in:

- item count;
- payload bytes; or
- block poll time.

Gossip should remain a potentially hazardous pattern until those measurements show otherwise.
