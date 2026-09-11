# Hydro feedback stage-telemetry: measurements and scoped hypothesis

Status: research evidence log (NOT a repository audit, NOT a validated classifier)

> **Correction (recovery pass).** An earlier version of this document presented
> reusable `SmellFinding` predicates as a repository-wide "smell test" and used
> verdict language such as "Clear smell" and "Measured feedback-amplification
> smell." Those predicates were **not** a sound linter and have been
> **quarantined** (see `hydro_test/src/feedback_smells.rs`, gated permanently
> off, and the recovery plan in `AI_WORK_STATUS.md`). Do not treat any claim
> that this repository has been automatically linted as true. This document now
> records only (a) the concrete measurements that were collected and (b) a
> single scoped hypothesis those measurements motivate. The reproducible
> telemetry parsing lives in `hydro_test/src/stage_telemetry.rs`; the
> deployment tests assert directly on measured `StageWindow` fields.

## Why the predicates were removed

The three predicates each conflated a measurement with a diagnosis:

- **`retained_state_network_work_without_ordinary_input`** flagged any stateful
  stage that emitted network work in a window with no ordinary handoff input.
  Ordinary queue *drainage* after an upstream burst produces exactly this shape.
  This was directly observed: in the retry run, the finite-rate service's
  response-queue block matched the predicate purely from draining buffered work,
  a false positive the predicate could not distinguish from retry reactivation.
- **`state_dependent_network_volume`** flagged a byte-volume increase between a
  hand-picked "small" and "large" configuration. Larger retained/configured
  state is expected to move more bytes even for perfectly productive programs;
  two hand-chosen sizes do not isolate a feedback hazard.
- **`multi_window_network_amplification`** treated traffic spanning more than one
  wall-clock window as amplification. Window boundaries are sampling artifacts;
  ordinary finite work routinely straddles two windows with no feedback gain.

Experiment selection was also entirely hand-authored, and no general
repository-wide analysis was ever demonstrated. The predicates are therefore
kept only as a quarantined record.

## What was actually measured (trustworthy)

These are execution measurements from real localhost deployments, collected by
the EMF stage sidecar. They are reproducible and are the useful residue of this
work. They are not, on their own, a classification of any program.

| Program | Controlled variable held fixed | Varied | Measured result |
|---|---|---|---|
| `pure_heartbeat::pure_heartbeat` | membership (3), pump rate | — | Interval block activates and emits fixed-size, membership-bounded network work. No stage shows a retained-state read/write emitting network work with zero ordinary input. Exhaustive simulation confirms exactly one heartbeat per timer event per member. |
| `distributed::timeout_retry::timeout_retry` | program, deployment | finite input burst | At least one retained-state stage emits physical network work in windows with zero ordinary handoff input. Client request windows carried 37, 27, 15, 2 messages across successive windows. (The service response-queue block *also* matched the old predicate — a false positive from queue drainage, not amplification.) |
| `crdt_gossip::g_set_gossip` | membership (3), pump rate, duration | retained elements: 4 vs 400 | Pump **message** count fixed (63 vs 63). Serialized pump **bytes** grew 1,512 -> 101,304 (~67x). Poll time ~3,891us -> ~29,602us (~7.6x). |
| `multi_paxos_live::multi_paxos_live` | election activation, cluster (3) | pending commands: 1 vs 20 | Aggregate network volume grew 32 msgs / 782 bytes -> 450 msgs / 12,372 bytes. |
| `reliable_broadcast::reliable_broadcast_closed` | membership (3) | — (8 finite inputs) | Network traffic appeared in more than one measurement window (observed windows e.g. `[24, 72, 0, ...]`, 96 total messages), then drained to zero and stayed there. |

Every one of these is a *measurement of physical work*, not a proof of
metastability. In particular, byte growth with retained state is expected for
correct programs; the measurements do not by themselves separate hazardous
feedback from benign scaling.

## The scoped hypothesis (refined against the evidence)

The evidence above does not support a general smell classifier. It supports one
narrow, falsifiable hypothesis, expressed only in terms of quantities the
sidecar actually records:

> **H1 (activation/volume separation).** Using only DFIR block-activation and
> per-window network measurements — run count, ordinary vs feedback input items,
> output items, network message count, network byte count, retained-state
> read/write flags, and poll time — the isolated pure-heartbeat control is
> distinguishable from the timeout/retry loop by the combination:
> *(a)* a stage with a retained-state read/write reference that
> *(b)* emits physical network work in a window where it drained zero ordinary
> handoff input.

H1 is deliberately weaker than the earlier claims:

- It is a statement about **two specific programs**, not a repository audit.
- It classifies a **stage-window shape**, not a program's stability.
- It makes **no** claim that the shape implies metastability. The retry ground
  truth separately establishes that retry's emissions are physical attempts and
  that a finite burst amplifies them; H1 only asserts the *shape is visible in
  telemetry*.

### Known counterexamples to any stronger reading of H1

These are exactly why H1 must stay scoped and why the old predicates failed:

1. **Queue drainage.** A finite-rate service draining a buffered response queue
   produces the same "(a)+(b)" shape without any feedback amplification.
   Observed in the retry service block. Distinguishing drainage from
   reactivation requires an additional, not-yet-defined recurrence/gain
   condition (e.g. the emission recurs across windows funded only by retained
   state after ordinary input has ceased).
2. **Byte growth from larger state.** Gossip's 67x byte growth at fixed message
   count is expected for a correct state-based CRDT under a larger payload. It
   is a reason to measure bytes rather than items — not evidence of a hazard.
3. **Multi-window finite work.** Reliable broadcast's traffic spanning two
   windows then draining is ordinary finite propagation, not amplification.
4. **Completion-authorized work.** `bench_client` is a completion-driven closed
   loop with retained start-time state; each completion *intentionally*
   authorizes fresh work. Stage telemetry alone cannot prove freshness; this is
   the standing case for whether tuple- or API-level semantics are eventually
   needed.

## What would move H1 forward (before writing any new predicate)

Per the recovery plan, define the precise observable property *first*. Candidate
next measurements, each holding fresh logical work fixed so it cannot be
confounded with added input:

- **Recurrence condition.** Formalize "retained-funded emission that recurs
  after ordinary input drains" and test whether it separates retry from the
  service-queue drainage false positive.
- **Raft / dynamic Raft suffix replay.** Fix logical work; vary only follower
  catch-up history. The earlier 1-vs-20-command Raft run (76 vs 79 msgs, 2,748
  vs 3,633 bytes) was **inconclusive** because it also changed fresh input.
- **Broadcast-transcript / Paxos recovery.** History-dependent replay on
  election/recovery, with no heartbeat pump, isolated from added input.
- **Maelstrom periodic full-state broadcast.** Expected to reproduce the gossip
  fixed-message / growing-byte shape; a check on whether the byte-volume
  measurement generalizes.

Only after a candidate property survives these controls should a predicate be
re-introduced — and it should be validated against the four counterexamples
above, not hand-selected experiments.
