# Issue: Is `broadcast_closed` / `broadcast_from_member` EC sound when the SENDER fails?

## Date: 2026-08-07

## Context

The `FailStop` TCP policy in `hydro_lang/src/networking/mod.rs` (line 99-113) carries
`type ConsistencyGuarantee = EventualConsistency`. The justification comment says:

```
// A failed connection stops *all* future deliveries to that recipient, which models the
// recipient as having failed. Consistency guarantees only apply to live members, so
// eventual consistency of replicated outputs is preserved.
```

## The Problem

This reasoning assumes the *recipient* is the one that fails when a connection drops.
But if the *sender* crashes mid-broadcast, some recipients receive a message and others
don't. All recipients are still alive and correct — they just have divergent state.

This is NOT eventual consistency. EC requires that all correct (non-crashed) processes
eventually agree. A sender crash mid-broadcast violates this unless there's a mechanism
for recipients to forward messages to each other (i.e., reliable broadcast).

`broadcast_closed` and `broadcast_from_member` do NOT implement reliable broadcast.
They're just cross-product + demux over independent TCP connections. No redundant
retransmission, no recipient-to-recipient forwarding.

## Questions for Shadaj

1. Is the EC type annotation on `broadcast_closed` with `fail_stop` actually sound
   under sender failure? Or does it implicitly assume the sender never crashes?

2. If it assumes the sender never crashes, should this be documented as a precondition?
   Or should the failure model be refined (e.g., `fail_stop` only gives EC if the
   sender is a long-lived process, not a cluster member that might fail)?

3. Does the simulator ever test the scenario where a sender crashes mid-broadcast
   (some messages delivered, some not)?

4. Should there be a separate `ReliableBroadcast` transport that actually implements
   RB (e.g., via recipient forwarding / gossip), for use cases where sender failure
   must be tolerated?

## Impact on This Work

For the `broadcast_from_member` primitive we added, the EC annotation is justified
by the same reasoning as `broadcast_closed` — so it inherits the same potential
soundness issue. In our consensus protocol, this is mitigated by the quorum/view-change
mechanism (anything committed is recoverable via quorum intersection), but the raw
primitive's EC claim may be too strong.

## Location in Code

- `hydro_lang/src/networking/mod.rs` lines 99-113 (`FailStop` impl)
- `hydro_lang/src/live_collections/stream/networking.rs` line 413 (`broadcast_closed` trusted assertion)
- `hydro_lang/src/live_collections/stream/networking.rs` line ~1395 (`broadcast_from_member` trusted assertion)
