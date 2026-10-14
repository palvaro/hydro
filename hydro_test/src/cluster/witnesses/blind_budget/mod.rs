//! Client-side retry governance witnesses: one request/reply service, one attempt cap, and six
//! ways of deciding whether a timeout produces a re-send.
//!
//! All configurations use the same [`retry_service`] constructor and differ only in the `policy`
//! field of [`RetryConfig`], except `unbounded`, which also removes the attempt cap. The cap
//! (`max_attempts: 3`) applies under every policy: a request is sent at most three times and is
//! abandoned at its third timeout whether or not its earlier timeouts produced a send, so no
//! request outlives three timeouts. Every capped configuration therefore satisfies sends at most
//! `3N` for `N` input requests; what the policies change is the rate at which a disturbance is
//! allowed to add copies to the server's queue, and that is what decides whether the queue drains.
//!
//! Each row reports a label accepted by the study brief and the assertion measured by that
//! configuration's own harness (`harness.rs`): a deterministic run of 800 rounds at two requests per
//! round with twelve per round in rounds 100..160, read in rounds 600..800; a steady-state control;
//! and the closed-form bound asserted under 256 random schedules. The checker runs the same six
//! configurations through the `#[amplification_check]` attributes on [`retry_service`]
//! (`./scripts/cress hydro_test retry_service`).
//!
//! | configuration | policy | label | basis | expected tail (rounds 600..800) | closed form for sends and serves |
//! |---|---|---|---|---|---|
//! | [`unbounded`] | no cap, re-send on every timeout | hazardous | exhibited collapse | repeated sends in every round, queue still growing, fewer than half of offered requests complete | none |
//! | [`no_governance`] | cap 3, re-send on every timeout | hazardous | exhibited collapse | 800 repeated sends, queue still growing | `3N` |
//! | [`time_budget`] | cap 3, one token per client clock element, bucket 10 | benign | assurance argument | no repeated sends, queue empty, every request completes | `min(3N, N + 10 + T)` |
//! | [`success_budget`] | cap 3, one token per four useful replies, bucket 10 | benign | assurance argument | no repeated sends, queue empty, every request completes | `min(3N, N + 10 + floor(N/4))` |
//! | [`hybrid_budget`] | cap 3, both refills, bucket 10 | benign | assurance argument | no repeated sends, queue empty, every request completes | `min(3N, N + 10 + T + floor(N/4))` |
//! | [`circuit_breaker`] | cap 3, opens after 8 consecutive timeouts for 12 ticks, one probe | benign | assurance argument | no repeated sends, queue empty, every request completes; input refused while open | `3N` |
//!
//! # Provenance
//!
//! The first version of this set was written under the blind-author protocol
//! (`design_docs/2026-09_blind_witness_task.md`); its configurations and the checker's recorded
//! verdicts on them are in `design_docs/2026-09_blind_budget_results.md`. That version compared
//! unlike things: `no_governance` had an attempt cap of three, the three token-bucket policies had
//! no cap at all, and the clock refill of four tokens per tick exceeded the two requests per round
//! it was meant to throttle, so a "time budget" client sent more than an ungoverned one. It also
//! never removed a request from the pending map once its cap was reached, so abandoned requests
//! kept timing out for the rest of the run. The present version repairs those things: the cap is a
//! field of [`RetryConfig`] shared by every policy, abandonment removes the request, the refill
//! rates are below the load they govern, and an uncapped baseline is added. It was written by an
//! author who had read the checker and its results, and so is **not** a blind set; the labels
//! rest on the harness and the arguments in each module and on nothing else.

pub mod service;

#[cfg(test)]
mod harness;

pub mod circuit_breaker;
pub mod hybrid_budget;
pub mod no_governance;
pub mod success_budget;
pub mod time_budget;
pub mod unbounded;

pub use service::{
    Client, Completion, DropKind, Dropped, Policy, Reply, Request, RetryConfig, RetryOutputs,
    Server, retry_service, steady_state_inputs,
};
