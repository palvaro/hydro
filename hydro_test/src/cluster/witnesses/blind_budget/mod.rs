//! Client-side retry governance witnesses.
//!
//! All configurations use the same [`retry_service`] constructor and select a policy through
//! [`RetryConfig`]. Each row reports only a label accepted by the study brief and the assertion
//! measured by that configuration's own harness.
//!
//! | configuration | label | basis | headline number | redundant work or bound |
//! |---|---|---|---|---|
//! | [`no_governance`] | hazardous | exhibited collapse | tail has 800 repeated sends and 666 repeated serves; backlog 2,119 -> 2,318 | Continuing requests reach their retry caps in succession, sustaining redundant work after the burst. |
//! | [`time_budget`] | hazardous | exhibited collapse | tail has 800 repeated sends and 666 repeated serves; backlog 1,100 -> 1,299 | Timer refills sustain four retries per round after the input burst. |
//! | [`success_budget`] | benign | assurance argument | 220 inputs; sends and serves each at most 285 | With initial budget `B=10` and one token per `K=4` useful replies, each count is at most `N+B+floor(N/K)`. |
//! | [`hybrid_budget`] | hazardous | exhibited collapse | tail has 691 repeated sends and 634 repeated serves; backlog 914 -> 1,005 | Timer and success refills keep redundant copies arriving after the burst. |
//! | [`circuit_breaker`] | benign | assurance argument | 220 inputs; sends and serves each at most 660 | Closed retries and probes share the `A=3` attempt counter, so each count is at most `N*A`; new input is dropped while open or probing. |

pub mod service;

pub mod circuit_breaker;
pub mod hybrid_budget;
pub mod no_governance;
pub mod success_budget;
pub mod time_budget;

pub use service::{
    Client, Completion, Dropped, Policy, Reply, Request, RetryConfig, RetryOutputs, Server,
    retry_service, steady_state_inputs,
};
