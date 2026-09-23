//! The witness corpus: small Hydro programs labeled hazardous (some schedule makes the same input
//! cost more work, and more the longer delivery is delayed) or benign (work is bounded by the
//! input under every schedule), each with a stress-test harness that confirms its label. See
//! `design_docs/2026-09_witness_corpus_spec.md`.

pub mod backoff_retry;
pub mod bounded_queue_rejection;
pub mod cache_thundering_herd;
pub mod crdt_gossip_load;
pub mod election_stampede;
pub mod gossip_resend;
