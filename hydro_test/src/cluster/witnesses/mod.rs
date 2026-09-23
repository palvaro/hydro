//! The witness corpus: small Hydro programs that may or may not amplify work
//! under adversarial scheduling, each with a stress-test harness that confirms
//! its label. See `design_docs/2026-09_witness_corpus_spec.md`.

pub mod cache_thundering_herd;
pub mod bounded_queue_rejection;
pub mod crdt_gossip_load;
