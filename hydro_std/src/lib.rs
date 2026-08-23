#[cfg(stageleft_runtime)]
hydro_lang::setup!();

pub mod bench_client;
pub mod broadcast_live;
pub mod compartmentalize;
pub mod crdt_gossip;
pub mod fan_out;
pub mod membership;
pub mod quorum;
pub mod reliable_broadcast;
pub mod request_response;

#[cfg(test)]
mod taxonomy_tests;
