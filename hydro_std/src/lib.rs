#[cfg(stageleft_runtime)]
hydro_lang::setup!();

pub mod bench_client;
pub mod compartmentalize;
pub mod ec_inference_demos;
pub mod membership;
pub mod quorum;
pub mod request_response;

#[cfg(test)]
mod taxonomy_tests;
