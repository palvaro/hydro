//! # lego_replicate
//!
//! Transparent replication built from composable consensus primitives.
//! Same functionality as `hydro_transparent_replicate` but constructed
//! by composing pre-built protocol building blocks (legos).

#[cfg(stageleft_runtime)]
hydro_lang::setup!();

pub mod service_trait;
pub mod messages;
pub mod config;
pub mod primitives;
pub mod protocol;
pub mod client;
pub mod applier;
pub mod service_runner;

#[cfg(test)]
mod sim_composition_test;

#[cfg(any(
    feature = "backend_redb",
    feature = "backend_fjall",
    feature = "backend_rusqlite"
))]
pub mod backends;

pub use service_trait::ReplicableService;
pub use config::ReplicateConfig;
pub use messages::{TransparentReplica, View, Router};
pub use client::{ReplicatedClient, ClientError};
