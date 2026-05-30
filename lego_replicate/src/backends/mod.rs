//! Feature-gated backend implementations of `ReplicableService`.

pub mod hashmap_kv;

#[cfg(feature = "backend_redb")]
pub mod redb;

#[cfg(feature = "backend_fjall")]
pub mod fjall;

#[cfg(feature = "backend_rusqlite")]
pub mod rusqlite;
