//! Composable consensus primitives adapted from consensus-zoo.
//!
//! These are the same patterns as consensus-zoo's primitives, made generic
//! over command type and adapted for the current hydro API. They operate
//! within a single Cluster (dynamic primary topology).

pub mod decide;
pub mod ordered_deliver;
pub mod liveness;
pub mod discover;
