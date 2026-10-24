#![recursion_limit = "256"]

//! Standardized correctness, performance, and complexity comparison for the
//! repository's consensus backends.
//!
//! The data model and report generator have no deployment dependency. Enable
//! `deploy` for localhost execution and `ecs` to export the identical Hydro
//! graph as an ECS manifest.

#[cfg(stageleft_runtime)]
hydro_lang::setup!();

pub mod backend;
pub mod census;
pub mod html;
pub mod install;
pub mod maelstrom;
pub mod perf;
pub mod registry;
pub mod report;
pub mod review;
pub mod trust;

#[cfg(feature = "maelstrom")]
pub mod maelstrom_runner;
#[cfg(feature = "deploy")]
pub mod runner;
