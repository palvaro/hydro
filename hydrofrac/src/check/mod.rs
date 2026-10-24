//! The check: one entry point that searches every `EventualConsistency` label of a program
//! (docs/design/hydrofrac-lite.md §2, §4.3).
//!
//! ```ignore
//! let mut check = hydrofrac::EcCheck::new();
//! let sim = check
//!     .instrument(flow.sim().with_cluster_size(&members, 3))
//!     .compiled();
//! check
//!     .run(&sim, async || {
//!         send.send(Item(1));
//!         send.send(Item(2));
//!     })
//!     .assert_holds();
//! ```
//!
//! [`EcCheck::instrument`] taps every checked label ([`crate::observe`]) and attaches record
//! provenance; [`EcCheck::run`] runs the loop of docs/design/hydrofrac-lite.md §4.3 for every
//! label: the failure-free probe (hypothesis 0) learns each fact domain, then each label's
//! [`Sweep`](crate::search::Sweep) proposes fault sets from zero crashes upwards (§4.5). Each set
//! runs once over every schedule (or [`EcCheck::sample_schedules`] of them), shared by every
//! label; when the body returns the network heals (§4.6) and the run quiesces.
//!
//! A **schedule-sensitive** label ([`crate::observe::LabelPlan::sites`]) cannot be certified:
//! lineage pruning assumes a derivation recurs whenever its deliveries do. Its search only needs
//! to kill one schedule's derivations of each earlier run ([`crate::search::Sweep::next_any`])
//! and ends [`Verdict::Exhausted`] instead of certified.

#[cfg(stageleft_runtime)]
mod engine;
#[cfg(stageleft_runtime)]
mod results;

#[cfg(stageleft_runtime)]
pub use engine::EcCheck;
#[cfg(stageleft_runtime)]
#[cfg(any(test, feature = "cross-check"))]
pub use engine::{BruteForce, BruteLabel, CrossCheck};
#[cfg(stageleft_runtime)]
pub use results::{
    Counterexample, LabelResult, NondetSite, Results, Stats, VacuousReason, Verdict,
};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod brute_tests;
