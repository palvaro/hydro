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
//! provenance in fragile mode: each record names its deliveries and the fragile steps it passed.
//! [`EcCheck::run`] runs the search of docs/design/hydrofrac-races.md (§4.4, §12) for every
//! label:
//! - the failure-free probe (hypothesis 0) learns each fact domain;
//! - each label keeps one clause per derivation of each `(member, fact)` pair, and only robust
//!   derivations skip fault sets;
//! - its sweep ([`crate::races::Sweep`]) proposes fault sets from zero crashes upwards, and
//!   each set is explored over its schedules (or [`EcCheck::sample_schedules`] of them) until
//!   they cover it. Each run has a fault phase (the inputs, which may not place a barrier), a
//!   heal, and an optional fault-free recovery phase ([`EcCheck::run_then_recover`]).
//!
//! A label is certified when no untried fault set over the faults the runs revealed is
//! admissible. A sampled search certifies nothing ([`Verdict::Exhausted`]).

#[cfg(stageleft_runtime)]
mod engine;
#[cfg(stageleft_runtime)]
mod results;

#[cfg(stageleft_runtime)]
pub use engine::EcCheck;
#[cfg(stageleft_runtime)]
#[cfg(any(test, feature = "cross-check"))]
pub use engine::{BruteForce, BruteLabel, CrossCheck, Oracle2, Oracle2Breaking, Oracle2Collection};
#[cfg(stageleft_runtime)]
pub use results::{
    ChoiceSite, Counterexample, LabelResult, RaceNotes, Results, Stats, VacuousReason, Verdict,
};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod brute_tests;

#[cfg(test)]
mod oracle2_tests;

#[cfg(test)]
mod fanout_tests;

#[cfg(test)]
mod review2a_tests;

#[cfg(test)]
mod review2b_tests;

#[cfg(test)]
mod review_c_tests;

#[cfg(test)]
mod review3a_tests;
