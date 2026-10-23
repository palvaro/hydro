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
//! [`EcCheck::instrument`] taps every checked label ([`crate::observe`]), turns on the network
//! hook points and skips the program's own consistency assertions. [`EcCheck::run`] then runs
//! the Molly loop for every label:
//!
//! 1. The failure-free probe (hypothesis 0) runs over every schedule. It learns each label's
//!    fact domain ([`crate::spec::learn_domain`]); an empty domain makes the label vacuous.
//!    Its runs are evaluated like any other hypothesis's, so members that disagree with no
//!    faults at all are a counterexample at hypothesis 0.
//! 2. Each label's [`Sweep`](crate::search::Sweep) proposes the minimal admissible fault set
//!    that kills every derivation seen so far, from zero crashes upwards.
//! 3. The set runs over every schedule (or [`EcCheck::sample_schedules`] of them). In each
//!    schedule the body drives inputs; when it returns, the network **heals** (no more
//!    omissions, no new crashes) and the run quiesces.
//! 4. A schedule in which a correct member lacks a required fact is a counterexample. Every
//!    schedule that holds yields a clause: what the hypothesis after it must kill.
//!
//! Runs are shared: a fault set is run once, and every label reads its outcome.
//!
//! Only a label whose fault sets each have a single execution can be certified: lineage pruning
//! assumes a derivation seen in one run recurs whenever its deliveries do, which a choice of the
//! simulator's can break (docs/design/hydrofrac-lite.md §4.9). For a **schedule-sensitive**
//! label ([`crate::observe::LabelPlan::sites`]) step 2 is a heuristic instead: a hypothesis
//! need only kill the derivations of *one* schedule of each earlier run
//! ([`crate::search::Sweep::next_any`]), and a search that runs out of hypotheses ends
//! [`Verdict::Exhausted`], never certified. Counterexamples are counterexamples either way.

#[cfg(stageleft_runtime)]
mod engine;
#[cfg(stageleft_runtime)]
mod results;

#[cfg(stageleft_runtime)]
pub use engine::EcCheck;
#[cfg(stageleft_runtime)]
#[doc(hidden)]
pub use engine::{BruteForce, BruteLabel, CrossCheck};
#[cfg(stageleft_runtime)]
pub use results::{
    Counterexample, LabelResult, NondetSite, Results, Stats, VacuousReason, Verdict,
};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod brute_tests;
