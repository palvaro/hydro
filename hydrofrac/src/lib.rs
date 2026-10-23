//! hydrofrac: lineage-driven fault injection for Hydro programs, in the simulator.
//!
//! For every collection a program labels `EventualConsistency`, hydrofrac shows that under the
//! fault model its transports declare every correct member ends with the same facts, or it
//! produces a counterexample and its Lamport diagram. The search follows Molly: run the program,
//! read off how each observed fact was derived, propose a minimal set of faults that kills
//! every derivation seen so far, and repeat until no such set exists (a certificate) or one
//! breaks the label (a counterexample). The design, its conventions and the evidence behind
//! them are in `docs/design/hydrofrac-lite.md`.

#[cfg(stageleft_runtime)]
hydro_lang::setup!();

pub mod fault;
pub mod lamport;
pub mod lineage;
pub mod observe;
pub mod provenance;
pub mod report;
#[cfg(stageleft_runtime)]
pub mod search;
// Host-side only, so kept out of the staged copy of the crate that simulations link (`observe`
// gates its items itself, so that its tests' staged closures still resolve).
#[cfg(stageleft_runtime)]
pub mod spec;
