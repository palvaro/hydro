//! The search: minimal hitting sets of the derivations seen so far, swept across crash bounds.
//!
//! This module is pure. It knows nothing of the simulator: the loop runs hypotheses, turns their
//! observations into [`Clause`]s, and asks a [`Sweep`] what to run next.
//!
//! - [`clause`]: faults, derivations, and the positional kill relation.
//! - [`ilp`]: the hitting-set integer program, on `microlp` (or HiGHS, behind `highs`).
//! - [`sweep`]: one label's search from zero crashes upwards.

pub mod clause;
pub mod ilp;
pub mod sweep;

pub use clause::{
    Clause, CrashAt, Dependency, Derivation, Fault, FaultSet, Node, PreHealSends, SendSequences,
    crash_count, universe,
};
pub use ilp::{Backend, CheckedCluster, CrashBounds, hitting_set, weights};
pub use sweep::{HYPOTHESIS_BUDGET, Next, Sweep, Tolerance};

#[cfg(test)]
mod tests;
