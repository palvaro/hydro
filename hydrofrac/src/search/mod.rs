//! The search (docs/design/hydrofrac-lite.md §4.3–4.5), pure: the loop turns runs into
//! [`Clause`]s and asks a [`Sweep`] what to run next. [`clause`] holds the kill relation,
//! [`ilp`] the hitting-set program, [`sweep`] the crash sweep.

pub mod clause;
pub mod ilp;
pub mod sweep;

pub use clause::{
    Clause, CrashAt, Dependency, Derivation, Fault, FaultSet, Node, PreHealSends, SendSequences,
    completion, crash_count, universe,
};
pub use ilp::{Backend, CheckedCluster, CrashBounds, hitting_set_any, weights};
pub use sweep::{HYPOTHESIS_BUDGET, Next, Sweep, Tolerance};

// Runtime-only: the staged copy of this crate (compiled into simulation dylibs) does not need
// the tests, and drops private impls they rely on.
#[cfg(all(test, stageleft_runtime))]
mod tests;
