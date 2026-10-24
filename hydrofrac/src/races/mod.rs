//! The search core of hydrofrac-races (docs/design/hydrofrac-races.md §4.2–4.4), pure and
//! generic over the message type like [`crate::search`]: per-pair clauses and the crash rule
//! that does not depend on send order ([`clause`]), the hitting-set program with a disjunction
//! over pairs ([`ilp`]), the crash sweep ([`sweep`]), and the enumeration fallback for labels
//! the program cannot finish ([`enumerate`]).

pub mod clause;
pub mod enumerate;
pub mod ilp;
pub mod sweep;

pub use clause::{
    Candidate, Dead, PairClause, PairKey, Pairs, SendOrder, admissible_for, candidates, excused,
    kills, pruned, stuck_pairs, unconstrained_pairs, universe,
};
pub use enumerate::{Enumerator, within_bounds};
pub use ilp::{Scope, propose};
pub use sweep::{Next, Sweep};

// Runtime-only, as `search`'s tests.
#[cfg(all(test, stageleft_runtime))]
mod tests;
