//! The search core of hydrofrac-races (docs/design/hydrofrac-races.md §4.2–4.4, §12), pure and
//! generic over the message type like [`crate::search`]: per-pair clauses, the crash rule and
//! the candidates ([`clause`]), the literals proved constant ([`literals`]), the hitting-set
//! program with a disjunction over pairs ([`ilp`]), and the crash sweep ([`sweep`]).

pub mod clause;
pub mod ilp;
pub mod literals;
pub mod sweep;

pub use clause::{
    AtLeast, Candidate, Dead, PairClause, PairKey, Pairs, Proposal, breaks, candidates, excused,
    faults_of, image, kills, need, proposal_of, pruned, within_bounds,
};
pub use ilp::{Hits, Scope, propose};
pub use literals::{FanOutOrder, Literal, SendOrder, after};
pub use sweep::{Next, Sweep};

// Runtime-only, as `search`'s tests.
#[cfg(all(test, stageleft_runtime))]
mod tests;
