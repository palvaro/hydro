//! Which collections are checked, and how their contents reach the host
//! (docs/design/hydrofrac-lite.md §3, §4.1, §4.7).
//!
//! 1. [`ec_collections`] enumerates every collection the program labels
//!    `EventualConsistency`, on the IR as the program built it.
//! 2. [`plan`] decides for each whether the set-based specification applies
//!    ([`NotCheckedReason`]). A labelled scalar is not checked, even when it aggregates a
//!    checked stream: members agreeing on the stream's facts can disagree on a fold of it.
//! 3. The tap pass ([`tap_pass`], a `PreNetwork` IR pass) tees every checked collection, at
//!    every member, into an external output of its own. Each element leaves as
//!    `(bincode(element), Debug of the element if it has one)`; the host sees it through
//!    `SimNetworkHook::on_output`, with the provenance pass's meta attached.
//!
//! The host then evaluates the specification itself ([`crate::spec`]): there is no observer
//! process in the program.

// Everything here is host-side (it reads the IR and the host's observations), so it is kept out of
// the staged copy of the crate that simulations link. The module itself stays, so that the staged
// closures of its tests resolve (`crate::__staged::observe::tests`).
#[cfg(stageleft_runtime)]
mod collections;
#[cfg(stageleft_runtime)]
mod plan;
#[cfg(stageleft_runtime)]
pub mod source_names;
#[cfg(stageleft_runtime)]
mod tap;

#[cfg(stageleft_runtime)]
pub(crate) use collections::{ec_label, tick_of};
#[cfg(stageleft_runtime)]
pub use collections::{EcCollection, ec_collections, kind_name};
#[cfg(stageleft_runtime)]
pub use plan::{LabelPlan, NotCheckedReason, plan};
#[cfg(stageleft_runtime)]
pub use tap::{Fact, Labels, NO_IDENTITY, TapPass, decode_output, tap, tap_pass};

#[cfg(test)]
mod tests;
