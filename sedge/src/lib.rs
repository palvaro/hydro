//! Sedge, the overload harness: does a deployed Hydro program recover from a burst of load,
//! and if not, what does its collapse look like?
//!
//! Cress (the schedule-amplification checker) finds, in the schedule domain, the mechanism by
//! which a program does extra work in response to delay, and measures how much: an attempt cap
//! `A` gives a plateau of `(A − 1)·N` extra sends for `N` requests, a clock-refilled budget gives a
//! rate `b` of extra sends per round. Sedge is the time-domain half of the same question. It
//! runs the program as deployed, with real service times and wall-clock timeouts, offers it an
//! open-loop Poisson workload that follows a baseline → trigger → baseline profile, and reads the
//! tail of the run against the pre-trigger baseline to say whether the program recovered, is still
//! draining, or collapsed. Because arrivals are stochastic, the interesting output over many
//! trials is a collapse *probability* as a function of the overprovisioning factor `μ/λ`, which is
//! what the Cress multiplier claims to predict (`μ/λ > A` for plateau shapes, `μ > λ + b` for
//! rate shapes).
//!
//! # How a program is checked
//!
//! The attribute [`overload_check`] on a Hydro function generates the harness from the
//! function's signature, the same way `cress::amplification_check` does for the simulator:
//! `Process` parameters become deployed processes, `Cluster` parameters become deployed clusters
//! of the size the attribute gives (`cluster = 5`), the stream named `load = ...` becomes an
//! external port the generator feeds with sequence numbers (see [`LoadItem`]), `()` streams
//! become timers with the periods given in `timers(...)`, plain parameters take the values in
//! the attribute. Streams on clusters pass through one extra relay process (see [`SedgeRelay`]),
//! since a deployment has no channel between an external process and a cluster. The function's return value is handed to [`Observed::observe`], a trait the
//! program's crate implements by hand to say which output carries completions, which carries
//! abandonments, and which levels and counters to watch. `./scripts/sedge <crate>` at the
//! repository root builds a runner that links the crate, deploys every annotated function on
//! localhost, runs the trials, and prints a verdict per trial and a summary per factor. Traces
//! and a results table are written under `target/sedge/`.
//!
//! # What this crate contains
//!
//! - [`program`]: what the program under test and the generated harness share: [`LoadItem`],
//!   [`timer`], [`load_input`], [`Observed`] and [`Observer`]. These hold the harness's `q!`
//!   quotes, which must live in a crate's own source.
//! - [`profile`]: a [`Profile`] of offered load over time, and a Poisson [`Arrivals`] sampler.
//! - [`trace`]: the [`Recorder`] the harness feeds with offers, completions, abandonments and
//!   program-reported gauges and counters, bucketed per second into a [`Trace`].
//! - [`generator`]: [`drive`], the open-loop load generator loop.
//! - [`verdict`]: [`judge`], the verdict read from a trace, with the numbers it was read from.
//! - [`harness`]: the run configuration, the deployment of one trial, and the registry.
//!
//! # Open loop
//!
//! The generator never waits for a response before offering the next request. A closed-loop
//! generator slows down when latency rises and so hides exactly the condition the harness exists
//! to produce. Arrivals are Poisson at the profile's current rate, so trials differ from one
//! another; run several per factor.

#[cfg(stageleft_runtime)]
hydro_lang::setup!();

#[cfg(stageleft_runtime)]
pub mod capacity;
pub mod generator;
pub mod profile;
pub mod program;
pub mod trace;
pub mod verdict;

#[cfg(stageleft_runtime)]
#[doc(hidden)]
pub mod harness;

pub use generator::drive;
pub use profile::{Arrivals, Profile};
pub use program::{cluster_timer, timer};
#[cfg(stageleft_runtime)]
pub use program::{
    LoadItem, Observable, Observation, Observed, Observer, cluster_load_input, load_input,
};
pub use program::{ClusterRoundSender, RoundSender, SedgeRelay};
pub use trace::{Bucket, Recorder, Sample, Trace};
pub use verdict::{
    Outcome, StreamKind, StreamKinds, Verdict, judge, judge_with_kinds, level_rising,
};

/// Paths the generated harness code reaches through this crate, so that a crate under check
/// need not depend on `hydro_lang` or `hydro_deploy` itself.
#[doc(hidden)]
pub mod __private {
    pub use futures;
    pub use hydro_deploy;
    pub use hydro_lang;
}

/// Registers a Hydro function with the `sedge` command. See the
/// [macro's documentation](sedge_macro::overload_check).
pub use sedge_macro::overload_check;
/// Implements [`Observed`] for a struct of output streams by registering every field whole with
/// [`Observer::stream`]. See the [macro's documentation](sedge_macro::SedgeOutputs).
pub use sedge_macro::SedgeOutputs;
