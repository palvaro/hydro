//! Visualizations of what the LDFI search found, after Molly's two zoom levels for a
//! counterexample (Alvaro, Rosen, Hellerstein, SIGMOD 2015):
//!
//! 1. **The Lamport diagram of the failing run**: every node, every message, with the
//!    omissions and crashes the hypothesis injected marked ([`lamport`]).
//! 2. **The same picture of a run in which the goal held, with the goal's support
//!    highlighted**: the messages whose loss the search learned the goal depends on
//!    ([`Trace::support`] of a [`Derivation`]). Hydrofrac computes that support at one of
//!    four lineage levels ([`Level`](crate::sim::ldfi::Level)), from the whole execution
//!    graph down to a 1-minimal counterfactual set; the finer the level, the tighter the
//!    highlight.
//!
//! 3. **The why-provenance graph of the goal** ([`provenance`]): the DAG the backward
//!    closure walked from the goal, through executions and messages, down to the deliveries
//!    whose loss would break it, with every edge tagged by the lineage level that produced it
//!    and the places where a finer level fell back to a coarser one shown explicitly. This is
//!    the zoom level Molly drew as a derivation tree; hydrofrac's four levels give it more
//!    rungs. Recorded under
//!    [`Ldfi::with_provenance_graphs`](crate::sim::ldfi::Ldfi::with_provenance_graphs).
//!
//! The first two are drawn from a [`Trace`], which the search retains on every failing
//! [`Schedule`] and, under [`Ldfi::with_traces`](crate::sim::ldfi::Ldfi::with_traces), on
//! every schedule. [`Counterexample`] bundles the pictures for an [`Outcome`].

pub mod lamport;
pub mod provenance;
pub mod trace;

use std::fmt::Write as _;

use self::lamport::LamportOptions;
pub use self::trace::Trace;
use crate::sim::ldfi::{Derivation, FaultSet, Hypothesis, Outcome, Schedule};

/// A counterexample's pictures: the failing run, and a witness run in which the goal held.
#[derive(Clone, Copy, Debug)]
pub struct Counterexample<'a> {
    /// The faults that broke the goal.
    pub faults: &'a FaultSet,
    /// The failing schedule.
    pub schedule: &'a Schedule,
    /// Every hypothesis run, in order (the last one is the counterexample).
    pub runs: &'a [Hypothesis],
}

/// A schedule in which the goal held, and one of its derivations.
#[derive(Clone, Copy, Debug)]
pub struct Witness<'a> {
    /// The hypothesis the schedule was explored under.
    pub hypothesis: &'a Hypothesis,
    /// The schedule.
    pub schedule: &'a Schedule,
    /// The goal's derivation to show (the schedule's first).
    pub derivation: &'a Derivation,
}

impl Outcome {
    /// The counterexample's pictures, if the search found a violation.
    pub fn viz(&self) -> Option<Counterexample<'_>> {
        match self {
            Outcome::Counterexample {
                faults,
                schedule,
                runs,
            } => Some(Counterexample {
                faults,
                schedule,
                runs,
            }),
            _ => None,
        }
    }
}

impl<'a> Counterexample<'a> {
    /// The failing run's trace (always retained for a failing schedule).
    pub fn failing_trace(&self) -> Option<&'a Trace> {
        self.schedule.trace.as_deref()
    }

    /// The witness: the last explored schedule in which the goal held with a non-empty
    /// derivation and a retained trace. `None` unless the search ran under
    /// [`Ldfi::with_traces`](crate::sim::ldfi::Ldfi::with_traces) (only failing schedules
    /// keep their trace otherwise).
    pub fn witness(&self) -> Option<Witness<'a>> {
        self.runs.iter().rev().find_map(|hypothesis| {
            hypothesis.schedules.iter().rev().find_map(|schedule| {
                let derivation = schedule.derivations.iter().find(|d| !d.is_empty())?;
                (schedule.goal_held && schedule.trace.is_some()).then_some(Witness {
                    hypothesis,
                    schedule,
                    derivation,
                })
            })
        })
    }

    fn title(&self) -> String {
        let faults: Vec<String> = self.faults.iter().map(ToString::to_string).collect();
        format!("goal violated by {{{}}}", faults.join(", "))
    }

    /// Rendering options for the failing run.
    pub fn lamport_options(&self) -> LamportOptions {
        LamportOptions::default().title(self.title())
    }

    /// Rendering options for the witness: its derivation highlighted.
    pub fn support_options(&self) -> Option<LamportOptions> {
        let witness = self.witness()?;
        let trace = witness.schedule.trace.as_deref()?;
        let faults: Vec<String> = witness
            .hypothesis
            .faults
            .iter()
            .map(ToString::to_string)
            .collect();
        Some(
            LamportOptions::default()
                .highlight(trace.support(witness.derivation))
                .title(format!(
                    "goal held under {{{}}}: its support (★)",
                    faults.join(", ")
                )),
        )
    }

    /// Zoom level 0: the failing run as a Mermaid sequence diagram.
    pub fn lamport_mermaid(&self) -> Option<String> {
        Some(lamport::mermaid(
            self.failing_trace()?,
            &self.lamport_options(),
        ))
    }

    /// Zoom level 0: the failing run as a Graphviz DOT spacetime diagram.
    pub fn lamport_dot(&self) -> Option<String> {
        Some(lamport::dot(self.failing_trace()?, &self.lamport_options()))
    }

    /// Zoom level 1: the witness run with the goal's support highlighted, as a Mermaid
    /// sequence diagram. `None` without a witness (see [`Self::witness`]).
    pub fn support_lamport_mermaid(&self) -> Option<String> {
        let witness = self.witness()?;
        Some(lamport::mermaid(
            witness.schedule.trace.as_deref()?,
            &self.support_options()?,
        ))
    }

    /// Zoom level 1 as Graphviz DOT.
    pub fn support_lamport_dot(&self) -> Option<String> {
        let witness = self.witness()?;
        Some(lamport::dot(
            witness.schedule.trace.as_deref()?,
            &self.support_options()?,
        ))
    }

    /// Both zoom levels as a Markdown report with embedded Mermaid diagrams (as GitHub and
    /// most documentation sites render them).
    pub fn markdown(&self) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "# {}\n", self.title());
        let _ = writeln!(
            out,
            "Found after {} hypothesis run(s), {} schedule(s) explored.\n",
            self.runs.len(),
            self.runs.iter().map(|h| h.schedules.len()).sum::<usize>()
        );
        let _ = writeln!(out, "## The failing run\n");
        match self.lamport_mermaid() {
            Some(diagram) => {
                let _ = writeln!(out, "```mermaid\n{diagram}```\n");
            }
            None => {
                let _ = writeln!(out, "_(no trace retained)_\n");
            }
        }
        let _ = writeln!(out, "## Why the goal held before\n");
        match (self.witness(), self.support_lamport_mermaid()) {
            (Some(witness), Some(diagram)) => {
                let deps: Vec<String> =
                    witness.derivation.iter().map(ToString::to_string).collect();
                let _ = writeln!(
                    out,
                    "The goal's support in the last run in which it held: {{{}}}\n",
                    deps.join(", ")
                );
                let _ = writeln!(out, "```mermaid\n{diagram}```\n");
            }
            _ => {
                let _ = writeln!(
                    out,
                    "_(no witness trace retained; run the search with `Ldfi::with_traces()`)_\n"
                );
            }
        }
        out
    }
}
