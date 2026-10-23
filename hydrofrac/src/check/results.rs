//! What [`super::EcCheck::run`] found: one verdict per label, and a readable report.

use std::fmt::{self, Display};
use std::path::PathBuf;
use std::rc::Rc;
use std::time::Duration;

use hydro_lang::location::dynamic::LocationId;

use crate::fault::{InstanceRecord, MessageId, NodeNames};
use crate::observe::{Fact, LabelPlan, NotCheckedReason};
use crate::search::{FaultSet, Tolerance, crash_count};

/// Why a label has nothing to check.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VacuousReason {
    /// The failure-free probe held no facts at any correct member: nothing to agree on.
    EmptyDomain,
    /// The label lives on a single node (a process, or a cluster of one member), which always
    /// agrees with itself.
    SingleMember,
}

impl Display for VacuousReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VacuousReason::EmptyDomain => write!(f, "the failure-free run holds no facts"),
            VacuousReason::SingleMember => write!(f, "a single member agrees with itself"),
        }
    }
}

/// A run that breaks a label.
#[derive(Clone, Debug)]
pub struct Counterexample {
    /// The fault set (the hypothesis) the run was under. Empty for a failure-free run that
    /// already breaks the label.
    pub faults: FaultSet<MessageId>,
    /// The largest crash bound the label was certified for before this counterexample: it
    /// tolerates every fault set with at most this many crashes. `None` when nothing was
    /// certified, not even omissions alone.
    pub tolerates: Option<usize>,
    /// The index of the hypothesis in the label's search (0 is the failure-free probe).
    pub hypothesis: usize,
    /// The `(correct member, fact)` pairs the run did not deliver.
    pub missing: Vec<(u32, Fact)>,
    /// Facts outside the fact domain that some correct members hold and others do not: a fault
    /// made members derive something the failure-free run never did, and they disagree on it.
    pub divergent: Vec<Fact>,
    /// The panic message, if the run panicked (a program that panics under injected faults is
    /// a counterexample).
    pub panic: Option<String>,
    /// The failing schedule: its event log and crashed nodes (for the Lamport diagram).
    pub record: Rc<InstanceRecord>,
    /// Where the failing schedule's Lamport diagram was written (the `.svg` if Graphviz
    /// rendered it, else the `.dot`), under `target/hydrofrac/<test>/<label>/`. `None` if
    /// writing it failed.
    pub diagram: Option<PathBuf>,
}

impl Counterexample {
    /// The number of crashes in [`Self::faults`].
    pub fn crashes(&self) -> usize {
        crash_count(&self.faults)
    }
}

/// The verdict on one label.
#[derive(Clone, Debug)]
pub enum Verdict {
    /// No admissible fault set breaks the label.
    Certified {
        /// The crashes it tolerates.
        tolerates: Tolerance,
    },
    /// A fault set that breaks the label.
    Counterexample(Box<Counterexample>),
    /// Nothing to check.
    Vacuous(VacuousReason),
    /// The hypothesis budget ran out: not a certificate.
    Budget {
        /// The largest crash bound certified before the budget ran out, if any.
        tolerates: Option<usize>,
    },
    /// The generated specification does not apply to the collection.
    NotChecked(NotCheckedReason),
    /// A labelled scalar checked at the labelled stream it aggregates: the label with this
    /// index, whose verdict is this one's.
    CoveredBy(usize),
}

impl Verdict {
    /// Whether the verdict is a counterexample or an exhausted budget (what
    /// [`Results::assert_holds`] rejects).
    pub fn fails(&self) -> bool {
        matches!(self, Verdict::Counterexample(_) | Verdict::Budget { .. })
    }

    /// The counterexample, if this is one.
    pub fn counterexample(&self) -> Option<&Counterexample> {
        match self {
            Verdict::Counterexample(c) => Some(c),
            _ => None,
        }
    }
}

/// What one label's search cost.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    /// Hypotheses tried, counting the failure-free probe.
    pub hypotheses: usize,
    /// Schedules explored over those hypotheses (a run shared with another label counts for
    /// both).
    pub schedules: usize,
    /// Wall time of the search, counting the runs it caused (not those it reused).
    pub wall: Duration,
    /// Records whose derivation fell back to the execution rule (opaque to the provenance
    /// rewrite), summed over the runs.
    pub opaque_fallbacks: usize,
}

/// The result for one labelled collection.
#[derive(Clone, Debug)]
pub struct LabelResult {
    /// The label's index ([`crate::observe::Labels::plans`]).
    pub index: usize,
    /// The labelled collection and how it is checked.
    pub plan: LabelPlan,
    /// The verdict.
    pub verdict: Verdict,
    /// What the search cost (zero for labels that were not searched).
    pub stats: Stats,
}

impl LabelResult {
    /// The label's display name: its binding (or operator), file and occurrence.
    pub fn name(&self) -> String {
        label_name(&self.plan)
    }
}

/// The display name of the label `plan` checks: its binding (or operator), file and
/// occurrence.
pub(crate) fn label_name(plan: &LabelPlan) -> String {
    let c = &plan.collection;
    let mut name = c.display_name().to_owned();
    if let Some(file) = &c.file {
        name.push_str(&format!(" ({file}"));
        if c.occurrence > 0 {
            name.push_str(&format!(" #{}", c.occurrence));
        }
        name.push(')');
    }
    name
}

/// The verdicts on every label of the program.
#[derive(Clone, Debug)]
pub struct Results {
    /// One result per labelled collection, in label order.
    pub labels: Vec<LabelResult>,
    /// The program's node names (counterexamples name nodes by them).
    pub names: NodeNames,
    /// The operators the provenance rewrite could not see into (opaque islands), by IR node
    /// name. Records computed through them use the execution rule: sound, but coarser.
    /// Empty when the whole program is tracked precisely.
    pub islands: Vec<String>,
}

impl Results {
    /// The results.
    pub fn results(&self) -> &[LabelResult] {
        &self.labels
    }

    /// The results of every label whose display name (binding or operator) is `name`.
    pub fn labels_named<'r>(&'r self, name: &'r str) -> impl Iterator<Item = &'r LabelResult> {
        self.labels
            .iter()
            .filter(move |l| l.plan.collection.display_name() == name)
    }

    /// The result of the label whose display name (binding or operator) is `name`.
    ///
    /// # Panics
    /// If no label, or more than one, has that name.
    pub fn label(&self, name: &str) -> &LabelResult {
        let mut found = self
            .labels
            .iter()
            .filter(|l| l.plan.collection.display_name() == name);
        let first = found
            .next()
            .unwrap_or_else(|| panic!("no label `{name}` in\n{self}"));
        assert!(found.next().is_none(), "several labels named `{name}`");
        first
    }

    /// Whether every label holds: no counterexample and no exhausted budget.
    pub fn holds(&self) -> bool {
        !self.labels.iter().any(|l| l.verdict.fails())
    }

    /// Panics with the report unless every label holds (see [`Self::holds`]).
    #[track_caller]
    pub fn assert_holds(&self) {
        assert!(self.holds(), "hydrofrac: a label does not hold\n{self}");
    }
}

fn plural(n: usize, one: &str, many: &str) -> String {
    if n == 1 {
        format!("{n} {one}")
    } else {
        format!("{n} {many}")
    }
}

fn crashes(n: usize) -> String {
    plural(n, "crash", "crashes")
}

fn fault_set(set: &FaultSet<MessageId>) -> String {
    let faults: Vec<String> = set.iter().map(ToString::to_string).collect();
    format!("{{{}}}", faults.join(", "))
}

impl Display for Verdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Verdict::Certified {
                tolerates: Tolerance::All,
            } => write!(f, "holds under every admissible fault set"),
            Verdict::Certified {
                tolerates: Tolerance::UpTo(k),
            } => write!(
                f,
                "holds with up to {} (the `max_crashes` cap)",
                crashes(*k)
            ),
            Verdict::Counterexample(c) => {
                match c.tolerates {
                    Some(k) => write!(
                        f,
                        "tolerates {}, breaks with {}: ",
                        crashes(k),
                        crashes(c.crashes())
                    )?,
                    None if c.faults.is_empty() => write!(f, "breaks without faults: ")?,
                    None => write!(f, "breaks without crashes: ")?,
                }
                write!(f, "{}", fault_set(&c.faults))?;
                if let Some(panic) = &c.panic {
                    write!(f, " (the run panicked: {panic})")?;
                }
                Ok(())
            }
            Verdict::Vacuous(reason) => write!(f, "vacuous: {reason}"),
            Verdict::Budget { tolerates } => {
                write!(f, "hypothesis budget spent, not a certificate")?;
                if let Some(k) = tolerates {
                    write!(f, " (holds with up to {})", crashes(*k))?;
                }
                Ok(())
            }
            Verdict::NotChecked(reason) => write!(f, "not checked: {reason}"),
            Verdict::CoveredBy(label) => write!(f, "checked at label {label}"),
        }
    }
}

impl Display for Results {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut clusters: Vec<&LocationId> = vec![];
        for l in &self.labels {
            let cluster = l.plan.cluster().root();
            if !clusters.contains(&cluster) {
                clusters.push(cluster);
            }
        }
        for cluster in clusters {
            match self.names.label(cluster) {
                Some(label) => writeln!(f, "{label}:")?,
                None => writeln!(f, "{cluster:?}:")?,
            }
            for l in self
                .labels
                .iter()
                .filter(|l| l.plan.cluster().root() == cluster)
            {
                write!(f, "  [{}] {}: {}", l.index, l.name(), l.verdict)?;
                if l.stats.hypotheses > 0 {
                    write!(
                        f,
                        " [{}, {}, {:.2?}]",
                        plural(l.stats.hypotheses, "hypothesis", "hypotheses"),
                        plural(l.stats.schedules, "schedule", "schedules"),
                        l.stats.wall
                    )?;
                }
                writeln!(f)?;
                if let Verdict::Counterexample(c) = &l.verdict {
                    for (member, fact) in &c.missing {
                        writeln!(f, "      member {member} lacks {fact}")?;
                    }
                    for fact in &c.divergent {
                        writeln!(
                            f,
                            "      correct members disagree on {fact} (outside the fact domain)"
                        )?;
                    }
                    if let Some(diagram) = &c.diagram {
                        writeln!(f, "      diagram: {}", diagram.display())?;
                    }
                }
            }
        }
        if !self.islands.is_empty() {
            writeln!(
                f,
                "opaque to provenance (execution rule): {}",
                self.islands.join(", ")
            )?;
        }
        Ok(())
    }
}
