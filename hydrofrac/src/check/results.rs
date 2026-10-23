//! What [`super::EcCheck::run`] found: one verdict per label, and a readable report.

use std::fmt::{self, Display};
use std::path::PathBuf;
use std::rc::Rc;
use std::time::Duration;

use hydro_lang::location::dynamic::LocationId;
use hydro_lang::sim::network_hook::ChannelInfo;

use crate::fault::{Event, InstanceRecord, MessageId, NodeNames};
use crate::observe::{Fact, LabelPlan, Labels, NotCheckedReason, decode_output, kind_name};
use crate::report::{ReportNames, ReportRow};
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
    /// The program's network channels (the report names channels by them).
    pub channels: Vec<ChannelInfo>,
    /// The labels and their tap output ports (the report decodes observed facts by them).
    pub taps: Labels,
    /// The test that ran the check: the libtest thread name's last segment, or `run`.
    pub test: String,
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

impl Results {
    /// How this report names nodes, channels and payloads: facts the runs observed decode the
    /// payloads that carry them.
    pub fn report_names(&self) -> ReportNames<'_> {
        let mut facts = vec![];
        for l in &self.labels {
            if let Verdict::Counterexample(c) = &l.verdict {
                facts.extend(counterexample_facts(c, &self.taps));
            }
        }
        ReportNames::new(&self.names, &self.channels).with_facts(facts.iter())
    }

    /// One flat row per label, for comparing verdicts with the research records
    /// (docs/design/hydrofrac-lite.md §7).
    #[doc(hidden)]
    pub fn rows(&self) -> Vec<ReportRow> {
        let names = self.report_names();
        self.labels
            .iter()
            .map(|l| {
                let c = &l.plan.collection;
                let (verdict, cex, tolerates) = match &l.verdict {
                    Verdict::Certified { tolerates } => {
                        ("certificate", "-".to_owned(), tolerance(Some(*tolerates)))
                    }
                    Verdict::Counterexample(cex) => (
                        "counterexample",
                        names.normalized(&cex.faults),
                        tolerance(cex.tolerates.map(Tolerance::UpTo)),
                    ),
                    Verdict::Vacuous(_) => ("vacuous", "-".to_owned(), "-".to_owned()),
                    Verdict::Budget { tolerates } => (
                        "exhausted",
                        "-".to_owned(),
                        tolerance(tolerates.map(Tolerance::UpTo)),
                    ),
                    Verdict::NotChecked(_) => ("not-checked", "-".to_owned(), "-".to_owned()),
                    Verdict::CoveredBy(_) => ("covered", "-".to_owned(), "-".to_owned()),
                };
                ReportRow {
                    test: self.test.clone(),
                    index: l.index,
                    label: l.name(),
                    file: c.file.clone().unwrap_or_default(),
                    collection: c.display_name().to_owned(),
                    occurrence: c.occurrence,
                    kind: kind_name(&c.collection_kind).to_owned(),
                    verdict,
                    cex,
                    tolerates,
                    merged_at: l.plan.merged_at,
                    hypotheses: l.stats.hypotheses,
                    schedules: l.stats.schedules,
                    wall_ms: l.stats.wall.as_secs_f64() * 1000.0,
                }
            })
            .collect()
    }
}

fn tolerance(t: Option<Tolerance>) -> String {
    match t {
        Some(Tolerance::All) => "all".to_owned(),
        Some(Tolerance::UpTo(k)) => k.to_string(),
        None => "-".to_owned(),
    }
}

/// The facts a counterexample's report and diagram can decode payloads with: the missing and
/// divergent facts, and every fact the failing schedule's taps observed.
pub(crate) fn counterexample_facts(cex: &Counterexample, taps: &Labels) -> Vec<Fact> {
    let mut facts: Vec<Fact> = cex.missing.iter().map(|(_, f)| f.clone()).collect();
    facts.extend(cex.divergent.iter().cloned());
    for e in cex.record.log.events() {
        if let Event::Output { port, bytes, .. } = e
            && taps.label_of_port(*port).is_some()
        {
            facts.push(decode_output(bytes));
        }
    }
    facts
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

/// The verdict line of a label, naming faults with `names` and other labels with `label`.
pub(crate) fn verdict_line(
    verdict: &Verdict,
    names: &ReportNames<'_>,
    label: impl Fn(usize) -> String,
) -> String {
    match verdict {
        Verdict::Certified {
            tolerates: Tolerance::All,
        } => "holds under any crashes".to_owned(),
        Verdict::Certified {
            tolerates: Tolerance::UpTo(k),
        } => format!("holds with up to {} (the `max_crashes` cap)", crashes(*k)),
        Verdict::Counterexample(c) => {
            let n = c.crashes();
            let mut line = match c.tolerates {
                _ if c.faults.is_empty() => "breaks without faults".to_owned(),
                Some(k) => format!("tolerates {}, breaks at {n}", crashes(k)),
                None if n == 0 => "breaks under omissions alone".to_owned(),
                None => format!("breaks at {}", crashes(n)),
            };
            if !c.faults.is_empty() {
                line.push_str(": ");
                line.push_str(&names.fault_set(&c.faults));
            }
            if let Some(panic) = &c.panic {
                line.push_str(&format!(" (the run panicked: {panic})"));
            }
            line
        }
        Verdict::Vacuous(reason) => format!("vacuous: {reason}"),
        Verdict::Budget { tolerates } => {
            let mut line = "hypothesis budget spent, not a certificate".to_owned();
            if let Some(k) = tolerates {
                line.push_str(&format!(" (holds with up to {})", crashes(*k)));
            }
            line
        }
        Verdict::NotChecked(reason) => format!("not checked: {reason}"),
        Verdict::CoveredBy(j) => format!("covered by [{j}] {}", label(*j)),
    }
}

/// The verdict without source names (faults print by their raw identities); the report
/// ([`Results`]'s `Display`) names them.
impl Display for Verdict {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names = NodeNames::default();
        let names = ReportNames::new(&names, &[]);
        f.write_str(&verdict_line(self, &names, |j| format!("label {j}")))
    }
}

impl Results {
    fn write_label(
        &self,
        f: &mut fmt::Formatter<'_>,
        l: &LabelResult,
        names: &ReportNames<'_>,
        hop: bool,
    ) -> fmt::Result {
        let (indent, detail) = if hop { ("    ↳ ", "        ") } else { ("  ", "      ") };
        let c = &l.plan.collection;
        write!(
            f,
            "{indent}[{}] {} ({}, {})",
            l.index,
            c.display_name(),
            kind_name(&c.collection_kind),
            c.label
        )?;
        if hop {
            write!(f, ", a hop merged at [{}]", l.plan.merged_at.unwrap_or(l.index))?;
        }
        let line = verdict_line(&l.verdict, names, |j| {
            self.labels
                .get(j)
                .map_or_else(|| format!("label {j}"), |o| o.plan.collection.display_name().to_owned())
        });
        writeln!(f, ": {line}")?;
        if let Verdict::Counterexample(cx) = &l.verdict {
            let cluster = self.names.label(l.plan.cluster()).unwrap_or("member");
            for (member, fact) in &cx.missing {
                writeln!(
                    f,
                    "{detail}{cluster}@{member} lacks {}",
                    names.abbreviate(fact.to_string())
                )?;
            }
            for fact in &cx.divergent {
                writeln!(
                    f,
                    "{detail}correct members disagree on {} (outside the fact domain)",
                    names.abbreviate(fact.to_string())
                )?;
            }
            if let Some(diagram) = &cx.diagram {
                writeln!(f, "{detail}diagram: {}", diagram.display())?;
            }
        }
        if l.stats.hypotheses > 0 {
            writeln!(
                f,
                "{detail}{}, {}, {:.2?}",
                plural(l.stats.hypotheses, "hypothesis", "hypotheses"),
                plural(l.stats.schedules, "schedule", "schedules"),
                l.stats.wall
            )?;
        }
        Ok(())
    }
}

/// The report: one block per cluster, one row per label in source names. A merge-dominated
/// hop (a `broadcast_closed` whose contents are merged with another labelled stream before
/// they leave the labelled region) is listed under the label it merges into, whose verdict is
/// the authoritative one; a hop on its own breaks more easily than the merged label.
impl Display for Results {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names = self.report_names();
        let mut clusters: Vec<&LocationId> = vec![];
        for l in &self.labels {
            let cluster = l.plan.cluster().root();
            if !clusters.contains(&cluster) {
                clusters.push(cluster);
            }
        }
        for cluster in clusters {
            let in_cluster: Vec<&LabelResult> = self
                .labels
                .iter()
                .filter(|l| l.plan.cluster().root() == cluster)
                .collect();
            let shown = |j: usize| in_cluster.iter().any(|l| l.index == j);
            match self.names.label(cluster) {
                Some(label) => writeln!(f, "{label}:")?,
                None => writeln!(f, "{cluster:?}:")?,
            }
            for l in &in_cluster {
                if l.plan.merged_at.is_some_and(shown) {
                    continue;
                }
                self.write_label(f, l, &names, false)?;
                for hop in in_cluster
                    .iter()
                    .filter(|h| h.index != l.index && h.plan.merged_at == Some(l.index))
                {
                    self.write_label(f, hop, &names, true)?;
                }
            }
        }
        let legend = names.legend();
        if !legend.is_empty() {
            writeln!(f, "legend:")?;
            for (k, text) in legend {
                writeln!(f, "  ‹{k}› = {text}")?;
            }
        }
        if !self.islands.is_empty() {
            writeln!(
                f,
                "opaque to provenance (execution rule): {}",
                self.islands.join(", ")
            )?;
        }
        let opaque: usize = self.labels.iter().map(|l| l.stats.opaque_fallbacks).sum();
        if opaque > 0 {
            writeln!(
                f,
                "records derived by the execution rule (opaque to provenance): {opaque}"
            )?;
        }
        Ok(())
    }
}
