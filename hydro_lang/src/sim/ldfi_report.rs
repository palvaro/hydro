//! The readable rendering of [`LabelResults`]: one block per cluster, one row per labelled
//! collection, named by the user's own names, with verdicts and faults in plain words.
//!
//! ```text
//! acceptors (3 members)
//!   prepares     multi_paxos.rs:281  DIVERGES under crash(proposer@0 after sending prepares Ts{round: 1, writer: 0} to acceptor 1)
//!
//! learners (3 members)
//!   learned           :588                 no violation found (25 fault sets, 8 sampled schedules each)
//!   facts             :608                 no violation found (32 fault sets, 8 sampled schedules each)
//!   splice_epoch_log  epoch_splice.rs:190  covered by facts: no violation found
//!   hops merged into learned:
//!     initial  :582  diverges alone under crash(proposer@0 after sending initial (1, 0, 0, 7) to learner 1)
//!     echo     :591  diverges alone under crash(learner@0 after sending echo (1, 0, 0, 7) to learner 1)
//!
//! summary: 5 hold, 1 DIVERGE (acceptors/prepares)
//! ```
//!
//! Within a cluster, collections searched in their own right come first, then covered scalars,
//! then those not checked, then merge-dominated hops under the exit they merge into. A site is
//! `file:line` at its file's first mention and `:line` after.
//!
//! Names come from [`ReportNames`], collected by [`Ldfi::search_all_labels`] from the
//! compiled program and the test's registrations:
//!
//! - a **cluster** is named by its `crashable_cluster` label, pluralized (`learner` →
//!   `learners`), else by its Rust type (`Learner cluster`), else by its location id
//!   (`cluster loc3v1`); a member is `<label> <id>` (`learner 1`);
//! - a **collection** by its tag, else the variable its statement binds or the function whose
//!   tail it is ([`EcCollection::name`]), else its operator;
//! - a **channel** (in faults) by its `.name(..)`, else the variable its `send` statement
//!   binds ([`ChannelInfo::name`]), else its site.
//!
//! Payloads are shortened ([`short_payload`]). The alternate form (`{:#}`), or the
//! `HYDROFRAC_VERBOSE` environment variable, adds each collection's exact identity (site,
//! operator, role, location) and the exact fault notation.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fmt::{self, Display, Write as _};

use super::{
    Exploration, Fault, FaultSet, LabelResult, LabelResults, LabelVerdict, Message, NodeKey,
    Outcome, render_set,
};
use crate::location::dynamic::LocationId;
use crate::sim::compiled::CompiledSim;
use crate::sim::slice::EcCollection;

/// How the report names locations and channels.
#[derive(Clone, Debug, Default)]
pub struct ReportNames {
    locations: BTreeMap<LocationId, LocationName>,
    channels: BTreeMap<usize, ChannelName>,
}

#[derive(Clone, Debug)]
struct LocationName {
    /// One node: `learner` (followed by a member id), `leader`, `Learner`, `cluster loc3v1`.
    singular: String,
    /// The block heading: `learners`, `Learner cluster`, `cluster loc3v1`.
    group: String,
    /// The number of members, for a cluster.
    members: Option<usize>,
}

#[derive(Clone, Debug)]
struct ChannelName {
    from: LocationId,
    to: LocationId,
    /// The user's name, else the channel's label.
    name: String,
}

impl ReportNames {
    /// The names of `sim`'s locations and channels, with the crash labels `nodes` registers.
    pub(super) fn new(sim: &CompiledSim, nodes: &BTreeMap<String, NodeKey>) -> Self {
        let mut locations = BTreeMap::new();
        let mut crash_labels: BTreeMap<&LocationId, &str> = BTreeMap::new();
        for (label, (location, member)) in nodes {
            let label = match member {
                Some(_) => label.rsplit_once('@').map_or(label.as_str(), |(l, _)| l),
                None => label.as_str(),
            };
            crash_labels.entry(location).or_insert(label);
        }
        let mut all: BTreeSet<LocationId> = sim.cluster_members.keys().cloned().collect();
        all.extend(
            sim.slices
                .channels
                .values()
                .flat_map(|c| [c.from.clone(), c.to.clone()]),
        );
        all.extend(nodes.values().map(|(location, _)| location.clone()));
        for location in all {
            let cluster = matches!(location, LocationId::Cluster(_));
            let key = match &location {
                LocationId::Process(key) | LocationId::Cluster(key) => key.to_string(),
                _ => continue,
            };
            let crash_label = crash_labels.get(&location).copied();
            let type_name = sim.location_name(&location);
            let (singular, group) = match (crash_label, type_name, cluster) {
                (Some(label), _, true) => (label.to_owned(), plural(label)),
                (Some(label), _, false) => (label.to_owned(), label.to_owned()),
                (None, Some(name), true) => (name.to_owned(), format!("{name} cluster")),
                (None, Some(name), false) => (name.to_owned(), name.to_owned()),
                (None, None, true) => (format!("cluster {key}"), format!("cluster {key}")),
                (None, None, false) => (format!("process {key}"), format!("process {key}")),
            };
            let members = cluster
                .then(|| sim.cluster_members(&location).map(<[u32]>::len))
                .flatten();
            locations.insert(
                location,
                LocationName {
                    singular,
                    group,
                    members,
                },
            );
        }
        let channels = sim
            .slices
            .channels
            .iter()
            .map(|(id, c)| {
                (
                    *id,
                    ChannelName {
                        from: c.from.clone(),
                        to: c.to.clone(),
                        name: c.name.clone().unwrap_or_else(|| c.label.clone()),
                    },
                )
            })
            .collect();
        ReportNames {
            locations,
            channels,
        }
    }

    /// A location's block heading (`learners`).
    pub(super) fn group(&self, location: &LocationId) -> String {
        self.locations
            .get(location)
            .map(|l| l.group.clone())
            .unwrap_or_else(|| format!("{location:?}"))
    }

    fn members(&self, location: &LocationId) -> Option<usize> {
        self.locations.get(location).and_then(|l| l.members)
    }

    /// A node: `learner 1` for a cluster member, `leader` for a process.
    fn node(&self, location: &LocationId, member: Option<u32>) -> String {
        let name = self
            .locations
            .get(location)
            .map(|l| l.singular.clone())
            .unwrap_or_else(|| format!("{location:?}"));
        match member {
            Some(m) => format!("{name} {m}"),
            None => name,
        }
    }

    /// `name payload` for a message on `channel`.
    fn message(&self, channel: usize, payload: &str) -> String {
        let payload = short_payload(payload);
        match self.channels.get(&channel) {
            Some(c) => format!("{} {payload}", c.name),
            None => payload,
        }
    }

    /// A fault in plain words.
    pub(super) fn fault(&self, fault: &Fault) -> String {
        match fault {
            Fault::Crash { node, after: None } => format!("crash({node} before sending anything)"),
            Fault::Crash {
                node,
                after: Some(send),
            } => {
                let message = self.message(send.channel, &send.payload);
                match self.channels.get(&send.channel) {
                    Some(c) => format!(
                        "crash({node} after sending {message} to {})",
                        self.node(&c.to, send.to_member)
                    ),
                    None => format!("crash({node} after sending {message})"),
                }
            }
            Fault::Omit(m) => self.omit(m),
        }
    }

    fn omit(&self, m: &Message) -> String {
        let message = self.message(m.channel_id, &m.display);
        match self.channels.get(&m.channel_id) {
            Some(c) => format!(
                "omit({message} {}→{})",
                self.node(&c.from, m.from_member),
                self.node(&c.to, m.to_member)
            ),
            None => format!("omit({message})"),
        }
    }

    /// A fault set in plain words, `+`-separated (in exact notation when no names are known).
    pub(super) fn faults(&self, faults: &FaultSet) -> String {
        if faults.is_empty() {
            return "no faults at all".to_owned();
        }
        faults
            .iter()
            .map(|f| {
                if self.channels.is_empty() {
                    f.to_string()
                } else {
                    self.fault(f)
                }
            })
            .collect::<Vec<_>>()
            .join(" + ")
    }
}

/// A naive English plural, for a cluster's heading (`learner` → `learners`).
fn plural(word: &str) -> String {
    if word.ends_with('s') || word.ends_with('x') {
        word.to_owned()
    } else {
        format!("{word}s")
    }
}

fn count(n: usize, one: &str) -> String {
    if n == 1 {
        format!("1 {one}")
    } else {
        format!("{n} {}", plural(one))
    }
}

/// An outcome as a verdict phrase. `brief` drops the counts (for a covered scalar, whose
/// input's row has them).
fn verdict(names: &ReportNames, outcome: &Outcome, brief: bool) -> String {
    match outcome {
        Outcome::Certificate {
            runs, exploration, ..
        } => {
            let (word, detail) = match exploration {
                Exploration::Exhaustive => (
                    "certified",
                    format!("{}, all schedules", count(runs.len(), "fault set")),
                ),
                Exploration::Fuzz { instances } => (
                    "no violation found",
                    format!(
                        "{}, {} each",
                        count(runs.len(), "fault set"),
                        count(*instances, "sampled schedule")
                    ),
                ),
            };
            if brief {
                word.to_owned()
            } else {
                format!("{word} ({detail})")
            }
        }
        Outcome::Counterexample { faults, .. } => {
            format!("DIVERGES under {}", names.faults(faults))
        }
        Outcome::Budget { runs } => format!(
            "UNDECIDED: the hypothesis budget ran out after {}",
            count(runs.len(), "fault set")
        ),
        Outcome::Vacuous { .. } => "VACUOUS: the scenario's precondition never held without \
                                    faults, so nothing was checked"
            .to_owned(),
        Outcome::Exhausted { runs } => {
            if brief {
                "no violation found".to_owned()
            } else {
                format!(
                    "no violation found (every admissible fault set tried, {})",
                    count(runs.len(), "run")
                )
            }
        }
    }
}

/// One result's verdict phrase, without the report's names (see [`LabelResult`]'s `Display`).
pub(super) fn single_verdict(result: &LabelResult) -> String {
    let names = ReportNames::default();
    match &result.verdict {
        LabelVerdict::Searched(outcome) => verdict(&names, outcome, false),
        LabelVerdict::CoveredBy {
            input_site,
            outcome,
        } => format!(
            "covered by its input at {input_site}: {}",
            verdict(&names, outcome, false)
        ),
        LabelVerdict::Hop { merged_at, outcome } => {
            format!(
                "merged into {merged_at}, which decides; {}",
                hop_verdict(&names, outcome)
            )
        }
        LabelVerdict::NotChecked(reason) => format!("not checked ({reason})"),
    }
}

/// A hop's own outcome, as a phrase about the hop alone.
fn hop_verdict(names: &ReportNames, outcome: &Outcome) -> String {
    match outcome {
        Outcome::Counterexample { faults, .. } => {
            format!("diverges alone under {}", names.faults(faults))
        }
        Outcome::Certificate { .. } | Outcome::Exhausted { .. } => "agrees alone".to_owned(),
        other => verdict(names, other, false),
    }
}

/// A collection's report name (see the module docs).
pub(super) fn collection_name(collection: &EcCollection) -> String {
    collection
        .name
        .clone()
        .unwrap_or_else(|| collection.op.to_lowercase())
}

fn name(result: &LabelResult) -> String {
    collection_name(&result.collection)
}

/// `file:line`, or `:line` once the file has been mentioned.
fn site(label: &str, mentioned: &mut HashSet<String>) -> String {
    match label.rsplit_once(':') {
        Some((file, line)) if !file.is_empty() && line.chars().all(|c| c.is_ascii_digit()) => {
            if mentioned.insert(file.to_owned()) {
                label.to_owned()
            } else {
                format!(":{line}")
            }
        }
        _ => label.to_owned(),
    }
}

fn role(result: &LabelResult) -> &'static str {
    let c = &result.collection;
    match (c.minted, c.exit, c.aggregated) {
        (true, true, _) => "minted and delivered",
        (true, false, _) => "minted",
        (false, true, _) => "delivered",
        (false, false, true) => "aggregated",
        (false, false, false) => "labelled",
    }
}

/// The exact identity and fault notation of a row, for the verbose form.
fn detail(result: &LabelResult) -> String {
    let c = &result.collection;
    let mut out = format!("{} ({}, {}, {:?})", c.label, c.op, role(result), c.location);
    match result.verdict.outcome() {
        Some(Outcome::Counterexample { faults, runs, .. }) => {
            let _ = write!(
                out,
                ": violated by {} after {}",
                render_set(faults),
                count(runs.len(), "hypothesis run")
            );
        }
        Some(Outcome::Certificate { grade, .. }) => {
            let _ = write!(out, ": certificate {grade}");
        }
        _ => {}
    }
    out
}

/// Whether the verbose form is on by environment (`HYDROFRAC_VERBOSE` set, not `0`).
fn verbose_from_env() -> bool {
    std::env::var("HYDROFRAC_VERBOSE").is_ok_and(|v| !v.is_empty() && v != "0")
}

struct Row {
    name: String,
    site: String,
    verdict: String,
    detail: String,
}

fn write_rows(
    f: &mut fmt::Formatter<'_>,
    rows: &[Row],
    indent: &str,
    verbose: bool,
) -> fmt::Result {
    let name_width = rows
        .iter()
        .map(|r| r.name.chars().count())
        .max()
        .unwrap_or(0);
    let site_width = rows
        .iter()
        .map(|r| r.site.chars().count())
        .max()
        .unwrap_or(0);
    for row in rows {
        writeln!(
            f,
            "{indent}{:<name_width$}  {:<site_width$}  {}",
            row.name, row.site, row.verdict
        )?;
        if verbose {
            writeln!(f, "{indent}    {}", row.detail)?;
        }
    }
    Ok(())
}

impl Display for LabelResults {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let verbose = f.alternate() || verbose_from_env();
        let names = &self.names;
        // Clusters in flow order (location ids are allocated as the flow builds them).
        let mut clusters: BTreeMap<&LocationId, Vec<&LabelResult>> = BTreeMap::new();
        for result in &self.verdicts {
            clusters
                .entry(&result.collection.location)
                .or_default()
                .push(result);
        }
        let mut mentioned: HashSet<String> = HashSet::new();
        let mut holds = 0;
        let mut diverges: Vec<String> = vec![];
        let mut other = 0;
        let mut vacuous = 0;
        let mut not_checked = 0;
        for (i, (location, results)) in clusters.iter().enumerate() {
            if i > 0 {
                writeln!(f)?;
            }
            match names.members(location) {
                Some(n) => writeln!(f, "{} ({})", names.group(location), count(n, "member"))?,
                None => writeln!(f, "{}", names.group(location))?,
            }
            let mut rows: Vec<Row> = vec![];
            let row = |result: &LabelResult, verdict: String, mentioned: &mut HashSet<_>| Row {
                name: name(result),
                site: site(&result.collection.label, mentioned),
                verdict,
                detail: detail(result),
            };
            // Checked collections, then covered scalars, then what was not checked.
            for result in results {
                if let LabelVerdict::Searched(outcome) = &result.verdict {
                    match outcome {
                        Outcome::Certificate { .. } | Outcome::Exhausted { .. } => holds += 1,
                        Outcome::Counterexample { .. } => {
                            diverges.push(format!("{}/{}", names.group(location), name(result)))
                        }
                        Outcome::Vacuous { .. } => vacuous += 1,
                        _ => other += 1,
                    }
                    rows.push(row(result, verdict(names, outcome, false), &mut mentioned));
                }
            }
            for result in results {
                if let LabelVerdict::CoveredBy {
                    input_site,
                    outcome,
                } = &result.verdict
                {
                    match outcome {
                        Outcome::Certificate { .. } | Outcome::Exhausted { .. } => holds += 1,
                        Outcome::Counterexample { .. } => {
                            diverges.push(format!("{}/{}", names.group(location), name(result)))
                        }
                        Outcome::Vacuous { .. } => vacuous += 1,
                        _ => other += 1,
                    }
                    let input = self
                        .verdicts
                        .iter()
                        .find(|r| {
                            r.collection.label == *input_site
                                && r.collection.location == result.collection.location
                        })
                        .map_or_else(|| input_site.clone(), name);
                    let verdict = format!("covered by {input}: {}", verdict(names, outcome, true));
                    rows.push(row(result, verdict, &mut mentioned));
                }
            }
            for result in results {
                if let LabelVerdict::NotChecked(reason) = &result.verdict {
                    not_checked += 1;
                    rows.push(row(
                        result,
                        format!("not checked ({reason})"),
                        &mut mentioned,
                    ));
                }
            }
            write_rows(f, &rows, "  ", verbose)?;
            // Hops, nested under the exit they merge into, in order of first hop.
            let mut exits: Vec<&String> = vec![];
            for result in results {
                if let LabelVerdict::Hop { merged_at, .. } = &result.verdict
                    && !exits.contains(&merged_at)
                {
                    exits.push(merged_at);
                }
            }
            for exit in exits {
                let exit_name = results
                    .iter()
                    .find(|r| {
                        r.collection.label == *exit
                            && !matches!(r.verdict, LabelVerdict::Hop { .. })
                    })
                    .map_or_else(|| exit.clone(), |r| name(r));
                writeln!(f, "  hops merged into {exit_name}:")?;
                let hops: Vec<Row> = results
                    .iter()
                    .filter_map(|result| match &result.verdict {
                        LabelVerdict::Hop { merged_at, outcome } if merged_at == exit => {
                            Some(row(result, hop_verdict(names, outcome), &mut mentioned))
                        }
                        _ => None,
                    })
                    .collect();
                write_rows(f, &hops, "    ", verbose)?;
            }
        }
        if !self.verdicts.is_empty() {
            writeln!(f)?;
            let mut summary = vec![format!("{holds} hold")];
            if !diverges.is_empty() {
                summary.push(format!(
                    "{} DIVERGE ({})",
                    diverges.len(),
                    diverges.join(", ")
                ));
            }
            if vacuous > 0 {
                summary.push(format!("{vacuous} vacuous"));
            }
            if other > 0 {
                summary.push(format!("{other} undecided"));
            }
            if not_checked > 0 {
                summary.push(format!("{not_checked} not checked"));
            }
            writeln!(f, "summary: {}", summary.join(", "))?;
        }
        Ok(())
    }
}

/// A payload's `Debug` rendering, shortened for a report: a wrapper with a single field shows
/// its content (`Some(7)` → `7`, `MemberId::<L>(Legacy { raw_id: 0 })` → `0`), a type keeps
/// only its last path segment without generics, long lists keep their first elements, groups
/// nested too deep to fit become `…`, and the whole is cut at 48 characters. Text that does not
/// parse as a `Debug` rendering is only cut.
pub(super) fn short_payload(display: &str) -> String {
    const MAX: usize = 48;
    let short = match parse::value(display) {
        Some(value) => {
            let mut short = value.render(usize::MAX);
            for depth in (1..=3).rev() {
                if short.chars().count() <= MAX {
                    break;
                }
                short = value.render(depth);
            }
            short
        }
        None => display.to_owned(),
    };
    if short.chars().count() > MAX {
        let cut: String = short.chars().take(MAX - 1).collect();
        format!("{cut}…")
    } else {
        short
    }
}

mod parse {
    //! A small parser for `Debug` renderings: atoms, and named or anonymous `(..)`, `{..}`
    //! and `[..]` groups whose items may be `field: value`.

    pub(super) enum Value {
        Atom(String),
        Group {
            name: Option<String>,
            open: char,
            items: Vec<(Option<String>, Value)>,
        },
    }

    impl Value {
        /// The short form, with groups nested deeper than `depth` elided.
        pub(super) fn render(&self, depth: usize) -> String {
            match self {
                Value::Atom(a) => a.clone(),
                Value::Group { name, open, items } => {
                    if name.is_some() && *open != '[' && items.len() == 1 {
                        return items[0].1.render(depth);
                    }
                    let close = match open {
                        '(' => ')',
                        '[' => ']',
                        _ => '}',
                    };
                    let name = name.as_deref().map(short_name).unwrap_or_default();
                    if depth == 0 {
                        return format!("{name}{open}…{close}");
                    }
                    const KEEP: usize = 4;
                    let mut parts: Vec<String> = items
                        .iter()
                        .take(KEEP)
                        .map(|(field, value)| match field {
                            Some(field) => format!("{field}: {}", value.render(depth - 1)),
                            None => value.render(depth - 1),
                        })
                        .collect();
                    if items.len() > KEEP {
                        parts.push(format!("… {} more", items.len() - KEEP));
                    }
                    format!("{name}{open}{}{close}", parts.join(", "))
                }
            }
        }
    }

    /// The last path segment of a type, without generics.
    fn short_name(name: &str) -> String {
        let base = match name.find('<') {
            Some(i) => name[..i].trim_end_matches("::"),
            None => name,
        };
        base.rsplit("::").next().unwrap_or(base).to_owned()
    }

    pub(super) fn value(text: &str) -> Option<Value> {
        let mut parser = Parser {
            chars: text.chars().collect(),
            at: 0,
        };
        let value = parser.value()?;
        parser.skip_ws();
        (parser.at == parser.chars.len()).then_some(value)
    }

    struct Parser {
        chars: Vec<char>,
        at: usize,
    }

    impl Parser {
        fn peek(&self) -> Option<char> {
            self.chars.get(self.at).copied()
        }

        fn skip_ws(&mut self) {
            while self.peek().is_some_and(char::is_whitespace) {
                self.at += 1;
            }
        }

        fn value(&mut self) -> Option<Value> {
            self.skip_ws();
            let c = self.peek()?;
            if matches!(c, '(' | '{' | '[') {
                return self.group(None);
            }
            if c == '"' || c == '\'' {
                return self.quoted(c).map(Value::Atom);
            }
            let atom = self.atom()?;
            self.skip_ws();
            match self.peek() {
                Some('(' | '{') => self.group(Some(atom)),
                _ => Some(Value::Atom(atom)),
            }
        }

        /// An identifier, path (with balanced `<..>` generics) or number.
        fn atom(&mut self) -> Option<String> {
            let start = self.at;
            let mut depth = 0usize;
            while let Some(c) = self.peek() {
                match c {
                    '<' => depth += 1,
                    '>' if depth > 0 => depth -= 1,
                    _ if depth > 0 => {}
                    c if c.is_alphanumeric() || matches!(c, '_' | ':' | '.' | '-' | '+') => {}
                    _ => break,
                }
                self.at += 1;
            }
            (self.at > start && depth == 0).then(|| self.chars[start..self.at].iter().collect())
        }

        fn quoted(&mut self, quote: char) -> Option<String> {
            let start = self.at;
            self.at += 1;
            while let Some(c) = self.peek() {
                self.at += 1;
                if c == '\\' {
                    self.at += 1;
                } else if c == quote {
                    return Some(self.chars[start..self.at].iter().collect());
                }
            }
            None
        }

        fn group(&mut self, name: Option<String>) -> Option<Value> {
            let open = self.peek()?;
            let close = match open {
                '(' => ')',
                '[' => ']',
                _ => '}',
            };
            self.at += 1;
            let mut items = vec![];
            loop {
                self.skip_ws();
                if self.peek()? == close {
                    self.at += 1;
                    break;
                }
                // `field: value` in a struct.
                let save = self.at;
                let mut field = None;
                if open == '{'
                    && let Some(ident) = self.ident()
                {
                    self.skip_ws();
                    if self.peek() == Some(':') && self.chars.get(self.at + 1) != Some(&':') {
                        self.at += 1;
                        field = Some(ident);
                    } else {
                        self.at = save;
                    }
                }
                items.push((field, self.value()?));
                self.skip_ws();
                match self.peek()? {
                    ',' => self.at += 1,
                    c if c == close => {}
                    _ => return None,
                }
            }
            Some(Value::Group { name, open, items })
        }

        fn ident(&mut self) -> Option<String> {
            let start = self.at;
            while self.peek().is_some_and(|c| c.is_alphanumeric() || c == '_') {
                self.at += 1;
            }
            (self.at > start).then(|| self.chars[start..self.at].iter().collect())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::short_payload;

    #[test]
    fn short_payloads() {
        assert_eq!(short_payload("(1, 0, 0, Some(7))"), "(1, 0, 0, 7)");
        assert_eq!(
            short_payload("Ts { round: 1, writer: Legacy { raw_id: 0 } }"),
            "Ts{round: 1, writer: 0}"
        );
        assert_eq!(
            short_payload("(MemberId::<hydro_test::Proposer>(Legacy { raw_id: 0 }), \"a\")"),
            "(0, \"a\")"
        );
        assert_eq!(
            short_payload("[1, 2, 3, 4, 5, 6]"),
            "[1, 2, 3, 4, … 2 more]"
        );
        assert_eq!(short_payload("None"), "None");
        assert_eq!(
            short_payload(
                "(0, Write { rid: 1, ts: Ts { round: 1, writer: Legacy { raw_id: 0 } }, value: Some(42) })"
            ),
            "(0, Write{rid: 1, ts: Ts{…}, value: 42})"
        );
        assert_eq!(short_payload("{1: 2}"), "{1: 2}");
        assert_eq!(short_payload("0a1b…"), "0a1b…");
        assert_eq!(
            short_payload("a very long payload that does not parse ((( and keeps going on"),
            "a very long payload that does not parse ((( and…"
        );
    }
}
