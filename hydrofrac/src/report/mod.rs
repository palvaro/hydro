//! The per-label report, in source names (docs/design/hydrofrac-lite.md §4.8).
//!
//! [`crate::check::Results`] prints itself with these helpers; this module holds what turns
//! the search's identities into names a reader recognizes:
//!
//! - **nodes** by their location labels (`process`, `member@1`; [`NodeNames`]);
//! - **channels** by the name the program gave them, else the variable the sending statement
//!   binds (read back from the source, [`crate::observe::source_names`]), else its `file:line`,
//!   else `c<id>`;
//! - **payloads** by the `Debug` rendering of a fact with the same bytes when one is known (a
//!   message carrying an element of a labelled collection has the bytes of the fact the tap
//!   shipped for it), else short hex. Renderings longer than [`MAX_INLINE`] characters become
//!   `‹k›` and are listed in a legend under the report.
//!
//! It also produces the rows for comparing verdicts with the research records ([`ReportRow`]),
//! with counterexamples in the records' normalized form ([`ReportNames::normalized`]).

use std::cell::RefCell;
use std::collections::{BTreeSet, HashMap};
use std::fmt::Write as _;

use hydro_lang::sim::network_hook::ChannelInfo;

use crate::fault::{MessageId, NodeId, NodeNames};
use crate::observe::Fact;
use crate::observe::source_names::{site_name, user_frame};
use crate::search::{CrashAt, Fault, FaultSet};

#[cfg(test)]
mod tests;

/// Payload renderings longer than this many characters are abbreviated to a legend entry.
pub const MAX_INLINE: usize = 32;

/// How a report names nodes, channels and payloads, and the legend of abbreviated payloads.
pub struct ReportNames<'a> {
    nodes: &'a NodeNames,
    channels: HashMap<usize, String>,
    payloads: HashMap<Vec<u8>, String>,
    legend: RefCell<Vec<String>>,
}

impl<'a> ReportNames<'a> {
    /// Names for a program with these nodes and channels (as `CompiledSim::channels` lists
    /// them).
    pub fn new(nodes: &'a NodeNames, channels: &[ChannelInfo]) -> Self {
        ReportNames {
            nodes,
            channels: channels.iter().map(|c| (c.id, channel_name(c))).collect(),
            payloads: HashMap::new(),
            legend: RefCell::new(vec![]),
        }
    }

    /// Renders payloads whose bytes are those of one of `facts` as that fact's `Debug`.
    #[must_use]
    pub fn with_facts<'f>(mut self, facts: impl IntoIterator<Item = &'f Fact>) -> Self {
        for fact in facts {
            if let Some(debug) = &fact.debug {
                self.payloads
                    .entry(fact.bytes.clone())
                    .or_insert_with(|| debug.clone());
            }
        }
        self
    }

    /// The name of node `node`.
    pub fn node(&self, node: &NodeId) -> String {
        self.nodes.name(node)
    }

    /// The name of channel `id`.
    pub fn channel(&self, id: usize) -> String {
        self.channels
            .get(&id)
            .cloned()
            .unwrap_or_else(|| format!("c{id}"))
    }

    /// The full rendering of a payload: the `Debug` of the fact with these bytes, else short
    /// hex.
    pub fn payload_full(&self, bytes: &[u8]) -> String {
        match self.payloads.get(bytes) {
            Some(debug) => debug.clone(),
            None => short_hex(bytes),
        }
    }

    /// A payload as the report shows it inline: [`Self::payload_full`], or `‹k›` with a legend
    /// entry when that is longer than [`MAX_INLINE`] characters.
    pub fn payload(&self, bytes: &[u8]) -> String {
        self.abbreviate(self.payload_full(bytes))
    }

    /// `text`, or `‹k›` with a legend entry when it is longer than [`MAX_INLINE`] characters.
    pub fn abbreviate(&self, text: String) -> String {
        if text.chars().count() <= MAX_INLINE {
            return text;
        }
        let mut legend = self.legend.borrow_mut();
        let k = if let Some(i) = legend.iter().position(|t| *t == text) {
            i + 1
        } else {
            legend.push(text);
            legend.len()
        };
        format!("‹{k}›")
    }

    /// A message: `channel[from→to]: payload`.
    pub fn message(&self, m: &MessageId) -> String {
        format!(
            "{}[{}→{}]: {}",
            self.channel(m.channel),
            self.node(&m.from),
            self.node(&m.to),
            self.payload(&m.bytes)
        )
    }

    /// A fault: `omit(message)`, `crash(node)` (before any send) or `crash(node after
    /// message)`.
    pub fn fault(&self, fault: &Fault<MessageId>) -> String {
        match fault {
            Fault::Omit(m) => format!("omit({})", self.message(m)),
            Fault::Crash {
                node,
                at: CrashAt::BeforeAnySend,
            } => format!("crash({node})"),
            Fault::Crash {
                node,
                at: CrashAt::After(m),
            } => format!("crash({node} after {})", self.message(m)),
        }
    }

    /// A fault set: `{fault, fault}`, crashes first.
    pub fn fault_set(&self, set: &FaultSet<MessageId>) -> String {
        let (crashes, omissions): (Vec<_>, Vec<_>) =
            set.iter().partition(|f| f.crashed_node().is_some());
        let faults: Vec<String> = crashes
            .into_iter()
            .chain(omissions)
            .map(|f| self.fault(f))
            .collect();
        format!("{{{}}}", faults.join(", "))
    }

    /// A fault set in the research records' normalized form
    /// (`docs/design/hydrofrac-lite-data/rec-positional.tsv`, column 6): `crash(node)` without
    /// its position, `omit(channel[i→j]:payload)` with member indices only (empty for a
    /// process) and the payload in full, sorted.
    pub fn normalized(&self, set: &FaultSet<MessageId>) -> String {
        let faults: BTreeSet<String> = set
            .iter()
            .map(|f| match f {
                Fault::Crash { node, .. } => format!("crash({node})"),
                Fault::Omit(m) => format!(
                    "omit({}[{}→{}]:{})",
                    self.channel(m.channel),
                    member_index(&m.from),
                    member_index(&m.to),
                    self.payload_full(&m.bytes)
                ),
            })
            .collect();
        format!("{{{}}}", faults.into_iter().collect::<Vec<_>>().join(", "))
    }

    /// The legend of abbreviated payloads so far: `(k, full text)`.
    pub fn legend(&self) -> Vec<(usize, String)> {
        self.legend
            .borrow()
            .iter()
            .enumerate()
            .map(|(i, t)| (i + 1, t.clone()))
            .collect()
    }
}

fn member_index(node: &NodeId) -> String {
    node.member.map(|m| m.to_string()).unwrap_or_default()
}

/// The report name of a channel: the name the program gave it, else the variable the sending
/// statement binds (or the function it is the tail of), else its `file:line`, else `c<id>`.
pub fn channel_name(channel: &ChannelInfo) -> String {
    if let Some(name) = &channel.name {
        return name.clone();
    }
    match user_frame(&channel.op.backtrace) {
        Some(frame) => site_name(&frame).unwrap_or_else(|| {
            let file = frame.file.rsplit(['/', '\\']).next().unwrap_or(&frame.file);
            format!("{file}:{}", frame.line)
        }),
        None => format!("c{}", channel.id),
    }
}

/// Up to 8 bytes of hex, then `…`; `()` for no bytes.
pub fn short_hex(bytes: &[u8]) -> String {
    let mut out = String::new();
    for b in bytes.iter().take(8) {
        let _ = write!(out, "{b:02x}");
    }
    if bytes.len() > 8 {
        out.push('…');
    }
    if out.is_empty() {
        out.push_str("()");
    }
    out
}

/// One label's verdict as a flat row, for comparing with the research records
/// (docs/design/hydrofrac-lite.md §7). See [`crate::check::Results::rows`].
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq)]
pub struct ReportRow {
    /// The test that ran the check (the libtest thread name's last segment), or `run`.
    pub test: String,
    /// The label's index in the program's enumeration.
    pub index: usize,
    /// The label's display name with its file and occurrence (as the report prints it).
    pub label: String,
    /// The labelling call's file name (no directories); empty if unknown.
    pub file: String,
    /// The collection's name (its binding), else its operator.
    pub collection: String,
    /// The position among labels with the same file and collection name.
    pub occurrence: usize,
    /// `Stream`, `KeyedStream`, `Singleton`, ...
    pub kind: String,
    /// In the records' vocabulary: `certificate`, `counterexample`, `vacuous`, `exhausted`
    /// (hypothesis budget); and `not-checked` or `covered` for labels the records never had.
    pub verdict: &'static str,
    /// The counterexample in the records' normalized form ([`ReportNames::normalized`]), or `-`.
    pub cex: String,
    /// The crashes tolerated: `all`, a count, or `-` (none certified, or not searched).
    pub tolerates: String,
    /// The index of the label whose verdict is authoritative for this hop, if it is a
    /// merge-dominated hop.
    pub merged_at: Option<usize>,
    /// Hypotheses tried, counting the probe.
    pub hypotheses: usize,
    /// Schedules explored.
    pub schedules: usize,
    /// Wall time, in milliseconds.
    pub wall_ms: f64,
}

impl ReportRow {
    /// The row as tab-separated values, in field order.
    pub fn to_tsv(&self) -> String {
        [
            self.test.clone(),
            self.index.to_string(),
            self.label.clone(),
            self.file.clone(),
            self.collection.clone(),
            self.occurrence.to_string(),
            self.kind.clone(),
            self.verdict.to_owned(),
            self.cex.clone(),
            self.tolerates.clone(),
            self.merged_at
                .map_or_else(|| "-".to_owned(), |i| i.to_string()),
            self.hypotheses.to_string(),
            self.schedules.to_string(),
            format!("{:.1}", self.wall_ms),
        ]
        .join("\t")
    }
}
