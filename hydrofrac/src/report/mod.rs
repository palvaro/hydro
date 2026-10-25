//! The per-label report, in source names (docs/design/hydrofrac-lite.md §4.8).
//!
//! [`crate::check::Results`] prints itself with these helpers; this module holds what turns
//! the search's identities into names a reader recognizes:
//!
//! - **nodes** by their location labels (`process`, `member@1`; [`NodeNames`]);
//! - **channels** by the name the program gave them, else the variable the sending statement
//!   binds (read back from the source, [`crate::observe::source_names`]), else its `file:line`,
//!   else `c<id>`;
//! - **payloads** by the `Debug` rendering their send carried ([`ReportNames::with_log`]), else
//!   that of a fact with the same bytes (a message carrying an element of a labelled collection
//!   has the bytes of the fact the tap shipped for it), else short hex. Renderings longer than [`MAX_INLINE`] characters become `‹k›` and are listed in
//!   a legend under the report.

use std::cell::RefCell;
use std::collections::HashMap;

use hydro_lang::sim::network_hook::ChannelInfo;

use crate::fault::{Event, EventLog, MessageId, NodeId, NodeNames};
use crate::lamport::short_hex;
use crate::observe::Fact;
use crate::observe::source_names::{short_debug, site_name, user_frame};
use crate::provenance::split_meta;
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

    /// Renders the payloads `log` sent by the `Debug` renderings their metadata carries
    /// ([`crate::provenance::split_meta`]), over any fact's.
    #[must_use]
    pub fn with_log(mut self, log: &EventLog) -> Self {
        for event in log.events() {
            if let Event::Sent { msg, meta, .. } = event
                && let Some((_, Some(debug))) = meta.as_deref().and_then(split_meta)
            {
                self.payloads
                    .insert(msg.bytes.to_vec(), short_debug(&debug));
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

    /// A message: `channel[from→to]: payload`, then ` (copy k)` for the `k`-th repeat of it.
    pub fn message(&self, m: &MessageId) -> String {
        let copy = if m.copy > 0 {
            format!(" (copy {})", m.copy)
        } else {
            String::new()
        };
        format!(
            "{}[{}→{}]: {}{copy}",
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
