//! Why-provenance graphs for LDFI derivations, in the style of Molly's lineage diagrams.
//!
//! An LDFI [`Derivation`](crate::sim::ldfi::Derivation) is a flat set of deliveries: the
//! leaves of the backward closure the search walked from the goal. The closure itself is a
//! DAG, from the goal through the outputs that witnessed it, the executions that produced
//! them, the messages those executions consumed, the executions that sent *those*, and so on
//! down to the deliveries that are fault points. [`ProvGraph`] is that DAG, recorded as the
//! closure runs (`Ldfi::with_provenance_graphs`) and retained on `Schedule::provenance`,
//! one graph per derivation.
//!
//! Every edge carries the lineage oracle that produced it ([`ProvLevel`]: the execution
//! closure, record provenance, or the counterfactual replay that `ldfi::Level::Record` runs at
//! an opaque record), so a graph shows where a record-level derivation went through replay or
//! fell back to the execution-level closure: the fallback appears as an
//! [`ProvNodeKind::Opaque`] node under the step that could not be resolved more finely.
//!
//! Edges point from the thing that depends to the thing it depends on, so the goal is at the
//! top and the fault points are at the bottom. [`dot`] and [`mermaid`] render a graph;
//! [`ProvOptions::collapse_execs`] elides the executions for the compact "goal → messages"
//! picture.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Display, Write as _};

use serde::{Deserialize, Serialize};

/// A message in a provenance graph: the omission vocabulary's view of a send, as a
/// [`Dependency`](crate::sim::ldfi::Dependency) names it, without the wire bytes.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ProvMessage {
    /// The channel's label.
    pub channel: String,
    /// The sending member, for a channel out of a cluster.
    pub from_member: Option<u32>,
    /// The receiving member, for a channel into a cluster.
    pub to_member: Option<u32>,
    /// The payload's rendering.
    pub display: String,
    /// `(node label, ordinal)` when the sender is crashable.
    pub sender: Option<(String, u64)>,
    /// Whether the channel admits omission (and the send happened before `EFF`).
    pub lossy: bool,
}

impl Display for ProvMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.from_member, self.to_member) {
            (None, None) => write!(f, "{}", self.channel)?,
            (from, to) => write!(
                f,
                "{}[{}→{}]",
                self.channel,
                from.map(|m| m.to_string()).unwrap_or_default(),
                to.map(|m| m.to_string()).unwrap_or_default()
            )?,
        }
        write!(f, ":{}", self.display)?;
        if let Some((sender, ordinal)) = &self.sender {
            write!(f, "←{sender}#{ordinal}")?;
        }
        Ok(())
    }
}

/// What the simulator's filter did with a send.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ProvFate {
    /// Put on the wire.
    Delivered,
    /// Dropped by an installed omission.
    Omitted,
    /// Attempted after the sender had crashed.
    Crashed,
}

/// The lineage oracle that produced an edge (see `ldfi::Level`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ProvLevel {
    /// The execution closure (`Level::Execution`, and the fallback of the others): an
    /// execution depends on everything that reached it and its predecessors.
    Execution,
    /// Record-level provenance carried by each element (`Level::Record`).
    Record,
    /// Counterfactual replay with deliveries removed: the oracle `Level::Record` uses at a
    /// record the provenance rewrite could not see into, and the derivation of a hand-written
    /// specification view at the observer.
    Replay,
}

impl ProvLevel {
    /// A short tag for edge labels.
    pub fn tag(self) -> &'static str {
        match self {
            ProvLevel::Execution => "exec",
            ProvLevel::Record => "record",
            ProvLevel::Replay => "replay",
        }
    }
}

/// What a node of the graph stands for.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ProvNodeKind {
    /// The goal that was checked (a specification view, an output port, a labelled
    /// collection).
    Goal {
        /// A description of the goal.
        label: String,
    },
    /// An output that witnessed the goal.
    Output {
        /// The producing node's label.
        node: String,
        /// The producing execution.
        exec: u64,
        /// The output port.
        port: usize,
        /// The value's rendering.
        display: String,
    },
    /// One execution of a node: an async run or one tick execution.
    Exec {
        /// The node's label.
        node: String,
        /// The execution's id (global scheduling order).
        exec: u64,
        /// Whether this is a tick execution (else an async run).
        tick: bool,
        /// The execution's location (the tick or the process/cluster).
        location: String,
    },
    /// A message: a send and, when delivered, its delivery.
    Message {
        /// The message in the omission vocabulary.
        message: ProvMessage,
        /// The sending node's label.
        from: String,
        /// The receiving node's label.
        to: String,
        /// What the filter did with the send.
        fate: ProvFate,
        /// A send to the specification's observer (not a message of the protocol).
        observation: bool,
        /// Whether some fault could have prevented this delivery: it is a leaf of the
        /// derivation. A message that is not a fault point still leads to the execution that
        /// sent it.
        fault_point: bool,
    },
    /// A witnessed output also depends on its producer having survived to emit it.
    Alive {
        /// The node's label.
        node: String,
        /// The sends the node had performed by the output.
        through: u64,
        /// The dependency as the derivation names it.
        message: ProvMessage,
    },
    /// Something computed from nothing that came off a channel: a constant, a timer, an
    /// external input.
    Base {
        /// A description.
        label: String,
    },
    /// A step the configured oracle could not resolve, which fell back to a coarser one.
    Opaque {
        /// Why.
        reason: String,
    },
}

/// A node of a provenance graph.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ProvNode {
    /// What the node stands for.
    pub kind: ProvNodeKind,
}

impl ProvNode {
    /// The node (process or cluster member) this node belongs to, for clustering.
    pub fn at(&self) -> Option<&str> {
        match &self.kind {
            ProvNodeKind::Output { node, .. }
            | ProvNodeKind::Exec { node, .. }
            | ProvNodeKind::Alive { node, .. } => Some(node),
            _ => None,
        }
    }

    /// The dependency this node contributes to the flat derivation, if it is a fault point.
    pub fn dependency(&self) -> Option<&ProvMessage> {
        match &self.kind {
            ProvNodeKind::Message {
                message,
                fault_point: true,
                ..
            } => Some(message),
            ProvNodeKind::Alive { message, .. } => Some(message),
            _ => None,
        }
    }

    /// A short label for rendering.
    pub fn label(&self) -> String {
        match &self.kind {
            ProvNodeKind::Goal { label } => format!("goal: {label}"),
            ProvNodeKind::Output {
                node,
                exec,
                port,
                display,
            } => format!("{node} output@{port}\n{display}\n(exec #{exec})"),
            ProvNodeKind::Exec {
                node, exec, tick, ..
            } => format!(
                "{node}\n{} #{exec}",
                if *tick { "tick" } else { "async run" }
            ),
            ProvNodeKind::Message {
                message,
                fate,
                observation,
                ..
            } => {
                let mut s = message.to_string();
                if *observation {
                    s.push_str("\n(observation)");
                }
                match fate {
                    ProvFate::Delivered => {}
                    ProvFate::Omitted => s.push_str("\n✗ omitted"),
                    ProvFate::Crashed => s.push_str("\n✗ sender crashed"),
                }
                s
            }
            ProvNodeKind::Alive { node, through, .. } => {
                format!("{node} alive\nthrough send #{through}")
            }
            ProvNodeKind::Base { label } => label.clone(),
            ProvNodeKind::Opaque { reason } => format!("opaque:\n{reason}"),
        }
    }
}

/// How one node depends on another.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ProvEdgeKind {
    /// Goal → output: the output witnessed the goal.
    Checked,
    /// Output or message → execution: the execution produced it.
    Produced,
    /// Execution → message: the execution consumed the delivery (a hook release, a top-level
    /// receive, or an arrival an unscripted batch may have released).
    Consumed,
    /// Execution → execution: state carried from an earlier execution (of the same tick, or
    /// of a tick whose yields the run consumed).
    Carried,
    /// Output or message → message: record-level provenance (the element was computed from
    /// the delivery).
    Derived,
    /// Output or message → message: kept by counterfactual replay (the target disappears
    /// without this delivery).
    Counterfactual,
    /// Any → opaque: the step fell back to a coarser lineage.
    Fallback,
    /// Output → alive: the producer had to survive to emit the output.
    Alive,
}

/// An edge: `from` depends on `to`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ProvEdge {
    /// The dependent node.
    pub from: usize,
    /// The node it depends on.
    pub to: usize,
    /// How.
    pub kind: ProvEdgeKind,
    /// The lineage oracle that produced the edge, when one did.
    pub level: Option<ProvLevel>,
}

/// The stable identity of a node within one run, so that recording the same execution or
/// send twice (or merging two graphs of the same run) yields one node.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ProvKey {
    Goal(String),
    Output {
        exec: u64,
        port: usize,
        member: Option<u32>,
        bytes: Vec<u8>,
    },
    Exec(u64),
    /// By index into the run's sends.
    Message(usize),
    Alive(String, u64),
    Base(String),
    /// Under the step anchored at the key, for the reason.
    Opaque(Box<ProvKey>, String),
}

/// A why-provenance graph of one derivation.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ProvGraph {
    /// The nodes.
    pub nodes: Vec<ProvNode>,
    /// The edges, deduplicated.
    pub edges: BTreeSet<ProvEdge>,
    /// The goal nodes.
    pub roots: Vec<usize>,
    #[serde(skip)]
    index: BTreeMap<ProvKey, usize>,
    #[serde(skip)]
    keys: Vec<ProvKey>,
}

impl ProvGraph {
    /// The number of nodes.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Whether the graph has no nodes.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// The nodes nothing depends on further: the leaves of the closure.
    pub fn leaves(&self) -> Vec<usize> {
        let mut has_out = vec![false; self.nodes.len()];
        for edge in &self.edges {
            has_out[edge.from] = true;
        }
        (0..self.nodes.len()).filter(|i| !has_out[*i]).collect()
    }

    /// The fault points of the graph: what the flat derivation consists of.
    pub fn dependencies(&self) -> BTreeSet<ProvMessage> {
        self.nodes
            .iter()
            .filter_map(ProvNode::dependency)
            .cloned()
            .collect()
    }

    /// The edges out of `node`.
    pub fn out_edges(&self, node: usize) -> impl Iterator<Item = &ProvEdge> + '_ {
        self.edges.iter().filter(move |e| e.from == node)
    }

    /// The edges into `node`.
    pub fn in_edges(&self, node: usize) -> impl Iterator<Item = &ProvEdge> + '_ {
        self.edges.iter().filter(move |e| e.to == node)
    }

    /// Inserts a node under `key` (returning the existing one if the key is known).
    pub(crate) fn node(&mut self, key: ProvKey, kind: impl FnOnce() -> ProvNodeKind) -> usize {
        if let Some(&i) = self.index.get(&key) {
            return i;
        }
        let i = self.nodes.len();
        self.nodes.push(ProvNode { kind: kind() });
        self.keys.push(key.clone());
        self.index.insert(key, i);
        i
    }

    /// The key of node `i` (only for graphs built in this process; deserialized graphs have
    /// no keys).
    pub(crate) fn key(&self, i: usize) -> Option<&ProvKey> {
        self.keys.get(i)
    }

    /// Adds an edge.
    pub(crate) fn edge(
        &mut self,
        from: usize,
        to: usize,
        kind: ProvEdgeKind,
        level: Option<ProvLevel>,
    ) {
        self.edges.insert(ProvEdge {
            from,
            to,
            kind,
            level,
        });
    }

    /// Marks `node` as a root.
    pub(crate) fn root(&mut self, node: usize) {
        if !self.roots.contains(&node) {
            self.roots.push(node);
        }
    }

    /// Merges `other` into `self`, identifying nodes by key.
    pub(crate) fn merge(&mut self, other: &ProvGraph) {
        let mut map = Vec::with_capacity(other.nodes.len());
        for (i, node) in other.nodes.iter().enumerate() {
            let key = other.keys[i].clone();
            let kind = node.kind.clone();
            map.push(self.node(key, || kind));
        }
        for edge in &other.edges {
            self.edge(map[edge.from], map[edge.to], edge.kind, edge.level);
        }
        for root in &other.roots {
            self.root(map[*root]);
        }
    }

    /// The graph with its execution nodes elided: whatever depended on an execution depends
    /// directly on what the execution (and the executions it carried state from) consumed.
    pub fn collapsed(&self) -> ProvGraph {
        let is_exec = |i: usize| matches!(self.nodes[i].kind, ProvNodeKind::Exec { .. });
        // Non-execution nodes reachable from each execution through execution nodes only,
        // with the kind of the edge that left the execution chain.
        let mut through: Vec<Option<Vec<(usize, ProvEdgeKind, Option<ProvLevel>)>>> =
            vec![None; self.nodes.len()];
        fn resolve(
            g: &ProvGraph,
            i: usize,
            through: &mut Vec<Option<Vec<(usize, ProvEdgeKind, Option<ProvLevel>)>>>,
            visiting: &mut Vec<bool>,
        ) -> Vec<(usize, ProvEdgeKind, Option<ProvLevel>)> {
            if let Some(done) = &through[i] {
                return done.clone();
            }
            if visiting[i] {
                return vec![];
            }
            visiting[i] = true;
            let mut out = vec![];
            for edge in g.out_edges(i) {
                if matches!(g.nodes[edge.to].kind, ProvNodeKind::Exec { .. }) {
                    out.extend(resolve(g, edge.to, through, visiting));
                } else {
                    out.push((edge.to, edge.kind, edge.level));
                }
            }
            visiting[i] = false;
            out.sort();
            out.dedup();
            through[i] = Some(out.clone());
            out
        }
        let mut visiting = vec![false; self.nodes.len()];
        let mut g = ProvGraph::default();
        let mut map: Vec<Option<usize>> = vec![None; self.nodes.len()];
        for (i, node) in self.nodes.iter().enumerate() {
            if is_exec(i) {
                continue;
            }
            let key = self
                .keys
                .get(i)
                .cloned()
                .unwrap_or_else(|| ProvKey::Base(format!("node {i}")));
            let kind = node.kind.clone();
            map[i] = Some(g.node(key, || kind));
        }
        for edge in &self.edges {
            let Some(from) = map[edge.from] else {
                continue;
            };
            if is_exec(edge.to) {
                for (to, kind, level) in resolve(self, edge.to, &mut through, &mut visiting) {
                    if let Some(to) = map[to] {
                        g.edge(from, to, kind, level);
                    }
                }
            } else if let Some(to) = map[edge.to] {
                g.edge(from, to, edge.kind, edge.level);
            }
        }
        for root in &self.roots {
            if let Some(r) = map[*root] {
                g.root(r);
            }
        }
        g
    }

    /// The graph as JSON.
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).expect("a provenance graph serializes")
    }
}

/// Records a provenance graph while a derivation is computed.
///
/// Sub-derivations (of one send) are memoized by the closure; so that a memoized derivation
/// can be spliced into every graph that reaches it, each sub-derivation is recorded into a
/// graph of its own ([`Self::push`]) that is merged into its parent when done
/// ([`Self::pop`]) and kept beside the memoized derivation.
#[derive(Debug, Default)]
pub(crate) struct ProvRecorder {
    stack: Vec<ProvGraph>,
    goal: Option<String>,
}

impl ProvRecorder {
    pub(crate) fn new() -> Self {
        ProvRecorder {
            stack: vec![ProvGraph::default()],
            goal: None,
        }
    }

    /// Starts a fresh graph for a goal.
    pub(crate) fn begin(&mut self, goal: Option<&str>) {
        self.stack = vec![ProvGraph::default()];
        self.goal = goal.map(str::to_owned);
        if let Some(label) = &self.goal {
            let label = label.clone();
            let i = self
                .top()
                .node(ProvKey::Goal(label.clone()), || ProvNodeKind::Goal {
                    label,
                });
            self.top().root(i);
        }
    }

    /// Takes the graph begun by [`Self::begin`].
    pub(crate) fn end(&mut self) -> ProvGraph {
        debug_assert_eq!(self.stack.len(), 1, "unbalanced provenance scopes");
        self.goal = None;
        std::mem::take(&mut self.stack[0])
    }

    /// The goal node of the current graph, if a goal was begun.
    pub(crate) fn goal(&mut self) -> Option<usize> {
        let label = self.goal.clone()?;
        Some(
            self.top()
                .node(ProvKey::Goal(label.clone()), || ProvNodeKind::Goal {
                    label,
                }),
        )
    }

    /// Opens a sub-graph.
    pub(crate) fn push(&mut self) {
        self.stack.push(ProvGraph::default());
    }

    /// Closes the sub-graph, merging it into its parent, and returns it.
    pub(crate) fn pop(&mut self) -> ProvGraph {
        let sub = self.stack.pop().expect("a pushed provenance scope");
        self.top().merge(&sub);
        sub
    }

    /// Merges a previously recorded sub-graph into the current one.
    pub(crate) fn splice(&mut self, sub: &ProvGraph) {
        self.top().merge(sub);
    }

    pub(crate) fn top(&mut self) -> &mut ProvGraph {
        if self.stack.is_empty() {
            self.stack.push(ProvGraph::default());
        }
        self.stack.last_mut().expect("a provenance scope")
    }

    pub(crate) fn node(&mut self, key: ProvKey, kind: impl FnOnce() -> ProvNodeKind) -> usize {
        self.top().node(key, kind)
    }

    pub(crate) fn key(&mut self, node: usize) -> Option<ProvKey> {
        self.top().key(node).cloned()
    }

    pub(crate) fn edge(
        &mut self,
        from: usize,
        to: usize,
        kind: ProvEdgeKind,
        level: Option<ProvLevel>,
    ) {
        self.top().edge(from, to, kind, level);
    }
}

/// Rendering options.
#[derive(Clone, Debug)]
pub struct ProvOptions {
    /// Elide execution nodes (see [`ProvGraph::collapsed`]).
    pub collapse_execs: bool,
    /// Label edges with the lineage oracle that produced them.
    pub show_levels: bool,
    /// Show messages that are not fault points (reliable channels, non-crashable senders).
    pub show_non_fault_points: bool,
    /// A title.
    pub title: Option<String>,
}

impl Default for ProvOptions {
    fn default() -> Self {
        ProvOptions {
            collapse_execs: false,
            show_levels: true,
            show_non_fault_points: true,
            title: None,
        }
    }
}

fn escape_dot(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

fn escape_mermaid(s: &str) -> String {
    s.replace('"', "#quot;")
        .replace('<', "#lt;")
        .replace('>', "#gt;")
        .replace('\n', "<br/>")
}

/// Which nodes to draw and how they are grouped.
fn layout(g: &ProvGraph, opts: &ProvOptions) -> (Vec<bool>, BTreeMap<String, Vec<usize>>) {
    let shown: Vec<bool> = g
        .nodes
        .iter()
        .map(|n| {
            opts.show_non_fault_points
                || !matches!(
                    n.kind,
                    ProvNodeKind::Message {
                        fault_point: false,
                        ..
                    }
                )
        })
        .collect();
    let mut clusters: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for (i, node) in g.nodes.iter().enumerate() {
        if shown[i]
            && let Some(at) = node.at()
        {
            clusters.entry(at.to_owned()).or_default().push(i);
        }
    }
    (shown, clusters)
}

fn dot_node_attrs(node: &ProvNode) -> &'static str {
    match &node.kind {
        ProvNodeKind::Goal { .. } => {
            "shape=doubleoctagon, style=\"filled,bold\", fillcolor=\"#fff2b2\""
        }
        ProvNodeKind::Output { .. } => "shape=box, style=\"rounded,filled\", fillcolor=\"#e8f0fe\"",
        ProvNodeKind::Exec { tick: true, .. } => "shape=box, style=filled, fillcolor=\"#f4f4f4\"",
        ProvNodeKind::Exec { tick: false, .. } => {
            "shape=box, style=\"filled,dashed\", fillcolor=\"#f4f4f4\""
        }
        ProvNodeKind::Message {
            fate: ProvFate::Delivered,
            fault_point: true,
            observation: false,
            ..
        } => "shape=note, style=filled, fillcolor=\"#dff5dd\"",
        ProvNodeKind::Message {
            fate: ProvFate::Delivered,
            observation: true,
            ..
        } => "shape=note, style=\"filled,dashed\", fillcolor=\"#f0e6ff\"",
        ProvNodeKind::Message {
            fate: ProvFate::Delivered,
            ..
        } => "shape=note, style=\"filled,dashed\", fillcolor=\"#eeeeee\", fontcolor=\"#666666\"",
        ProvNodeKind::Message { .. } => {
            "shape=note, style=filled, fillcolor=\"#ffd6d6\", color=red"
        }
        ProvNodeKind::Alive { .. } => {
            "shape=ellipse, style=\"filled,dashed\", fillcolor=\"#fbe4c8\""
        }
        ProvNodeKind::Base { .. } => "shape=plaintext",
        ProvNodeKind::Opaque { .. } => "shape=box, style=dashed, color=red, fontcolor=red",
    }
}

fn dot_edge_attrs(kind: ProvEdgeKind) -> &'static str {
    match kind {
        ProvEdgeKind::Checked => "penwidth=2",
        ProvEdgeKind::Produced => "",
        ProvEdgeKind::Consumed => "",
        ProvEdgeKind::Carried => "style=dashed, color=\"#888888\"",
        ProvEdgeKind::Derived => "color=\"#1a56db\"",
        ProvEdgeKind::Counterfactual => "color=\"#0b7a3b\", penwidth=2",
        ProvEdgeKind::Fallback => "style=dotted, color=red",
        ProvEdgeKind::Alive => "style=dotted, color=\"#b35900\"",
    }
}

/// Renders a graph as Graphviz DOT, goal at the top, grouped by node.
pub fn dot(g: &ProvGraph, opts: &ProvOptions) -> String {
    let collapsed;
    let g = if opts.collapse_execs {
        collapsed = g.collapsed();
        &collapsed
    } else {
        g
    };
    let (shown, clusters) = layout(g, opts);
    let mut out = String::new();
    let _ = writeln!(out, "digraph provenance {{");
    let _ = writeln!(out, "    rankdir=TB;");
    let _ = writeln!(out, "    compound=true;");
    // A dense graph (execution-level lineage, where every execution depends on most earlier
    // ones) is a hairball of parallel edges: merge the edges that share a path.
    if g.edges.len() > 60 {
        let _ = writeln!(out, "    concentrate=true;");
    }
    let _ = writeln!(
        out,
        "    node [fontname=\"Helvetica\", fontsize=10];\n    edge [fontname=\"Helvetica\", fontsize=8];"
    );
    if let Some(title) = &opts.title {
        let _ = writeln!(
            out,
            "    label=\"{}\"; labelloc=t; fontname=\"Helvetica\";",
            escape_dot(title)
        );
    }
    let mut clustered = vec![false; g.nodes.len()];
    for (k, (at, members)) in clusters.iter().enumerate() {
        let _ = writeln!(out, "    subgraph cluster_{k} {{");
        let _ = writeln!(
            out,
            "        label=\"{}\"; style=\"rounded\"; color=\"#999999\";",
            escape_dot(at)
        );
        for &i in members {
            clustered[i] = true;
            let node = &g.nodes[i];
            let _ = writeln!(
                out,
                "        n{i} [label=\"{}\", {}];",
                escape_dot(&node.label()),
                dot_node_attrs(node)
            );
        }
        let _ = writeln!(out, "    }}");
    }
    for (i, node) in g.nodes.iter().enumerate() {
        if shown[i] && !clustered[i] {
            let _ = writeln!(
                out,
                "    n{i} [label=\"{}\", {}];",
                escape_dot(&node.label()),
                dot_node_attrs(node)
            );
        }
    }
    for edge in &g.edges {
        if !shown[edge.from] || !shown[edge.to] {
            continue;
        }
        let mut attrs: Vec<String> = vec![];
        let style = dot_edge_attrs(edge.kind);
        if !style.is_empty() {
            attrs.push(style.to_owned());
        }
        if opts.show_levels
            && let Some(level) = edge.level
        {
            attrs.push(format!("label=\"{}\", fontcolor=\"#888888\"", level.tag()));
        }
        let _ = writeln!(
            out,
            "    n{} -> n{}{};",
            edge.from,
            edge.to,
            if attrs.is_empty() {
                String::new()
            } else {
                format!(" [{}]", attrs.join(", "))
            }
        );
    }
    let _ = writeln!(out, "}}");
    out
}

fn mermaid_shape(node: &ProvNode, label: &str) -> String {
    match &node.kind {
        ProvNodeKind::Goal { .. } => format!("{{{{\"{label}\"}}}}"),
        ProvNodeKind::Output { .. } => format!("([\"{label}\"])"),
        ProvNodeKind::Exec { .. } => format!("[\"{label}\"]"),
        ProvNodeKind::Message { .. } => format!(">\"{label}\"]"),
        ProvNodeKind::Alive { .. } => format!("((\"{label}\"))"),
        ProvNodeKind::Base { .. } => format!("[/\"{label}\"/]"),
        ProvNodeKind::Opaque { .. } => format!("[\\\"{label}\"\\]"),
    }
}

fn mermaid_class(node: &ProvNode) -> &'static str {
    match &node.kind {
        ProvNodeKind::Goal { .. } => "goal",
        ProvNodeKind::Output { .. } => "output",
        ProvNodeKind::Exec { .. } => "exec",
        ProvNodeKind::Message {
            fate: ProvFate::Delivered,
            observation: true,
            ..
        } => "observation",
        ProvNodeKind::Message {
            fate: ProvFate::Delivered,
            fault_point: true,
            ..
        } => "message",
        ProvNodeKind::Message {
            fate: ProvFate::Delivered,
            ..
        } => "reliable",
        ProvNodeKind::Message { .. } => "lost",
        ProvNodeKind::Alive { .. } => "alive",
        ProvNodeKind::Base { .. } => "base",
        ProvNodeKind::Opaque { .. } => "opaque",
    }
}

/// Renders a graph as a Mermaid flowchart, goal at the top, grouped by node.
pub fn mermaid(g: &ProvGraph, opts: &ProvOptions) -> String {
    let collapsed;
    let g = if opts.collapse_execs {
        collapsed = g.collapsed();
        &collapsed
    } else {
        g
    };
    let (shown, clusters) = layout(g, opts);
    let mut out = String::new();
    if let Some(title) = &opts.title {
        let _ = writeln!(out, "---\ntitle: {title}\n---");
    }
    let _ = writeln!(out, "flowchart TD");
    let mut clustered = vec![false; g.nodes.len()];
    for (k, (at, members)) in clusters.iter().enumerate() {
        let _ = writeln!(out, "    subgraph c{k}[\"{}\"]", escape_mermaid(at));
        for &i in members {
            clustered[i] = true;
            let node = &g.nodes[i];
            let _ = writeln!(
                out,
                "        n{i}{}:::{}",
                mermaid_shape(node, &escape_mermaid(&node.label())),
                mermaid_class(node)
            );
        }
        let _ = writeln!(out, "    end");
    }
    for (i, node) in g.nodes.iter().enumerate() {
        if shown[i] && !clustered[i] {
            let _ = writeln!(
                out,
                "    n{i}{}:::{}",
                mermaid_shape(node, &escape_mermaid(&node.label())),
                mermaid_class(node)
            );
        }
    }
    for edge in &g.edges {
        if !shown[edge.from] || !shown[edge.to] {
            continue;
        }
        let arrow = match edge.kind {
            ProvEdgeKind::Checked | ProvEdgeKind::Counterfactual => "==>",
            ProvEdgeKind::Carried | ProvEdgeKind::Fallback | ProvEdgeKind::Alive => "-.->",
            ProvEdgeKind::Produced | ProvEdgeKind::Consumed | ProvEdgeKind::Derived => "-->",
        };
        let label = match (opts.show_levels, edge.level) {
            (true, Some(level)) => format!("|{}|", level.tag()),
            _ => String::new(),
        };
        let _ = writeln!(out, "    n{} {arrow}{label} n{}", edge.from, edge.to);
    }
    let _ = writeln!(
        out,
        "    classDef goal fill:#fff2b2,stroke:#333,stroke-width:2px\n    \
         classDef output fill:#e8f0fe,stroke:#333\n    \
         classDef exec fill:#f4f4f4,stroke:#999\n    \
         classDef message fill:#dff5dd,stroke:#333\n    \
         classDef observation fill:#f0e6ff,stroke:#666,stroke-dasharray:3 3\n    \
         classDef reliable fill:#eeeeee,stroke:#999,color:#666,stroke-dasharray:3 3\n    \
         classDef lost fill:#ffd6d6,stroke:#c00\n    \
         classDef alive fill:#fbe4c8,stroke:#b35900,stroke-dasharray:3 3\n    \
         classDef base fill:#ffffff,stroke:#999\n    \
         classDef opaque fill:#ffffff,stroke:#c00,color:#c00,stroke-dasharray:4 4"
    );
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn message(channel: &str, from: Option<u32>, to: Option<u32>, display: &str) -> ProvMessage {
        ProvMessage {
            channel: channel.to_owned(),
            from_member: from,
            to_member: to,
            display: display.to_owned(),
            sender: None,
            lossy: true,
        }
    }

    /// goal → output → exec(tick) → message(lossy) → exec(sender) → base; exec → carried exec.
    fn tiny() -> ProvGraph {
        let mut g = ProvGraph::default();
        let goal = g.node(ProvKey::Goal("POST".into()), || ProvNodeKind::Goal {
            label: "POST".into(),
        });
        g.root(goal);
        let output = g.node(
            ProvKey::Output {
                exec: 7,
                port: 0,
                member: None,
                bytes: vec![1],
            },
            || ProvNodeKind::Output {
                node: "observer".into(),
                exec: 7,
                port: 0,
                display: "01".into(),
            },
        );
        let exec = g.node(ProvKey::Exec(7), || ProvNodeKind::Exec {
            node: "observer".into(),
            exec: 7,
            tick: true,
            location: "Tick(0)".into(),
        });
        let earlier = g.node(ProvKey::Exec(5), || ProvNodeKind::Exec {
            node: "observer".into(),
            exec: 5,
            tick: true,
            location: "Tick(0)".into(),
        });
        let msg = g.node(ProvKey::Message(0), || ProvNodeKind::Message {
            message: message("commit", None, Some(0), "Txn(1)"),
            from: "coordinator".into(),
            to: "participant@0".into(),
            fate: ProvFate::Delivered,
            observation: false,
            fault_point: true,
        });
        let sender = g.node(ProvKey::Exec(3), || ProvNodeKind::Exec {
            node: "coordinator".into(),
            exec: 3,
            tick: false,
            location: "Process(1)".into(),
        });
        let base = g.node(ProvKey::Base("external input".into()), || {
            ProvNodeKind::Base {
                label: "external input".into(),
            }
        });
        g.edge(goal, output, ProvEdgeKind::Checked, None);
        g.edge(
            output,
            exec,
            ProvEdgeKind::Produced,
            Some(ProvLevel::Execution),
        );
        g.edge(
            exec,
            earlier,
            ProvEdgeKind::Carried,
            Some(ProvLevel::Execution),
        );
        g.edge(
            earlier,
            msg,
            ProvEdgeKind::Consumed,
            Some(ProvLevel::Execution),
        );
        g.edge(
            msg,
            sender,
            ProvEdgeKind::Produced,
            Some(ProvLevel::Execution),
        );
        g.edge(
            sender,
            base,
            ProvEdgeKind::Consumed,
            Some(ProvLevel::Execution),
        );
        g
    }

    #[test]
    fn nodes_are_deduplicated_by_key() {
        let mut g = tiny();
        let n = g.node_count();
        let again = g.node(ProvKey::Exec(7), || unreachable!("existing key"));
        assert_eq!(again, 2);
        assert_eq!(g.node_count(), n);
    }

    #[test]
    fn dependencies_are_the_fault_points() {
        let g = tiny();
        let deps = g.dependencies();
        assert_eq!(deps.len(), 1);
        assert_eq!(deps.iter().next().unwrap().to_string(), "commit[→0]:Txn(1)");
        assert_eq!(g.leaves(), vec![6]);
    }

    #[test]
    fn merge_identifies_nodes_by_key() {
        let a = tiny();
        let mut b = ProvGraph::default();
        let msg = b.node(ProvKey::Message(0), || ProvNodeKind::Message {
            message: message("commit", None, Some(0), "Txn(1)"),
            from: "coordinator".into(),
            to: "participant@0".into(),
            fate: ProvFate::Delivered,
            observation: false,
            fault_point: true,
        });
        let other = b.node(ProvKey::Message(1), || ProvNodeKind::Message {
            message: message("prepare", None, Some(0), "Txn(1)"),
            from: "coordinator".into(),
            to: "participant@0".into(),
            fate: ProvFate::Delivered,
            observation: false,
            fault_point: true,
        });
        b.edge(msg, other, ProvEdgeKind::Derived, Some(ProvLevel::Record));
        let mut merged = a.clone();
        merged.merge(&b);
        assert_eq!(merged.node_count(), a.node_count() + 1);
        assert_eq!(merged.edges.len(), a.edges.len() + 1);
        assert_eq!(merged.dependencies().len(), 2);
    }

    #[test]
    fn collapsing_elides_executions_through_carried_state() {
        let g = tiny().collapsed();
        assert!(
            g.nodes
                .iter()
                .all(|n| !matches!(n.kind, ProvNodeKind::Exec { .. }))
        );
        // output → message (through exec 7 → exec 5), message → base (through exec 3).
        let labels: Vec<(String, String)> = g
            .edges
            .iter()
            .map(|e| {
                (
                    g.nodes[e.from].label().lines().next().unwrap().to_owned(),
                    g.nodes[e.to].label().lines().next().unwrap().to_owned(),
                )
            })
            .collect();
        assert!(labels.contains(&("observer output@0".into(), "commit[→0]:Txn(1)".into())));
        assert!(labels.contains(&("commit[→0]:Txn(1)".into(), "external input".into())));
        assert_eq!(g.edges.len(), 3);
    }

    #[test]
    fn dot_and_mermaid_render_every_node() {
        let g = tiny();
        let dot = dot(&g, &ProvOptions::default());
        assert!(dot.starts_with("digraph provenance {"));
        assert!(dot.contains("goal: POST"));
        assert!(dot.contains("subgraph cluster_0"));
        assert!(dot.contains("label=\"coordinator\""));
        assert!(dot.contains("commit[→0]:Txn(1)"));
        assert!(dot.contains("n0 -> n1 [penwidth=2]"));
        assert!(dot.contains("label=\"exec\""));
        let mermaid = mermaid(&g, &ProvOptions::default());
        assert!(mermaid.starts_with("flowchart TD"));
        assert!(mermaid.contains("n0{{\"goal: POST\"}}:::goal"));
        assert!(mermaid.contains("n0 ==> n1"));
        assert!(mermaid.contains("n1 -->|exec| n2"));
        assert!(mermaid.contains("subgraph c0[\"coordinator\"]"));
    }

    #[test]
    fn json_round_trips_nodes_and_edges() {
        let g = tiny();
        let json = g.to_json();
        let back: ProvGraph = serde_json::from_str(&json).unwrap();
        assert_eq!(back.nodes, g.nodes);
        assert_eq!(back.edges, g.edges);
        assert_eq!(back.roots, g.roots);
    }
}
