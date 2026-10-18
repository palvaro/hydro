//! Lamport (spacetime) diagrams of a [`Trace`]: one lane per node, time flowing down,
//! an arrow per message, with omitted messages, sends lost to a crash, messages still in
//! flight at the horizon, crash points and the end of finite failures marked. The first
//! of Molly's two pictures of a counterexample; the second (the goal's provenance) is a
//! different zoom and a different module.
//!
//! Two backends: a Mermaid `sequenceDiagram` ([`mermaid`]) for quick looks (GitHub, docs,
//! chat), and Graphviz DOT ([`dot`]) for a true spacetime layout in which a message's send
//! and receive are distinct points in time.
//!
//! A sequence diagram draws a message as one horizontal line, so it has to choose one
//! instant for it; see [`Placement`].

use std::collections::BTreeSet;
use std::fmt::Write as _;

use super::trace::{Crash, Fate, Trace, TraceEventKind, TraceMessage};

/// Where a sequence diagram (which draws a message as one instant) places a delivered
/// message.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Placement {
    /// At its receive: each lane's inbound order is the order the node actually consumed its
    /// messages, which is what decided its behaviour (and what the simulator's exploration
    /// varies). Messages never received are placed at their send.
    #[default]
    Receive,
    /// At its send: each lane's outbound order is exact; arrival order is lost.
    Send,
}

/// Rendering options.
#[derive(Clone, Debug)]
pub struct LamportOptions {
    /// Messages (indices into [`Trace::messages`]) to emphasize: a goal's support
    /// ([`Trace::support`]).
    pub highlight: BTreeSet<usize>,
    /// Whether to show the specification's observer and the traffic into it (off by default:
    /// observations are the specification's plumbing, not the protocol's messages).
    pub show_observer: bool,
    /// Whether to show external outputs as annotations on their node's lane.
    pub show_outputs: bool,
    /// Where a sequence diagram places a delivered message.
    pub placement: Placement,
    /// A title.
    pub title: Option<String>,
    /// Payload renderings longer than this are truncated with `…`.
    pub max_label: usize,
}

impl Default for LamportOptions {
    fn default() -> Self {
        LamportOptions {
            highlight: BTreeSet::new(),
            show_observer: false,
            show_outputs: true,
            placement: Placement::Receive,
            title: None,
            max_label: 60,
        }
    }
}

impl LamportOptions {
    /// Emphasizes the given messages.
    #[must_use]
    pub fn highlight(mut self, messages: impl IntoIterator<Item = usize>) -> Self {
        self.highlight.extend(messages);
        self
    }

    /// Sets the title.
    #[must_use]
    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }

    /// Shows the observer and the traffic into it.
    #[must_use]
    pub fn show_observer(mut self, show: bool) -> Self {
        self.show_observer = show;
        self
    }

    fn node_shown(&self, trace: &Trace, node: usize) -> bool {
        self.show_observer || !trace.nodes[node].is_observer
    }

    fn message_shown(&self, trace: &Trace, message: &TraceMessage) -> bool {
        self.node_shown(trace, message.from)
            && self.node_shown(trace, message.to)
            && (self.show_observer || !message.observation)
    }

    fn label(&self, text: &str) -> String {
        let mut chars = text.chars();
        let truncated: String = chars.by_ref().take(self.max_label).collect();
        if chars.next().is_some() {
            format!("{truncated}…")
        } else {
            truncated
        }
    }
}

/// What a diagram row shows, in the order rows are emitted.
#[derive(Clone, Copy, Debug)]
enum Row {
    Eff,
    Message(usize),
    Crash(usize),
    Output(usize),
}

/// The instant a sequence diagram places `message` at (see [`Placement`]).
fn placement(opts: &LamportOptions, message: &TraceMessage) -> usize {
    match (opts.placement, message.recv_event) {
        (Placement::Receive, Some(recv)) => recv,
        _ => message.send_event,
    }
}

/// The rows of a sequence diagram, ordered by the instant each is placed at: `(event
/// index, tie-break)`. EFF goes before the event it precedes; a crash right after the row of
/// the send that triggered it (wherever that message is placed); a send lost to the crash
/// right after the crash, as its consequence.
fn rows(trace: &Trace, opts: &LamportOptions) -> Vec<Row> {
    let mut rows: Vec<((usize, u8), Row)> = vec![];
    if let Some(cut) = trace.eff_cut {
        rows.push(((cut, 0), Row::Eff));
    }
    // Where each node's crash is drawn.
    let crash_at: Vec<Option<usize>> = trace
        .nodes
        .iter()
        .map(|info| match info.crash {
            Some(Crash::AfterEvent(event)) => Some(match trace.events[event].kind {
                TraceEventKind::Send(m) => placement(opts, &trace.messages[m]),
                _ => event,
            }),
            Some(Crash::BeforeAnySend) | None => None,
        })
        .collect();
    for (node, at) in crash_at.iter().enumerate() {
        if let Some(at) = at
            && opts.node_shown(trace, node)
        {
            rows.push(((*at, 2), Row::Crash(node)));
        }
    }
    for (index, message) in trace.messages.iter().enumerate() {
        if !opts.message_shown(trace, message) {
            continue;
        }
        let at = match (message.fate, crash_at[message.from]) {
            (Fate::Crashed, Some(crash)) => (crash.max(message.send_event), 3),
            _ => (placement(opts, message), 1),
        };
        rows.push((at, Row::Message(index)));
    }
    if opts.show_outputs {
        for (index, event) in trace.events.iter().enumerate() {
            if matches!(event.kind, TraceEventKind::Output { .. })
                && opts.node_shown(trace, event.node)
            {
                rows.push(((index, 1), Row::Output(index)));
            }
        }
    }
    rows.sort_by_key(|(at, _)| *at);
    rows.into_iter().map(|(_, row)| row).collect()
}

/// Escapes text for a Mermaid sequence-diagram message or note.
fn escape_mermaid(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '#' => out.push_str("#35;"),
            ';' => out.push_str("#59;"),
            '\n' => out.push_str("<br/>"),
            c => out.push(c),
        }
    }
    out
}

/// Renders the trace as a Mermaid `sequenceDiagram`.
///
/// Arrows: `->>` a delivered and consumed message; `-)` a message still in flight at the
/// horizon; `-x` an omitted message; `--x` a send lost because the sender had crashed.
/// Highlighted messages are prefixed `★` and boxed. Notes mark crash points (`💀`), the end
/// of finite failures (`EFF`) and, with [`LamportOptions::show_outputs`], external outputs
/// (`⇒`).
pub fn mermaid(trace: &Trace, opts: &LamportOptions) -> String {
    let mut out = String::new();
    if let Some(title) = &opts.title {
        let _ = writeln!(out, "---\ntitle: {}\n---", title.replace('\n', " "));
    }
    out.push_str("sequenceDiagram\n");
    let shown: Vec<usize> = (0..trace.nodes.len())
        .filter(|n| opts.node_shown(trace, *n))
        .collect();
    for node in &shown {
        let _ = writeln!(
            out,
            "    participant p{node} as {}",
            escape_mermaid(&trace.nodes[*node].label)
        );
    }
    for node in &shown {
        if trace.nodes[*node].crash == Some(Crash::BeforeAnySend) {
            let _ = writeln!(out, "    Note over p{node}: 💀 crashed before any send");
        }
    }
    for row in rows(trace, opts) {
        match row {
            Row::Eff => {
                let (Some(first), Some(last)) = (shown.first(), shown.last()) else {
                    continue;
                };
                let _ = writeln!(
                    out,
                    "    Note over p{first},p{last}: EFF: end of finite failures"
                );
            }
            Row::Crash(node) => {
                let _ = writeln!(out, "    Note over p{node}: 💀 crashed");
            }
            Row::Output(event) => {
                let TraceEventKind::Output { display, dropped } = &trace.events[event].kind else {
                    unreachable!("an output row names an output event")
                };
                let _ = writeln!(
                    out,
                    "    Note over p{}: ⇒ {}{}",
                    trace.events[event].node,
                    escape_mermaid(&opts.label(display)),
                    if *dropped {
                        " (suppressed: node crashed)"
                    } else {
                        ""
                    }
                );
            }
            Row::Message(index) => {
                let m = &trace.messages[index];
                let (arrow, suffix) = match (m.fate, m.recv_event) {
                    (Fate::Delivered, Some(_)) => ("->>", ""),
                    (Fate::Delivered, None) => ("-)", " (in flight)"),
                    (Fate::Omitted, _) => ("-x", " (omitted)"),
                    (Fate::Crashed, _) => ("--x", " (lost: sender crashed)"),
                };
                let highlighted = opts.highlight.contains(&index);
                let star = if highlighted { "★ " } else { "" };
                let text = escape_mermaid(&format!(
                    "{star}{}: {}{suffix}",
                    m.channel,
                    opts.label(&m.display)
                ));
                if highlighted {
                    out.push_str("    rect rgb(255, 243, 205)\n");
                }
                let _ = writeln!(out, "    p{}{arrow}p{}: {text}", m.from, m.to);
                if highlighted {
                    out.push_str("    end\n");
                }
            }
        }
    }
    out
}

/// Escapes text for a double-quoted DOT string.
fn escape_dot(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
    out
}

const HIGHLIGHT: &str = "#d97706";
const LANE: &str = "gray60";
const LOST: &str = "#dc2626";

/// Renders the trace as a Graphviz DOT spacetime diagram: a lane per node, one rank per
/// event in global order (an invisible time spine fixes the ranks), a message as an edge
/// from its send to its receive. An omitted message is a red dashed edge to a `✗`; a send
/// lost to the sender's crash a gray dashed edge; a message in flight at the horizon a dotted
/// edge to the bottom of the destination's lane. Highlighted messages are bold and colored.
/// `💀` marks a crash point, and the end of finite failures is a labelled rank.
pub fn dot(trace: &Trace, opts: &LamportOptions) -> String {
    let mut out = String::new();
    out.push_str("digraph lamport {\n");
    out.push_str("    rankdir=TB;\n    nodesep=0.9;\n    ranksep=0.18;\n    splines=line;\n");
    out.push_str("    node [fontname=\"Helvetica\", fontsize=10];\n");
    out.push_str("    edge [fontname=\"Helvetica\", fontsize=9];\n");
    if let Some(title) = &opts.title {
        let _ = writeln!(out, "    labelloc=t;\n    label=\"{}\";", escape_dot(title));
    }
    let shown: Vec<usize> = (0..trace.nodes.len())
        .filter(|n| opts.node_shown(trace, *n))
        .collect();

    // The ranks: one per shown event (plus the headers, EFF, and the horizon), in order.
    // `event_rank[i]` is the spine node of event `i` when it is shown.
    let mut spine: Vec<String> = vec!["t_head".to_owned()];
    let mut event_spine: Vec<Option<usize>> = vec![None; trace.events.len()];
    let mut eff_spine: Option<usize> = None;
    for (index, event) in trace.events.iter().enumerate() {
        if trace.eff_cut == Some(index) {
            eff_spine = Some(spine.len());
            spine.push("t_eff".to_owned());
        }
        let shown_event = opts.node_shown(trace, event.node)
            && match &event.kind {
                TraceEventKind::Exec => false,
                TraceEventKind::Send(m) | TraceEventKind::Recv(m) => {
                    opts.message_shown(trace, &trace.messages[*m])
                }
                TraceEventKind::Output { .. } => opts.show_outputs,
            };
        if shown_event {
            event_spine[index] = Some(spine.len());
            spine.push(format!("t{index}"));
        }
    }
    if trace.eff_cut == Some(trace.events.len()) {
        eff_spine = Some(spine.len());
        spine.push("t_eff".to_owned());
    }
    spine.push("t_end".to_owned());

    // The spine.
    out.push_str("    // time spine\n");
    for t in &spine {
        let label = if t == "t_eff" {
            "label=\"EFF\", shape=plaintext, fontcolor=gray40, fontsize=9"
        } else {
            "shape=point, style=invis"
        };
        let _ = writeln!(out, "    {t} [{label}];");
    }
    let _ = writeln!(
        out,
        "    {} [style=invis, weight=1000];",
        spine.join(" -> ")
    );

    // Headers, left to right in node order.
    out.push_str("    // lanes\n");
    out.push_str("    { rank=same; t_head;");
    for node in &shown {
        out.push_str(&format!(" h{node};"));
    }
    out.push_str(" }\n");
    for node in &shown {
        let info = &trace.nodes[*node];
        let _ = writeln!(
            out,
            "    h{node} [shape=box, style=\"rounded,filled\", fillcolor=\"#eeeeee\", label=\"{}\"];",
            escape_dot(&info.label)
        );
    }
    if shown.len() > 1 {
        let heads: Vec<String> = shown.iter().map(|n| format!("h{n}")).collect();
        let _ = writeln!(out, "    {} [style=invis];", heads.join(" -> "));
    }

    // Each lane: its shown events chained top to bottom, a crash marker where it dies, an
    // end point at the horizon.
    let mut lane_end: Vec<String> = vec![String::new(); trace.nodes.len()];
    for node in &shown {
        let info = &trace.nodes[*node];
        let mut chain: Vec<String> = vec![format!("h{node}")];
        if info.crash == Some(Crash::BeforeAnySend) {
            let _ = writeln!(
                out,
                "    crash{node} [shape=plaintext, label=\"💀\", fontsize=14];"
            );
            chain.push(format!("crash{node}"));
        }
        for (index, event) in trace.events.iter().enumerate() {
            if event.node != *node {
                continue;
            }
            let Some(t) = event_spine[index] else {
                continue;
            };
            let name = format!("e{index}");
            let attrs = match &event.kind {
                TraceEventKind::Send(m) | TraceEventKind::Recv(m) => {
                    let color = if opts.highlight.contains(m) {
                        HIGHLIGHT
                    } else {
                        "black"
                    };
                    format!("shape=point, width=0.09, color=\"{color}\"")
                }
                TraceEventKind::Output { display, dropped } => format!(
                    "shape=note, fontsize=9, style=filled, fillcolor=\"{}\", label=\"⇒ {}\"",
                    if *dropped { "#fde8e8" } else { "#fff7d6" },
                    escape_dot(&opts.label(display))
                ),
                TraceEventKind::Exec => unreachable!("exec events are never shown"),
            };
            let _ = writeln!(out, "    {name} [{attrs}];");
            let _ = writeln!(out, "    {{ rank=same; {}; {name}; }}", spine[t]);
            chain.push(name);
            if info.crash == Some(Crash::AfterEvent(index)) {
                let _ = writeln!(
                    out,
                    "    crash{node} [shape=plaintext, label=\"💀\", fontsize=14];"
                );
                chain.push(format!("crash{node}"));
            }
        }
        let _ = writeln!(
            out,
            "    end{node} [shape=point, width=0.05, color=\"{LANE}\"];"
        );
        let _ = writeln!(out, "    {{ rank=same; t_end; end{node}; }}");
        chain.push(format!("end{node}"));
        lane_end[*node] = format!("end{node}");
        let _ = writeln!(
            out,
            "    {} [arrowhead=none, color=\"{LANE}\", weight=100];",
            chain.join(" -> ")
        );
    }
    if let Some(t) = eff_spine {
        let _ = writeln!(out, "    // the end of finite failures: {}", spine[t]);
    }

    // Messages.
    out.push_str("    // messages\n");
    for (index, m) in trace.messages.iter().enumerate() {
        if !opts.message_shown(trace, m) {
            continue;
        }
        let highlighted = opts.highlight.contains(&index);
        let label = escape_dot(&format!(
            "{}{}: {}",
            if highlighted { "★ " } else { "" },
            m.channel,
            opts.label(&m.display)
        ));
        let emphasis = if highlighted {
            format!(", penwidth=2.2, color=\"{HIGHLIGHT}\", fontcolor=\"{HIGHLIGHT}\"")
        } else {
            String::new()
        };
        let from = format!("e{}", m.send_event);
        match (m.fate, m.recv_event) {
            (Fate::Delivered, Some(recv)) => {
                let _ = writeln!(
                    out,
                    "    {from} -> e{recv} [label=\"{label}\", constraint=false{emphasis}];"
                );
            }
            (Fate::Delivered, None) => {
                let _ = writeln!(
                    out,
                    "    {from} -> {} [label=\"{label} (in flight)\", style=dotted, arrowhead=open, constraint=false{emphasis}];",
                    lane_end[m.to]
                );
            }
            (Fate::Omitted, _) => {
                let _ = writeln!(
                    out,
                    "    lost{index} [shape=plaintext, label=\"✗\", fontcolor=\"{LOST}\", fontsize=14];"
                );
                let _ = writeln!(
                    out,
                    "    {{ rank=same; {}; lost{index}; }}",
                    spine[event_spine[m.send_event].expect("a shown message's send is shown")]
                );
                let _ = writeln!(
                    out,
                    "    {from} -> lost{index} [label=\"{label} (omitted)\", style=dashed, color=\"{LOST}\", fontcolor=\"{LOST}\", constraint=false];"
                );
            }
            (Fate::Crashed, _) => {
                let _ = writeln!(
                    out,
                    "    lost{index} [shape=plaintext, label=\"✗\", fontcolor=gray50, fontsize=12];"
                );
                let _ = writeln!(
                    out,
                    "    {{ rank=same; {}; lost{index}; }}",
                    spine[event_spine[m.send_event].expect("a shown message's send is shown")]
                );
                let _ = writeln!(
                    out,
                    "    {from} -> lost{index} [label=\"{label} (sender crashed)\", style=dashed, color=gray50, fontcolor=gray50, constraint=false];"
                );
            }
        }
    }
    out.push_str("}\n");
    out
}

#[cfg(test)]
mod tests {
    use super::super::trace::tests::two_node_trace;
    use super::*;

    #[test]
    fn mermaid_marks_fates() {
        let trace = two_node_trace();
        let opts = LamportOptions::default().highlight([0]).title("test");
        let out = mermaid(&trace, &opts);
        assert!(
            out.starts_with("---\ntitle: test\n---\nsequenceDiagram\n"),
            "{out}"
        );
        assert!(out.contains("participant p0 as sender"), "{out}");
        assert!(out.contains("p0->>p1: ★ msg: Msg { attempt: 1 }"), "{out}");
        assert!(out.contains("rect rgb(255, 243, 205)"), "{out}");
        assert!(
            out.contains("p0-xp1: msg: Msg { attempt: 2 } (omitted)"),
            "{out}"
        );
        assert!(
            out.contains("p0-)p1: msg: Msg { attempt: 3 } (in flight)"),
            "{out}"
        );
        assert!(
            out.contains("p1-)p0: ack: Ack { attempt: 1 } (in flight)"),
            "{out}"
        );
        assert!(
            out.contains("p0--xp1: msg: Msg { attempt: 4 } (lost: sender crashed)"),
            "{out}"
        );
        assert!(out.contains("Note over p0: 💀 crashed"), "{out}");
        // The crash is drawn right after the send that triggered it.
        let crash = out.find("💀 crashed").unwrap();
        assert!(
            out.find("attempt: 3 } (in flight)").unwrap() < crash,
            "{out}"
        );
        assert!(crash < out.find("Ack { attempt: 1 }").unwrap(), "{out}");
        assert!(out.contains("Note over p0,p1: EFF"), "{out}");
        assert!(out.contains("Note over p1: ⇒ Msg { attempt: 1 }"), "{out}");
        // EFF (before event 4) precedes the receive of message 0 (event 5), which under
        // receive placement is where that message is drawn.
        let eff = out.find("EFF").unwrap();
        let first = out.find("p0->>p1").unwrap();
        assert!(eff < first, "{out}");
        // Under send placement the message is drawn at its send (event 1), before EFF.
        let opts = LamportOptions {
            placement: Placement::Send,
            ..LamportOptions::default()
        };
        let out = mermaid(&trace, &opts);
        assert!(
            out.find("p0->>p1").unwrap() < out.find("EFF").unwrap(),
            "{out}"
        );
    }

    #[test]
    fn dot_is_well_formed() {
        let trace = two_node_trace();
        let opts = LamportOptions::default().highlight([0]);
        let out = dot(&trace, &opts);
        assert!(out.starts_with("digraph lamport {\n"), "{out}");
        assert!(out.trim_end().ends_with('}'), "{out}");
        assert_eq!(out.matches('{').count(), out.matches('}').count(), "{out}");
        assert!(out.contains("h0 [shape=box"), "{out}");
        assert!(
            out.contains("e1 -> e5 [label=\"★ msg: Msg { attempt: 1 }\""),
            "{out}"
        );
        assert!(
            out.contains("e2 -> lost1 [label=\"msg: Msg { attempt: 2 } (omitted)\""),
            "{out}"
        );
        assert!(
            out.contains("e3 -> end1 [label=\"msg: Msg { attempt: 3 } (in flight)\""),
            "{out}"
        );
        assert!(
            out.contains("e9 -> lost4 [label=\"msg: Msg { attempt: 4 } (sender crashed)\""),
            "{out}"
        );
        assert!(out.contains("t_eff [label=\"EFF\""), "{out}");
        assert!(
            out.contains("crash0 [shape=plaintext, label=\"💀\""),
            "{out}"
        );
        assert!(out.contains("e6 [shape=note"), "{out}");
    }

    #[test]
    fn escapes() {
        assert_eq!(escape_mermaid("a;b#c\nd"), "a#59;b#35;c<br/>d");
        assert_eq!(escape_dot("a\"b\\c\nd"), "a\\\"b\\\\c\\nd");
    }
}
