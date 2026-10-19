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
        let text = strip_paths(text);
        let mut chars = text.chars();
        let truncated: String = chars.by_ref().take(self.max_label).collect();
        if chars.next().is_some() {
            format!("{truncated}…")
        } else {
            truncated
        }
    }
}

/// Drops module paths from type names in a `Debug` rendering: `a::b::Name` → `Name`
/// (`MemberId::<crate::m::Role>(1)` → `MemberId::<Role>(1)`). A path segment is an identifier
/// followed by `::` and another identifier.
fn strip_paths(text: &str) -> String {
    let bytes = text.as_bytes();
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < bytes.len() {
        // At the start of an identifier: is it `ident::` followed by an identifier?
        if is_ident(bytes[i]) && (i == 0 || !is_ident(bytes[i - 1])) {
            let mut j = i;
            while j < bytes.len() && is_ident(bytes[j]) {
                j += 1;
            }
            if bytes.get(j) == Some(&b':')
                && bytes.get(j + 1) == Some(&b':')
                && bytes.get(j + 2).is_some_and(|b| is_ident(*b))
            {
                i = j + 2;
                continue;
            }
            out.push_str(&text[i..j]);
            i = j;
            continue;
        }
        let c = text[i..].chars().next().expect("in bounds");
        out.push(c);
        i += c.len_utf8();
    }
    out
}

/// Breaks a long title into lines of about `width` characters at `, ` boundaries, for a
/// diagram no wider than its lanes need.
fn wrap_title(title: &str, width: usize) -> Vec<String> {
    let mut lines = vec![];
    let mut current = String::new();
    for (i, piece) in title.split(", ").enumerate() {
        if i > 0 {
            current.push_str(", ");
        }
        if current.chars().count() + piece.chars().count() > width && !current.trim().is_empty() {
            lines.push(current.trim_end().to_owned());
            current = String::new();
        }
        current.push_str(piece);
    }
    if !current.is_empty() {
        lines.push(current);
    }
    lines
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

const HIGHLIGHT: &str = "#d35400";
const LANE: &str = "gray55";
const DEAD_LANE: &str = "gray80";
const LOST: &str = "#c0392b";
const CRASHED: &str = "gray50";
const EFF: &str = "gray45";

/// Escapes text for a Graphviz HTML-like label.
fn escape_html(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\n' => out.push_str("<BR/>"),
            c => out.push(c),
        }
    }
    out
}

/// One thing drawn on a lane at a rank: the DOT node's name and its attributes.
struct LaneItem {
    rank: usize,
    node: usize,
    name: String,
    attrs: String,
}

/// Vertical distance between consecutive ranks, in points.
const RANK_GAP: f64 = 16.0;

/// Approximate advance of one character of 9pt Helvetica, in points (generous, so that a
/// wider substitute font still fits).
const CHAR_WIDTH: f64 = 5.3;

/// Height of a one-line 9pt label box, in points.
const LABEL_HEIGHT: f64 = 11.0;

/// An axis-aligned box (center and size, in points) that text occupies; label placement
/// keeps labels off each other and off the lanes' wider marks.
#[derive(Clone, Copy, Debug)]
struct Box {
    x: f64,
    y: f64,
    w: f64,
    h: f64,
}

impl Box {
    fn text(x: f64, y: f64, chars: usize, pt: f64) -> Box {
        Box {
            x,
            y,
            w: CHAR_WIDTH * (pt / 9.0) * chars as f64 + 6.0,
            h: LABEL_HEIGHT * (pt / 9.0),
        }
    }

    fn overlaps(&self, other: &Box) -> bool {
        (self.x - other.x).abs() * 2.0 < self.w + other.w
            && (self.y - other.y).abs() * 2.0 < self.h + other.h
    }

    /// Whether the segment from `a` to `b` passes through the box (Liang–Barsky clipping).
    fn crossed_by(&self, a: (f64, f64), b: (f64, f64)) -> bool {
        let (dx, dy) = (b.0 - a.0, b.1 - a.1);
        let (mut t0, mut t1) = (0.0f64, 1.0f64);
        for (p, q) in [
            (-dx, a.0 - (self.x - self.w / 2.0)),
            (dx, (self.x + self.w / 2.0) - a.0),
            (-dy, a.1 - (self.y - self.h / 2.0)),
            (dy, (self.y + self.h / 2.0) - a.1),
        ] {
            if p == 0.0 {
                if q < 0.0 {
                    return false;
                }
            } else {
                let t = q / p;
                if p < 0.0 {
                    t0 = t0.max(t);
                } else {
                    t1 = t1.min(t);
                }
            }
        }
        t0 < t1
    }
}

/// Picks where a message's label goes: on its (straight) edge, in one of the lane gaps the
/// edge crosses, at the first of a few spots along the gap that collides with none of `taken`,
/// is crossed by none of the `lines` (the other messages), and keeps clear of both lanes
/// bounding the gap. Tries the gap nearest the sender first, then the others; when no spot on
/// the edge is free, tries the same spots just above or below it (the label still touches its
/// edge). Falls back to the middle of the first gap.
fn place_label(
    from: (f64, f64),
    to: (f64, f64),
    lane_gap: f64,
    width: f64,
    taken: &[Box],
    lines: &[((f64, f64), (f64, f64))],
) -> (f64, f64) {
    let (xa, ya) = from;
    let (xb, yb) = to;
    let lanes_crossed = ((xb - xa).abs() / lane_gap).round().max(1.0) as usize;
    let dir = if xb >= xa { 1.0 } else { -1.0 };
    let at_x = |x: f64| ya + (x - xa) / (xb - xa) * (yb - ya);
    let clearance = width / 2.0 + 4.0;
    let mut fallback = None;
    for dy in [0.0, -(LABEL_HEIGHT + 1.0), LABEL_HEIGHT + 1.0] {
        for gap in 0..lanes_crossed {
            let left = xa + dir * gap as f64 * lane_gap;
            for t in [0.5, 0.4, 0.6, 0.3, 0.7, 0.2, 0.8] {
                if t * lane_gap < clearance || (1.0 - t) * lane_gap < clearance {
                    continue;
                }
                let x = left + dir * t * lane_gap;
                let y = at_x(x) + dy;
                let candidate = Box {
                    x,
                    y,
                    w: width,
                    h: LABEL_HEIGHT,
                };
                if fallback.is_none() {
                    fallback = Some((x, y));
                }
                if !taken.iter().any(|b| b.overlaps(&candidate))
                    && !lines.iter().any(|(a, b)| candidate.crossed_by(*a, *b))
                {
                    return (x, y);
                }
            }
        }
    }
    fallback.unwrap_or_else(|| {
        let x = xa + dir * 0.5 * lane_gap;
        (x, at_x(x))
    })
}

/// Renders the trace as a Graphviz DOT spacetime diagram in Molly's style: a lane per node
/// (grouped by role, members in order), time flowing down with one rank per event in global
/// order, a straight edge per message from its send to its receive. Everything is pinned
/// (`pos="x,y!"`, `layout=neato`; render with `neato -n2`), so lanes are exactly vertical and
/// lane spacing is exactly what the longest label needs. Labels are placed here too, not by
/// Graphviz: each sits on its edge in a lane gap the edge crosses, at a spot that no other
/// label, edge, crash mark or output note occupies (`place_label`).
///
/// Only the instants that matter are drawn as points: a message's send and receive, an
/// output (with its note beside the lane), a crash. An omitted message is a red dashed edge
/// from its send to a `✗` on the lane of the node that should have received it; a send lost
/// because the sender had already crashed the same in gray. A message still in flight at the
/// horizon is a dotted edge to the bottom of the destination's lane. `☠ crashed` ends a lane
/// (Graphviz cannot render 4-byte UTF-8 such as `💀`); what the dead node still did is drawn on
/// a faint dotted continuation. The end of finite failures is a dashed rule across all lanes
/// labelled `EFF`. Highlighted messages ([`LamportOptions::highlight`]) are bold, colored, and
/// prefixed `★`. Module paths are stripped from `Debug` text in labels and the title.
pub fn dot(trace: &Trace, opts: &LamportOptions) -> String {
    // Lanes: the nodes grouped by location (in order of first appearance), members in order.
    let mut shown: Vec<usize> = (0..trace.nodes.len())
        .filter(|n| opts.node_shown(trace, *n))
        .collect();
    let location_order = |node: usize| {
        trace
            .nodes
            .iter()
            .position(|n| n.location == trace.nodes[node].location)
            .expect("the node's own location is in the list")
    };
    shown.sort_by_key(|n| (location_order(*n), trace.nodes[*n].member));
    let lane_of = |node: usize| shown.iter().position(|n| *n == node);
    let message_shown = |m: usize| opts.message_shown(trace, &trace.messages[m]);
    // The receive that a message's edge ends at: a message released by a scripted hook is
    // recorded twice (the top-level receive, then the release), and the trace pairs it with
    // the last.
    let is_recv_endpoint = |event: usize, m: usize| trace.messages[m].recv_event == Some(event);
    let crash_event = |node: usize| match trace.nodes[node].crash {
        Some(Crash::AfterEvent(event)) => Some(event),
        _ => None,
    };
    let dead_at = |node: usize, event: usize| match trace.nodes[node].crash {
        Some(Crash::BeforeAnySend) => true,
        Some(Crash::AfterEvent(crash)) => event > crash,
        None => false,
    };

    // Ranks, in time order: 0 is the header row. `event_rank[i]` is the rank of event `i`
    // when it is drawn; an omitted or crashed send gets an extra rank right below it for the
    // `✗` on the receiver's lane, a crash one for the `☠`.
    let mut ranks = 1usize;
    let mut event_rank: Vec<Option<usize>> = vec![None; trace.events.len()];
    let mut lost_rank: Vec<Option<usize>> = vec![None; trace.messages.len()];
    let mut crash_rank: Vec<Option<usize>> = vec![None; trace.nodes.len()];
    let mut eff_rank: Option<usize> = None;
    let mut next_rank = || {
        ranks += 1;
        ranks - 1
    };
    for node in &shown {
        if trace.nodes[*node].crash == Some(Crash::BeforeAnySend) {
            crash_rank[*node] = Some(next_rank());
        }
    }
    for (index, event) in trace.events.iter().enumerate() {
        if trace.eff_cut == Some(index) {
            eff_rank = Some(next_rank());
        }
        if !opts.node_shown(trace, event.node) {
            continue;
        }
        let drawn = match &event.kind {
            TraceEventKind::Exec => false,
            TraceEventKind::Send(m) => message_shown(*m),
            TraceEventKind::Recv(m) => message_shown(*m) && is_recv_endpoint(index, *m),
            TraceEventKind::Output { .. } => opts.show_outputs,
        };
        if drawn {
            event_rank[index] = Some(next_rank());
        }
        if let TraceEventKind::Send(m) = &event.kind
            && drawn
            && trace.messages[*m].fate != Fate::Delivered
        {
            lost_rank[*m] = Some(next_rank());
        }
        if crash_event(event.node) == Some(index) {
            crash_rank[event.node] = Some(next_rank());
        }
    }
    if trace.eff_cut == Some(trace.events.len()) {
        eff_rank = Some(next_rank());
    }
    let horizon = next_rank();

    // What each lane shows.
    let mut items: Vec<LaneItem> = vec![];
    for node in &shown {
        if let Some(rank) = crash_rank[*node] {
            items.push(LaneItem {
                rank,
                node: *node,
                name: format!("crash{node}"),
                attrs: "shape=plaintext, style=filled, fillcolor=white, label=\"☠ crashed\", fontsize=11, margin=\"0.04,0\", height=0.16".to_owned(),
            });
        }
    }
    for (index, event) in trace.events.iter().enumerate() {
        let Some(rank) = event_rank[index] else {
            continue;
        };
        let dead = dead_at(event.node, index);
        let attrs = match &event.kind {
            TraceEventKind::Send(m) | TraceEventKind::Recv(m) => {
                let color = if opts.highlight.contains(m) {
                    HIGHLIGHT
                } else if dead {
                    CRASHED
                } else {
                    "black"
                };
                format!("shape=point, width=0.08, color=\"{color}\"")
            }
            // The instant on the lane; the note itself is placed beside the lane below.
            TraceEventKind::Output { .. } => "shape=point, width=0.06, color=\"gray40\"".to_owned(),
            TraceEventKind::Exec => unreachable!("exec events are never drawn"),
        };
        items.push(LaneItem {
            rank,
            node: event.node,
            name: format!("e{index}"),
            attrs,
        });
    }
    for (index, m) in trace.messages.iter().enumerate() {
        let Some(rank) = lost_rank[index] else {
            continue;
        };
        let color = if m.fate == Fate::Omitted {
            LOST
        } else {
            CRASHED
        };
        items.push(LaneItem {
            rank,
            node: m.to,
            name: format!("lost{index}"),
            attrs: format!(
                "shape=plaintext, label=\"✗\", fontcolor=\"{color}\", fontsize=13, width=0.2, height=0.2, fixedsize=true, margin=0"
            ),
        });
    }
    for node in &shown {
        items.push(LaneItem {
            rank: horizon,
            node: *node,
            name: format!("end{node}"),
            attrs: format!("shape=point, width=0.04, color=\"{LANE}\""),
        });
    }
    items.sort_by_key(|item| item.rank);

    // Message labels decide how far apart the lanes are.
    let label_of = |index: usize, m: &TraceMessage| {
        format!(
            "{}{}: {}",
            if opts.highlight.contains(&index) {
                "★ "
            } else {
                ""
            },
            m.channel,
            opts.label(&m.display)
        )
    };
    let longest = trace
        .messages
        .iter()
        .enumerate()
        .filter(|(_, m)| opts.message_shown(trace, m))
        .map(|(i, m)| label_of(i, m).chars().count())
        .max()
        .unwrap_or(0);
    // Room for the longest label plus clearance from both lanes.
    let lane_gap = (CHAR_WIDTH * longest as f64 + 66.0).clamp(96.0, 400.0);
    let x = |lane: usize| lane as f64 * lane_gap;
    // `0.0 - …` rather than a negation: rank 0 must print as `0.0`, not `-0.0`.
    let y = |rank: usize| 0.0 - rank as f64 * RANK_GAP;
    let pos = |lane: usize, rank: usize| format!("pos=\"{:.1},{:.1}!\"", x(lane), y(rank));

    let mut out = String::new();
    out.push_str("digraph lamport {\n");
    out.push_str("    layout=neato;\n    splines=false;\n    overlap=true;\n    outputorder=edgesfirst;\n    pad=\"0.3,0.2\";\n");
    out.push_str("    node [fontname=\"Helvetica\", fontsize=10];\n");
    out.push_str("    edge [fontname=\"Helvetica\", fontsize=9];\n");
    let legend =
        "— delivered    ⋯ in flight    ╌ ✗ omitted (red) / lost to a crash (gray)    ★ support";
    let _ = match &opts.title {
        Some(title) => writeln!(
            out,
            "    labelloc=t;\n    fontname=\"Helvetica\";\n    label=<<FONT POINT-SIZE=\"12\">{}</FONT><BR/><FONT POINT-SIZE=\"8\" COLOR=\"gray40\">{}</FONT>>;",
            wrap_title(&strip_paths(title), 100)
                .iter()
                .map(|line| escape_html(line))
                .collect::<Vec<_>>()
                .join("<BR/>"),
            escape_html(legend)
        ),
        None => writeln!(
            out,
            "    labelloc=t;\n    fontname=\"Helvetica\";\n    label=<<FONT POINT-SIZE=\"8\" COLOR=\"gray40\">{}</FONT>>;",
            escape_html(legend)
        ),
    };

    // The end of finite failures: a dashed rule across the lanes, labelled at the right.
    if let Some(rank) = eff_rank {
        let right = x(shown.len().saturating_sub(1)) + 0.4 * lane_gap.min(90.0);
        let _ = writeln!(
            out,
            "    // the end of finite failures\n    eff_l [shape=point, style=invis, width=0.01, pos=\"{:.1},{:.1}!\"];",
            -0.3 * lane_gap.min(90.0),
            y(rank)
        );
        let _ = writeln!(
            out,
            "    eff_r [shape=point, style=invis, width=0.01, pos=\"{right:.1},{:.1}!\"];",
            y(rank)
        );
        let _ = writeln!(
            out,
            "    t_eff [label=\"EFF\", shape=plaintext, fontcolor=\"{EFF}\", fontsize=9, margin=\"0.04,0\", height=0.12, pos=\"{:.1},{:.1}!\"];",
            right + 12.0,
            y(rank)
        );
        let _ = writeln!(
            out,
            "    eff_l -> eff_r [style=dashed, color=\"{EFF}\", arrowhead=none];"
        );
    }

    // Lanes: a header, then each item top to bottom, chained; the chain is faint and dotted
    // once the node has died.
    out.push_str("    // lanes\n");
    for (lane, node) in shown.iter().enumerate() {
        let info = &trace.nodes[*node];
        let _ = writeln!(
            out,
            "    h{node} [shape=box, style=\"rounded,filled\", fillcolor=\"#eeeeee\", color=\"#bbbbbb\", height=0.3, label=\"{}\", {}];",
            escape_dot(&info.label),
            pos(lane, 0)
        );
    }
    // Text wide enough to reach into a lane gap: labels keep clear of it.
    let mut taken: Vec<Box> = vec![];
    for item in &items {
        let lane = lane_of(item.node).expect("items are on shown lanes");
        let _ = writeln!(
            out,
            "    {} [{}, {}];",
            item.name,
            item.attrs,
            pos(lane, item.rank)
        );
        if item.name.starts_with("crash") {
            taken.push(Box::text(x(lane), y(item.rank), 9, 11.0));
        }
    }
    for node in &shown {
        let mut prev = format!("h{node}");
        let mut dead = trace.nodes[*node].crash == Some(Crash::BeforeAnySend);
        for item in items.iter().filter(|item| item.node == *node) {
            let style = if dead {
                format!("style=dotted, color=\"{DEAD_LANE}\"")
            } else {
                format!("color=\"{LANE}\"")
            };
            let _ = writeln!(
                out,
                "    {prev} -> {} [arrowhead=none, {style}];",
                item.name
            );
            if item.name == format!("crash{node}") {
                dead = true;
            }
            prev = item.name.clone();
        }
    }

    // Outputs: a note just right of the lane, level with its instant.
    let mut notes = String::new();
    for (index, event) in trace.events.iter().enumerate() {
        let (Some(rank), TraceEventKind::Output { display, dropped }) =
            (event_rank[index], &event.kind)
        else {
            continue;
        };
        let lane = lane_of(event.node).expect("drawn events are on shown lanes");
        let text = format!("⇒ {}", opts.label(display));
        let chars = text.chars().count();
        let bx = Box::text(
            x(lane) + 7.0 + (CHAR_WIDTH * chars as f64 + 10.0) / 2.0,
            y(rank),
            chars,
            9.0,
        );
        let _ = writeln!(
            notes,
            "    o{index} [shape=note, fontsize=9, style=filled, fillcolor=\"{}\", color=\"{}\", margin=\"0.05,0.02\", height=0.2, label=\"{}\", pos=\"{:.1},{:.1}!\"];",
            if *dropped { "#fde8e8" } else { "#fff7d6" },
            if *dropped { CRASHED } else { "gray40" },
            escape_dot(&text),
            bx.x,
            bx.y
        );
        taken.push(bx);
    }
    if !notes.is_empty() {
        out.push_str("    // outputs\n");
        out.push_str(&notes);
    }

    // Messages: a straight edge, and its label placed on the edge in a lane gap it crosses.
    // A message a node sends itself bends out of its lane through `via`, so it is visible.
    struct Drawn {
        index: usize,
        to: String,
        kind: &'static str,
        line: String,
        head: &'static str,
        a: (f64, f64),
        b: (f64, f64),
    }
    let drawn: Vec<Drawn> = trace
        .messages
        .iter()
        .enumerate()
        .filter(|(_, m)| opts.message_shown(trace, m) && event_rank[m.send_event].is_some())
        .map(|(index, m)| {
            let (to, to_rank, kind, line, head) = match (m.fate, m.recv_event) {
                (Fate::Delivered, Some(recv)) => (
                    format!("e{recv}"),
                    event_rank[recv].expect("the receive of a drawn message is drawn"),
                    "",
                    String::new(),
                    "arrowsize=0.7",
                ),
                (Fate::Delivered, None) => (
                    format!("end{}", m.to),
                    horizon,
                    " (in flight)",
                    "style=dotted, ".to_owned(),
                    "arrowhead=open, arrowsize=0.7",
                ),
                (Fate::Omitted, _) => (
                    format!("lost{index}"),
                    lost_rank[index].expect("a lost message has its mark"),
                    " (omitted)",
                    format!("style=dashed, color=\"{LOST}\", "),
                    "arrowhead=none",
                ),
                (Fate::Crashed, _) => (
                    format!("lost{index}"),
                    lost_rank[index].expect("a lost message has its mark"),
                    " (sender crashed)",
                    format!("style=dashed, color=\"{CRASHED}\", "),
                    "arrowhead=none",
                ),
            };
            let from_lane = lane_of(m.from).expect("shown message");
            let to_lane = lane_of(m.to).expect("shown message");
            Drawn {
                index,
                to,
                kind,
                line,
                head,
                a: (x(from_lane), y(event_rank[m.send_event].expect("drawn"))),
                b: (x(to_lane), y(to_rank)),
            }
        })
        .collect();
    out.push_str("    // messages\n");
    let mut labels = String::new();
    for (i, d) in drawn.iter().enumerate() {
        let m = &trace.messages[d.index];
        let (index, kind, line, head, a, b) = (d.index, d.kind, &d.line, d.head, d.a, d.b);
        let (from, to) = (format!("e{}", m.send_event), &d.to);
        let highlighted = opts.highlight.contains(&index);
        let text = label_of(index, m);
        let label = escape_dot(&text);
        let fontcolor = if highlighted {
            HIGHLIGHT
        } else {
            match m.fate {
                Fate::Omitted => LOST,
                Fate::Crashed => CRASHED,
                Fate::Delivered => "black",
            }
        };
        let emphasis = if highlighted {
            format!(", penwidth=2, color=\"{HIGHLIGHT}\"")
        } else {
            String::new()
        };
        let width = CHAR_WIDTH * text.chars().count() as f64 + 6.0;
        let (lx, ly) = if a.0 == b.0 {
            // Bend 14pt out of the lane (to the left on the last lane) and label beside it.
            let last = lane_of(m.from) == Some(shown.len() - 1) && shown.len() > 1;
            let dir = if last { -1.0 } else { 1.0 };
            let vx = a.0 + dir * 14.0;
            let vy = (a.1 + b.1) / 2.0;
            let _ = writeln!(
                out,
                "    via{index} [shape=point, style=invis, width=0.01, pos=\"{vx:.1},{vy:.1}!\"];"
            );
            let _ = writeln!(
                out,
                "    {from} -> via{index} [tooltip=\"{label}{kind}\", {line}arrowhead=none{emphasis}];"
            );
            let _ = writeln!(
                out,
                "    via{index} -> {to} [tooltip=\"{label}{kind}\", {line}{head}{emphasis}];"
            );
            (vx + dir * (width / 2.0 + 2.0), vy)
        } else {
            let _ = writeln!(
                out,
                "    {from} -> {to} [tooltip=\"{label}{kind}\", {line}{head}{emphasis}];"
            );
            let others: Vec<_> = drawn
                .iter()
                .enumerate()
                .filter(|(j, _)| *j != i)
                .map(|(_, o)| (o.a, o.b))
                .collect();
            place_label(a, b, lane_gap, width, &taken, &others)
        };
        taken.push(Box {
            x: lx,
            y: ly,
            w: width,
            h: LABEL_HEIGHT,
        });
        let _ = writeln!(
            labels,
            "    l{index} [shape=plaintext, style=filled, fillcolor=white, fontsize=9, fontcolor=\"{fontcolor}\", margin=\"0.03,0\", height=0.14, label=\"{label}\", pos=\"{lx:.1},{ly:.1}!\"];"
        );
    }
    out.push_str("    // message labels\n");
    out.push_str(&labels);
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
        let opts = LamportOptions::default().highlight([0]).title("t");
        let out = dot(&trace, &opts);
        assert!(out.starts_with("digraph lamport {\n"), "{out}");
        assert!(out.trim_end().ends_with('}'), "{out}");
        assert_eq!(out.matches('{').count(), out.matches('}').count(), "{out}");
        assert!(out.contains("layout=neato"), "{out}");
        assert!(out.contains("splines=false"), "{out}");
        // Lanes are pinned: the sender's header and its points share an x.
        assert!(
            out.contains("h0 [") && out.contains("pos=\"0.0,0.0!\""),
            "{out}"
        );
        assert!(
            out.contains("e1 [shape=point, width=0.08, color=\"#d35400\", pos=\"0.0,-16.0!\"]"),
            "{out}"
        );
        assert!(out.contains("h0 [shape=box"), "{out}");
        // Message edges carry no label of their own: the label is a pinned node of its own,
        // placed in the lane gap the edge crosses.
        assert!(
            out.contains("e1 -> e5 [tooltip=\"★ msg: Msg { attempt: 1 }\", arrowsize=0.7, penwidth=2, color=\"#d35400\"]"),
            "{out}"
        );
        assert!(
            out.contains("l0 [shape=plaintext, style=filled, fillcolor=white, fontsize=9, fontcolor=\"#d35400\""),
            "{out}"
        );
        assert!(
            out.contains("label=\"★ msg: Msg { attempt: 1 }\", pos=\""),
            "{out}"
        );
        assert!(!out.contains("xlabel"), "{out}");
        // An omitted message ends at a `✗` on the receiver's lane, one rank below its send.
        assert!(
            out.contains(
                "e2 -> lost1 [tooltip=\"msg: Msg { attempt: 2 } (omitted)\", style=dashed"
            ),
            "{out}"
        );
        assert!(out.contains("lost1 [shape=plaintext, label=\"✗\""), "{out}");
        assert!(out.contains("h1 -> lost1 [arrowhead=none"), "{out}");
        assert!(
            out.contains("e3 -> end1 [tooltip=\"msg: Msg { attempt: 3 } (in flight)\", style=dotted, arrowhead=open"),
            "{out}"
        );
        // The sender died after event 3: its lane ends in `☠` and continues faintly, and the
        // send it attempted afterwards is a gray stub to the receiver's lane.
        assert!(
            out.contains(
                "crash0 [shape=plaintext, style=filled, fillcolor=white, label=\"☠ crashed\""
            ),
            "{out}"
        );
        assert!(
            out.contains("e3 -> crash0 [arrowhead=none, color=\"gray55\""),
            "{out}"
        );
        assert!(
            out.contains("crash0 -> e9 [arrowhead=none, style=dotted, color=\"gray80\""),
            "{out}"
        );
        assert!(
            out.contains("e9 -> lost4 [tooltip=\"msg: Msg { attempt: 4 } (sender crashed)\", style=dashed, color=\"gray50\""),
            "{out}"
        );
        // EFF is a dashed rule across the lanes, labelled at the right.
        assert!(out.contains("t_eff [label=\"EFF\""), "{out}");
        assert!(out.contains("eff_l -> eff_r [style=dashed"), "{out}");
        // An output is an instant on the lane and a note beside it.
        assert!(out.contains("e6 [shape=point, width=0.06"), "{out}");
        assert!(out.contains("o6 [shape=note"), "{out}");
        assert!(
            out.contains("label=\"⇒ Msg { attempt: 1 }\", pos=\""),
            "{out}"
        );
        // Only message endpoints, outputs and crashes are drawn: no exec points.
        assert!(!out.contains("e0 ["), "{out}");
        assert!(!out.contains("e4 ["), "{out}");
        assert!(!out.contains("e8 ["), "{out}");
        assert!(
            out.contains("label=<<FONT POINT-SIZE=\"12\">t</FONT>"),
            "{out}"
        );
    }

    #[test]
    fn strips_paths_and_wraps_titles() {
        assert_eq!(
            strip_paths(
                "Ballot { proposer_id: MemberId::<hydro_test::cluster::paxos::Proposer>(1) }"
            ),
            "Ballot { proposer_id: MemberId::<Proposer>(1) }"
        );
        assert_eq!(strip_paths("a::b::C::d(1)"), "d(1)");
        assert_eq!(strip_paths("x: 1, y: Some(\"x\")"), "x: 1, y: Some(\"x\")");
        assert_eq!(
            wrap_title("goal violated by {omit(a), omit(b), crash(c)}", 24),
            ["goal violated by {omit(a),", "omit(b), crash(c)}"]
        );
        assert_eq!(wrap_title("short", 24), ["short"]);
    }

    #[test]
    fn escapes() {
        assert_eq!(escape_mermaid("a;b#c\nd"), "a#59;b#35;c<br/>d");
        assert_eq!(escape_dot("a\"b\\c\nd"), "a\\\"b\\\\c\\nd");
        assert_eq!(
            escape_html("a<b>&\"c\"\nd"),
            "a&lt;b&gt;&amp;&quot;c&quot;<BR/>d"
        );
    }
}
