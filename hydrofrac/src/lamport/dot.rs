//! The DOT backend: Molly's spacetime layout with every position pinned (`neato -n2`).

use std::collections::{BTreeSet, HashSet};
use std::fmt::Write as _;

use super::{Trace, TraceEventKind, TraceMessage};
use crate::fault::{DeliveryId, Fate};

/// How [`dot`] draws a [`Trace`].
#[derive(Clone, Debug)]
pub struct Options {
    /// A title (for instance the label and the verdict).
    pub title: Option<String>,
    /// Messages to emphasize, by delivery id (for instance the support of a fact).
    pub highlight: BTreeSet<DeliveryId>,
    /// Whether to draw eager locations (observers) and the traffic into them.
    pub show_eager: bool,
    /// Whether to draw external outputs as notes beside their lane.
    pub show_outputs: bool,
    /// Whether to draw consecutive identical rounds once, with a `×k` bracket.
    pub collapse_rounds: bool,
    /// Labels longer than this many characters are cut; their full text goes to the legend.
    pub max_label: usize,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            title: None,
            highlight: BTreeSet::new(),
            show_eager: false,
            show_outputs: true,
            collapse_rounds: true,
            max_label: 40,
        }
    }
}

impl Options {
    /// Sets the title.
    #[must_use]
    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }

    /// Emphasizes the messages with these delivery ids.
    #[must_use]
    pub fn highlight(mut self, ids: impl IntoIterator<Item = DeliveryId>) -> Self {
        self.highlight.extend(ids);
        self
    }
}

/// Drops module paths from type names in a `Debug` rendering: `a::b::Name` becomes `Name`, and
/// `MemberId::<crate::m::Role>(1)` becomes `MemberId::<Role>(1)`.
pub(super) fn strip_paths(text: &str) -> String {
    let bytes = text.as_bytes();
    let is_ident = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < bytes.len() {
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

/// Breaks a long line into lines of about `width` characters at `, ` boundaries.
fn wrap(line: &str, width: usize) -> Vec<String> {
    let mut lines = vec![];
    let mut current = String::new();
    for (i, piece) in line.split(", ").enumerate() {
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

/// Escapes text for a double-quoted DOT string.
pub(super) fn escape_dot(text: &str) -> String {
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

/// Escapes text for a Graphviz HTML-like label.
pub(super) fn escape_html(text: &str) -> String {
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

const HIGHLIGHT: &str = "#d35400";
const LANE: &str = "gray55";
const DEAD_LANE: &str = "gray80";
const LOST: &str = "#c0392b";
const CRASHED: &str = "gray50";
const HEAL: &str = "#27ae60";
const REPEAT: &str = "gray45";

/// Vertical distance between consecutive ranks, in points.
const RANK_GAP: f64 = 16.0;

/// Approximate advance of one character of 9pt Helvetica, in points (generous, so that a wider
/// substitute font still fits).
const CHAR_WIDTH: f64 = 5.3;

/// Height of a one-line 9pt label box, in points.
const LABEL_HEIGHT: f64 = 11.0;

/// The longest legend entry kept, in characters.
const MAX_LEGEND: usize = 240;

/// An axis-aligned box (center and size, in points) that text occupies.
#[derive(Clone, Copy, Debug)]
struct Box {
    x: f64,
    y: f64,
    w: f64,
    h: f64,
}

// Free functions rather than methods: the staged copy of this crate keeps no inherent impls of
// private types.

/// The box `chars` characters of `pt`-point text centered at `(x, y)` occupy.
fn text_box(x: f64, y: f64, chars: usize, pt: f64) -> Box {
    Box {
        x,
        y,
        w: CHAR_WIDTH * (pt / 9.0) * chars as f64 + 6.0,
        h: LABEL_HEIGHT * (pt / 9.0),
    }
}

fn overlaps(a: &Box, b: &Box) -> bool {
    (a.x - b.x).abs() * 2.0 < a.w + b.w && (a.y - b.y).abs() * 2.0 < a.h + b.h
}

/// Whether the segment from `a` to `b` passes through `bx` (Liang–Barsky clipping).
fn crossed_by(bx: &Box, a: (f64, f64), b: (f64, f64)) -> bool {
    let (dx, dy) = (b.0 - a.0, b.1 - a.1);
    let (mut t0, mut t1) = (0.0f64, 1.0f64);
    for (p, q) in [
        (-dx, a.0 - (bx.x - bx.w / 2.0)),
        (dx, (bx.x + bx.w / 2.0) - a.0),
        (-dy, a.1 - (bx.y - bx.h / 2.0)),
        (dy, (bx.y + bx.h / 2.0) - a.1),
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

/// Picks where a message's label goes: on its straight edge, in one of the lane gaps it
/// crosses, at the first spot that overlaps no `taken` box, is crossed by none of the other
/// edges (`lines`) and keeps clear of both lanes of the gap. Tries spots just above and below
/// the edge next, and falls back to the middle of the first gap.
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
                let candidate = Box {
                    x,
                    y: at_x(x) + dy,
                    w: width,
                    h: LABEL_HEIGHT,
                };
                if fallback.is_none() {
                    fallback = Some((candidate.x, candidate.y));
                }
                if !taken.iter().any(|b| overlaps(b, &candidate))
                    && !lines.iter().any(|(a, b)| crossed_by(&candidate, *a, *b))
                {
                    return (candidate.x, candidate.y);
                }
            }
        }
    }
    fallback.unwrap_or_else(|| {
        let x = xa + dir * 0.5 * lane_gap;
        (x, at_x(x))
    })
}

/// Cuts long labels, keeping their full text in a numbered legend.
struct Legend {
    entries: Vec<String>,
    max: usize,
}

/// `text` with module paths stripped, cut to `legend.max` characters with a reference to its
/// legend entry when it is longer.
fn cut(legend: &mut Legend, text: &str) -> String {
    let text = strip_paths(text);
    if text.chars().count() <= legend.max {
        return text;
    }
    let k = if let Some(i) = legend.entries.iter().position(|e| *e == text) {
        i + 1
    } else {
        legend.entries.push(text.clone());
        legend.entries.len()
    };
    let prefix: String = text
        .chars()
        .take(legend.max.saturating_sub(4).max(1))
        .collect();
    format!("{prefix}…‹{k}›")
}

/// What one drawable item of the run is, for finding repeated rounds.
#[derive(PartialEq, Eq)]
enum Sig<'a> {
    Message {
        from: usize,
        to: usize,
        channel: &'a str,
        display: &'a str,
        fate: Fate,
        received: bool,
    },
    Output {
        node: usize,
        port: usize,
        display: &'a str,
    },
    /// A crash or the heal: never part of a repeated round.
    Barrier(usize),
}

/// A round drawn once for `times` consecutive identical ones: the events of the first.
struct Repeat {
    events: Vec<usize>,
    times: usize,
}

/// The longest round looked for, in items.
const MAX_ROUND: usize = 64;

/// Finds consecutive identical rounds among the drawable items: returns the events of every
/// round after the first of each run of repeats (to hide), and the repeats.
fn collapse(
    trace: &Trace,
    node_shown: &dyn Fn(usize) -> bool,
    message_shown: &dyn Fn(usize) -> bool,
    show_outputs: bool,
) -> (HashSet<usize>, Vec<Repeat>) {
    let mut items: Vec<(Sig<'_>, Vec<usize>)> = vec![];
    for (index, event) in trace.events.iter().enumerate() {
        match &event.kind {
            TraceEventKind::Send(m) if message_shown(*m) => {
                let msg = &trace.messages[*m];
                items.push((
                    Sig::Message {
                        from: msg.from,
                        to: msg.to,
                        channel: &msg.channel,
                        display: &msg.display,
                        fate: msg.fate,
                        received: msg.recv_event.is_some(),
                    },
                    std::iter::once(index).chain(msg.recv_event).collect(),
                ));
            }
            TraceEventKind::Output { port, display } => {
                let node = event.node.expect("an output has a node");
                if show_outputs && node_shown(node) {
                    items.push((
                        Sig::Output {
                            node,
                            port: *port,
                            display,
                        },
                        vec![index],
                    ));
                }
            }
            TraceEventKind::Crash | TraceEventKind::Heal => {
                items.push((Sig::Barrier(index), vec![]));
            }
            TraceEventKind::Exec | TraceEventKind::Send(_) | TraceEventKind::Recv(_) => {}
        }
    }
    let same = |a: usize, b: usize, len: usize| (0..len).all(|j| items[a + j].0 == items[b + j].0);
    let mut hidden = HashSet::new();
    let mut repeats = vec![];
    let mut i = 0;
    while i < items.len() {
        // The round starting here that hides the most items (the shortest among equals).
        let mut best: Option<(usize, usize)> = None;
        for len in 1..=MAX_ROUND.min((items.len() - i) / 2) {
            let mut times = 1;
            while i + (times + 1) * len <= items.len() && same(i, i + times * len, len) {
                times += 1;
            }
            if times >= 2 && best.is_none_or(|(l, t)| (times - 1) * len > (t - 1) * l) {
                best = Some((len, times));
            }
        }
        match best {
            Some((len, times)) => {
                for (_, events) in &items[i + len..i + times * len] {
                    hidden.extend(events.iter().copied());
                }
                repeats.push(Repeat {
                    events: items[i..i + len]
                        .iter()
                        .flat_map(|(_, e)| e.iter().copied())
                        .collect(),
                    times,
                });
                i += times * len;
            }
            None => i += 1,
        }
    }
    (hidden, repeats)
}

/// One thing drawn on a lane at a rank: the DOT node's name and its attributes.
struct LaneItem {
    rank: usize,
    node: usize,
    name: String,
    attrs: String,
}

/// Renders the trace as a Graphviz DOT spacetime diagram (render with `neato -n2`).
///
/// A lane per node, grouped by location with members in order, and time flowing down with one
/// rank per drawn instant in program order. Every position is pinned (`pos="x,y!"`), so lanes are
/// exactly vertical and as far apart as the longest label needs; message labels are placed here
/// too, each on its edge in a lane gap where nothing else is drawn. Only the instants that matter
/// are drawn: sends, receives, outputs, crashes and the heal.
pub fn dot(trace: &Trace, opts: &Options) -> String {
    let node_shown = |n: usize| opts.show_eager || !trace.nodes[n].eager;
    let message_shown = |m: usize| {
        let msg = &trace.messages[m];
        node_shown(msg.from) && node_shown(msg.to)
    };
    let shown: Vec<usize> = (0..trace.nodes.len()).filter(|n| node_shown(*n)).collect();
    let lane_of = |node: usize| shown.iter().position(|n| *n == node);
    let (hidden, repeats) = if opts.collapse_rounds {
        collapse(trace, &node_shown, &message_shown, opts.show_outputs)
    } else {
        (HashSet::new(), vec![])
    };
    let dead_at = |node: usize, event: usize| trace.nodes[node].crash.is_some_and(|c| event > c);

    // Ranks, in time order; 0 is the header row. A lost send gets an extra rank right below it
    // for the `✗` on the receiver's lane.
    let mut ranks = 1usize;
    let mut next_rank = || {
        ranks += 1;
        ranks - 1
    };
    let mut event_rank: Vec<Option<usize>> = vec![None; trace.events.len()];
    let mut lost_rank: Vec<Option<usize>> = vec![None; trace.messages.len()];
    let mut crash_rank: Vec<Option<usize>> = vec![None; trace.nodes.len()];
    let mut heal_rank: Option<usize> = None;
    for (index, event) in trace.events.iter().enumerate() {
        let Some(node) = event.node else {
            if event.kind == TraceEventKind::Heal && heal_rank.is_none() {
                heal_rank = Some(next_rank());
            }
            continue;
        };
        if !node_shown(node) {
            continue;
        }
        let drawn = !hidden.contains(&index)
            && match &event.kind {
                TraceEventKind::Send(m) => message_shown(*m),
                TraceEventKind::Recv(m) => {
                    message_shown(*m) && trace.messages[*m].recv_event == Some(index)
                }
                TraceEventKind::Output { .. } => opts.show_outputs,
                TraceEventKind::Exec | TraceEventKind::Crash | TraceEventKind::Heal => false,
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
        if event.kind == TraceEventKind::Crash && trace.nodes[node].crash == Some(index) {
            crash_rank[node] = Some(next_rank());
        }
    }
    let horizon = next_rank();

    // Labels, in time order (so legend numbers follow the run).
    let mut legend = Legend {
        entries: vec![],
        max: opts.max_label,
    };
    let mut message_text: Vec<Option<String>> = vec![None; trace.messages.len()];
    let mut output_text: Vec<Option<String>> = vec![None; trace.events.len()];
    for (index, event) in trace.events.iter().enumerate() {
        if event_rank[index].is_none() {
            continue;
        }
        match &event.kind {
            TraceEventKind::Send(m) => {
                let msg = &trace.messages[*m];
                let star = if opts.highlight.contains(&msg.id) {
                    "★ "
                } else {
                    ""
                };
                message_text[*m] = Some(format!(
                    "{star}{}: {}",
                    msg.channel,
                    cut(&mut legend, &msg.display)
                ));
            }
            TraceEventKind::Output { display, .. } => {
                output_text[index] = Some(format!("⇒ {}", cut(&mut legend, display)));
            }
            _ => {}
        }
    }

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
        let (Some(rank), Some(node)) = (event_rank[index], event.node) else {
            continue;
        };
        let attrs = match &event.kind {
            TraceEventKind::Send(m) | TraceEventKind::Recv(m) => {
                let color = if opts.highlight.contains(&trace.messages[*m].id) {
                    HIGHLIGHT
                } else if dead_at(node, index) {
                    CRASHED
                } else {
                    "black"
                };
                format!("shape=point, width=0.08, color=\"{color}\"")
            }
            TraceEventKind::Output { .. } => "shape=point, width=0.06, color=\"gray40\"".to_owned(),
            TraceEventKind::Exec | TraceEventKind::Crash | TraceEventKind::Heal => {
                unreachable!("only sends, receives and outputs have event ranks")
            }
        };
        items.push(LaneItem {
            rank,
            node,
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
    let longest = message_text
        .iter()
        .flatten()
        .map(|t| t.chars().count())
        .chain(shown.iter().map(|n| trace.nodes[*n].label.chars().count()))
        .max()
        .unwrap_or(0);
    let lane_gap = (CHAR_WIDTH * longest as f64 + 66.0).clamp(96.0, 400.0);
    let x = |lane: usize| lane as f64 * lane_gap;
    // `0.0 - …` rather than a negation: rank 0 must print as `0.0`, not `-0.0`.
    let y = |rank: usize| 0.0 - rank as f64 * RANK_GAP;
    let pos = |lane: usize, rank: usize| format!("pos=\"{:.1},{:.1}!\"", x(lane), y(rank));
    let last_x = x(shown.len().saturating_sub(1));

    let mut out = String::new();
    out.push_str("digraph lamport {\n");
    out.push_str("    layout=neato;\n    splines=false;\n    overlap=true;\n    outputorder=edgesfirst;\n    pad=\"0.3,0.2\";\n");
    out.push_str("    node [fontname=\"Helvetica\", fontsize=10];\n");
    out.push_str("    edge [fontname=\"Helvetica\", fontsize=9];\n");
    let mut heading = String::new();
    if let Some(title) = &opts.title {
        let lines: Vec<String> = wrap(&strip_paths(title), 100)
            .iter()
            .map(|l| escape_html(l))
            .collect();
        let _ = write!(
            heading,
            "<FONT POINT-SIZE=\"12\">{}</FONT><BR/>",
            lines.join("<BR/>")
        );
    }
    for (what, list) in [("faults", &trace.faults), ("never fired", &trace.unfired)] {
        if list.is_empty() {
            continue;
        }
        let line = format!("{what}: {}", strip_paths(&list.join(", ")));
        let lines: Vec<String> = wrap(&line, 110).iter().map(|l| escape_html(l)).collect();
        let _ = write!(
            heading,
            "<FONT POINT-SIZE=\"9\">{}</FONT><BR/>",
            lines.join("<BR/>")
        );
    }
    let key =
        "— delivered    ⋯ in flight    ╌ ✗ omitted (red) / lost to a crash (gray)    ★ highlighted";
    let _ = writeln!(
        out,
        "    labelloc=t;\n    fontname=\"Helvetica\";\n    label=<{heading}<FONT POINT-SIZE=\"8\" COLOR=\"gray40\">{}</FONT>>;",
        escape_html(key)
    );

    // The heal: a dashed rule across the lanes, labelled at the right.
    if let Some(rank) = heal_rank {
        let left = -0.3 * lane_gap.min(90.0);
        let right = last_x + 0.4 * lane_gap.min(90.0);
        let _ = writeln!(
            out,
            "    // the heal\n    heal_l [shape=point, style=invis, width=0.01, pos=\"{left:.1},{:.1}!\"];",
            y(rank)
        );
        let _ = writeln!(
            out,
            "    heal_r [shape=point, style=invis, width=0.01, pos=\"{right:.1},{:.1}!\"];",
            y(rank)
        );
        let _ = writeln!(
            out,
            "    t_heal [label=\"heal\", shape=plaintext, fontcolor=\"{HEAL}\", fontsize=9, margin=\"0.04,0\", height=0.12, pos=\"{:.1},{:.1}!\"];",
            right + 14.0,
            y(rank)
        );
        let _ = writeln!(
            out,
            "    heal_l -> heal_r [style=dashed, color=\"{HEAL}\", arrowhead=none];"
        );
    }

    // Lanes: a header, then each item top to bottom, chained; the chain is faint and dotted once
    // the node has died.
    out.push_str("    // lanes\n");
    for (lane, node) in shown.iter().enumerate() {
        let _ = writeln!(
            out,
            "    h{node} [shape=box, style=\"rounded,filled\", fillcolor=\"#eeeeee\", color=\"#bbbbbb\", height=0.3, label=\"{}\", {}];",
            escape_dot(&trace.nodes[*node].label),
            pos(lane, 0)
        );
    }
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
            taken.push(text_box(x(lane), y(item.rank), 9, 11.0));
        }
    }
    for node in &shown {
        let mut prev = format!("h{node}");
        let mut dead = false;
        let crash = format!("crash{node}");
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
            if item.name == crash {
                dead = true;
            }
            prev.clone_from(&item.name);
        }
    }

    // Outputs: a note just right of the lane, level with its instant.
    let mut notes = String::new();
    let mut right_extent = last_x + 24.0;
    for (index, event) in trace.events.iter().enumerate() {
        let (Some(rank), Some(text), Some(node)) =
            (event_rank[index], &output_text[index], event.node)
        else {
            continue;
        };
        let lane = lane_of(node).expect("drawn events are on shown lanes");
        let chars = text.chars().count();
        let bx = text_box(
            x(lane) + 7.0 + (CHAR_WIDTH * chars as f64 + 10.0) / 2.0,
            y(rank),
            chars,
            9.0,
        );
        let _ = writeln!(
            notes,
            "    o{index} [shape=note, fontsize=9, style=filled, fillcolor=\"#fff7d6\", color=\"gray40\", margin=\"0.05,0.02\", height=0.2, label=\"{}\", pos=\"{:.1},{:.1}!\"];",
            escape_dot(text),
            bx.x,
            bx.y
        );
        right_extent = right_extent.max(bx.x + bx.w / 2.0 + 12.0);
        taken.push(bx);
    }
    if !notes.is_empty() {
        out.push_str("    // outputs\n");
        out.push_str(&notes);
    }

    // Repeated rounds: a bracket right of everything, spanning the round drawn, labelled `×k`.
    let mut brackets = String::new();
    for (i, repeat) in repeats.iter().enumerate() {
        let drawn: Vec<usize> = repeat
            .events
            .iter()
            .filter_map(|e| event_rank[*e])
            .collect();
        let (Some(top), Some(bottom)) = (drawn.iter().min(), drawn.iter().max()) else {
            continue;
        };
        let (ya, yb) = (y(*top) + 5.0, y(*bottom) - 5.0);
        let _ = writeln!(
            brackets,
            "    rep{i}a [shape=point, style=invis, width=0.01, pos=\"{right_extent:.1},{ya:.1}!\"];\n    rep{i}b [shape=point, style=invis, width=0.01, pos=\"{right_extent:.1},{yb:.1}!\"];\n    rep{i}a -> rep{i}b [arrowhead=tee, arrowtail=tee, dir=both, arrowsize=0.5, color=\"{REPEAT}\"];\n    rep{i}l [shape=plaintext, label=\"×{}\", fontsize=10, fontcolor=\"{REPEAT}\", tooltip=\"this round repeated {} times; drawn once\", pos=\"{:.1},{:.1}!\"];",
            repeat.times,
            repeat.times,
            right_extent + 16.0,
            (ya + yb) / 2.0
        );
    }
    if !brackets.is_empty() {
        out.push_str("    // repeated rounds, drawn once\n");
        out.push_str(&brackets);
    }

    // Messages: a straight edge, and its label placed on the edge in a lane gap it crosses. A
    // message a node sends itself bends out of its lane through `via`, so it is visible.
    struct Drawn<'a> {
        index: usize,
        msg: &'a TraceMessage,
        to: String,
        kind: &'static str,
        line: String,
        head: &'static str,
        a: (f64, f64),
        b: (f64, f64),
    }
    let drawn: Vec<Drawn<'_>> = trace
        .messages
        .iter()
        .enumerate()
        .filter(|(i, m)| message_shown(*i) && event_rank[m.send_event].is_some())
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
                (Fate::SenderDead, _) => (
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
                msg: m,
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
        let (index, m) = (d.index, d.msg);
        let (kind, line, head, a, b) = (d.kind, &d.line, d.head, d.a, d.b);
        let (from, to) = (format!("e{}", m.send_event), &d.to);
        let highlighted = opts.highlight.contains(&m.id);
        let text = message_text[index]
            .as_deref()
            .expect("a drawn message has a label");
        let label = escape_dot(text);
        let fontcolor = if highlighted {
            HIGHLIGHT
        } else {
            match m.fate {
                Fate::Omitted => LOST,
                Fate::SenderDead => CRASHED,
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

    // The legend: the full text of every label that was cut, under the diagram.
    if !legend.entries.is_empty() {
        let mut rows = String::new();
        let mut widest = 0usize;
        for (i, entry) in legend.entries.iter().enumerate() {
            let mut text: String = entry.chars().take(MAX_LEGEND).collect();
            if entry.chars().count() > MAX_LEGEND {
                text.push('…');
            }
            let tag = format!("‹{}›", i + 1);
            widest = widest.max(tag.chars().count() + 2 + text.chars().count());
            let _ = write!(
                rows,
                "<TR><TD ALIGN=\"LEFT\">{tag}</TD><TD ALIGN=\"LEFT\">{}</TD></TR>",
                escape_html(&text)
            );
        }
        let w = CHAR_WIDTH * widest as f64;
        let h = (LABEL_HEIGHT + 2.0) * legend.entries.len() as f64;
        let _ = writeln!(
            out,
            "    // legend: labels cut short\n    legend [shape=plaintext, fontsize=9, label=<<TABLE BORDER=\"0\" CELLBORDER=\"0\" CELLSPACING=\"0\" CELLPADDING=\"1\">{rows}</TABLE>>, pos=\"{:.1},{:.1}!\"];",
            -0.3 * lane_gap.min(90.0) + w / 2.0,
            y(horizon) - 24.0 - h / 2.0
        );
    }
    out.push_str("}\n");
    out
}
