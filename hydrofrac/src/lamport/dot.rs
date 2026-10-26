//! The DOT backend: Molly's spacetime layout, one lane per node and time flowing down, with every
//! position pinned (render with `neato -n2`).

use std::fmt::Write as _;

use super::{Trace, TraceEventKind};
use crate::fault::Fate;

/// Labels longer than this many characters are cut; their full text goes to the legend.
const MAX_LABEL: usize = 40;
const LANE: &str = "gray55";
const LOST: &str = "#c0392b";
const CRASHED: &str = "gray50";
const HEAL: &str = "#27ae60";
/// Vertical distance between consecutive ranks, in points.
const RANK_GAP: f64 = 16.0;
/// Approximate advance of one character of 9pt Helvetica, in points.
const CHAR_WIDTH: f64 = 5.3;
/// The height of a one-line message label, in points.
const LABEL_HEIGHT: f64 = 11.0;
/// The clearance kept around a placed message label, in points.
const LABEL_MARGIN: f64 = 4.0;
/// Heading lines longer than this many characters are wrapped.
const MAX_HEADING: usize = 110;

/// A box a label must not cover: centre and size, in points.
#[derive(Clone, Copy, Debug)]
struct Rect {
    x: f64,
    y: f64,
    w: f64,
    h: f64,
}

/// The area `a` and `b` share. (A free function: the staged copy of the crate keeps no `impl`
/// of a private type.)
fn overlap(a: &Rect, b: &Rect) -> f64 {
    let dx = (a.w + b.w) / 2.0 - (a.x - b.x).abs();
    let dy = (a.h + b.h) / 2.0 - (a.y - b.y).abs();
    if dx > 0.0 && dy > 0.0 { dx * dy } else { 0.0 }
}

/// A straight piece of an edge, from one point to another.
type Segment = ((f64, f64), (f64, f64));

/// Whether the segment crosses the inside of `r` (Liang–Barsky clipping).
fn crosses(r: &Rect, ((px, py), (qx, qy)): Segment) -> bool {
    let (dx, dy) = (qx - px, qy - py);
    let (left, right) = (r.x - r.w / 2.0, r.x + r.w / 2.0);
    let (bottom, top) = (r.y - r.h / 2.0, r.y + r.h / 2.0);
    let (mut t0, mut t1) = (0.0_f64, 1.0_f64);
    for (p, q) in [
        (-dx, px - left),
        (dx, right - px),
        (-dy, py - bottom),
        (dy, top - py),
    ] {
        if p == 0.0 {
            if q <= 0.0 {
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

/// What one of a label's crossings of another edge costs, against the area it covers of
/// another label or mark: crossing an edge is better than covering text.
const CROSSING: f64 = 5.0;

/// The first of `candidates` that covers nothing in `taken` and crosses none of `edges`, else
/// the one that costs least.
fn least_covering(candidates: &[Rect], taken: &[Rect], edges: &[Segment]) -> Rect {
    let cost = |c: &Rect| {
        taken.iter().map(|t| overlap(c, t)).sum::<f64>()
            + CROSSING * edges.iter().filter(|e| crosses(c, **e)).count() as f64
    };
    candidates
        .iter()
        .find(|c| cost(c) == 0.0)
        .or_else(|| candidates.iter().min_by(|a, b| cost(a).total_cmp(&cost(b))))
        .copied()
        .expect("a label has a candidate place")
}

/// `line` broken at spaces into lines of at most [`MAX_HEADING`] characters (a longer word
/// stays whole).
fn wrap(line: &str) -> Vec<String> {
    let mut lines: Vec<String> = vec![];
    let mut current = String::new();
    for word in line.split(' ') {
        if !current.is_empty() && current.chars().count() + 1 + word.chars().count() > MAX_HEADING {
            lines.push(std::mem::take(&mut current));
        } else if !current.is_empty() {
            current.push(' ');
        }
        current.push_str(word);
    }
    lines.push(current);
    lines
}

/// Escapes text for a double-quoted DOT string.
pub(super) fn escape(text: &str) -> String {
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

/// `text`, or its first characters and `…‹k›` when longer than [`MAX_LABEL`], with its full text
/// as entry `k` of `legend`.
fn cut(legend: &mut Vec<String>, text: &str) -> String {
    if text.chars().count() <= MAX_LABEL {
        return text.to_owned();
    }
    let k = if let Some(i) = legend.iter().position(|e| e == text) {
        i + 1
    } else {
        legend.push(text.to_owned());
        legend.len()
    };
    let prefix: String = text.chars().take(MAX_LABEL - 4).collect();
    format!("{prefix}…‹{k}›")
}

/// Renders the trace as a Graphviz DOT spacetime diagram, titled `title`.
///
/// Each send, receive, output, crash and the heal gets its own rank, in program order (a lost
/// send gets a second one, for the `✗` on the receiver's lane). Lanes are as far apart as the
/// longest message label or lane note needs. A message's label sits on its edge, inside a lane
/// gap it crosses (a self-send's just right of its bend), at the first place where it covers
/// no other label, mark, note or event point (else where it covers least); an output's note
/// sits just right of its lane, and [`Trace::notes`] in red below their lanes' ends.
pub fn dot(trace: &Trace, title: &str) -> String {
    let mut rank = 0usize;
    let mut event_rank: Vec<Option<usize>> = vec![None; trace.events.len()];
    let mut lost_rank: Vec<Option<usize>> = vec![None; trace.messages.len()];
    let mut legend = vec![];
    let mut text: Vec<String> = vec![String::new(); trace.events.len()];
    for (i, event) in trace.events.iter().enumerate() {
        if event.kind == TraceEventKind::Crash
            && event.node.is_some_and(|n| trace.nodes[n].crash != Some(i))
        {
            continue;
        }
        rank += 1;
        event_rank[i] = Some(rank);
        match &event.kind {
            TraceEventKind::Send(m) => {
                let msg = &trace.messages[*m];
                text[i] = format!("{}: {}", msg.channel, cut(&mut legend, &msg.display));
                if msg.fate != Fate::Delivered {
                    rank += 1;
                    lost_rank[*m] = Some(rank);
                }
            }
            TraceEventKind::Output { display, .. } => {
                text[i] = format!("⇒ {}", cut(&mut legend, display));
            }
            _ => {}
        }
    }
    let horizon = rank + 1;

    let longest = trace
        .events
        .iter()
        .enumerate()
        .filter(|(_, e)| matches!(e.kind, TraceEventKind::Send(_)))
        .map(|(i, _)| text[i].chars().count())
        .chain(trace.nodes.iter().map(|n| n.label.chars().count()))
        .chain(trace.notes.iter().map(|(_, n)| n.chars().count()))
        .max()
        .unwrap_or(0);
    let gap = (CHAR_WIDTH * longest as f64 + 66.0).clamp(96.0, 400.0);
    let x = |lane: usize| lane as f64 * gap;
    // `0.0 - …` rather than a negation: rank 0 must print as `0.0`, not `-0.0`.
    let y = |rank: usize| 0.0 - rank as f64 * RANK_GAP;
    let pos = |x: f64, y: f64| format!("pos=\"{x:.1},{y:.1}!\"");
    let last_x = x(trace.nodes.len().saturating_sub(1));

    let mut out = String::from("digraph lamport {\n");
    out.push_str("    layout=neato;\n    splines=false;\n    overlap=true;\n    outputorder=edgesfirst;\n    pad=\"0.3,0.2\";\n");
    out.push_str("    node [fontname=\"Helvetica\", fontsize=9];\n    edge [arrowsize=0.7];\n");
    let mut heading: Vec<String> = vec![];
    if !title.is_empty() {
        heading.extend(title.lines().flat_map(wrap));
    }
    for (what, list) in [("faults", &trace.faults), ("never fired", &trace.unfired)] {
        if !list.is_empty() {
            heading.extend(wrap(&format!("{what}: {}", list.join(", "))));
        }
    }
    heading.push("— delivered    ⋯ in flight    ╌ ✗ omitted (red) / lost to a crash (gray)".into());
    let _ = writeln!(
        out,
        "    labelloc=t;\n    fontname=\"Helvetica\";\n    fontsize=10;\n    label=\"{}\";",
        escape(&heading.join("\n"))
    );

    // Every message's edge, as segments: a label should cross no other.
    let self_bend = |(ax, ay): (f64, f64), (_, by): (f64, f64)| (ax + 14.0, (ay + by) / 2.0);
    let edges: Vec<Vec<Segment>> = trace
        .messages
        .iter()
        .enumerate()
        .map(|(index, m)| {
            let Some(send_rank) = event_rank[m.send_event] else {
                return vec![];
            };
            let to_rank = match (m.fate, m.recv_event) {
                (Fate::Delivered, Some(recv)) => event_rank[recv].unwrap_or(horizon),
                (Fate::Delivered, None) => horizon,
                _ => lost_rank[index].unwrap_or(horizon),
            };
            let (a, b) = ((x(m.from), y(send_rank)), (x(m.to), y(to_rank)));
            if m.from == m.to {
                let via = self_bend(a, b);
                vec![(a, via), (via, b)]
            } else {
                vec![(a, b)]
            }
        })
        .collect();
    let others = |index: usize| -> Vec<Segment> {
        edges
            .iter()
            .enumerate()
            .filter(|(i, _)| *i != index)
            .flat_map(|(_, e)| e.iter().copied())
            .collect()
    };

    // Every lane's points, as (lane, rank, DOT node name), chained into the lane below.
    let mut items: Vec<(usize, usize, String)> = vec![];
    // What message labels must not cover: event points, marks, notes, then the labels placed.
    let mut taken: Vec<Rect> = vec![];
    // An event point, with room around it for the arrowheads that meet it.
    let point = |x: f64, y: f64| Rect {
        x,
        y,
        w: 14.0,
        h: 14.0,
    };
    // A placed label, with a margin so that two labels never touch.
    let margin = |r: Rect| Rect {
        w: r.w + 2.0 * LABEL_MARGIN,
        h: r.h + 2.0 * LABEL_MARGIN,
        ..r
    };
    for (lane, node) in trace.nodes.iter().enumerate() {
        let _ = writeln!(
            out,
            "    h{lane} [shape=box, style=\"rounded,filled\", fillcolor=\"#eeeeee\", color=\"#bbbbbb\", fontsize=10, height=0.3, label=\"{}\", {}];",
            escape(&node.label),
            pos(x(lane), y(0))
        );
        let _ = writeln!(
            out,
            "    end{lane} [shape=point, width=0.04, color=\"{LANE}\", {}];",
            pos(x(lane), y(horizon))
        );
        items.push((lane, horizon, format!("end{lane}")));
        taken.push(point(x(lane), y(horizon)));
    }
    // What each lane's node ends without, in red below the lane.
    let mut noted = vec![String::new(); trace.nodes.len()];
    for (lane, note) in &trace.notes {
        if !noted[*lane].is_empty() {
            noted[*lane].push('\n');
        }
        noted[*lane].push_str(&cut(&mut legend, note));
    }
    let mut note_lines = 0;
    for (lane, note) in noted.iter().enumerate().filter(|(_, n)| !n.is_empty()) {
        let lines = note.lines().count();
        note_lines = note_lines.max(lines);
        let note_y = y(horizon) - 8.0 - 6.0 * lines as f64;
        let _ = writeln!(
            out,
            "    note{lane} [shape=plaintext, fontcolor=\"{LOST}\", margin=0, height=0.15, label=\"{}\", {}];",
            escape(note),
            pos(x(lane), note_y)
        );
    }
    // The lost marks, before any label is placed.
    for (index, m) in trace.messages.iter().enumerate() {
        if let Some(r) = lost_rank[index] {
            taken.push(Rect {
                x: x(m.to),
                y: y(r),
                w: 12.0,
                h: 14.0,
            });
        }
    }
    // Outputs as (event, lane, rank): their notes are placed once every point is known.
    let mut outputs: Vec<(usize, usize, usize)> = vec![];
    for (i, event) in trace.events.iter().enumerate() {
        let Some(r) = event_rank[i] else { continue };
        let Some(lane) = event.node else {
            // The heal: a dashed rule across the lanes, labelled at the right.
            let (left, right) = (-0.3 * gap.min(90.0), last_x + 0.4 * gap.min(90.0));
            let _ = writeln!(
                out,
                "    heal_l [shape=point, style=invis, width=0.01, {}];\n    heal_r [shape=point, style=invis, width=0.01, {}];\n    t_heal [label=\"heal\", shape=plaintext, fontcolor=\"{HEAL}\", {}];\n    heal_l -> heal_r [style=dashed, color=\"{HEAL}\", arrowhead=none];",
                pos(left, y(r)),
                pos(right, y(r)),
                pos(right + 14.0, y(r))
            );
            continue;
        };
        let dead = trace.nodes[lane].crash.is_some_and(|c| i > c);
        let (name, attrs) = match &event.kind {
            TraceEventKind::Crash => {
                taken.push(Rect {
                    x: x(lane),
                    y: y(r),
                    w: 62.0,
                    h: 15.0,
                });
                (
                format!("crash{lane}"),
                "shape=plaintext, style=filled, fillcolor=white, label=\"☠ crashed\", fontsize=11, margin=\"0.04,0\", height=0.16".to_owned(),
            )
            }
            TraceEventKind::Output { .. } => {
                outputs.push((i, lane, r));
                taken.push(point(x(lane), y(r)));
                (
                    format!("e{i}"),
                    "shape=point, width=0.06, color=\"gray40\"".to_owned(),
                )
            }
            _ => {
                taken.push(point(x(lane), y(r)));
                let color = if dead { CRASHED } else { "black" };
                (
                    format!("e{i}"),
                    format!("shape=point, width=0.08, color=\"{color}\""),
                )
            }
        };
        let _ = writeln!(out, "    {name} [{attrs}, {}];", pos(x(lane), y(r)));
        items.push((lane, r, name));
    }

    // Each output's note just beside its lane: right, else left, of it, where it covers
    // no edge.
    let all_edges: Vec<Segment> = edges.iter().flatten().copied().collect();
    for (i, lane, r) in outputs {
        let w = CHAR_WIDTH * text[i].chars().count() as f64 + 16.0;
        let candidates = [1.0, -1.0].map(|side| Rect {
            x: x(lane) + side * (14.0 + w / 2.0),
            y: y(r),
            w,
            h: 15.0,
        });
        let spot = least_covering(&candidates, &taken, &all_edges);
        let _ = writeln!(
            out,
            "    o{i} [shape=note, style=filled, fillcolor=\"#fff7d6\", color=\"gray40\", margin=\"0.05,0.02\", height=0.2, label=\"{}\", {}];",
            escape(&text[i]),
            pos(spot.x, spot.y)
        );
        taken.push(spot);
    }

    // Messages: a straight edge (a self-send bends out of its lane through `via`), labelled.
    for (index, m) in trace.messages.iter().enumerate() {
        let Some(send_rank) = event_rank[m.send_event] else {
            continue;
        };
        let (to, to_rank, kind, style) = match (m.fate, m.recv_event) {
            (Fate::Delivered, Some(recv)) => (
                format!("e{recv}"),
                event_rank[recv].unwrap(),
                "",
                String::new(),
            ),
            (Fate::Delivered, None) => (
                format!("end{}", m.to),
                horizon,
                " (in flight)",
                "style=dotted, arrowhead=open".to_owned(),
            ),
            (fate, _) => {
                let (kind, color) = if fate == Fate::Omitted {
                    (" (omitted)", LOST)
                } else {
                    (" (sender crashed)", CRASHED)
                };
                let r = lost_rank[index].expect("a lost message has its mark");
                let _ = writeln!(
                    out,
                    "    lost{index} [shape=plaintext, label=\"✗\", fontcolor=\"{color}\", fontsize=13, width=0.2, height=0.2, fixedsize=true, margin=0, {}];",
                    pos(x(m.to), y(r))
                );
                items.push((m.to, r, format!("lost{index}")));
                (
                    format!("lost{index}"),
                    r,
                    kind,
                    format!("style=dashed, color=\"{color}\", arrowhead=none"),
                )
            }
        };
        let label = escape(&text[m.send_event]);
        let mut attrs = format!("tooltip=\"{label}{kind}\"");
        if !style.is_empty() {
            attrs = format!("{attrs}, {style}");
        }
        let (a, b) = ((x(m.from), y(send_rank)), (x(m.to), y(to_rank)));
        let from = format!("e{}", m.send_event);
        let width = CHAR_WIDTH * text[m.send_event].chars().count() as f64 + 6.0;
        let (lx, ly) = if m.from == m.to {
            let (vx, vy) = self_bend(a, b);
            let _ = writeln!(
                out,
                "    via{index} [shape=point, style=invis, width=0.01, {}];\n    {from} -> via{index} [{attrs}, arrowhead=none];\n    via{index} -> {to} [{attrs}];",
                pos(vx, vy)
            );
            // Just right of the bend, at its height or nudged along the loop.
            let half = ((a.1 - b.1).abs() / 2.0).max(0.0);
            let candidates: Vec<Rect> = [0.0, 1.0, -1.0, 2.0, -2.0, 3.0, -3.0]
                .iter()
                .map(|k| vy + k * 4.0)
                .filter(|cy| (cy - vy).abs() <= half)
                .chain([vy + 8.0, vy - 8.0])
                .map(|cy| Rect {
                    x: vx + width / 2.0 + 2.0,
                    y: cy,
                    w: width,
                    h: LABEL_HEIGHT,
                })
                .collect();
            let spot = least_covering(&candidates, &taken, &others(index));
            taken.push(margin(spot));
            (spot.x, spot.y)
        } else {
            let _ = writeln!(out, "    {from} -> {to} [{attrs}];");
            // On the edge, inside a lane gap it crosses: each gap's middle first, then along it.
            let dir = (b.0 - a.0).signum();
            let room = ((gap - width) / 2.0 - 4.0).max(0.0);
            let on_edge = |lx: f64| Rect {
                x: lx,
                y: a.1 + (lx - a.0) / (b.0 - a.0) * (b.1 - a.1),
                w: width,
                h: LABEL_HEIGHT,
            };
            let candidates: Vec<Rect> = (0..m.from.abs_diff(m.to))
                .flat_map(|k| {
                    let middle = a.0 + dir * gap * (k as f64 + 0.5);
                    [0.0, -0.25, 0.25, -0.5, 0.5, -0.75, 0.75, -1.0, 1.0]
                        .into_iter()
                        .map(move |f| middle + f * room)
                })
                .map(on_edge)
                .collect();
            let spot = least_covering(&candidates, &taken, &others(index));
            taken.push(margin(spot));
            (spot.x, spot.y)
        };
        let color = match m.fate {
            Fate::Omitted => LOST,
            Fate::SenderDead => CRASHED,
            Fate::Delivered => "black",
        };
        let _ = writeln!(
            out,
            "    l{index} [shape=plaintext, style=filled, fillcolor=white, fontcolor=\"{color}\", margin=\"0.03,0\", height=0.14, label=\"{label}\", {}];",
            pos(lx, ly)
        );
    }

    // Lanes: header to end through every point, faint and dotted once the node has died.
    items.sort_by_key(|(_, r, _)| *r);
    for lane in 0..trace.nodes.len() {
        let (mut prev, mut dead) = (format!("h{lane}"), false);
        for (_, _, name) in items.iter().filter(|(l, _, _)| *l == lane) {
            let style = if dead {
                "style=dotted, color=\"gray80\""
            } else {
                "color=\"gray55\""
            };
            let _ = writeln!(out, "    {prev} -> {name} [arrowhead=none, {style}];");
            dead |= name.starts_with("crash");
            prev.clone_from(name);
        }
    }

    if !legend.is_empty() {
        let mut rows = String::new();
        for (i, entry) in legend.iter().enumerate() {
            let _ = write!(rows, "‹{}› {}\\l", i + 1, escape(entry));
        }
        let _ = writeln!(
            out,
            "    legend [shape=plaintext, label=\"{rows}\", {}];",
            pos(
                last_x / 2.0,
                y(horizon) - 16.0 - 12.0 * note_lines as f64 - 7.0 * legend.len() as f64
            )
        );
    }
    out.push_str("}\n");
    out
}
