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
/// longest message label needs; a message's label sits on its edge, in the first lane gap it
/// crosses where it covers no other label, and an output's note just right of its lane.
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
        heading.push(title.to_owned());
    }
    for (what, list) in [("faults", &trace.faults), ("never fired", &trace.unfired)] {
        if !list.is_empty() {
            heading.push(format!("{what}: {}", list.join(", ")));
        }
    }
    heading.push("— delivered    ⋯ in flight    ╌ ✗ omitted (red) / lost to a crash (gray)".into());
    let _ = writeln!(
        out,
        "    labelloc=t;\n    fontname=\"Helvetica\";\n    fontsize=10;\n    label=\"{}\";",
        escape(&heading.join("\n"))
    );

    // Every lane's points, as (lane, rank, DOT node name), chained into the lane below.
    let mut items: Vec<(usize, usize, String)> = vec![];
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
    }
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
            TraceEventKind::Crash => (
                format!("crash{lane}"),
                "shape=plaintext, style=filled, fillcolor=white, label=\"☠ crashed\", fontsize=11, margin=\"0.04,0\", height=0.16".to_owned(),
            ),
            TraceEventKind::Output { .. } => {
                let note_x = x(lane) + 22.0 + CHAR_WIDTH * text[i].chars().count() as f64 / 2.0;
                let _ = writeln!(
                    out,
                    "    o{i} [shape=note, style=filled, fillcolor=\"#fff7d6\", color=\"gray40\", margin=\"0.05,0.02\", height=0.2, label=\"{}\", {}];",
                    escape(&text[i]),
                    pos(note_x, y(r))
                );
                (format!("e{i}"), "shape=point, width=0.06, color=\"gray40\"".to_owned())
            }
            _ => {
                let color = if dead { CRASHED } else { "black" };
                (format!("e{i}"), format!("shape=point, width=0.08, color=\"{color}\""))
            }
        };
        let _ = writeln!(out, "    {name} [{attrs}, {}];", pos(x(lane), y(r)));
        items.push((lane, r, name));
    }

    // Messages: a straight edge (a self-send bends out of its lane through `via`), labelled.
    let mut placed: Vec<(f64, f64)> = vec![];
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
            let (vx, vy) = (a.0 + 14.0, (a.1 + b.1) / 2.0);
            let _ = writeln!(
                out,
                "    via{index} [shape=point, style=invis, width=0.01, {}];\n    {from} -> via{index} [{attrs}, arrowhead=none];\n    via{index} -> {to} [{attrs}];",
                pos(vx, vy)
            );
            (vx + width / 2.0 + 2.0, vy)
        } else {
            let _ = writeln!(out, "    {from} -> {to} [{attrs}];");
            // In the first lane gap it crosses where no other label sits.
            let at = |k: usize| {
                let lx = a.0 + (b.0 - a.0).signum() * gap * (k as f64 + 0.5);
                (lx, a.1 + (lx - a.0) / (b.0 - a.0) * (b.1 - a.1))
            };
            let free = |&(lx, ly): &(f64, f64)| {
                !placed
                    .iter()
                    .any(|&(px, py): &(f64, f64)| px == lx && (py - ly).abs() < 12.0)
            };
            let spot = (0..m.from.abs_diff(m.to))
                .map(at)
                .find(free)
                .unwrap_or_else(|| at(0));
            placed.push(spot);
            spot
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
            pos(last_x / 2.0, y(horizon) - 16.0 - 7.0 * legend.len() as f64)
        );
    }
    out.push_str("}\n");
    out
}
