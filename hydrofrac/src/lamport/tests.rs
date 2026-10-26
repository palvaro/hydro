use std::collections::HashMap;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::rc::Rc;

use hydro_lang::compile::ir::HydroIrOpMetadata;
use hydro_lang::location::LocationKey;
use hydro_lang::location::dynamic::LocationId;
use hydro_lang::networking::{NetworkingInfo, TcpFault};
use hydro_lang::sim::network_hook::{ChannelInfo, LocationInfo};

use super::dot::escape;
use super::*;
use crate::search::Fault;

/// Three processes `A`, `B` and `C`.
fn locations() -> Vec<LocationInfo> {
    ["A", "B", "C"]
        .iter()
        .enumerate()
        .map(|(i, name)| LocationInfo {
            id: LocationId::Process(format!("loc{}v1", i + 1).parse::<LocationKey>().unwrap()),
            name: format!("test::{name}"),
            members: None,
        })
        .collect()
}

fn names() -> NodeNames {
    NodeNames::new(&locations())
}

fn node(name: &str) -> NodeId {
    names().id(name).unwrap().clone()
}

/// Builds a log the way [`crate::fault::FaultHook`] records one.
#[derive(Default)]
struct Log {
    events: Vec<Event>,
    next_id: DeliveryId,
    ordinals: HashMap<NodeId, u64>,
}

impl Log {
    fn exec(&mut self, at: &str) -> &mut Self {
        self.events.push(Event::Exec { node: node(at) });
        self.events.push(Event::ExecEnd);
        self
    }

    /// A send of `payload` from `from` to `to` on `channel`; its delivery id and message.
    fn send(
        &mut self,
        channel: usize,
        from: &str,
        to: &str,
        payload: &str,
        fate: Fate,
    ) -> (DeliveryId, MessageId) {
        self.next_id += 1;
        let id = self.next_id;
        let from = node(from);
        let o = self.ordinals.entry(from.clone()).or_default();
        let ordinal = *o;
        *o += 1;
        let msg = MessageId {
            channel,
            from,
            to: node(to),
            bytes: Rc::from(payload.as_bytes()),
            copy: 0,
        };
        self.events.push(Event::Sent {
            id,
            msg: msg.clone(),
            ordinal,
            meta: None,
            fate,
        });
        (id, msg)
    }

    fn recv(&mut self, id: DeliveryId, channel: usize, to: &str) -> &mut Self {
        self.events.push(Event::Recv {
            id,
            channel,
            to: node(to),
        });
        self
    }

    /// A send from `from` to `to`, received right away.
    fn round_trip(&mut self, channel: usize, from: &str, to: &str, payload: &str) -> &mut Self {
        let (id, _) = self.send(channel, from, to, payload, Fate::Delivered);
        self.recv(id, channel, to)
    }

    fn output(&mut self, at: &str, value: &str) -> &mut Self {
        self.events.push(Event::Output {
            port: 0,
            node: node(at),
            bytes: value.as_bytes().to_vec(),
            meta: None,
        });
        self
    }

    fn halt(&mut self, at: &str) -> &mut Self {
        self.events.push(Event::Halted { node: node(at) });
        self
    }

    fn heal(&mut self) -> &mut Self {
        self.events.push(Event::Heal);
        self
    }

    fn log(&self) -> EventLog {
        EventLog(self.events.clone())
    }
}

/// Channels named `msg` (0) and `ack` (1); payloads and outputs are their text.
fn labels() -> Labels<'static> {
    let at = locations()[0].id.clone();
    let channels: Vec<ChannelInfo> = ["msg", "ack"]
        .iter()
        .enumerate()
        .map(|(id, name)| ChannelInfo {
            id,
            name: Some((*name).to_owned()),
            from: at.clone(),
            to: at.clone(),
            networking: NetworkingInfo::Tcp {
                fault: TcpFault::LossyDelayedForever,
            },
            intercepted: true,
            op: HydroIrOpMetadata::new(),
        })
        .collect();
    Labels::default()
        .with_channels(&channels)
        .payloads(|m| String::from_utf8_lossy(&m.bytes).into_owned())
        .outputs(|_, bytes| String::from_utf8_lossy(bytes).into_owned())
}

fn render(log: &Log, faults: &FaultSet<MessageId>) -> (Trace, String) {
    let trace = trace_with(&log.log(), &names(), faults, &labels());
    let out = dot(&trace, "");
    (trace, out)
}

/// The pinned position of DOT node `name`.
fn pos_of(out: &str, name: &str) -> (f64, f64) {
    let line = out
        .lines()
        .find(|l| l.trim_start().starts_with(&format!("{name} [")))
        .unwrap_or_else(|| panic!("no node `{name}` in\n{out}"));
    let start = line.find("pos=\"").expect("pinned") + 5;
    let end = line[start..].find('!').expect("pinned") + start;
    let (x, y) = line[start..end].split_once(',').unwrap();
    (x.parse().unwrap(), y.parse().unwrap())
}

fn well_formed(out: &str) {
    assert!(out.starts_with("digraph lamport {\n"), "{out}");
    assert!(out.trim_end().ends_with('}'), "{out}");
    assert!(out.contains("layout=neato"), "{out}");
}

#[test]
fn a_message_in_flight_across_a_crash_starts_above_the_crash() {
    let mut log = Log::default();
    log.exec("A");
    let (first, _) = log.send(0, "A", "B", "one", Fate::Delivered);
    let (second, m2) = log.send(0, "A", "B", "two", Fate::Delivered);
    log.halt("A");
    log.exec("B").recv(first, 0, "B").recv(second, 0, "B");
    let faults = [Fault::crash_after("A", Some(m2))].into_iter().collect();
    let (trace, out) = render(&log, &faults);
    well_formed(&out);

    let a = trace.node("A").unwrap();
    let b = trace.node("B").unwrap();
    let crash = trace.nodes[a].crash.expect("A crashed");
    assert_eq!(trace.events[crash].kind, TraceEventKind::Crash);
    let m = trace.messages.iter().position(|m| m.id == second).unwrap();
    let (send, recv) = (
        trace.messages[m].send_event,
        trace.messages[m].recv_event.unwrap(),
    );
    assert!(send < crash && crash < recv);

    // The edge runs from A's lane at the send to B's lane at the receive, across the crash.
    assert!(
        out.contains(&format!("e{send} -> e{recv} [tooltip=\"msg: two\"")),
        "{out}"
    );
    let (send_x, send_y) = pos_of(&out, &format!("e{send}"));
    let (crash_x, crash_y) = pos_of(&out, &format!("crash{a}"));
    let (recv_x, recv_y) = pos_of(&out, &format!("e{recv}"));
    assert_eq!(send_x, pos_of(&out, &format!("h{a}")).0);
    assert_eq!(crash_x, send_x);
    assert_eq!(recv_x, pos_of(&out, &format!("h{b}")).0);
    // Time flows down (y decreases): send, then crash, then receive.
    assert!(send_y > crash_y && crash_y > recv_y, "{out}");
    // A's lane continues faintly after the crash.
    assert!(
        out.contains(&format!("crash{a} -> end{a} [arrowhead=none, style=dotted")),
        "{out}"
    );
    assert!(out.contains("faults: crash(A after msg: →B two)"), "{out}");
    assert!(trace.unfired.is_empty());
}

#[test]
fn an_omitted_message_ends_in_a_cross_on_the_receivers_lane() {
    let mut log = Log::default();
    log.exec("A");
    let (_, m) = log.send(0, "A", "B", "lost", Fate::Omitted);
    let (id, _) = log.send(0, "A", "B", "kept", Fate::Delivered);
    log.exec("B").recv(id, 0, "B");
    let faults = [Fault::Omit(m)].into_iter().collect();
    let (trace, out) = render(&log, &faults);
    well_formed(&out);
    let lost = trace
        .messages
        .iter()
        .position(|m| m.fate == Fate::Omitted)
        .unwrap();
    let send = trace.messages[lost].send_event;
    assert!(
        out.contains(&format!(
            "e{send} -> lost{lost} [tooltip=\"msg: lost (omitted)\", style=dashed, color=\"#c0392b\""
        )),
        "{out}"
    );
    assert!(
        out.contains(&format!(
            "lost{lost} [shape=plaintext, label=\"✗\", fontcolor=\"#c0392b\""
        )),
        "{out}"
    );
    let b = trace.node("B").unwrap();
    assert_eq!(
        pos_of(&out, &format!("lost{lost}")).0,
        pos_of(&out, &format!("h{b}")).0
    );
    assert!(out.contains("faults: omit(msg: A→B lost)"), "{out}");
}

#[test]
fn a_node_crashed_from_the_start_loses_its_sends() {
    let mut log = Log::default();
    log.halt("A").exec("A");
    log.send(0, "A", "B", "never", Fate::SenderDead);
    let faults = [Fault::crash_after("A", None)].into_iter().collect();
    let (trace, out) = render(&log, &faults);
    well_formed(&out);
    let a = trace.node("A").unwrap();
    assert_eq!(trace.nodes[a].crash, Some(0));
    assert!(out.contains("(sender crashed)"), "{out}");
    assert!(out.contains("color=\"gray50\""), "{out}");
    // The lane is dead from the crash mark on, which comes before the lost send.
    assert!(pos_of(&out, &format!("crash{a}")).1 > pos_of(&out, "e1").1);
    assert!(out.contains("faults: crash(A)"), "{out}");
}

#[test]
fn a_crash_that_never_fired_is_named() {
    let mut log = Log::default();
    log.exec("A").round_trip(0, "A", "B", "x");
    let point = MessageId {
        channel: 0,
        from: node("C"),
        to: node("A"),
        bytes: Rc::from(&b"y"[..]),
        copy: 0,
    };
    let faults = [Fault::crash_after("C", Some(point))].into_iter().collect();
    let (trace, out) = render(&log, &faults);
    assert_eq!(trace.unfired, ["crash(C after msg: →A y)"]);
    assert!(
        out.contains("never fired: crash(C after msg: →A y)"),
        "{out}"
    );
    // C still gets a lane, without a crash mark.
    let c = trace.node("C").unwrap();
    assert!(out.contains(&format!("h{c} [")), "{out}");
    assert!(!out.contains(&format!("crash{c} [")), "{out}");
}

#[test]
fn the_heal_is_drawn_where_it_happened() {
    let mut log = Log::default();
    log.exec("A").round_trip(0, "A", "B", "before");
    log.heal();
    log.exec("A").round_trip(0, "A", "B", "after");
    let (trace, out) = render(&log, &FaultSet::new());
    let heal = trace
        .events
        .iter()
        .position(|e| e.kind == TraceEventKind::Heal)
        .unwrap();
    let before = trace.messages[0].send_event;
    let after = trace.messages[1].send_event;
    assert!(before < heal && heal < after);
    let heal_y = pos_of(&out, "heal_l").1;
    assert!(pos_of(&out, &format!("e{before}")).1 > heal_y);
    assert!(heal_y > pos_of(&out, &format!("e{after}")).1);
    assert!(out.contains("t_heal [label=\"heal\""), "{out}");
    assert!(out.contains("heal_l -> heal_r [style=dashed"), "{out}");
}

#[test]
fn long_labels_are_cut_and_listed_in_the_legend() {
    let long = format!("Payload {{ data: \"{}\" }}", "x".repeat(80));
    let mut log = Log::default();
    log.exec("A").round_trip(0, "A", "B", &long);
    log.exec("B").output("B", &long).output("B", "short");
    let (_, out) = render(&log, &FaultSet::new());
    well_formed(&out);
    // The message and the output share one legend entry.
    assert!(out.contains("…‹1›\""), "{out}");
    assert!(!out.contains("‹2›"), "{out}");
    let tagged = |prefix: &str| {
        out.lines()
            .any(|l| l.trim_start().starts_with(prefix) && l.contains("…‹1›"))
    };
    assert!(tagged("l0 ["), "{out}");
    assert!(tagged("o2 ["), "{out}");
    assert!(out.contains("label=\"⇒ short\""), "{out}");
    let legend = out
        .lines()
        .find(|l| l.trim_start().starts_with("legend ["))
        .expect("a legend");
    assert!(legend.contains("‹1›"), "{legend}");
    assert!(legend.contains(&escape(&long)), "{legend}");
    // Lanes are only as far apart as the cut labels need.
    assert!(pos_of(&out, "h1").0 < 400.0, "{out}");
}

#[test]
fn default_labels_are_hex() {
    let mut log = Log::default();
    log.exec("A").round_trip(3, "A", "B", "\u{1}\u{2}");
    let trace = trace(&log.log(), &names(), &FaultSet::new());
    assert_eq!(trace.messages[0].channel, "c3");
    assert_eq!(trace.messages[0].display, "0102");
    assert_eq!(short_hex(&[0; 9]), "0000000000000000…");
    assert_eq!(short_hex(&[]), "()");
}

#[test]
fn writes_dot_and_svg_when_graphviz_is_installed() {
    let mut log = Log::default();
    log.exec("A");
    let (_, m) = log.send(0, "A", "B", "lost", Fate::Omitted);
    let (id, m2) = log.send(0, "A", "B", "two", Fate::Delivered);
    log.halt("A");
    log.exec("B").recv(id, 0, "B").output("B", "two");
    log.heal();
    log.exec("B").round_trip(1, "B", "B", "self");
    let faults = [Fault::Omit(m), Fault::crash_after("A", Some(m2))]
        .into_iter()
        .collect();
    let trace = trace_with(&log.log(), &names(), &faults, &labels());
    let dir = std::env::temp_dir().join(format!("hydrofrac-lamport-{}", std::process::id()));
    let written = write(&dir, "failing run", &trace, "A/x: counterexample").unwrap();
    assert_eq!(written.dot, dir.join("failing_run.dot"));
    assert!(written.dot.is_file());
    let graphviz = Command::new("neato")
        .arg("-V")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success());
    if graphviz {
        let svg = written.svg.as_deref().expect("neato renders the diagram");
        let svg = std::fs::read_to_string(svg).unwrap();
        assert!(svg.contains("<svg"), "{svg}");
    } else {
        eprintln!("neato is not installed; not rendering");
        assert!(written.svg.is_none());
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn directories() {
    assert_eq!(sanitize("learners"), "learners");
    assert_eq!(sanitize("Learner cluster"), "Learner_cluster");
    assert_eq!(sanitize("../a/b"), "a_b");
    assert_eq!(sanitize("::"), "_");
    let dir = default_dir("my::test", "cluster/votes");
    assert!(dir.ends_with("hydrofrac/my_test/cluster_votes"), "{dir:?}");
    let base = PathBuf::from("/nonexistent/hydrofrac-lamport-claim/x");
    assert_eq!(claim(base.clone()), base);
    assert_eq!(
        claim(base),
        PathBuf::from("/nonexistent/hydrofrac-lamport-claim/x-2")
    );
    assert_eq!(
        test_name(),
        "directories",
        "libtest names the thread after the test"
    );
}

#[test]
fn escaping() {
    assert_eq!(escape("a\"b\\c\nd"), "a\\\"b\\\\c\\nd");
}

#[test]
fn the_focus_keeps_the_lineage_and_the_faults_and_drops_the_rest() {
    let mut log = Log::default();
    log.exec("B");
    let (dep, _) = log.send(0, "B", "A", "dep", Fate::Delivered);
    log.exec("A").recv(dep, 0, "A");
    // Opaque, so it depends on everything A received before it: `dep`.
    log.round_trip(0, "A", "B", "fact");
    let (_, lost) = log.send(0, "A", "B", "lost", Fate::Omitted);
    log.output("B", "fact");
    // Received after the output: in nothing's lineage.
    log.exec("C").round_trip(0, "C", "B", "noise");
    let (full, faults) = (log.log(), [Fault::Omit(lost)].into_iter().collect());
    let lineage = crate::lineage::derive(&full, &crate::lineage::Tables::from_parts(names(), [0]));
    let is_fact = |e: &Event| match e {
        Event::Sent { msg, .. } => &*msg.bytes == b"fact",
        Event::Output { bytes, .. } => bytes == b"fact",
        _ => false,
    };

    let (focused, caption) = focus(&full, &lineage, &faults, is_fact);
    assert_eq!(caption.as_deref(), Some("1 unrelated message not shown"));
    let trace = trace_with(&focused, &names(), &faults, &labels());
    let shown: Vec<&str> = trace.messages.iter().map(|m| m.display.as_str()).collect();
    assert_eq!(shown, ["dep", "fact", "lost"]);
    assert!(
        trace
            .messages
            .iter()
            .all(|m| m.fate != Fate::Delivered || m.recv_event.is_some())
    );
    // C sent only the noise: its lane is gone.
    assert_eq!(trace.node("C"), None);
    assert!(
        trace
            .events
            .iter()
            .any(|e| matches!(e.kind, TraceEventKind::Output { .. }))
    );

    // Nothing to focus on: the whole run.
    let (same, caption) = focus(&full, &lineage, &faults, |_| false);
    assert_eq!((same, caption), (full, None));
}

#[test]
fn a_lane_note_says_what_a_node_never_holds() {
    let mut log = Log::default();
    log.exec("A").round_trip(0, "A", "B", "x");
    let notes = [(node("C"), "✗ never holds x".to_owned())];
    let trace = trace_with_notes(&log.log(), &names(), &FaultSet::new(), &labels(), &notes);
    // Nothing happened at C, and it still gets a lane, for its note.
    let c = trace.node("C").expect("a noted node has a lane");
    assert_eq!(trace.notes, [(c, "✗ never holds x".to_owned())]);
    let out = dot(&trace, "");
    let line = out
        .lines()
        .find(|l| l.trim_start().starts_with(&format!("note{c} [")))
        .unwrap_or_else(|| panic!("no note on C's lane in\n{out}"));
    assert!(line.contains("label=\"✗ never holds x\""), "{line}");
    assert!(line.contains("#c0392b"), "red: {line}");
    // Below the end of C's lane.
    let (note_x, note_y) = pos_of(&out, &format!("note{c}"));
    let (end_x, end_y) = pos_of(&out, &format!("end{c}"));
    assert_eq!(note_x, end_x);
    assert!(note_y < end_y, "{out}");
}

/// Every message label's box (centre, size), from its DOT line.
fn label_boxes(out: &str) -> Vec<(String, f64, f64, f64)> {
    out.lines()
        .map(str::trim_start)
        .filter(|l| {
            let name = l.split(' ').next().unwrap_or("");
            name.len() > 1 && name.starts_with('l') && name[1..].parse::<usize>().is_ok()
        })
        .map(|l| {
            let name = l.split(' ').next().unwrap();
            let start = l.find("label=\"").unwrap() + 7;
            let text = &l[start..start + l[start..].find('"').unwrap()];
            let (x, y) = pos_of(out, name);
            (
                text.to_owned(),
                x,
                y,
                5.3 * text.chars().count() as f64 + 6.0,
            )
        })
        .collect()
}

#[test]
fn message_labels_cover_no_other_label_and_no_event_point() {
    // A member delivers, then crashes with its forwards in flight and lost: the self-send's
    // label and the lost forward's start at the same point.
    let mut log = Log::default();
    log.exec("A");
    let (first, _) = log.send(0, "A", "B", "1", Fate::Delivered);
    let (_, lost) = log.send(0, "A", "C", "1", Fate::SenderDead);
    log.halt("A");
    log.exec("B").recv(first, 0, "B").output("B", "1");
    log.send(1, "B", "B", "1", Fate::Delivered);
    let (_, forward) = log.send(1, "B", "C", "1", Fate::SenderDead);
    log.halt("B");
    let faults = [
        Fault::crash_after("A", Some(lost)),
        Fault::crash_after("B", Some(forward)),
    ]
    .into_iter()
    .collect();
    let (trace, out) = render(&log, &faults);
    let labels = label_boxes(&out);
    assert_eq!(labels.len(), trace.messages.len(), "{out}");
    for (i, (a, ax, ay, aw)) in labels.iter().enumerate() {
        for (b, bx, by, bw) in &labels[i + 1..] {
            let apart = (ax - bx).abs() >= (aw + bw) / 2.0 || (ay - by).abs() >= 11.0;
            assert!(
                apart,
                "`{a}` at ({ax}, {ay}) covers `{b}` at ({bx}, {by})\n{out}"
            );
        }
        for e in 0..trace.events.len() {
            let Some(line) = out
                .lines()
                .find(|l| l.trim_start().starts_with(&format!("e{e} [")))
            else {
                continue;
            };
            assert!(line.contains("pos="), "{line}");
            let (ex, ey) = pos_of(&out, &format!("e{e}"));
            let clear = (ax - ex).abs() >= aw / 2.0 + 3.0 || (ay - ey).abs() >= 5.5 + 3.0;
            assert!(clear, "`{a}` at ({ax}, {ay}) covers the point e{e}\n{out}");
        }
    }
}
