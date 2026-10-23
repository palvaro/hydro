use std::collections::HashMap;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::rc::Rc;

use hydro_lang::location::LocationKey;
use hydro_lang::location::dynamic::LocationId;
use hydro_lang::sim::network_hook::{ExecKind, LocationInfo};

use super::dot::{escape_dot, escape_html, strip_paths};
use super::*;
use crate::search::Fault;

/// Three processes `A`, `B` and `C`, plus an eager `Obs`.
fn locations() -> Vec<LocationInfo> {
    ["A", "B", "C", "Obs"]
        .iter()
        .enumerate()
        .map(|(i, name)| LocationInfo {
            id: LocationId::Process(format!("loc{}v1", i + 1).parse::<LocationKey>().unwrap()),
            name: format!("test::{name}"),
            members: None,
            eager: *name == "Obs",
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
        self.events.push(Event::Exec {
            node: node(at),
            kind: ExecKind::Async,
        });
        self.events.push(Event::ExecEnd { progressed: true });
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
    Labels::default()
        .channel_names(|c| ["msg", "ack"].get(c).map(|s| (*s).to_owned()))
        .payloads(|m| String::from_utf8_lossy(&m.bytes).into_owned())
        .outputs(|_, bytes| String::from_utf8_lossy(bytes).into_owned())
}

fn render(log: &Log, faults: &FaultSet<MessageId>, opts: &Options) -> (Trace, String) {
    let trace = trace_with(&log.log(), &names(), faults, &labels());
    let out = dot(&trace, opts);
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
    let (trace, out) = render(&log, &faults, &Options::default());
    well_formed(&out);

    let a = trace.node("A").unwrap();
    let b = trace.node("B").unwrap();
    let crash = trace.nodes[a].crash.expect("A crashed");
    assert_eq!(trace.events[crash].kind, TraceEventKind::Crash);
    let m = trace.message(second).unwrap();
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
    let (trace, out) = render(&log, &faults, &Options::default());
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
    let (trace, out) = render(&log, &faults, &Options::default());
    well_formed(&out);
    let a = trace.node("A").unwrap();
    assert_eq!(trace.nodes[a].crash, Some(0));
    assert!(out.contains("(sender crashed)"), "{out}");
    assert!(out.contains("color=\"gray50\""), "{out}");
    // The lane is dead from the crash mark on, which comes before the lost send.
    assert!(pos_of(&out, &format!("crash{a}")).1 > pos_of(&out, "e2").1);
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
    };
    let faults = [Fault::crash_after("C", Some(point))].into_iter().collect();
    let (trace, out) = render(&log, &faults, &Options::default());
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
fn identical_rounds_are_drawn_once() {
    let mut log = Log::default();
    for _ in 0..5 {
        log.exec("A").round_trip(0, "A", "B", "ping");
        log.exec("B").round_trip(1, "B", "A", "pong");
    }
    log.exec("A").round_trip(0, "A", "B", "bye");
    let faults = FaultSet::new();
    let (_, out) = render(&log, &faults, &Options::default());
    well_formed(&out);
    assert_eq!(out.matches("label=\"msg: ping\"").count(), 1, "{out}");
    assert_eq!(out.matches("label=\"ack: pong\"").count(), 1, "{out}");
    assert_eq!(out.matches("label=\"msg: bye\"").count(), 1, "{out}");
    assert!(out.contains("label=\"×5\""), "{out}");

    let opts = Options {
        collapse_rounds: false,
        ..Options::default()
    };
    let (_, out) = render(&log, &faults, &opts);
    assert_eq!(out.matches("label=\"msg: ping\"").count(), 5, "{out}");
    assert!(!out.contains('×'), "{out}");
}

#[test]
fn rounds_do_not_collapse_across_a_heal_or_a_crash() {
    let mut log = Log::default();
    for _ in 0..2 {
        log.exec("A").round_trip(0, "A", "B", "ping");
    }
    log.heal();
    for _ in 0..2 {
        log.exec("A").round_trip(0, "A", "B", "ping");
    }
    log.halt("B");
    for _ in 0..2 {
        log.exec("A").round_trip(0, "A", "C", "ping");
    }
    let (_, out) = render(&log, &FaultSet::new(), &Options::default());
    well_formed(&out);
    assert_eq!(out.matches("label=\"×2\"").count(), 3, "{out}");
    assert!(!out.contains("×4"), "{out}");
    assert!(out.contains("t_heal [label=\"heal\""), "{out}");
    assert!(out.contains("heal_l -> heal_r [style=dashed"), "{out}");
}

#[test]
fn the_heal_is_drawn_where_it_happened() {
    let mut log = Log::default();
    log.exec("A").round_trip(0, "A", "B", "before");
    log.heal();
    log.exec("A").round_trip(0, "A", "B", "after");
    let (trace, out) = render(&log, &FaultSet::new(), &Options::default());
    let heal = trace.heal_event().unwrap();
    let before = trace.messages[0].send_event;
    let after = trace.messages[1].send_event;
    assert!(before < heal && heal < after);
    let heal_y = pos_of(&out, "heal_l").1;
    assert!(pos_of(&out, &format!("e{before}")).1 > heal_y);
    assert!(heal_y > pos_of(&out, &format!("e{after}")).1);
}

#[test]
fn long_labels_are_cut_and_listed_in_the_legend() {
    let long = format!("Payload {{ data: \"{}\" }}", "x".repeat(80));
    let mut log = Log::default();
    log.exec("A").round_trip(0, "A", "B", &long);
    log.exec("B").output("B", &long).output("B", "short");
    let opts = Options {
        collapse_rounds: false,
        ..Options::default()
    };
    let (_, out) = render(&log, &FaultSet::new(), &opts);
    well_formed(&out);
    // The message and the output share one legend entry.
    assert!(out.contains("…‹1›\""), "{out}");
    assert!(!out.contains("‹2›"), "{out}");
    let tagged = |prefix: &str| {
        out.lines()
            .any(|l| l.trim_start().starts_with(prefix) && l.contains("…‹1›"))
    };
    assert!(tagged("l0 ["), "{out}");
    assert!(tagged("o4 ["), "{out}");
    assert!(out.contains("label=\"⇒ short\""), "{out}");
    let legend = out
        .lines()
        .find(|l| l.trim_start().starts_with("legend ["))
        .expect("a legend");
    assert!(legend.contains("‹1›"), "{legend}");
    assert!(legend.contains(&escape_html(&long)), "{legend}");
    // Lanes are only as far apart as the cut labels need.
    assert!(pos_of(&out, "h1").0 < 400.0, "{out}");
}

#[test]
fn eager_locations_are_hidden_by_default() {
    let mut log = Log::default();
    log.exec("A").round_trip(0, "A", "Obs", "fact");
    log.round_trip(0, "A", "B", "msg");
    let (trace, out) = render(&log, &FaultSet::new(), &Options::default());
    let obs = trace.node("Obs").unwrap();
    assert!(trace.nodes[obs].eager);
    assert!(!out.contains("label=\"Obs\""), "{out}");
    assert!(!out.contains("msg: fact"), "{out}");
    assert!(out.contains("msg: msg"), "{out}");
    let opts = Options {
        show_eager: true,
        ..Options::default()
    };
    let (_, out) = render(&log, &FaultSet::new(), &opts);
    assert!(out.contains("label=\"Obs\""), "{out}");
    assert!(out.contains("msg: fact"), "{out}");
}

#[test]
fn highlighted_messages_are_emphasized() {
    let mut log = Log::default();
    log.exec("A");
    let (id, _) = log.send(0, "A", "B", "key", Fate::Delivered);
    log.recv(id, 0, "B");
    log.round_trip(0, "A", "B", "other");
    let (_, out) = render(&log, &FaultSet::new(), &Options::default().highlight([id]));
    assert!(out.contains("label=\"★ msg: key\""), "{out}");
    assert!(out.contains("penwidth=2, color=\"#d35400\""), "{out}");
    assert!(out.contains("label=\"msg: other\""), "{out}");
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
    let written = write(
        &dir,
        "failing run",
        &trace,
        &Options::default().title("A/x: counterexample"),
    )
    .unwrap();
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
fn text_helpers() {
    assert_eq!(
        strip_paths("Ballot { proposer_id: MemberId::<hydro_test::cluster::paxos::Proposer>(1) }"),
        "Ballot { proposer_id: MemberId::<Proposer>(1) }"
    );
    assert_eq!(strip_paths("x: 1, y: Some(\"x\")"), "x: 1, y: Some(\"x\")");
    assert_eq!(escape_dot("a\"b\\c\nd"), "a\\\"b\\\\c\\nd");
    assert_eq!(
        escape_html("a<b>&\"c\"\nd"),
        "a&lt;b&gt;&amp;&quot;c&quot;<BR/>d"
    );
}
