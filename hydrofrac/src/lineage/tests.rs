use std::collections::{BTreeSet, HashMap};
use std::rc::Rc;

use hydro_lang::location::LocationKey;
use hydro_lang::location::dynamic::LocationId;
use hydro_lang::sim::network_hook::LocationInfo;

use super::*;
use crate::races::PairClause;
use crate::search::Fault;

/// The processes of the hand-built runs: `A`, `B`, `C` and `Obs`.
fn locations() -> Vec<LocationInfo> {
    ["A", "B", "C", "Obs"]
        .iter()
        .enumerate()
        .map(|(i, name)| LocationInfo {
            id: LocationId::Process(format!("loc{}v1", i + 1).parse::<LocationKey>().unwrap()),
            name: format!("test::{name}"),
            members: None,
        })
        .collect()
}

fn node(name: &str) -> NodeId {
    let locs = locations();
    let names = NodeNames::new(&locs);
    names.id(name).unwrap().clone()
}

/// Channels: 0 is lossy, 1 is fail-stop.
const LOSSY: usize = 0;
const RELIABLE: usize = 1;

fn tables() -> Tables {
    Tables::from_parts(NodeNames::new(&locations()), [LOSSY])
}

fn ids(ids: &[DeliveryId]) -> Option<Vec<u8>> {
    Some(RecordProv::Ids(ids.iter().copied().collect()).encode())
}

fn opaque() -> Option<Vec<u8>> {
    Some(RecordProv::Opaque.encode())
}

/// Builds a log the way [`crate::fault::FaultHook`] records one: fresh delivery ids from 1 and
/// per-sender ordinals from 0.
#[derive(Default)]
struct Log {
    events: Vec<Event>,
    next_id: DeliveryId,
    ordinals: HashMap<NodeId, u64>,
}

impl Log {
    fn exec(&mut self, at: &str) -> &mut Self {
        self.events.push(Event::Exec { node: node(at) });
        self
    }

    fn end(&mut self) -> &mut Self {
        self.events.push(Event::ExecEnd);
        self
    }

    /// A send from `from` to `to` on `channel` with payload `[byte]`; its delivery id.
    fn send_with(
        &mut self,
        channel: usize,
        from: &str,
        to: &str,
        byte: u8,
        meta: Option<Vec<u8>>,
        fate: Fate,
    ) -> DeliveryId {
        self.next_id += 1;
        let id = self.next_id;
        let from = node(from);
        let o = self.ordinals.entry(from.clone()).or_default();
        let ordinal = *o;
        *o += 1;
        self.events.push(Event::Sent {
            id,
            msg: msg(channel, &from, &node(to), byte),
            ordinal,
            meta,
            fate,
        });
        id
    }

    fn send(
        &mut self,
        channel: usize,
        from: &str,
        to: &str,
        byte: u8,
        meta: Option<Vec<u8>>,
    ) -> DeliveryId {
        self.send_with(channel, from, to, byte, meta, Fate::Delivered)
    }

    fn recv(&mut self, id: DeliveryId, channel: usize, to: &str) -> &mut Self {
        self.events.push(Event::Recv {
            id,
            channel,
            to: node(to),
        });
        self
    }

    /// An output at `at`; its event index.
    fn output(&mut self, at: &str, meta: Option<Vec<u8>>) -> usize {
        self.events.push(Event::Output {
            port: 0,
            node: node(at),
            bytes: vec![],
            meta,
        });
        self.events.len() - 1
    }

    fn heal(&mut self) -> &mut Self {
        self.events.push(Event::Heal);
        self
    }

    fn log(&self) -> EventLog {
        EventLog(self.events.clone())
    }
}

fn msg(channel: usize, from: &NodeId, to: &NodeId, byte: u8) -> MessageId {
    MessageId {
        channel,
        from: from.clone(),
        to: to.clone(),
        bytes: Rc::from(vec![byte]),
        copy: 0,
    }
}

/// The messages of a derivation, as `(channel, from, to, byte)` names.
fn messages(d: &Derivation<MessageId>) -> BTreeSet<(usize, String, String, u8)> {
    let names = NodeNames::new(&locations());
    d.iter()
        .map(|dep| {
            (
                dep.message.channel,
                names.name(&dep.message.from),
                names.name(&dep.message.to),
                dep.message.bytes[0],
            )
        })
        .collect()
}

fn m(channel: usize, from: &str, to: &str, byte: u8) -> (usize, String, String, u8) {
    (channel, from.to_owned(), to.to_owned(), byte)
}

#[test]
fn record_prov_encoding_is_bincode() {
    // bincode 1.x: a u32 variant index, then a u64 length and the u64 elements.
    let ids = RecordProv::Ids([1, 258].into_iter().collect());
    let mut expected = vec![0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0];
    expected.extend_from_slice(&1u64.to_le_bytes());
    expected.extend_from_slice(&258u64.to_le_bytes());
    assert_eq!(ids.encode(), expected);
    // The rewrite's generated code writes the variants by hand (it can name only
    // `hydro_lang` and std); its bytes must be `RecordProv`'s.
    let generated: BTreeSet<u64> = [1, 258].into_iter().collect();
    assert_eq!(bincode::serialize(&(0u32, generated)).unwrap(), expected);
    assert_eq!(
        bincode::serialize(&1u32).unwrap(),
        RecordProv::Opaque.encode()
    );
    assert_eq!(decode_meta(Some(&expected)), ids);
    assert_eq!(RecordProv::Opaque.encode(), vec![1, 0, 0, 0]);
    assert_eq!(decode_meta(Some(&[1, 0, 0, 0])), RecordProv::Opaque);
    // Missing or malformed metadata is opaque.
    assert_eq!(decode_meta(None), RecordProv::Opaque);
    assert_eq!(decode_meta(Some(&[0, 0, 0])), RecordProv::Opaque);
    assert_eq!(
        decode_meta(Some(&expected[..expected.len() - 1])),
        RecordProv::Opaque
    );
    let mut trailing = expected.clone();
    trailing.push(0);
    assert_eq!(decode_meta(Some(&trailing)), RecordProv::Opaque);
    assert_eq!(decode_meta(Some(&[7, 0, 0, 0])), RecordProv::Opaque);
}

/// A → B → C, with an unrelated delivery A → B that B received first.
#[test]
fn record_rule_closes_across_two_hops() {
    let mut l = Log::default();
    l.exec("A");
    let d1 = l.send(LOSSY, "A", "B", 1, ids(&[]));
    let d3 = l.send(LOSSY, "A", "B", 3, ids(&[]));
    l.end();
    l.exec("B").recv(d3, LOSSY, "B").recv(d1, LOSSY, "B");
    let d2 = l.send(RELIABLE, "B", "C", 2, ids(&[d1]));
    l.end();
    l.exec("C").recv(d2, RELIABLE, "C");
    let out = l.output("C", ids(&[d2]));
    l.end();
    let log = l.log();
    let lineage = derive(&log, &tables());

    assert_eq!(
        messages(&lineage.derivation(out)),
        [m(LOSSY, "A", "B", 1), m(RELIABLE, "B", "C", 2)].into()
    );
    assert_eq!(lineage.opaque_fallbacks(), 0);
    assert_eq!(
        messages(&lineage.send_derivation(d2).unwrap()),
        [m(LOSSY, "A", "B", 1)].into()
    );
}

/// An opaque output depends on every delivery its node received before its execution ended, and
/// on nothing received after.
#[test]
fn execution_rule_takes_every_earlier_receive_and_nothing_later() {
    let mut l = Log::default();
    l.exec("A");
    let early = l.send(LOSSY, "A", "C", 1, ids(&[]));
    let same = l.send(LOSSY, "A", "C", 2, ids(&[]));
    let late = l.send(LOSSY, "A", "C", 3, ids(&[]));
    let elsewhere = l.send(LOSSY, "A", "B", 4, ids(&[]));
    l.end();
    l.exec("C").recv(early, LOSSY, "C").end();
    l.exec("B").recv(elsewhere, LOSSY, "B").end();
    // The output's own execution receives `same` after emitting: still before the execution
    // ended, so it counts.
    l.exec("C");
    let out = l.output("C", opaque());
    l.recv(same, LOSSY, "C").end();
    l.exec("C").recv(late, LOSSY, "C").end();
    let log = l.log();
    let lineage = derive(&log, &tables());

    assert_eq!(
        messages(&lineage.derivation(out)),
        [m(LOSSY, "A", "C", 1), m(LOSSY, "A", "C", 2)].into()
    );
    assert_eq!(lineage.opaque_fallbacks(), 1);
}

/// No metadata at all is treated like an opaque record.
#[test]
fn missing_metadata_falls_back_to_the_execution_rule() {
    let mut l = Log::default();
    l.exec("A");
    let d = l.send(LOSSY, "A", "C", 1, None);
    l.end();
    l.exec("C").recv(d, LOSSY, "C");
    let out = l.output("C", None);
    l.end();
    let log = l.log();
    let lineage = derive(&log, &tables());
    assert_eq!(
        messages(&lineage.derivation(out)),
        [m(LOSSY, "A", "C", 1)].into()
    );
    // The output fell back; so did the send behind its delivery, which received nothing.
    assert_eq!(lineage.opaque_fallbacks(), 2);
}

/// The record rule at the output, the execution rule at an opaque send on the way, and the
/// record rule again behind that.
#[test]
fn rules_mix_along_the_closure() {
    let mut l = Log::default();
    l.exec("A");
    let a1 = l.send(LOSSY, "A", "B", 1, ids(&[]));
    l.end();
    l.exec("C");
    let c1 = l.send(LOSSY, "C", "B", 5, ids(&[]));
    l.end();
    l.exec("B").recv(a1, LOSSY, "B").recv(c1, LOSSY, "B");
    let b1 = l.send(RELIABLE, "B", "C", 2, opaque());
    l.end();
    // Received by B after the execution that sent `b1`: not in its support.
    l.exec("A");
    let a2 = l.send(LOSSY, "A", "B", 3, ids(&[]));
    l.end();
    l.exec("B").recv(a2, LOSSY, "B").end();
    l.exec("C").recv(b1, RELIABLE, "C");
    let out = l.output("C", ids(&[b1]));
    l.end();
    let log = l.log();
    let lineage = derive(&log, &tables());

    assert_eq!(
        messages(&lineage.derivation(out)),
        [
            m(RELIABLE, "B", "C", 2),
            m(LOSSY, "A", "B", 1),
            m(LOSSY, "C", "B", 5)
        ]
        .into()
    );
    assert_eq!(lineage.opaque_fallbacks(), 1);
}

#[test]
fn derivations_are_memoized_and_fallbacks_counted_once() {
    let mut l = Log::default();
    l.exec("A");
    let d = l.send(LOSSY, "A", "C", 1, opaque());
    l.end();
    l.exec("C").recv(d, LOSSY, "C");
    let o1 = l.output("C", ids(&[d]));
    let o2 = l.output("C", ids(&[d]));
    l.end();
    let log = l.log();
    let lineage = derive(&log, &tables());

    let first = lineage.derivation(o1);
    assert!(Rc::ptr_eq(&first, &lineage.derivation(o1)));
    assert_eq!(lineage.opaque_fallbacks(), 1);
    // A second output reaching the same opaque send does not count it again.
    assert_eq!(*lineage.derivation(o2), *first);
    assert_eq!(lineage.opaque_fallbacks(), 1);
}

/// Omitted sends and sends of a dead sender were never received, so no rule reaches them, not
/// even provenance that (wrongly) names them.
#[test]
fn undelivered_sends_never_appear_in_a_support() {
    let mut l = Log::default();
    l.exec("A");
    let delivered = l.send(LOSSY, "A", "C", 1, ids(&[]));
    let omitted = l.send_with(LOSSY, "A", "C", 2, ids(&[]), Fate::Omitted);
    l.events.push(Event::Halted { node: node("A") });
    let dead = l.send_with(LOSSY, "A", "C", 3, ids(&[]), Fate::SenderDead);
    l.end();
    l.exec("C").recv(delivered, LOSSY, "C");
    let by_rule = l.output("C", opaque());
    let by_ids = l.output("C", ids(&[delivered, omitted, dead]));
    l.end();
    let log = l.log();
    let lineage = derive(&log, &tables());

    let expected: BTreeSet<_> = [m(LOSSY, "A", "C", 1)].into();
    assert_eq!(messages(&lineage.derivation(by_rule)), expected);
    assert_eq!(messages(&lineage.derivation(by_ids)), expected);
}

/// After the heal no omission applies, so later deliveries are not omittable; their senders'
/// pre-heal crash points still kill them.
#[test]
fn sends_after_the_heal_are_not_omittable() {
    let mut l = Log::default();
    l.exec("A");
    let before = l.send(LOSSY, "A", "C", 1, ids(&[]));
    l.end();
    l.heal();
    l.exec("A");
    let after = l.send(LOSSY, "A", "C", 2, ids(&[]));
    l.end();
    l.exec("C").recv(before, LOSSY, "C").recv(after, LOSSY, "C");
    let out = l.output("C", ids(&[before, after]));
    l.end();
    let log = l.log();
    let lineage = derive(&log, &tables());

    let d = lineage.derivation(out);
    let lossy: BTreeSet<u8> = d
        .iter()
        .filter(|dep| dep.lossy)
        .map(|dep| dep.message.bytes[0])
        .collect();
    assert_eq!(lossy, [1].into());
    assert_eq!(d.len(), 2);

    // The crash right after A's first send fires before the heal and kills the post-heal send.
    // (No crash point names a post-heal send: the search's candidates come from pre-heal sends
    // only.)
    let clause = PairClause::robust(d.iter().cloned());
    let first = msg(LOSSY, &node("A"), &node("C"), 1);
    let second = msg(LOSSY, &node("A"), &node("C"), 2);
    assert!(clause.hit_by(&Fault::crash_after("A", Some(first.clone())), &()));
    assert!(clause.hit_by(&Fault::Omit(first), &()));
    assert!(!clause.hit_by(&Fault::Omit(second), &()));
}

/// The kill relation of a derivation, through the search's crash rule, on a two-send run.
#[test]
fn clause_kill_relation_matches_the_run() {
    let mut l = Log::default();
    l.exec("A");
    let d1 = l.send(LOSSY, "A", "B", 1, ids(&[]));
    let d2 = l.send(LOSSY, "A", "B", 2, ids(&[]));
    l.end();
    l.exec("B").recv(d1, LOSSY, "B").recv(d2, LOSSY, "B");
    let out = l.output("B", ids(&[d2]));
    l.end();
    let log = l.log();
    let lineage = derive(&log, &tables());
    let clause = PairClause::robust(lineage.derivation(out).iter().cloned());

    let m1 = msg(LOSSY, &node("A"), &node("B"), 1);
    let m2 = msg(LOSSY, &node("A"), &node("B"), 2);
    assert!(clause.hit_by(&Fault::Omit(m2.clone()), &()));
    assert!(!clause.hit_by(&Fault::Omit(m1.clone()), &()));
    assert!(clause.hit_by(&Fault::crash_after("A", None), &()));
    assert!(clause.hit_by(&Fault::crash_after("A", Some(m1)), &()));
    assert!(!clause.hit_by(&Fault::crash_after("A", Some(m2)), &()));
    // B produced the fact; its crash removes it from the correct members instead.
    assert!(!clause.hit_by(&Fault::crash_after("B", None), &()));
}

/// A fact computed from nothing that came off a channel has an empty derivation: no fault set
/// kills it.
#[test]
fn local_facts_have_empty_derivations() {
    let mut l = Log::default();
    l.exec("A");
    let out = l.output("A", ids(&[]));
    l.end();
    let log = l.log();
    let lineage = derive(&log, &tables());
    assert!(lineage.derivation(out).is_empty());
    assert_eq!(
        lineage.observations().map(|o| o.event).collect::<Vec<_>>(),
        vec![out]
    );
}

/// A long chain of opaque hops is closed without recursion.
#[test]
fn long_chains_do_not_recurse() {
    let mut l = Log::default();
    let mut prev: Option<DeliveryId> = None;
    let hops = 20_000;
    for i in 0..hops {
        let (from, to) = if i % 2 == 0 { ("A", "B") } else { ("B", "A") };
        l.exec(from);
        if let Some(p) = prev {
            l.recv(p, LOSSY, from);
        }
        prev = Some(l.send(LOSSY, from, to, (i % 251) as u8, opaque()));
        l.end();
    }
    l.exec("A").recv(prev.unwrap(), LOSSY, "A");
    let out = l.output("A", opaque());
    l.end();
    let log = l.log();
    let lineage = derive(&log, &tables());
    // Every delivery is in the support (payloads repeat, so messages dedup only by ordinal).
    assert_eq!(lineage.derivation(out).len(), hops);
    assert_eq!(lineage.opaque_fallbacks(), hops + 1);
}
