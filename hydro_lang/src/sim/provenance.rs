//! Forward provenance ("taint") tracking for simulated Hydro programs.
//!
//! When a simulation is compiled with [`crate::sim::flow::SimFlow::with_provenance`], every item
//! flowing through the program is wrapped in a [`Tagged`] carrying the set of *source events* it
//! descends from. Source events are the items sent into the simulation through `sim_input`
//! ports, and are of two kinds:
//!
//! - [`TagKind::Data`]: ordinary application input (a request, an edge, a write);
//! - [`TagKind::Operational`]: a stimulus that carries no information of its own (a timer tick,
//!   an election timeout), declared with `sim_input_operational`.
//!
//! Tags are propagated structurally by the dataflow: `map`/`filter` keep them, `join`/`cross`
//! union the two sides, `fold`/`reduce` accumulate every input, and network edges carry them in
//! a side frame alongside the real serialized payload (so byte counts are those of the untagged
//! program). Closures that read Hydro state through `by_ref`/`by_mut` references inherit *all*
//! tags the referenced state has accumulated and mark their outputs [`Tagged::coarse`], since the
//! dataflow cannot see inside the closure to do better.
//!
//! Every *physical emission* — a message crossing a network edge, an item leaving through a sim
//! output, an item crossing a cycle sink — is appended to a log as an [`EmissionRecord`]. The
//! test reads the log back with [`take_emissions`] and interprets it with [`classify`], which
//! labels each emission by the *shape of its lineage* alone, without reference to operator or
//! protocol names.
//!
//! The instrumentation is passive: it does not consume simulator decisions and does not change
//! how items compare (`Eq`/`Hash`/`Ord` on [`Tagged`] delegate to the value), so a recorded
//! fuzz/exhaustive execution replays identically under the instrumented build.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::hash::{Hash, Hasher};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Whether a source event is application data or an operational stimulus.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum TagKind {
    /// Ordinary application input.
    Data,
    /// A timer tick, timeout, or similar stimulus with no informational content.
    Operational,
}

/// Identifies one source event: the `seq`-th item delivered on sim input `port` at cluster
/// member `member` (`u32::MAX` for processes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Tag {
    /// Data or operational.
    pub kind: TagKind,
    /// The sim input port the event arrived on.
    pub port: u32,
    /// The receiving cluster member, or `u32::MAX` for a process.
    pub member: u32,
    /// Position of the event among those delivered on this port to this member.
    pub seq: u64,
}

/// The lineage of one item: the set of source events it descends from.
pub type TagSet = BTreeSet<Tag>;

/// An item together with its lineage.
///
/// Comparison, hashing and ordering delegate to `value` only, so deduplicating, sorting and
/// set-difference operators behave exactly as they do on the untagged program.
#[derive(Clone)]
pub struct Tagged<T> {
    /// Source events this item descends from.
    pub tags: TagSet,
    /// Set when some ancestor was produced by a closure reading opaque Hydro state, in which
    /// case `tags` is an over-approximation (everything that state had accumulated).
    pub coarse: bool,
    /// The item as the untagged program would see it.
    pub value: T,
}

impl<T> Tagged<T> {
    /// An item with no lineage at all (a compile-time constant, cluster membership, ...).
    pub fn pristine(value: T) -> Self {
        Self {
            tags: TagSet::new(),
            coarse: false,
            value,
        }
    }

    /// An item entering the program as source event `tag`.
    pub fn source(tag: Tag, value: T) -> Self {
        let mut tags = TagSet::new();
        tags.insert(tag);
        Self {
            tags,
            coarse: false,
            value,
        }
    }

    /// Rebuilds an item from parts.
    pub fn from_parts(tags: TagSet, coarse: bool, value: T) -> Self {
        Self {
            tags,
            coarse,
            value,
        }
    }

    /// Splits an item into `(lineage, coarse, value)`.
    pub fn into_parts(self) -> (TagSet, bool, T) {
        (self.tags, self.coarse, self.value)
    }

    /// Absorbs the lineage of another item into this one (for folds and reference reads).
    pub fn absorb(&mut self, tags: &TagSet, coarse: bool) {
        self.tags.extend(tags.iter().copied());
        self.coarse |= coarse;
    }
}

impl<T: Debug> Debug for Tagged<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.value.fmt(f)
    }
}

impl<T: PartialEq> PartialEq for Tagged<T> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<T: Eq> Eq for Tagged<T> {}

impl<T: Hash> Hash for Tagged<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}

impl<T: PartialOrd> PartialOrd for Tagged<T> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.value.partial_cmp(&other.value)
    }
}

impl<T: Ord> Ord for Tagged<T> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.value.cmp(&other.value)
    }
}

/// Applies an un-annotated closure to a value. Used by generated code so that rustc infers the
/// closure's parameter type from the argument (closures passed as arguments are type-checked
/// after the other arguments), which a plain `let f = |x| ..; f(v)` binding would not allow.
#[doc(hidden)]
#[inline(always)]
pub fn apply1<A, B>(f: impl FnOnce(A) -> B, a: A) -> B {
    f(a)
}

/// Merges two lineages (for joins and cross products).
pub fn union(mut a: TagSet, b: &TagSet) -> TagSet {
    a.extend(b.iter().copied());
    a
}

// ---------------------------------------------------------------------------------------------
// Network framing: tags ride alongside the real payload so byte accounting stays honest.
// ---------------------------------------------------------------------------------------------

#[derive(Serialize)]
struct FrameRef<'a> {
    tags: &'a TagSet,
    coarse: bool,
    #[serde(with = "serde_bytes")]
    payload: &'a [u8],
}

#[derive(Deserialize)]
struct FrameOwned {
    tags: TagSet,
    coarse: bool,
    #[serde(with = "serde_bytes")]
    payload: Vec<u8>,
}

mod serde_bytes {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &&[u8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_bytes(v)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        Vec::<u8>::deserialize(d)
    }
}

/// The value produced by a network serialize function: either raw bytes or, for demuxed sends,
/// a `(recipient, bytes)` pair. Implemented so the provenance pass can wrap either uniformly.
pub trait NetworkPayload: Sized {
    /// Bytes of the real (untagged) payload.
    fn payload_len(&self) -> usize;
    /// Content hash of the real payload.
    fn payload_hash(&self) -> u64 {
        use std::hash::Hasher;
        let mut h = std::collections::hash_map::DefaultHasher::new();
        h.write(self.payload_bytes());
        h.finish()
    }
    /// The real payload bytes.
    fn payload_bytes(&self) -> &[u8];
    /// The destination member for demuxed sends, if any.
    fn recipient(&self) -> Option<u32>;
    /// Attaches a lineage frame around the payload.
    fn frame(self, tags: &TagSet, coarse: bool) -> Self;
    /// Removes the lineage frame, returning it with the real payload.
    fn unframe(self) -> (TagSet, bool, Self);
}

fn frame_bytes(bytes: &[u8], tags: &TagSet, coarse: bool) -> Bytes {
    bincode::serialize(&FrameRef {
        tags,
        coarse,
        payload: bytes,
    })
    .expect("provenance frame serialization cannot fail")
    .into()
}

fn unframe_bytes(bytes: &[u8]) -> (TagSet, bool, Bytes) {
    let frame: FrameOwned =
        bincode::deserialize(bytes).expect("provenance frame deserialization failed");
    (frame.tags, frame.coarse, frame.payload.into())
}

impl NetworkPayload for Bytes {
    fn payload_len(&self) -> usize {
        self.len()
    }

    fn payload_bytes(&self) -> &[u8] {
        self
    }

    fn recipient(&self) -> Option<u32> {
        None
    }

    fn frame(self, tags: &TagSet, coarse: bool) -> Self {
        frame_bytes(&self, tags, coarse)
    }

    fn unframe(self) -> (TagSet, bool, Self) {
        unframe_bytes(&self)
    }
}

impl NetworkPayload for (crate::location::TaglessMemberId, Bytes) {
    fn payload_len(&self) -> usize {
        self.1.len()
    }

    fn payload_bytes(&self) -> &[u8] {
        &self.1
    }

    fn recipient(&self) -> Option<u32> {
        Some(self.0.get_raw_id())
    }

    fn frame(self, tags: &TagSet, coarse: bool) -> Self {
        (self.0, frame_bytes(&self.1, tags, coarse))
    }

    fn unframe(self) -> (TagSet, bool, Self) {
        let (tags, coarse, payload) = unframe_bytes(&self.1);
        (tags, coarse, (self.0, payload))
    }
}

// ---------------------------------------------------------------------------------------------
// Emission log.
// ---------------------------------------------------------------------------------------------

/// Where a physical emission was observed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum EmissionPointKind {
    /// A message serialized onto a network channel.
    Network,
    /// A message deserialized off a network channel: the recipient now holds this lineage. Not
    /// physical work; used to advance per-channel history so that novelty is judged against
    /// what was *received*, which differs from what was sent under message loss.
    Receive,
    /// An item serialized to a sim output port.
    Output,
    /// An item crossing a cycle sink (includes per-tick state carry; usually ignored).
    Cycle,
}

/// One physical emission and the lineage of the item that caused it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmissionRecord {
    /// Network edge, sim output, or cycle sink.
    pub kind: EmissionPointKind,
    /// Stable index of the emission point within the program (assigned by the provenance pass).
    pub point: u32,
    /// Human-readable name of the emission point (channel name if given, else a location pair).
    pub name: String,
    /// The sending cluster member, if the sender is a cluster (for `Receive` records this is
    /// recovered from the channel's demux id, so send and receipt share a [`Channel`]).
    pub member: Option<u32>,
    /// The receiving location (a location for network sends, the port for sim outputs).
    pub destination: String,
    /// The receiving cluster member, if the receiver is a cluster.
    pub recipient: Option<u32>,
    /// Lineage of the emitted item.
    pub tags: TagSet,
    /// Whether the lineage is an over-approximation (see [`Tagged::coarse`]).
    pub coarse: bool,
    /// Serialized payload bytes (zero for cycle sinks).
    pub bytes: usize,
    /// Hash of the serialized payload (zero for cycle sinks). Content identity, not lineage:
    /// lets a test count *distinct* payloads on a channel when lineage downstream of opaque
    /// state is too coarse to separate originals from duplicates.
    pub payload_hash: u64,
}

impl EmissionRecord {
    /// Data lineage only.
    pub fn data_tags(&self) -> impl Iterator<Item = &Tag> {
        self.tags.iter().filter(|t| t.kind == TagKind::Data)
    }

    /// Operational lineage only.
    pub fn operational_tags(&self) -> impl Iterator<Item = &Tag> {
        self.tags.iter().filter(|t| t.kind == TagKind::Operational)
    }
}

thread_local! {
    static EMISSIONS: RefCell<Vec<EmissionRecord>> = const { RefCell::new(Vec::new()) };
    static SOURCE_SEQ: RefCell<BTreeMap<(u32, u32), u64>> = const { RefCell::new(BTreeMap::new()) };
}

/// Allocates the next source-event sequence number for `(port, member)`. Called from generated
/// simulation code; lives outside the dataflow because DFIR re-evaluates operator closure
/// expressions on every subgraph run, so a counter captured by the closure would reset.
#[doc(hidden)]
pub fn next_source_seq(port: u32, member: u32) -> u64 {
    SOURCE_SEQ.with(|m| {
        let mut m = m.borrow_mut();
        let e = m.entry((port, member)).or_insert(0);
        let s = *e;
        *e += 1;
        s
    })
}

/// Appends an emission to the log. Called from generated simulation code.
#[doc(hidden)]
pub fn record(record: EmissionRecord) {
    EMISSIONS.with(|log| log.borrow_mut().push(record));
}

/// Drains the log into a serialized buffer. Exported from the simulation dylib as
/// `__hydro_provenance_drain`; not intended to be called directly.
#[doc(hidden)]
pub fn drain_serialized() -> Vec<u8> {
    let records = EMISSIONS.with(|log| std::mem::take(&mut *log.borrow_mut()));
    // A drain marks the end of an observation epoch but not of the instance; sequence numbers
    // keep increasing so tags stay unique across epochs within one instance. They are reset
    // when a new instance starts (see `reset_instance`).
    bincode::serialize(&records).expect("emission log serialization cannot fail")
}

/// Clears per-instance state (emission log and source counters). Exported from the simulation
/// dylib as `__hydro_provenance_reset`; called by the host when a new instance starts.
#[doc(hidden)]
pub fn reset_instance() {
    EMISSIONS.with(|log| log.borrow_mut().clear());
    SOURCE_SEQ.with(|m| m.borrow_mut().clear());
}

/// Signature of the dylib's exported reset function.
pub(crate) type ResetFn = unsafe extern "Rust" fn();

/// Signature of the dylib's exported drain function.
pub(crate) type DrainFn = unsafe extern "Rust" fn() -> Vec<u8>;

thread_local! {
    pub(crate) static HOST_DRAIN: RefCell<Option<DrainFn>> = const { RefCell::new(None) };
}

/// Takes every emission recorded since the last call, in program order.
///
/// Must be called from within a simulation compiled with
/// [`crate::sim::flow::SimFlow::with_provenance`]; typically after
/// [`crate::sim::quiesce`] so the log covers a complete causal epoch.
pub fn take_emissions() -> Vec<EmissionRecord> {
    let drain = HOST_DRAIN
        .with(|d| *d.borrow())
        .expect("take_emissions() requires a simulation compiled with `with_provenance()`");
    let bytes = unsafe { drain() };
    bincode::deserialize(&bytes).expect("emission log deserialization failed")
}

// ---------------------------------------------------------------------------------------------
// Classification.
// ---------------------------------------------------------------------------------------------

/// The lineage shape of one emission. Derived purely from tag sets; no operator or protocol
/// names are consulted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Label {
    /// No lineage at all: a constant or membership-derived item.
    Constant,
    /// Operational lineage only. Fixed-size work funded by a stimulus, independent of any data
    /// (a heartbeat, a vote request).
    FixedOperational,
    /// Carries data lineage that had never crossed this emission point before: new information
    /// is leaving the node.
    Productive,
    /// Carries only already-emitted data lineage *and* an operational tag: a stimulus turned
    /// retained state back into physical work.
    Reactivated,
    /// Carries only already-emitted data lineage and no operational tag: a data-driven
    /// re-emission (e.g. an at-least-once relay).
    Redundant,
}

/// One classified emission.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Classified {
    /// The emission.
    pub record: EmissionRecord,
    /// Its lineage-shape label.
    pub label: Label,
    /// Data tags the recipient had never received from this sender before this emission.
    /// May be empty for a `Productive` emission whose lineage is a novel *combination*.
    pub novel: TagSet,
}

/// Identifies one sender→receiver relationship: the scope over which "already told" is judged.
pub type Channel = (Option<u32>, String, Option<u32>);

impl EmissionRecord {
    /// The (sender member, destination, recipient member) this emission travelled along.
    pub fn channel(&self) -> Channel {
        (self.member, self.destination.clone(), self.recipient)
    }
}

/// Classifies each emission relative to what its sender had already told its recipient, in log
/// order. Cycle-sink records are skipped unless `include_cycles` is set, since they include
/// per-tick state carry that is not physical work.
///
/// **Novelty is dominance-based.** An emission is non-novel when its data lineage is a subset
/// of a *single* earlier emission on the same channel: everything it descends from had
/// already reached that recipient together. A transitive-closure fact derived from two known
/// edges is therefore novel (no single prior emission carried both), while a gossip pump
/// re-sending an unchanged set, or a retry of a request, is not.
///
/// **History is per channel**, not per operator. A node that has told a peer something over one
/// network edge and repeats it over another has done redundant physical work; a node
/// forwarding to a peer that has not heard it has not.
///
/// **History advances on receipt, not on send.** A network send is judged against the lineage
/// its recipient has already *received* ([`EmissionPointKind::Receive`] records), so a retry of
/// a message that was lost is `Productive`, while a retry of one that arrived is `Reactivated`.
/// Sim outputs and cycle sinks have no receiver and advance history themselves.
///
/// **Runs of identical *coarse* lineage are one causal unit.** A closure reading opaque state
/// stamps every item it emits in one invocation with the same (over-approximate) tag set, so
/// consecutive coarse emissions with identical tags are classified against the history as it
/// stood before the run, then history is updated once. Exact-lineage emissions are always
/// judged individually: identical exact lineage twice really is the same information twice.
pub fn classify(records: &[EmissionRecord], include_cycles: bool) -> Vec<Classified> {
    // Per channel: the antichain of maximal data-lineage sets emitted so far, plus the union.
    struct History {
        maximal: Vec<TagSet>,
        union: TagSet,
    }
    impl History {
        fn dominates(&self, data: &TagSet) -> bool {
            self.maximal.iter().any(|m| data.is_subset(m))
        }
        fn add(&mut self, data: &TagSet) {
            if self.dominates(data) {
                return;
            }
            self.maximal.retain(|m| !m.is_subset(data));
            self.maximal.push(data.clone());
            self.union.extend(data.iter().copied());
        }
    }

    let records: Vec<&EmissionRecord> = records
        .iter()
        .filter(|r| include_cycles || r.kind != EmissionPointKind::Cycle)
        .collect();
    let mut seen: BTreeMap<Channel, History> = BTreeMap::new();
    let mut out = Vec::with_capacity(records.len());
    let mut i = 0;
    while i < records.len() {
        if records[i].kind == EmissionPointKind::Receive {
            let data: TagSet = records[i].data_tags().copied().collect();
            seen.entry(records[i].channel())
                .or_insert_with(|| History {
                    maximal: Vec::new(),
                    union: TagSet::new(),
                })
                .add(&data);
            i += 1;
            continue;
        }
        let mut j = i + 1;
        while records[i].coarse
            && j < records.len()
            && records[j].coarse
            && records[j].tags == records[i].tags
        {
            j += 1;
        }
        let run = &records[i..j];
        let data: TagSet = run[0].data_tags().copied().collect();
        let has_operational = run[0].operational_tags().next().is_some();
        for r in run {
            let history = seen.entry(r.channel()).or_insert_with(|| History {
                maximal: Vec::new(),
                union: TagSet::new(),
            });
            let novel: TagSet = data.difference(&history.union).copied().collect();
            let label = if data.is_empty() {
                if has_operational {
                    Label::FixedOperational
                } else {
                    Label::Constant
                }
            } else if !history.dominates(&data) {
                Label::Productive
            } else if has_operational {
                Label::Reactivated
            } else {
                Label::Redundant
            };
            out.push(Classified {
                record: (*r).clone(),
                label,
                novel,
            });
        }
        for r in run {
            // Network sends advance history only when received (see above).
            if r.kind != EmissionPointKind::Network {
                seen.get_mut(&r.channel()).unwrap().add(&data);
            }
        }
        i = j;
    }
    out
}

/// Aggregate physical work at one emission point attributed to one operational source event.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Attribution {
    /// Number of emissions.
    pub messages: usize,
    /// Total payload bytes.
    pub bytes: usize,
    /// Distinct data tags carried by those messages (the retained state the stimulus funded).
    pub data_tags: TagSet,
    /// Whether any contributing lineage was coarse.
    pub coarse: bool,
}

/// Work per (emission point name, operational tag). Emissions with no operational lineage are
/// grouped under `None`.
pub fn attribute(records: &[EmissionRecord]) -> BTreeMap<(String, Option<Tag>), Attribution> {
    let mut out: BTreeMap<(String, Option<Tag>), Attribution> = BTreeMap::new();
    for record in records {
        if matches!(
            record.kind,
            EmissionPointKind::Cycle | EmissionPointKind::Receive
        ) {
            continue;
        }
        let ops: Vec<Option<Tag>> = {
            let v: Vec<_> = record.operational_tags().copied().map(Some).collect();
            if v.is_empty() { vec![None] } else { v }
        };
        for op in ops {
            let entry = out.entry((record.name.clone(), op)).or_default();
            entry.messages += 1;
            entry.bytes += record.bytes;
            entry.data_tags.extend(record.data_tags().copied());
            entry.coarse |= record.coarse;
        }
    }
    out
}

/// Counts of each label at each emission point name.
pub fn label_counts(classified: &[Classified]) -> BTreeMap<String, BTreeMap<Label, usize>> {
    let mut out: BTreeMap<String, BTreeMap<Label, usize>> = BTreeMap::new();
    for c in classified {
        *out.entry(c.record.name.clone())
            .or_default()
            .entry(c.label)
            .or_default() += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tag(kind: TagKind, seq: u64) -> Tag {
        Tag {
            kind,
            port: 0,
            member: 0,
            seq,
        }
    }

    fn coarse(mut r: EmissionRecord) -> EmissionRecord {
        r.coarse = true;
        r
    }

    fn rec(tags: &[Tag], point: u32) -> EmissionRecord {
        EmissionRecord {
            kind: EmissionPointKind::Output,
            point,
            name: format!("edge{point}"),
            member: None,
            destination: format!("dest{point}"),
            recipient: None,
            tags: tags.iter().copied().collect(),
            coarse: false,
            bytes: 10,
            payload_hash: 0,
        }
    }

    #[test]
    fn combinations_are_novel_and_channels_share_history_across_edges() {
        let ab = tag(TagKind::Data, 1);
        let bc = tag(TagKind::Data, 2);
        let t = tag(TagKind::Operational, 1);
        let same_dest = |tags: &[Tag], point: u32| {
            let mut r = rec(tags, point);
            r.destination = "peer".into();
            r
        };
        let records = vec![
            same_dest(&[ab], 0),        // base fact
            same_dest(&[bc], 0),        // base fact
            same_dest(&[ab, bc, t], 0), // derived fact: novel combination despite no new tag
            same_dest(&[ab, bc, t], 1), // same lineage to the same peer over another edge
            same_dest(&[ab], 0),        // dominated by the derived fact
        ];
        let labels: Vec<Label> = classify(&records, false)
            .into_iter()
            .map(|c| c.label)
            .collect();
        assert_eq!(
            labels,
            vec![
                Label::Productive,
                Label::Productive,
                Label::Productive,
                Label::Reactivated,
                Label::Redundant,
            ]
        );
    }

    #[test]
    fn labels_follow_lineage_shape() {
        let d1 = tag(TagKind::Data, 1);
        let d2 = tag(TagKind::Data, 2);
        let t1 = tag(TagKind::Operational, 1);
        let t2 = tag(TagKind::Operational, 2);
        let records = vec![
            rec(&[], 0),                   // constant
            rec(&[t1], 0),                 // heartbeat
            rec(&[d1], 0),                 // first send of d1
            rec(&[d1, t1], 0),             // retry of d1 on timer
            coarse(rec(&[d1, d2, t2], 0)), // opaque-state pump carrying new d2
            coarse(rec(&[d1, d2, t2], 0)), // same invocation: grouped with the previous record
            rec(&[d1], 0),                 // data-driven duplicate
            rec(&[d1, d2, t2], 0),         // later pump with nothing new
            rec(&[d1], 1),                 // same data on a different edge is novel there
        ];
        let labels: Vec<Label> = classify(&records, false)
            .into_iter()
            .map(|c| c.label)
            .collect();
        assert_eq!(
            labels,
            vec![
                Label::Constant,
                Label::FixedOperational,
                Label::Productive,
                Label::Reactivated,
                Label::Productive,
                Label::Productive,
                Label::Redundant,
                Label::Reactivated,
                Label::Productive,
            ]
        );
    }

    #[test]
    fn network_history_advances_on_receipt_not_send() {
        let d1 = tag(TagKind::Data, 1);
        let t1 = tag(TagKind::Operational, 1);
        let send = |tags: &[Tag]| {
            let mut r = rec(tags, 0);
            r.kind = EmissionPointKind::Network;
            r
        };
        let receive = |tags: &[Tag]| {
            let mut r = rec(tags, 0);
            r.kind = EmissionPointKind::Receive;
            r
        };
        // Sent, lost (no receipt), retried: the retry is the first thing the recipient gets.
        let lost = vec![send(&[d1]), send(&[d1, t1])];
        let labels: Vec<Label> = classify(&lost, false).into_iter().map(|c| c.label).collect();
        assert_eq!(labels, vec![Label::Productive, Label::Productive]);
        // Sent, received, retried: the retry is redundant work.
        let delivered = vec![send(&[d1]), receive(&[d1]), send(&[d1, t1])];
        let labels: Vec<Label> = classify(&delivered, false).into_iter().map(|c| c.label).collect();
        assert_eq!(labels, vec![Label::Productive, Label::Reactivated]);
    }

    #[test]
    fn frame_roundtrip_preserves_payload_and_tags() {
        let tags: TagSet = [tag(TagKind::Data, 7)].into_iter().collect();
        let payload = Bytes::from_static(b"hello");
        let framed = payload.clone().frame(&tags, true);
        assert_ne!(framed, payload);
        let (t, coarse, p) = framed.unframe();
        assert_eq!(t, tags);
        assert!(coarse);
        assert_eq!(p, payload);
        assert_eq!(p.payload_len(), 5);
    }
}
