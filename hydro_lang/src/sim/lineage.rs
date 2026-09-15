//! A side log of the records that cross the simulator's tick-boundary hooks, each under a
//! record id, kept on the host (E3.1 of the amplification design,
//! `design_docs/2026-09_amplification_as_adversarial_scheduling.md`).
//!
//! [`super::edge_counts`] counts how many records a `batch` hook releases. This module also says
//! *which*: every record that reaches a hook's buffer is given a fresh id the first time the
//! scheduler asks that hook for a decision, and every release is logged with the ids and
//! contents of the records released, the tick of the hook's slice at which each of them arrived,
//! and the ids still held in the buffer afterwards (the deliveries a schedule is holding back,
//! which is what links a re-derivation to the adversary's move).
//!
//! The ids live here, not in the program: the compiled program is a separate dylib whose
//! statics are not shared with the host, and a `batch` buffer only ever grows at its back and
//! only shrinks through a decision the scheduler sees. So the host keeps, per hook, a list of
//! ids parallel to the buffer, extends it with fresh ids when the buffer has grown since the
//! last decision, and removes the positions the hook reports it is releasing
//! ([`super::runtime::SimHook::pending_release_positions`]). The record contents come across as
//! bincode (when the element type is `Serialize`) and as a `Debug` string, produced by function
//! pointers the generated code stores in the hook; the host deserializes with its own copy of
//! the type, exactly as `sim_output` does.
//!
//! This is lineage at the boundary only: a record released by a hook has no recorded parents.
//! Attribution to a goal is done by the harness, which knows what its records mean (a request
//! id, a log entry). Intra-tick derivation records are E3.2.
//!
//! Nothing is recorded unless a run is wrapped in [`trace_lineage`].

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap, VecDeque};

use serde::de::DeserializeOwned;

use super::edge_counts::{EdgeLocation, edge_key};
pub use super::lineage_pass::OperatorInfo;
use super::lineage_rt::LineageEvent;
use super::runtime::ReleasedRecord;

/// The id of a record crossing an edge, unique within a run.
pub type RecordId = u64;

/// Ids the host assigns itself (when the program was not compiled with the lineage pass) start
/// here, so they never collide with the program's own ids.
const HOST_ID_BASE: RecordId = 1 << 63;

/// One derivation record from the instrumented program (E3.2): operator `op` produced `record`
/// from `parents` (none for a leaf).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Derivation {
    /// The produced record.
    pub record: RecordId,
    /// The operator, an index into [`Lineage::operators`].
    pub op: u32,
    /// The records it was computed from.
    pub parents: Vec<RecordId>,
    /// How many hook releases had been logged when this derivation happened; derivations of a
    /// tick follow the releases of that tick's hooks.
    pub after_release: u64,
    /// Whether this is a new version of a state a closure mutated in place (`by_mut`), rather
    /// than a record the operator emitted. State-carry edges are followed or not at analysis
    /// time (see [`Lineage::children_with`]).
    pub state_version: bool,
}

/// A record an operator discarded (E3.2): a duplicate at `unique`, with the survivor named.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Drop {
    /// The discarded record.
    pub record: RecordId,
    /// The operator that discarded it.
    pub op: u32,
    /// The record kept in its place, if any.
    pub survivor: Option<RecordId>,
    /// See [`Derivation::after_release`].
    pub after_release: u64,
}

/// A record that left the program through an external output (`sim_output`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Output {
    /// The record.
    pub record: RecordId,
    /// The output operator.
    pub op: u32,
}

/// One record released by a hook.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// The record's id, fresh when it first appeared in a hook's buffer.
    pub id: RecordId,
    /// The hook's decision index (tick of its slice) at which the record was first seen in the
    /// buffer. `tick - arrived` of the enclosing [`Release`] is how long it was held.
    pub arrived: u64,
    /// bincode serialization of the record, if its type is `Serialize`.
    pub payload: Option<Vec<u8>>,
    /// `Debug` rendering of the record, if its type is `Debug`.
    pub debug: Option<String>,
}

impl Record {
    /// Deserializes the record's payload as `T`, the host's copy of the element type. Panics
    /// if the payload does not decode as `T` (the harness named the wrong type for the edge);
    /// `None` if the element type is not `Serialize`.
    pub fn decode<T: DeserializeOwned>(&self) -> Option<T> {
        self.payload.as_ref().map(|bytes| {
            bincode::deserialize(bytes).unwrap_or_else(|e| {
                panic!(
                    "record {} ({}) does not decode as {}: {e}",
                    self.id,
                    self.debug.as_deref().unwrap_or("?"),
                    std::any::type_name::<T>()
                )
            })
        })
    }
}

/// One decision of one hook: what it released and what it kept.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Release {
    /// The edge, keyed as in [`edge_key`].
    pub edge: String,
    /// The cluster member the hook belongs to, if any.
    pub member: Option<u32>,
    /// The hook's decision index (0-based): the tick count of its slice when it released.
    pub tick: u64,
    /// The position of this release among all recorded releases, ordering releases of
    /// different hooks.
    pub serial: u64,
    /// The records released, in release order.
    pub released: Vec<Record>,
    /// Ids still buffered after this release, oldest first: the deliveries being held.
    pub held: Vec<RecordId>,
}

/// The log of one run: every release of every edge hook, in scheduler order (E3.1), and, when
/// the program was compiled with the lineage pass (`SimFlow::with_lineage`), every derivation,
/// drop and output the operators reported (E3.2) with the operator table.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Lineage {
    /// Every release of every edge hook, in scheduler order.
    pub releases: Vec<Release>,
    /// Whether the program was compiled with the lineage pass (the fields below are populated
    /// and record ids are the program's own).
    pub instrumented: bool,
    /// Every derivation the operators reported, in execution order.
    pub derivations: Vec<Derivation>,
    /// Every record an operator discarded.
    pub drops: Vec<Drop>,
    /// Every record that left through an external output.
    pub outputs: Vec<Output>,
    /// The operator table; `Derivation::op` indexes it.
    pub operators: Vec<OperatorInfo>,
}

impl Lineage {
    /// Releases on edges whose key satisfies `pred`.
    pub fn releases_on<'a>(
        &'a self,
        pred: impl Fn(&str) -> bool + 'a,
    ) -> impl Iterator<Item = &'a Release> + 'a {
        self.releases.iter().filter(move |r| pred(&r.edge))
    }

    /// Records released on edges whose key satisfies `pred`, with their release.
    pub fn records_on<'a>(
        &'a self,
        pred: impl Fn(&str) -> bool + 'a,
    ) -> impl Iterator<Item = (&'a Release, &'a Record)> + 'a {
        self.releases_on(pred)
            .flat_map(|release| release.released.iter().map(move |record| (release, record)))
    }

    /// Records released on edges whose key satisfies `pred`; equals the edge count of
    /// [`super::edge_counts`] for the same edges.
    pub fn record_count(&self, pred: impl Fn(&str) -> bool) -> u64 {
        self.records_on(pred).count() as u64
    }

    /// Derivations per goal on the edges matching `pred`: every record is decoded as `T` (the
    /// harness's type for that edge) and `goals` names the goals it was derived toward (none,
    /// one, or several: an `AppendEntries` carries many entries). Returns how many records
    /// were derived toward each goal.
    pub fn per_goal<T: DeserializeOwned, G: Ord>(
        &self,
        pred: impl Fn(&str) -> bool,
        goals: impl Fn(T) -> Vec<G>,
    ) -> BTreeMap<G, u64> {
        let mut counts = BTreeMap::new();
        for (_, record) in self.records_on(pred) {
            let value: T = record.decode().unwrap_or_else(|| {
                panic!("record {} on a non-Serialize edge cannot be attributed", record.id)
            });
            for goal in goals(value) {
                *counts.entry(goal).or_default() += 1;
            }
        }
        counts
    }

    /// Edge keys that appear in the log, in order of first appearance.
    pub fn edges(&self) -> Vec<&str> {
        let mut seen = vec![];
        for release in &self.releases {
            if !seen.contains(&release.edge.as_str()) {
                seen.push(release.edge.as_str());
            }
        }
        seen
    }
}

/// The host's id bookkeeping for one hook: the ids (and arrival ticks) of the records in the
/// buffer, in buffer order, and how many decisions the hook has been asked for.
#[derive(Debug, Default)]
struct HookIds {
    ticks: u64,
    buffered: VecDeque<(RecordId, u64)>,
}

#[derive(Debug)]
struct Recorder {
    log: Lineage,
    next_id: RecordId,
    hooks: HashMap<(String, Option<u32>), HookIds>,
}

impl Default for Recorder {
    fn default() -> Self {
        Self {
            log: Lineage::default(),
            next_id: HOST_ID_BASE,
            hooks: HashMap::new(),
        }
    }
}

/// Called by the host's lineage sink for every event the instrumented program reports.
pub(crate) fn record_event(event: LineageEvent) {
    RECORDER.with(|r| {
        let mut borrow = r.borrow_mut();
        let Some(recorder) = borrow.as_mut() else {
            return;
        };
        let after_release = recorder.log.releases.len() as u64;
        match event {
            LineageEvent::Derived {
                record,
                op,
                parents,
            } => recorder.log.derivations.push(Derivation {
                record,
                op,
                parents,
                after_release,
                state_version: false,
            }),
            LineageEvent::StateVersion {
                record,
                op,
                parents,
            } => recorder.log.derivations.push(Derivation {
                record,
                op,
                parents,
                after_release,
                state_version: true,
            }),
            LineageEvent::Dropped {
                record,
                op,
                survivor,
            } => recorder.log.drops.push(Drop {
                record,
                op,
                survivor,
                after_release,
            }),
            LineageEvent::Output { record, op } => {
                recorder.log.outputs.push(Output { record, op })
            }
        }
    });
}

thread_local! {
    static RECORDER: RefCell<Option<Recorder>> = const { RefCell::new(None) };
}

/// Runs `f` with lineage recording enabled on this thread and returns the log. Nested calls
/// are not supported (the inner run replaces the outer recorder).
pub fn trace_lineage<R>(f: impl FnOnce() -> R) -> (R, Lineage) {
    RECORDER.with(|r| *r.borrow_mut() = Some(Recorder::default()));
    let result = f();
    let log = RECORDER
        .with(|r| r.borrow_mut().take())
        .map(|recorder| recorder.log)
        .unwrap_or_default();
    (result, log)
}

/// Whether a lineage log is being kept on this thread (so the scheduler can skip asking hooks
/// to serialize their records otherwise).
pub(crate) fn recording() -> bool {
    RECORDER.with(|r| r.borrow().is_some())
}

/// Called by the scheduler once per hook per tick, after the hook has decided and before it
/// releases. `buffered_after` is the buffer length after the decision; `positions` are the
/// positions the decision releases, in the buffer as it stood before (see
/// [`super::runtime::SimHook::pending_release_positions`]); `records` produces the released
/// records' contents in the same order.
///
/// `ids` and `held_ids` are the program's own ids for the released and the still-buffered
/// records, when it was compiled with the lineage pass; then they replace the host's ids and the
/// 8-byte id prefix is stripped from each payload.
pub(crate) fn record_release(
    location: EdgeLocation,
    index: usize,
    member: Option<u32>,
    buffered_after: usize,
    positions: Vec<usize>,
    records: impl FnOnce() -> Vec<ReleasedRecord>,
    ids: Option<Vec<u64>>,
    held_ids: Option<Vec<u64>>,
) {
    RECORDER.with(|r| {
        let mut borrow = r.borrow_mut();
        let Some(recorder) = borrow.as_mut() else {
            return;
        };
        let edge = edge_key(location, index);
        let hook = recorder
            .hooks
            .entry((edge.clone(), member))
            .or_default();
        let tick = hook.ticks;
        hook.ticks += 1;

        let buffered_before = buffered_after + positions.len();
        assert!(
            hook.buffered.len() <= buffered_before,
            "{edge}: the buffer shrank from {} to {buffered_before} without a decision",
            hook.buffered.len()
        );
        while hook.buffered.len() < buffered_before {
            hook.buffered.push_back((recorder.next_id, tick));
            recorder.next_id += 1;
        }

        let contents = records();
        assert_eq!(contents.len(), positions.len(), "{edge}: released records and positions differ");
        if let Some(ids) = &ids {
            assert_eq!(ids.len(), positions.len(), "{edge}: released ids and positions differ");
        }
        let mut released = Vec::with_capacity(positions.len());
        for (i, (&position, (payload, debug))) in positions.iter().zip(contents).enumerate() {
            let (host_id, arrived) = hook.buffered[position];
            let (id, payload) = match &ids {
                Some(ids) => {
                    let id = ids[i];
                    // The payload is bincode of `(u64, T)`: a fixed 8-byte little-endian id, then `T`.
                    let payload = payload.map(|bytes| {
                        assert!(bytes.len() >= 8, "{edge}: instrumented payload without an id prefix");
                        let prefix = u64::from_le_bytes(bytes[..8].try_into().unwrap());
                        assert_eq!(prefix, id, "{edge}: payload id prefix and hook id differ");
                        bytes[8..].to_vec()
                    });
                    (id, payload)
                }
                None => (host_id, payload),
            };
            released.push(Record {
                id,
                arrived,
                payload,
                debug,
            });
        }
        // Remove released positions back to front so the earlier indexes stay valid.
        let mut sorted = positions;
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), released.len(), "{edge}: a position was released twice");
        for position in sorted.into_iter().rev() {
            hook.buffered.remove(position);
        }
        let held = match held_ids {
            Some(held) => {
                assert_eq!(held.len(), hook.buffered.len(), "{edge}: held ids and positions differ");
                held
            }
            None => hook.buffered.iter().map(|(id, _)| *id).collect(),
        };

        let serial = recorder.log.releases.len() as u64;
        recorder.log.releases.push(Release {
            edge,
            member,
            tick,
            serial,
            released,
            held,
        });
    });
}

/// Analysis over the derivation records (E3.2): reachability from a goal's input record to the
/// records derived toward it.
impl Lineage {
    /// The operator a derivation record refers to.
    pub fn operator(&self, op: u32) -> &OperatorInfo {
        &self.operators[op as usize]
    }

    /// Operators whose table row satisfies `pred`.
    pub fn operators_where<'a>(
        &'a self,
        pred: impl Fn(&OperatorInfo) -> bool + 'a,
    ) -> impl Iterator<Item = &'a OperatorInfo> + 'a {
        self.operators.iter().filter(move |o| pred(o))
    }

    /// Every record with a derivation record, mapped to its parents.
    pub fn parents(&self) -> HashMap<RecordId, &[RecordId]> {
        self.derivations
            .iter()
            .map(|d| (d.record, d.parents.as_slice()))
            .collect()
    }

    /// Every record mapped to the records derived directly from it.
    pub fn children(&self) -> HashMap<RecordId, Vec<RecordId>> {
        self.children_with(true)
    }

    /// [`Self::children`], optionally without the state chain: when `follow_state` is false, a
    /// state version is not a child of the previous version, so a record reaches only the
    /// inputs of the tick that produced it and of the tick that last changed the state, not
    /// everything the state ever absorbed.
    pub fn children_with(&self, follow_state: bool) -> HashMap<RecordId, Vec<RecordId>> {
        let versions: std::collections::HashSet<RecordId> = self
            .derivations
            .iter()
            .filter(|d| d.state_version)
            .map(|d| d.record)
            .collect();
        let mut children: HashMap<RecordId, Vec<RecordId>> = HashMap::new();
        for d in &self.derivations {
            for parent in &d.parents {
                if !follow_state && d.state_version && versions.contains(parent) {
                    continue;
                }
                children.entry(*parent).or_default().push(d.record);
            }
        }
        children
    }

    /// Every record derived, directly or transitively, from `root` (not including `root`), given
    /// a children index from [`Self::children`].
    pub fn descendants(
        &self,
        root: RecordId,
        children: &HashMap<RecordId, Vec<RecordId>>,
    ) -> std::collections::HashSet<RecordId> {
        let mut seen = std::collections::HashSet::new();
        let mut stack = vec![root];
        while let Some(id) = stack.pop() {
            if let Some(kids) = children.get(&id) {
                for kid in kids {
                    if seen.insert(*kid) {
                        stack.push(*kid);
                    }
                }
            }
        }
        seen
    }

    /// Derivations per goal by ancestry: `goals` names each goal by the id of its input record
    /// (e.g. the record on the requests edge that carries request `k`); the count for a goal is
    /// the number of records released on the edges matching `pred` whose ancestry contains that
    /// record. A record descending from several goals counts toward each.
    pub fn per_goal_by_ancestry<G: Ord + Clone>(
        &self,
        pred: impl Fn(&str) -> bool,
        goals: &BTreeMap<RecordId, G>,
    ) -> BTreeMap<G, u64> {
        self.per_goal_by_ancestry_with(pred, goals, true)
    }

    /// [`Self::per_goal_by_ancestry`] with or without the state chain (see
    /// [`Self::children_with`]).
    pub fn per_goal_by_ancestry_with<G: Ord + Clone>(
        &self,
        pred: impl Fn(&str) -> bool,
        goals: &BTreeMap<RecordId, G>,
        follow_state: bool,
    ) -> BTreeMap<G, u64> {
        let children = self.children_with(follow_state);
        let on_edge: std::collections::HashSet<RecordId> =
            self.records_on(pred).map(|(_, r)| r.id).collect();
        let mut counts = BTreeMap::new();
        for (root, goal) in goals {
            let reached = self.descendants(*root, &children);
            let n = on_edge.iter().filter(|id| reached.contains(id)).count() as u64;
            counts.insert(goal.clone(), n);
        }
        counts
    }

    /// Drops reported by operators whose table row satisfies `pred`.
    pub fn drops_at<'a>(
        &'a self,
        pred: impl Fn(&OperatorInfo) -> bool + 'a,
    ) -> impl Iterator<Item = &'a Drop> + 'a {
        self.drops.iter().filter(move |d| pred(self.operator(d.op)))
    }
}
