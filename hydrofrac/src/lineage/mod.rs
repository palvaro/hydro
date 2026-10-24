//! Derivations of observed facts from one run's event log (docs/design/hydrofrac-lite.md §4.2).
//!
//! An observed fact is an [`Event::Output`]. Its *support* is the set of deliveries it was
//! computed from, closed backwards through the sends that carried them:
//!
//! - **Record rule.** A record whose provenance names deliveries (the metadata the provenance
//!   rewrite attaches, [`RecordProv::Ids`]) was computed from exactly those deliveries. Each of
//!   them was produced by a send whose own record names *its* deliveries, and so on.
//! - **Execution rule**, the fallback for opaque records ([`RecordProv::Opaque`], or no metadata
//!   at all). A record produced in execution `X` at node `n` depends on every delivery `n`
//!   received before `X` ended, in program order. That covers what `X` received and everything
//!   earlier executions at `n` received and may have folded into state, so it is sound for any
//!   program; it only costs precision.
//!
//! The two rules mix freely: the closure applies whichever rule each record on the way admits.
//!
//! A delivery in a support becomes a [`Dependency`]: it is omittable if its channel is faulty
//! (lossy) and it was sent before the network healed, and it is
//! killed by a crash of its sender if the sender can crash. A delivery that neither kind of fault
//! can kill is dropped from the derivation, but the closure still runs through it. A
//! [`Clause`] pairs a derivation with the run's send sequences and pre-heal send counts, which
//! give positional crash points their meaning in that run.
//!
//! Only delivered sends can appear in a support: provenance names deliveries by the tags their
//! receivers saw, and the execution rule follows receives.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::rc::Rc;

use hydro_lang::sim::network_hook::{ChannelInfo, LocationInfo};

use crate::fault::{DeliveryId, Event, EventLog, Fate, MessageId, NodeId, NodeNames};
use crate::search::{Clause, Dependency, Derivation, PreHealSends, SendSequences};

pub use crate::provenance::RecordProv;
use crate::provenance::fragile_site;

/// Decodes a record's metadata ([`RecordProv`], as the provenance rewrite encodes it).
/// Missing or malformed metadata is [`RecordProv::Opaque`]: the execution rule is sound for
/// any record.
pub fn decode_meta(meta: Option<&[u8]>) -> RecordProv {
    meta.and_then(RecordProv::try_decode)
        .unwrap_or(RecordProv::Opaque)
}

/// What the lineage needs to know about the simulation besides the log: node names, which nodes
/// can crash, and which channels admit omission.
#[derive(Clone, Debug)]
pub struct Tables {
    names: NodeNames,
    faulty: BTreeSet<usize>,
}

impl Tables {
    /// The tables of a compiled simulation ([`CompiledSim::channels`] and
    /// [`CompiledSim::locations`]). A channel is faulty if it is lossy, as
    /// [`crate::fault::FaultHook`] decides.
    ///
    /// [`CompiledSim::channels`]: hydro_lang::sim::compiled::CompiledSim::channels
    /// [`CompiledSim::locations`]: hydro_lang::sim::compiled::CompiledSim::locations
    pub fn new(channels: &[ChannelInfo], locations: &[LocationInfo]) -> Self {
        let faulty: Vec<usize> = channels
            .iter()
            .filter(|c| crate::fault::is_lossy(&c.networking))
            .map(|c| c.id)
            .collect();
        Tables::from_parts(NodeNames::new(locations), faulty)
    }

    /// Tables from node names and the ids of the faulty channels.
    pub fn from_parts(names: NodeNames, faulty: impl IntoIterator<Item = usize>) -> Self {
        Tables {
            names,
            faulty: faulty.into_iter().collect(),
        }
    }

    /// The node names.
    pub fn names(&self) -> &NodeNames {
        &self.names
    }

    /// Whether channel `channel` admits omission.
    pub fn is_faulty(&self, channel: usize) -> bool {
        self.faulty.contains(&channel)
    }
}

/// One observed fact of the run: an [`Event::Output`].
#[derive(Clone, Copy, Debug)]
pub struct Observation<'a> {
    /// Its index in the event log (the argument of [`Lineage::clause_for`]).
    pub event: usize,
    /// The external port.
    pub port: usize,
    /// The node that emitted it.
    pub node: &'a NodeId,
    /// The serialized value.
    pub bytes: &'a [u8],
    /// Its metadata.
    pub meta: Option<&'a [u8]>,
}

/// A send of the log, with what the closure needs.
struct SendRecord {
    from: NodeId,
    fate: Fate,
    prov: RecordProv,
    /// The event index before which the execution rule looks for receives.
    bound: usize,
    /// `None` if no fault can kill the delivery.
    dependency: Option<Dependency<MessageId>>,
}

/// An output of the log, with what the closure needs.
struct OutputRecord {
    node: NodeId,
    prov: RecordProv,
    /// The event index before which the execution rule looks for receives.
    bound: usize,
}

/// A record the closure starts from.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
enum Record {
    /// An index into [`Lineage::sends`].
    Send(usize),
    /// An event index of an output.
    Output(usize),
}

/// The lineage of one run: derivations of its observations.
pub struct Lineage<'a> {
    log: &'a EventLog,
    sends: Vec<SendRecord>,
    by_id: HashMap<DeliveryId, usize>,
    /// Outputs by event index.
    outputs: BTreeMap<usize, OutputRecord>,
    /// Receives per node in program order: `(event index, index into sends)`.
    recvs_at: HashMap<NodeId, Vec<(usize, usize)>>,
    sequences: Rc<SendSequences<MessageId>>,
    pre_heal: Rc<PreHealSends>,
    memo: RefCell<HashMap<Record, Closure>>,
    opaque: RefCell<BTreeSet<Record>>,
}

/// The backward closure of one record: its derivation and the fragile sites on it.
#[derive(Clone)]
struct Closure {
    derivation: Rc<Derivation<MessageId>>,
    sites: Rc<BTreeSet<usize>>,
}

/// The site [`Lineage::fragile_sites`] reports for a derivation that reached an opaque record:
/// its lineage is unknown, so it counts as fragile.
pub const OPAQUE_SITE: usize = usize::MAX;

/// The lineage of the run `log` records.
pub fn derive<'a>(log: &'a EventLog, tables: &Tables) -> Lineage<'a> {
    Lineage::new(log, tables)
}

impl<'a> Lineage<'a> {
    /// The lineage of the run `log` records, in a simulation described by `tables`.
    pub fn new(log: &'a EventLog, tables: &Tables) -> Self {
        let events = log.events();
        let heal = events
            .iter()
            .position(|e| matches!(e, Event::Heal))
            .unwrap_or(events.len());

        // Where each execution ends: the bound of every record it produced.
        let mut exec_end: Vec<Option<usize>> = vec![None; events.len()];
        let mut current: Option<usize> = None;
        for (i, event) in events.iter().enumerate() {
            match event {
                Event::Exec { .. } => current = Some(i),
                Event::ExecEnd => {
                    if let Some(start) = current.take() {
                        for slot in &mut exec_end[start..i] {
                            *slot = Some(i);
                        }
                    }
                }
                _ => {}
            }
        }
        if let Some(start) = current {
            // An execution the log does not see end: it extends to the end of the log.
            for slot in &mut exec_end[start..] {
                *slot = Some(events.len());
            }
        }
        let bound = |i: usize| exec_end[i].unwrap_or(i);

        let names = tables.names();
        let crashable: BTreeSet<&NodeId> = names.crashable().iter().collect();
        let mut sends = Vec::new();
        let mut by_id = HashMap::new();
        let mut outputs = BTreeMap::new();
        let mut recv_events = Vec::new();
        let mut sequences: SendSequences<MessageId> = BTreeMap::new();
        let mut pre_heal: PreHealSends = BTreeMap::new();
        for (i, event) in events.iter().enumerate() {
            match event {
                Event::Sent {
                    id,
                    msg,
                    ordinal,
                    meta,
                    fate,
                } => {
                    let before_heal = i < heal;
                    let sender = crashable
                        .contains(&msg.from)
                        .then(|| (names.name(&msg.from), *ordinal));
                    if let Some((node, _)) = &sender
                        && *fate != Fate::SenderDead
                    {
                        sequences.entry(node.clone()).or_default().push(msg.clone());
                        if before_heal {
                            *pre_heal.entry(node.clone()).or_default() += 1;
                        }
                    }
                    let lossy = before_heal && tables.is_faulty(msg.channel);
                    let dependency = (lossy || sender.is_some()).then(|| Dependency {
                        message: msg.clone(),
                        sender,
                        lossy,
                    });
                    by_id.insert(*id, sends.len());
                    sends.push(SendRecord {
                        from: msg.from.clone(),
                        fate: *fate,
                        prov: decode_meta(meta.as_deref()),
                        bound: bound(i),
                        dependency,
                    });
                }
                Event::Recv { id, to, .. } => recv_events.push((i, *id, to)),
                Event::Output { node, meta, .. } => {
                    outputs.insert(
                        i,
                        OutputRecord {
                            node: node.clone(),
                            prov: decode_meta(meta.as_deref()),
                            bound: bound(i),
                        },
                    );
                }
                _ => {}
            }
        }
        let mut recvs_at: HashMap<NodeId, Vec<(usize, usize)>> = HashMap::new();
        for (i, id, to) in recv_events {
            if let Some(&s) = by_id.get(&id)
                && sends[s].fate == Fate::Delivered
            {
                recvs_at.entry(to.clone()).or_default().push((i, s));
            }
        }

        Lineage {
            log,
            sends,
            by_id,
            outputs,
            recvs_at,
            sequences: Rc::new(sequences),
            pre_heal: Rc::new(pre_heal),
            memo: RefCell::new(HashMap::new()),
            opaque: RefCell::new(BTreeSet::new()),
        }
    }

    /// The run's observations, in program order.
    pub fn observations(&self) -> impl Iterator<Item = Observation<'a>> + 'a {
        let events = self.log.events();
        events.iter().enumerate().filter_map(|(event, e)| match e {
            Event::Output {
                port,
                node,
                bytes,
                meta,
            } => Some(Observation {
                event,
                port: *port,
                node,
                bytes,
                meta: meta.as_deref(),
            }),
            _ => None,
        })
    }

    /// Every crashable node's sends in this run, in order (sends after its crash fired
    /// excluded).
    pub fn send_sequences(&self) -> &Rc<SendSequences<MessageId>> {
        &self.sequences
    }

    /// How many of each crashable node's sends happened before the network healed.
    pub fn pre_heal_sends(&self) -> &Rc<PreHealSends> {
        &self.pre_heal
    }

    /// The derivation of the observation at event index `event`.
    ///
    /// # Panics
    /// If `event` is not the index of an [`Event::Output`].
    pub fn derivation(&self, event: usize) -> Rc<Derivation<MessageId>> {
        assert!(
            self.outputs.contains_key(&event),
            "event {event} is not an output"
        );
        self.closure(Record::Output(event)).derivation
    }

    /// The fragile steps the derivation of the observation at event index `event` passed
    /// through (docs/design/hydrofrac-races.md §4.1): the site indices the records of its
    /// backward closure name (ids with [`crate::provenance::FRAGILE_BIT`] set), plus
    /// [`OPAQUE_SITE`] if the closure reached an opaque record. Empty iff the derivation is
    /// robust. Only a run instrumented in fragile mode
    /// ([`crate::provenance::install_fragile`]) names any site.
    ///
    /// # Panics
    /// If `event` is not the index of an [`Event::Output`].
    pub fn fragile_sites(&self, event: usize) -> Rc<BTreeSet<usize>> {
        assert!(
            self.outputs.contains_key(&event),
            "event {event} is not an output"
        );
        self.closure(Record::Output(event)).sites
    }

    /// The derivation of the send carrying delivery `id`, if the log has it: what the sending
    /// record was computed from, not including the delivery itself.
    pub fn send_derivation(&self, id: DeliveryId) -> Option<Rc<Derivation<MessageId>>> {
        let &s = self.by_id.get(&id)?;
        Some(self.closure(Record::Send(s)).derivation)
    }

    /// The clause of the observation at event index `event`: its derivation, with this run's send
    /// sequences and pre-heal send counts. Its premises are empty; the loop sets them to the
    /// crashes of the hypothesis the run was under.
    ///
    /// # Panics
    /// If `event` is not the index of an [`Event::Output`].
    pub fn clause_for(&self, event: usize) -> Clause<MessageId> {
        Clause {
            deps: (*self.derivation(event)).clone(),
            sends: self.sequences.clone(),
            pre_heal_sends: self.pre_heal.clone(),
            premises: BTreeSet::new(),
        }
    }

    /// How many distinct records (sends and outputs) the derivations computed so far reached
    /// that fell back to the execution rule.
    pub fn opaque_fallbacks(&self) -> usize {
        self.opaque.borrow().len()
    }

    /// The closure from `root` (memoized).
    fn closure(&self, root: Record) -> Closure {
        if let Some(d) = self.memo.borrow().get(&root) {
            return d.clone();
        }

        enum Work {
            /// The provenance of a record: what it was computed from.
            Record(Record),
            /// A delivery the closure reached (index into `sends`).
            Delivery(usize),
        }

        let mut derivation = Derivation::new();
        let mut sites = BTreeSet::new();
        let mut reached: BTreeSet<usize> = BTreeSet::new();
        let mut expanded: BTreeSet<Record> = BTreeSet::new();
        // Per node, the bound up to which its receives were already taken: the execution rule
        // is monotone in the bound, so a lower one adds nothing.
        let mut scanned: HashMap<&NodeId, usize> = HashMap::new();
        let mut stack = vec![Work::Record(root)];
        while let Some(work) = stack.pop() {
            match work {
                Work::Delivery(s) => {
                    if !reached.insert(s) {
                        continue;
                    }
                    if let Some(dependency) = &self.sends[s].dependency {
                        derivation.insert(dependency.clone());
                    }
                    stack.push(Work::Record(Record::Send(s)));
                }
                Work::Record(record) => {
                    if !expanded.insert(record) {
                        continue;
                    }
                    let (prov, node, bound) = match record {
                        Record::Send(s) => {
                            let send = &self.sends[s];
                            (&send.prov, &send.from, send.bound)
                        }
                        Record::Output(event) => {
                            let output = &self.outputs[&event];
                            (&output.prov, &output.node, output.bound)
                        }
                    };
                    match prov {
                        RecordProv::Ids(ids) => {
                            sites.extend(ids.iter().filter_map(|&id| fragile_site(id)));
                            self.push_ids(ids, &mut stack, Work::Delivery);
                        }
                        RecordProv::Opaque => {
                            sites.insert(OPAQUE_SITE);
                            self.opaque.borrow_mut().insert(record);
                            self.push_recvs(node, bound, &mut scanned, &mut stack, Work::Delivery);
                        }
                    }
                }
            }
        }

        let closure = Closure {
            derivation: Rc::new(derivation),
            sites: Rc::new(sites),
        };
        self.memo.borrow_mut().insert(root, closure.clone());
        closure
    }

    /// Pushes the delivered sends `ids` names (unknown or undelivered ids name nothing a fault
    /// could kill, and are skipped).
    fn push_ids<W>(
        &self,
        ids: &BTreeSet<DeliveryId>,
        stack: &mut Vec<W>,
        delivery: fn(usize) -> W,
    ) {
        for id in ids {
            if let Some(&s) = self.by_id.get(id)
                && self.sends[s].fate == Fate::Delivered
            {
                stack.push(delivery(s));
            }
        }
    }

    /// The execution rule: pushes every delivery `node` received before event index `bound`.
    fn push_recvs<'s, W>(
        &'s self,
        node: &'s NodeId,
        bound: usize,
        scanned: &mut HashMap<&'s NodeId, usize>,
        stack: &mut Vec<W>,
        delivery: fn(usize) -> W,
    ) {
        let from = scanned.get(node).copied().unwrap_or(0);
        if bound <= from {
            return;
        }
        scanned.insert(node, bound);
        for &(at, s) in self.recvs_at.get(node).into_iter().flatten() {
            if at >= bound {
                break;
            }
            if at >= from {
                stack.push(delivery(s));
            }
        }
    }
}

// Runtime-only, like the search's tests: they need no staged code.
#[cfg(all(test, stageleft_runtime))]
mod tests;

// In the staged copy too: the tests' `q!` closures are resolved there.
#[cfg(test)]
mod sim_tests;

// In the staged copy too, like `sim_tests`.
#[cfg(test)]
mod fragile_tests;
