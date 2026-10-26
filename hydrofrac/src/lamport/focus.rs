//! Focusing a counterexample's diagram on the fact it breaks.

use std::collections::BTreeSet;

use crate::fault::{DeliveryId, Event, EventLog, Fate, MessageId};
use crate::lineage::Lineage;
use crate::search::{CrashAt, Fault, FaultSet};

/// The part of `log` a diagram of the facts `seed` picks out has to show, and a caption saying
/// what it left out (`None` when nothing).
///
/// `seed` picks the sends and outputs of those facts. Kept are the seeds, every delivery in
/// their lineage (`lineage` is `log`'s), every message a fault took (omitted, or lost to a
/// crash) or names, the crashes and the heal. Dropped are the other messages and outputs, and
/// the executions, so a node with nothing left loses its lane. When `seed` picks nothing the
/// focus is unknown, and `log` is kept whole.
pub fn focus(
    log: &EventLog,
    lineage: &Lineage<'_>,
    faults: &FaultSet<MessageId>,
    seed: impl Fn(&Event) -> bool,
) -> (EventLog, Option<String>) {
    let named: BTreeSet<&MessageId> = faults
        .iter()
        .filter_map(|f| match f {
            Fault::Omit(m)
            | Fault::Crash {
                at: CrashAt::After(m),
                ..
            } => Some(m),
            Fault::Crash { .. } => None,
        })
        .collect();
    let events = log.events();
    let (mut keep, mut outputs, mut seeded) =
        (BTreeSet::<DeliveryId>::new(), BTreeSet::new(), false);
    for (i, event) in events.iter().enumerate() {
        let picked = seed(event);
        seeded |= picked;
        match event {
            Event::Sent { id, msg, fate, .. }
                if picked || *fate != Fate::Delivered || named.contains(msg) =>
            {
                keep.insert(*id);
                keep.extend(lineage.support(i));
            }
            Event::Output { .. } if picked => {
                outputs.insert(i);
                keep.extend(lineage.support(i));
            }
            _ => {}
        }
    }
    if !seeded {
        return (log.clone(), None);
    }
    let (mut messages, mut notes) = (0usize, 0usize);
    let mut kept = vec![];
    for (i, event) in events.iter().enumerate() {
        let shown = match event {
            Event::Sent { id, .. } => keep.contains(id),
            Event::Recv { id, .. } => keep.contains(id),
            Event::Output { .. } => outputs.contains(&i),
            Event::Exec { .. } | Event::ExecEnd => continue,
            Event::Halted { .. } | Event::Heal => true,
        };
        match event {
            _ if shown => kept.push(event.clone()),
            Event::Sent { .. } => messages += 1,
            Event::Output { .. } => notes += 1,
            _ => {}
        }
    }
    let count =
        |n: usize, what: &str| format!("{n} unrelated {what}{}", if n == 1 { "" } else { "s" });
    let caption = match (messages, notes) {
        (0, 0) => None,
        (m, 0) => Some(format!("{} not shown", count(m, "message"))),
        (0, o) => Some(format!("{} not shown", count(o, "output"))),
        (m, o) => Some(format!(
            "{} and {} not shown",
            count(m, "message"),
            count(o, "output")
        )),
    };
    (EventLog(kept), caption)
}
