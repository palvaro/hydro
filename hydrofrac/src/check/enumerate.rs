//! Bounded certificates by enumeration for schedule-sensitive labels (docs/design/hydrofrac-lite.md
//! §4.9, "d2").
//!
//! A schedule-sensitive label's search is a heuristic: running out of hypotheses proves nothing
//! ([`Verdict::Exhausted`]). When the fault-set space is small enough, and every schedule of
//! every set is explored, it can instead be enumerated outright: every admissible fault set is
//! run, and if none breaks the label, that is a certificate of its own, a bounded one
//! ([`Verdict::CertifiedByEnumeration`]), which needs no lineage at all.
//!
//! **What is enumerated.** The universe of candidate faults is the fault model's, learned from
//! the runs themselves ([`harvest`]):
//! - `omit(m)` for every message `m` sent before the heal on a lossy channel;
//! - `crash(n)` for every crashable node, and `crash(n after m)` for every message `m` it sent
//!   before the heal.
//!
//! The sets are every admissible set (the search's bounds: at most one crash per node, never
//! every node, never all or all but one of the checked cluster, the `max_crashes` cap) of at most
//! `s` faults, for the largest `s` that keeps them within [`ENUMERATION_LIMIT`]; when no
//! admissible set is larger than that, every admissible set. Every run is read exactly as the
//! search reads it.
//!
//! **Why the learned universe suffices.** The enumeration repeats until no set's run reveals a
//! new candidate. Take any admissible set `H = H' ∪ {g}` of at most `s` faults with `g` outside
//! the final universe. `H'` is admissible, smaller, and so was run over every schedule. Under the
//! same choices, the runs of `H` and `H'` are identical until `g` first fires, and `g` fires only
//! when its message is sent before the heal, which would then have happened in `H'`'s run too
//! and put `g` in the universe. So `g` never fires under `H`, which therefore runs exactly as
//! `H'`; by induction on `|H|`, every admissible set of at most `s` faults behaves like one the
//! enumeration ran. This needs every schedule of every set: under
//! [`EcCheck::sample_schedules`](super::EcCheck::sample_schedules) nothing is enumerated.

use std::collections::BTreeSet;

use super::{Engine, Run, Target};
use crate::check::results::{Stats, Verdict};
use crate::fault::{Event, Fate, InstanceRecord, MessageId, NodeId};
use crate::lineage::Tables;
use crate::search::{CrashBounds, Fault, FaultSet, Tolerance, crash_count};

/// The most fault sets a bounded certificate enumerates for one label (counting the sets the
/// search already ran). A convention, not a knob: beyond it the label stays
/// [`Verdict::Exhausted`].
pub(super) const ENUMERATION_LIMIT: usize = 2000;

/// Adds the candidate faults `record` reveals to `universe` (see the [module docs](self)).
pub(super) fn harvest(
    record: &InstanceRecord,
    tables: &Tables,
    universe: &mut BTreeSet<Fault<MessageId>>,
) {
    let names = tables.names();
    let crashable: BTreeSet<&NodeId> = names.crashable().iter().collect();
    for node in &crashable {
        universe.insert(Fault::crash_after(names.name(node), None));
    }
    for event in record.log.events() {
        match event {
            // Neither omissions nor new crashes fire after the heal.
            Event::Heal => break,
            Event::Sent { msg, fate, .. } if *fate != Fate::SenderDead => {
                if crashable.contains(&msg.from) {
                    universe.insert(Fault::crash_after(names.name(&msg.from), Some(msg.clone())));
                }
                if tables.is_faulty(msg.channel) {
                    universe.insert(Fault::Omit(msg.clone()));
                }
            }
            _ => {}
        }
    }
}

/// Whether `set` is admissible under `bounds`: at most one crash per node, and within the
/// crash cap, the vacuity bound and the checked cluster's cap.
pub(super) fn admissible(set: &FaultSet<MessageId>, bounds: &CrashBounds) -> bool {
    let crashed: Vec<&String> = set.iter().filter_map(Fault::crashed_node).collect();
    let distinct: BTreeSet<&String> = crashed.iter().copied().collect();
    if distinct.len() != crashed.len() {
        return false;
    }
    if bounds.max_crashes().is_some_and(|max| crashed.len() > max) {
        return false;
    }
    match &bounds.checked {
        Some(checked) => {
            crashed
                .iter()
                .filter(|n| checked.nodes.contains(**n))
                .count()
                <= checked.cap
        }
        None => true,
    }
}

/// Every nonempty admissible set of at most `max_size` faults of `universe`, or `None` if there
/// are more than `limit`. Admissibility is closed under subsets, so the search prunes every
/// extension of an inadmissible set.
pub(super) fn admissible_sets(
    universe: &[Fault<MessageId>],
    bounds: &CrashBounds,
    max_size: usize,
    limit: usize,
) -> Option<Vec<FaultSet<MessageId>>> {
    fn go(
        universe: &[Fault<MessageId>],
        bounds: &CrashBounds,
        max_size: usize,
        limit: usize,
        start: usize,
        current: &mut FaultSet<MessageId>,
        out: &mut Vec<FaultSet<MessageId>>,
    ) -> bool {
        if current.len() >= max_size {
            return true;
        }
        for (i, fault) in universe.iter().enumerate().skip(start) {
            current.insert(fault.clone());
            if admissible(current, bounds) {
                out.push(current.clone());
                if out.len() > limit || !go(universe, bounds, max_size, limit, i + 1, current, out)
                {
                    current.remove(fault);
                    return false;
                }
            }
            current.remove(fault);
        }
        true
    }
    let mut out = vec![];
    go(
        universe,
        bounds,
        max_size,
        limit,
        0,
        &mut FaultSet::new(),
        &mut out,
    )
    .then_some(out)
}

/// The largest set size whose admissible sets over `universe` fit within `limit`, and whether
/// that covers every admissible set (`None` if even single faults do not fit).
pub(super) fn fit(
    universe: &[Fault<MessageId>],
    bounds: &CrashBounds,
    limit: usize,
) -> Option<(usize, bool)> {
    let mut fitted = None;
    let mut previous = 0;
    for size in 1..=universe.len().max(1) {
        let Some(sets) = admissible_sets(universe, bounds, size, limit) else {
            return fitted;
        };
        if size > 1 && sets.len() == previous {
            // No admissible set has `size` faults, so none has more.
            return Some((size - 1, true));
        }
        previous = sets.len();
        fitted = Some((size, false));
    }
    fitted.map(|(size, _)| (size, true))
}

/// What a bounded enumeration concluded.
pub(super) struct Enumerated {
    pub verdict: Verdict,
    /// Sets the enumeration ran that no earlier search had (each a hypothesis).
    pub new_sets: usize,
}

impl<B: AsyncFnMut()> Engine<'_, B> {
    /// Enumerates every admissible fault set of `target` (see the [module docs](self)), up to
    /// the crash bound the search swept (`swept`), if every schedule is explored and the sets
    /// fit within [`ENUMERATION_LIMIT`]. `stats` gains the runs it causes. `hypotheses` is how
    /// many the search tried (a counterexample's index continues from there).
    pub(super) fn enumerate(
        &mut self,
        target: &Target,
        targets: &[Target],
        swept: Tolerance,
        stats: &mut Stats,
        hypotheses: usize,
    ) -> Option<Enumerated> {
        if self.sample_schedules.is_some() {
            return None;
        }
        let bounds = match swept {
            Tolerance::All => self.bounds(target),
            Tolerance::UpTo(k) => self.bounds(target).capped_at(k),
        };
        let mut universe: BTreeSet<Fault<MessageId>> = self
            .runs
            .values()
            .flat_map(|run| run.faults.iter().cloned())
            .collect();
        let mut fitted: Option<(usize, bool)> = None;
        let mut new_sets = 0;
        // The sets run here, in order (a counterexample's hypothesis index).
        let mut order: Vec<FaultSet<MessageId>> = vec![];
        loop {
            let candidates: Vec<Fault<MessageId>> = universe.iter().cloned().collect();
            let now = fit(&candidates, &bounds, ENUMERATION_LIMIT)?;
            // A pass that covered every admissible set may be followed by a larger one (the
            // universe grew); once one cannot, the sets only grow, so neither can any later pass,
            // and the size that fits can only shrink.
            let (size, complete) = match fitted {
                Some((s, false)) => (s.min(now.0), false),
                _ => now,
            };
            fitted = Some((size, complete));
            let sets = admissible_sets(&candidates, &bounds, size, ENUMERATION_LIMIT)?;
            let mut fresh: Vec<FaultSet<MessageId>> = sets
                .into_iter()
                .filter(|s| !self.runs.contains_key(s))
                .collect();
            if fresh.is_empty() {
                break;
            }
            fresh.sort_by_key(|s| (crash_count(s), s.len()));
            for set in fresh {
                let run = self.run(&set, targets);
                new_sets += 1;
                stats.schedules += run.schedules;
                stats.opaque_fallbacks += run.opaque_fallbacks;
                universe.extend(run.faults.iter().cloned());
                order.push(set);
            }
        }
        let (size, complete) = fitted.expect("fitted at least once");

        // Every set in the coverage, and the breaking ones, fewest crashes first.
        let covered: Vec<(&FaultSet<MessageId>, &Run)> = self
            .runs
            .iter()
            .filter(|(set, _)| set.len() <= size && admissible(set, &bounds))
            .map(|(set, run)| (set, run.as_ref()))
            .collect();
        let omissions = universe
            .iter()
            .filter(|f| matches!(f, Fault::Omit(_)))
            .count();
        // The crash bound the sets cover in full: every admissible set with at most `k` crashes
        // has at most `k + omissions` faults.
        let covers_crashes = |k: usize| complete || k + omissions <= size;
        let breaking = covered
            .iter()
            .filter(|(_, run)| {
                run.labels
                    .get(&target.label)
                    .is_some_and(|lr| lr.failure.is_some())
            })
            .min_by_key(|(set, _)| (crash_count(set), set.len()));
        let verdict = if let Some((set, run)) = breaking {
            let crashes = crash_count(set);
            let tolerates = crashes.checked_sub(1);
            let failure = run.labels[&target.label].failure.as_ref().unwrap();
            // Its index among the hypotheses; a run another label's search made counts as
            // the first after this label's own.
            let index = hypotheses + order.iter().position(|s| s == *set).unwrap_or(0);
            super::counterexample(
                (*set).clone(),
                tolerates,
                tolerates.is_none_or(covers_crashes),
                index,
                failure,
            )
        } else {
            let tolerates = if complete {
                swept
            } else {
                // The largest crash count covered in full, within what the search swept.
                let k = size.saturating_sub(omissions);
                match swept {
                    Tolerance::UpTo(cap) => Tolerance::UpTo(cap.min(k)),
                    Tolerance::All => Tolerance::UpTo(k),
                }
            };
            Verdict::CertifiedByEnumeration {
                tolerates,
                max_faults: (!complete).then_some(size),
                sets: covered.len(),
                sites: target.sites.clone(),
            }
        };
        Some(Enumerated { verdict, new_sets })
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use hydro_lang::location::LocationKey;
    use hydro_lang::location::dynamic::LocationId;

    use super::*;
    use crate::search::CheckedCluster;

    fn node(member: u32) -> NodeId {
        NodeId {
            location: LocationId::Cluster(LocationKey::FIRST),
            member: Some(member),
        }
    }

    fn message(from: u32, to: u32, byte: u8) -> MessageId {
        MessageId {
            channel: 0,
            from: node(from),
            to: node(to),
            bytes: Rc::from(vec![byte]),
        }
    }

    fn bounds(nodes: usize, checked: &[&str]) -> CrashBounds {
        CrashBounds {
            cap: None,
            nodes,
            checked: Some(CheckedCluster::new(checked.iter().map(|s| s.to_string()))),
        }
    }

    /// Three nodes, two of them in a checked cluster of three (so at most one of its members
    /// crashes), each with one send: per node, no crash or one of two crash points.
    fn universe() -> Vec<Fault<MessageId>> {
        let mut u = vec![];
        for (n, name) in ["a", "b", "c"].iter().enumerate() {
            u.push(Fault::crash_after(*name, None));
            u.push(Fault::crash_after(
                *name,
                Some(message(n as u32, 0, n as u8)),
            ));
        }
        u.push(Fault::Omit(message(0, 1, 9)));
        u
    }

    #[test]
    fn admissible_sets_are_every_admissible_subset() {
        let b = bounds(4, &["a", "b", "x"]);
        let all = admissible_sets(&universe(), &b, usize::MAX, 10_000).unwrap();
        // Per node: none, crash(n), crash(n after m); a and b not both crashed; the omission
        // or not. (1 + 2 + 2 + 0)·3 (c) · 2 (omission) − 1 (the empty set) = 29.
        assert_eq!(all.len(), 29, "{all:?}");
        assert!(all.iter().all(|s| admissible(s, &b)));
        let distinct: BTreeSet<&FaultSet<MessageId>> = all.iter().collect();
        assert_eq!(distinct.len(), all.len(), "each set once");
        // Bounded by size, and by the limit.
        let singles = admissible_sets(&universe(), &b, 1, 10_000).unwrap();
        assert_eq!(singles.len(), 7);
        assert!(admissible_sets(&universe(), &b, usize::MAX, 28).is_none());
    }

    #[test]
    fn fit_finds_the_largest_size_within_the_limit() {
        let b = bounds(4, &["a", "b", "x"]);
        // Every admissible set fits: complete.
        assert_eq!(fit(&universe(), &b, 29), Some((3, true)));
        // 7 singles, 14 pairs (crashes of a or b with c: 8; a crash with the omission: 6), 8
        // triples: 7, 21, then all 29.
        assert_eq!(fit(&universe(), &b, 28), Some((2, false)));
        assert_eq!(fit(&universe(), &b, 20), Some((1, false)));
        assert_eq!(fit(&universe(), &b, 6), None);
    }
}
