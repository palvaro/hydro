//! A read-through cache with entry expiry in front of a slow origin, with and without request
//! coalescing.
//!
//! The cache keeps a table of keys and the logical tick from which each entry's age is counted. A
//! lookup for a key whose entry is present and younger than [`CacheConfig::ttl_ticks`] completes
//! at once. A lookup for a missing or expired key waits, and the cache asks the origin for the
//! key. The origin keeps fetches in a FIFO and answers at most [`OriginConfig::max_fetch_per_tick`]
//! of them per clock tick. When a fill arrives at the cache, every lookup waiting on that key
//! completes, and the entry is written if the fill is still fresh.
//!
//! # Mechanism and knobs
//!
//! Without coalescing ([`CacheConfig::coalesce`] false), every lookup that misses produces its own
//! fetch. While the origin is slow, an expired hot key stays missing for as long as the origin's
//! queueing delay, and every lookup on it during that time adds a fetch to the origin's queue. All
//! but the first of those fetches are redundant. The redundant fetches lengthen the queue, which
//! lengthens the window during which the next expired key is missing, which produces more
//! redundant fetches. With coalescing on, the cache keeps a set of keys with a fetch outstanding
//! and sends one fetch per missing key, so a hot key costs one fetch per expiry however slow the
//! origin is.
//!
//! Whether the origin then *collapses* depends on a second knob, [`CacheConfig::request_dated`],
//! which says from when an entry's age is counted. With `request_dated = false` the age counts
//! from the tick the fill arrived, so every fill, redundant or not, gives the key another
//! `ttl_ticks` of life. With `request_dated = true` the age counts from the tick the fetch was
//! issued, which is the conservative age calculation of RFC 7234 (a cache cannot know how long
//! the origin sat on the request, so it assumes the response is as old as the request). A fill
//! whose age already exceeds the TTL when it arrives is *stale*: it answers the waiting lookups
//! but is not stored. Once the origin's queueing delay exceeds the TTL, every fill is stale, the
//! hot keys are never present again, and every lookup is a fetch. The dating knob changes how far
//! the herd can go, not whether there is one: without coalescing, a held fill turns the lookups
//! that arrive meanwhile into fetches under either dating rule, so both non-coalescing
//! configurations are hazardous.
//!
//! # Timer parameters
//!
//! - `cache_clock`: one element per logical cache tick. Lookups and fetches are stamped with the
//!   index of the latest element seen, and expiry is judged against it.
//! - `origin_clock`: one element per unit of origin time. The origin's budget in a tick is
//!   `max_fetch_per_tick` per clock element observed in that tick, so a tick woken only by
//!   arriving fetches queues them and serves nothing.
//!
//! A deployment wires `location.source_interval(period)` into each of them; the simulation feeds
//! both from `sim_input` and thereby owns time.
//!
//! # Measured (see `sim_tests`)
//!
//! Ten hot keys, 8 lookups per round walking them round-robin, `ttl_ticks = 20`, origin capacity
//! 4 fetches per round (baseline fetch load about 0.5 per round). The trigger adds 10 lookups per
//! round on never-repeated cold keys during rounds 100 to 160. Tail is rounds 600 to 800; baseline
//! completions over the tail would be 1600 and origin capacity 800.
//!
//! | run | request_dated | coalesce | trigger | tail completions | tail mean latency | tail fetches offered | tail fills applied / redundant / stale | origin queue at 600 -> 800 | label |
//! |---|---|---|---|---|---|---|---|---|---|
//! | herd, request dated | true | false | yes | 1600 | 0.8 | 1600 | 0 / 0 / 800 | 2462 -> 3258 | hazardous (collapses) |
//! | herd, fill dated | false | false | yes | 1600 | 0.0 | 100 | 100 / 0 / 0 | 0 -> 0 (peak 680, 568 redundant fills over the run) | no ground truth; recovers, and the tool is expected to find the herd (see hold experiment) |
//! | no trigger | true | false | no | 1600 | 0.0 | 546 over rounds 10 to 800 | 156 wasted over rounds 10 to 800 | 0 -> 0 | healthy |
//! | coalescing | true | true | yes | 1600 | 0.0 | 100 | 100 / 0 / 0 | 0 -> 0 (peak 390) | benign, by assurance argument: one outstanding fetch per key bounds fetches by distinct misses |
//!
//! The fill-dated herd recovers from the trigger, so its label rests on the hold experiment
//! (`fill_dated_herd_does_more_work_the_longer_fills_are_held`): the same 1920 baseline lookups
//! over 240 rounds, with the origin's clock withheld for `k` rounds from round 100 and then
//! delivered at once, cost the origin 124, 160, 200, 272 and 498 fetches for `k` of 0, 5, 10, 20
//! and 50. Every lookup completes in every case and the origin drains by the end; only the work
//! differs, and it grows with the hold. The coalescing configuration's label rests on
//! `coalescing_bound_holds_across_schedules`, which shows fetches bounded by distinct misses under
//! 256 fuzzed schedules.
//!
//! In the collapsing run the lookups themselves are served: a lookup waiting on key `k` is answered
//! by the stale fill of an older fetch for `k`, and one of those arrives every couple of rounds.
//! What does not recover is the origin, which is offered 8 fetches per round against a capacity of
//! 4 under the same 8 lookups per round it served with 0.5 fetches before the trigger, and whose
//! queue grows by 4 per round with nothing useful in it. The fill-dated herd was expected to
//! collapse and does not: the redundant fills for a key arrive spread over the queueing delay and
//! each restarts the key's life, so no new fetches are issued while the queue drains.

use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

pub struct Cache;
pub struct Origin;

pub type Key = u32;

/// A request from the cache to the origin for one key. `lookup_id` is the lookup that caused it
/// (the first waiting lookup, when coalescing) and `issued_at` the cache tick in which it was
/// sent. Ordered by `lookup_id` so that sorting a batch of fetches recovers lookup order.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Fetch {
    pub lookup_id: u64,
    pub key: Key,
    pub issued_at: u64,
}

/// The origin's answer to a [`Fetch`], echoing its fields.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Fill {
    pub lookup_id: u64,
    pub key: Key,
    pub issued_at: u64,
}

/// A lookup that has completed. Latency is measured in cache clock ticks from the lookup's
/// arrival to the tick in which a fill for its key arrived; hits have latency 0.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Completion {
    pub id: u64,
    pub key: Key,
    pub latency_ticks: u64,
}

/// A lookup waiting for a fill.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Waiting {
    pub id: u64,
    pub issued_at: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct CacheConfig {
    /// An entry dated `t` serves lookups in ticks `t .. t + ttl_ticks`.
    pub ttl_ticks: u64,
    /// When true, at most one fetch is outstanding per key.
    pub coalesce: bool,
    /// When true, an entry is dated from the tick its fetch was issued (RFC 7234 conservative
    /// age); when false, from the tick its fill arrived.
    pub request_dated: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct OriginConfig {
    /// Fetches the origin answers per `origin_clock` element: its capacity.
    pub max_fetch_per_tick: u32,
}

/// Everything observable about a run.
pub struct CacheOutputs<'a> {
    /// Every lookup, once, when its key was served.
    pub completed: Stream<Completion, Process<'a, Cache>, Unbounded, NoOrder, ExactlyOnce>,
    /// Every fetch the cache puts on the wire.
    pub fetches: Stream<Fetch, Process<'a, Cache>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Fills that wrote an entry the cache did not have.
    pub fills_applied: Stream<Fill, Process<'a, Cache>, Unbounded, NoOrder, ExactlyOnce>,
    /// Fills for keys the cache already had: origin work that bought nothing.
    pub fills_redundant: Stream<Fill, Process<'a, Cache>, Unbounded, NoOrder, ExactlyOnce>,
    /// Fills for missing keys that were already older than the TTL on arrival: they answered the
    /// waiting lookups but were not stored.
    pub fills_stale: Stream<Fill, Process<'a, Cache>, Unbounded, NoOrder, ExactlyOnce>,
    /// Number of lookups waiting for a fill at the end of each cache tick.
    pub waiting_trace: Stream<usize, Process<'a, Cache>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Every fetch the origin serves.
    pub processed: Stream<Fetch, Process<'a, Origin>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Depth of the origin's queue at the end of each origin tick.
    pub origin_backlog_trace: Stream<usize, Process<'a, Origin>, Unbounded, TotalOrder, ExactlyOnce>,
}

/// Builds the cache/origin program. `lookups` is the application's lookup stream at the cache;
/// ids are assigned in arrival order.
pub fn cache_with_expiry<'a>(
    cache: &Process<'a, Cache>,
    origin: &Process<'a, Origin>,
    lookups: Stream<Key, Process<'a, Cache>, Unbounded, TotalOrder, ExactlyOnce>,
    cache_clock: Stream<(), Process<'a, Cache>, Unbounded>,
    origin_clock: Stream<(), Process<'a, Origin>, Unbounded>,
    cache_config: CacheConfig,
    origin_config: OriginConfig,
) -> CacheOutputs<'a> {
    let CacheConfig {
        ttl_ticks,
        coalesce,
        request_dated,
    } = cache_config;
    let OriginConfig { max_fetch_per_tick } = origin_config;

    // Fills come back from the origin, which is downstream of `fetches` (below).
    let (fills_complete, fills) = cache
        .forward_ref::<Stream<Fill, Process<'a, Cache>, Unbounded, TotalOrder, ExactlyOnce>>();

    // ---- Cache: logical clock, entries with expiry, waiting lookups, fetches -----------------
    let (completed, fetches, fills_applied, fills_redundant, fills_stale, waiting_trace) = sliced! {
        let clock = use::batch(cache_clock.enumerate(), nondet!(/** batching only shifts which cache tick observes a clock element; every element still advances the clock */));
        let lookups = use::batch(lookups.enumerate(), nondet!(/** batching decides which lookups share a tick, and so which of them see an entry that a fill in the same tick wrote */));
        let fills = use::batch(fills, nondet!(/** holding a fill keeps its key missing for longer, so more lookups miss on it, and ages the fill */));
        // key -> tick from which the entry's age is counted.
        let mut entries = use::state_null::<KeyedSingleton<Key, u64, Tick<_>, Bounded>>();
        // Keys with a fetch outstanding. Only consulted when `coalesce` is set.
        let mut pending = use::state_null::<KeyedSingleton<Key, (), Tick<_>, Bounded>>();
        // Lookups that missed and are waiting for their key's fill.
        let mut waiting = use::state_null::<KeyedStream<Key, Waiting, Tick<_>, Bounded, TotalOrder>>();
        let mut now = use::state(|l| l.singleton(q!(0u64)));
        let now_cur = clock.map(q!(|(i, _)| i as u64)).max().unwrap_or(now.clone());
        now = now_cur.clone();

        // Expiry: an entry dated `t` is live while `now - t < ttl_ticks`.
        let live = entries
            .into_keyed_stream()
            .cross_singleton(now_cur.clone())
            .filter(q!(move |(dated, now)| now - dated < ttl_ticks))
            .map(q!(|(dated, _)| dated))
            .first();

        // Fills. A fill for a live key is redundant work by the origin. A fill for a key that is
        // not live is dated, and is stored if still fresh, or stale if not.
        let fills = fills.map(q!(|f| (f.key, f))).into_keyed();
        let redundant = fills
            .clone()
            .join_keyed_singleton(live.clone())
            .entries()
            .map(q!(|(_, (f, _))| f));
        let arrived = fills.filter_key_not_in(live.clone().keys());
        let dated = if request_dated {
            arrived
                .clone()
                .cross_singleton(now_cur.clone())
                .map(q!(|(f, now)| (f, f.issued_at, now)))
        } else {
            arrived
                .clone()
                .cross_singleton(now_cur.clone())
                .map(q!(|(f, now)| (f, now, now)))
        };
        let applied = dated
            .clone()
            .filter(q!(move |(_, dated, now)| now - dated < ttl_ticks));
        let stale = dated
            .filter(q!(move |(_, dated, now)| now - dated >= ttl_ticks))
            .entries()
            .map(q!(|(_, (f, _, _))| f));
        let arrived_keys = arrived.clone().keys();
        let inserted = applied.clone().map(q!(|(_, dated, _)| dated));
        // Keys of `live` and `inserted` are disjoint, so `first()` is exact.
        entries = live.into_keyed_stream().chain(inserted).first();

        // Lookups, judged against the entries as of this tick (including this tick's fills).
        let lookups = lookups.map(q!(|(i, key)| (key, i as u64))).into_keyed();
        let hits = lookups
            .clone()
            .join_keyed_singleton(entries.clone())
            .entries()
            .map(q!(|(key, (id, _))| Completion { id, key, latency_ticks: 0 }));
        let misses = lookups.filter_key_not_in(entries.clone().keys());

        // Fetches: one per miss, or one per missing key that has none outstanding.
        let fetches = if coalesce {
            misses
                .clone()
                .filter_key_not_in(pending.clone().keys())
                .first()
                .entries()
                .cross_singleton(now_cur.clone())
                .map(q!(|((key, lookup_id), now)| Fetch { lookup_id, key, issued_at: now }))
                .sort()
        } else {
            misses
                .clone()
                .entries()
                .cross_singleton(now_cur.clone())
                .map(q!(|((key, lookup_id), now)| Fetch { lookup_id, key, issued_at: now }))
                .sort()
        };
        pending = pending
            .filter_key_not_in(arrived_keys.clone())
            .into_keyed_stream()
            .chain(fetches.clone().map(q!(|f| (f.key, ()))).into_keyed())
            .first();

        // Waiting lookups: those whose key's fill arrived this tick complete now; misses join.
        let done = waiting
            .clone()
            .join_keyed_singleton(arrived.first())
            .entries()
            .cross_singleton(now_cur.clone())
            .map(q!(|((key, (w, _)), now)| Completion {
                id: w.id,
                key,
                latency_ticks: now - w.issued_at,
            }));
        let new_waiting = misses
            .cross_singleton(now_cur.clone())
            .map(q!(|(id, now)| Waiting { id, issued_at: now }));
        waiting = waiting.filter_key_not_in(arrived_keys).chain(new_waiting);
        let waiting_depth = waiting.clone().entries().count();

        (
            hits.chain(done),
            fetches,
            applied.entries().map(q!(|(_, (f, _, _))| f)),
            redundant,
            stale,
            waiting_depth.into_stream(),
        )
    };

    // ---- Origin: FIFO of fetches, budget per clock element ------------------------------
    let incoming = fetches.clone().send(origin, TCP.fail_stop().bincode());

    let (processed, origin_backlog_trace) = sliced! {
        let pump = use::batch(origin_clock, nondet!(/** batching two clock elements into one tick gives that tick two units of budget; the total budget over a run is unchanged */));
        let arrivals = use::batch(incoming, nondet!(/** arrivals are appended to the FIFO; batching only affects how many share a tick */));
        let mut backlog = use::state_null::<Stream<Fetch, Tick<_>, Bounded, TotalOrder>>();

        let budget = pump.count().map(q!(move |n| n * max_fetch_per_tick as usize));
        let queued = backlog.chain(arrivals).enumerate().cross_singleton(budget);
        let served = queued
            .clone()
            .filter_map(q!(|((i, req), budget)| if i < budget { Some(req) } else { None }));
        backlog = queued.filter_map(q!(|((i, req), budget)| if i >= budget { Some(req) } else { None }));

        (served, backlog.clone().count().into_stream())
    };

    fills_complete.complete(
        processed
            .clone()
            .map(q!(|f| Fill {
                lookup_id: f.lookup_id,
                key: f.key,
                issued_at: f.issued_at,
            }))
            .send(cache, TCP.fail_stop().bincode()),
    );

    CacheOutputs {
        completed,
        fetches,
        fills_applied,
        fills_redundant,
        fills_stale,
        waiting_trace,
        processed,
        origin_backlog_trace,
    }
}

/// An open-loop workload for the tests. Hot lookups walk a small key set round-robin; inside the
/// trigger window each round also carries lookups on cold keys that are never looked up again,
/// each of which is a miss and so a fetch, which is how the harness loads the origin without
/// touching the program. This lives in the harness, not in the program.
#[derive(Clone, Copy, Debug)]
pub struct Workload {
    pub hot_keys: u32,
    pub hot_per_round: u32,
    /// Cold lookups per round inside the trigger window, each on a fresh key.
    pub cold_per_round: u32,
    /// Trigger window in rounds: `[trigger_start, trigger_end)`.
    pub trigger_start: u64,
    pub trigger_end: u64,
}

impl Workload {
    pub fn cold_at(&self, round: u64) -> u32 {
        if round >= self.trigger_start && round < self.trigger_end {
            self.cold_per_round
        } else {
            0
        }
    }
}

/// Simulation under the fixed prompt schedule. A round is one element on each clock plus that
/// round's lookups, followed by quiescence.
#[cfg(test)]
mod sim_tests {
    use hydro_lang::sim::quiesce;

    use super::*;

    #[derive(Debug, Clone, Default)]
    struct Round {
        completed: u64,
        latency_sum: u64,
        /// Fetches on the wire this round.
        fetched: u64,
        /// Fills that wrote a missing entry.
        applied: u64,
        /// Fills for entries that were already present.
        redundant: u64,
        /// Fills for missing entries that were too old to store.
        stale: u64,
        /// Origin queue after this round (carried over if the origin did not tick).
        backlog: usize,
        /// Lookups waiting for a fill after this round.
        waiting: usize,
    }

    /// A hold on the origin's clock: no `origin_clock` element is delivered during rounds
    /// `[start, start + rounds)`, and the withheld elements are all delivered in round
    /// `start + rounds`. The origin's total budget over the run is unchanged, but every fill it
    /// would have produced in the window arrives up to `rounds` rounds late. This is how the
    /// harness delays the fill edge without touching the program: the origin's tick, woken only
    /// by arriving fetches, queues them and serves nothing until its clock returns.
    #[derive(Clone, Copy, Debug)]
    struct Hold {
        start: u64,
        rounds: u64,
    }

    fn run(workload: Workload, cache_config: CacheConfig, origin_config: OriginConfig, rounds: usize) -> Vec<Round> {
        run_with_hold(workload, cache_config, origin_config, rounds, None)
    }

    fn run_with_hold(
        workload: Workload,
        cache_config: CacheConfig,
        origin_config: OriginConfig,
        rounds: usize,
        hold: Option<Hold>,
    ) -> Vec<Round> {
        let mut flow = FlowBuilder::new();
        let cache = flow.process::<Cache>();
        let origin = flow.process::<Origin>();

        let (lookup_send, lookups) = cache.sim_input::<Key, TotalOrder, ExactlyOnce>();
        let (cache_clock_send, cache_clock) = cache.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (origin_clock_send, origin_clock) = origin.sim_input::<(), TotalOrder, ExactlyOnce>();

        let outputs = cache_with_expiry(
            &cache,
            &origin,
            lookups,
            cache_clock,
            origin_clock,
            cache_config,
            origin_config,
        );
        let completed = outputs.completed.sim_output();
        let fetches = outputs.fetches.sim_output();
        let applied = outputs.fills_applied.sim_output();
        let redundant = outputs.fills_redundant.sim_output();
        let stale = outputs.fills_stale.sim_output();
        let waiting_trace = outputs.waiting_trace.sim_output();
        let backlog_trace = outputs.origin_backlog_trace.sim_output();

        let mut trace: Vec<Round> = Vec::with_capacity(rounds);
        let trace_ref = &mut trace;

        flow.sim().run_prompt(async move || {
            let mut backlog = 0usize;
            let mut waiting = 0usize;
            let mut next_cold_key: Key = 1_000_000;
            for round in 0..rounds as u64 {
                cache_clock_send.send(());
                match hold {
                    Some(h) if round >= h.start && round < h.start + h.rounds => {}
                    Some(h) if round == h.start + h.rounds => {
                        for _ in 0..=h.rounds {
                            origin_clock_send.send(());
                        }
                    }
                    _ => origin_clock_send.send(()),
                }
                for i in 0..workload.hot_per_round {
                    lookup_send.send(((round * workload.hot_per_round as u64 + i as u64) % workload.hot_keys as u64) as Key);
                }
                for _ in 0..workload.cold_at(round) {
                    lookup_send.send(next_cold_key);
                    next_cold_key += 1;
                }
                quiesce().await;

                let mut r = Round::default();
                for c in completed.collect_sorted::<Vec<_>>().await {
                    r.completed += 1;
                    r.latency_sum += c.latency_ticks;
                }
                r.fetched = fetches.collect::<Vec<_>>().await.len() as u64;
                r.applied = applied.collect_sorted::<Vec<_>>().await.len() as u64;
                r.redundant = redundant.collect_sorted::<Vec<_>>().await.len() as u64;
                r.stale = stale.collect_sorted::<Vec<_>>().await.len() as u64;
                while let Some(depth) = backlog_trace.try_next().await {
                    backlog = depth;
                }
                while let Some(depth) = waiting_trace.try_next().await {
                    waiting = depth;
                }
                r.backlog = backlog;
                r.waiting = waiting;
                trace_ref.push(r);
            }
        });
        trace
    }

    fn sum(trace: &[Round], from: usize, to: usize, f: impl Fn(&Round) -> u64) -> u64 {
        trace[from..to].iter().map(f).sum()
    }

    fn mean_latency(trace: &[Round], from: usize, to: usize) -> f64 {
        let completed = sum(trace, from, to, |r| r.completed);
        assert!(completed > 0, "no completions in rounds [{from}, {to})");
        sum(trace, from, to, |r| r.latency_sum) as f64 / completed as f64
    }

    /// Hand-computed expectation, written before measuring.
    ///
    /// Ten hot keys, 8 lookups per round, so each key is looked up in 8 of every 10 rounds and a
    /// key that is missing for `d` rounds collects about `0.8 d` lookups. With `ttl = 20` each
    /// key expires once per 20 rounds, so baseline fetch load is `10 / 20 = 0.5` per round
    /// (about 0.8 fetches per expiry without coalescing when the origin is idle) against a
    /// capacity of 4: 12% utilization, latency 0 or 1 round.
    ///
    /// Without coalescing, a key that is missing for `d` rounds produces `0.8 d` fetches, of
    /// which one is useful, over a cycle of `d + 20` rounds; total fetch load is
    /// `F(d) = 10 * 0.8 d / (d + 20) = 8 d / (d + 20)`, which exceeds the capacity of 4 once
    /// `d > 20` rounds, i.e. once the origin's queue is longer than 80. Past that point the queue
    /// grows, `d` grows, and `F` approaches the full lookup rate of 8 per round.
    ///
    /// The trigger adds 10 cold lookups per round for 60 rounds, 600 fetches against a spare
    /// capacity of about 3.5 per round, building a queue of about 390 (`d` about 100, far past
    /// 20). Every hot key expires during those 100 rounds and every hot lookup becomes a fetch.
    ///
    /// Whether the loop then closes depends on dating. With request dating, `d > 20` means every
    /// fill is stale on arrival, so no hot key is ever stored again and the origin is offered 8
    /// per round against 4 forever: the queue grows by about 4 per round. With fill dating (the
    /// first version of this program, kept as a knob), the `0.8 d` fills for a key arrive spread
    /// over `d` rounds after the first, each restarting the key's 20-round life, so the key never
    /// expires while its redundant fills keep arriving, no new fetches are issued, and the queue
    /// drains at 4 per round; the measured run below confirms that it recovers.
    ///
    /// With coalescing, a missing key produces exactly one fetch per cycle, so `F <= 10 / 20 =
    /// 0.5` per round however long `d` is, and the queue drains at about 3.5 per round in
    /// roughly 110 rounds, i.e. by round 270 or so.
    pub(super) const WORKLOAD: Workload = Workload {
        hot_keys: 10,
        hot_per_round: 8,
        cold_per_round: 10,
        trigger_start: 100,
        trigger_end: 160,
    };
    pub(super) const CACHE: CacheConfig = CacheConfig {
        ttl_ticks: 20,
        coalesce: false,
        request_dated: true,
    };
    pub(super) const ORIGIN: OriginConfig = OriginConfig { max_fetch_per_tick: 4 };

    pub(super) const ROUNDS: usize = 800;
    const TAIL_START: usize = 600;

    fn print_trajectory(trace: &[Round]) {
        for i in [0, 5, 50, 99, 130, 159, 200, 250, 300, 400, 500, 600, 700, 799] {
            if i < trace.len() {
                let r = &trace[i];
                println!(
                    "round {i}: completed={} latency_sum={} fetched={} applied={} redundant={} stale={} backlog={} waiting={}",
                    r.completed, r.latency_sum, r.fetched, r.applied, r.redundant, r.stale, r.backlog, r.waiting
                );
            }
        }
    }

    fn print_tail(trace: &[Round]) {
        let tail = &trace[TAIL_START..];
        let tail_rounds = (ROUNDS - TAIL_START) as u64;
        println!(
            "tail (rounds {TAIL_START}..{ROUNDS}): completed {} (baseline {}), mean latency {:.1}, fetched {} (capacity {}), fills applied={} redundant={} stale={}, backlog {} -> {}, waiting {} -> {}",
            sum(trace, TAIL_START, ROUNDS, |r| r.completed),
            8 * tail_rounds,
            mean_latency(trace, TAIL_START, ROUNDS),
            sum(trace, TAIL_START, ROUNDS, |r| r.fetched),
            ORIGIN.max_fetch_per_tick as u64 * tail_rounds,
            sum(trace, TAIL_START, ROUNDS, |r| r.applied),
            sum(trace, TAIL_START, ROUNDS, |r| r.redundant),
            sum(trace, TAIL_START, ROUNDS, |r| r.stale),
            tail.first().unwrap().backlog,
            tail.last().unwrap().backlog,
            tail.first().unwrap().waiting,
            tail.last().unwrap().waiting,
        );
    }

    /// Healthy means: the origin's queue is empty at the end of almost every round and never
    /// deeper than one round's worth of synchronized expiries (`hot_per_round - capacity`), every
    /// lookup completes within a round, and fetch load is the expiry rate. With request dating the
    /// keys filled from fetches issued in the same round expire in lockstep, so once per TTL the
    /// origin sees 8 fetches against a capacity of 4 and clears the excess within a round or two.
    fn assert_healthy(trace: &[Round], from: usize, to: usize) {
        let window = &trace[from..to];
        let rounds = (to - from) as u64;
        let slack = (WORKLOAD.hot_per_round - ORIGIN.max_fetch_per_tick) as usize;
        assert!(
            window.iter().all(|r| r.backlog <= slack && r.waiting <= slack),
            "origin queue and waiting set should stay within one round's synchronized expiries in rounds [{from}, {to})"
        );
        let empty = window.iter().filter(|r| r.backlog == 0).count() as u64;
        assert!(empty * 10 >= rounds * 8, "origin queue should be empty in at least 80% of rounds [{from}, {to}), got {empty} of {rounds}");
        let completed = sum(trace, from, to, |r| r.completed);
        assert!(
            completed.abs_diff(8 * rounds) <= 8,
            "every lookup should complete within a round in [{from}, {to}): {completed} vs {}",
            8 * rounds
        );
        let fetched = sum(trace, from, to, |r| r.fetched);
        assert!(fetched <= rounds, "fetch load should be about 0.5 per round in [{from}, {to}), got {fetched} in {rounds} rounds");
        assert!(mean_latency(trace, from, to) <= 1.0);
    }

    fn assert_healthy_pre_trigger(trace: &[Round]) {
        assert_healthy(trace, 10, 100);
    }

    fn assert_healthy_tail(trace: &[Round]) {
        assert_healthy(trace, TAIL_START, ROUNDS);
    }

    #[test]
    fn herd_with_request_dating_keeps_the_origin_saturated() {
        let trace = run(WORKLOAD, CACHE, ORIGIN, ROUNDS);
        print_trajectory(&trace);
        print_tail(&trace);
        assert_healthy_pre_trigger(&trace);

        let tail = &trace[TAIL_START..];
        let tail_rounds = (ROUNDS - TAIL_START) as u64;
        let tail_fetched = sum(&trace, TAIL_START, ROUNDS, |r| r.fetched);
        let tail_applied = sum(&trace, TAIL_START, ROUNDS, |r| r.applied);
        let tail_wasted = sum(&trace, TAIL_START, ROUNDS, |r| r.redundant + r.stale);
        // Offered fetch load in the tail exceeds the origin's capacity although the lookup
        // workload is back at baseline.
        assert!(
            tail_fetched > ORIGIN.max_fetch_per_tick as u64 * tail_rounds,
            "the origin should be offered more than it can serve: {tail_fetched} fetches in {tail_rounds} rounds"
        );
        // Almost nothing the origin serves gets stored.
        assert!(
            tail_wasted > 10 * tail_applied,
            "stale and redundant fills should dominate (applied={tail_applied}, wasted={tail_wasted})"
        );
        // The queue is still growing at the end of the run, by about capacity per round.
        assert!(
            tail.last().unwrap().backlog > tail.first().unwrap().backlog + 3 * tail_rounds as usize,
            "the origin's queue should grow by about 4 per round in the tail: {} -> {}",
            tail.first().unwrap().backlog,
            tail.last().unwrap().backlog
        );
        // Latency does not suffer: a waiting lookup on key `k` is answered by the stale fill of an
        // older fetch for `k`, and one of those arrives every couple of rounds. The lookups are
        // served; the origin is buried.
        assert!(mean_latency(&trace, TAIL_START, ROUNDS) <= 2.0);
    }

    /// The first version of this program dated entries from the fill's arrival. It was expected
    /// to collapse and does not: the herd's redundant fills keep the hot keys alive while the
    /// queue drains. It is still hazardous, since a held fill still turns lookups into fetches;
    /// the hold experiment below shows that. Kept as a measured configuration without ground truth.
    #[test]
    fn herd_with_fill_dating_refreshes_itself_and_recovers() {
        let trace = run(WORKLOAD, CacheConfig { request_dated: false, ..CACHE }, ORIGIN, ROUNDS);
        print_trajectory(&trace);
        print_tail(&trace);
        assert_healthy_pre_trigger(&trace);
        assert!(trace.iter().all(|r| r.stale == 0), "fill dating never produces a stale fill");
        let peak = trace.iter().map(|r| r.backlog).max().unwrap();
        let redundant: u64 = trace.iter().map(|r| r.redundant).sum();
        println!("peak origin queue {peak}; redundant fills over the run {redundant}");
        assert!(peak > 200, "the trigger should have built a queue far past the ttl, got {peak}");
        assert!(redundant > 0, "the herd should have produced redundant fills");
        assert_healthy_tail(&trace);
        let total_fetched: u64 = trace.iter().map(|r| r.fetched).sum();
        let total_lookups = 8 * ROUNDS as u64 + 10 * 60;
        println!("total fetched {total_fetched} for {total_lookups} lookups");
        assert!(total_fetched <= total_lookups);
    }

    /// Hold experiment for the fill-dated herd, which recovers from the load trigger and so
    /// cannot be labeled by collapse. The label is hazardous if the same lookup input produces
    /// more origin work when fills are delayed, and more of it the longer they are delayed.
    ///
    /// Hand-computed expectation, written before measuring. Baseline lookups only (no cold keys),
    /// 240 rounds, the origin's clock withheld for `k` rounds from round 100 and then delivered
    /// all at once. With no coalescing, a key that is missing for `d` rounds collects about
    /// `0.8 d` lookups and so `0.8 d` fetches, one of which is useful; when the origin's clock
    /// returns it serves them all in one tick, the first fill for each key is applied and the rest
    /// arrive at a live key and are redundant. Keys expire at about one per two rounds, so a hold
    /// of `k <= 20` rounds catches about `k / 2` keys, missing for `k / 2` rounds on average,
    /// which is about `0.8 * (k / 2) * (k / 2) = 0.2 k^2` fetches: about 5 at `k = 5`, 20 at
    /// `k = 10`, 80 at `k = 20`. Past `k = 20` every key is missing and the herd grows at the full
    /// lookup rate of 8 per round, so about `80 + 8 (k - 20)`, which is about 320 at `k = 50`.
    /// Redundant fills are reported but not asserted on: when the withheld clock elements are
    /// delivered at once, the herd's fills for one key land in one cache tick, and the program
    /// counts all of them as applied because none was live at the start of that tick. Fetches put
    /// on the wire and served by the origin are the work measure. At `k = 0` the run is the
    /// no-trigger control. So fetches should increase with `k`, and be larger at `k = 50` than at
    /// `k = 0` by roughly 300.
    #[test]
    fn fill_dated_herd_does_more_work_the_longer_fills_are_held() {
        const HOLD_ROUNDS: usize = 240;
        const HOLDS: [u64; 5] = [0, 5, 10, 20, 50];
        let workload = Workload { cold_per_round: 0, ..WORKLOAD };
        let config = CacheConfig { request_dated: false, ..CACHE };

        let mut fetched_by_hold = Vec::new();
        let mut fills_by_hold = Vec::new();
        for k in HOLDS {
            let hold = (k > 0).then_some(Hold { start: 100, rounds: k });
            let trace = run_with_hold(workload, config, ORIGIN, HOLD_ROUNDS, hold);
            let lookups = 8 * HOLD_ROUNDS as u64;
            let fetched = sum(&trace, 0, HOLD_ROUNDS, |r| r.fetched);
            let redundant = sum(&trace, 0, HOLD_ROUNDS, |r| r.redundant);
            let applied = sum(&trace, 0, HOLD_ROUNDS, |r| r.applied);
            let completed = sum(&trace, 0, HOLD_ROUNDS, |r| r.completed);
            let peak_waiting = trace.iter().map(|r| r.waiting).max().unwrap();
            println!(
                "hold {k}: lookups {lookups}, completed {completed}, fetched {fetched}, fills applied {applied} redundant {redundant}, peak waiting {peak_waiting}, queue after run {}",
                trace.last().unwrap().backlog
            );
            // The same input is served in full whatever the hold; only the work differs.
            assert!(completed.abs_diff(lookups) <= 8, "completed {completed} vs lookups {lookups}");
            assert_eq!(trace.last().unwrap().backlog, 0, "the origin should have drained by the end");
            fetched_by_hold.push(fetched);
            fills_by_hold.push(applied + redundant);
        }
        println!("fetched by hold {HOLDS:?}: {fetched_by_hold:?}");
        println!("fills (applied + redundant) by hold {HOLDS:?}: {fills_by_hold:?}");

        assert!(
            fetched_by_hold.windows(2).all(|w| w[0] <= w[1]),
            "fetches should not decrease as the hold grows: {fetched_by_hold:?}"
        );
        let excess = fetched_by_hold.last().unwrap() - fetched_by_hold[0];
        assert!(excess > 100, "a 50-round hold should add well over 100 fetches for the same input, added {excess}");
    }

    /// Control: request dating, no coalescing, no trigger. The herd factor at an idle origin is
    /// at most the lookups that share a round, and the queue never builds.
    #[test]
    fn without_a_trigger_the_origin_keeps_up() {
        let trace = run(Workload { cold_per_round: 0, ..WORKLOAD }, CACHE, ORIGIN, ROUNDS);
        print_trajectory(&trace);
        assert_healthy(&trace, 10, ROUNDS);
        let fetched = sum(&trace, 10, ROUNDS, |r| r.fetched);
        let wasted = sum(&trace, 10, ROUNDS, |r| r.redundant + r.stale);
        println!("fetched {fetched}, redundant or stale {wasted}, mean latency {:.2}", mean_latency(&trace, 10, ROUNDS));
        // The synchronized expiry queues fetches for a round, and the lookups arriving in that
        // round herd on the pending keys; this is the mechanism at a scale the origin absorbs.
        assert!(wasted <= (ROUNDS as u64 - 10) / 4, "wasted fills should be a small fraction of rounds, got {wasted}");
    }

    /// Control: request dating, same trigger, coalescing on. One fetch per missing key, so the
    /// queue the trigger built drains and the tail is healthy.
    #[test]
    fn with_coalescing_the_origin_recovers() {
        let trace = run(WORKLOAD, CacheConfig { coalesce: true, ..CACHE }, ORIGIN, ROUNDS);
        print_trajectory(&trace);
        print_tail(&trace);
        assert_healthy_pre_trigger(&trace);
        assert!(trace.iter().all(|r| r.redundant == 0), "coalescing never produces a redundant fill");
        let peak = trace.iter().map(|r| r.backlog).max().unwrap();
        println!("peak origin queue {peak}; tail queue {}", trace.last().unwrap().backlog);
        assert!(peak > 200, "the trigger should have built a queue far past the ttl, got {peak}");
        assert_healthy_tail(&trace);
        let total_fetched: u64 = trace.iter().map(|r| r.fetched).sum();
        let total_arrived: u64 = trace.iter().map(|r| r.applied + r.stale).sum();
        println!("total fetched {total_fetched}, applied or stale {total_arrived}");
        assert!(total_fetched <= total_arrived + WORKLOAD.hot_keys as u64);
    }

    /// The coalescing bound holds under schedule exploration, not only under the prompt
    /// schedule: however fills and lookups are held, no fill is ever redundant and the number of
    /// fetches never exceeds the number of fills that arrived plus the number of keys that can
    /// still be pending.
    #[test]
    fn coalescing_bound_holds_across_schedules() {
        const ROUNDS: usize = 12;
        let workload = Workload {
            hot_keys: 3,
            hot_per_round: 4,
            cold_per_round: 3,
            trigger_start: 2,
            trigger_end: 5,
        };
        let config = CacheConfig {
            ttl_ticks: 3,
            coalesce: true,
            request_dated: true,
        };

        let mut flow = FlowBuilder::new();
        let cache = flow.process::<Cache>();
        let origin = flow.process::<Origin>();
        let (lookup_send, lookups) = cache.sim_input::<Key, TotalOrder, ExactlyOnce>();
        let (cache_clock_send, cache_clock) = cache.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (origin_clock_send, origin_clock) = origin.sim_input::<(), TotalOrder, ExactlyOnce>();
        let outputs = cache_with_expiry(
            &cache,
            &origin,
            lookups,
            cache_clock,
            origin_clock,
            config,
            OriginConfig { max_fetch_per_tick: 1 },
        );
        let fetches = outputs.fetches.sim_output();
        let applied = outputs.fills_applied.sim_output();
        let redundant = outputs.fills_redundant.sim_output();
        let stale = outputs.fills_stale.sim_output();

        flow.sim().unit_test_fuzz_iterations(256).fuzz(async || {
            let mut total_fetched = 0usize;
            let mut total_arrived = 0usize;
            let mut next_cold_key: Key = 1_000_000;
            let pending_bound = workload.hot_keys as usize
                + (workload.cold_per_round as u64 * (workload.trigger_end - workload.trigger_start)) as usize;
            for round in 0..ROUNDS as u64 {
                cache_clock_send.send(());
                origin_clock_send.send(());
                for i in 0..workload.hot_per_round {
                    lookup_send.send(((round * workload.hot_per_round as u64 + i as u64) % workload.hot_keys as u64) as Key);
                }
                for _ in 0..workload.cold_at(round) {
                    lookup_send.send(next_cold_key);
                    next_cold_key += 1;
                }
                quiesce().await;
                total_fetched += fetches.collect::<Vec<_>>().await.len();
                total_arrived += applied.collect_sorted::<Vec<_>>().await.len();
                total_arrived += stale.collect_sorted::<Vec<_>>().await.len();
                assert!(redundant.collect_sorted::<Vec<_>>().await.is_empty(), "a redundant fill under coalescing");
                assert!(
                    total_fetched <= total_arrived + pending_bound,
                    "fetched {total_fetched} > arrived {total_arrived} + pending bound {pending_bound}"
                );
            }
        });
    }
}
