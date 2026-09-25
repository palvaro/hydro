//! Replicated-log catch-up with timeout retries at a lagging follower.
//!
//! The leader numbers append inputs in arrival order. Every sixth follower clock tick, when no
//! request is outstanding, the follower asks for a bounded chunk beginning at its next missing
//! index. The leader expands each request into one work item per available record, up to
//! `chunk_size`; those items share a FIFO and a fixed per-tick capacity with unrelated work. The
//! first returned record acknowledges the request, while all records advance the follower's
//! applied prefix. If no record arrives within `retry_after_ticks`, the follower sends the same
//! request again on every clock tick. Thus delayed replies duplicate both wire messages and leader
//! record reads. Those duplicate reads occupy the capacity needed to produce a reply.
//!
//! `follower_clock` and `leader_clock` are logical timer parameters. A deployment wires
//! `follower.source_interval(period)` and `leader.source_interval(period)` into them; simulation
//! uses `sim_input`.
//!
//! # Measured configurations
//!
//! | baseline | capacity | trigger | timeout | asserted tail | label and basis |
//! |---|---|---|---|---|---|
//! | 2 appends/round | 10 jobs/tick | 20 unrelated jobs/round, rounds 100..160 | 4 follower ticks | tail completions 1, retry requests 192, backlog 1624 -> 1940 | hazardous, by exhibited collapse |
//! | 2 appends/round | 10 jobs/tick | none | 4 follower ticks | tail completions 34, retries 0, final backlog 0 | healthy control |

use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
use hydro_lang::prelude::*;
use hydro_lang::sim::amplification::{SimOutputs, amplification_check};
use serde::{Deserialize, Serialize};

pub struct CatchupFollower;
pub struct LogLeader;

#[derive(Clone, Copy, Debug)]
pub struct CatchupConfig {
    /// How often an idle follower asks for the next chunk.
    pub poll_every_ticks: u64,
    /// Age at which an unanswered request is sent again on every subsequent tick.
    pub retry_after_ticks: u64,
    /// Maximum records read for one request.
    pub chunk_size: u64,
    /// Leader FIFO jobs served per leader clock element.
    pub leader_capacity: u32,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Fetch {
    pub request_id: u64,
    pub from_index: u64,
    pub retry: bool,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RecordReply {
    pub request_id: u64,
    pub index: u64,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum LeaderJob {
    Read {
        request_id: u64,
        index: u64,
        retry: bool,
    },
    Other(u64),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct PendingFetch {
    pub from_index: u64,
    pub first_sent: u64,
    pub last_sent: u64,
}

#[derive(Clone, Copy, Debug)]
enum PendingVerdict {
    Keep((u64, PendingFetch)),
    Retry((u64, PendingFetch)),
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FetchCompletion {
    pub request_id: u64,
    pub latency_ticks: u64,
}

#[derive(SimOutputs)]
pub struct CatchupOutputs<'a> {
    /// Every fetch request put on the wire, including retries.
    pub requests: Stream<Fetch, Process<'a, CatchupFollower>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Every FIFO job actually served by the leader.
    pub served: Stream<LeaderJob, Process<'a, LogLeader>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Every record reply admitted and processed by the follower, including duplicates.
    pub received:
        Stream<RecordReply, Process<'a, CatchupFollower>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Records that extend the follower's applied prefix; each index appears at most once.
    pub applied: Stream<u64, Process<'a, CatchupFollower>, Unbounded, TotalOrder, ExactlyOnce>,
    /// A request's first matching response, with follower-clock latency.
    pub completions:
        Stream<FetchCompletion, Process<'a, CatchupFollower>, Unbounded, TotalOrder, ExactlyOnce>,
    /// Leader FIFO depth after each leader tick.
    pub backlog: Stream<usize, Process<'a, LogLeader>, Unbounded, TotalOrder, ExactlyOnce>,
}

#[amplification_check(
    workload(appends = 2, unrelated_work = 0),
    config = CatchupConfig { poll_every_ticks: 6, retry_after_ticks: 4, chunk_size: 12, leader_capacity: 10 },
)]
pub fn replicated_log_catchup<'a>(
    follower: &Process<'a, CatchupFollower>,
    leader: &Process<'a, LogLeader>,
    appends: Stream<(), Process<'a, LogLeader>, Unbounded, TotalOrder, ExactlyOnce>,
    unrelated_work: Stream<(), Process<'a, LogLeader>, Unbounded, TotalOrder, ExactlyOnce>,
    follower_clock: Stream<(), Process<'a, CatchupFollower>, Unbounded, TotalOrder, ExactlyOnce>,
    leader_clock: Stream<(), Process<'a, LogLeader>, Unbounded, TotalOrder, ExactlyOnce>,
    config: CatchupConfig,
) -> CatchupOutputs<'a> {
    let CatchupConfig {
        poll_every_ticks,
        retry_after_ticks,
        chunk_size,
        leader_capacity,
    } = config;

    let (replies_complete, replies) = follower.forward_ref::<Stream<
        RecordReply,
        Process<'a, CatchupFollower>,
        Unbounded,
        TotalOrder,
        ExactlyOnce,
    >>();

    let (requests, received, applied, completions) = sliced! {
        let clock = use::batch(follower_clock.enumerate(), nondet!(/** batching changes when a clock element is observed, but every element advances logical time exactly once */));
        let replies = use::batch(replies, nondet!(/** batching changes response latency and can therefore cause retries; it cannot alter response contents */));
        let mut pending = use::state_null::<KeyedSingleton<u64, PendingFetch, Tick<_>, Bounded>>();
        let mut now = use::state(|l| l.singleton(q!(0u64)));
        let mut next_index = use::state(|l| l.singleton(q!(0u64)));

        let ticked = clock.clone().count().map(q!(|n| n > 0));
        let now_cur = clock.map(q!(|(i, _)| i as u64)).max().unwrap_or(now.clone());
        now = now_cur.clone();

        let received = replies.clone().sort();
        let acknowledged = replies.clone().map(q!(|reply| reply.request_id)).unique();
        let completions = acknowledged
            .clone()
            .map(q!(|id| (id, ())))
            .into_keyed()
            .join_keyed_singleton(pending.clone())
            .entries()
            .cross_singleton(now_cur.clone())
            .map(q!(|((request_id, (_, pending)), now)| FetchCompletion {
                request_id,
                latency_ticks: now - pending.first_sent,
            }))
            .sort();

        let old_next = next_index.clone();
        let next_cur = replies
            .map(q!(|reply| reply.index + 1))
            .max()
            .unwrap_or(old_next.clone())
            .zip(old_next.clone())
            .map(q!(|(observed, old)| observed.max(old)));
        next_index = next_cur.clone();
        let applied = old_next
            .zip(next_cur.clone())
            .into_stream()
            .flat_map_ordered(q!(|(old, new)| old..new));

        let waiting = pending.filter_key_not_in(acknowledged).into_keyed_stream().entries();
        let judged = waiting
            .cross_singleton(now_cur.clone())
            .map(q!(move |((id, p), now)| {
                if now - p.first_sent >= retry_after_ticks && now > p.last_sent {
                    PendingVerdict::Retry((id, PendingFetch { last_sent: now, ..p }))
                } else {
                    PendingVerdict::Keep((id, p))
                }
            }));
        let kept = judged.clone().filter_map(q!(|verdict| match verdict {
            PendingVerdict::Keep(value) => Some(value),
            _ => None,
        }));
        let retried = judged.filter_map(q!(|verdict| match verdict {
            PendingVerdict::Retry(value) => Some(value),
            _ => None,
        }));

        let is_idle = kept.clone().chain(retried.clone()).count().map(q!(|n| n == 0));
        let due = now_cur
            .clone()
            .filter(q!(move |now| *now % poll_every_ticks == 0))
            .filter_if(ticked)
            .filter_if(is_idle);
        let started = due
            .zip(next_cur)
            .map(q!(|(now, from_index)| (now, PendingFetch {
                from_index,
                first_sent: now,
                last_sent: now,
            })))
            .into_stream();

        pending = kept.chain(retried.clone()).chain(started.clone()).sort().into_keyed().first();
        let requests = started
            .map(q!(|(request_id, p)| Fetch {
                request_id,
                from_index: p.from_index,
                retry: false,
            }))
            .chain(retried.map(q!(|(request_id, p)| Fetch {
                request_id,
                from_index: p.from_index,
                retry: true,
            })))
            .sort();

        (requests, received, applied, completions)
    };

    let incoming_requests = requests.clone().send(leader, TCP.fail_stop().bincode());
    let numbered_other = unrelated_work.enumerate().map(q!(|(id, ())| id as u64));

    let (served, backlog) = sliced! {
        let clock = use::batch(leader_clock, nondet!(/** batching only groups capacity grants; every clock element grants exactly leader_capacity jobs */));
        let appends = use::batch(appends, nondet!(/** batching determines which appended prefix a request can observe, without creating log records */));
        let requests = use::batch(incoming_requests, nondet!(/** batching determines when a request reaches the leader and hence the available log prefix */));
        let other = use::batch(numbered_other, nondet!(/** batching determines when unrelated jobs enter the shared FIFO */));
        let mut highwater = use::state(|l| l.singleton(q!(0u64)));
        let mut queued = use::state_null::<Stream<LeaderJob, Tick<_>, Bounded, TotalOrder>>();

        let appended = appends.count().map(q!(|n| n as u64));
        let highwater_cur = highwater.zip(appended).map(q!(|(old, count)| old + count));
        highwater = highwater_cur.clone();

        let reads = requests
            .cross_singleton(highwater_cur)
            .flat_map_ordered(q!(move |(fetch, highwater)| {
                let end = (fetch.from_index + chunk_size).min(highwater);
                (fetch.from_index..end).map(move |index| LeaderJob::Read {
                    request_id: fetch.request_id,
                    index,
                    retry: fetch.retry,
                })
            }));
        let arrivals = other.map(q!(|id| LeaderJob::Other(id))).chain(reads);
        let budget = clock.count().map(q!(move |n| n * leader_capacity as usize));
        let all = queued.chain(arrivals).enumerate().cross_singleton(budget);
        let served = all.clone().filter_map(q!(|((i, job), budget)| (i < budget).then_some(job)));
        queued = all.filter_map(q!(|((i, job), budget)| (i >= budget).then_some(job)));

        (served, queued.clone().count().into_stream())
    };

    let reply_records: Stream<
        RecordReply,
        Process<'a, LogLeader>,
        Unbounded,
        TotalOrder,
        ExactlyOnce,
    > = served.clone().filter_map(q!(|job| match job {
        LeaderJob::Read {
            request_id, index, ..
        } => Some(RecordReply { request_id, index }),
        LeaderJob::Other(_) => None,
    }));
    replies_complete.complete(reply_records.send(follower, TCP.fail_stop().bincode()));

    CatchupOutputs {
        requests,
        served,
        received,
        applied,
        completions,
        backlog,
    }
}

#[cfg(test)]
mod sim_tests {
    use hydro_lang::sim::quiesce;

    use super::*;

    #[derive(Clone, Debug, Default)]
    struct Round {
        first_requests: u64,
        retry_requests: u64,
        served_reads: u64,
        served_retry_reads: u64,
        served_other: u64,
        received: u64,
        applied: u64,
        completions: u64,
        latency_sum: u64,
        backlog: usize,
    }

    #[derive(Clone, Copy)]
    struct Workload {
        trigger: bool,
    }

    fn run(workload: Workload, rounds: usize) -> Vec<Round> {
        let mut flow = FlowBuilder::new();
        let follower = flow.process::<CatchupFollower>();
        let leader = flow.process::<LogLeader>();
        let (append_send, appends) = leader.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (other_send, other) = leader.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (follower_clock_send, follower_clock) =
            follower.sim_input::<(), TotalOrder, ExactlyOnce>();
        let (leader_clock_send, leader_clock) = leader.sim_input::<(), TotalOrder, ExactlyOnce>();
        let outputs = replicated_log_catchup(
            &follower,
            &leader,
            appends,
            other,
            follower_clock,
            leader_clock,
            CatchupConfig {
                poll_every_ticks: 6,
                retry_after_ticks: 4,
                chunk_size: 12,
                leader_capacity: 10,
            },
        );
        let requests = outputs.requests.sim_output();
        let served = outputs.served.sim_output();
        let received = outputs.received.sim_output();
        let applied = outputs.applied.sim_output();
        let completions = outputs.completions.sim_output();
        let backlog = outputs.backlog.sim_output();

        let mut trace = Vec::with_capacity(rounds);
        let trace_ref = &mut trace;
        flow.sim().run_prompt(async move || {
            let mut last_backlog = 0;
            for round in 0..rounds {
                // Hand expectation written before measurement: baseline adds 12 records per six
                // rounds and asks for one 12-record chunk, so average read load is 2/tick against
                // capacity 10. The 60-round trigger adds 1,200 jobs and should build about 720
                // queued jobs after spare capacity. That is far beyond the four-tick timeout. Once
                // retries begin they offer 12 reads/tick against capacity 10, predicting backlog
                // growth of roughly 2/tick after the trigger and near-zero useful catch-up.
                append_send.send_many([(), ()]);
                let other_count = if workload.trigger && (100..160).contains(&round) {
                    20
                } else {
                    0
                };
                for _ in 0..other_count {
                    other_send.send(());
                }
                follower_clock_send.send(());
                leader_clock_send.send(());
                quiesce().await;

                let mut item = Round::default();
                while let Some(request) = requests.try_next().await {
                    if request.retry {
                        item.retry_requests += 1;
                    } else {
                        item.first_requests += 1;
                    }
                }
                while let Some(job) = served.try_next().await {
                    match job {
                        LeaderJob::Read { retry, .. } => {
                            item.served_reads += 1;
                            item.served_retry_reads += retry as u64;
                        }
                        LeaderJob::Other(_) => item.served_other += 1,
                    }
                }
                item.received = received.collect::<Vec<_>>().await.len() as u64;
                item.applied = applied.collect::<Vec<_>>().await.len() as u64;
                for completion in completions.collect::<Vec<_>>().await {
                    item.completions += 1;
                    item.latency_sum += completion.latency_ticks;
                }
                while let Some(depth) = backlog.try_next().await {
                    last_backlog = depth;
                }
                item.backlog = last_backlog;
                trace_ref.push(item);
            }
        });
        trace
    }

    fn sum(trace: &[Round], range: std::ops::Range<usize>, f: impl Fn(&Round) -> u64) -> u64 {
        trace[range].iter().map(f).sum()
    }

    #[test]
    fn delayed_catchup_retries_collapse_the_leader() {
        let trace = run(Workload { trigger: true }, 800);
        let baseline_completions = sum(&trace, 20..100, |r| r.completions);
        let tail_completions = sum(&trace, 600..800, |r| r.completions);
        let tail_retries = sum(&trace, 600..800, |r| r.retry_requests);
        eprintln!(
            "baseline completions {baseline_completions}; tail completions {tail_completions}; tail retries {tail_retries}; backlog {} -> {}",
            trace[599].backlog, trace[799].backlog
        );
        assert!(
            baseline_completions >= 10,
            "the pre-trigger follower must be healthy"
        );
        assert!(
            tail_retries >= 150,
            "an unanswered fetch must keep retrying in the tail"
        );
        assert!(
            tail_completions <= 2,
            "useful fetches must remain near zero long after the trigger"
        );
        assert!(
            trace[799].backlog > trace[599].backlog + 200,
            "the tail backlog must keep growing"
        );
    }

    #[test]
    fn without_the_trigger_catchup_stays_healthy() {
        let trace = run(Workload { trigger: false }, 800);
        let tail_retries = sum(&trace, 600..800, |r| r.retry_requests);
        let tail_completions = sum(&trace, 600..800, |r| r.completions);
        eprintln!(
            "control tail completions {tail_completions}; retries {tail_retries}; final backlog {}",
            trace[799].backlog
        );
        assert_eq!(tail_retries, 0);
        assert!(tail_completions >= 30);
        assert!(trace.iter().all(|r| r.backlog <= 12));
    }
}
