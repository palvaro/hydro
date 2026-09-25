use std::collections::BTreeMap;

use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
use hydro_lang::prelude::*;
use serde::{Deserialize, Serialize};

pub struct Client;
pub struct Server;

#[derive(Clone, Copy, Debug)]
pub enum Policy {
    /// Retry every timeout until `max_attempts`, including the first send.
    None { max_attempts: u32 },
    /// Spend one token per retry. Timer elements add `refill_per_tick` tokens.
    TimeBudget {
        initial_tokens: u64,
        capacity: u64,
        refill_per_tick: u64,
    },
    /// Spend one token per retry. Each group of `replies_per_token` useful replies adds one token.
    SuccessBudget {
        initial_tokens: u64,
        capacity: u64,
        replies_per_token: u64,
    },
    /// Combine timer and useful-reply token grants.
    HybridBudget {
        initial_tokens: u64,
        capacity: u64,
        refill_per_tick: u64,
        replies_per_token: u64,
    },
    /// Drop new requests while open or probing. Existing requests supply the single probe.
    CircuitBreaker {
        max_attempts: u32,
        timeout_threshold: u32,
        open_ticks: u64,
    },
}

#[derive(Clone, Copy, Debug)]
pub struct RetryConfig {
    pub timeout_ticks: u64,
    pub server_capacity: u32,
    pub policy: Policy,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Request {
    pub id: u64,
    pub payload: u64,
    pub first_sent: u64,
    pub attempt: u32,
}

#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Reply {
    pub id: u64,
    pub first_sent: u64,
    pub attempt: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Completion {
    pub id: u64,
    pub latency_ticks: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Dropped {
    pub id: u64,
    pub retry: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct Pending {
    payload: u64,
    first_sent: u64,
    last_timeout: u64,
    attempts: u32,
}

#[derive(Clone, Copy, Debug)]
pub enum BreakerState {
    Closed { consecutive_timeouts: u32 },
    Open { until: u64 },
    Probe { id: u64, deadline: u64 },
}

#[derive(Clone, Debug)]
pub struct ClientState {
    pub pending: BTreeMap<u64, Pending>,
    tokens: u64,
    reply_remainder: u64,
    breaker: BreakerState,
}

impl ClientState {
    pub fn new(policy: Policy) -> Self {
        let tokens = match policy {
            Policy::TimeBudget { initial_tokens, .. }
            | Policy::SuccessBudget { initial_tokens, .. }
            | Policy::HybridBudget { initial_tokens, .. } => initial_tokens,
            Policy::None { .. } | Policy::CircuitBreaker { .. } => 0,
        };
        Self {
            pending: BTreeMap::new(),
            tokens,
            reply_remainder: 0,
            breaker: BreakerState::Closed {
                consecutive_timeouts: 0,
            },
        }
    }
}

/// Advances all client-side operator state once. An unavailable budget marks that timeout handled,
/// so a denied retry is dropped rather than waiting to consume a later token immediately.
pub fn client_step(
    mut state: ClientState,
    arrivals: Vec<(u64, u64)>,
    replies: Vec<Reply>,
    now: u64,
    clock_ticks: u64,
    config: RetryConfig,
) -> (ClientState, Vec<Request>, Vec<Completion>, Vec<Dropped>) {
    let mut sent = Vec::new();
    let mut completed = Vec::new();
    let mut dropped = Vec::new();
    let mut useful_replies = 0u64;

    for reply in replies {
        if let Some(pending) = state.pending.remove(&reply.id) {
            useful_replies += 1;
            completed.push(Completion {
                id: reply.id,
                latency_ticks: now.saturating_sub(pending.first_sent),
            });
        }
        if matches!(state.breaker, BreakerState::Probe { id, .. } if id == reply.id) {
            state.breaker = BreakerState::Closed {
                consecutive_timeouts: 0,
            };
        }
    }

    if useful_replies > 0 {
        if matches!(state.breaker, BreakerState::Closed { .. }) {
            state.breaker = BreakerState::Closed {
                consecutive_timeouts: 0,
            };
        }
    }

    let (capacity, time_grant, replies_per_token) = match config.policy {
        Policy::TimeBudget {
            capacity,
            refill_per_tick,
            ..
        } => (capacity, refill_per_tick * clock_ticks, 0),
        Policy::SuccessBudget {
            capacity,
            replies_per_token,
            ..
        } => (capacity, 0, replies_per_token),
        Policy::HybridBudget {
            capacity,
            refill_per_tick,
            replies_per_token,
            ..
        } => (capacity, refill_per_tick * clock_ticks, replies_per_token),
        Policy::None { .. } | Policy::CircuitBreaker { .. } => (0, 0, 0),
    };
    if capacity > 0 {
        let reply_grant = if replies_per_token > 0 {
            let numerator = state.reply_remainder + useful_replies;
            state.reply_remainder = numerator % replies_per_token;
            numerator / replies_per_token
        } else {
            0
        };
        state.tokens = (state.tokens + time_grant + reply_grant).min(capacity);
    }

    let accepts_first = matches!(state.breaker, BreakerState::Closed { .. });
    for (id, payload) in arrivals {
        if accepts_first {
            state.pending.insert(
                id,
                Pending {
                    payload,
                    first_sent: now,
                    last_timeout: now,
                    attempts: 1,
                },
            );
            sent.push(Request {
                id,
                payload,
                first_sent: now,
                attempt: 1,
            });
        } else {
            dropped.push(Dropped { id, retry: false });
        }
    }

    if let Policy::CircuitBreaker {
        max_attempts,
        timeout_threshold,
        open_ticks,
    } = config.policy
    {
        match state.breaker {
            BreakerState::Open { until } if now >= until => {
                if let Some((&id, pending)) = state
                    .pending
                    .iter_mut()
                    .find(|(_, pending)| pending.attempts < max_attempts)
                {
                    pending.attempts += 1;
                    pending.last_timeout = now;
                    sent.push(Request {
                        id,
                        payload: pending.payload,
                        first_sent: pending.first_sent,
                        attempt: pending.attempts,
                    });
                    state.breaker = BreakerState::Probe {
                        id,
                        deadline: now + config.timeout_ticks,
                    };
                } else {
                    state.breaker = BreakerState::Closed {
                        consecutive_timeouts: 0,
                    };
                }
            }
            BreakerState::Probe { deadline, .. } if now >= deadline => {
                state.breaker = BreakerState::Open {
                    until: now + open_ticks,
                };
            }
            BreakerState::Closed {
                mut consecutive_timeouts,
            } => {
                let due = state
                    .pending
                    .iter()
                    .filter_map(|(&id, pending)| {
                        (now.saturating_sub(pending.last_timeout) >= config.timeout_ticks)
                            .then_some(id)
                    })
                    .collect::<Vec<_>>();
                for id in due {
                    let pending = state.pending.get_mut(&id).expect("pending request exists");
                    pending.last_timeout = now;
                    consecutive_timeouts += 1;
                    if consecutive_timeouts >= timeout_threshold {
                        dropped.push(Dropped { id, retry: true });
                        state.breaker = BreakerState::Open {
                            until: now + open_ticks,
                        };
                        break;
                    }
                    if pending.attempts < max_attempts {
                        pending.attempts += 1;
                        sent.push(Request {
                            id,
                            payload: pending.payload,
                            first_sent: pending.first_sent,
                            attempt: pending.attempts,
                        });
                    }
                }
                if matches!(state.breaker, BreakerState::Closed { .. }) {
                    state.breaker = BreakerState::Closed {
                        consecutive_timeouts,
                    };
                }
            }
            _ => {}
        }
    } else {
        let due = state
            .pending
            .iter()
            .filter_map(|(&id, pending)| {
                (now.saturating_sub(pending.last_timeout) >= config.timeout_ticks).then_some(id)
            })
            .collect::<Vec<_>>();
        for id in due {
            let pending = state.pending.get_mut(&id).expect("pending request exists");
            pending.last_timeout = now;
            let permitted = match config.policy {
                Policy::None { max_attempts } => pending.attempts < max_attempts,
                Policy::TimeBudget { .. }
                | Policy::SuccessBudget { .. }
                | Policy::HybridBudget { .. } => {
                    if state.tokens > 0 {
                        state.tokens -= 1;
                        true
                    } else {
                        false
                    }
                }
                Policy::CircuitBreaker { .. } => unreachable!(),
            };
            if permitted {
                pending.attempts += 1;
                sent.push(Request {
                    id,
                    payload: pending.payload,
                    first_sent: pending.first_sent,
                    attempt: pending.attempts,
                });
            } else {
                dropped.push(Dropped { id, retry: true });
            }
        }
    }

    (state, sent, completed, dropped)
}

pub struct RetryOutputs<'a> {
    pub sent: Stream<Request, Process<'a, Client>, Unbounded, TotalOrder, ExactlyOnce>,
    pub served: Stream<Request, Process<'a, Server>, Unbounded, TotalOrder, ExactlyOnce>,
    pub replies: Stream<Reply, Process<'a, Server>, Unbounded, TotalOrder, ExactlyOnce>,
    pub completed: Stream<Completion, Process<'a, Client>, Unbounded, TotalOrder, ExactlyOnce>,
    pub dropped: Stream<Dropped, Process<'a, Client>, Unbounded, TotalOrder, ExactlyOnce>,
    pub pending: Stream<usize, Process<'a, Client>, Unbounded, TotalOrder, ExactlyOnce>,
    pub backlog: Stream<usize, Process<'a, Server>, Unbounded, TotalOrder, ExactlyOnce>,
}

/// Builds one bounded request/reply server and a client whose retry policy is selected at build
/// time by `config`. Deployments wire `source_interval` streams into both clock parameters.
pub fn retry_service<'a>(
    client: &Process<'a, Client>,
    server: &Process<'a, Server>,
    requests: Stream<u64, Process<'a, Client>, Unbounded, TotalOrder, ExactlyOnce>,
    client_clock: Stream<(), Process<'a, Client>, Unbounded, TotalOrder, ExactlyOnce>,
    server_clock: Stream<(), Process<'a, Server>, Unbounded, TotalOrder, ExactlyOnce>,
    config: RetryConfig,
) -> RetryOutputs<'a> {
    assert!(config.timeout_ticks > 0, "the timeout must be positive");
    assert!(config.server_capacity > 0, "server capacity must be positive");
    let (reply_complete, reply_incoming) = client.forward_ref::<Stream<
        Reply,
        Process<'a, Client>,
        Unbounded,
        TotalOrder,
        ExactlyOnce,
    >>();

    let (sent, completed, dropped, pending) = sliced! {
        let clocks = use::batch(client_clock.enumerate(), nondet!(/** delaying client clock admission changes when absence is judged, but does not invent timer input */));
        let arrivals = use::batch(requests.enumerate(), nondet!(/** request admission changes its first-send tick while preserving its arrival-assigned id */));
        let replies = use::batch(reply_incoming, nondet!(/** delayed reply admission can permit a timeout retry before completion */));
        let mut now = use::state(|l| l.singleton(q!(0u64)));
        let mut state = use::state(|l| l.singleton(q!(crate::cluster::witnesses::blind_budget::service::ClientState::new(config.policy))));
        let now_cur = clocks.clone().map(q!(|(tick, _)| tick as u64)).max().unwrap_or(now.clone());
        now = now_cur.clone();
        let tick_count = clocks.count().map(q!(|count| count as u64));
        let arrival_vec = arrivals
            .map(q!(|(id, payload)| (id as u64, payload)))
            .fold(q!(Vec::new), q!(|items, item| items.push(item)));
        let reply_vec = replies.fold(q!(Vec::new), q!(|items, item| items.push(item)));
        let stepped = state
            .zip(arrival_vec)
            .zip(reply_vec)
            .zip(now_cur)
            .zip(tick_count)
            .map(q!(move |((((state, arrivals), replies), now), ticks)| {
                crate::cluster::witnesses::blind_budget::service::client_step(
                    state, arrivals, replies, now, ticks, config,
                )
            }));
        state = stepped.clone().map(q!(|(state, _, _, _)| state));
        let sent = stepped.clone().flat_map_ordered(q!(|(_, sent, _, _)| sent));
        let completed = stepped.clone().flat_map_ordered(q!(|(_, _, completed, _)| completed));
        let dropped = stepped.clone().flat_map_ordered(q!(|(_, _, _, dropped)| dropped));
        let pending = state.clone().map(q!(|state| state.pending.len())).into_stream();
        (sent, completed, dropped, pending)
    };

    let incoming = sent.clone().send(server, TCP.fail_stop().bincode());
    let (served, replies, backlog) = sliced! {
        let clocks = use::batch(server_clock, nondet!(/** delaying server clocks withholds a fixed service quantum */));
        let arrivals = use::batch(incoming, nondet!(/** delaying request admission changes FIFO latency but not a request copy */));
        let mut queue = use::state_null::<Stream<Request, Tick<_>, Bounded, TotalOrder>>();
        let budget = clocks.count().map(q!(move |count| count * config.server_capacity as usize));
        let queued = queue.chain(arrivals).enumerate().cross_singleton(budget);
        let served = queued.clone().filter_map(q!(|((position, request), budget)| (position < budget).then_some(request)));
        queue = queued.filter_map(q!(|((position, request), budget)| (position >= budget).then_some(request)));
        let backlog = queue.clone().count().into_stream();
        let replies = served.clone().map(q!(|request| Reply {
            id: request.id,
            first_sent: request.first_sent,
            attempt: request.attempt,
        }));
        (served, replies, backlog)
    };
    reply_complete.complete(replies.clone().send(client, TCP.fail_stop().bincode()));

    RetryOutputs {
        sent,
        served,
        replies,
        completed,
        dropped,
        pending,
        backlog,
    }
}

/// Returns the payloads offered by a no-trigger steady-state round.
pub fn steady_state_inputs(round: usize, per_round: u32) -> impl Iterator<Item = u64> {
    (0..per_round).map(move |offset| ((round as u64) << 32) | offset as u64)
}
