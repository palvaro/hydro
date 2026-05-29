use std::hash::Hash;

use hydro_lang::live_collections::stream::{NoOrder, Ordering};
use hydro_lang::location::{Location, NoTick};
use hydro_lang::prelude::*;

#[expect(clippy::type_complexity, reason = "stream types with ordering")]
pub fn collect_quorum_with_response<
    'a,
    L: Location<'a> + NoTick,
    Order: Ordering,
    K: Clone + Eq + Hash,
    V: Clone,
    E: Clone,
>(
    responses: Stream<(K, Result<V, E>), L, Unbounded, Order>,
    min: usize,
    max: usize,
) -> (
    Stream<(K, V), L, Unbounded, Order>,
    Stream<(K, E), L, Unbounded, Order>,
) {
    let quorums = sliced! {
        let new_inputs = use(responses.clone(), nondet!(
            /// We always persist values that have not reached quorum, so even
            /// with arbitrary batching we always produce deterministic quorum results.
        ));

        let mut not_all = use::state_null::<Stream<_, _, Bounded, Order>>();
        let mut min_but_not_max = use::state_null::<Stream<K, _, Bounded, NoOrder>>();

        let current_responses = not_all.chain(new_inputs);

        let count_per_key = current_responses.clone().into_keyed().fold(
            q!(move || (0, 0)),
            q!(move |accum, value| {
                if value.is_ok() {
                    accum.0 += 1;
                } else {
                    accum.1 += 1;
                }
            }, commutative = manual_proof!(/** increment counters is commutative */)),
        );

         let not_reached_min_count = count_per_key
            .clone()
            .filter(q!(move |(success, _error)| success < &min))
            .keys();

        let reached_min_count = count_per_key
            .clone()
            .filter(q!(move |(success, _error)| success >= &min))
            .keys();

        let just_reached_quorum = if max == min {
            not_all = current_responses.clone().anti_join(reached_min_count);

            current_responses.anti_join(not_reached_min_count)
        } else {
            let received_from_all = count_per_key
                .filter(q!(move |(success, error)| (success + error) >= max))
                .keys();

            not_all = current_responses.clone().anti_join(received_from_all.clone());

            let out = current_responses
                .anti_join(not_reached_min_count)
                .anti_join(min_but_not_max);

            min_but_not_max = reached_min_count.filter_not_in(received_from_all);

            out
        };

        just_reached_quorum.filter_map(q!(move |(key, res)| match res {
            Ok(v) => Some((key, v)),
            Err(_) => None,
        }))
    };

    (
        quorums,
        responses.filter_map(q!(move |(key, res)| match res {
            Ok(_) => None,
            Err(e) => Some((key, e)),
        })),
    )
}

#[expect(clippy::type_complexity, reason = "stream types with ordering")]
pub fn collect_quorum<
    'a,
    L: Location<'a> + NoTick,
    Order: Ordering,
    K: Clone + Eq + Hash,
    E: Clone,
>(
    responses: Stream<(K, Result<(), E>), L, Unbounded, Order>,
    min: usize,
    max: usize,
) -> (
    Stream<K, L, Unbounded, NoOrder>,
    Stream<(K, E), L, Unbounded, Order>,
) {
    let just_reached_quorum = sliced! {
        let new_inputs = use(responses.clone(), nondet!(
            /// We always persist values that have not reached quorum, so even
            /// with arbitrary batching we always produce deterministic quorum results.
        ));

        let mut not_all = use::state_null::<Stream<_, _, Bounded, Order>>();
        let mut min_but_not_max = use::state_null::<Stream<K, _, Bounded, NoOrder>>();

        let current_responses = not_all.chain(new_inputs);

        let count_per_key = current_responses.clone().into_keyed().fold(
            q!(move || (0, 0)),
            q!(move |accum, value| {
                if value.is_ok() {
                    accum.0 += 1;
                } else {
                    accum.1 += 1;
                }
            }, commutative = manual_proof!(/** increment counters is commutative */)),
        );

        let reached_min_count = count_per_key
            .clone()
            .entries()
            .filter_map(q!(move |(key, (success, _error))| if success >= min {
                Some(key)
            } else {
                None
            }));

        let just_reached_quorum = if max == min {
            not_all = current_responses.anti_join(reached_min_count.clone());

            reached_min_count
        } else {
            let received_from_all = count_per_key
                .filter(q!(move |(success, error)| (success + error) >= max))
                .keys();

            not_all = current_responses.anti_join(received_from_all.clone());

            let out = reached_min_count.clone().filter_not_in(min_but_not_max);

            min_but_not_max = reached_min_count.filter_not_in(received_from_all);

            out
        };

        just_reached_quorum
    };

    (
        just_reached_quorum,
        responses.filter_map(q!(move |(key, res)| match res {
            Ok(_) => None,
            Err(e) => Some((key, e)),
        })),
    )
}

/// Like [`collect_quorum`] but with a dynamic quorum threshold provided as a
/// runtime [`Singleton`]. Use this when the quorum size can change (e.g., on
/// view changes in a primary/backup protocol).
///
/// Emits a slot key when the number of successful acks for that key reaches
/// the current value of `quorum_size`. Acks are discarded once quorum is reached.
#[expect(clippy::type_complexity, reason = "stream types with ordering")]
pub fn collect_dynamic_quorum<
    'a,
    L: Location<'a> + NoTick,
    K: Clone + Eq + Hash,
>(
    acks: Stream<(K, Result<(), ()>), L, Unbounded, NoOrder>,
    quorum_size: Singleton<usize, L, Unbounded>,
) -> Stream<K, L, Unbounded, NoOrder> {
    sliced! {
        let new_acks = use(acks, nondet!(
            /// Persist acks that haven't reached quorum yet.
        ));
        let threshold = use(quorum_size, nondet!(
            /// Dynamic quorum size may be stale; safe because a stale (larger)
            /// threshold only delays commits, never causes incorrect ones.
        ));

        let mut pending = use::state_null::<Stream<(K, Result<(), ()>), _, Bounded, NoOrder>>();

        let all_acks = pending.chain(new_acks);

        let count_per_key = all_acks.clone().into_keyed().fold(
            q!(|| 0usize),
            q!(|count, ack: Result<(), ()>| {
                if ack.is_ok() { *count += 1; }
            }, commutative = manual_proof!(/** counting is commutative */)),
        );

        let reached = count_per_key.entries()
            .cross_singleton(threshold)
            .filter_map(q!(|((key, count), min)| if count >= min { Some(key) } else { None }));

        pending = all_acks.anti_join(reached.clone());

        reached
    }
}

#[cfg(test)]
mod tests {
    use hydro_lang::live_collections::stream::{NoOrder, TotalOrder};
    use hydro_lang::prelude::*;

    use super::{collect_quorum, collect_quorum_with_response, collect_dynamic_quorum};

    #[test]
    fn collect_quorum_with_response_preserves_order() {
        let mut flow = FlowBuilder::new();
        let node = flow.process::<()>();

        let (in_send, input) = node.sim_input();
        let out_recv = collect_quorum_with_response(input, 3, 3).0.sim_output();

        flow.sim().exhaustive(async || {
            in_send.send((1, Ok::<(), ()>(())));
            in_send.send((1, Ok(())));
            in_send.send((1, Ok(())));
            in_send.send((2, Ok(())));
            in_send.send((2, Ok(())));
            in_send.send((3, Ok(())));
            in_send.send((3, Ok(())));
            in_send.send((3, Ok(())));

            assert_eq!(
                out_recv.collect::<Vec<_>>().await,
                vec![(1, ()), (1, ()), (1, ()), (3, ()), (3, ()), (3, ())]
            )
        });
    }

    #[test]
    fn collect_quorum_with_response_no_order() {
        let mut flow = FlowBuilder::new();
        let node = flow.process::<()>();

        let (in_send, input) = node.sim_input::<_, NoOrder, _>();
        let out_recv = collect_quorum_with_response(input, 2, 2).0.sim_output();

        flow.sim().exhaustive(async || {
            in_send.send_many_unordered([
                (1, Ok::<(), ()>(())),
                (1, Ok(())),
                (2, Ok(())),
                (3, Ok(())),
                (3, Ok(())),
            ]);

            out_recv
                .assert_yields_only_unordered([(1, ()), (1, ()), (3, ()), (3, ())])
                .await;
        });
    }

    #[test]
    fn collect_quorum_functionality() {
        let mut flow = FlowBuilder::new();
        let node = flow.process::<()>();

        let (in_send, input) = node.sim_input();
        let (success_recv, error_recv) = {
            let (success, error) = collect_quorum(input, 2, 3);
            (success.sim_output(), error.sim_output())
        };

        let compiled_sim = flow.sim().compiled();

        // Test case 1: Key reaches exact minimum quorum (2/3)
        compiled_sim.exhaustive(async || {
            in_send.send((1, Ok::<(), ()>(())));
            in_send.send((1, Ok(())));

            success_recv.assert_yields_only_unordered([1]).await;
            error_recv.assert_no_more().await;
        });

        // Test case 2: Key reaches maximum responses with mixed results (2 success, 1 error)
        compiled_sim.exhaustive(async || {
            in_send.send((2, Ok::<(), ()>(())));
            in_send.send((2, Ok(())));
            in_send.send((2, Err(())));

            success_recv.assert_yields_only_unordered([2]).await;
            error_recv.assert_yields_only([(2, ())]).await;
        });

        // Test case 3: Key doesn't reach quorum (1 success, 2 errors)
        compiled_sim.exhaustive(async || {
            in_send.send((3, Ok::<(), ()>(())));
            in_send.send((3, Err(())));
            in_send.send((3, Err(())));

            success_recv.assert_no_more().await;
            error_recv.assert_yields_only([(3, ()), (3, ())]).await;
        });

        // Test case 4: Key reaches quorum with extra responses
        compiled_sim.exhaustive(async || {
            in_send.send((4, Ok::<(), ()>(())));
            in_send.send((4, Ok(())));
            in_send.send((4, Ok(()))); // This should be ignored after quorum

            success_recv.assert_yields_only_unordered([4]).await;
            error_recv.assert_no_more().await;
        });

        // Test case 5: Key with only errors (no quorum)
        compiled_sim.exhaustive(async || {
            in_send.send((5, Err::<(), ()>(())));
            in_send.send((5, Err(())));
            in_send.send((5, Err(())));

            success_recv.assert_no_more().await;
            error_recv
                .assert_yields_only([(5, ()), (5, ()), (5, ())])
                .await;
        });

        // Test case 6: Key that reaches quorum exactly at max (2 success, 1 error)
        compiled_sim.exhaustive(async || {
            in_send.send((6, Err::<(), ()>(())));
            in_send.send((6, Ok(())));
            in_send.send((6, Ok(())));

            success_recv.assert_yields_only_unordered([6]).await;
            error_recv.assert_yields_only([(6, ())]).await;
        });
    }

    #[test]
    fn collect_quorum_min_equals_max() {
        let mut flow = FlowBuilder::new();
        let node = flow.process::<()>();

        let (in_send, input) = node.sim_input();
        let success_recv = collect_quorum(input, 2, 2).0.sim_output();

        flow.sim().exhaustive(async || {
            // When min == max, we need exactly that many responses
            in_send.send((1, Ok::<(), ()>(())));
            in_send.send((1, Ok(())));

            // This key gets exactly 2 responses (1 success, 1 error) - should not reach quorum
            in_send.send((2, Ok(())));
            in_send.send((2, Err(())));

            // This key gets 2 successes - should reach quorum
            in_send.send((3, Ok(())));
            in_send.send((3, Ok(())));

            // Only keys 1 and 3 should reach quorum (both have 2 successes)
            success_recv.assert_yields_only_unordered([1, 3]).await;
        });
    }

    #[test]
    fn collect_quorum_single_response() {
        let mut flow = FlowBuilder::new();
        let node = flow.process::<()>();

        let (in_send, input) = node.sim_input();
        let success_recv = collect_quorum(input, 1, 1).0.sim_output();

        flow.sim().exhaustive(async || {
            // With min=max=1, any single success should immediately reach quorum
            in_send.send((1, Ok::<(), ()>(())));
            in_send.send((2, Err(())));
            in_send.send((3, Ok(())));

            // Keys 1 and 3 should reach quorum immediately
            success_recv.assert_yields_only_unordered([1, 3]).await;
        });
    }

    #[test]
    fn collect_quorum_no_responses() {
        let mut flow = FlowBuilder::new();
        let node = flow.process::<()>();

        let (_in_send, input) = node.sim_input::<_, TotalOrder, _>();
        let success_recv = {
            let (success, _error) = collect_quorum::<_, _, i32, ()>(input, 2, 3);
            success.sim_output()
        };

        flow.sim().exhaustive(async || {
            // No responses sent - should get empty results
            success_recv.assert_no_more().await;
        });
    }

    #[test]
    fn collect_quorum_no_double_quorum_before_max() {
        let mut flow = FlowBuilder::new();
        let node = flow.process::<()>();

        let (in_send, input) = node.sim_input::<_, TotalOrder, _>();
        let success_recv = collect_quorum(input, 2, 4).0.sim_output();

        flow.sim().exhaustive(async || {
            // Key 1: First reaches quorum with 2 successes
            in_send.send((1, Ok::<(), ()>(())));
            in_send.send((1, Ok(())));

            // Key 1: Additional responses after quorum - should not trigger quorum again
            in_send.send((1, Ok(())));
            in_send.send((1, Ok(())));

            // Key 2: Reaches quorum later with mixed responses
            in_send.send((2, Err(())));
            in_send.send((2, Ok(())));
            in_send.send((2, Ok(())));
            in_send.send((2, Err(()))); // Additional error after quorum

              // Each key should appear exactly once, even though they received
            // additional responses after reaching quorum
            success_recv.assert_yields_only_unordered([1, 2]).await;
        });
    }

    #[test]
    fn dynamic_quorum_basic() {
        let mut flow = FlowBuilder::new();
        let node = flow.process::<()>();

        let (ack_send, acks) = node.sim_input::<(usize, Result<(), ()>), NoOrder, _>();
        let (qs_send, qs_stream) = node.sim_input::<usize, _, _>();
        let quorum_size: Singleton<usize, _, _> = qs_stream.fold(
            q!(|| 2usize), q!(|cur, new| *cur = new),
        ).into();
        let confirmed = collect_dynamic_quorum(acks, quorum_size);
        let out = confirmed.sim_output();

        flow.sim().exhaustive(async || {
            qs_send.send(2);
            ack_send.send_many_unordered([
                (0, Ok(())),
                (0, Ok(())),
                (1, Ok(())),
            ]);

            out.assert_yields_only_unordered([0usize]).await;
        });
    }

    #[test]
    fn dynamic_quorum_errors_dont_count() {
        let mut flow = FlowBuilder::new();
        let node = flow.process::<()>();

        let (ack_send, acks) = node.sim_input::<(usize, Result<(), ()>), NoOrder, _>();
        let (qs_send, qs_stream) = node.sim_input::<usize, _, _>();
        let quorum_size: Singleton<usize, _, _> = qs_stream.fold(
            q!(|| 2usize), q!(|cur, new| *cur = new),
        ).into();
        let confirmed = collect_dynamic_quorum(acks, quorum_size);
        let out = confirmed.sim_output();

        flow.sim().exhaustive(async || {
            qs_send.send(2);
            ack_send.send_many_unordered([
                (0, Ok(())),
                (0, Err(())),
            ]);

            out.assert_no_more().await;
        });
    }

    #[test]
    fn dynamic_quorum_no_double_emit() {
        let mut flow = FlowBuilder::new();
        let node = flow.process::<()>();

        let (ack_send, acks) = node.sim_input::<(usize, Result<(), ()>), NoOrder, _>();
        let (qs_send, qs_stream) = node.sim_input::<usize, _, _>();
        let quorum_size: Singleton<usize, _, _> = qs_stream.fold(
            q!(|| 2usize), q!(|cur, new| *cur = new),
        ).into();
        let confirmed = collect_dynamic_quorum(acks, quorum_size);
        let out = confirmed.sim_output();

        flow.sim().exhaustive(async || {
            qs_send.send(2);
            ack_send.send_many_unordered([
                (0, Ok(())),
                (0, Ok(())),
                (0, Ok(())),
            ]);

            out.assert_yields_only_unordered([0usize]).await;
        });
    }
}
