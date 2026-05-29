//! Ordered delivery: buffer committed commands and emit in slot order.
//!
//! Adapted from consensus-zoo's `primitives::ordered_deliver::deliver_in_order_generic`.

use std::fmt::Debug;

use hydro_lang::live_collections::stream::NoOrder;
use hydro_lang::location::{Location, NoTick};
use hydro_lang::prelude::*;
use serde::{Serialize, de::DeserializeOwned};
use stageleft::q;

/// Buffer out-of-order committed (slot, cmd) pairs and emit them in slot order.
///
/// This is the ordering primitive without any state machine application.
/// Feed the output into any sequential state machine via scan.
pub fn deliver_in_order<'a, L: Location<'a> + NoTick, Cmd>(
    commits: Stream<(usize, Cmd), L, Unbounded, NoOrder>,
    location: &L,
) -> Stream<(usize, Cmd), L, Unbounded>
where
    Cmd: Clone + Serialize + DeserializeOwned + Debug + Send + Ord + 'static,
{
    let tick = location.tick();

    let (buf_complete, buffered) =
        tick.cycle::<Stream<(usize, Cmd), _, _>>();
    let (next_complete, next_slot) =
        tick.cycle_with_initial(tick.singleton(q!(0usize)));

    let sorted = commits
        .batch(&tick, nondet!(/** Commits arrive out of order. */))
        .chain(buffered)
        .sort();

    let all_commits = sorted.fold(
        q!(|| Vec::new()),
        q!(|v, commit| v.push(commit)),
    );

    let result = all_commits
        .zip(next_slot)
        .map(q!(|(commits, mut next)| {
            let mut delivered = Vec::new();
            let mut remaining = Vec::new();
            for (slot, cmd) in commits {
                if slot == next {
                    delivered.push((slot, cmd));
                    next += 1;
                } else {
                    remaining.push((slot, cmd));
                }
            }
            (delivered, next, remaining)
        }));

    next_complete.complete_next_tick(result.clone().map(q!(|(_, next, _)| next)));
    buf_complete.complete_next_tick(
        result.clone().into_stream().flat_map_ordered(q!(|(_, _, remaining)| remaining.into_iter()))
    );

    result
        .into_stream()
        .flat_map_ordered(q!(|(delivered, _, _)| delivered.into_iter()))
        .all_ticks()
}

#[cfg(test)]
mod tests {
    use hydro_lang::live_collections::stream::NoOrder;
    use hydro_lang::prelude::*;
    use super::deliver_in_order;

    #[test]
    fn in_order_delivery() {
        let mut flow = FlowBuilder::new();
        let node = flow.process::<()>();

        let (send, commits) = node.sim_input::<(usize, String)>();
        let delivered = deliver_in_order(commits, &node);
        let out = delivered.sim_output();

        flow.sim().exhaustive(async || {
            send.send_many_unordered([
                (0, "a".to_string()),
                (1, "b".to_string()),
                (2, "c".to_string()),
            ]);

            out.assert_yields_only([
                (0, "a".to_string()),
                (1, "b".to_string()),
                (2, "c".to_string()),
            ]).await;
        });
    }

    #[test]
    fn out_of_order_reordered() {
        let mut flow = FlowBuilder::new();
        let node = flow.process::<()>();

        let (send, commits) = node.sim_input::<(usize, String)>();
        let delivered = deliver_in_order(commits, &node);
        let out = delivered.sim_output();

        flow.sim().exhaustive(async || {
            send.send_many_unordered([
                (2, "c".to_string()),
                (0, "a".to_string()),
                (1, "b".to_string()),
            ]);

            out.assert_yields_only([
                (0, "a".to_string()),
                (1, "b".to_string()),
                (2, "c".to_string()),
            ]).await;
        });
    }

    #[test]
    fn gap_buffers() {
        let mut flow = FlowBuilder::new();
        let node = flow.process::<()>();

        let (send, commits) = node.sim_input::<(usize, String)>();
        let delivered = deliver_in_order(commits, &node);
        let out = delivered.sim_output();

        flow.sim().exhaustive(async || {
            send.send_many_unordered([
                (0, "a".to_string()),
                (2, "c".to_string()),
            ]);

            out.assert_yields_only([(0, "a".to_string())]).await;
        });
    }
}
