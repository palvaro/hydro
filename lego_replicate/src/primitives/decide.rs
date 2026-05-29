//! Decide: join confirmed slot numbers with buffered commands.
//!
//! Adapted from consensus-zoo's `primitives::decide::join_confirmed_generic`.

use std::fmt::Debug;

use hydro_lang::live_collections::stream::{NoOrder, Ordering};
use hydro_lang::location::{Location, NoTick};
use hydro_lang::prelude::*;
use serde::{Serialize, de::DeserializeOwned};

/// Join confirmed slot numbers with their corresponding commands.
///
/// Buffers (slot, cmd) pairs and confirmed slot numbers across slices.
/// Emits (slot, cmd) when both the command and its confirmation are present.
pub fn join_confirmed<'a, L: Location<'a> + NoTick, Cmd, O: Ordering>(
    slot_cmds: Stream<(usize, Cmd), L, Unbounded, O>,
    confirmed_slots: Stream<usize, L, Unbounded, NoOrder>,
) -> Stream<(usize, Cmd), L, Unbounded, NoOrder>
where
    Cmd: Clone + Serialize + DeserializeOwned + Debug + Send + 'static,
{
    sliced! {
        let mut pending_cmds = use::state_null::<Stream<(usize, Cmd), _, _, NoOrder>>();
        let mut pending_confirms = use::state_null::<Stream<usize, _, _, NoOrder>>();

        let new_cmds = use(slot_cmds, nondet!(/** commands arrive as assigned */));
        let new_confirms = use(confirmed_slots, nondet!(/** confirmations arrive after quorum */));

        let all_cmds = pending_cmds.chain(new_cmds);
        let all_confirms = pending_confirms.chain(new_confirms);

        let joined = all_cmds.clone()
            .join(all_confirms.clone().map(q!(|slot| (slot, ()))))
            .map(q!(|(slot, (cmd, ()))| (slot, cmd)));

        let joined_slots = joined.clone().map(q!(|(slot, _)| slot));
        pending_cmds = all_cmds.anti_join(joined_slots.clone());
        pending_confirms = all_confirms.filter_not_in(joined_slots);

        joined
    }
}

#[cfg(test)]
mod tests {
    use hydro_lang::live_collections::stream::NoOrder;
    use hydro_lang::prelude::*;
    use super::join_confirmed;

    #[test]
    fn basic_join() {
        let mut flow = FlowBuilder::new();
        let node = flow.process::<()>();

        let (cmd_send, cmds) = node.sim_input::<(usize, String), _, _>();
        let (conf_send, confs) = node.sim_input::<usize, NoOrder, _>();

        let joined = join_confirmed(cmds, confs);
        let out = joined.sim_output();

        flow.sim().exhaustive(async || {
            cmd_send.send((0, "a".to_string()));
            cmd_send.send((1, "b".to_string()));
            conf_send.send_many_unordered([0, 1]);

            out.assert_yields_only_unordered([
                (0, "a".to_string()),
                (1, "b".to_string()),
            ]).await;
        });
    }

    #[test]
    fn confirmation_before_command() {
        let mut flow = FlowBuilder::new();
        let node = flow.process::<()>();

        let (cmd_send, cmds) = node.sim_input::<(usize, String), _, _>();
        let (conf_send, confs) = node.sim_input::<usize, NoOrder, _>();

        let joined = join_confirmed(cmds, confs);
        let out = joined.sim_output();

        flow.sim().exhaustive(async || {
            conf_send.send_many_unordered([0]);
            cmd_send.send((0, "x".to_string()));

            out.assert_yields_only_unordered([(0, "x".to_string())]).await;
        });
    }

    #[test]
    fn unconfirmed_not_emitted() {
        let mut flow = FlowBuilder::new();
        let node = flow.process::<()>();

        let (cmd_send, cmds) = node.sim_input::<(usize, String), _, _>();
        let (conf_send, confs) = node.sim_input::<usize, NoOrder, _>();

        let joined = join_confirmed(cmds, confs);
        let out = joined.sim_output();

        flow.sim().exhaustive(async || {
            cmd_send.send((0, "a".to_string()));
            cmd_send.send((1, "b".to_string()));
            cmd_send.send((2, "c".to_string()));
            conf_send.send_many_unordered([1]);

            out.assert_yields_only_unordered([(1, "b".to_string())]).await;
        });
    }
}
