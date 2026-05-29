//! Sim test for the composition logic: slot assignment → quorum → decide → deliver.
//! Runs on a single Process to avoid the Cluster sim limitation.
//! Simulates what compose_protocol does but without networking.

#[cfg(test)]
mod tests {
    use hydro_lang::live_collections::stream::NoOrder;
    use hydro_lang::prelude::*;
    use hydro_std::quorum::collect_dynamic_quorum;
    use crate::primitives::decide::join_confirmed;
    use crate::primitives::ordered_deliver::deliver_in_order;

    /// End-to-end composition: payloads → slot assign → quorum → decide → deliver.
    /// Simulates the primary's perspective: it assigns slots, "broadcasts" (we simulate
    /// acks arriving), quorum fires, decide joins, deliver emits in order.
    #[test]
    fn composition_e2e() {
        let mut flow = FlowBuilder::new();
        let node = flow.process::<()>();
        let tick = node.tick();

        // Simulate: payloads arrive, get slotted
        let (payload_send, payloads) = node.sim_input::<String>();

        // Slot assignment (same pattern as compose_protocol)
        let (next_slot_complete, next_slot) =
            tick.cycle_with_initial(tick.singleton(q!(0usize)));

        let batch = payloads.batch(&tick, nondet!(/** batch */));
        let indexed = batch.enumerate()
            .cross_singleton(next_slot.clone())
            .map(q!(|((i, payload), base)| (base + i, payload)));

        let count = indexed.clone().count();
        next_slot_complete.complete_next_tick(count.zip(next_slot).map(q!(|(n, b)| b + n)));

        // The indexed payloads (slot, payload) — this is what the primary produces
        let slot_payloads = indexed.weaken_ordering::<NoOrder>().all_ticks();

        // Simulate acks: in real system, backups ack each slot.
        // Here we simulate by feeding acks directly.
        let (ack_send, acks) = node.sim_input::<(usize, Result<(), ()>)>();

        // Dynamic quorum (size = 2, simulating 2 replicas must ack)
        let (qs_send, qs_stream) = node.sim_input::<usize>();
        let quorum_size: Singleton<usize, _, _> = qs_stream.fold(
            q!(|| 2usize), q!(|cur, new| *cur = new),
        ).into();

        let confirmed = collect_dynamic_quorum(acks, quorum_size);

        // Decide: join slot_payloads with confirmed slots
        let committed = join_confirmed(slot_payloads, confirmed);

        // Ordered deliver: emit in slot order
        let delivered = deliver_in_order(committed, &node);

        let out = delivered.sim_output();

        flow.sim().fuzz(async || {
            qs_send.send(2);

            // Send 3 payloads
            payload_send.send("cmd_a".to_string());
            payload_send.send("cmd_b".to_string());
            payload_send.send("cmd_c".to_string());

            // Simulate: all 3 slots get 2 acks each (quorum = 2)
            ack_send.send_many_unordered([
                (0, Ok(())), (0, Ok(())),
                (1, Ok(())), (1, Ok(())),
                (2, Ok(())), (2, Ok(())),
            ]);

            // All 3 should be delivered in order
            let results = out.collect::<Vec<_>>().await;
            assert_eq!(results.len(), 3, "Expected 3 delivered, got {:?}", results);
            assert_eq!(results[0], (0, "cmd_a".to_string()));
            assert_eq!(results[1], (1, "cmd_b".to_string()));
            assert_eq!(results[2], (2, "cmd_c".to_string()));
        });
    }

    /// Partial quorum: only some slots reach quorum.
    #[test]
    fn composition_partial_quorum() {
        let mut flow = FlowBuilder::new();
        let node = flow.process::<()>();
        let tick = node.tick();

        let (payload_send, payloads) = node.sim_input::<String>();

        let (next_slot_complete, next_slot) =
            tick.cycle_with_initial(tick.singleton(q!(0usize)));
        let batch = payloads.batch(&tick, nondet!(/** batch */));
        let indexed = batch.enumerate()
            .cross_singleton(next_slot.clone())
            .map(q!(|((i, payload), base)| (base + i, payload)));
        let count = indexed.clone().count();
        next_slot_complete.complete_next_tick(count.zip(next_slot).map(q!(|(n, b)| b + n)));

        let slot_payloads = indexed.weaken_ordering::<NoOrder>().all_ticks();

        let (ack_send, acks) = node.sim_input::<(usize, Result<(), ()>)>();
        let (qs_send, qs_stream) = node.sim_input::<usize>();
        let quorum_size: Singleton<usize, _, _> = qs_stream.fold(
            q!(|| 2usize), q!(|cur, new| *cur = new),
        ).into();

        let confirmed = collect_dynamic_quorum(acks, quorum_size);
        let committed = join_confirmed(slot_payloads, confirmed);
        let delivered = deliver_in_order(committed, &node);
        let out = delivered.sim_output();

        flow.sim().fuzz(async || {
            qs_send.send(2);

            payload_send.send("a".to_string());
            payload_send.send("b".to_string());
            payload_send.send("c".to_string());

            // Only slots 0 and 2 reach quorum (slot 1 has only 1 ack)
            ack_send.send_many_unordered([
                (0, Ok(())), (0, Ok(())),
                (1, Ok(())), // only 1 — not quorum
                (2, Ok(())), (2, Ok(())),
            ]);

            // Only slot 0 delivered (contiguous prefix stops at gap)
            let results = out.collect::<Vec<_>>().await;
            assert_eq!(results.len(), 1, "Expected 1 (gap at slot 1), got {:?}", results);
            assert_eq!(results[0], (0, "a".to_string()));
        });
    }
}
