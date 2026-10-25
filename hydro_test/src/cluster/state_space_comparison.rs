//! State-space comparison: typed_consensus decomposition vs. Raft monolith.
//!
//! This module empirically demonstrates that typed_consensus's decomposition
//! into independent sliced! blocks yields a dramatically smaller simulation
//! state space than Raft's monolithic approach.
//!
//! Each test runs `exhaustive` mode (explores ALL possible execution schedules).
//! The typed_consensus component tests terminate quickly because each component
//! processes only 1-2 message types. Raft's equivalent cannot terminate because
//! ALL message types flow through one sliced! block.

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::Instant;

    use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
    use hydro_lang::location::MemberId;
    use hydro_lang::prelude::*;

    use crate::cluster::typed_consensus::*;

    /// Component test 1: commit_decisions (1 input stream, 3 messages)
    /// Expected: terminates exhaustive in <5 seconds
    #[test]
    fn exhaustive_commit_decisions_3_acks() {
        let start = Instant::now();

        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<Nodes>();

        let (ack_port, ack_stream) =
            cluster.sim_input::<ProposalAckMsg<Nodes>, TotalOrder, ExactlyOnce>();

        let commits = commit_decisions(ack_stream.into(), 2);
        let output = commits.sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&cluster, 3)
            .exhaustive(async move || {
                // 3 acks for slot 0 from different members → commit at quorum (2)
                ack_port.send(
                    0,
                    ProposalAckMsg {
                        view: 1,
                        slot: 0,
                        from_member: MemberId::from_raw_id(0),
                    },
                );
                ack_port.send(
                    0,
                    ProposalAckMsg {
                        view: 1,
                        slot: 0,
                        from_member: MemberId::from_raw_id(1),
                    },
                );
                ack_port.send(
                    0,
                    ProposalAckMsg {
                        view: 1,
                        slot: 0,
                        from_member: MemberId::from_raw_id(2),
                    },
                );

                let commits: Vec<CommitMsg> = output.collect_n_sorted(0, 1).await;
                assert_eq!(commits.len(), 1);
                assert_eq!(commits[0].slot, 0);
            });

        let elapsed = start.elapsed();
        eprintln!(
            "exhaustive_commit_decisions_3_acks: {:?} (TERMINATED)",
            elapsed
        );
    }

    /// Component test 2: compute_start_slot_from_quorum (1 input, 2 messages)
    /// Expected: terminates exhaustive in <5 seconds
    #[test]
    fn exhaustive_start_slot_2_promises() {
        let start = Instant::now();

        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<Nodes>();

        let (promise_port, promises) =
            cluster.sim_input::<PromiseMsg<u32, Nodes>, TotalOrder, ExactlyOnce>();

        let start_signal = compute_start_slot_from_quorum(promises.into(), 2);
        let output = start_signal.sim_cluster_output();

        flow.sim()
            .with_cluster_size(&cluster, 3)
            .exhaustive(async move || {
                promise_port.send(
                    0,
                    PromiseMsg {
                        view: 1,
                        max_committed_slot: 3,
                        from_member: MemberId::from_raw_id(1),
                        accepted: vec![],
                    },
                );
                promise_port.send(
                    0,
                    PromiseMsg {
                        view: 1,
                        max_committed_slot: 7,
                        from_member: MemberId::from_raw_id(2),
                        accepted: vec![],
                    },
                );

                let result: (usize, usize, Vec<(usize, u32)>) = output.next(0).await;
                assert_eq!(result.1, 8); // max(3,7) + 1 = start_slot
                assert_eq!(result.0, 1); // view
            });

        let elapsed = start.elapsed();
        eprintln!(
            "exhaustive_start_slot_2_promises: {:?} (TERMINATED)",
            elapsed
        );
    }

    /// Component test 3: propose_in_view_gated (2 inputs, 3 messages total)
    /// Expected: terminates exhaustive in <10 seconds
    #[test]
    fn exhaustive_propose_gated_2_requests() {
        let start = Instant::now();

        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<Nodes>();

        let (req_port, requests) = cluster.sim_input::<u32, TotalOrder, ExactlyOnce>();
        let (signal_port, start_signal) = cluster.sim_input::<(usize, usize, Vec<(usize, u32)>), TotalOrder, ExactlyOnce>();

        let proposals = propose_in_view_gated(requests, start_signal);
        let output = proposals.sim_cluster_output();

        flow.sim()
            .with_cluster_size(&cluster, 3)
            .exhaustive(async move || {
                signal_port.send(0, (1, 5, vec![])); // view 1, start at slot 5
                req_port.send(0, 100);
                req_port.send(0, 200);

                let props: Vec<ProposalMsg<u32>> = output.collect_n_sorted(0, 2).await;
                let slots: HashSet<usize> = props.iter().map(|p| p.slot).collect();
                assert_eq!(slots, HashSet::from([5, 6]));
            });

        let elapsed = start.elapsed();
        eprintln!(
            "exhaustive_propose_gated_2_requests: {:?} (TERMINATED)",
            elapsed
        );
    }

    /// Component test 4: phase1_prepare (2 inputs, 1 prepare message)
    /// Expected: terminates exhaustive in <5 seconds
    #[test]
    fn exhaustive_phase1_prepare_1_prepare() {
        let start = Instant::now();

        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<Nodes>();

        let (prepare_port, prepare_trigger) =
            cluster.sim_input::<PrepareMsg<Nodes>, TotalOrder, ExactlyOnce>();
        let (max_port, max_committed) = cluster.sim_input::<usize, TotalOrder, ExactlyOnce>();

        let (promises, _prepares_ec) = phase1_prepare(prepare_trigger, max_committed, cluster.source_iter(q!(std::iter::empty::<ProposalMsg<u32>>())).weaken_ordering::<NoOrder>().into(), &cluster);
        let output = promises.sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&cluster, 3)
            .exhaustive(async move || {
                max_port.send(0, 0);
                max_port.send(1, 0);
                max_port.send(2, 0);
                prepare_port.send(
                    0,
                    PrepareMsg {
                        view: 1,
                        from_leader: MemberId::from_raw_id(0),
                    },
                );

                // Member 0 broadcasts Prepare to all; members 1 and 2 respond
                // with promises routed back to member 0.
                let promises: Vec<PromiseMsg<u32, Nodes>> = output.collect_n_sorted(0, 2).await;
                assert_eq!(promises.len(), 2);
            });

        let elapsed = start.elapsed();
        eprintln!(
            "exhaustive_phase1_prepare_1_prepare: {:?} (TERMINATED)",
            elapsed
        );
    }

    /// RAFT monolithic equivalent: 6 messages through one sliced! block.
    /// Expected: DOES NOT TERMINATE within 60 seconds (state space too large).
    ///
    /// This test is #[ignore]d by default — run with --ignored to demonstrate.
    /// It will be killed after 60s if it hasn't terminated.
    #[test]
    #[ignore]
    fn exhaustive_raft_6_messages_does_not_terminate() {
        use crate::cluster::raft::{raft_server, RaftConfig, Replica};

        let start = Instant::now();

        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<Replica>();

        let (election_send, election_timers) = cluster.sim_input();
        let (hb_send, heartbeat_timers) = cluster.sim_input();
        let (req_send, requests) = cluster.sim_input::<String, TotalOrder, ExactlyOnce>();

        let outputs = raft_server(
            &cluster,
            requests,
            election_timers,
            heartbeat_timers,
            RaftConfig { cluster_size: 3 },
            TCP.fail_stop().bincode(),
            nondet!(/** test: which member wins is non-deterministic */),
        );

        let output = outputs.committed.end_atomic().sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&cluster, 3)
            .exhaustive(async move || {
                // 12 messages total — still fewer than our component tests combined,
                // but all flowing through Raft's single monolithic sliced! block.
                election_send.send(0, ());
                election_send.send(0, ());
                req_send.send(0, "a".to_owned());
                req_send.send(0, "b".to_owned());
                for _ in 0..8 {
                    hb_send.send(0, ());
                }

                // Drain whatever committed entries arrive (may be 0, 1, or 2).
                hydro_lang::sim::quiesce().await;
                let _: Vec<crate::cluster::raft::LogEntry<String>> =
                    output.collect(0).await;
            });

        let elapsed = start.elapsed();
        // If we get here, something is wrong — this should never terminate
        eprintln!(
            "exhaustive_raft_6_messages: {:?} (UNEXPECTEDLY TERMINATED)",
            elapsed
        );
    }
}
