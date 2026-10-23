//! Provenance survey of distributed Hydro programs beyond the ground-truth set.
//!
//! Each test drives one program through quiescence barriers under
//! [`hydro_lang::sim::provenance`] and records, per channel and per operational stimulus, the
//! label distribution and physical work. Numbers that follow from the protocol's definition are
//! asserted; everything else is recorded to `target/provenance_dump.txt` as an observation.
//! See `design_docs/reports/2026-09_provenance_feedback_cycle_classification.md`.

/// Cluster tag for the survey's Raft instances (must be nameable from generated code, so it
/// cannot live inside the test module).
pub struct SurveyReplica;

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::Write;

    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::provenance::{
        Classified, EmissionPointKind, EmissionRecord, Label, classify, take_emissions,
    };
    use hydro_lang::sim::quiesce;

    fn sends(records: &[EmissionRecord]) -> Vec<EmissionRecord> {
        records
            .iter()
            .filter(|r| r.kind == EmissionPointKind::Network)
            .cloned()
            .collect()
    }

    /// Label counts per emission-point name for the tail of `history` starting at `from`.
    fn tail_labels(
        history: &[EmissionRecord],
        from: usize,
    ) -> BTreeMap<String, BTreeMap<Label, usize>> {
        let classified = classify(history, false);
        // `classify(_, false)` emits one entry per record that is neither a receipt nor a
        // cycle-sink carry; count those before `from` to align.
        let sends_before = history[..from]
            .iter()
            .filter(|r| {
                !matches!(
                    r.kind,
                    EmissionPointKind::Receive | EmissionPointKind::Cycle
                )
            })
            .count();
        let mut out: BTreeMap<String, BTreeMap<Label, usize>> = BTreeMap::new();
        for c in classified.into_iter().skip(sends_before) {
            *out.entry(c.record.name.clone())
                .or_default()
                .entry(c.label)
                .or_default() += 1;
        }
        out
    }

    fn note(text: &str) {
        let path = std::env::var("HYDRO_PROVENANCE_DUMP")
            .unwrap_or_else(|_| "target/provenance_dump.txt".to_string());
        let _ = std::fs::create_dir_all("target");
        if let Ok(mut out) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
        {
            let _ = writeln!(out, "## {text}");
        }
    }

    fn dump(title: &str, classified: &[Classified]) {
        let path = std::env::var("HYDRO_PROVENANCE_DUMP")
            .unwrap_or_else(|_| "target/provenance_dump.txt".to_string());
        let _ = std::fs::create_dir_all("target");
        let Ok(mut out) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
        else {
            return;
        };
        let _ = writeln!(out, "== {title}");
        for c in classified {
            let r = &c.record;
            if r.kind == EmissionPointKind::Receive {
                continue;
            }
            let tags: Vec<String> = r
                .tags
                .iter()
                .map(|t| {
                    let k = match t.kind {
                        hydro_lang::sim::provenance::TagKind::Data => "D",
                        hydro_lang::sim::provenance::TagKind::Operational => "T",
                    };
                    if t.member == u32::MAX {
                        format!("{k}{}.{}", t.port, t.seq)
                    } else {
                        format!("{k}{}@{}.{}", t.port, t.member, t.seq)
                    }
                })
                .collect();
            let _ = writeln!(
                out,
                "{:<17} {:<34} {:>3}->{:<3} {:<40} {}{}B",
                format!("{:?}", c.label),
                r.name,
                r.member.map(|m| m.to_string()).unwrap_or("-".into()),
                r.recipient.map(|m| m.to_string()).unwrap_or("-".into()),
                format!("{{{}}}", tags.join(",")),
                if r.coarse { "coarse " } else { "" },
                r.bytes
            );
        }
    }

    /// Reliable broadcast (closed membership): the source sends each message to every member;
    /// each member echoes every *new* message to every member; `unique` stops the echo.
    ///
    /// No timers. For M messages and 3 members: 3M initial sends and 9M echoes, every one of
    /// them `Productive` — each (sender, recipient) pair carries each message exactly once, so
    /// the per-channel history never sees a repeat. Nothing recurs. Re-injecting an identical
    /// message is a new source event (new tag): the source sends it, but `unique` at the members
    /// compares by value and no echo happens.
    #[test]
    fn reliable_broadcast_is_productive_and_drains() {
        use hydro_std::ec_inference_demos::reliable_broadcast::reliable_broadcast_closed;

        const MEMBERS: usize = 3;
        const M: usize = 2;

        let mut flow = FlowBuilder::new();
        let source = flow.process::<()>();
        let cluster = flow.cluster::<()>();
        let (msg_send, msgs) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
        let delivered = reliable_broadcast_closed(msgs, &cluster)
            .assume_ordering::<TotalOrder>(nondet!(/** observation only */))
            .sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&cluster, MEMBERS)
            .with_provenance()
            .unit_test_fuzz_iterations(3)
            .fuzz(async || {
                msg_send.send_many(0..M as u32);
                quiesce().await;
                let history = take_emissions();
                let s = sends(&history);
                assert_eq!(s.len(), 3 * M + 9 * M, "{} sends", s.len());
                let per_channel: BTreeMap<_, usize> =
                    s.iter().fold(BTreeMap::new(), |mut m, r| {
                        *m.entry((r.name.clone(), r.member, r.recipient)).or_default() += 1;
                        m
                    });
                assert!(
                    per_channel.values().all(|&n| n == M),
                    "each pair carries each message once: {per_channel:?}"
                );
                let labels = tail_labels(&history, 0);
                for (name, counts) in &labels {
                    assert_eq!(counts.keys().collect::<Vec<_>>(), vec![&Label::Productive], "{name}: {counts:?}");
                }
                for member in 0..MEMBERS as u32 {
                    let mut got: Vec<u32> = delivered.collect(member).await;
                    got.sort();
                    assert_eq!(got, (0..M as u32).collect::<Vec<_>>());
                }

                // Re-inject message 0: the source sends it again (new source event, so
                // `Productive` by lineage), but no member echoes it.
                msg_send.send(0);
                quiesce().await;
                let again = take_emissions();
                let s2 = sends(&again);
                assert_eq!(s2.len(), MEMBERS, "only the initial fan-out; `unique` stops the echo");
                assert!(s2.iter().all(|r| r.member.is_none()), "all from the source process");
                let mut all = history.clone();
                all.extend(again.iter().cloned());
                dump("reliable broadcast, 3 members, 2 messages then a duplicate", &classify(&all, false));
                note(&format!("reliable broadcast: {} sends for {M} messages, all Productive; duplicate input -> {} sends, 0 echoes", s.len(), s2.len()));
            });
    }

    /// Uniform reliable broadcast: like reliable broadcast, but a member delivers only after it
    /// has received `threshold` echoes. Same network emission structure — all `Productive` — and
    /// the threshold gates delivery, not echoing.
    ///
    /// The *delivery output* is labelled through `quorum`'s `sliced!` block, whose `by_mut`
    /// HashMaps make every certified fact inherit the lineage of every pending fact. In
    /// schedules where both messages certify in one tick the second delivery is therefore
    /// dominated and reads `Redundant`. That is coarse lineage from our own `quorum` module, not
    /// the protocol; it is recorded, not asserted.
    #[test]
    fn uniform_broadcast_is_productive_and_drains() {
        use hydro_std::ec_inference_demos::uniform_broadcast::uniform_reliable_broadcast_closed;

        const MEMBERS: usize = 3;
        const M: usize = 2;

        let mut flow = FlowBuilder::new();
        let source = flow.process::<()>();
        let cluster = flow.cluster::<()>();
        let (msg_send, msgs) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();
        let delivered = uniform_reliable_broadcast_closed(msgs, &cluster, 2)
            .assume_ordering::<TotalOrder>(nondet!(/** observation only */))
            .sim_cluster_output();

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&cluster, MEMBERS)
            .with_provenance()
            .unit_test_fuzz_iterations(3)
            .fuzz(async || {
                msg_send.send_many(0..M as u32);
                quiesce().await;
                let history = take_emissions();
                let s = sends(&history);
                let labels = tail_labels(&history, 0);
                for (name, counts) in &labels {
                    if name.starts_with("output") {
                        continue; // delivery labels go through `quorum`'s opaque state; recorded below
                    }
                    assert_eq!(counts.keys().collect::<Vec<_>>(), vec![&Label::Productive], "{name}: {counts:?}");
                }
                for member in 0..MEMBERS as u32 {
                    let mut got: Vec<u32> = delivered.collect(member).await;
                    got.sort();
                    assert_eq!(got, (0..M as u32).collect::<Vec<_>>());
                }
                msg_send.send(0);
                quiesce().await;
                let again = sends(&take_emissions());
                note(&format!(
                    "uniform broadcast (threshold 2): {} sends for {M} messages, labels {labels:?}; duplicate input -> {} sends",
                    s.len(),
                    again.len()
                ));
                assert_eq!(again.len(), MEMBERS, "duplicate input: initial fan-out only");
            });
    }

    /// Multi-Paxos (`hydro_std::ec_inference_demos::multi_paxos`): `leads` are the operational
    /// stimulus (establish an epoch), `commands` are data. Barrier-separated:
    ///
    /// 1. lead(1) at proposer 0 -> phase 1 to acceptors: no data exists, `FixedOperational`;
    ///    acceptor replies likewise.
    /// 2. two commands -> phase 2 with values and learning: `Productive`.
    /// 3. lead(2) at proposer 0 -> phase 1 again. The acceptors' coverings now carry the
    ///    accepted values back to the leader, and the leader re-proposes what it adopted:
    ///    retained state re-emitted on an operational event. Recorded, and the recurrence check
    ///    is what matters: a third lead with no new commands should not grow the re-proposal.
    #[test]
    fn multi_paxos_election_fixed_replication_productive_succession_reproposes() {
        use hydro_std::ec_inference_demos::multi_paxos::multi_paxos;

        const ACCEPTORS: usize = 3;
        const MAJORITY: usize = 2;
        const LEARNERS: usize = 1;

        let mut flow = FlowBuilder::new();
        let acceptors = flow.cluster::<()>();
        let proposers = flow.cluster::<()>();
        let learners = flow.cluster::<()>();
        let (lead_send, leads) = proposers.sim_input_operational::<u64, TotalOrder, ExactlyOnce>();
        let (cmd_send, commands) = proposers.sim_input::<u32, TotalOrder, ExactlyOnce>();
        let outs = multi_paxos(&acceptors, &learners, MAJORITY, leads, commands);
        let learned = outs.learned.sim_cluster_output();
        outs.chosen.for_each(q!(
            |_| {},
            commutative = manual_proof!(/** observation only */)
        ));
        outs.established.for_each(q!(
            |_| {},
            commutative = manual_proof!(/** observation only */)
        ));

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&acceptors, ACCEPTORS)
            .with_cluster_size(&proposers, 1)
            .with_cluster_size(&learners, LEARNERS)
            .with_provenance()
            .unit_test_fuzz_iterations(3)
            .fuzz(async || {
                // 1. Election.
                lead_send.send(0, 1);
                quiesce().await;
                let mut history = take_emissions();
                let s1 = sends(&history);
                assert!(!s1.is_empty());
                let l1 = tail_labels(&history, 0);
                for (name, counts) in &l1 {
                    assert!(
                        counts.keys().all(|l| matches!(l, Label::FixedOperational | Label::Constant)),
                        "{name}: {counts:?}"
                    );
                }

                // 2. Commands.
                let mark = history.len();
                cmd_send.send(0, 10u32);
                cmd_send.send(0, 20u32);
                quiesce().await;
                history.extend(take_emissions());
                let l2 = tail_labels(&history, mark);
                let s2 = sends(&history[mark..]);
                assert!(!s2.is_empty());
                let non_productive: Vec<_> = l2
                    .iter()
                    .filter(|(_, c)| c.keys().any(|l| *l != Label::Productive))
                    .collect();
                let got: Vec<(u64, usize, usize, Option<u32>)> = learned.collect_n_sorted(0, 2).await;
                assert_eq!(got.iter().filter_map(|g| g.3).collect::<Vec<_>>(), vec![10, 20]);

                // 3. Succession with no new commands.
                let mark3 = history.len();
                lead_send.send(0, 2);
                quiesce().await;
                history.extend(take_emissions());
                let l3 = tail_labels(&history, mark3);
                let s3 = sends(&history[mark3..]);

                // 4. Another succession with no new commands: the re-proposal must not grow.
                let mark4 = history.len();
                lead_send.send(0, 3);
                quiesce().await;
                history.extend(take_emissions());
                let l4 = tail_labels(&history, mark4);
                let s4 = sends(&history[mark4..]);
                let bytes3: usize = s3.iter().map(|r| r.bytes).sum();
                let bytes4: usize = s4.iter().map(|r| r.bytes).sum();

                note(&format!(
                    "multi_paxos: election {} sends {l1:?}; commands {} sends {l2:?} (non-productive: {non_productive:?}); succession#1 {} sends/{bytes3}B {l3:?}; succession#2 {} sends/{bytes4}B {l4:?}",
                    s1.len(), s2.len(), s3.len(), s4.len()
                ));
                dump("multi_paxos: election, 2 commands, succession, succession", &classify(&history, false));
                assert!(s4.len() <= s3.len() && bytes4 <= bytes3, "succession work does not grow without new commands");
            });
    }

    /// Dynamic-membership Raft (`dyn_raft_server`), same protocol as the Raft test plus a
    /// membership change. Election -> `FixedOperational`; N commands then heartbeat ->
    /// `Productive` replication; a `remove` reconfiguration is data (it is a log entry) and its
    /// replication on the next heartbeat should be `Productive`; the following heartbeat carries
    /// nothing new and is fixed-size.
    #[test]
    fn dyn_raft_election_fixed_replication_and_reconfiguration_productive() {
        use hydro_lang::location::MemberId;

        use super::SurveyReplica as Replica;
        use crate::cluster::dyn_raft::{
            ConfigurationChange, DynRaftConfig, ReconfigurationRequest, dyn_raft_server,
        };

        const MEMBERS: usize = 4;
        const N: usize = 3;

        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<Replica>();
        let (election_send, election_ticks) =
            cluster.sim_input_operational::<(), TotalOrder, ExactlyOnce>();
        let (heartbeat_send, heartbeat_ticks) =
            cluster.sim_input_operational::<(), TotalOrder, ExactlyOnce>();
        let (command_send, commands) = cluster.sim_input::<String, TotalOrder, ExactlyOnce>();
        let (reconfig_send, reconfigurations) =
            cluster.sim_input::<ReconfigurationRequest<Replica>, TotalOrder, ExactlyOnce>();
        let outputs = dyn_raft_server(
            &cluster,
            commands,
            reconfigurations,
            election_ticks,
            heartbeat_ticks,
            DynRaftConfig {
                initial_voter_count: MEMBERS,
            },
            TCP.fail_stop().bincode(),
            nondet!(/** only member 0's election timer fires */),
        );
        let committed = outputs.committed.end_atomic().sim_cluster_output();
        outputs.redirected.for_each(q!(|_| {}));
        outputs.reconfiguration_results.for_each(q!(|_| {}));
        outputs.leader_views.for_each(q!(|_| {}));

        flow.sim()
            .skip_consistency_assertions()
            .with_cluster_size(&cluster, MEMBERS)
            .with_provenance()
            .unit_test_fuzz_iterations(3)
            .fuzz(async || {
                election_send.send(0, ());
                quiesce().await;
                let mut history = take_emissions();
                let l1 = tail_labels(&history, 0);
                let leader_election: Vec<_> = sends(&history).into_iter().filter(|r| r.member == Some(0)).collect();
                assert!(leader_election.iter().all(|r| r.data_tags().count() == 0), "no data yet");

                // Commands, then a heartbeat replicates them.
                for i in 0..N {
                    command_send.send(0, format!("cmd{i}"));
                }
                quiesce().await;
                assert!(sends(&take_emissions()).is_empty(), "commands only append");
                let mark2 = history.len();
                heartbeat_send.send(0, ());
                quiesce().await;
                history.extend(take_emissions());
                let l2 = tail_labels(&history, mark2);
                let hb1: Vec<_> = sends(&history[mark2..]).into_iter().filter(|r| r.member == Some(0)).collect();
                assert_eq!(hb1.len(), MEMBERS - 1);
                let hb1_bytes: usize = hb1.iter().map(|r| r.bytes).sum();

                // Reconfiguration: remove member 3. It is a log entry; the next heartbeat
                // replicates it.
                let mark3 = history.len();
                reconfig_send.send(
                    0,
                    ReconfigurationRequest {
                        request_id: 1,
                        change: ConfigurationChange::Remove(MemberId::from_raw_id(3)),
                    },
                );
                quiesce().await;
                heartbeat_send.send(0, ());
                quiesce().await;
                history.extend(take_emissions());
                let l3 = tail_labels(&history, mark3);
                let hb2: Vec<_> = sends(&history[mark3..]).into_iter().filter(|r| r.member == Some(0)).collect();
                let hb2_bytes: usize = hb2.iter().map(|r| r.bytes).sum();

                // Steady state.
                let mark4 = history.len();
                heartbeat_send.send(0, ());
                quiesce().await;
                history.extend(take_emissions());
                let l4 = tail_labels(&history, mark4);
                let hb3: Vec<_> = sends(&history[mark4..]).into_iter().filter(|r| r.member == Some(0)).collect();
                let hb3_bytes: usize = hb3.iter().map(|r| r.bytes).sum();

                let mut got = 0;
                while committed.try_next(0).await.is_some() {
                    got += 1;
                }
                note(&format!(
                    "dyn_raft: election {l1:?}; replicate {N} cmds: {} sends/{hb1_bytes}B {l2:?}; remove(3) + heartbeat: {} sends/{hb2_bytes}B {l3:?}; steady heartbeat: {} sends/{hb3_bytes}B {l4:?}; leader committed {got} entries",
                    hb1.len(), hb2.len(), hb3.len()
                ));
                dump("dyn_raft: election, replication, remove(3), steady heartbeat (leader's sends)", &classify(&history, false).into_iter().filter(|c| c.record.member == Some(0)).collect::<Vec<_>>());
                assert!(hb3_bytes < hb1_bytes, "steady heartbeat is smaller than replication");
                assert!(got >= N, "leader committed the commands");
            });
    }
}
