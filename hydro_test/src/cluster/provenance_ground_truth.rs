//! Provenance-based classification of feedback cycles, validated against the metastability
//! ground-truth programs.
//!
//! These tests exercise [`hydro_lang::sim::provenance`]: the simulator is compiled with forward
//! lineage tracking, the test drives the program through explicit quiescence barriers (so each
//! stimulus is the only cause of the work that follows it), and the emission log is classified
//! by lineage shape alone. See `design_docs/reports/2026-09_dfir_feedback_cycle_analysis_outcome.md`
//! for why aggregate telemetry could not do this and why forward provenance can.

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::provenance::{
        EmissionPointKind, EmissionRecord, Label, attribute, classify, take_emissions,
    };
    use hydro_lang::sim::quiesce;

    use crate::cluster::pure_heartbeat::pure_heartbeat;

    fn network_records(records: &[EmissionRecord]) -> Vec<EmissionRecord> {
        records
            .iter()
            .filter(|r| r.kind == EmissionPointKind::Network)
            .cloned()
            .collect()
    }

    /// Prints classified emissions compactly: `label  name  sender->recipient  tags  [coarse] bytes`.
    fn dump(title: &str, classified: &[hydro_lang::sim::provenance::Classified]) {
        use std::io::Write;
        // Written to a file because the fuzz harness captures the test's stderr.
        let path = std::env::var("HYDRO_PROVENANCE_DUMP")
            .unwrap_or_else(|_| "target/provenance_dump.txt".to_string());
        let _ = std::fs::create_dir_all("target");
        let mut out = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        let _ = writeln!(out, "== {title}");
        for c in classified {
            let r = &c.record;
            let tags: Vec<String> = r
                .tags
                .iter()
                .map(|t| {
                    let k = match t.kind {
                        hydro_lang::sim::provenance::TagKind::Data => "D",
                        hydro_lang::sim::provenance::TagKind::Operational => "T",
                    };
                    format!("{k}{}.{}", t.port, t.seq)
                })
                .collect();
            let _ = writeln!(
                out,
                "{:<17} {:<10} {:>3}->{:<3} {:<32} {}{}B",
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

    fn labels(records: &[EmissionRecord]) -> BTreeMap<Label, usize> {
        let mut out = BTreeMap::new();
        for c in classify(records, false) {
            *out.entry(c.label).or_default() += 1;
        }
        out
    }

    /// Heartbeat: every network message descends from exactly one timer tick and no data.
    /// Expected shape: all `FixedOperational`, per-tick gain equal to cluster size, independent
    /// of anything else in the system (there is nothing else).
    #[test]
    fn heartbeat_is_fixed_operational() {
        const MEMBERS: usize = 3;
        const PULSES: usize = 3;

        let mut flow = FlowBuilder::new();
        let cluster = flow.cluster::<()>();
        let (timer_send, timer) = cluster.sim_input_operational::<(), TotalOrder, ExactlyOnce>();
        let received = pure_heartbeat(&cluster, timer)
            .entries()
            .sim_cluster_output();

        let executions = flow
            .sim()
            .with_cluster_size(&cluster, MEMBERS)
            .with_provenance()
            .exhaustive(async || {
                let _ = take_emissions(); // discard anything left by a previous instance
                for pulse in 0..PULSES {
                    for member in 0..MEMBERS as u32 {
                        timer_send.send(member, ());
                    }
                    quiesce().await;
                    let records = take_emissions();
                    let net = network_records(&records);
                    assert_eq!(
                        net.len(),
                        MEMBERS * MEMBERS,
                        "pulse {pulse}: one message per (sender, recipient)"
                    );
                    for r in &net {
                        assert_eq!(
                            r.data_tags().count(),
                            0,
                            "heartbeat carries no data lineage"
                        );
                        assert_eq!(
                            r.operational_tags().count(),
                            1,
                            "exactly one tick caused it"
                        );
                        assert!(!r.coarse);
                    }
                    let counts = labels(&net);
                    assert_eq!(
                        counts.get(&Label::FixedOperational),
                        Some(&(MEMBERS * MEMBERS))
                    );
                    assert_eq!(counts.len(), 1, "no other label: {counts:?}");
                    // Gain per stimulus is exactly the cluster size, in messages.
                    for ((_, op), a) in attribute(&net) {
                        assert!(op.is_some());
                        assert_eq!(a.messages, MEMBERS);
                        assert!(a.data_tags.is_empty());
                    }
                }
                for member in 0..MEMBERS as u32 {
                    let got: Vec<_> = received.collect_sorted(member).await;
                    assert_eq!(got.len(), PULSES * MEMBERS);
                }
            });
        assert!(executions >= 1);
    }

    /// Timeout/retry, driven through quiescence barriers so each stimulus is the sole cause of
    /// the work that follows it.
    ///
    /// Phase A: N requests arrive; the service pump never fires, so N requests sit in the
    /// service queue and N remain outstanding at the client. Everything crossing `requests` is
    /// `Productive` (first crossing of each request's lineage).
    ///
    /// Phase B: one retry tick. Every message it causes on `requests` carries an operational tag
    /// and only already-emitted data lineage: `Reactivated`, gain = N messages. This is the
    /// shape the old telemetry could not separate from queue drainage — and drainage is now
    /// impossible to confuse with it, because drainage would have happened in phase A.
    ///
    /// Phase C: one service tick completes one request; its response crosses `responses` as
    /// `Productive` (novel on that edge). A second retry tick then reactivates N-1.
    ///
    /// Varying N shows the gain is state-funded (linear in the backlog), unlike heartbeat.
    #[test]
    fn timeout_retry_is_reactivated_with_backlog_proportional_gain() {
        use crate::distributed::timeout_retry::{Request, timeout_retry_with_timers};

        for n in [1usize, 4, 9] {
            let mut flow = FlowBuilder::new();
            let client = flow.process();
            let service = flow.process();
            let (request_send, requests) = client.sim_input::<Request, TotalOrder, ExactlyOnce>();
            let (retry_send, retry_ticks) =
                client.sim_input_operational::<(), TotalOrder, ExactlyOnce>();
            let (service_send, service_ticks) =
                service.sim_input_operational::<(), TotalOrder, ExactlyOnce>();
            let outputs = timeout_retry_with_timers(
                &client,
                &service,
                requests,
                retry_ticks,
                service_ticks,
                0,
            );
            let completed = outputs
                .completed
                .assume_ordering::<TotalOrder>(nondet!(/** observation only */))
                .sim_output();
            outputs.events.for_each(q!(
                |_| {},
                commutative = manual_proof!(/** observation only */)
            ));

            // Lineage is exact under every schedule once phases are separated by quiescence
            // barriers, so a few random schedules suffice; exhaustive enumeration of batching
            // choices is exponential in N and adds nothing here.
            flow.sim()
                .with_provenance()
                .unit_test_fuzz_iterations(4)
                .fuzz(async || {
                    let _ = take_emissions();

                    // Phase A: ordinary input only.
                    for i in 0..n as u64 {
                        request_send.send(Request {
                            id: i,
                            value: format!("r{i}"),
                        });
                    }
                    quiesce().await;
                    let a = network_records(&take_emissions());
                    let a_requests: Vec<_> = a.iter().filter(|r| r.name == "requests").collect();
                    assert_eq!(a_requests.len(), n, "N={n}: each request sent once");
                    assert!(
                        a.iter().all(|r| r.name == "requests"),
                        "no responses yet: {a:?}"
                    );
                    let a_labels = labels(&a);
                    assert_eq!(a_labels.get(&Label::Productive), Some(&n), "{a_labels:?}");
                    assert_eq!(a_labels.len(), 1);

                    // Phase B: one retry tick, nothing else.
                    retry_send.send(());
                    quiesce().await;
                    let b = network_records(&take_emissions());
                    assert_eq!(
                        b.len(),
                        n,
                        "N={n}: retry re-sends every outstanding request"
                    );
                    for r in &b {
                        assert_eq!(r.name, "requests");
                        assert_eq!(r.operational_tags().count(), 1, "caused by the one tick");
                        assert!(r.data_tags().count() >= 1, "funded by retained requests");
                        assert!(r.coarse, "retry state is read through an opaque reference");
                    }
                    // Classification must be relative to the full history of the edge.
                    let mut history: Vec<EmissionRecord> = a.clone();
                    history.extend(b.iter().cloned());
                    let b_labels: BTreeMap<Label, usize> = classify(&history, false)
                        .into_iter()
                        .skip(a.len())
                        .fold(BTreeMap::new(), |mut m, c| {
                            *m.entry(c.label).or_default() += 1;
                            m
                        });
                    assert_eq!(b_labels.get(&Label::Reactivated), Some(&n), "{b_labels:?}");
                    assert_eq!(b_labels.len(), 1);
                    // Gain attributed to the single operational event is exactly the backlog.
                    let attr = attribute(&b);
                    assert_eq!(attr.len(), 1);
                    let (_, gain) = attr.into_iter().next().unwrap();
                    assert_eq!(gain.messages, n);
                    assert_eq!(
                        gain.data_tags.len(),
                        n,
                        "the tick converted all N retained requests"
                    );

                    // Phase C: one service pulse completes one request.
                    service_send.send(());
                    quiesce().await;
                    let c = network_records(&take_emissions());
                    let responses: Vec<_> = c.iter().filter(|r| r.name == "responses").collect();
                    assert_eq!(
                        responses.len(),
                        1,
                        "one pulse serves one queued request: {c:?}"
                    );
                    // The service pulse caused it; the queue is opaque state that also holds the
                    // retried copy, so coarse lineage may include the retry tick as well.
                    assert!(responses[0].operational_tags().count() >= 1);
                    assert!(responses[0].coarse);
                    history.extend(c.iter().cloned());
                    let last = classify(&history, false).pop().unwrap();
                    assert_eq!(
                        last.label,
                        Label::Productive,
                        "a response's lineage is novel on the responses edge"
                    );
                    let _one_completion = completed.next().await;

                    // Second retry tick: backlog shrank by one, and so does the gain.
                    retry_send.send(());
                    quiesce().await;
                    let d = network_records(&take_emissions());
                    let d_requests = d.iter().filter(|r| r.name == "requests").count();
                    assert_eq!(
                        d_requests,
                        n - 1,
                        "N={n}: recurrence tracks the remaining backlog"
                    );

                    if n == 4 {
                        history.extend(d.iter().cloned());
                        let all = classify(&history, false);
                        let (na, nb, nc) = (a.len(), b.len(), c.len());
                        dump("retry N=4, phase A: 4 requests, quiesce", &all[..na]);
                        dump("retry N=4, phase B: one retry tick, quiesce", &all[na..na + nb]);
                        dump(
                            "retry N=4, phase C: one service pulse, quiesce",
                            &all[na + nb..na + nb + nc],
                        );
                        dump(
                            "retry N=4, phase D: second retry tick, quiesce",
                            &all[na + nb + nc..],
                        );
                    }
                });
        }
    }

    /// G-Set gossip: a pump tick re-broadcasts the whole retained set. Messages per tick are
    /// fixed (one per peer); bytes and data lineage grow with the retained state.
    ///
    /// The *first* pump after the initial broadcast is labelled `Productive`: its lineage
    /// {D0..Dn} is a combination the recipient never received as a single emission (it got
    /// each element separately), and lineage alone cannot distinguish "a fold batched old
    /// tuples into a set" from "a join derived a new tuple" — both are novel combinations.
    /// What separates gossip from transitive closure is recurrence: the second pump re-emits
    /// identical lineage with no new data and is `Reactivated` on every channel, and would be
    /// forever. The cycle-level verdict therefore rests on recurrence after data has stopped,
    /// which is exactly the "repeatedly turns retained state back into work" in the definition.
    /// (History is per sender→recipient channel, not per operator, so the echo edge is judged
    /// against what the recipient already received on the initial-broadcast edge.)
    #[test]
    fn gossip_is_reactivated_with_state_proportional_bytes() {
        use std::collections::BTreeSet;

        use hydro_lang::live_collections::stream::NoOrder;
        use hydro_std::ec_inference_demos::crdt_gossip::g_set_gossip;

        const MEMBERS: usize = 3;
        let mut gains: Vec<(usize, usize, usize)> = Vec::new(); // (N, msgs, bytes) per pump

        for n in [2u32, 8] {
            let mut flow = FlowBuilder::new();
            let cluster = flow.cluster::<()>();
            let (update_send, updates) = cluster.sim_input::<u32, NoOrder, ExactlyOnce>();
            let (pump_send, pumps) = cluster.sim_input_operational::<(), TotalOrder, ExactlyOnce>();
            let state = g_set_gossip(&cluster, updates, pumps);
            state
                .sample_eager(nondet!(/** observation only */))
                .assume_ordering::<TotalOrder>(nondet!(/** observation only */))
                .for_each(q!(
                    |_| {},
                    idempotent = manual_proof!(/** observation only */)
                ));

            let gains_cell = std::sync::Mutex::new(Vec::new());
            flow.sim()
                .with_cluster_size(&cluster, MEMBERS)
                .skip_consistency_assertions()
                .with_provenance()
                .unit_test_fuzz_iterations(3)
                .fuzz(async || {
                    let _ = take_emissions();

                    // Phase A: member 0 learns N elements and broadcasts each once.
                    update_send.send_many_unordered((0..n).map(|v| (0u32, v)));
                    quiesce().await;
                    let a = network_records(&take_emissions());
                    let a_labels = labels(&a);
                    assert_eq!(a_labels.len(), 1, "{a_labels:?}");
                    assert_eq!(
                        a_labels.get(&Label::Productive),
                        Some(&(n as usize * MEMBERS)),
                        "N={n}: every update broadcast once to every member; got {a:#?}"
                    );

                    // Phase B: one pump at member 0 — full-state re-broadcast.
                    pump_send.send(0, ());
                    quiesce().await;
                    let b = network_records(&take_emissions());
                    assert_eq!(b.len(), MEMBERS, "one message per peer, independent of N");
                    let mut history = a.clone();
                    history.extend(b.iter().cloned());
                    let b_labels: BTreeMap<Label, usize> = classify(&history, false)
                        .into_iter()
                        .skip(a.len())
                        .fold(BTreeMap::new(), |mut m, c| {
                            *m.entry(c.label).or_default() += 1;
                            m
                        });
                    // First pump: a novel combination of separately-received elements.
                    assert_eq!(
                        b_labels.get(&Label::Productive),
                        Some(&MEMBERS),
                        "{b_labels:?}"
                    );
                    for r in &b {
                        assert_eq!(r.operational_tags().count(), 1, "caused by the pump tick");
                    }
                    let bytes: usize = b.iter().map(|r| r.bytes).sum();
                    let data: BTreeSet<_> = b.iter().flat_map(|r| r.data_tags().copied()).collect();
                    assert_eq!(
                        data.len(),
                        n as usize,
                        "the pump converted all N retained elements"
                    );
                    gains_cell
                        .lock()
                        .unwrap()
                        .push((n as usize, b.len(), bytes));

                    // Phase C: a second pump with unchanged state re-emits identical lineage —
                    // reactivation, and it would recur on every further pump.
                    pump_send.send(0, ());
                    quiesce().await;
                    let c = network_records(&take_emissions());
                    history.extend(c.iter().cloned());
                    let c_labels: BTreeMap<Label, usize> = classify(&history, false)
                        .into_iter()
                        .skip(a.len() + b.len())
                        .fold(BTreeMap::new(), |mut m, cl| {
                            *m.entry(cl.label).or_default() += 1;
                            m
                        });
                    assert_eq!(
                        c_labels.get(&Label::Reactivated),
                        Some(&MEMBERS),
                        "{c_labels:?}"
                    );
                });
            gains.extend(gains_cell.into_inner().unwrap());
        }

        // Messages per pump are constant in N; bytes grow with N.
        let small = gains.iter().find(|g| g.0 == 2).unwrap();
        let large = gains.iter().find(|g| g.0 == 8).unwrap();
        assert_eq!(small.1, large.1, "gossip message gain is fixed: {gains:?}");
        assert!(
            large.2 > small.2,
            "gossip byte gain is state-funded: {gains:?}"
        );
    }

    /// Semi-naive transitive closure with each edge admitted as its own input (so each edge is
    /// its own data tag) and steps as operational stimuli. Every fact leaving through the sim
    /// output carries a lineage no single earlier output dominated — a novel combination of
    /// edges — so every emission is `Productive`, even though many carry step (operational)
    /// tags and no individually-new edge tag. Once the frontier is exhausted a step causes no
    /// emission at all, and re-admitting the same graph is suppressed before the output.
    ///
    /// Admission is incremental (edge, step, edge, step, ...): the program's frontier is the
    /// current tick's novel facts, so an admission must be followed by a step before the next
    /// admission or its frontier is never joined. Steps continue after each admission until the
    /// frontier is exhausted.
    #[test]
    fn transitive_closure_output_is_productive_and_drains() {
        use crate::local::productive_tc::productive_transitive_closure;

        // Admitted in reverse topological order: the program joins only the current frontier
        // (left) against base edges (right), so an edge must be admitted after every edge that
        // extends it for the closure to be fully discovered incrementally.
        let edges: Vec<(u32, u32)> = vec![(2, 3), (1, 2), (0, 1), (0, 2)];
        let expected_facts = 6usize; // 23 12 13 01 02 03

        let mut flow = FlowBuilder::new();
        let process = flow.process::<()>();
        let (graph_send, graphs) = process.sim_input::<Vec<(u32, u32)>, TotalOrder, ExactlyOnce>();
        let (step_send, steps) = process.sim_input_operational::<(), TotalOrder, ExactlyOnce>();
        let (facts, traces) = productive_transitive_closure(graphs, steps);
        let fact_recv = facts
            .assume_ordering::<TotalOrder>(nondet!(/** facts compared as a set */))
            .sim_output();
        traces.for_each(q!(|_| {}));

        flow.sim()
            .with_provenance()
            .unit_test_fuzz_iterations(4)
            .fuzz(async || {
                let _ = take_emissions();
                let outputs = |records: Vec<EmissionRecord>| -> Vec<EmissionRecord> {
                    records
                        .into_iter()
                        .filter(|r| r.kind == EmissionPointKind::Output)
                        .collect()
                };
                let mut all: Vec<EmissionRecord> = Vec::new();
                // Incremental admission: each edge is its own source event, followed by a step
                // so its frontier is joined.
                let mut steps_taken = 0;
                for e in &edges {
                    graph_send.send(vec![*e]);
                    quiesce().await;
                    all.extend(outputs(take_emissions()));
                    // Step until a step causes no emission: the frontier is exhausted.
                    loop {
                        step_send.send(());
                        steps_taken += 1;
                        quiesce().await;
                        let produced = outputs(take_emissions());
                        if produced.is_empty() {
                            break;
                        }
                        all.extend(produced);
                        assert!(steps_taken < 20, "TC must drain; outputs so far: {all:?}");
                    }
                }
                assert_eq!(all.len(), expected_facts, "{all:?}");
                let classified = classify(&all, false);
                for c in &classified {
                    assert_eq!(c.label, Label::Productive, "{c:?}");
                    assert!(!c.record.coarse, "TC lineage is exact: {c:?}");
                }
                // Derived facts carry step (operational) lineage; base facts do not. Edge (0,2)
                // was derived (01 ⋈ 12) before it was admitted, so its admission is suppressed:
                // three base facts, three derived.
                let with_ops = classified
                    .iter()
                    .filter(|c| c.record.operational_tags().next().is_some())
                    .count();
                assert_eq!(with_ops, 3, "{classified:#?}");
                // A derived fact's lineage is a combination of edge tags.
                assert!(classified.iter().any(|c| c.record.data_tags().count() >= 2));

                // Re-admitting the graph produces no output: suppressed before emission.
                for e in &edges {
                    graph_send.send(vec![*e]);
                    quiesce().await;
                    step_send.send(());
                    quiesce().await;
                }
                let again = outputs(take_emissions());
                assert!(
                    again.is_empty(),
                    "known facts are not re-emitted: {again:?}"
                );

                let got: Vec<(u32, u32)> = fact_recv.collect_n(expected_facts).await;
                assert_eq!(got.len(), expected_facts);
            });
    }
}
