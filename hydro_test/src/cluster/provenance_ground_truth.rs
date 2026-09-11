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
                    if t.member == u32::MAX {
                        format!("{k}{}.{}", t.port, t.seq)
                    } else {
                        format!("{k}{}@{}.{}", t.port, t.member, t.seq)
                    }
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
                        dump(
                            "retry N=4, phase B: one retry tick, quiesce",
                            &all[na..na + nb],
                        );
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

    /// Appends a free-form line to the dump file.
    fn dump_note(note: &str) {
        use std::io::Write;
        let path = std::env::var("HYDRO_PROVENANCE_DUMP")
            .unwrap_or_else(|_| "target/provenance_dump.txt".to_string());
        let _ = std::fs::create_dir_all("target");
        if let Ok(mut out) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
        {
            let _ = writeln!(out, "## {note}");
        }
    }

    /// Loop gain: does reactivated *input* at a node cause wasted *output* from it?
    ///
    /// Retry, N=4 outstanding, k retry ticks before the service is allowed to drain. Each tick
    /// puts N duplicates in the service queue; the service pops one item per pulse regardless,
    /// so draining takes N(k+1) pulses. Measured on the `responses` channel:
    ///
    /// - physical emissions = N(k+1): every queued item, original or duplicate, costs a pulse;
    /// - distinct payloads = N: only N responses carry new information.
    ///
    /// Waste = N·k grows with the number of reactivating inputs: the closed loop.
    ///
    /// This is measured by content, not by lineage label, because the service's queue is opaque
    /// `by_mut` state: every response inherits the whole queue's lineage (coarse), so after the
    /// first response all of them — originals included — are dominated and labelled
    /// `Reactivated`. That is recorded here as a known limit of lineage downstream of opaque
    /// state; the dump shows it.
    #[test]
    fn timeout_retry_loop_gain_grows_with_retry_ticks() {
        use std::collections::BTreeSet;

        use crate::distributed::timeout_retry::{Request, timeout_retry_with_timers};

        const N: usize = 4;
        // (k, physical responses, distinct payloads, reactivated-labelled, productive-labelled)
        let observed = std::sync::Mutex::new(Vec::<(usize, usize, usize, usize, usize)>::new());

        for k in 1usize..=3 {
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
            // The program's own deduplicated completions: the logical-progress oracle.
            let completed = outputs
                .completed
                .assume_ordering::<TotalOrder>(nondet!(/** observation only */))
                .sim_output();
            outputs.events.for_each(q!(
                |_| {},
                commutative = manual_proof!(/** observation only */)
            ));

            flow.sim()
                .with_provenance()
                .unit_test_fuzz_iterations(3)
                .fuzz(async || {
                    for i in 0..N as u64 {
                        request_send.send(Request {
                            id: i,
                            value: format!("r{i}"),
                        });
                    }
                    quiesce().await;
                    let mut history = network_records(&take_emissions());
                    for _ in 0..k {
                        retry_send.send(());
                        quiesce().await;
                        history.extend(network_records(&take_emissions()));
                    }
                    // Drain: the queue holds N originals + N*k duplicates.
                    for _ in 0..N * (k + 1) {
                        service_send.send(());
                        quiesce().await;
                    }
                    let drained = network_records(&take_emissions());
                    let before = history.len();
                    history.extend(drained.iter().cloned());

                    let responses: Vec<_> = drained.iter().filter(|r| r.name == "responses").collect();
                    let distinct: BTreeSet<u64> = responses.iter().map(|r| r.payload_hash).collect();
                    assert_eq!(responses.len(), N * (k + 1), "k={k}: every queued item costs a pulse");
                    assert_eq!(distinct.len(), N, "k={k}: only N distinct responses exist");
                    // The program agrees: its deduplicated output has exactly N completions.
                    let done: Vec<_> = completed.collect_n(N).await;
                    assert_eq!(done.len(), N);

                    let labelled: Vec<_> = classify(&history, false)
                        .into_iter()
                        .skip(before)
                        .filter(|c| c.record.name == "responses")
                        .collect();
                    let reactivated = labelled.iter().filter(|c| c.label == Label::Reactivated).count();
                    let productive = labelled.iter().filter(|c| c.label == Label::Productive).count();
                    assert!(labelled.iter().all(|c| c.record.coarse), "responses inherit opaque queue state");
                    observed
                        .lock()
                        .unwrap()
                        .push((k, responses.len(), distinct.len(), reactivated, productive));
                    if k == 2 {
                        dump(
                            "retry loop gain, N=4, k=2: drain phase (labels are coarse: see test doc)",
                            &classify(&history, false)[before..],
                        );
                    }
                });
        }
        let observed = observed.into_inner().unwrap();
        dump_note(&format!(
            "retry loop gain observations (k, physical responses, distinct payloads, reactivated-labelled, productive-labelled): {observed:?}"
        ));
        for (k, physical, distinct, _, _) in &observed {
            assert_eq!(
                physical - distinct,
                N * k,
                "waste grows with k: {observed:?}"
            );
        }
    }

    /// The same measurement on gossip, as an *observation* — gossip has no ground truth label.
    /// N updates at member 0, k pumps at member 0, then one pump at member 1: how many
    /// emissions does member 1 produce, and does that depend on k? Recorded, not asserted as a
    /// class.
    #[test]
    fn gossip_loop_gain_measurement() {
        use hydro_lang::live_collections::stream::NoOrder;
        use hydro_std::ec_inference_demos::crdt_gossip::g_set_gossip;

        const MEMBERS: usize = 3;
        const N: u32 = 4;
        let observed = std::sync::Mutex::new(Vec::<(usize, usize, usize, usize)>::new()); // (k, member1 msgs, reactivated, productive)

        for k in [0usize, 1, 3] {
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

            flow.sim()
                .with_cluster_size(&cluster, MEMBERS)
                .skip_consistency_assertions()
                .with_provenance()
                .unit_test_fuzz_iterations(2)
                .fuzz(async || {
                    update_send.send_many_unordered((0..N).map(|v| (0u32, v)));
                    quiesce().await;
                    let mut history = network_records(&take_emissions());
                    for _ in 0..k {
                        pump_send.send(0, ());
                        quiesce().await;
                        history.extend(network_records(&take_emissions()));
                    }
                    pump_send.send(1, ());
                    quiesce().await;
                    let m1 = network_records(&take_emissions());
                    let before = history.len();
                    history.extend(m1.iter().cloned());
                    let out: Vec<_> = classify(&history, false)
                        .into_iter()
                        .skip(before)
                        .filter(|c| c.record.member == Some(1))
                        .collect();
                    let reactivated = out.iter().filter(|c| c.label == Label::Reactivated).count();
                    let productive = out.iter().filter(|c| c.label == Label::Productive).count();
                    observed
                        .lock()
                        .unwrap()
                        .push((k, out.len(), reactivated, productive));
                    dump(
                        &format!("gossip loop gain, N=4, k={k}: member 1 pumps once"),
                        &classify(&history, false)[before..],
                    );
                });
        }
        let observed = observed.into_inner().unwrap();
        // Recorded for the report; the only thing checked is that the measurement is
        // schedule-independent (same numbers across fuzz iterations for a given k).
        for k in [0usize, 1, 3] {
            let rows: Vec<_> = observed.iter().filter(|o| o.0 == k).collect();
            assert!(rows.windows(2).all(|w| w[0] == w[1]), "{observed:?}");
        }
        dump_note(&format!(
            "gossip loop gain observations (k, member1 msgs, reactivated, productive): {observed:?}"
        ));
    }

    /// Raft (`raft_server`, timers already threaded as inputs). Barrier-separated:
    ///
    /// 1. election timer at member 0 -> RequestVote to the other two members. No requests have
    ///    arrived, so even coarse lineage holds no data: `FixedOperational`.
    /// 2. N requests at the leader -> nothing is sent (they only append to the log).
    /// 3. heartbeat at the leader -> AppendEntries carrying the N-entry suffix to each follower:
    ///    `Productive`, bytes grow with N. Followers ack, entries commit.
    /// 4. heartbeat again -> AppendEntries with an empty suffix: bytes do not grow with N. The
    ///    label is coarse (`by_mut` server state) and recorded, not asserted.
    ///
    /// Raft's replication is semi-naive by construction (`next_index`), so a heartbeat re-sends
    /// only what a follower has not acknowledged; with fail-stop networking every follower acks
    /// and the steady-state heartbeat is fixed-size work.
    #[test]
    fn raft_election_is_fixed_and_replication_is_productive() {
        use std::collections::BTreeMap as Map;

        use crate::cluster::raft::{RaftConfig, Replica, raft_server};

        const MEMBERS: usize = 3;
        // (N, vote-request msgs, hb1 msgs, hb1 bytes, hb2 msgs, hb2 bytes, hb2 labels)
        type Row = (usize, usize, usize, usize, usize, usize, String);
        let observed = std::sync::Mutex::new(Vec::<Row>::new());

        for n in [1usize, 5] {
            let mut flow = FlowBuilder::new();
            let cluster = flow.cluster::<Replica>();
            let (election_send, election_ticks) =
                cluster.sim_input_operational::<(), TotalOrder, ExactlyOnce>();
            let (heartbeat_send, heartbeat_ticks) =
                cluster.sim_input_operational::<(), TotalOrder, ExactlyOnce>();
            let (request_send, requests) = cluster.sim_input::<String, TotalOrder, ExactlyOnce>();
            let outputs = raft_server(
                &cluster,
                requests,
                election_ticks,
                heartbeat_ticks,
                RaftConfig {
                    cluster_size: MEMBERS,
                },
                TCP.fail_stop().bincode(),
                nondet!(/** only member 0's election timer fires, so the outcome is fixed */),
            );
            let committed = outputs.committed.end_atomic().sim_cluster_output();
            outputs.redirected.for_each(q!(|_| {}));
            outputs.leader_views.for_each(q!(|_| {}));

            flow.sim()
                .skip_consistency_assertions()
                .with_cluster_size(&cluster, MEMBERS)
                .with_provenance()
                .unit_test_fuzz_iterations(3)
                .fuzz(async || {
                    // 1. Election.
                    election_send.send(0, ());
                    quiesce().await;
                    let election = network_records(&take_emissions());
                    let mut history = election.clone();
                    let votes: Vec<_> = election
                        .iter()
                        .filter(|r| r.member == Some(0) && r.data_tags().count() == 0)
                        .collect();
                    assert!(votes.len() >= MEMBERS - 1, "leader asked every peer: {election:#?}");
                    let election_labels = labels(&election);
                    assert_eq!(
                        election_labels.get(&Label::FixedOperational),
                        Some(&election.len()),
                        "no data exists yet, so all election traffic is fixed-operational: {election_labels:?}"
                    );

                    // 2. Requests at the leader: retained, not sent.
                    for i in 0..n {
                        request_send.send(0, format!("cmd{i}"));
                    }
                    quiesce().await;
                    let after_requests = network_records(&take_emissions());
                    assert!(
                        after_requests.is_empty(),
                        "requests only append to the log: {after_requests:?}"
                    );

                    // 3. Heartbeat: replication of the retained suffix.
                    heartbeat_send.send(0, ());
                    quiesce().await;
                    let hb1 = network_records(&take_emissions());
                    let hb1_sends: Vec<_> = hb1.iter().filter(|r| r.member == Some(0)).collect();
                    assert_eq!(
                        hb1_sends.len(),
                        MEMBERS - 1,
                        "one AppendEntries per follower: {hb1:#?}"
                    );
                    for r in &hb1_sends {
                        assert!(r.data_tags().count() >= n, "carries the N retained entries' lineage");
                        assert!(r.operational_tags().count() >= 1, "caused by the heartbeat");
                    }
                    let before = history.len();
                    history.extend(hb1.iter().cloned());
                    let hb1_labels: Map<Label, usize> = classify(&history, false)
                        .into_iter()
                        .skip(before)
                        .filter(|c| c.record.member == Some(0))
                        .fold(Map::new(), |mut m, c| {
                            *m.entry(c.label).or_default() += 1;
                            m
                        });
                    assert_eq!(
                        hb1_labels.get(&Label::Productive),
                        Some(&(MEMBERS - 1)),
                        "{hb1_labels:?}"
                    );
                    let hb1_bytes: usize = hb1_sends.iter().map(|r| r.bytes).sum();
                    // Acks arrive within the same quiescent phase, so the leader commits now;
                    // followers learn `leader_commit` from the next AppendEntries.
                    let mut got = 0;
                    while committed.try_next(0).await.is_some() {
                        got += 1;
                    }
                    assert_eq!(got, n, "leader committed all N entries");

                    // 4. Heartbeat with nothing to replicate.
                    heartbeat_send.send(0, ());
                    quiesce().await;
                    let hb2 = network_records(&take_emissions());
                    let hb2_sends: Vec<_> = hb2.iter().filter(|r| r.member == Some(0)).collect();
                    assert_eq!(hb2_sends.len(), MEMBERS - 1);
                    let hb2_bytes: usize = hb2_sends.iter().map(|r| r.bytes).sum();
                    assert!(hb2_bytes < hb1_bytes, "empty suffix is smaller than the replication");
                    for member in 1..MEMBERS as u32 {
                        let mut got = 0;
                        while committed.try_next(member).await.is_some() {
                            got += 1;
                        }
                        assert_eq!(got, n, "follower {member} committed all N entries");
                    }
                    let before = history.len();
                    history.extend(hb2.iter().cloned());
                    let hb2_labels: Map<Label, usize> = classify(&history, false)
                        .into_iter()
                        .skip(before)
                        .filter(|c| c.record.member == Some(0))
                        .fold(Map::new(), |mut m, c| {
                            *m.entry(c.label).or_default() += 1;
                            m
                        });
                    observed.lock().unwrap().push((
                        n,
                        votes.len(),
                        hb1_sends.len(),
                        hb1_bytes,
                        hb2_sends.len(),
                        hb2_bytes,
                        format!("{hb2_labels:?}"),
                    ));
                    if n == 5 {
                        let all = classify(&history, false);
                        dump(
                            "raft N=5: election, replication heartbeat, empty heartbeat (leader's sends only)",
                            &all
                                .into_iter()
                                .filter(|c| c.record.member == Some(0))
                                .collect::<Vec<_>>(),
                        );
                    }
                });
        }
        let observed = observed.into_inner().unwrap();
        dump_note(&format!(
            "raft observations (N, vote msgs, hb1 msgs, hb1 bytes, hb2 msgs, hb2 bytes, hb2 labels): {observed:?}"
        ));
        let small = observed.iter().find(|o| o.0 == 1).unwrap();
        let large = observed.iter().find(|o| o.0 == 5).unwrap();
        assert_eq!(small.2, large.2, "replication message count is per follower, not per entry");
        assert!(large.3 > small.3, "replication bytes grow with the retained log");
        assert_eq!(small.5, large.5, "steady-state heartbeat bytes do not grow with the log");
    }

    /// Retry under *request* loss: the service discards the first arrival of every odd-id
    /// request, so with N=4 the retries of requests 1 and 3 are *necessary* — they are the first
    /// copies the service ever acts on — and all four requests complete only because of them.
    ///
    /// The classifier labels all four retries `Reactivated` anyway, and that is the intended
    /// answer. The labels describe mechanism, not utility: a timer fired and the client re-sent
    /// retained state, exactly as it would have had nothing been lost. The client cannot tell a
    /// lost copy from a slow one, and that blindness is what makes the loop amplify under
    /// overload. Whether a particular re-send turned out to be useful is a property of the
    /// scenario, not of the code, and the vulnerability analysis must be invariant to it.
    #[test]
    fn timeout_retry_under_request_loss_is_still_reactivated() {
        use crate::distributed::timeout_retry::{
            LossPolicy, Request, timeout_retry_lossy_with_timers,
        };

        const N: usize = 4;
        let mut flow = FlowBuilder::new();
        let client = flow.process();
        let service = flow.process();
        let (request_send, requests) = client.sim_input::<Request, TotalOrder, ExactlyOnce>();
        let (retry_send, retry_ticks) = client.sim_input_operational::<(), TotalOrder, ExactlyOnce>();
        let (service_send, service_ticks) =
            service.sim_input_operational::<(), TotalOrder, ExactlyOnce>();
        let outputs = timeout_retry_lossy_with_timers(
            &client,
            &service,
            requests,
            retry_ticks,
            service_ticks,
            0,
            LossPolicy {
                drop_first_odd_request: true,
                black_hole_odd_responses: false,
            },
        );
        let completed = outputs
            .completed
            .assume_ordering::<TotalOrder>(nondet!(/** observation only */))
            .sim_output();
        outputs.events.for_each(q!(
            |_| {},
            commutative = manual_proof!(/** observation only */)
        ));

        flow.sim()
            .with_provenance()
            .unit_test_fuzz_iterations(3)
            .fuzz(async || {
                for i in 0..N as u64 {
                    request_send.send(Request {
                        id: i,
                        value: format!("r{i}"),
                    });
                }
                quiesce().await;
                let mut history = take_emissions();
                let a_sends = history.iter().filter(|r| r.kind == EmissionPointKind::Network).count();
                assert_eq!(a_sends, N);

                retry_send.send(());
                quiesce().await;
                let b = take_emissions();
                let before = history.len();
                history.extend(b.iter().cloned());
                let retries: Vec<_> = classify(&history, false)
                    .into_iter()
                    .skip_while(|c| c.record.kind == EmissionPointKind::Receive)
                    .filter(|c| c.record.kind == EmissionPointKind::Network && c.record.name == "requests")
                    .skip(N)
                    .collect();
                assert_eq!(retries.len(), N, "one retry per outstanding request");
                let reactivated = retries.iter().filter(|c| c.label == Label::Reactivated).count();
                assert_eq!(reactivated, N, "{:?}", retries.iter().map(|c| c.label).collect::<Vec<_>>());
                let _ = before;

                // Half of those retries were necessary: only after them can all N complete. The
                // label does not, and should not, change because of it.
                for _ in 0..(N + N / 2) {
                    service_send.send(());
                    quiesce().await;
                }
                let done: Vec<_> = completed.collect_n(N).await;
                assert_eq!(done.len(), N, "all requests eventually complete under request loss");
                dump_note("request loss: 4 sent, 2 discarded by the service; all 4 retries labelled Reactivated (mechanism, not utility); all 4 complete only after the retries");
            });
    }

    /// Retry under a *response black hole* (ground truth for sustained, futile reactivation): the
    /// client discards every response to an odd-id request, so requests 1 and 3 never complete.
    /// After the even ones complete, every retry tick re-sends exactly the two odd requests,
    /// each costs the service a pulse, and each response is discarded again. Per tick: 2
    /// `Reactivated` on `requests`; the service's work never delivers anything new. Repeating k
    /// times gives 2k wasted requests and 2k wasted responses: unbounded, never drains.
    #[test]
    fn timeout_retry_black_hole_recurs_without_bound() {
        use std::collections::BTreeSet;

        use crate::distributed::timeout_retry::{
            LossPolicy, Request, timeout_retry_lossy_with_timers,
        };

        const N: usize = 4;
        const K: usize = 3;
        let mut flow = FlowBuilder::new();
        let client = flow.process();
        let service = flow.process();
        let (request_send, requests) = client.sim_input::<Request, TotalOrder, ExactlyOnce>();
        let (retry_send, retry_ticks) = client.sim_input_operational::<(), TotalOrder, ExactlyOnce>();
        let (service_send, service_ticks) =
            service.sim_input_operational::<(), TotalOrder, ExactlyOnce>();
        let outputs = timeout_retry_lossy_with_timers(
            &client,
            &service,
            requests,
            retry_ticks,
            service_ticks,
            0,
            LossPolicy {
                drop_first_odd_request: false,
                black_hole_odd_responses: true,
            },
        );
        let completed = outputs
            .completed
            .assume_ordering::<TotalOrder>(nondet!(/** observation only */))
            .sim_output();
        outputs.events.for_each(q!(
            |_| {},
            commutative = manual_proof!(/** observation only */)
        ));

        flow.sim()
            .with_provenance()
            .unit_test_fuzz_iterations(3)
            .fuzz(async || {
                for i in 0..N as u64 {
                    request_send.send(Request {
                        id: i,
                        value: format!("r{i}"),
                    });
                }
                quiesce().await;
                let mut history = take_emissions();
                // Serve everything once: even requests complete, odd responses vanish.
                for _ in 0..N {
                    service_send.send(());
                    quiesce().await;
                }
                history.extend(take_emissions());
                let done: Vec<_> = completed.collect_n(N / 2).await;
                assert_eq!(done.len(), N / 2);

                // Steady state: each retry tick re-sends exactly the black-holed requests; each
                // service pulse spent on them produces a response nobody keeps.
                let mut per_tick = Vec::new();
                for k in 0..K {
                    retry_send.send(());
                    quiesce().await;
                    let sent = take_emissions();
                    let requests_sent: Vec<_> = sent
                        .iter()
                        .filter(|r| r.kind == EmissionPointKind::Network && r.name == "requests")
                        .cloned()
                        .collect();
                    assert_eq!(requests_sent.len(), N / 2, "tick {k}: only the black-holed requests");
                    let before = history.len();
                    history.extend(sent.iter().cloned());
                    let labels: Vec<Label> = classify(&history, false)
                        .into_iter()
                        .skip_while(|c| c.record.kind == EmissionPointKind::Receive)
                        .filter(|c| c.record.kind == EmissionPointKind::Network && c.record.name == "requests")
                        .skip(N + k * (N / 2))
                        .map(|c| c.label)
                        .collect();
                    assert_eq!(labels, vec![Label::Reactivated; N / 2], "tick {k}");
                    let _ = before;

                    for _ in 0..N / 2 {
                        service_send.send(());
                        quiesce().await;
                    }
                    let served = take_emissions();
                    let responses: Vec<_> = served
                        .iter()
                        .filter(|r| r.kind == EmissionPointKind::Network && r.name == "responses")
                        .cloned()
                        .collect();
                    assert_eq!(responses.len(), N / 2, "tick {k}: each retry cost a pulse");
                    let distinct: BTreeSet<u64> = responses.iter().map(|r| r.payload_hash).collect();
                    history.extend(served.iter().cloned());
                    per_tick.push((requests_sent.len(), responses.len(), distinct.len()));
                }
                // Nothing further ever completes.
                assert!(completed.try_next().await.is_none(), "black-holed requests never complete");
                assert_eq!(per_tick, vec![(N / 2, N / 2, N / 2); K], "constant per-tick waste, forever");
                dump_note(&format!(
                    "black hole: N=4, after serving once, {K} retry ticks each re-send {} requests (all Reactivated) and cost {} pulses whose responses are discarded: {per_tick:?}",
                    N / 2, N / 2
                ));
            });
    }
}
