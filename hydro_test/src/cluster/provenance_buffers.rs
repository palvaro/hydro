//! Per-buffer admission table over the provenance ground-truth programs.
//!
//! The per-emission label ([`hydro_lang::sim::provenance::classify`]) is the primitive; the
//! deliverable of the provenance work is a decision *per buffer*, not a warning per program. A
//! buffer is anything that admits lineage and can hold it across time — a network edge feeding a
//! node's input, a cycle sink carrying state to the next tick. For each buffer,
//! [`hydro_lang::sim::provenance::buffer_table`] reads three facts straight off the emission log:
//!
//!   1. does it re-admit lineage it has already admitted, and only under an operational stimulus?
//!   2. does that re-admission grow with retained state, or stay fixed per stimulus?
//!   3. is a dedup gate on the path from the operational source? (structural, supplied here)
//!
//! The report's expected reading (§"Which buffers to bound" / §"Next step"): the retry service
//! queue is the one buffer that re-admits with backlog-proportional growth and no gate; the
//! transitive-closure frontier never re-admits; the gossip pump re-admits at fixed size. These
//! tests drive the three archetypes through quiescence barriers and assert exactly that, and dump
//! the whole table for the report.
//!
//! See `design_docs/reports/2026-09_provenance_feedback_cycle_classification.md`.

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::io::Write;

    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::provenance::{
        BufferAdmissions, EmissionPointKind, EmissionRecord, Gate, Growth, buffer_table,
        take_emissions,
    };
    use hydro_lang::sim::quiesce;

    /// Appends the per-buffer table to the shared dump file so it lands beside the classified
    /// logs the survey already writes.
    fn dump_table(title: &str, table: &[BufferAdmissions]) {
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
        let _ = writeln!(out, "== buffer table: {title}");
        let _ = writeln!(
            out,
            "{:<10} {:<12} {:>4}->{:<4} {:>4} {:>5} {:>5} {:>5}  {:<10} {:<8} {}",
            "kind",
            "name",
            "send",
            "recv",
            "adm",
            "novel",
            "react",
            "redun",
            "growth",
            "gate",
            "verdict"
        );
        for b in table {
            let verdict = if !b.re_admits() {
                "draining"
            } else if b.is_unbounded_reactivation() {
                "UNBOUNDED"
            } else if b.growth == Growth::Fixed {
                "fixed-rate"
            } else {
                "re-admits"
            };
            let _ = writeln!(
                out,
                "{:<10} {:<12} {:>4}->{:<4} {:>4} {:>5} {:>5} {:>5}  {:<10} {:<8} {}",
                format!("{:?}", b.id.kind),
                b.id.name,
                b.id.channel.0.map(|m| m.to_string()).unwrap_or("-".into()),
                b.id.channel.2.map(|m| m.to_string()).unwrap_or("-".into()),
                b.admissions,
                b.novel,
                b.reactivated,
                b.redundant,
                format!("{:?}", b.growth),
                format!("{:?}", b.gate),
                verdict
            );
        }
    }

    /// The retry service queue: the work-buffer archetype. Requests fan into the service over the
    /// `requests` edge; the service queue re-admits the whole backlog on every retry tick, growing
    /// with it, and there is no dedup gate on the request id before the queue (the client
    /// deduplicates *completions*, not requests). So `requests` must read as the one unbounded
    /// buffer: re-admits, `Growth::WithState`, `Gate::Absent`.
    #[test]
    fn retry_service_queue_is_the_unbounded_buffer() {
        use crate::distributed::timeout_retry::{Request, timeout_retry_with_timers};

        const N: usize = 4;
        let mut flow = FlowBuilder::new();
        let client = flow.process();
        let service = flow.process();
        let (request_send, requests) = client.sim_input::<Request, TotalOrder, ExactlyOnce>();
        let (retry_send, retry_ticks) =
            client.sim_input_operational::<(), TotalOrder, ExactlyOnce>();
        let (service_send, service_ticks) =
            service.sim_input_operational::<(), TotalOrder, ExactlyOnce>();
        let outputs =
            timeout_retry_with_timers(&client, &service, requests, retry_ticks, service_ticks, 0);
        let completed = outputs
            .completed
            .assume_ordering::<TotalOrder>(nondet!(/** observation only */))
            .sim_output();
        outputs.events.for_each(q!(
            |_| {},
            commutative = manual_proof!(/** observation only */)
        ));

        // `requests` has no dedup gate before the service queue; `responses` is emitted once per
        // completion but its lineage is coarse queue state, so it re-admits without a request-id
        // gate too — recorded, but the queue is the buffer we care about.
        let mut gates = BTreeMap::new();
        gates.insert("requests".to_string(), Gate::Absent);
        gates.insert("responses".to_string(), Gate::Absent);

        flow.sim()
            .with_provenance()
            .unit_test_fuzz_iterations(3)
            .fuzz(async || {
                let _ = take_emissions();
                // Grow the backlog across retry ticks: send 2 requests, retry (re-admits 2), send
                // 2 more, retry (re-admits 4). Successive re-admissions on `requests` carry more
                // lineage each time — the backlog-proportional growth the report calls out — so
                // the buffer reads `Growth::WithState`, unlike a fixed-set gossip pump.
                let mut history: Vec<EmissionRecord> = Vec::new();
                let mut next_id = 0u64;
                for _ in 0..2 {
                    for _ in 0..2 {
                        request_send.send(Request {
                            id: next_id,
                            value: format!("r{next_id}"),
                        });
                        next_id += 1;
                    }
                    quiesce().await;
                    history.extend(take_emissions());
                    retry_send.send(());
                    quiesce().await;
                    history.extend(take_emissions());
                }
                // Serve the whole queue: 4 originals + (2 + 4) retried duplicates = 10 items. One
                // pulse pops one item regardless of whether it is new, so drain all of them to be
                // sure every distinct request completes.
                for _ in 0..(N + 2 + 4) {
                    service_send.send(());
                    quiesce().await;
                    history.extend(take_emissions());
                }
                let _done: Vec<_> = completed.collect_n(N).await;

                let table = buffer_table(&history, &gates, false);
                dump_table("retry (N=4, growing backlog): work buffer", &table);

                // The request edge is the service queue's admission edge: it re-admits the backlog
                // under the retry timer, growing with retained state, with no gate.
                let req = table
                    .iter()
                    .find(|b| b.id.name == "requests" && b.id.kind == EmissionPointKind::Network)
                    .expect("a requests buffer exists");
                assert!(
                    req.re_admits(),
                    "the retry timer re-admits the backlog: {req:?}"
                );
                assert!(
                    req.re_admits_only_operationally(),
                    "re-admission is timer-driven, not data-driven: {req:?}"
                );
                assert_eq!(
                    req.growth,
                    Growth::WithState,
                    "the backlog re-admitted grows as requests accumulate: {req:?}"
                );
                assert!(
                    req.is_unbounded_reactivation(),
                    "the service queue is the collapse-candidate buffer: {req:?}"
                );
            });
    }

    /// The transitive-closure frontier: the fixpoint-buffer archetype. Every fact leaving the
    /// output carries a novel combination of edge lineages; the `anti_join` against `known` is the
    /// gate, so the frontier admits only lineage it has never admitted and drains. No buffer here
    /// re-admits, regardless of how many step stimuli fire.
    #[test]
    fn transitive_closure_frontier_never_re_admits() {
        use crate::local::productive_tc::productive_transitive_closure;

        let edges: Vec<(u32, u32)> = vec![(2, 3), (1, 2), (0, 1), (0, 2)];
        let expected_facts = 6usize;

        let mut flow = FlowBuilder::new();
        let process = flow.process::<()>();
        let (graph_send, graphs) = process.sim_input::<Vec<(u32, u32)>, TotalOrder, ExactlyOnce>();
        let (step_send, steps) = process.sim_input_operational::<(), TotalOrder, ExactlyOnce>();
        let (facts, traces) = productive_transitive_closure(graphs, steps);
        let fact_recv = facts
            .assume_ordering::<TotalOrder>(nondet!(/** facts compared as a set */))
            .sim_output();
        traces.for_each(q!(|_| {}));

        // The frontier cycle carries an `anti_join` gate against the known set.
        let mut gates = BTreeMap::new();
        gates.insert("output".to_string(), Gate::Present);

        flow.sim()
            .with_provenance()
            .unit_test_fuzz_iterations(3)
            .fuzz(async || {
                let _ = take_emissions();
                let mut history: Vec<EmissionRecord> = Vec::new();
                let mut steps_taken = 0;
                for e in &edges {
                    graph_send.send(vec![*e]);
                    quiesce().await;
                    history.extend(take_emissions());
                    loop {
                        step_send.send(());
                        steps_taken += 1;
                        quiesce().await;
                        let produced = take_emissions();
                        let progressed =
                            produced.iter().any(|r| r.kind == EmissionPointKind::Output);
                        history.extend(produced);
                        if !progressed {
                            break;
                        }
                        assert!(steps_taken < 20, "TC must drain");
                    }
                }
                let _got: Vec<(u32, u32)> = fact_recv.collect_n(expected_facts).await;

                // Network/output edges only: the frontier's *output* admits only novel
                // combinations. (The cycle-sink carry of the `known` accumulator re-emits its
                // whole retained set every tick by construction; that is the fixpoint working, not
                // physical re-admission, and is excluded — see `buffer_table`.)
                let table = buffer_table(&history, &gates, false);
                dump_table(
                    "transitive closure: fixpoint buffer (network/output)",
                    &table,
                );
                assert!(
                    table.iter().all(|b| !b.re_admits()),
                    "no TC buffer re-admits any lineage: {table:#?}"
                );
                assert!(
                    table.iter().all(|b| b.growth == Growth::None),
                    "nothing grows; the closure is bounded: {table:#?}"
                );
                assert!(
                    table.iter().all(|b| !b.is_unbounded_reactivation()),
                    "no collapse-candidate buffer in TC"
                );
            });
    }

    /// The gossip pump: fixed-size reactivation. A pump re-broadcasts the whole retained set to
    /// every peer; after the first (novel-combination) pump, every further pump re-admits the same
    /// lineage — `Growth::Fixed`, not backlog-proportional — so it is a rate-limit candidate, not
    /// an unbounded buffer, even though no dedup gate stops the re-broadcast.
    #[test]
    fn gossip_pump_re_admits_at_fixed_size() {
        use hydro_lang::live_collections::stream::NoOrder;
        use hydro_std::ec_inference_demos::crdt_gossip::g_set_gossip;

        const MEMBERS: usize = 3;
        const N: u32 = 4;

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

        // The gossip broadcast has no dedup gate against previously-broadcast state on the pump
        // path — a pump re-sends the whole set. The broadcast edges are auto-named by location
        // (no user channel name), so the gate is left Unknown here; the conclusion for gossip
        // rests on growth being fixed, not on the gate.
        let gates: BTreeMap<String, Gate> = BTreeMap::new();

        flow.sim()
            .with_cluster_size(&cluster, MEMBERS)
            .skip_consistency_assertions()
            .with_provenance()
            .unit_test_fuzz_iterations(2)
            .fuzz(async || {
                let _ = take_emissions();
                update_send.send_many_unordered((0..N).map(|v| (0u32, v)));
                quiesce().await;
                let mut history: Vec<EmissionRecord> = take_emissions();
                // Three pumps: the first is a novel combination, the next two re-admit the same
                // lineage — the shape that must read as fixed-size, not state-growing.
                for _ in 0..3 {
                    pump_send.send(0, ());
                    quiesce().await;
                    history.extend(take_emissions());
                }

                let table = buffer_table(&history, &gates, false);
                dump_table(
                    "gossip (N=4, 3 pumps from member 0): fixed-rate buffer",
                    &table,
                );

                // Member 0's outbound edges re-admit the whole set on every pump, at fixed size.
                let readmitting: Vec<_> = table
                    .iter()
                    .filter(|b| {
                        b.id.kind == EmissionPointKind::Network
                            && b.id.channel.0 == Some(0)
                            && b.re_admits()
                    })
                    .collect();
                assert!(
                    !readmitting.is_empty(),
                    "the pump re-admits retained state: {table:#?}"
                );
                for b in &readmitting {
                    assert!(
                        b.re_admits_only_operationally(),
                        "re-broadcast is pump-driven: {b:?}"
                    );
                    assert_eq!(
                        b.growth,
                        Growth::Fixed,
                        "each pump re-admits the same set, so growth is fixed: {b:?}"
                    );
                    assert!(
                        !b.is_unbounded_reactivation(),
                        "fixed-size re-admission is a rate-limit candidate, not unbounded: {b:?}"
                    );
                }
            });
    }
}
