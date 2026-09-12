//! Provenance probe of the catchup v2 client (see hydro's
//! `design_docs/reports/2026-09_provenance_feedback_cycle_classification.md`).
//!
//! Ticks and config are declared operational (a tick is the driver's clock; config is policy);
//! server status and slot data are application data. Phases are separated by quiescence so each
//! tick is the sole cause of the work that follows it.

use std::collections::BTreeMap;
use std::io::Write;

use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
use hydro_lang::sim::provenance::{EmissionPointKind, EmissionRecord, Label, classify, take_emissions};
use hydro_lang::sim::quiesce;

use super::state_machine::{RESET_REASON_NONE, RESULT_CODE_OK};
use super::*;

fn config_with_keepalive(gap_keepalive_ns: u64) -> ClientConfig {
    ClientConfig {
        open_timeout_ns: 1_000,
        stream_timeout_ns: 1_000,
        gap_keepalive_ns,
        requested_slots: DEFAULT_REQUESTED_SLOTS,
    }
}

fn tick(now_ns: u64, next_contiguous_slot: u64, selected_server: Option<u32>) -> ClientTick {
    ClientTick {
        now_ns,
        next_contiguous_slot,
        selected_server: selected_server.map(|server| (server, now_ns, now_ns)),
        catchup_needed: true,
        has_gaps: false,
    }
}

fn tick_with_gaps(now_ns: u64, next_contiguous_slot: u64) -> ClientTick {
    ClientTick {
        has_gaps: true,
        ..tick(now_ns, next_contiguous_slot, None)
    }
}

#[allow(clippy::type_complexity)]
fn sim_client_logic<'a>(
    ticks: Stream<ClientTick, Cluster<'a, Client>>,
    config: Stream<ClientConfig, Cluster<'a, Client>>,
    server_status: Stream<(MemberId<Server>, ServerStatusMsg), Cluster<'a, Client>, Unbounded>,
    slot_data: Stream<(MemberId<Server>, SlotDataMsg), Cluster<'a, Client>, Unbounded>,
) -> (
    Stream<(MemberId<Server>, ClientStatusMsg), Cluster<'a, Client>, Unbounded>,
    Stream<SlotDataMsg, Cluster<'a, Client>, Unbounded>,
) {
    let (client_status, accepted_slot_data) = client_logic(
        ticks,
        config,
        server_status.weaken_ordering::<NoOrder>(),
        slot_data.weaken_ordering::<NoOrder>(),
        nondet!(/** interleavings explored by the simulator */),
    );
    (
        client_status,
        accepted_slot_data.assume_ordering::<TotalOrder>(nondet!(/** receiver needs a total order */)),
    )
}

fn outputs(records: &[EmissionRecord]) -> Vec<EmissionRecord> {
    records.iter().filter(|r| r.kind == EmissionPointKind::Output).cloned().collect()
}

fn labels_from(history: &[EmissionRecord], from: usize) -> BTreeMap<String, BTreeMap<Label, usize>> {
    let before = history[..from]
        .iter()
        .filter(|r| !matches!(r.kind, EmissionPointKind::Receive | EmissionPointKind::Cycle))
        .count();
    let mut out: BTreeMap<String, BTreeMap<Label, usize>> = BTreeMap::new();
    for c in classify(history, false).into_iter().skip(before) {
        *out.entry(c.record.name.clone()).or_default().entry(c.label).or_default() += 1;
    }
    out
}

fn dump(title: &str, history: &[EmissionRecord]) {
    let _ = std::fs::create_dir_all("target");
    let Ok(mut out) = std::fs::OpenOptions::new().create(true).append(true).open("target/provenance_dump.txt") else { return };
    let _ = writeln!(out, "== {title}");
    for c in classify(history, false) {
        let r = &c.record;
        let tags: Vec<String> = r.tags.iter().map(|t| {
            let k = match t.kind { hydro_lang::sim::provenance::TagKind::Data => "D", hydro_lang::sim::provenance::TagKind::Operational => "T" };
            format!("{k}{}.{}", t.port, t.seq)
        }).collect();
        let _ = writeln!(out, "{:<17} {:<10} {:<48} {}{}B", format!("{:?}", c.label), r.name, format!("{{{}}}", tags.join(",")), if r.coarse { "coarse " } else { "" }, r.bytes);
    }
}

#[test]
fn catchup_client_timer_paths_under_provenance() {
    let mut flow = FlowBuilder::new();
    let client = flow.cluster::<Client>();

    let (tick_send, ticks) = client.sim_input_operational::<ClientTick, TotalOrder, ExactlyOnce>();
    let (config_send, config) = client.sim_input_operational::<ClientConfig, TotalOrder, ExactlyOnce>();
    let (status_send, server_status) =
        client.sim_input::<(MemberId<Server>, ServerStatusMsg), TotalOrder, ExactlyOnce>();
    let (slot_send, slot_data) = client.sim_input::<(MemberId<Server>, SlotDataMsg), TotalOrder, ExactlyOnce>();
    let (client_status, ingest) = sim_client_logic(ticks, config, server_status, slot_data);
    let status_recv = client_status.sim_cluster_output();
    let ingest_recv = ingest.sim_cluster_output();

    flow.sim()
        .with_cluster_size(&client, 1)
        .with_provenance()
        .unit_test_fuzz_iterations(3)
        .fuzz(async || {
            config_send.send(0, config_with_keepalive(100));
            quiesce().await;
            let mut history = take_emissions();

            // 1. Open: a tick with a selected server -> one open request.
            let m = history.len();
            tick_send.send(0, tick(100, 42, Some(3)));
            quiesce().await;
            history.extend(take_emissions());
            let open_labels = labels_from(&history, m);
            let (_, open) = status_recv.next(0).await;

            // 2. Open timeout (no server response) then reopen to another server.
            let m2 = history.len();
            tick_send.send(0, tick(1_101, 42, None));
            quiesce().await;
            tick_send.send(0, tick(1_102, 42, Some(4)));
            quiesce().await;
            history.extend(take_emissions());
            let reopen_labels = labels_from(&history, m2);
            let (_, open2) = status_recv.next(0).await;

            // 3. Accept, then two slot bursts leaving a hole at 142.
            let m3 = history.len();
            status_send.send(0, (MemberId::from_raw_id(4), ServerStatusMsg {
                stream_id_hi: open2.stream_id_hi, stream_id_lo: open2.stream_id_lo,
                actual_start_slot_index: 42, result_code: RESULT_CODE_OK, reset_reason: RESET_REASON_NONE,
            }));
            quiesce().await;
            for (first, ts) in [(42u64, 1_150u64), (143, 1_200)] {
                slot_send.send(0, (MemberId::from_raw_id(4), SlotDataMsg {
                    stream_id_hi: open2.stream_id_hi, stream_id_lo: open2.stream_id_lo,
                    first_slot_index: first, slot_count: 100, slot_payload_len: 100 * SLOT_SIZE, received_timestamp: ts,
                }));
                quiesce().await;
                let _ = ingest_recv.next(0).await;
            }
            history.extend(take_emissions());
            let data_labels = labels_from(&history, m3);

            // 4. Gap keepalive: stalled stream, gap-reporting ticks past the interval.
            let m4 = history.len();
            let mut keepalives = Vec::new();
            for now in [1_300u64, 1_450, 1_600] {
                tick_send.send(0, tick_with_gaps(now, 142));
                quiesce().await;
                let sent = take_emissions();
                let n = outputs(&sent).iter().filter(|r| r.name != "output 1").count();
                keepalives.push(n);
                history.extend(sent);
                let (_, ka) = status_recv.next(0).await;
                assert_eq!(ka.last_contiguous_slot_index, 142);
            }
            let keepalive_labels = labels_from(&history, m4);

            dump("microbus catchup client: open, timeout/reopen, accept+slots, 3 gap keepalives", &history);
            let _ = std::fs::create_dir_all("target");
            if let Ok(mut out) = std::fs::OpenOptions::new().create(true).append(true).open("target/provenance_dump.txt") {
                let _ = writeln!(out, "## open {open_labels:?}; reopen {reopen_labels:?}; accept+slots {data_labels:?}; keepalives per tick {keepalives:?} {keepalive_labels:?}");
            }
            let _ = open;
        });
}
