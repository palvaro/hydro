//! Controls for the sweep (`hydro_lang::sim::sweep`) over the rest of the corpus
//! (`design_docs/2026-09_amplification_as_adversarial_scheduling.md`, "Next: the sweep"): the
//! programs nobody has pointed the instrument at, expected to come back "no moves" or bounded.
//! Each harness takes its fixed inputs from the program's existing sim test (the thunk, minus
//! the `fuzz`/`exhaustive` wrapper and the mid-run assertions), declares which edges are clocks
//! (none of these programs has a timer, so they are single-shot: no logical time, no shape),
//! and names the goal a record's payload carries. Any sustained loop here is a finding.

use std::collections::BTreeMap;

use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
use hydro_lang::location::MemberId;
use hydro_lang::prelude::*;
use hydro_lang::sim::hold_schedule::EdgePolicy;
use hydro_lang::sim::quiesce;
use hydro_lang::sim::sweep::{Gain, InputShape, Options, Program, Report, Run, clock, grid};

use super::quorum::Ts;

fn ds() -> Vec<u64> {
    std::env::var("SWEEP_DS")
        .ok()
        .map(|s| s.split(',').map(|d| d.parse().unwrap()).collect())
        .unwrap_or_else(|| grid(16))
}

fn options() -> Options {
    Options { ds: ds(), refine: true, verbose: true, empty_ticks: true }
}

/// Fails unless every swept edge came back without a finding; prints the findings otherwise.
fn assert_no_moves<G: Ord + Clone + std::fmt::Debug>(report: &Report<G>) {
    let findings: Vec<String> = report
        .findings()
        .iter()
        .map(|s| format!("[{}] {:?} threshold {:?} verdict {:?}", s.edge, s.shape, s.threshold, s.verdict))
        .collect();
    assert!(findings.is_empty(), "{}: findings where none were expected:\n{}", report.program, findings.join("\n"));
}

/// Uniform reliable broadcast: 20 messages to 3 members, threshold 2. The echo cycle is
/// top-level (no hooks); the only edge is the certificate mint's attestation batch. Hand
/// computation: every message collects one attestation per member at every member,
/// `MESSAGES * N * N` records in total, under every schedule; a hold delays delivery only.
#[test]
fn uniform_broadcast_has_no_moves() {
    const N: usize = 3;
    const THRESHOLD: usize = 2;
    const MESSAGES: u32 = 20;

    let program = Program::<u32> {
        name: "uniform_broadcast".to_owned(),
        inputs: InputShape::UpFront,
        run: Box::new(|driver| {
            let mut flow = FlowBuilder::new();
            let sender = flow.process::<()>();
            let cluster = flow.cluster::<()>();
            let (in_send, data) = sender.sim_input::<u32, TotalOrder, ExactlyOnce>();
            let out_recv = super::uniform_broadcast::uniform_reliable_broadcast_closed(data, &cluster, THRESHOLD).sim_cluster_output();
            let mut delivered = vec![0usize; N];
            let delivered_ref = &mut delivered;
            let (counts, lineage) = flow
                .sim()
                .skip_consistency_assertions()
                .with_cluster_size(&cluster, N)
                .run_traced(driver, async move || {
                    for m in 0..MESSAGES {
                        in_send.send(m);
                    }
                    quiesce().await;
                    for member in 0..N as u32 {
                        delivered_ref[member as usize] = out_recv.collect_sorted::<Vec<u32>>(member).await.len();
                    }
                });
            let mut progress = BTreeMap::new();
            progress.insert("delivered".to_owned(), delivered.iter().sum::<usize>() as i64);
            Run { counts, lineage, progress }
        }),
        clocks: vec![],
        time: None,
        goals: Box::new(|_, record| vec![record.decode::<(u32, MemberId<()>)>().unwrap().0]),
        reached: None,
    };
    let report = program.sweep(&options());
    assert_eq!(report.prompt.derivations(), (MESSAGES as usize * N * N) as u64);
    assert_eq!(report.prompt.progress["delivered"], (MESSAGES as usize * N) as i64);
    for s in &report.sweeps {
        for c in &s.cells {
            assert_eq!(c.measure.derivations(), report.prompt.derivations(), "[{}] {:?} d = {}", s.edge, s.shape, c.d);
            assert_eq!(c.measure.progress["delivered"], report.prompt.progress["delivered"], "[{}] {:?} d = {}", s.edge, s.shape, c.d);
        }
    }
    assert_no_moves(&report);
}

/// Synod, sole proposer: one proposal to 3 acceptors. Four edges (prepares and accepts at the
/// acceptors, promises and acks at the proposer's mints). Hand computation: 3 prepares, 3
/// promises, 3 accepts, 3 acks, one chosen certificate, under every schedule.
#[test]
fn synod_sole_proposer_has_no_moves() {
    let report = synod_program(vec![(0, (1, 42u32))], 1).sweep(&options());
    assert_eq!(report.prompt.derivations(), 12);
    assert_eq!(report.prompt.progress["chosen"], 1);
    for s in &report.sweeps {
        for c in &s.cells {
            assert_eq!(c.measure.derivations(), 12, "[{}] {:?} d = {}", s.edge, s.shape, c.d);
        }
    }
    assert_no_moves(&report);
}

/// Synod, dueling proposers (rounds 1 and 2 for values 10 and 20): the election theorem. A hold
/// can change which prepares reach the acceptors first, so a lower ballot may be refused (fewer
/// accepts/acks) or both may complete; the work is bounded by the two proposals' 24 records and
/// agreement must hold under every schedule. Recorded, not a loop: no retry exists in `synod`
/// ("the caller owns ballot escalation").
#[test]
fn synod_duel_is_bounded() {
    let report = synod_program(vec![(0, (1, 10u32)), (1, (2, 20u32))], 2).sweep(&options());
    let mut failures = vec![];
    for s in &report.sweeps {
        for c in &s.cells {
            if c.measure.derivations() > 24 {
                failures.push(format!("[{}] {:?} d = {}: {} records, at most 24 for two proposals", s.edge, s.shape, c.d, c.measure.derivations()));
            }
            if c.measure.progress["distinct chosen values"] > 1 {
                failures.push(format!("[{}] {:?} d = {}: agreement violated", s.edge, s.shape, c.d));
            }
            if c.measure.progress["chosen"] == 0 {
                failures.push(format!("[{}] {:?} d = {}: nothing chosen", s.edge, s.shape, c.d));
            }
            if c.gain == Gain::Extra {
                println!("  duel, [{}] {:?} d = {}: {} records (prompt {}), per ballot {:?}", s.edge, s.shape, c.d, c.measure.derivations(), report.prompt.derivations(), c.measure.per_goal);
            }
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Goals are ballots (`Ts`): every record on every edge carries one.
fn synod_program(proposals: Vec<(u32, (u64, u32))>, proposers: usize) -> Program<Ts> {
    const N: usize = 3;
    const MAJORITY: usize = 2;
    let acceptor_slice = slice_pred("ec_inference_demos/synod.rs", "let acceptor_out = sliced", "");
    // The two mints: `certified` (acks) is unique in quorum.rs; the covering mint's block name
    // is not, so promises are "the other quorum.rs edge".
    let acks = slice_pred("ec_inference_demos/quorum.rs", "let certified = sliced", "");
    let promises = move |key: &str| key.contains("quorum.rs:") && !acks(key);
    Program::<Ts> {
        name: format!("synod ({proposers} proposer(s))"),
        inputs: InputShape::UpFront,
        run: Box::new(move |driver| {
            let mut flow = FlowBuilder::new();
            let acceptors = flow.cluster::<()>();
            let proposer_cluster = flow.cluster::<()>();
            let (p_send, proposal_stream) = proposer_cluster.sim_input::<(u64, u32), TotalOrder, ExactlyOnce>();
            let chosen_recv = super::synod::synod(&acceptors, MAJORITY, proposal_stream).sim_cluster_output();
            let mut chosen: Vec<(Ts, u32)> = vec![];
            let chosen_ref = &mut chosen;
            let proposals = proposals.clone();
            let (counts, lineage) = flow
                .sim()
                .skip_consistency_assertions()
                .with_cluster_size(&acceptors, N)
                .with_cluster_size(&proposer_cluster, proposers)
                .run_traced(driver, async move || {
                    for (member, proposal) in proposals {
                        p_send.send(member, proposal);
                    }
                    quiesce().await;
                    for member in 0..proposers as u32 {
                        chosen_ref.extend(chosen_recv.collect_sorted::<Vec<(Ts, u32)>>(member).await);
                    }
                });
            let mut progress = BTreeMap::new();
            progress.insert("chosen".to_owned(), chosen.len() as i64);
            progress.insert("distinct chosen values".to_owned(), chosen.iter().map(|(_, v)| *v).collect::<std::collections::BTreeSet<_>>().len() as i64);
            Run { counts, lineage, progress }
        }),
        clocks: vec![],
        time: None,
        goals: Box::new(move |edge, record| {
            if acceptor_slice(edge) {
                if edge.contains("#0 ") {
                    vec![record.decode::<(MemberId<()>, Ts)>().unwrap().1]
                } else {
                    vec![record.decode::<(MemberId<()>, (Ts, u32))>().unwrap().1.0]
                }
            } else if promises(edge) {
                vec![record.decode::<(Ts, (MemberId<()>, Option<(Ts, u32)>))>().unwrap().0]
            } else {
                vec![record.decode::<(Ts, MemberId<()>)>().unwrap().0]
            }
        }),
        reached: None,
    }
}

/// ABD register, one client: write then read (closed loop: the read is issued when the write is
/// done, as the contract requires). Goals are request ids on the two mint edges (the replica
/// edges carry a private type and are left unattributed). Hand computation: per operation 3
/// queries, 3 query responses, 3 phase-2 messages, 3 acks; the read repairs once. No retry.
#[test]
fn abd_write_then_read_has_no_moves() {
    const N: usize = 3;
    const MAJORITY: usize = 2;
    let acks = slice_pred("ec_inference_demos/quorum.rs", "let certified = sliced", "");
    let promises = { let acks = acks.clone(); move |key: &str| key.contains("quorum.rs:") && !acks(key) };
    let program = Program::<u64> {
        name: "abd (write then read)".to_owned(),
        inputs: InputShape::ClosedLoop,
        run: Box::new(|driver| {
            let mut flow = FlowBuilder::new();
            let replicas = flow.cluster::<()>();
            let clients = flow.cluster::<()>();
            let (w_send, writes) = clients.sim_input::<(u64, u32), TotalOrder, ExactlyOnce>();
            let (r_send, reads) = clients.sim_input::<u64, TotalOrder, ExactlyOnce>();
            let outs = super::abd::abd_register(&replicas, MAJORITY, writes, reads);
            let done_recv = outs.write_done.sim_cluster_output();
            let read_recv = outs.read_result.sim_cluster_output();
            let mut read_value = None;
            let read_ref = &mut read_value;
            let (counts, lineage) = flow
                .sim()
                .skip_consistency_assertions()
                .with_cluster_size(&replicas, N)
                .with_cluster_size(&clients, 1)
                .run_traced(driver, async move || {
                    w_send.send(0, (1, 10u32));
                    let _done: Vec<(u64, Ts)> = done_recv.collect_n_sorted(0, 1).await;
                    r_send.send(0, 2);
                    let got: Vec<(u64, Option<(Ts, u32)>)> = read_recv.collect_n_sorted(0, 1).await;
                    *read_ref = got[0].1.as_ref().map(|(_, v)| *v);
                    quiesce().await;
                });
            let mut progress = BTreeMap::new();
            progress.insert("read value".to_owned(), read_value.map_or(-1, |v| v as i64));
            Run { counts, lineage, progress }
        }),
        clocks: vec![],
        time: None,
        goals: Box::new(move |edge, record| {
            if promises(edge) {
                vec![record.decode::<(u64, (MemberId<()>, Option<(Ts, u32)>))>().unwrap().0]
            } else if acks(edge) {
                vec![record.decode::<(u64, MemberId<()>)>().unwrap().0]
            } else {
                vec![]
            }
        }),
        reached: None,
    };
    let report = program.sweep(&options());
    assert_eq!(report.prompt.progress["read value"], 10);
    for s in &report.sweeps {
        for c in &s.cells {
            assert_eq!(c.measure.derivations(), report.prompt.derivations(), "[{}] {:?} d = {}", s.edge, s.shape, c.d);
            assert_eq!(c.measure.unattributed, report.prompt.unattributed, "[{}] {:?} d = {}", s.edge, s.shape, c.d);
            assert_eq!(c.measure.progress["read value"], 10, "[{}] {:?} d = {}", s.edge, s.shape, c.d);
        }
    }
    assert_no_moves(&report);
}

/// G-set gossip, 2 members, one element each, 8 pump ticks per member up front and metered.
/// The pump is the only hook edge (the network deliveries land in a top-level fold, which has no
/// hook), so once the pump is the clock the sweep has no candidate edge: the honest verdict is
/// "nothing holdable", and the design doc already records that this program's deliveries are
/// invisible to hook counting.
#[test]
fn crdt_gossip_has_no_holdable_edge() {
    const N: usize = 2;
    const ROUNDS: usize = 8;
    let pump = |key: &str| key.contains("crdt_gossip.rs:") && key.ends_with(" <()>");
    let program = Program::<u32> {
        name: "crdt_gossip".to_owned(),
        inputs: InputShape::UpFront,
        run: Box::new(|driver| {
            let mut flow = FlowBuilder::new();
            let cluster = flow.cluster::<()>();
            let (upd_send, updates) = cluster.sim_input::<u32, NoOrder, ExactlyOnce>();
            let (tick_send, ticks) = cluster.sim_input::<(), TotalOrder, ExactlyOnce>();
            let state = super::crdt_gossip::g_set_gossip(&cluster, updates, ticks);
            let obs_tick = cluster.tick();
            let out_recv = state.snapshot(&obs_tick, nondet!(/** test observation */)).all_ticks().sim_cluster_output();
            let mut sizes = vec![0usize; N];
            let sizes_ref = &mut sizes;
            let (counts, lineage) = flow
                .sim()
                .skip_consistency_assertions()
                .with_cluster_size(&cluster, N)
                .run_traced(driver, async move || {
                    upd_send.send_many_unordered([(0, 1u32), (1, 2u32)]);
                    for member in 0..N as u32 {
                        for _ in 0..ROUNDS {
                            tick_send.send(member, ());
                        }
                    }
                    quiesce().await;
                    for member in 0..N as u32 {
                        let snapshots: Vec<std::collections::BTreeSet<u32>> = out_recv.collect(member).await;
                        sizes_ref[member as usize] = snapshots.last().map_or(0, |s| s.len());
                    }
                });
            let mut progress = BTreeMap::new();
            progress.insert("converged members".to_owned(), sizes.iter().filter(|s| **s == 2).count() as i64);
            Run { counts, lineage, progress }
        }),
        clocks: vec![clock(pump, EdgePolicy::Metered(1))],
        time: Some(Box::new(pump)),
        goals: Box::new(|_, _| vec![]),
        reached: None,
    };
    let report = program.sweep(&options());
    assert_eq!(report.prompt.progress["converged members"], N as i64);
    assert_eq!(report.clocks.len(), 1, "the pump is the only hook edge: {:?}", report.prompt.records);
    assert!(report.sweeps.is_empty(), "no edge to hold: {:?}", report.sweeps.iter().map(|s| &s.edge).collect::<Vec<_>>());
    assert_eq!(report.runs, 1);
}

/// 1-based line of the unique line of `file` (under `src/`) containing `needle!`; hook keys
/// carry the `sliced!` location.
fn line_of(file: &str, needle: &str) -> usize {
    let source = std::fs::read_to_string(format!("{}/src/{file}", env!("CARGO_MANIFEST_DIR"))).unwrap();
    let mut hits = source.lines().enumerate().filter(|(_, l)| l.contains(needle)).map(|(i, _)| i + 1);
    let line = hits.next().unwrap_or_else(|| panic!("no line of {file} contains {needle:?}"));
    assert!(hits.next().is_none(), "more than one line of {file} contains {needle:?}");
    line
}

fn slice_pred(file: &'static str, needle: &'static str, type_part: &'static str) -> impl Fn(&str) -> bool + Clone + 'static {
    let tag = format!("{}:{}:", file.rsplit('/').next().unwrap(), line_of(file, &format!("{needle}!")));
    move |key: &str| key.contains(&tag) && key.contains(type_part)
}
