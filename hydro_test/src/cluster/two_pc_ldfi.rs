//! Two-phase commit, instrumented for lineage-driven fault injection: the LDFI paper's third
//! example and the classic blocking case.
//!
//! The coordinator broadcasts `prepare`, collects a vote from every participant (here every
//! participant votes yes), and broadcasts `commit`. The property is **agreement**: if any
//! participant commits a transaction (even one that later crashes), every correct participant
//! commits it. Links are reliable (`fail_stop`), so the fault model is crashes.
//!
//! - Plain 2PC violates agreement under one crash: the coordinator dies after sending `commit`
//!   to some participants; those commit, the rest wait forever.
//! - With the **cooperative termination protocol** (`ctp`), a participant that voted yes and
//!   has heard no decision asks its peers when its timer fires, and a peer that knows the
//!   decision answers. That survives the coordinator's crash, but not the coordinator's crash
//!   *plus* the crash of the one participant that learned the decision before it could answer:
//!   the committed transaction is then known only to a dead node. Two crashes, as the paper
//!   found.
//!
//! # Shape
//!
//! Each participant runs one tick that consumes the coordinator's two channels, the peer
//! channel, and its timer through hooked batches (traced delivery points), keeps the set of
//! transactions it is uncertain about and the set it has decided, and emits votes, peer
//! messages, and its commits. The coordinator runs one tick that counts votes and emits a
//! decision once every participant has voted.

use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
use hydro_lang::location::MemberId;
use hydro_lang::prelude::*;
use hydro_lang::sim_hooks::{BatchHook, OnCluster, OnProcess};
use serde::{Deserialize, Serialize};

pub struct Coordinator {}

pub struct Participant {}

/// A transaction identifier.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Txn(pub u32);

/// Participant-to-participant traffic for cooperative termination.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum PeerMsg {
    /// "I voted yes on this transaction and have heard no decision; do you know one?"
    Query(Txn),
    /// "This transaction was committed."
    Decided(Txn),
}

/// Hook on each participant's batch of `prepare` messages.
pub type PrepareHook = BatchHook<Txn, TotalOrder, ExactlyOnce, OnCluster<Participant>>;
/// Hook on each participant's batch of `commit` messages.
pub type CommitHook = BatchHook<Txn, NoOrder, ExactlyOnce, OnCluster<Participant>>;
/// Hook on each participant's batch of peer messages, keyed by the sending participant.
pub type PeerHook =
    BatchHook<(MemberId<Participant>, PeerMsg), NoOrder, ExactlyOnce, OnCluster<Participant>>;
/// Hook on the coordinator's batch of votes, keyed by the voting participant.
pub type VoteHook =
    BatchHook<(MemberId<Participant>, Txn), NoOrder, ExactlyOnce, OnProcess<Coordinator>>;
/// Hook on each participant's batch of timer interrupts.
pub type TimeoutHook = BatchHook<(), TotalOrder, ExactlyOnce, OnCluster<Participant>>;

/// Builds the protocol. `txns` are the transactions the coordinator runs; `timeouts` fire a
/// participant's uncertainty timer (one `()` per member, driven by the test). Returns the
/// transactions each participant committed.
///
/// # Non-Determinism
/// - `nondet_prepares`, `nondet_commits`, `nondet_peers`, `nondet_votes`: the batching of
///   each channel at its receiver. All are LDFI delivery points.
/// - `nondet_timeouts`: when a participant's timer fires relative to the messages it
///   receives, which only decides whether it asks its peers before hearing the decision.
#[expect(clippy::too_many_arguments, reason = "one hook per channel")]
pub fn two_pc<'a>(
    coordinator: &Process<'a, Coordinator>,
    participants: &Cluster<'a, Participant>,
    num_participants: usize,
    txns: Stream<Txn, Process<'a, Coordinator>>,
    timeouts: Stream<(), Cluster<'a, Participant>>,
    ctp: bool,
    nondet_prepares: NonDet<Option<PrepareHook>>,
    nondet_commits: NonDet<Option<CommitHook>>,
    nondet_peers: NonDet<Option<PeerHook>>,
    nondet_votes: NonDet<Option<VoteHook>>,
    nondet_timeouts: NonDet<Option<TimeoutHook>>,
) -> Stream<Txn, Cluster<'a, Participant>, Unbounded, NoOrder> {
    // Phase 1: the coordinator asks every participant to prepare.
    let prepares = txns.broadcast_closed(participants, TCP.fail_stop().name("prepare").bincode());

    // Phase 2: decisions, taken in the coordinator's tick below once every vote is in.
    let (decisions_handle, decisions) =
        coordinator.forward_ref::<Stream<Txn, Process<'a, Coordinator>, Unbounded, NoOrder>>();
    let commits =
        decisions.broadcast_closed(participants, TCP.fail_stop().name("commit").bincode());

    // Peer traffic (cooperative termination), closing a cycle through the network.
    let (peer_handle, peer_in) =
        prepares
            .location()
            .forward_ref::<Stream<(MemberId<Participant>, PeerMsg), _, Unbounded, NoOrder>>();

    #[expect(
        clippy::type_complexity,
        reason = "the sliced! outputs are annotated with their full stream types"
    )]
    let (votes, peer_out, committed): (
        Stream<Txn, Cluster<'a, Participant>, Unbounded, NoOrder>,
        Stream<PeerMsg, Cluster<'a, Participant>, Unbounded, NoOrder>,
        Stream<Txn, Cluster<'a, Participant>, Unbounded, NoOrder>,
    ) = sliced! {
        let prepare_batch = use::batch(prepares, nondet_prepares);
        let commit_batch = use::batch(commits, nondet_commits);
        let peer_batch = use::batch(peer_in, nondet_peers);
        let timeout_batch = use::batch(timeouts, nondet_timeouts);
        let mut uncertain = use::state(|l| {
            l.source_iter(q!(Vec::<Txn>::new())).weaken_ordering::<NoOrder>()
        });
        let mut decided = use::state(|l| {
            l.source_iter(q!(Vec::<Txn>::new())).weaken_ordering::<NoOrder>()
        });

        // Every prepare is voted yes.
        let votes = prepare_batch.clone().weaken_ordering::<NoOrder>();

        // Decisions learned this tick: from the coordinator, or from a peer that knows.
        let peer_decisions = peer_batch.clone().filter_map(q!(|(_, message)| match message {
            PeerMsg::Decided(txn) => Some(txn),
            PeerMsg::Query(_) => None,
        }));
        let newly_decided = commit_batch
            .chain(peer_decisions)
            .unique()
            .filter_not_in(decided.clone());
        let decided_now = decided.chain(newly_decided.clone());
        decided = decided_now.clone();

        // Uncertain: voted yes, no decision heard.
        let uncertain_now = uncertain
            .chain(votes.clone())
            .filter_not_in(decided_now.clone());
        uncertain = uncertain_now.clone();

        // Cooperative termination: on a timeout, ask peers about everything uncertain; answer
        // every query about a transaction we have decided.
        let timed_out = timeout_batch.count().map(q!(move |n| ctp && n > 0));
        let queries = uncertain_now
            .filter_if(timed_out)
            .map(q!(|txn| PeerMsg::Query(txn)));
        let queried = peer_batch.filter_map(q!(|(_, message)| match message {
            PeerMsg::Query(txn) => Some((txn, ())),
            PeerMsg::Decided(_) => None,
        }));
        let answers = decided_now
            .map(q!(|txn| (txn, ())))
            .join(queried)
            .map(q!(|(txn, _)| PeerMsg::Decided(txn)));

        (votes, queries.chain(answers), newly_decided)
    };

    peer_handle.complete(
        peer_out
            .broadcast_closed(participants, TCP.fail_stop().name("peer").bincode())
            .entries(),
    );

    // The coordinator: count votes per transaction; decide once everyone has voted.
    let vote_entries = votes
        .send(coordinator, TCP.fail_stop().name("vote").bincode())
        .entries();
    let decided_at_coordinator: Stream<Txn, Process<'a, Coordinator>, Unbounded, NoOrder> = sliced! {
        let vote_batch = use::batch(vote_entries, nondet_votes);
        let mut all_votes = use::state(|l| {
            l.source_iter(q!(Vec::<(MemberId<Participant>, Txn)>::new()))
                .weaken_ordering::<NoOrder>()
        });
        let mut committed = use::state(|l| {
            l.source_iter(q!(Vec::<Txn>::new())).weaken_ordering::<NoOrder>()
        });

        let votes_now = all_votes.chain(vote_batch);
        all_votes = votes_now.clone();

        let complete = votes_now
            .map(q!(|(member, txn)| (txn, member)))
            .into_keyed()
            .fold(
                q!(|| 0usize),
                q!(
                    |count, _member| *count += 1,
                    commutative = manual_proof!(/** counting is commutative */)
                ),
            )
            .filter(q!(move |count| *count >= num_participants))
            .keys();
        let newly_committed = complete.filter_not_in(committed.clone());
        committed = committed.chain(newly_committed.clone());

        newly_committed
    };
    decisions_handle.complete(decided_at_coordinator);

    committed
}

/// The test harness: compiles a configuration and drives one run. Shared with the LDFI
/// measurement grid (`ldfi_grid.rs`).
#[cfg(test)]
pub(crate) mod harness {
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::compiled::CompiledSim;
    use hydro_lang::sim::ldfi::{Ldfi, Level, Run};
    use hydro_lang::sim::spec::Spec;
    use hydro_lang::sim::{SimClusterSender, SimSender};

    use super::{
        CommitHook, Coordinator, Participant, PeerHook, PrepareHook, TimeoutHook, Txn, VoteHook,
        two_pc,
    };

    /// Everything a search needs besides the compiled simulation.
    pub(crate) struct Harness {
        participants: u32,
        /// Transactions per run; the specification is about the first, the rest are load.
        txns: u32,
        txn_send: SimSender<Txn, TotalOrder, ExactlyOnce>,
        timeout_send: SimClusterSender<(), TotalOrder, ExactlyOnce>,
    }

    /// Builds and compiles 2PC with or without cooperative termination.
    pub(crate) fn compile(
        participants_count: u32,
        ctp: bool,
        fuzz_iterations: usize,
    ) -> (CompiledSim, Harness, impl Fn(usize) -> Ldfi) {
        compile_with(participants_count, ctp, 1, true, fuzz_iterations)
    }

    /// [`compile`] with `txns` transactions per run (the specification is about the first;
    /// the others are background load) and with or without record-level provenance.
    ///
    /// The specification is agreement on the first transaction, as views over the
    /// participants' commits (see `hydro_lang::sim::spec`):
    ///
    /// - `PRE`: some participant committed `Txn(1)` (even one that later crashed: a crashed
    ///   node's earlier outputs were observed).
    /// - `POST`: every correct participant committed `Txn(1)`, stated as "the correct
    ///   participants that committed are as many as the correct participants".
    pub(crate) fn compile_with(
        participants_count: u32,
        ctp: bool,
        txn_count: u32,
        provenance: bool,
        fuzz_iterations: usize,
    ) -> (CompiledSim, Harness, impl Fn(usize) -> Ldfi) {
        let mut flow = FlowBuilder::new();
        let coordinator = flow.process::<Coordinator>();
        let participants = flow.cluster::<Participant>();
        let prepare_hook: PrepareHook = flow.sim_hook();
        let commit_hook: CommitHook = flow.sim_hook();
        let peer_hook: PeerHook = flow.sim_hook();
        let vote_hook: VoteHook = flow.sim_hook();
        let timeout_hook: TimeoutHook = flow.sim_hook();

        let (txn_send, txns) = coordinator.sim_input::<Txn, TotalOrder, ExactlyOnce>();
        let (timeout_send, timeouts) = participants.sim_input::<(), TotalOrder, ExactlyOnce>();
        let committed = two_pc(
            &coordinator,
            &participants,
            participants_count as usize,
            txns,
            timeouts,
            ctp,
            nondet!(/** traced by the search */ hook = prepare_hook),
            nondet!(/** traced by the search */ hook = commit_hook),
            nondet!(/** traced by the search */ hook = peer_hook),
            nondet!(/** traced by the search */ hook = vote_hook),
            nondet!(/** external input, consumed as it arrives */ hook = timeout_hook),
        );

        // The specification.
        let mut spec = Spec::new(&mut flow);
        let committed = spec.observe_cluster(committed);
        let correct = spec.correct_members(&participants, 0..participants_count);
        let committed_first = committed.filter(q!(|(_, txn)| txn.0 == 1));
        spec.pre(committed_first.clone().first());
        let committing = committed_first.map(q!(|(p, _)| (p, ()))).unique();
        let correct_committing = correct
            .clone()
            .map(q!(|p| (p, ())))
            .join(committing)
            .count();
        spec.post(
            correct_committing
                .zip(correct.count())
                .filter(q!(|(committing, correct)| committing == correct)),
        );

        let coordinator_id = coordinator.clone();
        let participants_id = participants.clone();
        let mut sim = flow
            .sim()
            .skip_consistency_assertions()
            .with_cluster_size(&participants, participants_count as usize)
            .unit_test_fuzz_iterations(fuzz_iterations);
        if provenance && std::env::var("LDFI_NO_PROVENANCE").is_err() {
            sim = sim.with_provenance();
        }
        let compiled = sim.compiled();

        let harness = Harness {
            participants: participants_count,
            txns: txn_count,
            txn_send,
            timeout_send,
        };
        // Fault model: the coordinator and every participant may crash; links are reliable
        // (`fail_stop`, so the filter admits no omission on them).
        let configure = move |crashes: usize| {
            Ldfi::new()
                .level(if provenance {
                    Level::Record
                } else {
                    Level::Slice
                })
                .level_from_env()
                .crashable("coordinator", &coordinator_id)
                .crashable_cluster("participant", &participants_id, 0..participants_count)
                .crash_budget(crashes)
                .spec(&spec)
        };
        (compiled, harness, configure)
    }

    /// One run of the protocol: the transactions, then every participant's uncertainty
    /// timer fires once the dust settles. The verdict is the specification's.
    pub(crate) async fn body(h: &Harness, _run: &Run<'_>) {
        for t in 1..=h.txns {
            h.txn_send.send(Txn(t));
        }
        hydro_lang::sim::quiesce().await;
        h.timeout_send
            .send_many((0..h.participants).map(|p| (p, ())));
        hydro_lang::sim::quiesce().await;
    }
}

#[cfg(test)]
mod tests {
    use hydro_lang::sim::ldfi::{Exploration, Fault, FuzzOutcome, Outcome, Strategy};

    use super::harness::{body, compile};

    fn search(ctp: bool, crashes: usize) -> Outcome {
        let (compiled, h, configure) = compile(2, ctp, 8);
        configure(crashes).search(&compiled, async |run| body(&h, run).await)
    }

    /// The blocking case: the coordinator dies after sending `commit` to one participant.
    /// The failure-free derivation is the whole protocol run (prepares, votes, commits), so
    /// the candidates are the coordinator's crash at each of its four sends (two prepares,
    /// two commits), weakest first: dying after the first commit is the bug, found on the
    /// second run.
    #[test]
    fn ldfi_blocks_two_pc_with_a_coordinator_crash() {
        let outcome = search(false, 1);
        eprintln!("{outcome}");
        let Outcome::Counterexample { faults, runs, .. } = &outcome else {
            panic!("expected a counterexample, got {outcome}");
        };
        // The coordinator dies after its first commit (its third send: two prepares, then
        // the commit to participant 0).
        assert_eq!(faults.len(), 1);
        assert!(
            matches!(faults.first(), Some(Fault::Crash { node, after: Some(m) })
                if node == "coordinator" && m.to_string().starts_with("commit[→0]:")),
            "expected the coordinator to die after its first commit: {faults:?}"
        );
        assert_eq!(runs.len(), 2);
    }

    /// Cooperative termination survives the coordinator's crash: the participant that got the
    /// `commit` answers its peers' queries, which appears as a new derivation through the peer
    /// channel. No single crash kills both derivations.
    #[test]
    fn ldfi_certifies_ctp_under_one_crash() {
        let outcome = search(true, 1);
        eprintln!("{outcome}");
        assert!(
            matches!(outcome, Outcome::Certificate { .. }),
            "expected a certificate, got {outcome}"
        );
    }

    /// ...but not two: the coordinator dies after one `commit`, and the participant that
    /// received it dies after its vote, before it can answer anyone. The transaction is
    /// committed on a dead node only; the correct participants stay uncertain forever.
    #[test]
    fn ldfi_breaks_ctp_with_two_crashes() {
        let outcome = search(true, 2);
        eprintln!("{outcome}");
        let Outcome::Counterexample { faults, .. } = &outcome else {
            panic!("expected a counterexample, got {outcome}");
        };
        assert!(
            faults
                .iter()
                .any(|f| matches!(f, Fault::Crash { node, after: Some(m) }
                if node == "coordinator" && m.to_string().starts_with("commit["))),
            "the coordinator must die after a commit: {faults:?}"
        );
        assert!(
            faults
                .iter()
                .any(|f| matches!(f, Fault::Crash { node, .. } if node == "participant@0")),
            "participant 0 (the one that got the commit) must die too: {faults:?}"
        );
    }

    /// Lineage-driven search against two lineage-blind searchers over the same fault space,
    /// the same simulator, and the same goal:
    ///
    /// - **ldfi / exhaustive**: every schedule per hypothesis; its certificate is a proof.
    /// - **ldfi / fuzz k**: `k` sampled schedules per hypothesis; same budget unit as the
    ///   random fuzzer (one simulator instance), no proof.
    /// - **blind**: the same loop with the cover constraints removed, so hypotheses are the
    ///   untried admissible fault sets over the observed universe, by increasing weight.
    /// - **fuzz**: every instance draws its crash set from the simulator's entropy (uniform
    ///   under `cargo test`, libFuzzer-guided under `cargo sim`), with a crash point bound of
    ///   `max_after = 6` since a blind tool does not know how many sends a node makes.
    ///
    /// "sched" is simulator instances, the comparable cost. The three-participant CTP rows run
    /// only the sampled searchers: under `exhaustive` each hypothesis there explores 10k-50k
    /// schedules (measured once: budget 1, LDFI certificate in 10 hypotheses / 139,764
    /// schedules vs. blind exhausted in 34 / 160,500; budget 2, LDFI violated in 13 / 209,652,
    /// 5m15s, the blind search cancelled after 37 more hypotheses and ~10 minutes; see
    /// `ldfi_ctp_three_participants_two_crashes`).
    #[test]
    fn compare_search_strategies() {
        const FUZZ_INSTANCES: usize = 2000;
        const SAMPLED: usize = 4;

        fn verdict(o: &Outcome) -> &'static str {
            match o {
                Outcome::Counterexample { .. } => "violated",
                Outcome::Certificate { .. } => "certificate",
                Outcome::Exhausted { .. } => "exhausted",
                Outcome::Budget { .. } => "budget",
                Outcome::Vacuous { .. } => "vacuous",
            }
        }
        fn cell(o: Option<&Outcome>) -> String {
            match o {
                Some(o) => format!(
                    "{:>12} {:>4} {:>7}",
                    verdict(o),
                    o.runs().len(),
                    o.schedules()
                ),
                None => format!("{:>12} {:>4} {:>7}", "-", "-", "-"),
            }
        }

        eprintln!(
            "\n{:<10} {:>2} {:>3} | {:<25} | {:<25} | {:<25} | {:<13}",
            "protocol",
            "n",
            "cr",
            "ldfi/exhaustive  runs sched",
            "ldfi/fuzz 4      runs sched",
            "blind            runs sched",
            "fuzz    inst"
        );
        for (participants, ctp, crashes, full) in [
            (2, false, 1, true),
            (2, true, 1, true),
            (2, true, 2, true),
            (3, false, 1, true),
            (3, true, 1, false),
            (3, true, 2, false),
        ] {
            let (compiled, h, configure) = compile(participants, ctp, FUZZ_INSTANCES);
            let exhaustive = full.then(|| {
                configure(crashes)
                    .quiet()
                    .search(&compiled, async |run| body(&h, run).await)
            });
            let sampled = configure(crashes)
                .exploration(Exploration::Fuzz { instances: SAMPLED })
                .quiet()
                .search(&compiled, async |run| body(&h, run).await);
            let blind = full.then(|| {
                configure(crashes)
                    .strategy(Strategy::Blind)
                    .quiet()
                    .search(&compiled, async |run| body(&h, run).await)
            });
            let crash_points = sampled.runs()[0].crash_points();
            let fuzz: FuzzOutcome = configure(crashes).quiet().fuzz_faults(
                &compiled,
                &[],
                &crash_points,
                async |run| {
                    body(&h, run).await;
                    true
                },
            );

            eprintln!(
                "{:<10} {:>2} {:>3} | {} | {} | {} | {:>8} {:>4}",
                if ctp { "2pc + ctp" } else { "2pc" },
                participants,
                crashes,
                cell(exhaustive.as_ref()),
                cell(Some(&sampled)),
                cell(blind.as_ref()),
                if fuzz.violation.is_some() {
                    "violated"
                } else {
                    "none"
                },
                fuzz.runs,
            );

            if let (Some(exhaustive), Some(blind)) = (&exhaustive, &blind) {
                assert_eq!(
                    exhaustive.is_counterexample(),
                    blind.is_counterexample(),
                    "lineage and blind search must agree on whether a violation exists"
                );
                assert!(
                    exhaustive.runs().len() <= blind.runs().len(),
                    "lineage took more runs than blind enumeration"
                );
                assert_eq!(
                    exhaustive.is_counterexample(),
                    sampled.is_counterexample(),
                    "sampling missed a violation (or found a spurious one)"
                );
            }
            // The random baseline's hit rate is reported, not asserted: with crash points
            // named by message (one per distinct send) it often misses the two-crash pair
            // within `FUZZ_INSTANCES` (see the measurement grid).
        }
    }

    /// The expensive row of the comparison, lineage-driven only, with the per-hypothesis log.
    /// Run with `--ignored`; every hypothesis explores tens of thousands of schedules.
    #[test]
    #[ignore = "minutes per run under exhaustive; run explicitly"]
    fn ldfi_ctp_three_participants_two_crashes() {
        let started = std::time::Instant::now();
        let (compiled, h, configure) = compile(3, true, 8);
        let outcome = configure(2).search(&compiled, async |run| body(&h, run).await);
        eprintln!(
            "{outcome}  ({} hypotheses, {} schedules, {:?})",
            outcome.runs().len(),
            outcome.schedules(),
            started.elapsed()
        );
        assert!(outcome.is_counterexample(), "got {outcome}");
    }
}
