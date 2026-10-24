//! HydroFrac (lineage-driven fault injection) on [`super::relay_broadcast`], the first step of
//! that module's story: one hop is not enough, and the echo relay with resend-until-acked
//! repairs it.
//!
//! Every search uses the generated specification for every `EventualConsistency` label
//! (`Spec::all_labels`). There are two labels, both on `Cluster<Member>`:
//!
//! - `initial`, the source's `broadcast_closed` over `fail_stop` (`relay_broadcast.rs:473`).
//! - `delivered`, the merge of `initial` with the relays, labelled with a `manual_proof!`
//!   (`relay_broadcast.rs:487`). This is the claim under test.
//!
//! Setup: three members, two request ids, and the source and every member crashable. The body
//! sends the requests and quiesces. Then it declares the end of finite failures (no new crash
//! or omission after that point, and crashes that already happened stand). Finally it fires
//! `ack_timeout_ticks + 3` timer rounds at every member. Those rounds are the recovery time
//! for re-sends.
//!
//! - [`RelayPolicy::None`]: `delivered` is `initial`, so the source crashing between two sends
//!   of its fan-out leaves the members disagreeing.
//! - [`RelayPolicy::Echo`] over `lossy_delayed_forever`: the echo covers the source's crash,
//!   but it is sent once. Omitting it (before the cut) leaves a member without the id.
//! - [`RelayPolicy::EchoResend`] over `lossy_delayed_forever`: the unacknowledged echo is
//!   re-sent after the cut, so the omission is repaired. Under one crash (`crash_budget(1)`),
//!   `delivered` holds: no violation in 18 fault sets, each with 8 sampled schedules. With
//!   crashes unbounded it does not hold. A member echoes only the ids it got *from the
//!   source*, not the ids it got from a relay. So the source can crash after reaching only
//!   member 0, and member 0 can then crash part-way through its echo fan-out. That leaves the
//!   two survivors disagreeing. The unbounded search reports this case.
//! - [`RelayPolicy::ReliableResend`] over `lossy_delayed_forever`: a member relays every id on
//!   its first receipt from any sender, source or peer, and re-sends until acked. This closes
//!   the gap above. `delivered` holds under one crash (no violation in 18 fault sets) and with
//!   crashes unbounded (no violation in 55 fault sets), each with 8 sampled schedules.
//!
//! In every variant `initial` diverges on its own under the source's crash. It is its own row,
//! not a hop merged into `delivered`, because the program also feeds it to the inbox.
//!
//! Schedules are sampled (`Exploration::Fuzz { instances: 8 }`), so "holds" means that no
//! violation was found, not a certificate. Exhaustive exploration does not finish here: the
//! simulator's state-space estimate for the first fault set was still growing past 16,000
//! schedules.
//!
//! ```sh
//! cargo test -p hydro_test --lib cluster::relay_broadcast_ldfi -- --nocapture --test-threads=1
//! ```

#[cfg(test)]
mod tests {
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::compiled::CompiledSim;
    use hydro_lang::sim::ldfi::{Exploration, Fault, LabelResults, LabelVerdict, Ldfi, Outcome};
    use hydro_lang::sim::spec::Spec;

    use crate::cluster::relay_broadcast::{
        Member, RelayChannel, RelayConfig, RelayPolicy, Request, Source, relay_broadcast,
    };

    const MEMBERS: u32 = 3;
    /// Where `delivered` gets its label (the `assert_has_consistency_of`).
    const DELIVERED: &str = "relay_broadcast.rs:487";
    /// Where `initial` gets its label (the source's `broadcast_closed`).
    const INITIAL: &str = "relay_broadcast.rs:473";

    /// Timer rounds after the cut: enough for the oldest echo to pass `ack_timeout_ticks`,
    /// be re-sent, and be merged and acknowledged.
    fn rounds_after_cut(policy: RelayPolicy) -> u64 {
        let timeout = match policy {
            RelayPolicy::EchoResend { ack_timeout_ticks }
            | RelayPolicy::ReliableResend { ack_timeout_ticks } => ack_timeout_ticks,
            RelayPolicy::None | RelayPolicy::Echo => 0,
        };
        timeout + 3
    }

    /// Every label of `relay_broadcast` under `policy` and `channel`, searched by HydroFrac.
    /// `configure` adjusts the search (for example, a crash budget).
    fn search(
        policy: RelayPolicy,
        channel: RelayChannel,
        configure: impl FnOnce(Ldfi) -> Ldfi,
    ) -> LabelResults {
        let mut flow = FlowBuilder::new();
        let source = flow.process::<Source>();
        let members = flow.cluster::<Member>();
        let (request, requests) = source.sim_input::<Request, TotalOrder, ExactlyOnce>();
        let (timer, timers) = members.sim_input::<(), TotalOrder, ExactlyOnce>();
        // Metrics windows are a deployment concern; this never fires.
        let (_report, report_ticks) = members.sim_input::<(), TotalOrder, ExactlyOnce>();
        let outputs = relay_broadcast(
            &source,
            &members,
            requests,
            timers,
            report_ticks,
            RelayConfig {
                policy,
                channel,
            },
        );
        let _delivered = outputs.delivered.weaken_consistency().sim_cluster_output();

        let spec = Spec::all_labels(&mut flow);

        // Under `lossy_delayed_forever` the simulator's own model is "a dropped message
        // arrives at the end". HydroFrac's omission is the drop itself, so the sim runs
        // safety-only. `skip_consistency_assertions` is needed because the simulator panics on
        // `assert_has_consistency_of` otherwise; checking that label is HydroFrac's job.
        let compiled: CompiledSim = flow
            .sim()
            .test_safety_only()
            .skip_consistency_assertions()
            .with_cluster_size(&members, MEMBERS as usize)
            .unit_test_fuzz_iterations(8)
            .with_provenance()
            .compiled();

        let rounds = rounds_after_cut(policy);
        let start = std::time::Instant::now();
        let results = configure(
            Ldfi::new()
                .exploration(Exploration::Fuzz { instances: 8 })
                .crashable("source", &source)
                .crashable_cluster("member", &members, 0..MEMBERS)
                .spec(&spec),
        )
        .search_all_labels(&compiled, async |run| {
            request.send(Request::new(1));
            request.send(Request::new(2));
            hydro_lang::sim::quiesce().await;
            run.end_of_finite_failures();
            for _ in 0..rounds {
                timer.send_many((0..MEMBERS).map(|m| (m, ())));
                hydro_lang::sim::quiesce().await;
            }
        });
        eprintln!(
            "relay_broadcast {policy:?} over {channel:?}: {:.1}s\n{results}",
            start.elapsed().as_secs_f64()
        );
        results
    }

    /// The one row at `site`.
    fn row<'r>(results: &'r LabelResults, site: &str) -> &'r LabelVerdict {
        let rows: Vec<_> = results.at(site).collect();
        assert_eq!(rows.len(), 1, "one row at {site}: {results}");
        &rows[0].verdict
    }

    /// The faults of the counterexample behind `verdict`.
    fn counterexample(verdict: &LabelVerdict, results: &LabelResults) -> Vec<Fault> {
        match verdict.outcome() {
            Some(Outcome::Counterexample { faults, .. }) => faults.iter().cloned().collect(),
            _ => panic!("expected a counterexample: {results}"),
        }
    }

    fn crashes(faults: &[Fault], who: &str) -> bool {
        faults
            .iter()
            .any(|f| matches!(f, Fault::Crash { node, .. } if node.starts_with(who)))
    }

    fn omits_relay(faults: &[Fault]) -> bool {
        faults
            .iter()
            .any(|f| matches!(f, Fault::Omit(m) if m.channel == "relay"))
    }

    /// Story step 1: with no relaying, `delivered` is the single hop. The source crashing in
    /// the middle of its fan-out makes it diverge.
    #[test]
    fn relay_none_diverges_under_a_source_crash() {
        let results = search(RelayPolicy::None, RelayChannel::FailStop, |l| l);
        let faults = counterexample(row(&results, DELIVERED), &results);
        assert!(crashes(&faults, "source"), "{results}");
        assert!(results.counterexamples().count() >= 1, "{results}");
    }

    /// Story step 2a: echo once over a lossy relay channel. One source crash plus one
    /// omitted echo leaves a member without the id, and nothing re-sends it.
    #[test]
    fn relay_echo_lossy_diverges_under_a_source_crash_and_an_omitted_relay() {
        let results = search(RelayPolicy::Echo, RelayChannel::LossyDelayedForever, |l| l);
        let faults = counterexample(row(&results, DELIVERED), &results);
        assert!(crashes(&faults, "source"), "{results}");
        assert!(omits_relay(&faults), "{results}");
    }

    const RESEND: RelayPolicy = RelayPolicy::EchoResend {
        ack_timeout_ticks: 3,
    };

    /// Story step 2b: echo with resend-until-acked over a lossy relay channel, under one
    /// crash. The re-send after the cut repairs any omission, so `delivered` holds (no
    /// violation found). `initial` still diverges on its own, which is expected: the relay is
    /// what fixes it.
    #[test]
    fn relay_echo_resend_lossy_holds_under_one_crash() {
        let results = search(RESEND, RelayChannel::LossyDelayedForever, |l| {
            l.crash_budget(1)
        });
        let delivered = row(&results, DELIVERED);
        assert!(
            matches!(delivered.outcome(), Some(Outcome::Certificate { .. })),
            "{results}"
        );
        // `initial` is a separate row, not a hop merged into `delivered`: the program also
        // feeds it to the inbox. So it is the only counterexample.
        assert!(
            results
                .counterexamples()
                .all(|(r, _)| r.label().contains(INITIAL)),
            "{results}"
        );
        // The single hop on its own, kept for the story.
        if let Some(Outcome::Counterexample { faults, .. }) = row(&results, INITIAL).outcome() {
            let faults: Vec<_> = faults.iter().cloned().collect();
            assert!(crashes(&faults, "source"), "{results}");
        }
    }

    /// The same protocol with crashes unbounded. A member does not re-echo what it got from a
    /// relay, so the source crashing after reaching only member 0, followed by member 0
    /// crashing part-way through its echoes, leaves the two survivors disagreeing. This is a
    /// gap in the protocol, not in the harness: no amount of recovery time fixes it, because
    /// no live member has the id and also echoes it. The manual proof on `delivered` ("any
    /// member that got id from the source relays it to all peers") does not cover a relayer
    /// that crashes.
    #[test]
    fn relay_echo_resend_lossy_diverges_under_a_source_and_a_relayer_crash() {
        let results = search(RESEND, RelayChannel::LossyDelayedForever, |l| l);
        let faults = counterexample(row(&results, DELIVERED), &results);
        assert!(crashes(&faults, "source"), "{results}");
        assert!(crashes(&faults, "member"), "{results}");
    }

    const RELIABLE: RelayPolicy = RelayPolicy::ReliableResend {
        ack_timeout_ticks: 3,
    };

    /// The final repair, reliable broadcast with retransmission: a member relays every id on
    /// its first receipt from any sender, so a relayer crashing part-way through its fan-out
    /// is covered by any peer it reached. Under one crash, `delivered` holds.
    #[test]
    fn relay_reliable_resend_lossy_holds_under_one_crash() {
        let results = search(RELIABLE, RelayChannel::LossyDelayedForever, |l| {
            l.crash_budget(1)
        });
        assert!(
            matches!(
                row(&results, DELIVERED).outcome(),
                Some(Outcome::Certificate { .. })
            ),
            "{results}"
        );
        assert!(
            results
                .counterexamples()
                .all(|(r, _)| r.label().contains(INITIAL)),
            "{results}"
        );
    }

    /// The same with crashes unbounded: the counterexample that refutes `EchoResend` (source
    /// and then the only relayer crash) no longer applies, since the peer member 0 reached
    /// relays the id onward.
    #[test]
    fn relay_reliable_resend_lossy_holds_under_unbounded_crashes() {
        let results = search(RELIABLE, RelayChannel::LossyDelayedForever, |l| l);
        assert!(
            matches!(
                row(&results, DELIVERED).outcome(),
                Some(Outcome::Certificate { .. })
            ),
            "{results}"
        );
        assert!(
            results
                .counterexamples()
                .all(|(r, _)| r.label().contains(INITIAL)),
            "{results}"
        );
    }
}
