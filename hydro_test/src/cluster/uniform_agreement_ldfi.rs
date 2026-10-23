//! **Uniform** agreement under hydrofrac, as a hand-written specification, next to the
//! non-uniform agreement the generated `EventualConsistency` specification checks
//! (`Spec::eventual_consistency`, exercised on the same demos in `ec_demos_ldfi`).
//!
//! - Non-uniform (generated): every fact some *correct* member holds is held by every correct
//!   member. A member that delivers and then crashes is ignored.
//! - Uniform (here): every fact *any* member delivered (a member that later crashes included,
//!   as long as it delivered before crashing) is held by every correct member at the end.
//!
//! Observations from a member that later crashes do reach the observer: the observation
//! channel goes through the simulator's filter like any send, and a crash after message `m`
//! drops only the node's *later* sends, so an observation sent before the crash is kept.
//! The observer therefore sees `(m, f)` for a crashed `m` exactly when `m` delivered `f`
//! (emitted it to its output) before dying.
//!
//! Plain (echo-based, non-uniform) reliable broadcast violates uniform agreement: the source
//! crashes after reaching one member, that member delivers and crashes before its echoes go
//! out. Uniform reliable broadcast (delivery gated on `F + 1` echoes) upholds it.

#[cfg(test)]
mod tests {
    use hydro_lang::live_collections::boundedness::Bounded;
    use hydro_lang::live_collections::stream::{ExactlyOnce, NoOrder, TotalOrder};
    use hydro_lang::location::{MemberId, Tick};
    use hydro_lang::prelude::*;
    use hydro_lang::sim::compiled::CompiledSim;
    use hydro_lang::sim::ldfi::{Exploration, Fault, FaultSet, Ldfi, Level, Outcome};
    use hydro_lang::sim::spec::{Observer, Spec};
    use hydro_std::ec_inference_demos::reliable_broadcast::reliable_broadcast_closed;
    use hydro_std::ec_inference_demos::uniform_broadcast::uniform_reliable_broadcast_closed;

    pub struct Source;
    pub struct Member;

    const MEMBERS: u32 = 3;

    /// Which broadcast demo the search runs.
    #[derive(Clone, Copy, Debug)]
    enum Broadcast {
        /// `reliable_broadcast_closed`: deliver on first receipt, echo to everyone.
        Reliable,
        /// `uniform_reliable_broadcast_closed` with this echo threshold.
        Uniform(usize),
    }

    /// Which agreement the specification checks.
    #[derive(Clone, Copy, Debug)]
    enum Agreement {
        /// The generated (non-uniform) `EventualConsistency` specification.
        Generated,
        /// The hand-written uniform agreement below.
        Uniform,
    }

    /// The inputs the body broadcasts: the fact domain of the uniform specification.
    const DOMAIN: &[u32] = &[1, 2];

    type Facts<'a, T> = Stream<T, Tick<Process<'a, Observer>>, Bounded, TotalOrder, ExactlyOnce>;

    /// Uniform agreement over `observed`, the `(member, fact)` deliveries of every member
    /// (crashed or not), and `correct`, the members the hypothesis does not crash. `POST`
    /// holds iff the distinct `(correct member, fact)` pairs over the facts delivered
    /// *anywhere* are as many as those facts times the correct members: every correct member
    /// holds every fact any member delivered. `PRE` is that some member delivered something
    /// ("if any member delivers `f`, ...") and that at most one member crashed (`F = 1`).
    ///
    /// The facts are restricted to the domain `DOMAIN`, the inputs the body broadcasts, as the
    /// generated specification restricts them to the failure-free run's facts: a fault cannot
    /// then make the view fail by having a member derive a fresh fact (none of these
    /// protocols can, so the restriction is the convention, not a filter that ever fires).
    ///
    /// The view is not monotone in the facts in the way the generated one is: a hypothesis can
    /// make a member that then crashes deliver a domain fact that, in an earlier run, no member
    /// delivered at all, which falsifies `POST` without removing any fact of that run's
    /// derivation. That only weakens a *certificate* (the lineage inference may skip such a
    /// hypothesis); a counterexample is a concrete run and is sound regardless.
    fn uniform_agreement<'a>(
        spec: &mut Spec<'a>,
        observed: Facts<'a, (MemberId<Member>, u32)>,
        correct: Facts<'a, MemberId<Member>>,
    ) {
        // The domain: the inputs the body broadcasts (`DOMAIN`).
        let observed = observed.filter(q!(|(_, fact)| *fact == 1 || *fact == 2));
        // Facts delivered by any member, crashed or not.
        let delivered = observed.clone().map(q!(|(_, fact)| fact)).unique();
        // Of those, the (member, fact) pairs at correct members.
        let held = correct
            .clone()
            .map(q!(|member| (member, ())))
            .join(observed)
            .map(q!(|(member, ((), fact))| (member, fact)))
            .unique();
        // PRE: some member delivered something, and at most one member crashed. Without the
        // first conjunct, a run in which nothing is delivered (the source crashing before any
        // send) satisfies POST with an empty derivation, and an empty clause cannot be hit:
        // the search would certify there. The second is the fault model (`F = 1` of three
        // members, the source crashable as well), as the generated specification's crash
        // bound (`|C| - 2` members) gives the EC searches over the same demos; `crash_budget`
        // caps crashes over all nodes and cannot say "one member and the source". A
        // hypothesis whose every schedule is vacuous is recorded and none of its supersets is
        // proposed, so each two-member crash costs one run.
        spec.pre(
            delivered
                .clone()
                .first()
                .zip(correct.clone().count())
                .filter(q!(|(_, correct)| *correct >= 2)),
        );
        let post = held
            .count()
            .zip(delivered.count())
            .zip(correct.count())
            .filter(q!(|((pairs, facts), members)| *pairs == *facts * *members));
        spec.post(post);
    }

    /// The source broadcasts `1` and `2` to three members; source and members crashable, all
    /// channels `fail_stop`. Crash bounds, exploration and fuzzing mirror `ec_demos_ldfi`'s
    /// `broadcast_search` for the same demo.
    fn search(which: Broadcast, agreement: Agreement) -> Outcome {
        let mut flow = FlowBuilder::new();
        let source = flow.process::<Source>();
        let members = flow.cluster::<Member>();
        let (send, items) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();

        let delivered: Stream<u32, Cluster<'_, Member>, Unbounded, NoOrder, ExactlyOnce> =
            match which {
                Broadcast::Reliable => {
                    reliable_broadcast_closed(items, &members).weaken_consistency()
                }
                Broadcast::Uniform(threshold) => {
                    uniform_reliable_broadcast_closed(items, &members, threshold)
                        .weaken_consistency()
                }
            };

        let mut spec = Spec::new(&mut flow);
        let correct = spec.correct_members(&members, 0..MEMBERS);
        let observed = spec.observe_cluster(delivered);
        match agreement {
            Agreement::Generated => spec.eventual_consistency(observed, correct),
            Agreement::Uniform => uniform_agreement(&mut spec, observed, correct),
        }

        let (exploration, fuzz) = match which {
            Broadcast::Uniform(_) => (Exploration::Fuzz { instances: 8 }, 8),
            Broadcast::Reliable => (Exploration::Exhaustive, 1),
        };
        let compiled: CompiledSim = flow
            .sim()
            .skip_consistency_assertions()
            .with_cluster_size(&members, MEMBERS as usize)
            .unit_test_fuzz_iterations(fuzz)
            .with_provenance()
            .compiled();

        let started = std::time::Instant::now();
        let outcome = Ldfi::new()
            .level(Level::Precise)
            .exploration(exploration)
            .crashable("source", &source)
            .crashable_cluster("member", &members, 0..MEMBERS)
            // Uniform broadcast under the uniform specification runs past the default 256
            // hypotheses (its echo fan-out gives each member many crash points, and the
            // two-member crashes are pruned by vacuity, after a run, rather than by a bound).
            .max_hypotheses(2_000)
            .spec(&spec)
            .search(&compiled, async |_run| {
                for item in DOMAIN {
                    send.send(*item);
                }
                hydro_lang::sim::quiesce().await;
            });
        eprintln!(
            "{which:?} under {agreement:?} agreement: {} hypotheses in {:.1?}\n{outcome}",
            runs(&outcome),
            started.elapsed()
        );
        outcome
    }

    fn runs(outcome: &Outcome) -> usize {
        match outcome {
            Outcome::Counterexample { runs, .. }
            | Outcome::Certificate { runs, .. }
            | Outcome::Budget { runs }
            | Outcome::Vacuous { runs }
            | Outcome::Exhausted { runs } => runs.len(),
        }
    }

    /// The crashed nodes of a fault set, sorted.
    fn crashed_nodes(faults: &FaultSet) -> Vec<String> {
        let mut nodes: Vec<String> = faults
            .iter()
            .filter_map(|f| match f {
                Fault::Crash { node, .. } => Some(node.clone()),
                Fault::Omit(_) => None,
            })
            .collect();
        nodes.sort();
        nodes
    }

    /// Non-uniform reliable broadcast violates uniform agreement: the source crashes after
    /// reaching one member, which delivers and crashes before its echoes reach anyone.
    #[test]
    fn uniform_agreement_reliable_broadcast_counterexample() {
        let outcome = search(Broadcast::Reliable, Agreement::Uniform);
        let Outcome::Counterexample { faults, .. } = outcome else {
            panic!("expected a counterexample to uniform agreement, got {outcome}");
        };
        // Observed (exhaustive schedules, deterministic search, 12 hypotheses):
        // {crash(source after <initial>[→0]:2), crash(member@0 after <echo>[0→0]:2)}. The
        // source sends `1` to everyone and `2` to member 0 only; member 0 delivers `2` (its
        // observation is its first send of the execution), echoes it to itself, and dies
        // before echoing to members 1 and 2, which are correct and never see `2`.
        let rendered: Vec<String> = faults.iter().map(|f| f.to_string()).collect();
        assert_eq!(faults.len(), 2, "{rendered:?}");
        let crashed = crashed_nodes(&faults);
        assert_eq!(crashed[1], "source", "{rendered:?}");
        let member = crashed[0]
            .strip_prefix("member@")
            .unwrap_or_else(|| panic!("{rendered:?}"));
        // The source's last send is item 2 to that member ...
        assert!(
            rendered
                .iter()
                .any(|f| f.starts_with("crash(source after")
                    && f.ends_with(&format!("[→{member}]:2)"))),
            "{rendered:?}"
        );
        // ... and the member's last send is an echo of item 2 that reaches no other member.
        assert!(
            rendered
                .iter()
                .any(|f| f.starts_with(&format!("crash(member@{member} after"))
                    && f.ends_with(&format!("[{member}→{member}]:2)"))),
            "{rendered:?}"
        );
    }

    /// The same search under the generated (non-uniform) specification certifies
    /// (`ec_demo_reliable_broadcast_certifies`), for the side-by-side comparison.
    #[test]
    fn uniform_agreement_reliable_broadcast_generated_spec_certifies() {
        let outcome = search(Broadcast::Reliable, Agreement::Generated);
        assert!(
            matches!(outcome, Outcome::Certificate { .. }),
            "expected a certificate, got {outcome}"
        );
    }

    /// Uniform reliable broadcast with threshold `F + 1 = 2` upholds uniform agreement under
    /// the same fault model (sampled schedules, as in `ec_demo_uniform_broadcast`). Observed:
    /// a certificate after 276 hypotheses at 8 schedules each (not a proof: sampled, and the
    /// specification is hand-written), 171 of them vacuous two-member crashes.
    #[test]
    fn uniform_agreement_uniform_broadcast() {
        let outcome = search(Broadcast::Uniform(2), Agreement::Uniform);
        assert!(
            matches!(outcome, Outcome::Certificate { .. }),
            "expected a certificate, got {outcome}"
        );
    }

    /// The generated specification on uniform broadcast, for the side-by-side comparison.
    #[test]
    fn uniform_agreement_uniform_broadcast_generated_spec() {
        let outcome = search(Broadcast::Uniform(2), Agreement::Generated);
        assert!(
            !matches!(outcome, Outcome::Budget { .. } | Outcome::Vacuous { .. }),
            "{outcome}"
        );
    }
}
