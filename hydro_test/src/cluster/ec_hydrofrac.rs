//! hydrofrac's `EcCheck` on every operator in `hydro_lang` that
//! *produces* a collection labelled `EventualConsistency` (docs/design/hydrofrac-lite.md §2,
//! §7). Ported from the research branch's `ec_ldfi.rs` (`0d801bbafc`).
//!
//! The label promises that replicas compute the same contents given the same inputs. These
//! tests ask whether the fault model the transport declares can withhold inputs from some
//! replicas, which is the premise the label leaves to the program. The producers:
//!
//! - `Stream::broadcast_closed` from a process to a cluster, over `fail_stop`. The
//!   transport's guarantee is `EventualConsistency`, because a failed connection models the
//!   *recipient* as having failed. The sender is not considered, so a crash of the sender
//!   between two sends of one fan-out leaves two members with different sets.
//! - The same over `lossy_delayed_forever`. A closed broadcast sends once and nothing
//!   retransmits, so a message delayed forever is a message a member never gets: one omission.
//! - `Stream::broadcast_closed` from a cluster to a cluster, over `fail_stop`: a source
//!   member crashing mid-fan-out.
//! - The echoing reliable broadcast of [`super::reliable_broadcast`], as the control. The same
//!   generated specification certifies it.
//!
//! Everything is crashable and the crash bound sweeps upwards from zero (§4.5), so each
//! counterexample also says how many crashes the label tolerates. The research harnesses
//! declared a crashable set and a `Spec` per test; here `EcCheck` finds
//! every label itself, and a test that observed one stream reads that stream's label.

#[cfg(test)]
mod tests {
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::prelude::*;
    // hydrofrac is a dev-dependency and host-only: the crate's staged copy (what simulation
    // dylibs link) must not name it.
    #[cfg(stageleft_runtime)]
    use hydrofrac::check::LabelResult;
    #[cfg(stageleft_runtime)]
    use hydrofrac::search::{CrashAt, Fault, Tolerance};
    #[cfg(stageleft_runtime)]
    use hydrofrac::{EcCheck, Results, Verdict};
    use serde::{Deserialize, Serialize};

    use crate::cluster::reliable_broadcast::{Broadcaster, Member, Payload, broadcast};

    pub struct Source;

    #[cfg(stageleft_runtime)]
    const MEMBERS: usize = 3;
    #[cfg(stageleft_runtime)]
    const SOURCES: usize = 2;
    /// Schedules sampled per hypothesis for the echoing broadcast. With plain `batch` at every
    /// member, its schedule space is too large to enumerate: the failure-free probe alone ran
    /// past 266,000 schedules without finishing. The research harness enumerated it (4,076
    /// schedules over 21 hypotheses) because its batches were driven by `BatchHook`s, which
    /// lite does not have.
    #[cfg(stageleft_runtime)]
    const ECHO_SCHEDULES: usize = 64;

    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
    pub struct Item(u32);

    /// Which transport the closed broadcast declares.
    #[derive(Clone, Copy, Debug)]
    enum Transport {
        FailStop,
        LossyDelayedForever,
    }

    /// `broadcast_closed` from one process to a cluster of three, two items. The labelled
    /// collection is `at_members`.
    #[cfg(stageleft_runtime)]
    fn process_to_cluster(transport: Transport) -> Results {
        let mut flow = FlowBuilder::new();
        let source = flow.process::<Source>();
        let members = flow.cluster::<Member>();
        let (send, items) = source.sim_input::<Item, TotalOrder, ExactlyOnce>();
        match transport {
            Transport::FailStop => {
                let at_members =
                    items.broadcast_closed(&members, TCP.fail_stop().name("bcast").bincode());
                let _out = at_members.sim_cluster_output();
            }
            Transport::LossyDelayedForever => {
                let at_members = items.broadcast_closed(
                    &members,
                    TCP.lossy_delayed_forever().name("bcast").bincode(),
                );
                let _out = at_members.sim_cluster_output();
            }
        }

        let mut check = EcCheck::new();
        let sim = check
            .instrument(flow.sim().with_cluster_size(&members, MEMBERS))
            .compiled();
        check.run(&sim, async || {
            send.send(Item(1));
            send.send(Item(2));
        })
    }

    /// `broadcast_closed` from a cluster of two sources to a cluster of three members over
    /// `fail_stop`, each source broadcasting its own item.
    #[cfg(stageleft_runtime)]
    fn cluster_to_cluster() -> Results {
        let mut flow = FlowBuilder::new();
        let sources = flow.cluster::<Source>();
        let members = flow.cluster::<Member>();
        let (send, items) = sources.sim_input::<Item, TotalOrder, ExactlyOnce>();

        let at_members = items
            .broadcast_closed(&members, TCP.fail_stop().name("bcast").bincode())
            .values();
        let _out = at_members.sim_cluster_output();

        let mut check = EcCheck::new();
        let sim = check
            .instrument(
                flow.sim()
                    .with_cluster_size(&sources, SOURCES)
                    .with_cluster_size(&members, MEMBERS),
            )
            .compiled();
        check.run(&sim, async || {
            send.send(0, Item(10));
            send.send(1, Item(20));
        })
    }

    /// The broadcast of `reliable_broadcast`, with or without the echo (every member
    /// re-broadcasts what it first sees before delivering it). Its output is labelled by an
    /// `assert_has_consistency_of` with no binding, so the label is named by its operator,
    /// `AssertIsConsistent`.
    #[cfg(stageleft_runtime)]
    fn echo_broadcast(echo: bool, check: EcCheck) -> Results {
        let mut flow = FlowBuilder::new();
        let broadcaster = flow.process::<Broadcaster>();
        let members = flow.cluster::<Member>();
        let (source_send, source) = broadcaster.sim_input::<Payload, TotalOrder, ExactlyOnce>();
        let _out = broadcast(
            &members,
            source,
            echo,
            nondet!(/** explored by the check */),
            nondet!(/** explored by the check */),
        )
        .sim_cluster_output();

        let mut check = check;
        let sim = check
            .instrument(flow.sim().with_cluster_size(&members, MEMBERS))
            .compiled();
        check.run(&sim, async || {
            source_send.send(Payload(42));
        })
    }

    /// The label keyed `(file, name, occurrence)` (docs/design/hydrofrac-lite.md §7): `name` is
    /// the collection's binding, or its operator when it has none.
    #[cfg(stageleft_runtime)]
    fn label<'r>(
        results: &'r Results,
        file: &str,
        name: &str,
        occurrence: usize,
    ) -> &'r LabelResult {
        results
            .results()
            .iter()
            .find(|l| {
                let c = &l.plan.collection;
                c.display_name() == name
                    && c.occurrence == occurrence
                    && c.file.as_deref().is_some_and(|f| f.ends_with(file))
            })
            .unwrap_or_else(|| panic!("no label ({file}, {name}, {occurrence}):\n{results}"))
    }

    /// The crash in a counterexample, which must be the only fault: its node's name and where
    /// it died.
    #[cfg(stageleft_runtime)]
    fn the_crash<'r>(
        results: &Results,
        label: &'r LabelResult,
    ) -> (&'r str, &'r CrashAt<hydrofrac::fault::MessageId>) {
        let cex = label
            .verdict
            .counterexample()
            .unwrap_or_else(|| panic!("{}: expected a counterexample:\n{results}", label.name()));
        let faults: Vec<_> = cex.faults.iter().collect();
        let [Fault::Crash { node, at }] = faults[..] else {
            panic!("{}: expected one crash:\n{results}", label.name());
        };
        (node.as_str(), at)
    }

    /// Process → cluster over `fail_stop`: the label is `EventualConsistency`, and one crash of
    /// the sender between two fan-out sends refutes its premise. No omission is admitted on
    /// `fail_stop`, so zero crashes are certified first.
    #[test]
    fn ec_broadcast_closed_fail_stop_breaks_under_a_sender_crash() {
        let results = process_to_cluster(Transport::FailStop);
        eprintln!("{results}");
        let label = label(&results, "ec_hydrofrac.rs", "at_members", 0);
        let cex = label.verdict.counterexample().expect("a counterexample");
        assert_eq!(cex.tolerates, Some(0), "{results}");
        let (node, at) = the_crash(&results, label);
        assert!(node.starts_with("Source"), "the sender's crash: {results}");
        assert!(matches!(at, CrashAt::After(_)), "mid fan-out: {results}");
    }

    /// Process → cluster over `lossy_delayed_forever`: the label is `EventualConsistency` too,
    /// on the reading that a dropped message is only delayed. Nothing retransmits a closed
    /// broadcast, so one omission refutes the premise, at the first hypothesis after the probe
    /// and with no crash at all.
    #[test]
    fn ec_broadcast_closed_lossy_delayed_forever_breaks_under_one_omission() {
        let results = process_to_cluster(Transport::LossyDelayedForever);
        eprintln!("{results}");
        let label = label(&results, "ec_hydrofrac.rs", "at_members", 0);
        let cex = label.verdict.counterexample().expect("a counterexample");
        assert_eq!(cex.tolerates, None, "omissions alone break it: {results}");
        assert_eq!(cex.faults.len(), 1, "one omission suffices: {results}");
        assert!(
            matches!(cex.faults.iter().next(), Some(Fault::Omit(_))),
            "{results}"
        );
        assert_eq!(cex.hypothesis, 1, "{results}");
    }

    /// Cluster → cluster over `fail_stop`: a source member crashing mid-fan-out. Two labels are
    /// bound to `at_members`: the keyed stream `broadcast_closed` yields (occurrence 0), and its
    /// `values()` (occurrence 1), which is what the research harness observed. Both break.
    #[test]
    fn ec_broadcast_closed_cluster_to_cluster_breaks_under_a_source_crash() {
        let results = cluster_to_cluster();
        eprintln!("{results}");
        for occurrence in [0, 1] {
            let label = label(&results, "ec_hydrofrac.rs", "at_members", occurrence);
            let cex = label.verdict.counterexample().expect("a counterexample");
            assert_eq!(cex.tolerates, Some(0), "{results}");
            let (node, at) = the_crash(&results, label);
            assert!(
                node.starts_with("Source@"),
                "a source member's crash: {results}"
            );
            assert!(matches!(at, CrashAt::After(_)), "mid fan-out: {results}");
        }
    }

    /// Control: the generated specification separates the protocols. The plain broadcast's
    /// deliveries break at one crash of the broadcaster (exhaustively); the echoing broadcast's
    /// survive every admissible combination of crashes (any number of them, as long as two
    /// members are left correct), over [`ECHO_SCHEDULES`] sampled schedules per hypothesis.
    #[test]
    fn ec_generated_spec_separates_plain_from_reliable_broadcast() {
        let plain = echo_broadcast(false, EcCheck::new());
        eprintln!("{plain}");
        let deliveries = label(&plain, "reliable_broadcast.rs", "AssertIsConsistent", 0);
        let cex = deliveries.verdict.counterexample().expect("plain breaks");
        assert_eq!(cex.tolerates, Some(0), "{plain}");
        let (node, _) = the_crash(&plain, deliveries);
        assert!(node.starts_with("Broadcaster"), "{plain}");

        let reliable = echo_broadcast(true, EcCheck::new().sample_schedules(ECHO_SCHEDULES));
        eprintln!("{reliable}");
        let deliveries = label(&reliable, "reliable_broadcast.rs", "AssertIsConsistent", 0);
        assert!(
            matches!(
                deliveries.verdict,
                Verdict::Certified {
                    tolerates: Tolerance::All
                }
            ),
            "reliable broadcast: expected a certificate under every admissible crash set:\n{reliable}"
        );
    }

    /// Every label of a plain `broadcast_closed`, with nothing named by the test: the one
    /// labelled collection is violated by the source's crash between two of its fan-out sends,
    /// and it is the only label.
    /// The echoing broadcast explored exhaustively. While the check kept every schedule's record
    /// until a run ended, this ran out of memory: about 37 KiB per schedule, 6.4 GiB after
    /// 150,000 schedules. Folding each schedule as it finishes keeps it flat at about 215 MiB.
    /// It is still too large to finish: its failure-free probe alone passes 240,000 schedules
    /// (about 400 per second, debug build) and keeps going. Run by hand:
    /// `cargo test -p hydro_test --lib ec_echo_broadcast_exhaustively -- --ignored --nocapture`.
    #[test]
    #[ignore = "exhaustive: the probe alone passes 240,000 schedules"]
    fn ec_echo_broadcast_exhaustively() {
        let start = std::time::Instant::now();
        let results = echo_broadcast(true, EcCheck::new());
        let schedules: usize = results.results().iter().map(|l| l.stats.schedules).sum();
        eprintln!(
            "{results}\n{schedules} schedules in {:?} ({:.0}/s)",
            start.elapsed(),
            schedules as f64 / start.elapsed().as_secs_f64()
        );
    }

    #[test]
    fn ec_broadcast_closed_all_labels() {
        let results = process_to_cluster(Transport::FailStop);
        eprintln!("{results}");
        let failing: Vec<_> = results
            .results()
            .iter()
            .filter(|l| l.verdict.fails())
            .collect();
        let [label] = failing[..] else {
            panic!("exactly one label should break:\n{results}");
        };
        let (node, _) = the_crash(&results, label);
        assert!(node.starts_with("Source"), "{results}");
        assert!(!results.holds());
    }

    /// Each counterexample's Lamport diagram is written under
    /// `target/hydrofrac/<test>/<label>/` with nothing asked of the test, and the report names
    /// it. The `.dot` is always there and draws the crash; the `.svg` is there when Graphviz is
    /// installed.
    #[test]
    fn ec_broadcast_closed_all_labels_writes_diagrams() {
        let results = process_to_cluster(Transport::FailStop);
        let label = label(&results, "ec_hydrofrac.rs", "at_members", 0);
        let cex = label.verdict.counterexample().expect("a counterexample");
        let diagram = cex.diagram.as_ref().expect("the diagram was written");
        let dot = diagram.with_extension("dot");
        assert!(dot.is_file(), "{}", dot.display());
        assert!(
            dot.parent()
                .unwrap()
                .to_string_lossy()
                .contains("hydrofrac/ec_broadcast_closed_all_labels_writes_diagrams/"),
            "{}",
            dot.display()
        );
        let source = std::fs::read_to_string(&dot).unwrap();
        assert!(source.starts_with("digraph"), "{source}");
        assert!(
            source.contains('☠'),
            "the source's crash is drawn: {source}"
        );

        let neato = std::process::Command::new("neato")
            .arg("-V")
            .output()
            .is_ok_and(|o| o.status.success());
        let svg = diagram.with_extension("svg");
        assert_eq!(svg.is_file(), neato, "{}", svg.display());
        assert!(results.to_string().contains("diagram: "), "{results}");
    }
}
