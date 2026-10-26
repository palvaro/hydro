//! The collections the generated specification does **not** check, and says so: here a label
//! minted inside a tick. The program compiles and runs under
//! [`EcCheck`](hydrofrac::EcCheck); the verdict for the out-of-class collection is
//! `NotChecked` with the reason, not silence. Ported from the research branch's
//! `ec_all_labels_classes.rs` (`0d801bbafc`).
//!
//! The other such class, a labelled `Singleton` whose value changes, is `abd`'s replica
//! register, tested in `hydro_std::ec_inference_demos::abd` (its message types are private to
//! `hydro_std`, so it only simulates from there).

#[cfg(test)]
mod tests {
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::location::cluster::EventualConsistency;
    use hydro_lang::prelude::*;
    // hydrofrac is a dev-dependency and host-only: the crate's staged copy (what simulation
    // dylibs link) must not name it.
    #[cfg(stageleft_runtime)]
    use hydrofrac::EcCheck;
    #[cfg(stageleft_runtime)]
    use hydrofrac::Verdict;
    #[cfg(stageleft_runtime)]
    use hydrofrac::observe::NotCheckedReason;

    pub struct Source;
    pub struct Member;

    /// A label minted inside a tick (an `assert_has_consistency_of` on a batched stream) is
    /// not checked, with the reason; the top-level collections around it are.
    #[test]
    fn a_label_inside_a_tick_is_reported_not_checked() {
        const MEMBERS: usize = 3;
        let mut flow = FlowBuilder::new();
        let source = flow.process::<Source>();
        let members = flow.cluster::<Member>();
        let (send, items) = source.sim_input::<u32, TotalOrder, ExactlyOnce>();

        // Minted at the top level by the transport.
        let at_members = items.broadcast_closed(&members, TCP.fail_stop().name("bcast").bincode());
        // Minted again inside a tick, by hand, on the batched (label-dropping) stream.
        let tick = members.tick();
        let in_tick = at_members
            .batch(&tick, nondet!(/** test */))
            .assert_has_consistency_of::<Tick<Cluster<'_, Member, EventualConsistency>>>(
                manual_proof!(/** test: the batch of a converging stream converges */),
            )
            .map(q!(|x| x * 10));
        let _out = in_tick
            .all_ticks()
            .weaken_consistency()
            .sim_cluster_output();

        let mut check = EcCheck::new();
        let sim = check
            .instrument(flow.sim().with_cluster_size(&members, MEMBERS))
            .compiled();
        let results = check.run(&sim, async || {
            send.send(1);
            send.send(2);
        });
        eprintln!("{results}");

        assert!(
            results.results().iter().any(|r| r.plan.collection.in_tick
                && matches!(r.verdict, Verdict::NotChecked(NotCheckedReason::InsideTick))),
            "the label minted inside the tick must be reported as inside a tick:\n{results}"
        );
        // The single-hop broadcast the tick consumes is checked, and the source's crash
        // between two fan-out sends breaks it.
        assert!(
            results
                .results()
                .iter()
                .any(|r| !r.plan.collection.in_tick && r.verdict.counterexample().is_some()),
            "the top-level broadcast is in class and violated by a sender crash:\n{results}"
        );
        assert!(
            !results
                .results()
                .iter()
                .any(|r| matches!(r.verdict, Verdict::Budget { .. })),
            "{results}"
        );
    }
}
