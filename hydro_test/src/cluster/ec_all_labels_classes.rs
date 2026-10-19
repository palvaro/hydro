//! The collections the automatic specification (`Spec::all_labels`) does **not** check, and
//! says so: here a label minted inside a tick. The program compiles and runs under
//! `Ldfi::search_all_labels`; the verdict for the out-of-class collection is `NotChecked`
//! with the reason, not silence. (The other such class, a labelled lattice `Singleton`, is
//! `abd`'s replica register, tested in `hydro_std::ec_inference_demos::abd`: the demo's
//! message types are private to `hydro_std`, so it only simulates from there.)

#[cfg(test)]
mod tests {
    use hydro_lang::live_collections::stream::{ExactlyOnce, TotalOrder};
    use hydro_lang::location::cluster::EventualConsistency;
    use hydro_lang::prelude::*;
    use hydro_lang::sim::compiled::CompiledSim;
    use hydro_lang::sim::ldfi::{LabelVerdict, Ldfi, Level, NotCheckedReason, Outcome};
    use hydro_lang::sim::spec::Spec;

    pub struct Source;
    pub struct Member;

    /// A label minted inside a tick (an `assert_has_consistency_of` on a batched stream) is
    /// not checked, with the reason; the top-level collections around it are.
    #[test]
    fn all_labels_reports_a_label_inside_a_tick() {
        const MEMBERS: u32 = 3;
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

        let spec = Spec::all_labels(&mut flow);

        let compiled: CompiledSim = flow
            .sim()
            .skip_consistency_assertions()
            .with_cluster_size(&members, MEMBERS as usize)
            .with_provenance()
            .compiled();

        let results = Ldfi::new()
            .level(Level::Precise)
            .crashable("source", &source)
            .crashable_cluster("member", &members, 0..MEMBERS)
            .crash_budget(1)
            .spec(&spec)
            .search_all_labels(&compiled, async |_run| {
                send.send(1);
                send.send(2);
                hydro_lang::sim::quiesce().await;
            });
        eprintln!("{results}");
        assert!(
            results.verdicts.iter().any(|r| r.collection.in_tick
                && matches!(
                    r.verdict,
                    LabelVerdict::NotChecked(NotCheckedReason::InsideTick)
                )),
            "the label minted inside the tick must be reported as inside a tick: {results}"
        );
        // The single-hop broadcast the tick consumes is checked, and the source's crash
        // between two fan-out sends breaks it.
        assert!(
            results
                .counterexamples()
                .any(|(r, _)| !r.collection.in_tick),
            "the top-level broadcast is in class and violated by a sender crash: {results}"
        );
        assert!(
            !results
                .verdicts
                .iter()
                .any(|r| matches!(r.verdict, LabelVerdict::Searched(Outcome::Budget { .. }))),
            "{results}"
        );
    }
}
