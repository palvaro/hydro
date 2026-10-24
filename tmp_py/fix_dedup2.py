p='hydrofrac/src/check/brute_tests.rs'; s=open(p).read()
o='.find(|l| l.name() == "facts")'
assert s.count(o)==1
s=s.replace(o,'.find(|l| l.plan.collection.display_name() == "facts")')
open(p,'w').write(s)

p='hydrofrac/src/provenance/fragile.rs'; s=open(p).read()
def rep(o,n):
    global s
    assert s.count(o)==1,o
    s=s.replace(o,n)
rep("""    let scan = View::new(ir);""","""    let scan = View::new(ir, trusted);""")
rep("""struct View<'a> {
    ir: &'a [HydroRoot],""","""struct View<'a> {
    ir: &'a [HydroRoot],
    /// The nodes inside a trusted `quorum` ([`classify`] does not tag them).
    trusted: &'a HashSet<usize>,""")
rep("""    fn new(ir: &'a [HydroRoot]) -> Self {""","""    fn new(ir: &'a [HydroRoot], trusted: &'a HashSet<usize>) -> Self {""")
rep("""        let mut view = View {
            ir,
            cycle_sinks,""","""        let mut view = View {
            ir,
            trusted,
            cycle_sinks,""")
rep("""    /// Whether only `allowed` read `ptr`'s records.
    fn read_only_by(&self, ptr: *const HydroNode, allowed: &[Reader]) -> bool {
        self.consumers(ptr).iter().all(|r| allowed.contains(r))
    }""","""    /// Whether only `allowed`, and readers that tag every record they emit ([`Self::tags`]),
    /// read `ptr`'s records.
    fn read_only_by(&self, ptr: *const HydroNode, allowed: &[Reader]) -> bool {
        self.consumers(ptr)
            .iter()
            .all(|r| allowed.contains(r) || matches!(r, Reader::Node(n) if self.tags(*n)))
    }

    /// Whether `ptr` is fragile whatever its context: a fold, reduce or scan, a
    /// `cross_singleton` (`filter_if`), `enumerate`, `chain_first`, or a join inside a tick,
    /// outside a trusted `quorum`, with an input that is not constant. Its records carry its
    /// tag, so a record it reads needs no tag of its own.
    fn tags(&self, ptr: *const HydroNode) -> bool {
        use HydroNode::*;
        // SAFETY: as in `classify`.
        let node = unsafe { &*ptr };
        let kind = match node {
            Fold { .. } | FoldKeyed { .. } | Reduce { .. } | ReduceKeyed { .. } | Scan { .. } => {
                true
            }
            CrossSingleton { .. } | Enumerate { .. } | ChainFirst { .. } => true,
            Join { .. } | CrossProduct { .. } | JoinHalf { .. } => {
                !node.metadata().location_id.is_top_level()
            }
            _ => false,
        };
        kind && !self.trusted.contains(&node_key(node))
            && !inputs(node).into_iter().all(|i| self.constant(i))
    }""")
rep("""    /// Only if nothing else reads the idiom's state: `seen` is read only by the negation and the
    /// cycle's sink, the sink only by its `CycleSink`, the cycle source only by the carrying
    /// `DeferTick`, which only `seen` reads, and the initial state's nodes only by each other and
    /// `seen`. Another reader of any of them (`seen.clone().all_ticks()`, say) would see records
    /// that exist only if a later tick runs.""","""    /// Only if nothing else reads the idiom's state untagged: `seen` is read only by the negation
    /// and the cycle's sink, the sink only by its `CycleSink`, the cycle source only by the
    /// carrying `DeferTick`, which only `seen` reads, and the initial state's nodes only by each
    /// other and `seen`, besides readers that tag everything they emit ([`Self::tags`]). Another
    /// reader of any of them (`seen.clone().all_ticks()`, say) would emit records that exist only
    /// if a later tick runs, untagged.""")
open(p,'w').write(s)
print('ok')
