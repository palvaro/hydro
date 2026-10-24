p = 'hydrofrac/src/observe/collections.rs'
s = open(p).read()

def rep(old, new, count=1):
    global s
    assert s.count(old) == count, (old[:80], s.count(old))
    s = s.replace(old, new)

rep("""fn sources(node: &HydroNode, cycle_sinks: &HashMap<CycleId, *const HydroNode>) -> Sources {
    fn go(
        node: &HydroNode,
        cycle_sinks: &HashMap<CycleId, *const HydroNode>,
        seen: &mut HashSet<*const HydroNode>,
        out: &mut Sources,
    ) {
        if !seen.insert(std::ptr::from_ref(node)) {
            return;
        }
        match node {""",
"""/// The tick a location belongs to: itself for a tick, the enclosing tick for an atomic region,
/// `None` at top level.
pub(crate) fn tick_of(location: &LocationId) -> Option<&LocationId> {
    match location {
        LocationId::Tick(..) => Some(location),
        LocationId::Atomic(tick) => tick_of(tick),
        LocationId::Process(_) | LocationId::Cluster(_) => None,
    }
}

/// Each tick's entry points: the `batch`es and `atomic`s whose records enter it. The simulator
/// runs a tick only when one of them has something to release, so whether a tick runs at all
/// (and so whether its constants and cycles emit anything) depends on what feeds them.
pub(crate) fn tick_entries(ir: &[HydroRoot]) -> HashMap<LocationId, Vec<*const HydroNode>> {
    let (mut seen, mut out) = (HashSet::new(), HashMap::<LocationId, Vec<_>>::new());
    let mut stack: Vec<*const HydroNode> = ir.iter().map(|r| std::ptr::from_ref(r.input())).collect();
    while let Some(ptr) = stack.pop() {
        if !seen.insert(ptr) {
            continue;
        }
        // SAFETY: every pointer points into `ir`, which is borrowed for the whole walk.
        let node = unsafe { &*ptr };
        match node {
            HydroNode::Placeholder => continue,
            HydroNode::Batch { metadata, .. } | HydroNode::BeginAtomic { metadata, .. } => {
                if let Some(tick) = tick_of(&metadata.location_id) {
                    out.entry(tick.clone()).or_default().push(ptr);
                }
            }
            _ => {}
        }
        match node {
            HydroNode::Tee { inner, .. }
            | HydroNode::Reference { inner, .. }
            | HydroNode::PartitionSide { inner, .. } => {
                stack.push(std::ptr::from_ref(&*inner.0.borrow()));
            }
            _ => stack.extend(
                node.input()
                    .into_iter()
                    .chain(closure_refs(node))
                    .map(std::ptr::from_ref),
            ),
        }
    }
    out
}

/// What feeds `node`: networks, membership, other sources. A node inside a tick is also fed by
/// whatever feeds that tick's entry points ([`tick_entries`]): if no delivery reaches them, the
/// tick never runs.
fn sources(
    node: &HydroNode,
    cycle_sinks: &HashMap<CycleId, *const HydroNode>,
    entries: &HashMap<LocationId, Vec<*const HydroNode>>,
) -> Sources {
    fn go(
        node: &HydroNode,
        cycle_sinks: &HashMap<CycleId, *const HydroNode>,
        entries: &HashMap<LocationId, Vec<*const HydroNode>>,
        seen: &mut HashSet<*const HydroNode>,
        out: &mut Sources,
    ) {
        if !seen.insert(std::ptr::from_ref(node)) {
            return;
        }
        if !matches!(node, HydroNode::Placeholder)
            && let Some(tick) = tick_of(&node.metadata().location_id)
        {
            for &entry in entries.get(tick).into_iter().flatten() {
                // SAFETY: see the comment at the construction of `cycle_sinks`.
                go(unsafe { &*entry }, cycle_sinks, entries, seen, out);
            }
        }
        match node {""")
rep("""                go(&inner.0.borrow(), cycle_sinks, seen, out);""",
"""                go(&inner.0.borrow(), cycle_sinks, entries, seen, out);""")
rep("""                    go(unsafe { &**sink }, cycle_sinks, seen, out);""",
"""                    go(unsafe { &**sink }, cycle_sinks, entries, seen, out);""")
rep("""                    go(input, cycle_sinks, seen, out);""",
"""                    go(input, cycle_sinks, entries, seen, out);""")
rep("""    let mut out = Sources::default();
    go(node, cycle_sinks, &mut HashSet::new(), &mut out);
    out""",
"""    let mut out = Sources::default();
    go(node, cycle_sinks, entries, &mut HashSet::new(), &mut out);
    out""")
open(p, 'w').write(s)
print("ok")
