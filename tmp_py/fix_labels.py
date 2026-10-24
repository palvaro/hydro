p='hydrofrac/src/observe/collections.rs'; s=open(p).read()
def rep(o,n,c=1):
    global s
    assert s.count(o)==c,(o,s.count(o))
    s=s.replace(o,n)
rep("""    /// The 0-based position among collections with the same file and display name.
    pub occurrence: usize,
}""","""    /// The 0-based position among collections with the same file and display name.
    pub occurrence: usize,
    /// The address of the labelled node in the enumerated IR: its identity. Two nodes are two
    /// collections, even with one [`EcKey`] (a helper that labels on one line, called twice).
    pub(crate) node: usize,
}""")
rep("""/// inherit the label. Nodes with one [`EcKey`] (one per member, or per instantiation of a
/// library function) are reported once, in first-seen order. A `Tee` stands for what it shares.""",
"""/// inherit the label. Each labelled node is one collection, in first-seen order, even if another
/// node has the same [`EcKey`] (a library function instantiated twice, a helper labelling on one
/// line): merging them would let one collection's facts mask another's missing ones. A `Tee`
/// stands for what it shares.""")
rep("""        by_key: HashMap<EcKey, usize>,""","""        by_node: HashMap<*const HydroNode, usize>,""")
rep("""                let key = ec_key(node);
                let fed = sources(node, cycle_sinks, &self.entries).network;
                let index = *self.by_key.entry(key.clone()).or_insert_with(|| {""",
"""                let key = ec_key(node);
                let fed = sources(node, cycle_sinks, &self.entries).network;
                let ptr = std::ptr::from_ref(node);
                let index = *self.by_node.entry(ptr).or_insert_with(|| {""")
rep("""                        occurrence: 0,
                    });""","""                        occurrence: 0,
                        node: ptr as usize,
                    });""")
rep("""                self.collections[index].fed_by_network |= fed;
                let ptr = std::ptr::from_ref(node);
                if self.nodes""","""                self.collections[index].fed_by_network |= fed;
                if self.nodes""")
rep("""        by_key: HashMap::new(),""","""        by_node: HashMap::new(),""")
open(p,'w').write(s)

p='hydrofrac/src/observe/tap.rs'; s=open(p).read()
rep("""use super::collections::{EcKey, ec_key, is_ec};""","""use super::collections::is_ec;""")
rep("""    let index_of: HashMap<EcKey, usize> = plans
        .iter()
        .enumerate()
        .map(|(i, plan)| (plan.collection.key(), i))
        .collect();""","""    // Each label is one node, by address: the walk below visits every node where the
    // enumeration saw it (it replaces a node only after visiting its inputs).
    let index_of: HashMap<usize, usize> = plans
        .iter()
        .enumerate()
        .map(|(i, plan)| (plan.collection.node, i))
        .collect();
    let mut tapped = vec![0usize; plans.len()];""")
rep("""    // Labelled nodes are re-identified by the enumeration's key (a `Tee` never matches: the
    // enumeration records what it shares).""","""    // Labelled nodes are re-identified by address (a `Tee` never matches: the enumeration
    // records what it shares).""")
rep("""        let Some(&index) = index_of.get(&ec_key(node)) else {
            return;
        };
        if !plans[index].is_searched() {
            return;
        }""","""        let Some(&index) = index_of.get(&(std::ptr::from_ref(node) as usize)) else {
            return;
        };
        if !plans[index].is_searched() {
            return;
        }
        tapped[index] += 1;""")
open(p,'w').write(s)
print('ok')
