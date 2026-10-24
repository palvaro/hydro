p='hydrofrac/src/provenance/fragile.rs'; s=open(p).read()
# remove debug
i=s.index("        eprintln!(\"DEDUPDBG")
j=s.index("        exclusive.then_some((defer, init_nodes))")
s=s[:i]+s[j:]
o="""    /// Whether only `allowed`, and readers that tag every record they emit ([`Self::tags`]),
    /// read `ptr`'s records.
    fn read_only_by(&self, ptr: *const HydroNode, allowed: &[Reader]) -> bool {
        self.consumers(ptr)
            .iter()
            .all(|r| allowed.contains(r) || matches!(r, Reader::Node(n) if self.tags(*n)))
    }
"""
assert s.count(o)==1
s=s.replace(o,"""    /// Whether only `allowed` read `ptr`'s records, besides readers whose every path to a root
    /// passes a node that tags what it emits ([`Self::covered`]).
    fn read_only_by(&self, ptr: *const HydroNode, allowed: &[Reader]) -> bool {
        self.consumers(ptr).iter().all(|r| {
            allowed.contains(r)
                || matches!(r, Reader::Node(n) if self.covered(*n, &mut HashSet::new()))
        })
    }

    /// Whether every record `ptr` emits reaches the roots only through a node that tags what it
    /// emits ([`Self::tags`]): `ptr` itself, or every reader of it, recursively.
    fn covered(&self, ptr: *const HydroNode, seen: &mut HashSet<*const HydroNode>) -> bool {
        if self.tags(ptr) || !seen.insert(ptr) {
            return true;
        }
        self.consumers(ptr).iter().all(|r| match r {
            Reader::Node(n) => self.covered(*n, seen),
            Reader::Root(_) => false,
        })
    }
""")
open(p,'w').write(s)
print('ok')
