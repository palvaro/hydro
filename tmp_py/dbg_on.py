p='hydrofrac/src/provenance/fragile.rs'; s=open(p).read()
o="""        let exclusive = self.read_only_by(state, &[Reader::Node(me), Reader::Node(sink)])
            && self.read_only_by(sink, &[Reader::Root(root)])
            && self.read_only_by(source, &[Reader::Node(defer)])
            && self.read_only_by(defer, &[Reader::Node(state)])
            && init_nodes.iter().all(|&n| self.read_only_by(n, &in_init));"""
assert s.count(o)==1
s=s.replace(o,o+"""
        eprintln!("DEDUPDBG state={} sink={} source={} defer={} init={} consumers(state)={:?} consumers(sink)={:?} me={:?} sinkp={:?} root={}",
            self.read_only_by(state, &[Reader::Node(me), Reader::Node(sink)]),
            self.read_only_by(sink, &[Reader::Root(root)]),
            self.read_only_by(source, &[Reader::Node(defer)]),
            self.read_only_by(defer, &[Reader::Node(state)]),
            init_nodes.iter().all(|&n| self.read_only_by(n, &in_init)),
            self.consumers(state).iter().map(|r| match r { Reader::Node(n) => format!("{:?}@{:?}", unsafe { &**n }.print_root(), n), Reader::Root(i) => format!("root{i}") }).collect::<Vec<_>>(),
            self.consumers(sink).iter().map(|r| match r { Reader::Node(n) => format!("{:?}@{:?}", unsafe { &**n }.print_root(), n), Reader::Root(i) => format!("root{i}") }).collect::<Vec<_>>(),
            me, sink, root);
        for &n in &init_nodes { if !self.read_only_by(n, &in_init) { eprintln!("DEDUPDBG init node {} read by {:?}", unsafe { &*n }.print_root(), self.consumers(n).iter().map(|r| match r { Reader::Node(n) => unsafe { &**n }.print_root(), Reader::Root(i) => format!("root{i}") }).collect::<Vec<_>>()); } }""")
open(p,'w').write(s)
print('ok')
