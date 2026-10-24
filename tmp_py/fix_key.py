p='hydrofrac/src/observe/collections.rs'; s=open(p).read()
i=s.index("    /// The identity of the collection among the program's labelled nodes.\n    pub(crate) fn key(&self) -> EcKey {")
j=s.index("    }\n", s.index("        )\n", i))+6
s=s[:i].rstrip()+"\n"+s[j:]
open(p,'w').write(s)
print(s[s.index("impl EcCollection"):s.index("impl EcCollection")+400])
