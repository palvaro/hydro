p='hydrofrac/src/check/engine.rs'
s=open(p).read()
a=s.index('        let results = Engine::new(self, sim, &labels, &mut body).check();')
b=s.index('        results\n',a)+len('        results\n')
s=s[:a]+'        Engine::new(self, sim, &labels, &mut body).check()\n'+s[b:]
open(p,'w').write(s)
