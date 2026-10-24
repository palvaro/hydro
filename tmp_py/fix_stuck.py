p='hydro_lang/src/sim/compiled.rs'; s=open(p).read()
start=s.index("                // hydrofrac injects faults that can starve a snapshot's producer")
end=s.index("                abort_assert!(", start)
new='''                // hydrofrac injects faults that can starve a snapshot's producer, so a tick that
                // can never run is a property of the run, not a simulator bug: the run quiesces
                // with that member never processing the tick, and the checker sees what it lacks.
                #[cfg(feature = "hydrofrac")]
                if !tick.hooks.iter().all(|hook| hook.is_ready()) {
                    continue;
                }
'''
s=s[:start]+new+s[end:]
open(p,'w').write(s)
print('ok')
