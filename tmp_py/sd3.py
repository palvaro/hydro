p='docs/design/hydrofrac-soundness.md'
s=open(p).read()
def rep(a,b):
    global s
    assert a in s, a[:80]
    s=s.replace(a,b)
rep('''**Code reference:** `hydrofrac-races` at `7df7e0753a`. Paths are relative to the repository root; `hydrofrac/src/` is''','''**Code reference:** `hydrofrac-races` at `dd5c4d05a5` (first written at `7df7e0753a`; section numbers are unchanged
since). Paths are relative to the repository root; `hydrofrac/src/` is''')
rep('''**The table** (every `HydroNode` kind at `7df7e0753a`):''','''**The table** (every `HydroNode` kind at `dd5c4d05a5`):''')
rep('''**Proved**, given A1, A2 and A13. **Tested:** every choice-free corpus program is cross-checked (`reliable_broadcast_closed`,
S1, S2).''','''**Proved**, given A1, A2 and A13. **Not tested** by any gate at `dd5c4d05a5`:
- the choice-free corpus programs (`reliable_broadcast_closed`, S1, S2) are also fragile-free, so the shortcut decides
  nothing there that the fragile-free rule would not;
- forcing `choice_free = false` changes no gate's verdict (found by the dead-code audit).''')
open(p,'w').write(s)
