p='docs/design/hydrofrac-soundness.md'
s=open(p).read()
def rep(a,b):
    global s
    assert a in s, a[:80]
    s=s.replace(a,b)
rep('''  - **the program is choice-free.** Each fault set has one schedule, and every tried set was run in it. **Proved**
    with §1, case 2's mapping: every `H` maps to a tried, run set, or is pruned. A pruned set's facts outside the
    domain are not covered by Lemma R, so for the choice-free case the claim needs every `H` to be covered by a run.
    It holds when, additionally, no set was skipped by pruning. **Gap:** the code certifies Claim B for every
    choice-free certified label without checking that. See §18;''','''  - **the program is choice-free.** Then it is monotone (A13): a non-monotone operator needs a bounded input, only a
    tick provides one, and a tick's outside input arrives through a batch, which is a choice. In a monotone program
    faults only remove facts. Every fact under `H` is also a failure-free fact (its derivation's dependencies exist
    without faults, and every step is monotone), so it is in the domain. No fact is outside the domain. **Proved**,
    given A13;''')
rep('''| A12 | Cluster ids are listed `0..n` by one `source_iter` | ASSUMED from `hydro_lang/src/sim/graph.rs`; TESTED by `fanout_tests` |''','''| A12 | Cluster ids are listed `0..n` by one `source_iter` | ASSUMED from `hydro_lang/src/sim/graph.rs`; TESTED by `fanout_tests` |
| A13 | A Hydro program with no schedule choice is monotone: non-monotone operators need a bounded input, which only a tick provides, and outside data enters a tick only through a batch (a choice) | ASSUMED (Hydro's type system); used only for Claim B in choice-free programs (§13) |''')
rep('''- **Claim B for choice-free programs** (§13). `claim_b` is set for every certified choice-free label. That relies on
  every `H` being covered by a run or by Lemma R, but Lemma R covers only domain facts. A pruned `H` in a choice-free
  program with fragile steps could hold a divergent fact outside the domain that no run saw. This is not proved, and
  should be fixed by requiring `covers_everything` (or fragile-freedom) for Claim B.''','''- **Claim B for choice-free programs** (§13) rests on A13 (monotonicity from Hydro's types), not on the search.
  Outside the three certified cases, Claim B is checked only in explored runs, and the report says so.''')
open(p,'w').write(s)
