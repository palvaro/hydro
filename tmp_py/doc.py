p='docs/design/hydrofrac-races.md'
s=open(p).read()
def rep(a,b):
    global s
    assert a in s, a[:80]
    s=s.replace(a,b)
rep('''- **Exact crash points.** Suppose the program is fragile-free, so the probe has seen every
  message any node can send, and every message `n` can send lies in `m`'s chain. Then
  `crash(n after m)` cuts exactly the chain after `m`, in every run.
  - Such a set's facts are the same in every schedule. It is run on one schedule.
  - If `n` is not a member of the label's cluster, a fact the set loses everywhere is dead
    (`Sweep::mark_dead`). The argument is the start-crash one: a derivation under a superset
    sends from `n` only messages up to `m`, which the rule spares.
  - For completion, every crash point of such an `n` is a candidate. Mapping a crash after an
    unnamed message onto the dead set's crash point would excuse a pair the original crash
    does not.''','''- **Exact crash points.** Suppose the program is fragile-free, so the probe has seen every
  message any node can send, and every message `n` can send lies in `m`'s chain. Then
  `crash(n after m)` touches only proved literals (§12.1), and cuts exactly the chain after
  `m`, in every run.
  - Such a set's facts are the same in every schedule. It is run on one schedule.
  - If `n` is not a member of the label's cluster, a fact the set loses everywhere is dead
    (`Sweep::mark_dead`), a row with no literal. The argument is the start-crash one: a
    derivation under a superset sends from `n` only messages up to `m`, which the rule spares.
  - Every crash point the runs revealed is a candidate (§12.2), so no image lands on a dead
    set's crash point unless the original crash is that one.''')
rep('''- **Completion carries over.** Lite's argument (§4.5 of the lite doc) needs one property: `crash(n)` kills
  everything any other crash of `n` kills. That still holds.''','''- **Completion** is §12.2's: the candidates are the faults the runs revealed, plus one unseen crash per
  node.''')
rep('''- **Completion.** Each `(channel, sender, payload)` is one totally ordered chain, and chains are
  disjoint.
  - A crash after a message `x` that no clause names has an image: the crash after the last
    named message that precedes `x` in its chain. Both spare the same named sends, so both
    kill the same named deliveries.
  - If no named message precedes `x`, the image is `crash(n)`, as before.
  - The stand-in for `crash(n)` used with exemptions must be a crash after an unnamed message
    that no named message precedes.''','''- **Chains.** Each `(channel, sender, payload)` is one totally ordered chain, and chains are
  disjoint. A crash literal `after(d, m)` is fixed when `d` and `m` lie in one chain, and free
  otherwise (§12.1).''')
rep('''4. **Conclude.**
   - When no admissible `H` is left over the completed universe (lite's completion, with §4.2's rule), the label
     is certified up to the bound, and the crash sweep goes on as in lite.
   - **Universe for pairs with no robust derivation.** Such a pair makes *every* fault set admissible, so the
     candidate faults its robust clauses would propose are not enough. For these pairs the universe is the
     brute-force enumerator's (`check/brute_force.rs` `harvest`): every lossy message and every crash point seen
     in any run, grown to a fixpoint. The ILP then proposes every fault set over it that is not skipped. So
     these labels cost about what brute force costs, minus the skipped sets.''','''4. **Conclude.** When no untried admissible `H` is left over the candidates, the label is certified up to the
   bound, and the crash sweep goes on to the next. The candidates are every fault the runs revealed (the
   brute-force enumerator's harvest) plus an unseen crash per node (§12.2). A pair with no robust derivation
   makes every fault set admissible, so the ILP proposes every set over the candidates: such labels cost about
   what brute force costs, minus the skipped sets. There is no other fallback and no size bound (§12).''')
rep('''In those programs v1 also falls back to enumeration, because their derivations pass through fragile steps. So v1
cannot certify them at any size worth having.''','''In those programs v1 also fell back to enumeration, because their derivations pass through fragile steps, so v1 could
not certify them at any size worth having. The trusted `quorum` (§8.2) and §12 changed that for uniform broadcast.''')
rep('''- **A regression against lite.** On `reliable_broadcast_closed` with several payloads, lite costs 25 / 46 fault
  sets and races costs 134 / 281.''','''- **A regression against lite** (lite has since been deleted, §12.8). On `reliable_broadcast_closed` with
  several payloads, lite cost 25 / 46 fault sets and races costs 134 / 281.''')
rep('''### Next

1. Order between successive broadcasts of a `TotalOrder` stream (fixes the regression).
2. v2 (§8.1): "trigger meets state" premises with barrier discharge. This is what would let G-Set gossip and CTP-2
   with the quiescing harness be certified without enumerating.
3. §8.2: a trusted quorum combinator, for uniform broadcast, multi-Paxos and ABD.''','''### Next (as of §11; see §12 for what followed)

1. Order between successive broadcasts of a `TotalOrder` stream (fixes the regression). Superseded by §12: the
   crash literals between two payload chains are free.
2. v2 (§8.1): "trigger meets state" premises with barrier discharge. This is what would let G-Set gossip and CTP-2
   with the quiescing harness be certified without enumerating.
3. §8.2: a trusted quorum combinator, for uniform broadcast, multi-Paxos and ABD. Done for `quorum`.''')
open(p,'w').write(s)
