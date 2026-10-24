p = 'docs/design/hydrofrac-soundness.md'
s = open(p).read()
anchor = '''**Argument-only** (brute force cannot finish): URB over 3 members, echo broadcast over 3 members, ABD, G-Set gossip,
multi-Paxos.
'''
assert anchor in s
add = '''
**Oracle 2** (`hf/check/oracle2.rs`, test-only, run by every `cross_check` after the brute force). It shares only the
simulator, the user's IR, the harness and the definition of the bounds with the search:
- **its own program**: `EcCheck::instrument` forks the user's flow (`SimFlow::try_fork`) before instrumenting it, and
  oracle 2 compiles the fork with hook points but without the provenance rewrite or hydrofrac's tap pass;
- **its own taps**: one external output per labelled cluster node where the label is minted or exits, by node
  identity (never merged by source line), carrying `bincode(element)`;
- **its own faults and hook**: omission of a message by content on a lossy channel, crash before the first send or
  right after the node's `k`-th send, all only before the heal;
- **its own comparison**: per schedule and collection, the correct members' element sets must be equal.

Its collections map to the search's labels by the `(label, operator, kind, cluster)` key of their node. Agreement:
a claim of holding with up to `k` crashes is refuted by any oracle-2 breaking set with at most `k` crashes; a
counterexample of `n <= max_faults` faults and `c` crashes must be matched by an oracle-2 breaking set with at most
`n` faults and `c` crashes; and both failure-free runs agree on vacuity. It is stricter than the search where a label
spans several nodes (it checks each node) and where a node resends a payload (it can crash after the second copy).
'''
s = s.replace(anchor, anchor + add)
open(p, 'w').write(s)
