p='docs/design/hydrofrac-soundness.md'
s=open(p).read()
anchor='''## 4. Premise literals'''
add='''### 3.1 The choice-free shortcut

**Claim.** In a program with no schedule choice, every derivation may be treated as robust. `read_schedule` drops
premises when `choice_free` (`hf/check/races_engine.rs:198`).

**What "choice-free" means.** The choice scan (`hf/provenance/choices.rs:72`, `choice`) finds nothing. The scan is an
allowlist too: an operator is choice-free only if its kind is in `ALLOWED` (`hf/provenance/choices.rs:21`) and no
special rule names it. A `batch`, a snapshot, an untrusted `observe_nondet`, a fold over an unordered input, an
unordered external input, an ordered lossy network, any source but iterators, membership and external networks, and
every unlisted kind are choices.

**Proof.**
- Each fault set has exactly one schedule (A1, with no hook decisions to make).
- By A13 the program is monotone: with no batch, no tick receives outside data, so no non-monotone operator is fed
  from outside it. Faults then only remove records.
- A totally ordered stream reaching a top-level fold or scan comes from one FIFO chain (merging two senders gives
  `NoOrder`, and restoring an order is a choice). Under faults its sequence is a subsequence of the failure-free one,
  and the fold's or scan's record carries every earlier input in its lineage (`hf/provenance/rewrite.rs:608`,
  `wrap_acc`; `:628`, `wrap_scan`).
- So a derivation whose dependencies all survive `H` is recomputed under `H`, which is Lemma R's conclusion.

**Proved**, given A1, A2 and A13. **Tested:** every choice-free corpus program is cross-checked (`reliable_broadcast_closed`,
S1, S2).

'''
assert anchor in s
s=s.replace(anchor, add+anchor,1)
open(p,'w').write(s)
