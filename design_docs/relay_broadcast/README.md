# `relay_broadcast`: HydroFrac, Cress, and Sedge on one program

`hydro_test/src/cluster/relay_broadcast.rs` has one `Source` process and a cluster of `Member`s. The
source broadcasts request ids, and members may relay what they receive. `RelayPolicy` picks the
relay protocol:

- `None`: a plain one-hop `broadcast_closed`.
- `Echo`: each member echoes ids it received **from the source** to every peer, once.
- `EchoResend { ack_timeout_ticks: 3 }`: `Echo`, and each echo is re-sent with identical bytes
  until the peer acks it.
- `ReliableResend { ack_timeout_ticks: 3 }`: each member relays every id on its **first receipt
  from any sender** (source or peer), and re-sends until acked.

Each tool answers a different question about these variants:

- **HydroFrac** (LDFI): is `delivered` eventually consistent under crashes and omissions?
- **Cress**: does the program contain a schedule amplifier, where delay causes more work?
- **Sedge**: deployed with a burst, does it recover, drain, or collapse?

## Results

| Variant | HydroFrac: `delivered` (`relay_broadcast.rs:250`) | Cress | Sedge f=2 / f=4 / f=8 |
|---|---|---|---|
| `None` | DIVERGES under crash(source after sending bcast 2 to member 1) | — | — |
| `Echo` (lossy relay) | DIVERGES under omit(relay Relay{from: 0, id: 2} member 0→member 1) + crash(source after sending bcast 2 to member 0) | `echo_only`: BENIGN | `echo_only`: RECOVERED / RECOVERED / RECOVERED |
| `EchoResend` | `crash_budget(1)`: no violation found (18 fault sets, 8 sampled schedules each). Unbounded: DIVERGES under crash(member@0 after sending relay Relay{from: 0, id: 2} to member 1) + crash(source after sending bcast 2 to member 0) | `resend_on`: HAZARDOUS, grows, 8.97× @ `relay_broadcast.rs:272` | `resend_on`: COLLAPSED / DRAINING / RECOVERED |
| `ReliableResend` | `crash_budget(1)`: no violation found (18 fault sets). Unbounded: no violation found (55 fault sets) | `reliable_resend`: HAZARDOUS, grows, 8.97× @ `relay_broadcast.rs:272` | `reliable_resend`: COLLAPSED / DRAINING / RECOVERED |

In every HydroFrac test, `initial` (`relay_broadcast.rs:237`, the one-hop broadcast) DIVERGES under
crash(source after sending bcast 2 to member 1). That is expected, since it is the unrepaired
stream.

Each step of the story fixes one tool's finding and exposes the next:

1. **One hop is not enough** (`None`). A source crash mid-broadcast leaves members disagreeing
   forever.
2. **Echo repairs crashes but not loss** (`Echo`). On a lossy relay channel, one omitted echo plus
   a source crash still leaves a member without the id.
3. **Resend repairs loss but not relayer crashes** (`EchoResend`). It holds under any single
   crash. With two crashes, a member that crashes mid-fan-out is never covered, because peers
   only echo ids they got from the source. That breaks the `manual_proof!` argument, and the gap
   is in the protocol, not in the harness.
4. **Reliable broadcast closes the gap** (`ReliableResend`). The peer that the crashed relayer
   reached relays the id onward. HydroFrac finds no violation across 55 fault sets.
5. **But resend-until-ack is a schedule amplifier** (Cress, both resend variants). Deep inboxes
   delay acks past the timeout, the relay re-sends, and the re-sends deepen the inboxes.
6. **And it collapses when deployed** (Sedge, both resend variants). At f=2 the re-sends alone
   keep the inboxes growing after the burst.

### Why `reliable_resend` has the same Cress multiplier and Sedge verdicts as `resend_on`

Cress and Sedge run without faults, on a `FailStop` channel. In a fault-free run, each member
still relays each id exactly once to each peer. Under `ReliableResend` that relay fires on the
first receipt, whether from the source or from a peer. Every later copy is a duplicate, which
the `relayed` table dedups. So the relay fan-out, the outstanding table, and the
resend-on-timeout loop (the `sliced!` block at `:272`) do exactly the same work as under
`EchoResend`. The two configurations differ only in which arrival triggers the relay. The fix
that HydroFrac needed costs nothing in steady state, and it inherits the same overload hazard.

The Sedge traces differ line by line, because arrival interleavings differ, but the aggregates
match `resend_on` to within rounding. The `wire`/`acks_sent` amplification is 2.47 / 2.61 / 0.88
here, against 2.47 / 2.62 / 0.88 for `resend_on`.

## Sedge details

Deployed setup: 5 members, `max_merges_per_tick = 7`, 50 ms timers, μ = 28 rounds/s, channel
`FailStop`, seed 1. Windows: baseline [5,10) s, trigger [10,15) s at 2μ, tail verdict window
[45,75) s (`--tail 60`).

| config | f | verdict | tail merged | wire (tail vs baseline) | inbox depth in tail (1st half → 2nd half) |
|---|---|---|---|---|---|
| `echo_only` | 2 | RECOVERED | — | 20 per round, no amplification | trigger backlog drained by ≈27 s |
| `echo_only` | 4 | RECOVERED | — | 1.00× | — |
| `echo_only` | 8 | RECOVERED | — | 1.00× | — |
| `resend_on` | 2 | COLLAPSED | 700/s (5 × 7 × 20, saturated) | 671/s vs 270/s (2.47×) | 7435 → 7838, still rising |
| `resend_on` | 4 | DRAINING | — | acks 378/s vs 142/s (2.62×) | falling at 76/s |
| `resend_on` | 8 | RECOVERED | 94/s vs 105/s | 0.88× | backlog (≈6000 inbox, ≈3800 outstanding) drained by ≈42 s |
| `reliable_resend` | 2 | COLLAPSED | 700/s (saturated) | 671.3/s vs 269.6/s (2.47×) | 7436 → 7839, still rising (peak 8124 at 73 s) |
| `reliable_resend` | 4 | DRAINING | 414/s | acks 377.7/s vs 142.6/s (2.61×) | 1151 → 11, falling at 76/s |
| `reliable_resend` | 8 | RECOVERED | 94.2/s vs 105.0/s | 75.3/s vs 84.0/s (0.88×) | 2 → 2. Peak 5913 at 14 s, drained by ≈42 s |

Plots (made with `scripts/sedge_plot.py TRACE OUT "title" --trigger 10,15 --tail-window 45,75 --rates delivered`):

- [`echo_only_f2.svg`](echo_only_f2.svg): no amplification, and the backlog drains.
- [`resend_on_f2.svg`](resend_on_f2.svg), [`reliable_resend_f2.svg`](reliable_resend_f2.svg):
  merges stay pinned at the budget, and the inbox keeps rising through the tail.
- [`resend_on_f8.svg`](resend_on_f8.svg), [`reliable_resend_f8.svg`](reliable_resend_f8.svg):
  the backlog drains by ≈42 s, and the tail rates return to baseline.

Raw data: [`results.tsv`](results.tsv), plus one trace per run,
`hydro_test.relay_broadcast.<config>.factor<f>.trial0.tsv`. The `trace` column in
`results.tsv` holds the sandbox path where each trace was written.

Under Sedge's default 23 s tail, `echo_only` f=2 is DRAINING (inbox noise, mean 13→20) and
`resend_on` f=8 is DRAINING (its backlog takes ≈27 s to clear). That is why these runs use
`--tail 60`.

## HydroFrac details

Test file: `hydro_test/src/cluster/relay_broadcast_ldfi.rs`. Setup: 3 members, 2 request ids;
the source and every member are crashable; `max_merges_per_tick = 7`;
`Exploration::Fuzz { instances: 8 }` with `.with_provenance()`. The relay channel is
`LossyDelayedForever` for `Echo`/`EchoResend`/`ReliableResend`, and `FailStop` for `None`. The
body sends both requests and quiesces, calls `end_of_finite_failures()`, then fires
`ack_timeout_ticks + 3` timer rounds at every member.

| Test | Policy / relay channel | `delivered` |
|---|---|---|
| `relay_none_diverges_under_a_source_crash` | `None` / `FailStop` | DIVERGES under crash(source after sending bcast 2 to member 1) |
| `relay_echo_lossy_diverges_under_a_source_crash_and_an_omitted_relay` | `Echo` / `LossyDelayedForever` | DIVERGES under omit(relay Relay{from: 0, id: 2} member 0→member 1) + crash(source after sending bcast 2 to member 0) |
| `relay_echo_resend_lossy_holds_under_one_crash` (`crash_budget(1)`) | `EchoResend{3}` / `LossyDelayedForever` | no violation found (18 fault sets, 8 sampled schedules each) |
| `relay_echo_resend_lossy_diverges_under_a_source_and_a_relayer_crash` | `EchoResend{3}` / `LossyDelayedForever` | DIVERGES under crash(member@0 after sending relay Relay{from: 0, id: 2} to member 1) + crash(source after sending bcast 2 to member 0) |
| `relay_reliable_resend_lossy_holds_under_one_crash` (`crash_budget(1)`) | `ReliableResend{3}` / `LossyDelayedForever` | no violation found (18 fault sets, 8 sampled schedules each) |
| `relay_reliable_resend_lossy_holds_under_unbounded_crashes` | `ReliableResend{3}` / `LossyDelayedForever` | no violation found (55 fault sets, 8 sampled schedules each) |

[`hydrofrac/`](hydrofrac/README.md) has the Lamport diagrams for the two failing runs: the
failing run, a witness run, and the witness's provenance.

## Reproduce

Build environment (the `ldfi` feature needs CMake ≥ 3.15 and libclang; see
`docs/design/integration-hydrofrac-cress-sedge.md`):

```sh
python3 -m pip install --target $TMPDIR/py cmake
python3 -m pip install --target $TMPDIR/pyclang libclang
export CMAKE=$TMPDIR/py/cmake/data/bin/cmake
export LIBCLANG_PATH=$TMPDIR/pyclang/clang/native
```

HydroFrac (all six tests):

```sh
cargo test -p hydro_test --lib cluster::relay_broadcast_ldfi -- --nocapture --test-threads=1
```

Cress, then Sedge. Run Cress **first**. It writes `target/cress/conservation.tsv`, and Sedge
reads its stream kinds from that file. Without it, `delivered` (a `u64` id stream) is read as a
level, and its mean id always rises.

```sh
for c in echo_only resend_on reliable_resend; do
  ./scripts/cress --config $c hydro_test relay_broadcast
  ./scripts/sedge --config $c --factors 2,4,8 --trials 1 --tail 60 hydro_test relay_broadcast
done
# traces: target/sedge/traces/, verdicts: target/sedge/results.tsv
python3 scripts/sedge_plot.py target/sedge/traces/hydro_test.relay_broadcast.reliable_resend.factor2.trial0.tsv \
  reliable_resend_f2.svg "relay_broadcast reliable_resend, f=2 (COLLAPSED)" \
  --trigger 10,15 --tail-window 45,75 --rates delivered
```

## Caveats

- **Sampled, not certified.** HydroFrac's "no violation found" covers the enumerated fault sets
  × 8 fuzzed schedules each, not every schedule. Sedge runs one trial per factor with seed 1.
- **Config-bounded.** HydroFrac runs N=3 members and 2 ids. Cress and Sedge run N=5 members.
  None of the results generalize beyond these sizes without further argument.
- **Critical factor.** The module-level model in `relay_broadcast.rs` predicts
  f_c = C/(C−R) ≈ 28/12 ≈ 2.3. f=2 collapses, which is consistent with that. f=4 is still
  DRAINING in the [45,75) s window, though, so the factor at which the system actually recovers
  within the tail lies between 4 and 8. The model is optimistic, and the measured f_c is between
  4 and 8.
- **Teardown noise.** `BrokenPipe` panics printed when Sedge tears down a deployment are noise,
  not failures.
- **HydroFrac fix.** These results needed a HydroFrac provenance fix for keyed
  `anti_join`/`cross_singleton`.
- **Branch history.** The integration branch was re-merged with an explicit merge base,
  because the hydrofrac branch's snapshot commit reverted the Cress/Sedge commits. See
  `docs/design/integration-hydrofrac-cress-sedge.md`.
