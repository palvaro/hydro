# HydroFrac on `relay_broadcast`

These are the Lamport diagrams that HydroFrac (`MOLLY_VIZ_OUT`) writes for the failing runs of
`hydro_test/src/cluster/relay_broadcast_ldfi.rs`. Each `<test>/members/<collection>/` directory holds:

- `failing.{dot,png}`: the failing run.
- `witness.{dot,png}`: a run where the goal held, with its support.
- `provenance.{dot,png}`: the provenance graph behind the witness.
- `report.md`: a Mermaid version of the same information.

Setup:

- Three members and two request ids.
- The source and every member are crashable.
- `max_merges_per_tick = 7`.
- `Exploration::Fuzz { instances: 8 }`, with `.with_provenance()`.
- The body sends both requests and quiesces, then calls `end_of_finite_failures()`, then fires
  `ack_timeout_ticks + 3` timer rounds at every member.

| Test | Policy / relay channel | `delivered` (`relay_broadcast.rs:250`) |
|---|---|---|
| `relay_none_diverges_under_a_source_crash` | `None` / `FailStop` | DIVERGES under crash(source after sending bcast 2 to member 1) |
| `relay_echo_lossy_diverges_under_a_source_crash_and_an_omitted_relay` | `Echo` / `LossyDelayedForever` | DIVERGES under omit(relay Relay{from: 0, id: 2} member 0→member 1) + crash(source after sending bcast 2 to member 0) |
| `relay_echo_resend_lossy_holds_under_one_crash` (`crash_budget(1)`) | `EchoResend{3}` / `LossyDelayedForever` | no violation found (18 fault sets, 8 sampled schedules each) |
| `relay_echo_resend_lossy_diverges_under_a_source_and_a_relayer_crash` | `EchoResend{3}` / `LossyDelayedForever` | DIVERGES under crash(member@0 after sending relay Relay{from: 0, id: 2} to member 1) + crash(source after sending bcast 2 to member 0) |
| `relay_reliable_resend_lossy_holds_under_one_crash` (`crash_budget(1)`) | `ReliableResend{3}` / `LossyDelayedForever` | no violation found (18 fault sets, 8 sampled schedules each) |
| `relay_reliable_resend_lossy_holds_under_unbounded_crashes` | `ReliableResend{3}` / `LossyDelayedForever` | no violation found (55 fault sets, 8 sampled schedules each) |

In every test, `initial` (`relay_broadcast.rs:237`) DIVERGES under crash(source after sending
bcast 2 to member 1).

The last row shows a gap in the protocol, not in the harness. Members echo only the ids they
received from the source, so a relayer that crashes mid-fan-out is never covered. The
`manual_proof!` on `delivered` assumes it is. `ReliableResend` closes the gap: a member relays
every id on its first receipt from any sender, so the peer that the crashed relayer reached
relays the id onward. (The diagrams in this directory are from the run where `delivered` was at
line 218 and `initial` at line 205; the program itself is the same for those variants.)

Regenerate the diagrams with:

```sh
MOLLY_VIZ_OUT=$PWD/design_docs/relay_broadcast/hydrofrac \
  cargo test -p hydro_test --lib cluster::relay_broadcast_ldfi::tests::relay_none_diverges_under_a_source_crash -- --nocapture --test-threads=1 --exact
MOLLY_VIZ_OUT=$PWD/design_docs/relay_broadcast/hydrofrac \
  cargo test -p hydro_test --lib cluster::relay_broadcast_ldfi::tests::relay_echo_lossy_diverges_under_a_source_crash_and_an_omitted_relay -- --nocapture --test-threads=1 --exact
```
