# MicroBus catchup v2 client under provenance

Probe of the Hydro catchup v2 client in the MicroBus package
(`ssh://git.amazon.com/pkg/MicroBus`, branch `hydro`, commit
`b7f18651bccd33582a8957d37e29408012a2eeda` — "(kiro): Integrate HydroClient into standby"), run against this
repository's `hydro_lang` with `SimFlow::with_provenance()`.

Only our files are here; no MicroBus source is copied into this repository.

- `provenance_probe.rs` — the test. Drop it in as
  `microbus_hydro/src/catchup_v2/provenance_probe.rs`, add
  `#[cfg(test)] mod provenance_probe;` next to `mod sim_tests;` in
  `catchup_v2.rs`, and point the crate's `hydro_lang` dependency at this
  repository (features `["sim"]`). The test drives `client_logic` directly
  through `sim_input`s, as the package's own sim tests do; the production
  embedded channels are not involved.
- `provenance_dump.txt` — the classified emission log the test wrote.

## Result

Ticks and config are declared operational (the driver's clock and its policy);
server status and slot data are data. Phases are separated by quiescence.
Identical across random schedules. `output 4` is the client→server status
stream, `output 5` the accepted-slot stream; `T0.k` is the k-th tick, `T1.0`
the config, `D2.0` the server's accept, `D3.k` the k-th slot burst.

```
FixedOperational  to server  {T0.0,T1.0}                          44B  open to server 3
FixedOperational  to server  {T0.0,T0.1,T0.2,T1.0}                44B  open timeout -> reopen to server 4
Productive        accepted   {D2.0,D3.0,...}                      42B  burst 42..141 ingested
Productive        accepted   {D2.0,D3.0,D3.1,...}                 42B  burst 143..242 ingested (hole at 142)
Productive        to server  {D2.0,D3.0,D3.1,T0.0..T0.3,T1.0}     44B  first gap-keepalive ack (last_contiguous=142)
Reactivated       to server  {..., T0.4}                          44B  second keepalive, nothing new
Reactivated       to server  {..., T0.5}                          44B  third keepalive, nothing new
```

- Open and timeout/reopen are `FixedOperational`: an open carries no
  application data, and each timeout costs one fixed 44-byte message however
  long the client has been trying. Heartbeat shape, not retry shape.
- The gap keepalive is the client's one `Reactivated` mechanism: on a stalled
  stream it re-sends the ack every `gap_keepalive_ns` for as long as the gap
  persists. It is fixed-size (44 B) regardless of how many slots were received,
  so on the client side this is bounded reactivation, one message per interval.
- Whether the loop is closed depends on the server, which is outside the Hydro
  boundary: the keepalive exists to provoke a gap retransmit. If the server
  answers every keepalive by retransmitting everything from the gap forward,
  a fixed-size ack buys a state-proportional response. That is the loop-gain
  question to ask of the C++ server: what does it send in response to a
  repeated gap ack, and is that rate-limited on its side.

All lineage is coarse: the client is a single `by_mut` state machine.
