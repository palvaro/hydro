# Counterexample gallery

Molly-style pictures (Alvaro, Rosen, Hellerstein, *Lineage-driven Fault Injection*, SIGMOD
2015) of the counterexamples hydrofrac finds on the protocols in `hydro_test`, drawn by
`hydro_lang::sim::viz`.

## How the pictures are made

The search draws them itself. Run any LDFI test with `MOLLY_VIZ_OUT=<dir>` set (or build the
search with `Ldfi::viz_out(<dir>)`, which drops the `<test>` level below). The search then
keeps every schedule's trace and the provenance graph of every derivation, so the test does
not need `with_traces()` or `with_provenance_graphs()`. It writes one directory per
counterexample:

| search | directory |
|---|---|
| `search_all_labels` | `<dir>/<test>/<cluster>/<collection>/` (named as in the report; a duplicate name gets its line, `<collection>_<line>`) |
| `search` / `run` (hand-written goal) | `<dir>/<test>/` (`-2`, `-3`, … if the test searches more than once) |

In each:

| file | zoom | what it shows |
|---|---|---|
| `failing.{dot,png}` | 0 | The Lamport diagram of the failing run: a lane per node, time down, an arrow per message; `✗` an omitted message (red) or a send lost because the sender had crashed (gray), `☠ crashed` ending a lane (what the dead node still did is on a faint dotted continuation), the dashed `EFF` rule the end of finite failures. |
| `witness.{dot,png}` | 1 | The last run in which the goal held, with the goal's support highlighted (`★`, orange): the sufficient set of deliveries the search learned the goal depends on, at the search's lineage level. Missing if no run had the goal hold with a derivation. |
| `provenance.{dot,png}` | 2 | The why-provenance graph behind that support, from the goal down to the deliveries whose loss would break it (executions collapsed), titled `<cluster>/<collection>: why the goal held (<level>-level lineage)` (the test's name for a hand-written goal). |
| `report.md` | 0–1 | Both Lamport diagrams as Mermaid sequence diagrams, and the lineage level. |

A search runs at one lineage level, so the file names have no level suffix. To compare
levels, run the test once per level, for example with `LDFI_LEVEL=exec` for tests that read
`Ldfi::level_from_env`.

The PNGs are rendered when Graphviz is on the `PATH`. The Lamport diagrams pin every point
(`layout=neato`, `pos="x,y!"`) and are rendered with `neato -n2 -Tpng -Gdpi=110`. The
provenance graph uses `dot -Tpng -Gdpi=110`. Under each `DIVERGES` / `diverges alone` row, the
report prints

```
    diagrams: failing.png, witness.png, provenance.png in <dir>/<test>/<cluster>/<collection>
```

and a hand-written search prints the same line after its outcome. See
`hydro_lang::sim::viz::out`. For example:

```sh
MOLLY_VIZ_OUT=/tmp/viz cargo test -p hydro_test --lib \
  cluster::ec_demos_ldfi::tests::ec_demo_multi_paxos_all_labels -- --nocapture --exact
```

writes `failing`, `witness`, `provenance` (`.dot` and `.png`) and `report.md` under
`/tmp/viz/ec_demo_multi_paxos_all_labels/{acceptors/{prepares,accepts_fwd,accepts},learners/{initial,echo}}/`.

## The committed samples

The directories here were drawn before the search wrote all three zoom levels itself. They
come from test-only gallery helpers, since removed, that searched each protocol at both
levels. That is why they hold `witness_{execution,record}.*` and
`provenance_{execution,record}.*` (`record` is the old name of `Level::Precise`) where a
regenerated directory holds `witness.*` and `provenance.*`. To regenerate one, run the test
that asserts its counterexample with `MOLLY_VIZ_OUT` set, for example:

```sh
MOLLY_VIZ_OUT=/tmp/gallery cargo test -p hydro_test --lib -- --test-threads=1 \
  two_pc_ldfi::tests::ldfi_blocks_two_pc_with_a_coordinator_crash \
  isr_replication::tests::ldfi_finds_the_unclean_election_bug_at_every_level \
  paxos_ldfi::tests::ldfi_finds_the_commit_quorum_bug \
  reliable_delivery::tests::ldfi_breaks_two_attempts_by_dropping_both \
  reliable_broadcast::tests::ldfi_breaks_plain_broadcast_with_a_mid_fanout_crash
```

Each test writes to `/tmp/gallery/<test>/`. The schedules a hypothesis runs under are sampled,
so a regenerated witness may have a different interleaving; the fault sets are stable.

## The counterexamples

- **`two_pc/`** — blocking two-phase commit, two participants, one crash allowed
  (`two_pc_ldfi::harness`). Property: *if any participant commits `Txn(1)`, every correct
  participant commits `Txn(1)`.* Faults: `{crash(coordinator after commit[→0]:Txn(1))}` — the
  coordinator dies between its two commit sends; participant 0 commits, participant 1 blocks.
- **`isr_unclean_election/`** — Kafka-style in-sync-replica replication with unclean leader
  election, three replicas, four rounds, one crash allowed (`isr_replication::harness`).
  Property: *if the client was acknowledged for `Write(1)` and ZK's final view names a leader,
  that leader's log holds `Write(1)`.* Faults: `{omit(repl[0→1]:Write(1)), crash(replica@0 after
  client-ack[0→]:Write(1))}` — the replication to replica 1 is lost, the leader shrinks it out
  of the ISR, acknowledges, and dies; ZK promotes the stale replica 1.
- **`paxos_commit_quorum/`** — core Paxos with an injected bug, a commit quorum one
  acknowledgement short (`f` P2bs instead of `f + 1`); two proposers, three acceptors, lossy
  acceptor links, one crash allowed (`paxos_ldfi`). Property: *if proposer 0 chose `x`, no two
  chosen `(slot, value)` pairs disagree at a slot.* Faults: `{omit(p2a[0→0]), omit(p2a[0→2])}` —
  only acceptor 1 accepts `x`; proposer 1's P1b quorum misses it and chooses `y` at slot 0.
- **`reliable_delivery_gives_up/`** — a sender that retries twice then gives up, over a lossy
  link (`distributed::reliable_delivery`, the LDFI paper's first example). Property: *if the
  sender sends a message and not all messages are dropped, the receiver eventually has it.*
  Faults: `{omit(msg:Msg { attempt: 1 }), omit(msg:Msg { attempt: 2 })}` — both attempts lost
  before the end of finite failures; nothing left to deliver once the network heals.
- **`plain_broadcast/`** — broadcast without echo, three members, one crash allowed
  (`cluster::reliable_broadcast`). Property: *if any member delivers `m`, every correct member
  delivers `m`.* Faults: `{crash(broadcaster after initial[→1]:Payload(42))}` — the broadcaster
  dies between its second and third fan-out send; members 0 and 1 deliver, member 2 never does.

Nodes the fault model does not name (the client and ZK in ISR replication, the sender and
receiver in reliable delivery) appear under their location, `Process(locNvM)`: the search only
has labels for the nodes registered as crashable.
