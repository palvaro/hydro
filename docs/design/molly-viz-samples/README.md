# Counterexample gallery

Molly-style pictures (Alvaro, Rosen, Hellerstein, *Lineage-driven Fault Injection*, SIGMOD
2015) of the counterexamples hydrofrac finds on the protocols in `hydro_test`, drawn by
`hydro_lang::sim::viz`. One directory per counterexample; in each:

| file | zoom | what it shows |
|---|---|---|
| `failing.{dot,png}` | 0 | The Lamport diagram of the failing run: a lane per node, time down, an arrow per message; `✗` an omitted message (red) or a send lost because the sender had crashed (gray), `☠ crashed` ending a lane (what the dead node still did is on a faint dotted continuation), the dashed `EFF` rule the end of finite failures. |
| `witness_{execution,record}.{dot,png}` | 1 | The last run in which the goal held, with the goal's support highlighted (`★`, orange): the messages whose loss the search learned the goal depends on, at execution-level and at record-level lineage. The finer the level, the tighter the highlight. |
| `provenance_{execution,record}.{dot,png}` | 2 | The why-provenance graph behind that support, from the goal down to the deliveries whose loss would break it (executions collapsed), at each level. |
| `report.md` | 0–1 | Both Lamport diagrams as Mermaid sequence diagrams. |

The Lamport diagrams pin every point (`layout=neato`, `pos="x,y!"`): render them with
`neato -n2 -Tpng`. The provenance graphs are plain `dot`. Regenerate everything with

```sh
MOLLY_VIZ_OUT=/tmp/gallery cargo test -p hydro_test gallery_ -- --test-threads=1
for f in /tmp/gallery/*/*.dot; do
  if grep -q layout=neato "$f"; then neato -n2 -Tpng -Gdpi=110 "$f" -o "${f%.dot}.png"
  else dot -Tpng -Gdpi=96 "$f" -o "${f%.dot}.png"; fi
done
```

(the `gallery_*` tests live in `hydro_test/src/cluster/ldfi_viz.rs`,
`hydro_test/src/cluster/reliable_broadcast.rs` and
`hydro_test/src/distributed/reliable_delivery.rs`, and are no-ops unless `MOLLY_VIZ_OUT` is
set). The schedules a hypothesis is run under are sampled, so a regenerated witness may differ
in its interleaving; the fault sets are stable.

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
