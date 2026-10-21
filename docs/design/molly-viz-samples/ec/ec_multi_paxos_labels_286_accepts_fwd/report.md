# goal violated by {crash(proposer@0 after multi_paxos.rs:549[0→1]:(Ts { round: 1, writer: Legacy { raw_id: 0 } }, 0, 1, Some(8)))}

Found after 8 hypothesis run(s), 64 schedule(s) explored.

## The failing run

```mermaid
---
title: goal violated by {crash(proposer@0 after multi_paxos.rs:549[0→1]:(Ts { round: 1, writer: Legacy { raw_id: 0 } }, 0, 1, Some(8)))}
---
sequenceDiagram
    participant p0 as proposer@0
    participant p1 as acceptor@0
    participant p2 as acceptor@1
    participant p3 as acceptor@2
    participant p4 as learner@0
    participant p5 as learner@1
    participant p6 as learner@2
    p0->>p1: multi_paxos.rs:281: Ts { round: 1, writer: Legacy { raw_id: 0 } }
    p0->>p2: multi_paxos.rs:281: Ts { round: 1, writer: Legacy { raw_id: 0 } }
    p0->>p3: multi_paxos.rs:281: Ts { round: 1, writer: Legacy { raw_id: 0 } }
    p2->>p0: multi_paxos.rs:392: Promise { b: Ts { round: 1, writer: Legacy { raw_id: 0 } }, …
    p1->>p0: multi_paxos.rs:392: Promise { b: Ts { round: 1, writer: Legacy { raw_id: 0 } }, …
    p3->>p0: multi_paxos.rs:392: Promise { b: Ts { round: 1, writer: Legacy { raw_id: 0 } }, …
    p0->>p1: multi_paxos.rs:549: (Ts { round: 1, writer: Legacy { raw_id: 0 } }, 0, 0, Some(7…
    p0->>p1: multi_paxos.rs:549: (Ts { round: 1, writer: Legacy { raw_id: 0 } }, 0, 1, Some(8…
    p0->>p2: multi_paxos.rs:549: (Ts { round: 1, writer: Legacy { raw_id: 0 } }, 0, 0, Some(7…
    p0->>p2: multi_paxos.rs:549: (Ts { round: 1, writer: Legacy { raw_id: 0 } }, 0, 1, Some(8…
    Note over p0: 💀 crashed
    p0--xp3: multi_paxos.rs:549: (Ts { round: 1, writer: Legacy { raw_id: 0 } }, 0, 1, Some(8… (lost: sender crashed)
    p0->>p3: multi_paxos.rs:549: (Ts { round: 1, writer: Legacy { raw_id: 0 } }, 0, 0, Some(7…
    p1->>p0: multi_paxos.rs:392: Accepted { b: Ts { round: 1, writer: Legacy { raw_id: 0 } },…
    p3->>p0: multi_paxos.rs:392: Accepted { b: Ts { round: 1, writer: Legacy { raw_id: 0 } },…
    p2->>p0: multi_paxos.rs:392: Accepted { b: Ts { round: 1, writer: Legacy { raw_id: 0 } },…
    p1->>p0: multi_paxos.rs:392: Accepted { b: Ts { round: 1, writer: Legacy { raw_id: 0 } },…
    p2->>p0: multi_paxos.rs:392: Accepted { b: Ts { round: 1, writer: Legacy { raw_id: 0 } },…
    p0--xp0: multi_paxos.rs:569: 1 (lost: sender crashed)
    p0--xp4: multi_paxos.rs:582: (1, 0, 1, Some(8)) (lost: sender crashed)
    p0--xp5: multi_paxos.rs:582: (1, 0, 1, Some(8)) (lost: sender crashed)
    p0--xp6: multi_paxos.rs:582: (1, 0, 1, Some(8)) (lost: sender crashed)
    p0--xp0: multi_paxos.rs:569: 0 (lost: sender crashed)
    p0--xp4: multi_paxos.rs:582: (1, 0, 0, Some(7)) (lost: sender crashed)
    p0--xp5: multi_paxos.rs:582: (1, 0, 0, Some(7)) (lost: sender crashed)
    p0--xp6: multi_paxos.rs:582: (1, 0, 0, Some(7)) (lost: sender crashed)
```

## Why the goal held before

The goal's support in the last run in which it held: {multi_paxos.rs:281[0→0]:Ts { round: 1, writer: Legacy { raw_id: 0 } }←proposer@0#0, multi_paxos.rs:281[0→1]:Ts { round: 1, writer: Legacy { raw_id: 0 } }←proposer@0#1, multi_paxos.rs:392[0→0]:Promise { b: Ts { round: 1, writer: Legacy { raw_id: 0 } }, accepted: [] }←acceptor@0#0, multi_paxos.rs:392[1→0]:Promise { b: Ts { round: 1, writer: Legacy { raw_id: 0 } }, accepted: [] }←acceptor@1#0, multi_paxos.rs:549[0→0]:(Ts { round: 1, writer: Legacy { raw_id: 0 } }, 0, 0, Some(7))←proposer@0#3, multi_paxos.rs:549[0→0]:(Ts { round: 1, writer: Legacy { raw_id: 0 } }, 0, 1, Some(8))←proposer@0#6, multi_paxos.rs:549[0→1]:(Ts { round: 1, writer: Legacy { raw_id: 0 } }, 0, 0, Some(7))←proposer@0#4, multi_paxos.rs:549[0→1]:(Ts { round: 1, writer: Legacy { raw_id: 0 } }, 0, 1, Some(8))←proposer@0#7, observed[0→]:(1, [0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 7, 0, 0, 0]) from acceptor@0 through send #1←acceptor@0#1, observed[0→]:(1, [0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 8, 0, 0, 0]) from acceptor@0 through send #1←acceptor@0#1, observed[1→]:(1, [0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 7, 0, 0, 0]) from acceptor@1 through send #1←acceptor@1#1, observed[1→]:(1, [0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 8, 0, 0, 0]) from acceptor@1 through send #1←acceptor@1#1}

```mermaid
---
title: goal held under {crash(acceptor@2 before any send)}: its support (★)
---
sequenceDiagram
    participant p0 as proposer@0
    participant p1 as acceptor@0
    participant p2 as acceptor@1
    participant p3 as acceptor@2
    participant p4 as learner@0
    participant p5 as learner@1
    participant p6 as learner@2
    Note over p3: 💀 crashed before any send
    rect rgb(255, 243, 205)
    p0->>p1: ★ multi_paxos.rs:281: Ts { round: 1, writer: Legacy { raw_id: 0 } }
    end
    rect rgb(255, 243, 205)
    p0->>p2: ★ multi_paxos.rs:281: Ts { round: 1, writer: Legacy { raw_id: 0 } }
    end
    p0->>p3: multi_paxos.rs:281: Ts { round: 1, writer: Legacy { raw_id: 0 } }
    rect rgb(255, 243, 205)
    p1->>p0: ★ multi_paxos.rs:392: Promise { b: Ts { round: 1, writer: Legacy { raw_id: 0 } }, …
    end
    p3--xp0: multi_paxos.rs:392: Promise { b: Ts { round: 1, writer: Legacy { raw_id: 0 } }, … (lost: sender crashed)
    rect rgb(255, 243, 205)
    p2->>p0: ★ multi_paxos.rs:392: Promise { b: Ts { round: 1, writer: Legacy { raw_id: 0 } }, …
    end
    rect rgb(255, 243, 205)
    p0->>p1: ★ multi_paxos.rs:549: (Ts { round: 1, writer: Legacy { raw_id: 0 } }, 0, 0, Some(7…
    end
    rect rgb(255, 243, 205)
    p0->>p1: ★ multi_paxos.rs:549: (Ts { round: 1, writer: Legacy { raw_id: 0 } }, 0, 1, Some(8…
    end
    rect rgb(255, 243, 205)
    p0->>p2: ★ multi_paxos.rs:549: (Ts { round: 1, writer: Legacy { raw_id: 0 } }, 0, 0, Some(7…
    end
    rect rgb(255, 243, 205)
    p0->>p2: ★ multi_paxos.rs:549: (Ts { round: 1, writer: Legacy { raw_id: 0 } }, 0, 1, Some(8…
    end
    p0->>p3: multi_paxos.rs:549: (Ts { round: 1, writer: Legacy { raw_id: 0 } }, 0, 0, Some(7…
    p0->>p3: multi_paxos.rs:549: (Ts { round: 1, writer: Legacy { raw_id: 0 } }, 0, 1, Some(8…
    p1->>p0: multi_paxos.rs:392: Accepted { b: Ts { round: 1, writer: Legacy { raw_id: 0 } },…
    p2->>p0: multi_paxos.rs:392: Accepted { b: Ts { round: 1, writer: Legacy { raw_id: 0 } },…
    p3--xp0: multi_paxos.rs:392: Accepted { b: Ts { round: 1, writer: Legacy { raw_id: 0 } },… (lost: sender crashed)
    p1->>p0: multi_paxos.rs:392: Accepted { b: Ts { round: 1, writer: Legacy { raw_id: 0 } },…
    p0->>p0: multi_paxos.rs:569: 1
    p0->>p4: multi_paxos.rs:582: (1, 0, 1, Some(8))
    Note over p4: ⇒ 0100000000000000…
    p0->>p5: multi_paxos.rs:582: (1, 0, 1, Some(8))
    p4->>p5: multi_paxos.rs:591: (1, 0, 1, Some(8))
    Note over p5: ⇒ 0100000000000000…
    p0->>p6: multi_paxos.rs:582: (1, 0, 1, Some(8))
    p4->>p6: multi_paxos.rs:591: (1, 0, 1, Some(8))
    p5->>p6: multi_paxos.rs:591: (1, 0, 1, Some(8))
    Note over p6: ⇒ 0100000000000000…
    p4->>p4: multi_paxos.rs:591: (1, 0, 1, Some(8))
    p5->>p4: multi_paxos.rs:591: (1, 0, 1, Some(8))
    p6->>p4: multi_paxos.rs:591: (1, 0, 1, Some(8))
    p5->>p5: multi_paxos.rs:591: (1, 0, 1, Some(8))
    p6->>p5: multi_paxos.rs:591: (1, 0, 1, Some(8))
    p6->>p6: multi_paxos.rs:591: (1, 0, 1, Some(8))
    p3--xp0: multi_paxos.rs:392: Accepted { b: Ts { round: 1, writer: Legacy { raw_id: 0 } },… (lost: sender crashed)
    p2->>p0: multi_paxos.rs:392: Accepted { b: Ts { round: 1, writer: Legacy { raw_id: 0 } },…
    p0->>p0: multi_paxos.rs:569: 0
    p0->>p4: multi_paxos.rs:582: (1, 0, 0, Some(7))
    Note over p4: ⇒ 0100000000000000…
    p0->>p5: multi_paxos.rs:582: (1, 0, 0, Some(7))
    p4->>p5: multi_paxos.rs:591: (1, 0, 0, Some(7))
    Note over p5: ⇒ 0100000000000000…
    p0->>p6: multi_paxos.rs:582: (1, 0, 0, Some(7))
    p4->>p6: multi_paxos.rs:591: (1, 0, 0, Some(7))
    p5->>p6: multi_paxos.rs:591: (1, 0, 0, Some(7))
    Note over p6: ⇒ 0100000000000000…
    p4->>p4: multi_paxos.rs:591: (1, 0, 0, Some(7))
    p5->>p4: multi_paxos.rs:591: (1, 0, 0, Some(7))
    p6->>p4: multi_paxos.rs:591: (1, 0, 0, Some(7))
    p5->>p5: multi_paxos.rs:591: (1, 0, 0, Some(7))
    p6->>p5: multi_paxos.rs:591: (1, 0, 0, Some(7))
    p6->>p6: multi_paxos.rs:591: (1, 0, 0, Some(7))
```

