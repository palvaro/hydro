# goal violated by {omit(relay[0→1]:Relay { from: 0, id: 2 }), crash(source after bcast[→0]:2)}

Found after 11 hypothesis run(s), 88 schedule(s) explored.

## The failing run

```mermaid
---
title: goal violated by {omit(relay[0→1]:Relay { from: 0, id: 2 }), crash(source after bcast[→0]:2)}
---
sequenceDiagram
    participant p0 as source
    participant p1 as member@0
    participant p2 as member@1
    participant p3 as member@2
    p0->>p1: bcast: 1
    p0->>p1: bcast: 2
    Note over p0: 💀 crashed
    p0--xp2: bcast: 2 (lost: sender crashed)
    p0--xp3: bcast: 2 (lost: sender crashed)
    Note over p1: ⇒ 0100000000000000
    Note over p1: ⇒ 0200000000000000
    p0->>p2: bcast: 1
    Note over p2: ⇒ 0100000000000000
    p0->>p3: bcast: 1
    Note over p3: ⇒ 0100000000000000
    p2->>p3: relay: Relay { from: 1, id: 1 }
    Note over p3: ⇒ 0100000000000000
    p2->>p1: relay: Relay { from: 1, id: 1 }
    Note over p1: ⇒ 0100000000000000
    p1->>p2: relay: Relay { from: 0, id: 1 }
    Note over p2: ⇒ 0100000000000000
    p1->>p3: relay: Relay { from: 0, id: 1 }
    Note over p3: ⇒ 0100000000000000
    p3->>p1: relay: Relay { from: 2, id: 1 }
    Note over p1: ⇒ 0100000000000000
    p3->>p2: relay: Relay { from: 2, id: 1 }
    Note over p2: ⇒ 0100000000000000
    p1-xp2: relay: Relay { from: 0, id: 2 } (omitted)
    p1->>p3: relay: Relay { from: 0, id: 2 }
    Note over p3: ⇒ 0200000000000000
    Note over p0,p3: EFF: end of finite failures
    p1->>p2: ack: Ack { from: 0, id: 1 }
    p1->>p3: ack: Ack { from: 0, id: 1 }
    p3->>p1: ack: Ack { from: 2, id: 1 }
    p3->>p1: ack: Ack { from: 2, id: 2 }
    p3->>p2: ack: Ack { from: 2, id: 1 }
    p2->>p3: ack: Ack { from: 1, id: 1 }
    p2->>p1: ack: Ack { from: 1, id: 1 }
```

## Why the goal held before

The goal's support in the last run in which it held: {bcast[→0]:1←source#0, relay[0→1]:Relay { from: 0, id: 1 }←member@0#0, relay[0→2]:Relay { from: 0, id: 1 }←member@0#1, observed[0→]:(1, [1, 0, 0, 0, 0, 0, 0, 0]) from member@0 through send #0←member@0#0, observed[1→]:(1, [1, 0, 0, 0, 0, 0, 0, 0]) from member@1 through send #0←member@1#0, observed[2→]:(1, [1, 0, 0, 0, 0, 0, 0, 0]) from member@2 through send #0←member@2#0}

```mermaid
---
title: goal held under {crash(source after bcast[→0]:1)}: its support (★)
---
sequenceDiagram
    participant p0 as source
    participant p1 as member@0
    participant p2 as member@1
    participant p3 as member@2
    rect rgb(255, 243, 205)
    p0->>p1: ★ bcast: 1
    end
    Note over p0: 💀 crashed
    p0--xp2: bcast: 1 (lost: sender crashed)
    p0--xp3: bcast: 1 (lost: sender crashed)
    p0--xp1: bcast: 2 (lost: sender crashed)
    p0--xp2: bcast: 2 (lost: sender crashed)
    p0--xp3: bcast: 2 (lost: sender crashed)
    Note over p1: ⇒ 0100000000000000
    rect rgb(255, 243, 205)
    p1->>p2: ★ relay: Relay { from: 0, id: 1 }
    end
    Note over p2: ⇒ 0100000000000000
    rect rgb(255, 243, 205)
    p1->>p3: ★ relay: Relay { from: 0, id: 1 }
    end
    Note over p3: ⇒ 0100000000000000
    Note over p0,p3: EFF: end of finite failures
    p3->>p1: ack: Ack { from: 2, id: 1 }
    p2->>p1: ack: Ack { from: 1, id: 1 }
```


_Lineage level: precise._
