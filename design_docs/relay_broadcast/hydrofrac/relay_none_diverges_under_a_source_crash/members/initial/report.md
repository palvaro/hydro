# goal violated by {crash(source after bcast[→1]:2)}

Found after 5 hypothesis run(s), 40 schedule(s) explored.

## The failing run

```mermaid
---
title: goal violated by {crash(source after bcast[→1]:2)}
---
sequenceDiagram
    participant p0 as source
    participant p1 as member@0
    participant p2 as member@1
    participant p3 as member@2
    p0->>p1: bcast: 1
    p0->>p1: bcast: 2
    Note over p1: ⇒ 0100000000000000
    Note over p1: ⇒ 0200000000000000
    p0->>p2: bcast: 1
    p0->>p2: bcast: 2
    Note over p0: 💀 crashed
    p0--xp3: bcast: 2 (lost: sender crashed)
    Note over p2: ⇒ 0100000000000000
    Note over p2: ⇒ 0200000000000000
    p0->>p3: bcast: 1
    Note over p3: ⇒ 0100000000000000
    Note over p0,p3: EFF: end of finite failures
```

## Why the goal held before

The goal's support in the last run in which it held: {bcast[→0]:1←source#0, bcast[→0]:2←source#3, bcast[→1]:1←source#1, bcast[→1]:2←source#4, observed[0→]:(0, [1, 0, 0, 0, 0, 0, 0, 0]) from member@0 through send #0←member@0#0, observed[0→]:(0, [2, 0, 0, 0, 0, 0, 0, 0]) from member@0 through send #0←member@0#0, observed[1→]:(0, [1, 0, 0, 0, 0, 0, 0, 0]) from member@1 through send #0←member@1#0, observed[1→]:(0, [2, 0, 0, 0, 0, 0, 0, 0]) from member@1 through send #0←member@1#0}

```mermaid
---
title: goal held under {crash(member@2 before any send)}: its support (★)
---
sequenceDiagram
    participant p0 as source
    participant p1 as member@0
    participant p2 as member@1
    participant p3 as member@2
    Note over p3: 💀 crashed before any send
    rect rgb(255, 243, 205)
    p0->>p1: ★ bcast: 1
    end
    rect rgb(255, 243, 205)
    p0->>p1: ★ bcast: 2
    end
    Note over p1: ⇒ 0100000000000000
    Note over p1: ⇒ 0200000000000000
    rect rgb(255, 243, 205)
    p0->>p2: ★ bcast: 1
    end
    rect rgb(255, 243, 205)
    p0->>p2: ★ bcast: 2
    end
    Note over p2: ⇒ 0100000000000000
    Note over p2: ⇒ 0200000000000000
    p0->>p3: bcast: 1
    p0->>p3: bcast: 2
    Note over p3: ⇒ 0100000000000000 (suppressed: node crashed)
    Note over p3: ⇒ 0200000000000000 (suppressed: node crashed)
    Note over p0,p3: EFF: end of finite failures
```


_Lineage level: precise._
