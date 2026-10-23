# goal violated by {omit(msg:Msg { attempt: 1 }), omit(msg:Msg { attempt: 2 })}

Found after 3 hypothesis run(s), 3 schedule(s) explored.

## The failing run

```mermaid
---
title: goal violated by {omit(msg:Msg { attempt: 1 }), omit(msg:Msg { attempt: 2 })}
---
sequenceDiagram
    participant p0 as Process(loc1v1)
    participant p1 as Process(loc2v1)
    p0-xp1: msg: Msg { attempt: 1 } (omitted)
    p0-xp1: msg: Msg { attempt: 2 } (omitted)
    Note over p0,p1: EFF: end of finite failures
```

## Why the goal held before

The goal's support in the last run in which it held: {msg:Msg { attempt: 2 }}

```mermaid
---
title: goal held under {omit(msg:Msg { attempt: 1 })}: its support (★)
---
sequenceDiagram
    participant p0 as Process(loc1v1)
    participant p1 as Process(loc2v1)
    p0-xp1: msg: Msg { attempt: 1 } (omitted)
    rect rgb(255, 243, 205)
    p0->>p1: ★ msg: Msg { attempt: 2 }
    end
    Note over p1: ⇒ 02000000
    p1->>p0: ack: Ack { attempt: 2 }
    Note over p0,p1: EFF: end of finite failures
```

