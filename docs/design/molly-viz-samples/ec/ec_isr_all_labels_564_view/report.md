# goal violated by {crash(zk after view[→1]:View { leader: Some(0), isr: [0, 1, 2] })}

Found after 8 hypothesis run(s), 64 schedule(s) explored.

## The failing run

```mermaid
---
title: goal violated by {crash(zk after view[→1]:View { leader: Some(0), isr: [0, 1, 2] })}
---
sequenceDiagram
    participant p0 as client
    participant p1 as replica@0
    participant p2 as replica@1
    participant p3 as replica@2
    participant p4 as zk
    p0->>p1: write: Write(1)
    Note over p1: ⇒ 01000000
    p0->>p2: write: Write(1)
    p1->>p2: repl: Write(1)
    Note over p2: ⇒ 01000000
    p0->>p3: write: Write(1)
    p1->>p3: repl: Write(1)
    Note over p3: ⇒ 01000000
    p2->>p1: ack: Write(1)
    p3->>p1: ack: Write(1)
    p1->>p0: client-ack: Write(1)
    Note over p0: ⇒ 01000000
    p2->>p4: hb: ()
    p3->>p4: hb: ()
    p1->>p4: hb: ()
    Note over p4: ⇒ 0100000000030000…
    p4->>p2: view: View { leader: Some(0), isr: [0, 1, 2] }
    Note over p4: 💀 crashed
    p4--xp3: view: View { leader: Some(0), isr: [0, 1, 2] } (lost: sender crashed)
    p4->>p1: view: View { leader: Some(0), isr: [0, 1, 2] }
    Note over p0,p4: EFF: end of finite failures
    p1->>p4: hb: ()
    p2->>p4: hb: ()
    p3->>p4: hb: ()
    Note over p4: ⇒ 0100000000030000… (suppressed: node crashed)
    p4--xp1: view: View { leader: Some(0), isr: [0, 1, 2] } (lost: sender crashed)
    p4--xp2: view: View { leader: Some(0), isr: [0, 1, 2] } (lost: sender crashed)
    p4--xp3: view: View { leader: Some(0), isr: [0, 1, 2] } (lost: sender crashed)
    p1->>p4: hb: ()
    p2->>p4: hb: ()
    p3->>p4: hb: ()
    Note over p4: ⇒ 0100000000030000… (suppressed: node crashed)
    p4--xp1: view: View { leader: Some(0), isr: [0, 1, 2] } (lost: sender crashed)
    p4--xp2: view: View { leader: Some(0), isr: [0, 1, 2] } (lost: sender crashed)
    p4--xp3: view: View { leader: Some(0), isr: [0, 1, 2] } (lost: sender crashed)
    p3->>p4: hb: ()
    p2->>p4: hb: ()
    p1->>p4: hb: ()
    Note over p4: ⇒ 0100000000030000… (suppressed: node crashed)
    p4--xp1: view: View { leader: Some(0), isr: [0, 1, 2] } (lost: sender crashed)
    p4--xp2: view: View { leader: Some(0), isr: [0, 1, 2] } (lost: sender crashed)
    p4--xp3: view: View { leader: Some(0), isr: [0, 1, 2] } (lost: sender crashed)
```

## Why the goal held before

The goal's support in the last run in which it held: {hb[0→]:()←replica@0#3, hb[1→]:()←replica@1#1, hb[2→]:()←replica@2#1, view[→0]:View { leader: Some(0), isr: [0, 1, 2] }←zk#0, view[→1]:View { leader: Some(0), isr: [0, 1, 2] }←zk#1, observed[0→]:(2, [1, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0]) from replica@0 through send #4←replica@0#4, observed[1→]:(2, [1, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0]) from replica@1 through send #2←replica@1#2}

```mermaid
---
title: goal held under {crash(replica@2 after hb[2→]:())}: its support (★)
---
sequenceDiagram
    participant p0 as client
    participant p1 as replica@0
    participant p2 as replica@1
    participant p3 as replica@2
    participant p4 as zk
    p0->>p1: write: Write(1)
    Note over p1: ⇒ 01000000
    p0->>p2: write: Write(1)
    p1->>p2: repl: Write(1)
    Note over p2: ⇒ 01000000
    p2->>p1: ack: Write(1)
    p0->>p3: write: Write(1)
    p1->>p3: repl: Write(1)
    Note over p3: ⇒ 01000000
    p3->>p1: ack: Write(1)
    p1->>p0: client-ack: Write(1)
    Note over p0: ⇒ 01000000
    rect rgb(255, 243, 205)
    p3->>p4: ★ hb: ()
    end
    Note over p3: 💀 crashed
    rect rgb(255, 243, 205)
    p2->>p4: ★ hb: ()
    end
    rect rgb(255, 243, 205)
    p1->>p4: ★ hb: ()
    end
    Note over p4: ⇒ 0100000000030000…
    p4->>p3: view: View { leader: Some(0), isr: [0, 1, 2] }
    rect rgb(255, 243, 205)
    p4->>p2: ★ view: View { leader: Some(0), isr: [0, 1, 2] }
    end
    rect rgb(255, 243, 205)
    p4->>p1: ★ view: View { leader: Some(0), isr: [0, 1, 2] }
    end
    Note over p0,p4: EFF: end of finite failures
    rect rgb(255, 243, 205)
    p3--xp4: ★ hb: () (lost: sender crashed)
    end
    rect rgb(255, 243, 205)
    p2->>p4: ★ hb: ()
    end
    rect rgb(255, 243, 205)
    p1->>p4: ★ hb: ()
    end
    Note over p4: ⇒ 0100000000020000…
    p4->>p3: view: View { leader: Some(0), isr: [0, 1] }
    p4->>p1: view: View { leader: Some(0), isr: [0, 1] }
    p4->>p2: view: View { leader: Some(0), isr: [0, 1] }
    rect rgb(255, 243, 205)
    p2->>p4: ★ hb: ()
    end
    rect rgb(255, 243, 205)
    p1->>p4: ★ hb: ()
    end
    rect rgb(255, 243, 205)
    p3--xp4: ★ hb: () (lost: sender crashed)
    end
    Note over p4: ⇒ 0100000000020000…
    p4->>p2: view: View { leader: Some(0), isr: [0, 1] }
    p4->>p1: view: View { leader: Some(0), isr: [0, 1] }
    p4->>p3: view: View { leader: Some(0), isr: [0, 1] }
    rect rgb(255, 243, 205)
    p1->>p4: ★ hb: ()
    end
    rect rgb(255, 243, 205)
    p3--xp4: ★ hb: () (lost: sender crashed)
    end
    rect rgb(255, 243, 205)
    p2->>p4: ★ hb: ()
    end
    Note over p4: ⇒ 0100000000020000…
    p4->>p2: view: View { leader: Some(0), isr: [0, 1] }
    p4->>p3: view: View { leader: Some(0), isr: [0, 1] }
    p4->>p1: view: View { leader: Some(0), isr: [0, 1] }
```

