# A Linearizable Protocol over an Eventually Consistent Substrate

2026-08. Companion code:
`hydro_std/src/ec_inference_demos/abd.rs`.

## The observation

Refactoring ABD exposed a semantic boundary that is more important than a
reduction in lines of code:

1. `abd_register_state` is a clean, monotone, eventually consistent object.
2. `abd_replica` is an explicitly schedule-sensitive asynchronous protocol.
3. The internal EC object is **not** linearizable.
4. The protocol built around it makes the externally visible register
   operations linearizable.

This is not an embarrassment or a mismatch between labels. It is the
architecture of ABD.

The refactoring is valuable because the source code now becomes ugly at the
same point where the proof obligation becomes ugly. Algebraic convergence is
short and compositional. Combining asynchronous observations and effects is
verbose, nondeterministic, and manually reasoned about. The code should not
make those two activities look alike.

## 1. The clean object: a max register

The internal replica state is approximately:

```rust
phase2
    .filter_map(extract_timestamped_value)
    .fold(max_by_timestamp)
```

Its contract is small:

- State is empty or one `(timestamp, value)` pair.
- Applying a pair with a greater timestamp replaces the current pair.
- Applying an older pair changes nothing.
- State never decreases by timestamp.
- Subject to timestamp/value uniqueness, order, duplicates, and batch
  boundaries do not affect the eventual maximum.
- Because every live replica folds the same EC stream through the same
  order-insensitive operation, the resulting register state is EC.

This is exactly the kind of program for which Hydro's type and algebraic
machinery helps. The dataflow is structurally symmetric, the fold is a join,
and scheduling nondeterminism is erased by the algebra: every permutation of
the same valid inputs yields the same maximum.

The implementation is correspondingly terse. That terseness is earned, not
merely stylistic. Most of the correctness argument is contained in the shape
of the computation and the max operation's obligations.

### The max register is not linearizable

The EC label must not be mistaken for an atomic-register specification.
During an execution:

- replicas can temporarily hold different maxima;
- a write can be present at only a minority;
- a snapshot at one replica can miss a value present at another;
- two concurrent snapshots can disagree;
- no single local snapshot is a completed ABD read;
- the fold provides no global linearization point.

The state object promises monotonic local evolution and eventual convergence.
It does not promise that every observation reflects a single global real-time
order.

Its cleanliness comes partly from not trying to provide that stronger
interface. It accepts timestamped writes, refuses nothing, and merges by max.

## 2. The ugly object: an asynchronous replica protocol

The replica protocol surrounds that state with operations resembling:

```rust
sliced! {
    let reg_now = snapshot(register, ...);
    let new_writes = batch(phase2, ...);
    let query_batch = batch(queries, ...);

    combine new writes with pending acknowledgements;
    acknowledge writes covered by reg_now;
    retain writes not yet covered;
    answer this batch of queries from reg_now;
}
```

This code crosses from algebraic state evolution into temporal reasoning.
The scheduler now chooses:

- which version of the register the slice observes;
- which phase-2 messages enter this tick;
- which messages remain for later ticks;
- which queries are answered now;
- whether a write is installed directly or is already superseded when acked;
- which value a query concurrent with writes observes.

These choices are not all erased. They can affect acknowledgements, covering
certificates, timestamps minted by later writes, and values returned by
concurrent reads.

Correctness therefore does not mean that every schedule produces the same
trace. It means that **every allowed schedule produces a trace satisfying the
replica contract**.

### The sequential reading is a feature

Despite combining asynchronous streams, the `sliced!` block has a useful
small-event-loop reading:

1. Observe the current register state.
2. Admit this tick's new work.
3. Combine new work with persisted pending work.
4. Acknowledge requests whose timestamps are now covered.
5. Preserve requests that are not yet covered.
6. Answer the queries admitted this tick.
7. Emit responses.

This sequential feel is helpful because it exposes the serialization the
programmer must reason about. It does not make the system deterministic; it
makes one scheduled step legible.

## 3. What the `nondet!` markers mean

The `nondet!` markers around snapshots and batches are not claims that the
nondeterminism is locally resolved. They are hazard markers:

> This operation admits a schedule-authored choice. The choice may be
> observable. The programmer must show that the required invariant holds for
> every resolution.

There are two importantly different kinds of nondeterminism in this code.

### Algebraically erased nondeterminism

Inside the max register, input ordering and batching do not alter the final
join. The schedule may choose an order, but the algebra removes that choice
from the eventual result.

This is where commutativity and idempotence are powerful. The type/algebraic
story can carry most of the burden.

### Observable, protocol-sanctioned nondeterminism

Inside the replica service:

- an older snapshot may delay an acknowledgement;
- a newer snapshot may permit it;
- a different phase-2 batch may let a higher write supersede a lower one;
- delaying a query may cause it to return a higher timestamp;
- a different response majority may produce a different covering maximum.

Those differences may escape into externally visible outcomes. They are legal
when operations overlap: linearizability permits a concurrent operation to be
placed at any point in its interval that yields a legal sequential history.

The proof obligation is not confluence of traces. It is closure of the safety
property under every schedule-authored choice.

## 4. Where the type system helps—and where it does not

The type system can materially help with the internal state:

- the phase-2 broadcast produces an EC stream;
- the register fold retains an explicit EC location type;
- the max operation is forced to state its algebraic obligations;
- a refactor that loses the EC structure fails the explicit type annotation.

The replica response stream correctly has no EC claim. Different replicas do
not and should not converge on the same response trace. They answer different
requesters at different times from different legal snapshots.

The type system does not establish temporal claims such as:

- an acknowledgement cannot escape before state covers its timestamp;
- pending acknowledgements cannot be forgotten;
- a query processed after an acknowledgement cannot observe older state;
- a read does not return before write-back reaches a quorum;
- two quorum sets intersect;
- operation intervals admit a legal linearization.

Those facts are relational across events, locations, and time. In the current
system they require explicit protocol code, component contracts, exhaustive
bounded audits, and a composition argument.

The honest message at the boundary is:

> Type system won't help here, bud.

That is not a rejection of types. It is a demand that the implementation make
clear where their jurisdiction ends.

## 5. How the ugly layer constructs linearizability

The internal EC state becomes an externally linearizable register only when
combined with the rest of ABD:

1. **Covering read.** An operation consults a quorum rather than one replica.
2. **Timestamp selection.** A write chooses a timestamp strictly above the
   covering maximum.
3. **Ack gate.** A replica attests only after its local monotone state covers
   the requested timestamp.
4. **Durable completion.** An operation completes phase 2 only after a quorum
   of distinct gated acknowledgements.
5. **Read repair.** A nonempty read writes its adopted pair back to a quorum
   before returning.
6. **Quorum intersection.** Every later covering intersects the durable set
   left by every earlier completed nonempty operation.

The key local fact supplied by the ugly replica protocol is:

> Once replica `r` acknowledges timestamp `t`, its register is at least `t`
> forever.

The quorum layer lifts enough of those local facts into a global one:

> Once an operation completes with timestamp `t`, at least a quorum of
> replicas remain at or above `t`.

Intersection then turns that durable global fact into real-time ordering:

> A covering begun after that completion observes a maximum at least `t`; a
> later write chooses strictly above it, while a later read adopts at least it.

In compact form:

```text
local monotonicity
+ sound gated acknowledgements
+ distinct-member quorum certificates
+ quorum intersection
+ read repair
= externally linearizable operations
```

Linearizability therefore belongs to the composition. It is not a property of
`abd_register_state`, `abd_replica`, or the quorum mint in isolation.

## 6. EC inside and linearizable outside is not a contradiction

A stronger abstraction can be implemented over weaker internal components if
the stronger interface performs additional coordination before exposing a
result.

ABD never exposes an arbitrary replica snapshot as a completed public read. A
public read:

1. obtains a covering quorum;
2. selects the maximum response;
3. writes that pair back;
4. waits for a quorum of sound acknowledgements;
5. only then returns.

Similarly, a public write is not complete merely because one EC replica has
seen it. Completion means that quorum evidence has been assembled.

The public operation boundaries deliberately hide transient internal
inconsistency. The internal object remains EC; the external operation history
can nevertheless be linearizable.

## 7. Ugliness as a correctness signal

It is tempting to abstract the replica service behind a generic "voting
round" callback. The attempted transport abstraction did not help: it moved
short networking expressions behind large generic signatures while leaving
the actual difficulty untouched.

A more aggressive abstraction would be worse if it made the schedule-sensitive
kernel appear clean. The snapshots, batches, pending state, and gates are not
incidental boilerplate. They are the implementation surface of the temporal
proof obligations.

A useful design principle is:

> Make convergent algebra pleasant and compositional. Make schedule-sensitive
> orchestration explicit, verbose, and difficult to write casually.

Some ugliness is debt. But some ugliness is also a warning label. Here it tells
the reviewer:

- several asynchronous histories are being combined;
- choices may escape into observable outcomes;
- the consistency label has weakened intentionally;
- manual invariants begin here;
- every state transition deserves scrutiny.

The right goal is not to beautify that boundary indiscriminately. It is to
localize it, give it a sequential reading, state its contract, and test the
exact production kernel over tractable finite schedules.

## 8. Review rule suggested by the refactoring

When reviewing a distributed Hydro program, ask which side of this boundary
each block inhabits.

For a convergent algebraic block:

- What is the join/order?
- Are the combiner obligations true?
- Is nondeterminism erased by the algebra?
- Does the consistency type follow structurally?

For a schedule-sensitive protocol block:

- Which asynchronous inputs are combined?
- Which schedule choices are observable?
- What pending work must persist?
- What effects are gated on which observations?
- Can an effect outrun the state fact that justifies it?
- What local contract is exported to the composition proof?
- Which parts are exhaustively auditable, and which remain manual assumptions?

A function boundary between those two regimes is valuable even if it does not
reduce LOC. It separates two different modes of reasoning.

## 9. The broader lesson

Consistency labels describe components, not automatically the strongest
semantics of the abstraction built from them.

ABD demonstrates all of the following simultaneously:

- an internal object can be EC and non-linearizable;
- the type system can correctly infer that EC property;
- a surrounding protocol can use quorums and temporal gates to construct a
  linearizable public interface;
- the surrounding response traffic can correctly carry no EC label;
- the strongest external property can be a manual composition theorem even
  when important internal properties are typed;
- source-code cleanliness should track the applicable reasoning discipline,
  not merely aesthetic uniformity.

The refactoring's main result is therefore not reuse. It is an honest map of
where correctness comes from:

```text
clean typed lattice            ugly explicit protocol
-------------------            ----------------------
eventual convergence           temporal safety
order-insensitive merge        schedule-authored observations
local algebraic obligations    cross-event invariants
EC-inferred state              linearizable composition
```

The clean half is not the finished register. The ugly half is not failed
abstraction. Together they are ABD.
