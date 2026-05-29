# Hydro Framework Issue: `SendExternal` from Cluster is unimplemented

## Summary

`bind_single_client_bincode` (and by extension `send_bincode_external`) compiles when called on a `Cluster`, but panics at runtime during network compilation with `not yet implemented` at `hydro_lang/src/compile/ir/mod.rs:811`.

This blocks the multi-router (Cluster<Coordinator>) pattern where an external client communicates bidirectionally with a cluster of stateless coordinators.

## Location

`hydro_lang/src/compile/ir/mod.rs`, line 811:

```rust
match input.metadata().location_id.root() {
    &LocationId::Process(process_key) => {
        // ... 60 lines of implementation for Process ...
    }
    LocationId::Cluster(_) => todo!(),  // <--- HERE
    _ => panic!()
}
```

This is inside the `compile_network` function, in the `HydroRoot::SendExternal` handling branch.

## Reproduction

```rust
let coordinators: Cluster<'a, Coordinator> = builder.cluster();
let external = builder.external::<()>();

// This compiles fine:
let (bidi_port, cmds, resp_sink) =
    coordinators.bind_single_client_bincode::<_, String, String>(&external);

// But panics at runtime during .deploy():
let nodes = builder
    .with_cluster(&coordinators, hosts.iter().map(|h| ...))
    .with_external(&external, deployment.Localhost())
    .deploy(&mut deployment);  // PANICS: "not yet implemented"
```

## Use Case

We're building a replicated state machine with a cluster of stateless coordinators (routers). Each coordinator independently:
- Receives commands from external clients
- Broadcasts them to replicas
- Runs a failure detector
- Returns responses

The external client should connect to any coordinator. If one dies, the client reconnects to another. This requires `SendExternal` to work from a `Cluster` location.

## Expected Behavior

For the `Cluster` case, the framework should wire the external port to the cluster similarly to how it handles `Process`:
- The external connects to one member of the cluster (e.g., the first, or round-robin)
- Each cluster member can send responses back to the external through its own port
- The external receives from whichever member responds

Alternatively, the external could connect to ALL members (like broadcast semantics), receiving merged responses from all of them.

## Workaround

Currently the only workaround is to use a single `Process<Coordinator>` as the external-facing gateway, which defeats the purpose of having a fault-tolerant coordinator cluster.

## Impact

This blocks:
1. Multi-router deployments where coordinators can come and go
2. Testing coordinator cluster resilience (kill one coordinator, system keeps working through another)
3. Horizontal scaling of the coordinator/router layer
