# Hydro Development Notes

## Critical API Gotchas

### `Stream::scan` — `None` TERMINATES the stream

`scan`'s closure returns `Option<U>`. Returning `None` does NOT mean "skip this element" — 
it means **terminate the stream permanently**. No more elements will ever be processed.

If you want scan-with-filter semantics (stateful processing that sometimes emits, sometimes doesn't),
return `Option<Option<T>>` — always `Some(...)` to keep the stream alive, with the inner `Option`
indicating whether to emit. Then `.filter_map(q!(|x| x))` after the scan.

```rust
// WRONG — terminates on first non-emitting event:
.scan(q!(|| state), q!(|s, x| {
    if should_emit(s, x) { Some(value) } else { None }  // None kills the stream!
}))

// RIGHT — never terminates, filters after:
.scan(q!(|| state), q!(|s, x| -> Option<Option<T>> {
    if should_emit(s, x) { Some(Some(value)) } else { Some(None) }
}))
.filter_map(q!(|x| x))
```

### `Stream::timeout` — returns `Optional` with `Unbounded` bound

`.timeout()` returns `Optional<(), L, Unbounded>`. You cannot call `.into_stream()` on it
because `into_stream` requires `Bounded`. Instead, track timeout logic manually in a scan
using `Instant::now()` comparisons driven by a periodic heartbeat.

### `interleave` — just concatenation in the dataflow graph

`interleave` is `Chain` — it merges streams without blocking. All input streams can produce
independently. It does NOT require all inputs to produce before emitting.
