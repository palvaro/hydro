

## v0.0.1 (2026-05-01)

### Chore

 - <csr-id-bbe8617b47a059d36e55ac1be1940023083cf6cb/> prepare for release

### New Features

 - <csr-id-eacc3cd85a0a9bdd70c9f6b9da4da312059f3c5a/> Add scan_async_blocking operator to hydro_lang and dfir
 - <csr-id-128e85a7b6980f76575ad0e7306f34e7de0076c6/> resolve_future[s]_blocking for streams, singleton
 - <csr-id-892bcb67cc7789db6f749c2a5c38796bcb17f14a/> Add pull::FlatMapStream operator to dfir_pipes
   Implements the missing `pull::FlatMapStream` combinator, completing the
   2x2 matrix of pull/push × flatten/flat_map stream operators.
   
   - Created `dfir_pipes/src/pull/flat_map_stream.rs` with the
   `FlatMapStream` struct that maps each item to a `Stream` via a closure
   and flattens the results by polling each inner stream. Modeled after the
   existing `pull::FlattenStream` (for structure/pinning) and
   `pull::FlatMap` (for the closure pattern).
   - Registered the module and re-export in `dfir_pipes/src/pull/mod.rs`.
   - Added `flat_map_stream` method to the `Pull` trait.
   - Includes `FusedPull` impl and two tests (basic + pending propagation).
 - <csr-id-9aac9a74004e99d0c7dd4a752322fdb7008998e0/> Add Pull version of FlattenStream operator to dfir_pipes
   Created `dfir_pipes/src/pull/flatten_stream.rs` implementing a
   pull-based `FlattenStream` combinator. This operator takes a pull whose
   items are `futures_core::Stream`s and flattens them by polling each
   inner stream, yielding individual elements downstream.
   
   Key design decisions:
   - Mirrors the existing push `FlattenStream` but as a pull operator
   - Follows the same pattern as the existing pull `Flatten` (for
   iterators)
   - Uses `pin_project_lite` for safe pin projection of the inner stream
   - `CanPend` is always `Yes` since inner streams may pend
   - `CanEnd` inherits from the upstream pull
   - Requires `core::task::Context` since inner streams need polling
   - Implements `FusedPull` when the upstream is `FusedPull`
   
   Also added `flatten_stream()` method to the `Pull` trait and registered
   the module/export in `pull/mod.rs`.
 - <csr-id-a0cd424a6b4b6d8d5c0f96ed4e2aeb7e6b64a2ac/> Add StreamCompat type in dfir_pipes/pull, mirroring SinkCompat in push
   Created `dfir_pipes/src/pull/stream_compat.rs` with a
   `StreamCompat<Pul>` adapter that wraps a `Pull` and implements
   `futures_core::stream::Stream`, dropping any `Meta` data
 - <csr-id-26afc34c237bd0821c5490d8777256245176692c/> Add flat_map_stream operator to dfir_pipes::push
   Created `FlatMapStream` push combinator that maps each input item to a
   stream via a user-provided function, then flattens the resulting stream
   by polling it and pushing each element downstream. This mirrors the
   relationship between `flat_map` and `flatten` but for the async stream
   variants (`flat_map_stream` is to `flatten_stream` what `flat_map` is to
   `flatten`).
 - <csr-id-b9b0322abf37ff99cdafbc80eb0df62a8c0d53c9/> Add FlattenStream push operator for flattening Stream items
   Added a new `FlattenStream<Next, St, Meta>` push combinator in
   `dfir_pipes/src/push/flatten_stream.rs` that is the async counterpart to
   the existing `Flatten` operator. While `Flatten` synchronously iterates
   over `IntoIterator` items, `FlattenStream` polls `Stream` items
   asynchronously and propagates `Poll::Pending` as `PushStep::Pending`.
   
   Key design:
   - `CanPend = Yes` since streams can pend
   - `Ctx = core::task::Context` for async polling
   - Buffers one stream and one item at a time, draining the stream in
   `poll_ready` before accepting new items
   
   Also registered the module and added a `flatten_stream` constructor
   function in `push/mod.rs`, plus two tests verifying correct drain
   behavior and pending propagation.
   
   This does not yet add the operator to `dfir_lang`/`dfir_rs`
 - <csr-id-62966dd0fa22ba6a60dc7f41cebda6165eb4990b/> Add `SinkCompat` to turn `Push` into `Sink`, replaces `PendingFlushSink` test, various cleanups

### Bug Fixes

 - <csr-id-0fe74c4025e2b402dc46df29c21855e8ad635fde/> fix Zip/ZipLongest size_hint and starvation bugs, fix #2664
   Both `Zip` and `ZipLongest` had two bugs:
   
   1. `size_hint()` did not account for the buffered `item1` field, so the
   reported remaining count could be off by one when an item was already
   fetched from one stream but the other stream returned Pending.
   
   2. The buffer only ever stored items from stream 1 (`item1`), meaning
   stream 2 could starve if stream 1 was always pending — stream 2 would
   never get polled.

### Refactor

 - <csr-id-be35ffa266cf564cf967bb653720dc664b24b813/> remove `Pin<&Self>`, use `&self` in `Pull::size_hint`, fix #2652

### Test

 - <csr-id-d0f9d3b5ae0b281401c78702f28399fcec5a6fcd/> simplify push test utilities to use history-based TestPush, fix #2661
   Add TestPush<Item> to push test_utils with separate
   ready_steps/flush_steps
   logs, call history recording via PushCall enum, and protocol invariant
   checks.
 - <csr-id-f76b0224d62af0d4ac34a7386e45c52d4f58a81a/> simplify pull test utilities to use history-based TestPull, fix #2661
   Replace four ad-hoc pull test helpers (AsyncPull, SyncPull,
   PanicsAfterEndPull, NonFusedPull) with a single generic TestPull<Item,
   Meta, CanPend, CanEnd, FUSED> that replays a log of PullSteps. When
   FUSED=true it implements FusedPull and returns Ended when the log is
   exhausted; when FUSED=false it panics.
   
   Convenience constructors:
   - TestPull::items(iter) — non-fused, CanPend=No, yields items then Ended
   - TestPull::items_fused(iter) — fused, CanPend=No, yields items then
   Ended
   
   Update all existing tests to use the new helpers.

### New Features (BREAKING)

 - <csr-id-52ed1062f8fb30b9b2ec8f4615d9187bba62e2b0/> Add `Push::size_hint`, `VecPush` terminal operator, use in dfir codegen [ci-bench]
   Added `size_hint(self: Pin<&mut Self>, hint: (usize, Option<usize>))` to
   the `Push` trait as the push-side analog of `Pull::size_hint`. This
   allows producers to announce how many items they're about to send,
   enabling downstream operators and sinks to pre-allocate.
   
   New terminal operator:
   - `VecPush<Buf>`: pushes items into a `Vec`, uses `size_hint` to call
   `Vec::reserve(hint.0)` for pre-allocation. Gated on `alloc` feature.
   - Constructor: `push::vec_push(buf)` creates a VecPush from a `&mut
   Vec<T>`.
 - <csr-id-a662ff38541e58bec801644b81b2bfc505779e7b/> use custom `dfir_pipes::Pull` trait [ci-bench]
   This is the pull-half of a big change from using other iterators
   (`std::iter::Iterator` or `futures_core::stream::Stream`) to our own
   `Pull` trait. Key to this more powerful iterator trait is the step enum:
   ```rust
   pub enum Step<Item, Meta, CanPend: Toggle, CanEnd: Toggle> {
       /// An item is ready with associated metadata.
       Ready(Item, Meta),
       /// The pull is not ready yet (only possible when `CanPend = Yes`).
       Pending(CanPend),
       /// The pull has ended (only possible when `CanEnd = Yes`).
       Ended(CanEnd),
   }
   ```
   This abstraction allows `Pull` to represent both synchronous `Iterator`s
   and asynchronous `Stream`s with zero cost. (As well as distinguishing
   between infinite vs finite iterators, which I guess is not actually that
   useful to us). In the future we will also add an `Error` variant
   (#2635). The `Meta` metadata field may be used for full record-level
   tracing (#2242).
   
   This trait has some pseudo-specialization around `Fuse`, and further
   performance improvements may come from true nightly
   `min_specialization`, as well as from converting from `Pusherator/Sink`
   to a new `Push` trait.
   
   Other changes:
   * Moves much of `dfir_rs::compiled::pull` into `dfir_pipes`, using new
   trait
   * Update itertools to `0.14`

### Refactor (BREAKING)

 - <csr-id-db8d7f1a7fb2556302d92ff77dbb40beef8f3a44/> TakeWhile should not do its own fusing, fix #2659
   Remove self-fusing behavior from TakeWhile pull combinator:
   - Remove done: bool field and FusedPull impl
   - Remove fuse_self!() macro invocation
   - Simplify pull() and size_hint() by removing done tracking
   - Remove test (fusing is already covered by Fuse's own tests)
   
   Users who need fused behavior should now call .fuse() explicitly.
   
   #2659
 - <csr-id-3e6e26c4cc87d6f7857591b10876074cba97caff/> use custom `dfir_pipes::Push` trait instead of `Sink` [ci-bench]

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 17 commits contributed to the release.
 - 17 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 16 unique issues were worked on: [#2618](https://github.com/hydro-project/hydro/issues/2618), [#2644](https://github.com/hydro-project/hydro/issues/2644), [#2665](https://github.com/hydro-project/hydro/issues/2665), [#2666](https://github.com/hydro-project/hydro/issues/2666), [#2671](https://github.com/hydro-project/hydro/issues/2671), [#2673](https://github.com/hydro-project/hydro/issues/2673), [#2674](https://github.com/hydro-project/hydro/issues/2674), [#2675](https://github.com/hydro-project/hydro/issues/2675), [#2678](https://github.com/hydro-project/hydro/issues/2678), [#2680](https://github.com/hydro-project/hydro/issues/2680), [#2681](https://github.com/hydro-project/hydro/issues/2681), [#2682](https://github.com/hydro-project/hydro/issues/2682), [#2683](https://github.com/hydro-project/hydro/issues/2683), [#2684](https://github.com/hydro-project/hydro/issues/2684), [#2686](https://github.com/hydro-project/hydro/issues/2686), [#2710](https://github.com/hydro-project/hydro/issues/2710)

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **[#2618](https://github.com/hydro-project/hydro/issues/2618)**
    - Use custom `dfir_pipes::Pull` trait [ci-bench] ([`a662ff3`](https://github.com/hydro-project/hydro/commit/a662ff38541e58bec801644b81b2bfc505779e7b))
 * **[#2644](https://github.com/hydro-project/hydro/issues/2644)**
    - Use custom `dfir_pipes::Push` trait instead of `Sink` [ci-bench] ([`3e6e26c`](https://github.com/hydro-project/hydro/commit/3e6e26c4cc87d6f7857591b10876074cba97caff))
 * **[#2665](https://github.com/hydro-project/hydro/issues/2665)**
    - TakeWhile should not do its own fusing, fix #2659 ([`db8d7f1`](https://github.com/hydro-project/hydro/commit/db8d7f1a7fb2556302d92ff77dbb40beef8f3a44))
 * **[#2666](https://github.com/hydro-project/hydro/issues/2666)**
    - Fix Zip/ZipLongest size_hint and starvation bugs, fix #2664 ([`0fe74c4`](https://github.com/hydro-project/hydro/commit/0fe74c4025e2b402dc46df29c21855e8ad635fde))
 * **[#2671](https://github.com/hydro-project/hydro/issues/2671)**
    - Remove `Pin<&Self>`, use `&self` in `Pull::size_hint`, fix #2652 ([`be35ffa`](https://github.com/hydro-project/hydro/commit/be35ffa266cf564cf967bb653720dc664b24b813))
 * **[#2673](https://github.com/hydro-project/hydro/issues/2673)**
    - Simplify pull test utilities to use history-based TestPull, fix #2661 ([`f76b022`](https://github.com/hydro-project/hydro/commit/f76b0224d62af0d4ac34a7386e45c52d4f58a81a))
 * **[#2674](https://github.com/hydro-project/hydro/issues/2674)**
    - Simplify push test utilities to use history-based TestPush, fix #2661 ([`d0f9d3b`](https://github.com/hydro-project/hydro/commit/d0f9d3b5ae0b281401c78702f28399fcec5a6fcd))
 * **[#2675](https://github.com/hydro-project/hydro/issues/2675)**
    - Add `SinkCompat` to turn `Push` into `Sink`, replaces `PendingFlushSink` test, various cleanups ([`62966dd`](https://github.com/hydro-project/hydro/commit/62966dd0fa22ba6a60dc7f41cebda6165eb4990b))
 * **[#2678](https://github.com/hydro-project/hydro/issues/2678)**
    - Add `Push::size_hint`, `VecPush` terminal operator, use in dfir codegen [ci-bench] ([`52ed106`](https://github.com/hydro-project/hydro/commit/52ed1062f8fb30b9b2ec8f4615d9187bba62e2b0))
 * **[#2680](https://github.com/hydro-project/hydro/issues/2680)**
    - Add FlattenStream push operator for flattening Stream items ([`b9b0322`](https://github.com/hydro-project/hydro/commit/b9b0322abf37ff99cdafbc80eb0df62a8c0d53c9))
 * **[#2681](https://github.com/hydro-project/hydro/issues/2681)**
    - Add Pull version of FlattenStream operator to dfir_pipes ([`9aac9a7`](https://github.com/hydro-project/hydro/commit/9aac9a74004e99d0c7dd4a752322fdb7008998e0))
 * **[#2682](https://github.com/hydro-project/hydro/issues/2682)**
    - Add flat_map_stream operator to dfir_pipes::push ([`26afc34`](https://github.com/hydro-project/hydro/commit/26afc34c237bd0821c5490d8777256245176692c))
 * **[#2683](https://github.com/hydro-project/hydro/issues/2683)**
    - Add StreamCompat type in dfir_pipes/pull, mirroring SinkCompat in push ([`a0cd424`](https://github.com/hydro-project/hydro/commit/a0cd424a6b4b6d8d5c0f96ed4e2aeb7e6b64a2ac))
 * **[#2684](https://github.com/hydro-project/hydro/issues/2684)**
    - Add pull::FlatMapStream operator to dfir_pipes ([`892bcb6`](https://github.com/hydro-project/hydro/commit/892bcb67cc7789db6f749c2a5c38796bcb17f14a))
 * **[#2686](https://github.com/hydro-project/hydro/issues/2686)**
    - Resolve_future[s]_blocking for streams, singleton ([`128e85a`](https://github.com/hydro-project/hydro/commit/128e85a7b6980f76575ad0e7306f34e7de0076c6))
 * **[#2710](https://github.com/hydro-project/hydro/issues/2710)**
    - Add scan_async_blocking operator to hydro_lang and dfir ([`eacc3cd`](https://github.com/hydro-project/hydro/commit/eacc3cd85a0a9bdd70c9f6b9da4da312059f3c5a))
 * **Uncategorized**
    - Prepare for release ([`bbe8617`](https://github.com/hydro-project/hydro/commit/bbe8617b47a059d36e55ac1be1940023083cf6cb))
</details>

