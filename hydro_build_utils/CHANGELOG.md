

## v0.1.0 (2026-05-01)

### Bug Fixes

 - <csr-id-76fe9db0388dd4d2fbede9db5aa0bce6ade5da3d/> fix generation of `surface_example.rs` test snapshots, used in book
   #2028 caused the snapshots to save to the wrong files, and #2283 renamed
   things further. Didn't cause any book build failure as the old snapshots
   still exist
   
   Also updates nightly snapshots

### Style

 - <csr-id-1c8f85366c592b2768df65ba1ee2e98d2c06d496/> leading colons to workaround rustfmt change
   Workaround change introduced in
   https://github.com/rust-lang/rustfmt/pull/6784

### New Features (BREAKING)

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

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 3 commits contributed to the release over the course of 147 calendar days.
 - 156 days passed between releases.
 - 3 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 3 unique issues were worked on: [#2350](https://github.com/hydro-project/hydro/issues/2350), [#2618](https://github.com/hydro-project/hydro/issues/2618), [#2623](https://github.com/hydro-project/hydro/issues/2623)

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **[#2350](https://github.com/hydro-project/hydro/issues/2350)**
    - Fix generation of `surface_example.rs` test snapshots, used in book ([`76fe9db`](https://github.com/hydro-project/hydro/commit/76fe9db0388dd4d2fbede9db5aa0bce6ade5da3d))
 * **[#2618](https://github.com/hydro-project/hydro/issues/2618)**
    - Use custom `dfir_pipes::Pull` trait [ci-bench] ([`a662ff3`](https://github.com/hydro-project/hydro/commit/a662ff38541e58bec801644b81b2bfc505779e7b))
 * **[#2623](https://github.com/hydro-project/hydro/issues/2623)**
    - Leading colons to workaround rustfmt change ([`1c8f853`](https://github.com/hydro-project/hydro/commit/1c8f85366c592b2768df65ba1ee2e98d2c06d496))
</details>

## v0.0.1 (2025-11-25)

<csr-id-dc170e63f62e890bfd0dd054e5a930607fd67545/>

### Bug Fixes

 - <csr-id-c40876ec4bd3b31254d683e479b9a235f3d11f67/> refactor github actions workflows, make stable the default toolchain

### Test

 - <csr-id-dc170e63f62e890bfd0dd054e5a930607fd67545/> exclude crate/module in lib snapshot file names [ci-full]
   Also removes unused
   `hydro_lang/src/compile/ir/snapshots/hydro_lang__compile__ir__backtrace__tests__backtrace.snap`

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 3 commits contributed to the release.
 - 2 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 2 unique issues were worked on: [#2028](https://github.com/hydro-project/hydro/issues/2028), [#2283](https://github.com/hydro-project/hydro/issues/2283)

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **[#2028](https://github.com/hydro-project/hydro/issues/2028)**
    - Refactor github actions workflows, make stable the default toolchain ([`c40876e`](https://github.com/hydro-project/hydro/commit/c40876ec4bd3b31254d683e479b9a235f3d11f67))
 * **[#2283](https://github.com/hydro-project/hydro/issues/2283)**
    - Exclude crate/module in lib snapshot file names [ci-full] ([`dc170e6`](https://github.com/hydro-project/hydro/commit/dc170e63f62e890bfd0dd054e5a930607fd67545))
 * **Uncategorized**
    - Release hydro_build_utils v0.0.1, dfir_lang v0.15.0, dfir_macro v0.15.0, variadics v0.0.10, sinktools v0.0.1, hydro_deploy_integration v0.15.0, lattices_macro v0.5.11, variadics_macro v0.6.2, lattices v0.6.2, multiplatform_test v0.6.0, dfir_rs v0.15.0, copy_span v0.1.0, hydro_deploy v0.15.0, hydro_lang v0.15.0, hydro_std v0.15.0, safety bump 5 crates ([`092de25`](https://github.com/hydro-project/hydro/commit/092de252238dfb9fa6b01e777c6dd8bf9db93398))
</details>

