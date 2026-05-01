

## v0.1.1 (2026-05-01)

<csr-id-6ead0cd2d1d89c393b0e5add7ceae5081c302077/>

### New Features

 - <csr-id-1866677da0a5b71e48d7b1cdf9442ee66d0f1e23/> improve quality of error spans in `sliced!` macro

### Refactor

 - <csr-id-6ead0cd2d1d89c393b0e5add7ceae5081c302077/> remove assumption that `use` always has two parameters
   Also includes some refactoring of the macro to simplify it.
   
   Includes a change to `copy_span` to take several expressions to copy the
   span from.

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 3 commits contributed to the release.
 - 156 days passed between releases.
 - 2 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 2 unique issues were worked on: [#2530](https://github.com/hydro-project/hydro/issues/2530), [#2531](https://github.com/hydro-project/hydro/issues/2531)

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **[#2530](https://github.com/hydro-project/hydro/issues/2530)**
    - Remove assumption that `use` always has two parameters ([`6ead0cd`](https://github.com/hydro-project/hydro/commit/6ead0cd2d1d89c393b0e5add7ceae5081c302077))
 * **[#2531](https://github.com/hydro-project/hydro/issues/2531)**
    - Improve quality of error spans in `sliced!` macro ([`1866677`](https://github.com/hydro-project/hydro/commit/1866677da0a5b71e48d7b1cdf9442ee66d0f1e23))
 * **Uncategorized**
    - Release hydro_build_utils v0.1.0, dfir_lang v0.16.0, dfir_macro v0.16.0, variadics v0.1.0, dfir_pipes v0.0.1, example_test v0.0.1, sinktools v0.1.0, hydro_deploy_integration v0.16.0, lattices_macro v0.6.0, variadics_macro v0.7.0, lattices v0.7.0, multiplatform_test v0.7.0, dfir_rs v0.16.0, copy_span v0.1.1, hydro_deploy v0.16.0, hydro_lang v0.16.0, hydro_std v0.16.0, safety bump 13 crates ([`c20757a`](https://github.com/hydro-project/hydro/commit/c20757ae0e9e10463b2a499de4b7d37ab02269d0))
</details>

## v0.1.0 (2025-11-25)

### New Features

 - <csr-id-194280baf21d554e2deb3c225ed9fdad197c3db2/> introduce `sliced!` syntax for processing with anonymous ticks

### Bug Fixes

 - <csr-id-bad2f84f0c410cef6186571f5b6223cf3d273fab/> Add CHANGELOG.md

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 3 commits contributed to the release.
 - 2 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 1 unique issue was worked on: [#2256](https://github.com/hydro-project/hydro/issues/2256)

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **[#2256](https://github.com/hydro-project/hydro/issues/2256)**
    - Introduce `sliced!` syntax for processing with anonymous ticks ([`194280b`](https://github.com/hydro-project/hydro/commit/194280baf21d554e2deb3c225ed9fdad197c3db2))
 * **Uncategorized**
    - Release copy_span v0.1.0, hydro_deploy v0.15.0, hydro_lang v0.15.0, hydro_std v0.15.0 ([`bdfd6e0`](https://github.com/hydro-project/hydro/commit/bdfd6e0d10a49f1b6c45f9514982a1c60da80b9f))
    - Add CHANGELOG.md ([`bad2f84`](https://github.com/hydro-project/hydro/commit/bad2f84f0c410cef6186571f5b6223cf3d273fab))
</details>

