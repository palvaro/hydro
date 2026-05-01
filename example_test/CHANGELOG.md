

## v0.0.1 (2026-05-01)

### Chore

 - <csr-id-df128eb7b093d3bd275349b83813c83d2d8929a5/> run `compute_pi` test without optimizations
   Reduces re-compilation overhead during CI, also reduces number of digits
   in assertion to make the test a bit faster.

### New Features

 - <csr-id-9b98d4858c8f0a13f19076e87f633f66f7636455/> use `trybuild-internals-api` to forward features and avoid re-compilation

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 2 commits contributed to the release.
 - 274 days passed between releases.
 - 2 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 2 unique issues were worked on: [#2461](https://github.com/hydro-project/hydro/issues/2461), [#2483](https://github.com/hydro-project/hydro/issues/2483)

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **[#2461](https://github.com/hydro-project/hydro/issues/2461)**
    - Run `compute_pi` test without optimizations ([`df128eb`](https://github.com/hydro-project/hydro/commit/df128eb7b093d3bd275349b83813c83d2d8929a5))
 * **[#2483](https://github.com/hydro-project/hydro/issues/2483)**
    - Use `trybuild-internals-api` to forward features and avoid re-compilation ([`9b98d48`](https://github.com/hydro-project/hydro/commit/9b98d4858c8f0a13f19076e87f633f66f7636455))
</details>

## v0.0.0 (2025-07-31)

<csr-id-cb54ace31866d68f424798f876f9e4e8ffd9d881/>
<csr-id-c3ccee6638f2e006f837fd6f946d1b942e40c144/>

### Refactor

 - <csr-id-cb54ace31866d68f424798f876f9e4e8ffd9d881/> move example testing code into separate crate
   To prep for testing of hydro_deploy #1374 #1810

### Test

 - <csr-id-c3ccee6638f2e006f837fd6f946d1b942e40c144/> test some hydro examples on localhost, fix #1374

### Commit Statistics

<csr-read-only-do-not-edit/>

 - 3 commits contributed to the release.
 - 2 commits were understood as [conventional](https://www.conventionalcommits.org).
 - 2 unique issues were worked on: [#1847](https://github.com/hydro-project/hydro/issues/1847), [#1848](https://github.com/hydro-project/hydro/issues/1848)

### Commit Details

<csr-read-only-do-not-edit/>

<details><summary>view details</summary>

 * **[#1847](https://github.com/hydro-project/hydro/issues/1847)**
    - Move example testing code into separate crate ([`cb54ace`](https://github.com/hydro-project/hydro/commit/cb54ace31866d68f424798f876f9e4e8ffd9d881))
 * **[#1848](https://github.com/hydro-project/hydro/issues/1848)**
    - Test some hydro examples on localhost, fix #1374 ([`c3ccee6`](https://github.com/hydro-project/hydro/commit/c3ccee6638f2e006f837fd6f946d1b942e40c144))
 * **Uncategorized**
    - Release example_test v0.0.0, dfir_rs v0.14.0, hydro_deploy v0.14.0, hydro_lang v0.14.0, hydro_optimize v0.13.0, hydro_std v0.14.0 ([`5f69ee0`](https://github.com/hydro-project/hydro/commit/5f69ee080a9e257bc07cdc4deda90ce5525a3d0e))
</details>

