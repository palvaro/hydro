# Consensus Gauntlet Validation Record

This file records durable conclusions and reproducible checks without retaining generated HTML, raw curves, or complete run logs.

## Historical execution evidence

The final corrected run from August 2026 used a checksum-pinned, source-built Maelstrom 0.2.4 revision with the single-process kill changes described by the installer. Its manifest and report recorded an exit status of zero, 18 successful Knossos analyses, successful Quorum-Ladder smoke and kill checks, and an explicit partition capability gap. A separate three-repetition localhost comparison exercised Raft and Quorum-Ladder Consensus. During the September recovery review, every checksum in both final manifests validated.

These observations establish that the gauntlet ran successfully in that repository state. They do not establish that every backend passed: Paxos-EC and typed consensus were reported as build failures, some topology and partition combinations were explicit capability gaps, and two earlier runs were retracted before the final corrected run.

The old generated reports, raw curves, and logs are not source inputs and are intentionally excluded from normal repository history. The rescue bundle created during the recovery review retains them if forensic inspection is ever necessary.

## Current source validation

The following checks were run against the recovered source in September 2026:

```bash
RUSTFLAGS='-C linker=/usr/bin/clang' cargo test -p consensus_gauntlet --lib
RUSTFLAGS='-C linker=/usr/bin/clang' cargo run -p consensus_gauntlet -- \
  report --output "$TMPDIR/consensus-gauntlet-current-report.html"
RUSTFLAGS='-C linker=/usr/bin/clang' cargo test -p consensus_gauntlet \
  --features deploy --test compare_smoke
```

The library suite passed all 39 tests. The report command produced a 42,367-byte self-contained HTML report containing all seven registered backends and their explicit capability gaps.

The deploy smoke test initially exposed two stale build seams: the runner used the later `quorum_ladder_bench` name while the retained protocol exports `multi_paxos_bench`, and telemetry did not enable DFIR's `meta` feature for deployment sub-builds. After those seams were repaired, the smoke test compiled its staged deployment binaries and launched the Raft topology. It then remained at zero completed requests and was stopped after more than one minute. Therefore the current source has verified library and report behavior, plus successful deployment compilation and launch, but it does not have a current successful end-to-end localhost comparison. The stalled workload remains a liveness issue to investigate rather than a passing result.
