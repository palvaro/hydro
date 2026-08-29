# Consensus gauntlet

`consensus_gauntlet` standardizes the repository's consensus comparison into a
single report model. Every complete `run` writes a clean, self-contained HTML
artifact with explanatory text, tables, and inline SVG graphs. Unsupported
combinations are results rather than silently weakened workloads.

## Sequestered qualitative review

The optional reviewer gives each backend a separate, blind, skeptical reading
focused on whether the implementation is easy to read and easy to check as
Hydro code. The current contract is
[`design_docs/2026-08_sequestered_review_rubric.md`](../design_docs/2026-08_sequestered_review_rubric.md).
Older trust-accounting documents are included in the prompt but explicitly
labeled historical context, never current-source evidence.

The gauntlet is provider-independent. `--adapter` is an executable that reads
one JSON request from stdin and writes only the strict JSON response described
inside that request. A fresh adapter process is invoked for each backend; its
request contains one implementation and no measurements, trust ledger, prior
verdict, or competing source.

```bash
cargo run -p consensus_gauntlet -- review \
  --adapter ./review-adapter \
  --model provider/model-revision \
  --backend all \
  --output-dir consensus-gauntlet-reviews

cargo run -p consensus_gauntlet -- report \
  --review consensus-gauntlet-reviews/raft.review.json \
  --review consensus-gauntlet-reviews/quorum-ladder-consensus.review.json \
  --output reviewed-report.html
```

Each pinned artifact records the requested and actual model, source/rubric/
historical-document hashes, and prompt/response hashes. Report loading refuses
artifacts whose inputs no longer match the workspace. Missing reviews remain
explicitly “not reviewed”; model judgment is never represented as proof.

## Commands

```bash
# Mechanical census + capability/not-run table as self-contained HTML
cargo run -p consensus_gauntlet -- report --output report.html

# Run the complete available gauntlet. This checksum-verifies and caches
# Maelstrom v0.2.4 automatically, continues after individual failures, and
# always writes a self-contained HTML artifact.
cargo run -p consensus_gauntlet --all-features -- run \
  --output consensus-gauntlet-report.html

# Focused/debug run while preserving the same HTML contract
cargo run -p consensus_gauntlet --all-features -- run \
  --backend raft --skip-performance --output raft-correctness.html

# Development pilot: four concurrency points, one repetition
cargo run -p consensus_gauntlet --features deploy -- compare \
  --output raft-vs-quorum-ladder.html

# Publication run: same four points, three repetitions
cargo run -p consensus_gauntlet --features deploy -- compare \
  --publication --output raft-vs-quorum-ladder-publication.html

# Classic saturation sweep for other backends
cargo run -p consensus_gauntlet --features deploy -- sweep library-paxos \
  --output library-paxos-curve.json
cargo run -p consensus_gauntlet --features deploy -- sweep compartmentalized-paxos \
  --output compartmentalized-paxos-curve.json

# A single fixed-concurrency point remains available for debugging
cargo run -p consensus_gauntlet --features deploy -- perf raft \
  --concurrency 100 --output raft-point.json

# Assemble a paste-ready HTML report from collected curves
cargo run -p consensus_gauntlet -- report \
  --curve library-paxos-curve.json \
  --curve compartmentalized-paxos-curve.json \
  --output design_docs/reports/consensus_gauntlet.html

# Markdown remains available as an explicit compatibility format
cargo run -p consensus_gauntlet -- report --format markdown

# Run the external lin-kv ladder. MAELSTROM_PATH is optional: without it,
# the checksum-pinned v0.2.4 release is cached automatically.
cargo run -p consensus_gauntlet --features maelstrom -- lin-kv raft
# A single rung is available for focused validation:
cargo run -p consensus_gauntlet --features maelstrom -- \
  lin-kv raft --rung smoke

# Export the same performance graph for ECS. Hydro currently exports the
# manifest; an external orchestrator launches it and captures aggregator stdout.
cargo run -p consensus_gauntlet --features ecs -- \
  export-ecs raft ./ecs-raft

# Render prefixed metric lines collected from ECS/CloudWatch as HTML with the
# same tables and inline SVG charts
cargo run -p consensus_gauntlet -- render-metrics ecs-stdout.log --target ecs
```

ECS export writes `hydro-manifest.json` and `run-spec.json`. The run spec pins
the metric prefix and the exact 3-warmup/12-steady collection contract so future
ECS orchestration does not need a separate statistics/report implementation.

## Backend coverage and explicit failures

The immediate performance portfolio includes Raft, library Paxos,
compartmentalized Paxos, broadcast-transcript consensus, and Quorum-Ladder
Consensus. Paxos-EC and typed-consensus remain in every report even though they
are disabled against the current Hydro API; their build failure is a finding,
and their source still receives the mechanical census.

Library and compartmentalized Paxos use multiple logical clusters, while
Hydro's current Maelstrom deployment supports exactly one. Their lin-kv cells
are explicit capability gaps, but both run through the concurrency performance
sweep. Quorum-Ladder Consensus fixes core links to fail-stop TCP, so its
partition cell is a capability gap; smoke and kill remain runnable.

## Complexity caveat

The S1–S5/LOC/kernel table is intentionally labeled a **mechanical, shallow
source census**. Determination depth is not reported: its semantics are not
settled. Richer trust accounting (antecedent/consequent gap, evidence grades,
blast scope, red coverage, and per-claim bills of materials) remains future
work rather than being replaced by an experimental scalar.
