# Sequestered implementation-review rubric

2026-08

**Status:** current evaluation contract for `consensus_gauntlet` qualitative reviews.

## Purpose

The gauntlet asks an independent language-model reviewer to read each consensus implementation in isolation. The review complements external correctness tests, performance measurements, and mechanical source accounting; it must not replace or reinterpret those results.

The review has two headline questions:

1. **Is the implementation easy to read?** Can a Hydro-literate engineer recover the protocol phases, state, message flow, failure assumptions, and output guarantee without reconstructing them from scattered incidental details?
2. **Is the implementation easy to check?** Are the implementation's guarantees and load-bearing assumptions local, explicit, cited, and amenable to mechanical challenge, or must an auditor accept whole-protocol reasoning, prose contracts, hidden conventions, or obligations forwarded elsewhere?

These axes are related but distinct. Familiar-looking or concise code is not necessarily checkable. Verbose code may still be easy to check if it names obligations precisely and makes evidence local.

## Reviewer stance

The reviewer must be:

- **Independent and blind:** each invocation receives exactly one implementation source. It receives no benchmark results, census counts, generated trust ledger, prior model verdict, or other implementation source. It must not rank the implementation against unseen alternatives.
- **Skeptical:** a comment, type name, or assertion is a claim to inspect, not proof. The reviewer distinguishes guarantees earned by visible code and types from guarantees delegated to callers, runtime behavior, framework machinery, or prose.
- **Hydro-idiomatic:** the reviewer evaluates the implementation as Hydro code rather than generic Rust. It considers tick/stratum boundaries, location and cluster boundaries, `sliced!` stateful kernels, feedback, consistency labels, explicit nondeterminism, assumptions, proof tokens, and whether the implementation works with or around Hydro's type discipline.
- **Evidence-bound:** every substantive judgment cites current implementation line ranges. Unsupported impressions are reported as uncertainty, not fact.
- **Sober:** the reviewer avoids advocacy, novelty claims, and false precision. It identifies both strengths and concerns and explains what additional evidence would change an uncertain verdict.

## Current research goals

The project currently seeks consensus implementations whose distributed guarantees are understandable and auditable in the program itself. In particular:

- consistency guarantees should be inferred or derived by typed composition where possible, rather than introduced by broad unchecked assertions;
- irreducible human obligations should be localized into named, greppable seams with narrow antecedents;
- nondeterministic choices should be explicit, and the code should make clear whether each choice is mechanically erased, intentionally observable, or still an unaccounted escape;
- caller contracts, convention seals, trusted imports, timer/failure assumptions, and state-bounding assumptions should be visible rather than silently excluded from the apparent proof surface;
- decomposition should improve local reasoning without hiding cross-location protocol obligations or merely moving them into adapters;
- evidence should challenge load-bearing assumptions (including red tests or type refusals), not merely demonstrate happy-path execution.

A review assesses alignment with these goals; it does not presume that the goals have already been achieved.

## Structured verdict

For each implementation, the reviewer returns:

- a readability verdict and cited rationale;
- a checkability verdict and cited rationale;
- the important guarantees the code appears to claim, whether each is visibly earned, partly earned, not earned, or unclear, and the evidence for that judgment;
- Hydro-idiom strengths and concerns;
- obligations that appear forwarded, hidden, convention-dependent, or otherwise outside the local source;
- a research-alignment verdict and concrete next checks that would most reduce uncertainty.

The allowed ease ratings are `easy`, `mostly-easy`, `mixed`, `difficult`, and `very-difficult`. A rating is an ordinal summary for scanning, not a scientific score.

## Historical context

`2026-08_trust_and_complexity_accounting.md` and `2026-08_nondet_vs_manual_proof.md` are supplied to the reviewer as **old research history**. They provide vocabulary, earlier hypotheses, and examples of the questions that motivated this rubric. They are not current measurements or authoritative verdicts. Their claims about any implementation may be stale, and the reviewer must not cite them as evidence about current source. Only the current implementation may support the verdict.
