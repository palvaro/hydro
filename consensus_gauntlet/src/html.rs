//! Self-contained HTML rendering for consensus gauntlet reports.
//!
//! Reports intentionally use no external fonts, scripts, stylesheets, or chart
//! libraries. They can be archived, emailed, or opened directly from disk.

use std::fmt::Write;

use crate::backend::{
    BuildStatus, Checkpointing, ConsistencyOutput, SupportStatus, TimerInput, Topology,
};
use crate::perf::WindowMetrics;
use crate::report::{GauntletReport, Outcome, Status};
use crate::trust::{
    AntecedentScope, Assessed, AssessmentProvenance, EvidenceGrade, NondetClass, SeamKind,
    TrustLedger,
};

/// Render a complete, self-contained HTML report.
pub fn render_html(report: &GauntletReport) -> String {
    let mut out = String::with_capacity(48_000);
    out.push_str("<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">");
    out.push_str("<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">");
    out.push_str("<title>Consensus gauntlet report</title><style>");
    out.push_str(CSS);
    out.push_str("</style></head><body><main>");
    out.push_str("<header class=\"hero\"><div><p class=\"eyebrow\">HYDRO · CONSENSUS GAUNTLET</p><h1>Consensus gauntlet report</h1><p class=\"lede\">One reproducible profile of external correctness, closed-loop performance, implementation source measurements, and cited manual claim review. Capability gaps are reported—not smoothed over.</p></div>");
    out.push_str("<dl class=\"metadata\">");
    metadata(&mut out, "Date", &report.environment.date);
    metadata(&mut out, "Commit", &report.environment.commit);
    metadata(&mut out, "Host", &report.environment.host);
    metadata(
        &mut out,
        "Target",
        &report.environment.execution.target.to_string(),
    );
    if let Some(region) = &report.environment.execution.region {
        metadata(&mut out, "Region", region);
    }
    if let Some(cluster) = &report.environment.execution.ecs_cluster {
        metadata(&mut out, "ECS cluster", cluster);
    }
    out.push_str("</dl></header>");

    section_start(
        &mut out,
        "capabilities",
        "Backend capabilities",
        "Protocol differences are first-class report data. A capability cell describes what the adapter can actually run; a gap is not treated as a passing result.",
    );
    out.push_str("<div class=\"table-wrap\"><table><thead><tr><th>Backend</th><th>Build</th><th>Topology</th><th>Maelstrom</th><th>Performance</th><th>Partition nemesis</th><th>Timer inputs</th><th>Checkpointing</th><th>Consistency output</th></tr></thead><tbody>");
    for row in &report.backends {
        let cap = row.backend.capabilities();
        write!(
            out,
              "<tr><th scope=\"row\"><code>{}</code></th><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
            escape(&row.backend.to_string()),
            build_status(cap.build),
            topology(cap.topology),
            support(cap.maelstrom),
            support(cap.performance),
            if cap.supports_partition_nemesis { "yes" } else { "no" },
            cap.timers.iter().map(timer).collect::<Vec<_>>().join(", "),
            checkpoint(cap.checkpointing),
              consistency(cap.consistency_output),
        )
        .unwrap();
    }
    out.push_str("</tbody></table></div></section>");

    section_start(
        &mut out,
        "correctness",
        "Tier 1 · Maelstrom lin-kv",
        "Maelstrom drives the same linearizable key-value workload through every supported backend and validates histories with its external checker. Smoke establishes basic wiring; kill and partition rungs exercise fault recovery.",
    );
    out.push_str("<div class=\"table-wrap\"><table><thead><tr><th>Backend</th><th>Smoke <small>20 s</small></th><th>Kill <small>60 s × 3</small></th><th>Partition <small>45 s × 3</small></th></tr></thead><tbody>");
    for row in &report.backends {
        write!(
            out,
            "<tr><th scope=\"row\"><code>{}</code></th><td>{}</td><td>{}</td><td>{}</td></tr>",
            escape(&row.backend.to_string()),
            status(&row.lin_kv.smoke),
            status(&row.lin_kv.kill),
            status(&row.lin_kv.partition),
        )
        .unwrap();
    }
    out.push_str("</tbody></table></div></section>");

    section_start(
        &mut out,
        "performance",
        "Tier 2 · Performance",
        "The bench_client workload is closed-loop and leader-pinned. Each plotted point is a fresh deployment at a controlled total concurrency. Three repetitions summarize twelve steady one-second windows after three warmup windows; raw repetitions remain embedded for audit.",
    );
    out.push_str("<div class=\"table-wrap\"><table><thead><tr><th>Backend</th><th>Status</th><th class=\"num\">Peak median req/s</th><th class=\"num\">Concurrency at peak</th><th class=\"num\">p50 at peak</th><th class=\"num\">p99 at peak</th><th>Knee</th></tr></thead><tbody>");
    for row in &report.backends {
        write!(
            out,
            "<tr><th scope=\"row\"><code>{}</code></th><td>{}</td>",
            escape(&row.backend.to_string()),
            status(&row.perf_status),
        )
        .unwrap();
        if let Some(curve) = report
            .saturation_curves
            .iter()
            .find(|curve| curve.backend == row.backend.as_str())
        {
            let peak = curve
                .points
                .iter()
                .max_by(|left, right| {
                    left.throughput_rps
                        .median
                        .total_cmp(&right.throughput_rps.median)
                })
                .expect("validated curve is non-empty");
            write!(
                out,
                "<td class=\"num\"><strong>{:.0}</strong></td><td class=\"num\">{}</td><td class=\"num\">{:.3} ms</td><td class=\"num\">{:.3} ms</td><td>{}</td>",
                peak.throughput_rps.median,
                peak.requested_concurrency,
                peak.p50_ms.median,
                peak.p99_ms.median,
                curve
                    .knee
                    .as_ref()
                    .map(|knee| knee.requested_concurrency.to_string())
                    .unwrap_or_else(|| "not detected".to_owned()),
            )
            .unwrap();
        } else {
            out.push_str("<td class=\"num muted\">—</td><td class=\"num muted\">—</td><td class=\"num muted\">—</td><td class=\"num muted\">—</td><td class=\"muted\">—</td>");
        }
        out.push_str("</tr>");
    }
    out.push_str("</tbody></table></div>");
    for curve in &report.saturation_curves {
        saturation_card(&mut out, curve);
    }
    if report.saturation_curves.is_empty() {
        out.push_str("<div class=\"empty\"><strong>No saturation curves attached.</strong><span>Performance cells show point-run status only; run the concurrency sweep to locate each backend's throughput/latency knee.</span></div>");
    }
    out.push_str("</section>");

    section_start(
        &mut out,
        "census",
        "Tier 3 · Source measurements",
        "These are literal measurements of the implementation source, not correctness scores. The measured body is every physical source line before the first #[cfg(test)] marker; the marker and subsequent test code are excluded. Comments and string literals are removed before code sites are counted.",
    );
    out.push_str("<div class=\"metric-guide\"><h3>What each column means</h3><dl><div><dt>Body LOC</dt><dd>Physical source lines in the measured body, including blank lines and comments. This is a file-size measure, not logical statements.</dd></div><div><dt>Consistency assertions</dt><dd>Calls to <code>assert_has_consistency_of</code>. Each call explicitly asks Hydro to accept a consistency claim supplied by the implementation. The count says where such claims enter; it does not say whether they are true.</dd></div><div><dt>Manual algebra proofs</dt><dd><code>manual_proof!</code> calls not used by a consistency assertion. They tell an operator that a merge is commutative or idempotent. The count identifies manual proof sites; it does not measure proof difficulty.</dd></div><div><dt>Ordering assumptions</dt><dd>Calls to <code>assume_ordering</code> or <code>assume_retries</code>. At these sites the program explicitly chooses stronger ordering or retry semantics than the input type established.</dd></div><div><dt>Nondeterministic choices</dt><dd>Actual <code>nondet!(...)</code> invocations. These name choices such as arrival order, batching, snapshots, or timer timing. Different choices have different risk; the count alone does not rank them.</dd></div><div><dt>Forwarded choice parameters</dt><dd>Function parameters whose type spelling is <code>NonDet</code>. The callee requires its caller to provide and document a nondeterministic choice rather than creating it locally.</dd></div><div><dt><code>sliced!</code> blocks / total LOC / largest LOC / body share</dt><dd>An actual <code>sliced! { ... }</code> block contains tick-separated, order-sensitive stateful dataflow. Total LOC sums each block's inclusive opening-to-closing physical line span; largest LOC is the largest one; body share is total <code>sliced!</code> LOC divided by Body LOC. These are source-size proxies only: they are not IR operator counts, runtime cost, or proof difficulty. Nested blocks would be counted in both spans; none of the measured portfolio sources nest them.</dd></div><div><dt>Atomic boundaries</dt><dd>Calls to <code>end_atomic</code>, which explicitly end an atomic dataflow region. This counts sites, not waiting time or runtime coordination cost.</dd></div><div><dt>Feedback edges</dt><dd>Uses of <code>forward_ref</code>, Hydro's API for wiring dataflow feedback. This counts feedback construction sites, not graph-theoretic cycles.</dd></div></dl></div>");
    out.push_str("<div class=\"table-wrap\"><table><thead><tr><th>Backend</th><th>Status</th><th class=\"num\">Body LOC</th><th class=\"num\">Consistency assertions</th><th class=\"num\">Manual algebra proofs</th><th class=\"num\">Ordering assumptions</th><th class=\"num\">Nondeterministic choices</th><th class=\"num\">Forwarded choice parameters</th><th class=\"num\"><code>sliced!</code> blocks</th><th class=\"num\"><code>sliced!</code> total LOC</th><th class=\"num\">Largest <code>sliced!</code> LOC</th><th class=\"num\"><code>sliced!</code> body share</th><th class=\"num\">Atomic boundaries</th><th class=\"num\">Feedback edges</th></tr></thead><tbody>");
    for row in &report.backends {
        write!(
            out,
            "<tr><th scope=\"row\"><code>{}</code></th><td>{}</td>",
            escape(&row.backend.to_string()),
            status(&row.census_status),
        )
        .unwrap();
        if let Some(c) = &row.census {
            write!(out, "<td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{}</td><td class=\"num\">{:.1}%</td><td class=\"num\">{}</td><td class=\"num\">{}</td>", c.body_loc, c.consistency_mints, c.algebra_proofs, c.assumers, c.introducer_nondets, c.forwarded_nondet_params, c.kernels, c.kernel_total_loc, c.kernel_largest_loc, c.kernel_body_percent(), c.cuts, c.cycles).unwrap();
        } else {
            for _ in 0..12 {
                out.push_str("<td class=\"num muted\">—</td>");
            }
        }
        out.push_str("</tr>");
    }
    out.push_str("</tbody></table></div><aside class=\"note\"><strong>Do not rank implementations by these counts.</strong> They answer only how much source was measured and where specific Hydro APIs occur. A low count can mean less manual machinery, an unlabelled output, or simply a different implementation style. Correctness requires the external tests and cited evidence below.</aside>");

    out.push_str("<div class=\"section-head deep\"><div><p class=\"eyebrow\">CITED CLAIM REVIEW</p><h3>Manual evidence records</h3></div><p>This section is separate from the automatic source counts. A record is shown only when a reviewer entered a concrete claim and source citation. A dash means that review has not been performed; it never means zero risk.</p></div>");
    out.push_str("<div class=\"metric-guide\"><h3>How to read the summary</h3><dl><div><dt>Claims without a local consistency assertion</dt><dd>Among entered typed claims, the percentage whose listed dependencies contain no local <code>assert_has_consistency_of</code> site. This does not mean the compiler proved the protocol; imported trusted code and caller contracts can still carry the claim.</dd></div><div><dt>Largest review scope</dt><dd>The broadest source scope needed to check an entered consistency assertion or manual algebra proof: one closure, one combinator, one phase, several communicating locations, or code plus a caller promise.</dd></div><div><dt>Manually trusted source share</dt><dd>The intended percentage of graph operators that must be accepted from prose-backed reasoning after deduplicating overlap. It is reported only when an IR graph measurement exists; otherwise it is missing.</dd></div><div><dt>Mechanically challenged contracts</dt><dd>The percentage of entered caller/convention contracts tested by a deliberately violating test, an adversarial checker, or a compile-time refusal. A green test alone does not qualify.</dd></div><div><dt>Failure-witness coverage</dt><dd>The percentage of all entered claim sites for which negating the claim caused a test failure or compiler rejection. It measures whether the test can notice a broken assumption, not whether the assumption is universally true.</dd></div><div><dt>Choice-resolution classes</dt><dd>Counts of entered nondeterministic-choice sites in four explicit buckets: mechanically shown irrelevant; claimed irrelevant only in prose; intentionally allowed to affect a named output property; or not yet accounted for. Missing classification stays missing.</dd></div></dl></div>");
    out.push_str("<div class=\"table-wrap\"><table><thead><tr><th>Backend</th><th>Claims without local consistency assertion</th><th>Largest review scope</th><th>Manually trusted source share</th><th>Mechanically challenged contracts</th><th>Failure-witness coverage</th><th>Choice resolution: mechanical / prose / named / unaccounted</th></tr></thead><tbody>");
    for ledger in &report.trust_ledgers {
        if !report
            .backends
            .iter()
            .any(|row| row.backend == ledger.backend)
        {
            continue;
        }
        let summary = ledger.summary();
        write!(out, "<tr><th scope=\"row\"><code>{}</code></th><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>", escape(ledger.backend.as_str()), assessed_ratio(&summary.inference_ratio), assessed_scope(&summary.worst_gap), assessed_ratio(&summary.trusted_surface_ratio), assessed_ratio(&summary.contract_e2_ratio), assessed_ratio(&summary.red_coverage), assessed_nondet(&summary.nondet_counts)).unwrap();
    }
    out.push_str("</tbody></table></div>");
    for ledger in &report.trust_ledgers {
        if !report
            .backends
            .iter()
            .any(|row| row.backend == ledger.backend)
        {
            continue;
        }
        trust_ledger(&mut out, ledger);
    }
    out.push_str("</section>");

    section_start(
        &mut out,
        "sequestered-review",
        "Independent implementation review",
        "Each verdict is a blind, skeptical read of one implementation as Hydro code. The reviewer sees no measurements, generated trust ledger, previous verdict, or competing source. Historical notes are supplied only as explicitly old context. Missing means not reviewed.",
    );
    out.push_str("<div class=\"table-wrap\"><table><thead><tr><th>Backend</th><th>Readability</th><th>Checkability</th><th>Research alignment</th><th>Model</th></tr></thead><tbody>");
    for backend in crate::backend::BackendId::ALL {
        if let Some(review) = report
            .reviews
            .iter()
            .find(|review| review.backend == backend.as_str())
        {
            write!(
                out,
                "<tr><th scope=\"row\"><code>{}</code></th><td>{}</td><td>{}</td><td>{}</td><td><code>{}</code></td></tr>",
                escape(backend.as_str()),
                escape(review.verdict.readability.rating.label()),
                escape(review.verdict.checkability.rating.label()),
                escape(review.verdict.research_alignment.alignment.label()),
                escape(&review.actual_model),
            )
            .unwrap();
        } else {
            write!(out, "<tr><th scope=\"row\"><code>{}</code></th><td class=\"muted\">not reviewed</td><td class=\"muted\">not reviewed</td><td class=\"muted\">not reviewed</td><td class=\"muted\">—</td></tr>", escape(backend.as_str())).unwrap();
        }
    }
    out.push_str("</tbody></table></div>");
    for review in &report.reviews {
        qualitative_review(&mut out, review);
    }
    out.push_str("<aside class=\"note\"><strong>Model judgment is not proof.</strong> The verdict is retained with source, rubric, history, prompt, and response hashes so drift is visible. Only citations to the current implementation count as evidence.</aside></section>");
    out.push_str("<footer>Generated by <code>consensus_gauntlet</code> · self-contained artifact · no external assets</footer></main></body></html>");
    out
}

fn qualitative_review(out: &mut String, review: &crate::review::ReviewArtifact) {
    let verdict = &review.verdict;
    write!(out, "<details class=\"ledger\"><summary><strong><code>{}</code></strong><span>{} readability · {} checkability</span></summary><div class=\"ledger-body\">", escape(&review.backend), escape(verdict.readability.rating.label()), escape(verdict.checkability.rating.label())).unwrap();
    out.push_str("<div class=\"metric-guide\"><h3>Headline verdict</h3><dl>");
    review_axis(
        out,
        "Readability",
        verdict.readability.rating.label(),
        &verdict.readability.rationale,
        &review.source.path,
        &verdict.readability.citations,
    );
    review_axis(
        out,
        "Checkability",
        verdict.checkability.rating.label(),
        &verdict.checkability.rationale,
        &review.source.path,
        &verdict.checkability.citations,
    );
    review_axis(
        out,
        "Research alignment",
        verdict.research_alignment.alignment.label(),
        &verdict.research_alignment.rationale,
        &review.source.path,
        &verdict.research_alignment.citations,
    );
    out.push_str("</dl></div>");
    if !verdict.guarantees.is_empty() {
        out.push_str("<h4>Claimed guarantees</h4><div class=\"table-wrap compact\"><table><thead><tr><th>Guarantee</th><th>Verdict</th><th>Rationale</th><th>Current-source evidence</th></tr></thead><tbody>");
        for guarantee in &verdict.guarantees {
            write!(
                out,
                "<tr><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>",
                escape(&guarantee.guarantee),
                escape(guarantee.status.label()),
                escape(&guarantee.rationale),
                html_citations(&review.source.path, &guarantee.citations)
            )
            .unwrap();
        }
        out.push_str("</tbody></table></div>");
    }
    for (heading, observations) in [
        ("Hydro-idiom strengths", &verdict.hydro_idiom_strengths),
        ("Hydro-idiom concerns", &verdict.hydro_idiom_concerns),
        (
            "External or hidden obligations",
            &verdict.external_or_hidden_obligations,
        ),
    ] {
        if observations.is_empty() {
            continue;
        }
        write!(out, "<h4>{}</h4><ul>", escape(heading)).unwrap();
        for observation in observations {
            write!(
                out,
                "<li>{} <small>{}</small></li>",
                escape(&observation.observation),
                html_citations(&review.source.path, &observation.citations)
            )
            .unwrap();
        }
        out.push_str("</ul>");
    }
    if !verdict.next_checks.is_empty() {
        out.push_str("<h4>Next checks</h4><ul>");
        for check in &verdict.next_checks {
            write!(out, "<li>{}</li>", escape(check)).unwrap();
        }
        out.push_str("</ul>");
    }
    write!(out, "<p class=\"muted\"><small>Requested model <code>{}</code>; actual model <code>{}</code>; prompt SHA-256 <code>{}</code>; response SHA-256 <code>{}</code>.</small></p></div></details>", escape(&review.requested_model), escape(&review.actual_model), escape(&review.prompt_sha256), escape(&review.response_sha256)).unwrap();
}

fn review_axis(
    out: &mut String,
    heading: &str,
    rating: &str,
    rationale: &str,
    path: &str,
    citations: &[crate::review::SourceCitation],
) {
    write!(
        out,
        "<div><dt>{}: {}</dt><dd>{} <small>{}</small></dd></div>",
        escape(heading),
        escape(rating),
        escape(rationale),
        html_citations(path, citations)
    )
    .unwrap();
}

fn html_citations(path: &str, citations: &[crate::review::SourceCitation]) -> String {
    citations
        .iter()
        .map(|citation| {
            format!(
                "<code>{}:{}-{}</code> ({})",
                escape(path),
                citation.start_line,
                citation.end_line,
                escape(&citation.note)
            )
        })
        .collect::<Vec<_>>()
        .join("; ")
}

fn metadata(out: &mut String, term: &str, value: &str) {
    write!(
        out,
        "<div><dt>{}</dt><dd>{}</dd></div>",
        escape(term),
        escape(value)
    )
    .unwrap();
}

fn section_start(out: &mut String, id: &str, heading: &str, prose: &str) {
    write!(out, "<section id=\"{}\"><div class=\"section-head\"><div><p class=\"eyebrow\">{}</p><h2>{}</h2></div><p>{}</p></div>", escape(id), escape(id), escape(heading), escape(prose)).unwrap();
}

fn status(value: &Status) -> String {
    let (class, label) = match value.outcome {
        Outcome::Passed => ("pass", "Pass"),
        Outcome::Failed => ("fail", "Failed"),
        Outcome::Skipped => ("skip", "Skipped"),
        Outcome::CapabilityGap => ("gap", "Capability gap"),
        Outcome::NotRun => ("idle", "Not run"),
    };
    if value.detail.is_empty() {
        format!("<span class=\"badge {class}\">{label}</span>")
    } else {
        format!(
            "<span class=\"badge {class}\">{label}</span><span class=\"detail\">{}</span>",
            escape(&value.detail)
        )
    }
}

fn build_status(value: BuildStatus) -> String {
    match value {
        BuildStatus::Builds => "<span class=\"badge pass\">Builds</span>".to_owned(),
        BuildStatus::Broken(reason) => format!(
            "<span class=\"badge fail\">Broken</span><span class=\"detail\">{}</span>",
            escape(reason)
        ),
    }
}

fn topology(value: Topology) -> &'static str {
    match value {
        Topology::Colocated => "colocated",
        Topology::ProposersAcceptorsReplicas => "proposers + acceptors + replicas",
        Topology::Compartmentalized => "proposers + proxies + acceptor grid + replicas",
    }
}

fn support(value: SupportStatus) -> String {
    match value {
        SupportStatus::Supported => "<span class=\"badge pass\">Supported</span>".to_owned(),
        SupportStatus::Partial(reason) => format!(
            "<span class=\"badge skip\">Partial</span><span class=\"detail\">{}</span>",
            escape(reason)
        ),
        SupportStatus::Gap(reason) => format!(
            "<span class=\"badge gap\">Gap</span><span class=\"detail\">{}</span>",
            escape(reason)
        ),
    }
}

fn assessed_ratio(value: &Assessed<f64>) -> String {
    assessed(value, |ratio| format!("{:.0}%", ratio * 100.0))
}

fn assessed_scope(value: &Assessed<AntecedentScope>) -> String {
    assessed(value, |scope| antecedent_scope(*scope).to_owned())
}

fn antecedent_scope(value: AntecedentScope) -> &'static str {
    match value {
        AntecedentScope::A0Closure => "one closure",
        AntecedentScope::A1Combinator => "one combinator or sliced block",
        AntecedentScope::A2Phase => "one protocol phase",
        AntecedentScope::A3CrossLocation => "multiple communicating locations",
        AntecedentScope::A4ContractDependent => "source code plus a caller promise",
    }
}

fn seam_kind(value: SeamKind) -> &'static str {
    match value {
        SeamKind::S1ConsistencyMint => "explicit consistency assertion",
        SeamKind::S2AlgebraProof => "manual commutativity/idempotence proof",
        SeamKind::S3Assumer => "explicit ordering/retry assumption",
        SeamKind::S4Introducer => "locally introduced nondeterministic choice",
        SeamKind::S5ForwardedObligation => "caller-supplied nondeterministic choice",
        SeamKind::S6CallerContract => "caller contract",
        SeamKind::S7ConventionSeal => "convention relied on for authenticity",
        SeamKind::S8TrustedBaseImport => "claim imported from trusted library code",
    }
}

fn nondet_class(value: NondetClass) -> &'static str {
    match value {
        NondetClass::N0MechanicallyErased => "mechanically shown not to affect output",
        NondetClass::N1ProseErased => "claimed in prose not to affect output",
        NondetClass::N2NamedFreedom => "allowed to affect a named output property",
        NondetClass::N3UnaccountedEscape => "effect on output is not accounted for",
    }
}

fn evidence_grade(value: EvidenceGrade) -> &'static str {
    match value {
        EvidenceGrade::E0Prose => "prose only; no mechanical check",
        EvidenceGrade::E1Exhaustive => "green exhaustive test over a bounded model",
        EvidenceGrade::E1Fuzz => "green fuzz or randomized test",
        EvidenceGrade::E2RedTested => "deliberate violation produced a failing test",
        EvidenceGrade::E3Adversarial => "external adversarial history checker",
        EvidenceGrade::E4TypeRefused => "invalid construction rejected at compile time",
    }
}

fn assessed_nondet(value: &Assessed<[usize; 4]>) -> String {
    assessed(value, |counts| {
        format!("{}/{}/{}/{}", counts[0], counts[1], counts[2], counts[3])
    })
}

fn assessed<T>(value: &Assessed<T>, known: impl FnOnce(&T) -> String) -> String {
    match value {
        Assessed::Known {
            value,
            provenance,
            citations,
        } => format!(
            "{}<span class=\"detail\">{} · {} citation(s)</span>",
            escape(&known(value)),
            match provenance {
                AssessmentProvenance::Mechanical => "derived",
                AssessmentProvenance::Manual => "manual",
            },
            citations.len()
        ),
        Assessed::Missing { reason } => format!(
            "<span class=\"muted\">—</span><span class=\"detail\">missing: {}</span>",
            escape(reason)
        ),
    }
}

fn trust_ledger(out: &mut String, ledger: &TrustLedger) {
    write!(
        out,
          "<details class=\"trust-ledger\"><summary><code>{}</code> manual evidence — {} reviewed site(s), {} correctness claim(s)</summary>",
        escape(ledger.backend.as_str()),
        ledger.seams.len(),
        ledger.claims.len()
    )
    .unwrap();
    for note in &ledger.notes {
        write!(out, "<p class=\"detail\">{}</p>", escape(note)).unwrap();
    }
    if ledger.seams.is_empty() {
        out.push_str("<div class=\"empty\"><strong>Manual claim review missing.</strong><span>No unreviewed claim site is counted as safe or absent.</span></div>");
    } else {
        out.push_str("<div class=\"table-wrap compact\"><table><thead><tr><th>ID</th><th>Site type</th><th>Claim being relied on</th><th>Review scope</th><th>Claims broken if false</th><th>Strongest evidence</th><th>Choice resolution</th><th>Source citations</th></tr></thead><tbody>");
        for seam in &ledger.seams {
            write!(out, "<tr><th scope=\"row\"><code>{}</code></th><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td><td>{}</td></tr>", escape(seam.id), seam_kind(seam.kind), escape(seam.proposition), assessed(&seam.antecedent, |scope| antecedent_scope(*scope).to_owned()), assessed(&seam.blast_scope, |claims| claims.iter().map(|claim| escape(claim)).collect::<Vec<_>>().join(", ")), assessed(&seam.evidence, |grade| evidence_grade(*grade).to_owned()), assessed(&seam.nondeterminism, |class| class.map(nondet_class).unwrap_or("not applicable").to_owned()), citations(&seam.citations)).unwrap();
        }
        out.push_str("</tbody></table></div>");
    }
    if !ledger.claims.is_empty() {
        out.push_str("<h4>Claim dependency lists</h4><div class=\"table-wrap compact\"><table><thead><tr><th>Claim</th><th>Represented in a consistency type</th><th>Relied-on evidence records</th><th>Source citations</th></tr></thead><tbody>");
        for claim in &ledger.claims {
            write!(
                out,
                "<tr><th scope=\"row\">{}</th><td>{}</td><td>{}</td><td>{}</td></tr>",
                escape(claim.claim),
                if claim.typed { "yes" } else { "no" },
                assessed(&claim.seam_ids, |ids| ids
                    .iter()
                    .map(|id| format!("<code>{}</code>", escape(id)))
                    .collect::<Vec<_>>()
                    .join(", ")),
                citations(&claim.citations)
            )
            .unwrap();
        }
        out.push_str("</tbody></table></div>");
    }
    out.push_str("</details>");
}

fn citations(values: &[crate::trust::Citation]) -> String {
    if values.is_empty() {
        return "<span class=\"muted\">—</span>".to_owned();
    }
    values
        .iter()
        .map(|cite| {
            format!(
                "<code>{}{}</code><span class=\"detail\">{}</span>",
                escape(cite.path),
                cite.line.map(|line| format!(":{line}")).unwrap_or_default(),
                escape(cite.note)
            )
        })
        .collect::<Vec<_>>()
        .join("<br>")
}

fn timer(value: &TimerInput) -> &'static str {
    match value {
        TimerInput::Election => "election",
        TimerInput::Heartbeat => "heartbeat",
    }
}
fn checkpoint(value: Checkpointing) -> &'static str {
    match value {
        Checkpointing::Unsupported => "unsupported",
        Checkpointing::External => "external",
        Checkpointing::Internal => "internal",
    }
}
fn consistency(value: ConsistencyOutput) -> &'static str {
    match value {
        ConsistencyOutput::Asserted => "asserted",
        ConsistencyOutput::Inferred => "inferred",
        ConsistencyOutput::Unlabeled => "unlabeled",
    }
}

fn saturation_card(out: &mut String, curve: &crate::perf::SaturationCurve) {
    write!(out, "<article class=\"backend-card\"><div class=\"card-title\"><div><p class=\"eyebrow\">CONCURRENCY SATURATION</p><h3>{}</h3></div></div><div class=\"charts\">", escape(&curve.backend)).unwrap();
    out.push_str(&saturation_chart(curve, false));
    out.push_str(&saturation_chart(curve, true));
    out.push_str("</div><div class=\"table-wrap compact\"><table><thead><tr><th>Concurrency</th><th class=\"num\">Throughput req/s</th><th class=\"num\">p50 ms</th><th class=\"num\">p99 ms</th><th class=\"num\">p99.9 ms</th><th class=\"num\">Repetitions</th></tr></thead><tbody>");
    for point in &curve.points {
        write!(out, "<tr><th scope=\"row\">{}</th><td class=\"num\">{:.0}</td><td class=\"num\">{:.3}</td><td class=\"num\">{:.3}</td><td class=\"num\">{:.3}</td><td class=\"num\">{}</td></tr>", point.requested_concurrency, point.throughput_rps.median, point.p50_ms.median, point.p99_ms.median, point.p999_ms.median, point.repetitions.len()).unwrap();
    }
    out.push_str("</tbody></table></div>");
    if let Some(knee) = &curve.knee {
        write!(out, "<aside class=\"note\"><strong>Observed saturation near concurrency {}.</strong> {}</aside>", knee.requested_concurrency, escape(&knee.detail)).unwrap();
    }
    out.push_str("</article>");
}

fn saturation_chart(curve: &crate::perf::SaturationCurve, latency: bool) -> String {
    let windows: Vec<WindowMetrics> = curve
        .points
        .iter()
        .map(|point| WindowMetrics {
            sequence: point.requested_concurrency as u64,
            throughput_rps: point.throughput_rps.median,
            p50_ms: point.p50_ms.median,
            p99_ms: point.p99_ms.median,
            p999_ms: point.p999_ms.median,
            samples: 0,
        })
        .collect();
    if latency {
        chart(
            &format!("{} latency by concurrency", curve.backend),
            "Latency · ms (x: concurrency)",
            &windows,
            0,
            &[
                ("p50", "#0f766e", Box::new(|w: &WindowMetrics| w.p50_ms)),
                ("p99", "#d97706", Box::new(|w: &WindowMetrics| w.p99_ms)),
                ("p99.9", "#dc2626", Box::new(|w: &WindowMetrics| w.p999_ms)),
            ],
        )
    } else {
        chart(
            &format!("{} throughput by concurrency", curve.backend),
            "Throughput · req/s (x: concurrency)",
            &windows,
            0,
            &[(
                "throughput",
                "#2563eb",
                Box::new(|w: &WindowMetrics| w.throughput_rps),
            )],
        )
    }
}

type ValueFn = Box<dyn Fn(&WindowMetrics) -> f64>;
fn chart(
    title: &str,
    y_label: &str,
    windows: &[WindowMetrics],
    warmup: usize,
    series: &[(&str, &str, ValueFn)],
) -> String {
    const W: f64 = 680.0;
    const H: f64 = 300.0;
    const L: f64 = 62.0;
    const R: f64 = 18.0;
    const T: f64 = 22.0;
    const B: f64 = 42.0;
    let pw = W - L - R;
    let ph = H - T - B;
    let max = series
        .iter()
        .flat_map(|(_, _, f)| windows.iter().map(f))
        .fold(0.0_f64, f64::max)
        .max(1.0)
        * 1.08;
    // Points are geometrically spaced by requested concurrency, so equal pixel
    // spacing is a log2-style concurrency axis. Labels show actual concurrency.
    let x = |i: usize| L + i as f64 * pw / windows.len().saturating_sub(1).max(1) as f64;
    let y = |v: f64| T + ph - v / max * ph;
    let mut out = format!(
        "<figure class=\"chart\"><figcaption>{}</figcaption><svg viewBox=\"0 0 {W} {H}\" role=\"img\" aria-label=\"{}\">",
        escape(y_label),
        escape(title)
    );
    out.push_str(&format!("<title>{}</title>", escape(title)));
    if warmup > 0 && !windows.is_empty() {
        let boundary = if warmup >= windows.len() {
            W - R
        } else {
            (x(warmup - 1) + x(warmup)) / 2.0
        };
        write!(out, "<rect class=\"warmup\" x=\"{L}\" y=\"{T}\" width=\"{:.1}\" height=\"{ph}\"/><text class=\"annotation\" x=\"{}\" y=\"{}\">warmup</text>", boundary-L, L+7.0, T+14.0).unwrap();
    }
    for tick in 0..=4 {
        let value = max * tick as f64 / 4.0;
        let yy = y(value);
        write!(out, "<line class=\"grid\" x1=\"{L}\" x2=\"{}\" y1=\"{yy:.1}\" y2=\"{yy:.1}\"/><text class=\"axis y\" x=\"{}\" y=\"{:.1}\">{}</text>", W-R, L-8.0, yy+4.0, compact(value)).unwrap();
    }
    for (i, window) in windows.iter().enumerate() {
        if i % 2 == 0 || i + 1 == windows.len() {
            write!(
                out,
                "<text class=\"axis\" x=\"{:.1}\" y=\"{}\">{}</text>",
                x(i),
                H - 17.0,
                window.sequence
            )
            .unwrap();
        }
    }
    for (name, color, value) in series {
        let points = windows
            .iter()
            .enumerate()
            .map(|(i, w)| format!("{:.1},{:.1}", x(i), y(value(w))))
            .collect::<Vec<_>>()
            .join(" ");
        write!(
            out,
            "<polyline fill=\"none\" stroke=\"{color}\" stroke-width=\"2.5\" points=\"{points}\"/>"
        )
        .unwrap();
        for (i, window) in windows.iter().enumerate() {
            let v = value(window);
            write!(out, "<circle cx=\"{:.1}\" cy=\"{:.1}\" r=\"3\" fill=\"{color}\"><title>window {} · {} {}</title></circle>", x(i), y(v), window.sequence, escape(name), compact(v)).unwrap();
        }
    }
    out.push_str("<g class=\"legend\">");
    for (i, (name, color, _)) in series.iter().enumerate() {
        let xx = L + i as f64 * 86.0;
        write!(out, "<line x1=\"{xx}\" x2=\"{}\" y1=\"{}\" y2=\"{}\" stroke=\"{color}\" stroke-width=\"3\"/><text x=\"{}\" y=\"{}\">{}</text>", xx+18.0, H-4.0, H-4.0, xx+23.0, H, escape(name)).unwrap();
    }
    out.push_str("</g></svg></figure>");
    out
}

fn compact(value: f64) -> String {
    if value >= 1_000_000.0 {
        format!("{:.1}m", value / 1_000_000.0)
    } else if value >= 1_000.0 {
        format!("{:.0}k", value / 1_000.0)
    } else if value >= 100.0 {
        format!("{value:.0}")
    } else {
        format!("{value:.1}")
    }
}

fn escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

const CSS: &str = r#"
:root{color-scheme:light;--ink:#172033;--muted:#657084;--line:#dfe4ec;--panel:#fff;--wash:#f4f7fb;--blue:#2457d6;--shadow:0 12px 38px rgba(25,39,70,.08)}*{box-sizing:border-box}body{margin:0;background:var(--wash);color:var(--ink);font:14px/1.55 ui-sans-serif,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}main{max-width:1240px;margin:auto;padding:38px 28px 64px}.hero{display:grid;grid-template-columns:1fr minmax(280px,.42fr);gap:48px;padding:42px;border-radius:20px;color:#fff;background:linear-gradient(125deg,#172554,#1d4ed8 68%,#0f766e);box-shadow:var(--shadow)}h1{font-size:clamp(34px,5vw,58px);line-height:1.02;letter-spacing:-.045em;margin:8px 0 20px}h2{font-size:28px;letter-spacing:-.025em;margin:4px 0}h3{font-size:21px;margin:2px 0}.lede{font-size:17px;max-width:720px;color:#dbeafe}.eyebrow{margin:0;color:inherit;font-size:11px;font-weight:800;letter-spacing:.16em;text-transform:uppercase}.metadata{margin:0;display:grid;align-content:center;gap:11px}.metadata div{display:grid;grid-template-columns:80px 1fr;gap:12px;padding-bottom:9px;border-bottom:1px solid rgba(255,255,255,.18)}dt{color:#bfdbfe;font-size:11px;text-transform:uppercase;letter-spacing:.08em}dd{margin:0;font:12px ui-monospace,SFMono-Regular,Menlo,monospace;overflow-wrap:anywhere}section{margin-top:26px;padding:30px;border:1px solid var(--line);border-radius:18px;background:var(--panel);box-shadow:var(--shadow)}.section-head{display:grid;grid-template-columns:minmax(260px,.55fr) 1fr;gap:40px;align-items:end;margin-bottom:24px}.section-head .eyebrow{color:var(--blue)}.section-head>p{margin:0;color:var(--muted);max-width:700px}.metric-guide{margin:18px 0 22px;padding:20px;border:1px solid var(--line);border-radius:12px;background:#f8fafc}.metric-guide h3{margin:0 0 14px}.metric-guide dl{display:grid;grid-template-columns:repeat(2,minmax(260px,1fr));gap:0 28px;margin:0}.metric-guide dl div{padding:11px 0;border-top:1px solid var(--line)}.metric-guide dt{color:var(--ink);font-size:12px;font-weight:800;letter-spacing:0;text-transform:none}.metric-guide dd{margin:4px 0 0;color:var(--muted);font:13px/1.5 ui-sans-serif,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}.table-wrap{overflow-x:auto;border:1px solid var(--line);border-radius:12px}table{border-collapse:collapse;width:100%;min-width:760px}th,td{padding:13px 15px;border-bottom:1px solid var(--line);text-align:left;vertical-align:top}thead th{background:#f8fafc;color:#526079;font-size:11px;letter-spacing:.055em;text-transform:uppercase}tbody tr:last-child>*{border-bottom:0}tbody tr:hover{background:#fafcff}th[scope=row]{font-weight:650}.num{text-align:right;font-variant-numeric:tabular-nums}code{font:12px ui-monospace,SFMono-Regular,Menlo,monospace;background:#eef2f8;padding:2px 5px;border-radius:4px}.badge{display:inline-block;white-space:nowrap;padding:3px 8px;margin-right:7px;border-radius:999px;font-size:10px;font-weight:800;letter-spacing:.045em;text-transform:uppercase}.badge.pass{color:#166534;background:#dcfce7}.badge.fail{color:#991b1b;background:#fee2e2}.badge.gap{color:#92400e;background:#fef3c7}.badge.skip{color:#5b21b6;background:#ede9fe}.badge.idle{color:#475569;background:#e2e8f0}.detail{display:block;margin-top:6px;color:var(--muted);font-size:12px;max-width:290px}.muted{color:#a1aabc}.backend-card{margin-top:22px;padding:24px;border:1px solid var(--line);border-radius:14px;background:#fbfdff}.card-title{display:flex;justify-content:space-between;gap:20px;align-items:center;margin-bottom:16px}.card-title .eyebrow{color:var(--blue)}.summary-chip{display:flex;flex-direction:column;text-align:right}.summary-chip span{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.08em}.summary-chip strong{font-size:18px}.charts{display:grid;grid-template-columns:1fr 1fr;gap:16px}.chart{margin:0;padding:12px;border:1px solid var(--line);border-radius:10px;background:#fff}.chart figcaption{font-weight:700;margin:2px 5px 8px}.chart svg{width:100%;height:auto}.grid{stroke:#e8ecf2;stroke-width:1}.warmup{fill:#e9eef9}.axis{font:10px ui-sans-serif,sans-serif;fill:#68758a;text-anchor:middle}.axis.y{text-anchor:end}.annotation{font:9px ui-sans-serif,sans-serif;fill:#78869b;text-transform:uppercase}.legend text{font:10px ui-sans-serif,sans-serif;fill:#536177}.compact table{min-width:650px}.compact th,.compact td{padding:8px 11px}details{margin-top:14px}summary{cursor:pointer;color:var(--blue);font-weight:700;margin-bottom:10px}.empty{display:flex;flex-direction:column;gap:5px;padding:28px;text-align:center;color:var(--muted);border:1px dashed #bdc7d6;border-radius:12px}.empty strong{color:var(--ink)}.note{margin-top:18px;padding:16px 20px;border-left:4px solid #d97706;background:#fff8e8;color:#72500c}.note strong{margin-right:5px}small{display:block;color:#7b8799;font-weight:500;text-transform:none;letter-spacing:0}footer{text-align:center;margin-top:28px;color:var(--muted);font-size:12px}@media(max-width:850px){main{padding:18px 12px 40px}.hero,.section-head,.charts,.metric-guide dl{grid-template-columns:1fr}.hero{padding:28px;gap:26px}section{padding:20px}.section-head{gap:10px}.charts{gap:12px}}@media print{body{background:#fff}main{max-width:none;padding:0}.hero,section{box-shadow:none;break-inside:avoid}.backend-card{break-before:page}.chart{break-inside:avoid}details>div{display:block!important}summary{display:none}}
"#;

#[cfg(test)]
mod tests {
    use crate::backend::BackendId;
    use crate::perf::{ExecutionMetadata, PerfConfig, PerfSummary, WindowMetrics};
    use crate::report::{Environment, GauntletReport, Outcome, Status};

    use super::*;

    fn report() -> GauntletReport {
        GauntletReport::new(Environment {
            host: "host <one>".into(),
            date: "2026-08-31".into(),
            commit: "abc&123".into(),
            execution: ExecutionMetadata::localhost("host"),
        })
    }

    #[test]
    fn renders_complete_self_contained_document_and_escapes_metadata() {
        let html = render_html(&report());
        assert!(html.starts_with("<!doctype html>"));
        assert!(html.contains("Tier 1 · Maelstrom lin-kv"));
        assert!(html.contains("Tier 2 · Performance"));
        assert!(html.contains("Tier 3 · Source measurements"));
        assert!(html.contains("What each column means"));
        assert!(html.contains("sliced!</code> total LOC"));
        assert!(html.contains("These are source-size proxies only"));
        assert!(!html.contains("S1–S5"));
        assert!(!html.contains(">Assumer<"));
        assert!(html.contains("host &lt;one&gt;"));
        assert!(html.contains("abc&amp;123"));
        assert!(!html.contains("<script"));
        assert!(!html.contains("https://"));
    }

    #[test]
    fn saturation_curve_generates_accessible_inline_svg_and_point_data() {
        let mut report = report();
        let config = crate::perf::SweepConfig {
            concurrency: vec![1, 2, 4],
            repetitions: 1,
            ..crate::perf::SweepConfig::default()
        };
        let points = config
            .concurrency
            .iter()
            .map(|&concurrency| {
                let windows = (0..15)
                    .map(|sequence| WindowMetrics {
                        sequence,
                        throughput_rps: concurrency as f64 * 100.0,
                        p50_ms: concurrency as f64,
                        p99_ms: concurrency as f64 * 2.0,
                        p999_ms: concurrency as f64 * 3.0,
                        samples: 100,
                    })
                    .collect();
                let summary = PerfSummary::new(windows, PerfConfig::default()).unwrap();
                crate::perf::SaturationPoint::new(concurrency, 1, vec![summary], 1).unwrap()
            })
            .collect();
        report.saturation_curves.push(
            crate::perf::SaturationCurve::new(
                "raft",
                ExecutionMetadata::localhost("host"),
                config,
                points,
            )
            .unwrap(),
        );
        report.backend_mut(BackendId::Raft).perf_status =
            Status::new(Outcome::Passed, "local <run>");
        let html = render_html(&report);
        assert!(html.contains("<svg"));
        assert!(html.contains("role=\"img\""));
        assert!(html.contains("raft throughput by concurrency"));
        assert!(html.contains("p99.9"));
        assert!(html.contains("local &lt;run&gt;"));
        assert!(html.contains("Concurrency"));
    }

    #[test]
    fn empty_performance_is_explained_not_omitted() {
        let html = render_html(&report());
        assert!(html.contains("No saturation curves attached."));
        assert!(html.contains("Capability gap"));
    }
}
