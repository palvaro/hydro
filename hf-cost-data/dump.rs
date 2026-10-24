
/// TEMPORARY (cost table measurement; not to be committed).
pub(crate) fn hf_dump(results: &Results) {
    use std::io::Write;
    let Ok(path) = std::env::var("HF_DUMP") else { return };
    let test = std::thread::current().name().unwrap_or("?").to_owned();
    let mut out = String::new();
    for l in &results.labels {
        let v = match &l.verdict {
            Verdict::Certified { tolerates } => format!("CERT({tolerates:?})"),
            Verdict::Counterexample(c) => format!(
                "CEX({} crashes, {} faults; tol={:?}{})",
                c.crashes(),
                c.faults.len(),
                c.tolerates,
                if c.tolerance_certified { " cert" } else { "" }
            ),
            Verdict::Vacuous(r) => format!("VAC({r:?})"),
            Verdict::Budget { tolerates } => format!("BUDGET(tol={tolerates:?})"),
            Verdict::Exhausted { tolerates, .. } => format!("EXH({tolerates:?})"),
            Verdict::NotChecked(_) => "NC".to_owned(),
            Verdict::CoveredBy(i) => format!("COV({i})"),
        };
        out.push_str(&format!(
            "{test}\t{}\t{v}\t{}\t{}\t{}\n",
            l.name(),
            l.stats.hypotheses,
            l.stats.schedules,
            l.stats.wall.as_micros()
        ));
    }
    let mut f = std::fs::OpenOptions::new().create(true).append(true).open(path).unwrap();
    f.write_all(out.as_bytes()).unwrap();
}
