p='hydrofrac/src/check/engine.rs'
s=open(p).read()
writer='''        let results = Engine::new(self, sim, &labels, &mut body).check();
        // TEMPORARY (not committed): one row per label, for the change report.
        if let Some(path) = std::env::var_os("HYDROFRAC_ROWS") {
            use std::io::Write;
            let test = crate::lamport::test_name();
            let mut rows = String::new();
            for l in results.results() {
                let v = match &l.verdict {
                    Verdict::Certified { tolerates } => match tolerates {
                        crate::search::Tolerance::All => "CERT(All)".to_owned(),
                        crate::search::Tolerance::UpTo(k) => format!("CERT(UpTo {k})"),
                    },
                    Verdict::Counterexample(c) => format!(
                        "CEX({} crashes, {} faults; tol={:?}{})",
                        c.crashes(),
                        c.faults.len(),
                        c.tolerates,
                        if c.tolerance_certified { " cert" } else { "" }
                    ),
                    Verdict::Exhausted { tolerates, .. } => match tolerates {
                        crate::search::Tolerance::All => "EXH(All)".to_owned(),
                        crate::search::Tolerance::UpTo(k) => format!("EXH(UpTo {k})"),
                    },
                    Verdict::Budget { .. } => "BUDGET".to_owned(),
                    Verdict::Vacuous(r) => format!("VAC({r:?})"),
                    Verdict::NotChecked(_) => "NC".to_owned(),
                    Verdict::CoveredBy(i) => format!("COV({i})"),
                };
                rows.push_str(&format!(
                    "ROW\\t{test}\\t{}\\t{v}\\t{}\\t{}\\t{}\\t{}\\n",
                    l.name(),
                    l.stats.hypotheses,
                    l.stats.schedules,
                    l.stats.wall.as_millis(),
                    l.stats.races.is_some_and(|r| r.claim_b),
                ));
            }
            if let Ok(mut f) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
            {
                let _ = f.write_all(rows.as_bytes());
            }
        }
        results
'''
assert '        Engine::new(self, sim, &labels, &mut body).check()\n' in s
s=s.replace('        Engine::new(self, sim, &labels, &mut body).check()\n', writer, 1)
open(p,'w').write(s)
