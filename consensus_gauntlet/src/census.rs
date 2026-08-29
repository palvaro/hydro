//! Mechanical source census for protocol implementations.
//!
//! This is deliberately a syntax-level measurement, not a correctness or
//! semantic-complexity score. Comments and string literals are removed before
//! counting code sites. Kernel size is measured from balanced `sliced! { ... }`
//! source spans; it is a review-size proxy, not an IR-operator or runtime-cost
//! measurement.

/// Counts produced for one protocol source file.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Census {
    /// Physical source lines before the first top-level test-module marker.
    pub body_loc: usize,
    /// Calls to `assert_has_consistency_of`.
    pub consistency_mints: usize,
    /// `manual_proof!` invocations not consumed by a consistency assertion.
    pub algebra_proofs: usize,
    /// Calls to `assume_ordering` or `assume_retries`.
    pub assumers: usize,
    /// `nondet!(...)` macro invocations.
    pub introducer_nondets: usize,
    /// `NonDet` parameters in function signatures.
    pub forwarded_nondet_params: usize,
    /// Number of actual `sliced! { ... }` invocations.
    pub kernels: usize,
    /// Sum of the physical line spans of all `sliced!` blocks, including each
    /// block's opening and closing line. Nested blocks, if any, are counted in
    /// both spans; current protocol sources do not nest them.
    pub kernel_total_loc: usize,
    /// Largest physical line span of any one `sliced!` block.
    pub kernel_largest_loc: usize,
    /// Explicit `.end_atomic(...)` calls.
    pub cuts: usize,
    /// Actual `forward_ref(...)` calls.
    pub cycles: usize,
}

impl Census {
    /// Percentage of the measured protocol body covered by kernel source spans.
    pub fn kernel_body_percent(&self) -> f64 {
        if self.body_loc == 0 {
            0.0
        } else {
            self.kernel_total_loc as f64 * 100.0 / self.body_loc as f64
        }
    }
}

/// Census a Rust protocol source string.
///
/// The measured body is everything before the first line whose whitespace-free
/// spelling is `#[cfg(test)]`. That marker and all following test code are
/// excluded. The lightweight lexer blanks comments and literals while preserving
/// byte offsets and newlines, so prose mentions do not become code sites.
pub fn census_source(source: &str) -> Census {
    let body = protocol_body(source);
    let code = code_mask(body);
    let consistency_mints = count_identifier(&code, "assert_has_consistency_of");
    let manual_proofs = count_macro_invocations(&code, "manual_proof!");
    let kernel_spans = macro_block_line_spans(body, &code, "sliced!");

    Census {
        body_loc: body.lines().count(),
        consistency_mints,
        algebra_proofs: manual_proofs.saturating_sub(consistency_mints),
        assumers: count_identifier(&code, "assume_ordering")
            + count_identifier(&code, "assume_retries"),
        introducer_nondets: count_macro_invocations(&code, "nondet!"),
        forwarded_nondet_params: count_nondet_parameters(&code),
        kernels: kernel_spans.len(),
        kernel_total_loc: kernel_spans.iter().sum(),
        kernel_largest_loc: kernel_spans.into_iter().max().unwrap_or(0),
        cuts: count_identifier(&code, "end_atomic"),
        cycles: count_identifier(&code, "forward_ref"),
    }
}

fn protocol_body(source: &str) -> &str {
    let mut offset = 0;
    for line in source.split_inclusive('\n') {
        if is_cfg_test(line.trim_end_matches(['\r', '\n'])) {
            return &source[..offset];
        }
        offset += line.len();
    }
    source
}

fn is_cfg_test(line: &str) -> bool {
    let compact: String = line
        .trim()
        .chars()
        .filter(|ch| !ch.is_whitespace())
        .collect();
    compact == "#[cfg(test)]"
}

fn code_mask(source: &str) -> String {
    let bytes = source.as_bytes();
    let mut out = bytes.to_vec();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i..].starts_with(b"//") {
            let start = i;
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            blank_non_newlines(&mut out[start..i]);
        } else if bytes[i..].starts_with(b"/*") {
            let start = i;
            i += 2;
            let mut depth = 1;
            while i < bytes.len() && depth > 0 {
                if bytes[i..].starts_with(b"/*") {
                    depth += 1;
                    i += 2;
                } else if bytes[i..].starts_with(b"*/") {
                    depth -= 1;
                    i += 2;
                } else {
                    i += 1;
                }
            }
            blank_non_newlines(&mut out[start..i]);
        } else if let Some((hashes, quote)) = raw_string_start(bytes, i) {
            let start = i;
            i = quote + 1;
            while i < bytes.len() {
                if bytes[i] == b'"'
                    && (0..hashes).all(|hash| bytes.get(i + 1 + hash) == Some(&b'#'))
                {
                    i += 1 + hashes;
                    break;
                }
                i += 1;
            }
            blank_non_newlines(&mut out[start..i]);
        } else if bytes[i] == b'"' {
            let start = i;
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\\' {
                    i = (i + 2).min(bytes.len());
                } else if bytes[i] == b'"' {
                    i += 1;
                    break;
                } else {
                    i += 1;
                }
            }
            blank_non_newlines(&mut out[start..i]);
        } else {
            i += 1;
        }
    }
    String::from_utf8(out).expect("mask only replaces bytes with ASCII spaces")
}

fn blank_non_newlines(bytes: &mut [u8]) {
    for byte in bytes {
        if *byte != b'\n' && *byte != b'\r' {
            *byte = b' ';
        }
    }
}

fn raw_string_start(bytes: &[u8], offset: usize) -> Option<(usize, usize)> {
    let mut i = offset;
    if bytes.get(i) == Some(&b'b') {
        i += 1;
    }
    if bytes.get(i) != Some(&b'r') {
        return None;
    }
    i += 1;
    let hashes_start = i;
    while bytes.get(i) == Some(&b'#') {
        i += 1;
    }
    (bytes.get(i) == Some(&b'"')).then_some((i - hashes_start, i))
}

fn count_identifier(source: &str, name: &str) -> usize {
    source
        .match_indices(name)
        .filter(|(offset, _)| {
            let before = offset.checked_sub(1).and_then(|i| source.as_bytes().get(i));
            let after = source.as_bytes().get(offset + name.len());
            before.is_none_or(|byte| !byte.is_ascii_alphanumeric() && *byte != b'_')
                && after.is_none_or(|byte| !byte.is_ascii_alphanumeric() && *byte != b'_')
        })
        .count()
}

fn count_macro_invocations(source: &str, macro_name: &str) -> usize {
    source
        .match_indices(macro_name)
        .filter(|(offset, _)| next_non_whitespace(source, offset + macro_name.len()) == Some(b'('))
        .count()
}

fn next_non_whitespace(source: &str, offset: usize) -> Option<u8> {
    source.as_bytes()[offset..]
        .iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
}

fn macro_block_line_spans(source: &str, code: &str, macro_name: &str) -> Vec<usize> {
    code.match_indices(macro_name)
        .filter_map(|(start, _)| {
            let mut open = start + macro_name.len();
            while code
                .as_bytes()
                .get(open)
                .is_some_and(u8::is_ascii_whitespace)
            {
                open += 1;
            }
            if code.as_bytes().get(open) != Some(&b'{') {
                return None;
            }
            let mut depth = 0usize;
            for (relative, byte) in code.as_bytes()[open..].iter().enumerate() {
                match byte {
                    b'{' => depth += 1,
                    b'}' => {
                        depth -= 1;
                        if depth == 0 {
                            let close = open + relative;
                            let start_line = source.as_bytes()[..start]
                                .iter()
                                .filter(|&&byte| byte == b'\n')
                                .count();
                            let close_line = source.as_bytes()[..close]
                                .iter()
                                .filter(|&&byte| byte == b'\n')
                                .count();
                            return Some(close_line - start_line + 1);
                        }
                    }
                    _ => {}
                }
            }
            None
        })
        .collect()
}

/// Count the signature spelling `name: NonDet` after comments and literals have
/// been blanked. This remains a textual signature census, not a Rust AST pass.
fn count_nondet_parameters(source: &str) -> usize {
    source
        .lines()
        .map(|code| {
            let compact: String = code.chars().filter(|ch| !ch.is_whitespace()).collect();
            compact.match_indices(":NonDet").count()
        })
        .sum()
}

#[cfg(test)]
mod tests {
    use super::census_source;

    #[test]
    fn excludes_tests_and_does_not_count_prose_mentions() {
        let fixture = r#"fn protocol(a: NonDet, b: NonDet) {
    let _ = assume_ordering(nondet!(/** choice */));
    sliced! { forward_ref(); value.end_atomic(); }
    assert_has_consistency_of(manual_proof!(/** mint */));
    fold(commutative = manual_proof!(/** algebra */));
}
// `forward_ref`, `sliced!`, and `nondet!()` are prose, not code.
#[cfg(test)]
mod tests { fn ignored() { sliced! { nondet!(/** test */); } } }
"#;
        let census = census_source(fixture);
        assert_eq!(census.body_loc, 7);
        assert_eq!(census.consistency_mints, 1);
        assert_eq!(census.algebra_proofs, 1);
        assert_eq!(census.assumers, 1);
        assert_eq!(census.introducer_nondets, 1);
        assert_eq!(census.forwarded_nondet_params, 2);
        assert_eq!(census.kernels, 1);
        assert_eq!(census.kernel_total_loc, 1);
        assert_eq!(census.kernel_largest_loc, 1);
        assert_eq!(census.cuts, 1);
        assert_eq!(census.cycles, 1);
    }

    #[test]
    fn measures_balanced_multiline_kernel_spans() {
        let source = "fn p() {\n  sliced! {\n    if true {\n      work();\n    }\n  }\n}\n";
        let census = census_source(source);
        assert_eq!(census.kernels, 1);
        assert_eq!(census.kernel_total_loc, 5);
        assert_eq!(census.kernel_largest_loc, 5);
        assert!((census.kernel_body_percent() - 500.0 / 7.0).abs() < 0.001);
    }

    #[test]
    fn portfolio_kernel_magnitude_matches_current_sources() {
        let cases = [
            (
                include_str!("../../hydro_test/src/cluster/raft.rs"),
                1,
                119,
                119,
            ),
            (
                include_str!("../../hydro_test/src/cluster/paxos.rs"),
                2,
                56,
                33,
            ),
            (
                include_str!("../../hydro_test/src/cluster/compartmentalized_paxos.rs"),
                0,
                0,
                0,
            ),
            (
                include_str!("../../hydro_test/src/cluster/paxos_ec.rs"),
                3,
                220,
                95,
            ),
            (
                include_str!("../../hydro_test/src/cluster/typed_consensus.rs"),
                9,
                646,
                106,
            ),
            (
                include_str!("../../hydro_test/src/cluster/broadcast_transcript_consensus.rs"),
                1,
                159,
                159,
            ),
            (
                include_str!("../../hydro_std/src/ec_inference_demos/multi_paxos.rs"),
                2,
                197,
                108,
            ),
        ];
        for (source, blocks, total, largest) in cases {
            let census = census_source(source);
            assert_eq!(
                (
                    census.kernels,
                    census.kernel_total_loc,
                    census.kernel_largest_loc
                ),
                (blocks, total, largest)
            );
        }
    }
}
