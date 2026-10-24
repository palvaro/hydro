use std::process::Command;

#[test]
fn compare_command_writes_two_backend_report_and_raw_curves() {
    let binary = env!("CARGO_BIN_EXE_consensus_gauntlet");
    let root = tempfile::tempdir().unwrap();
    let report = root.path().join("comparison.html");
    let artifacts = root.path().join("artifacts");

    let status = Command::new(binary)
        .args([
            "compare",
            "--other",
            "quorum-ladder-consensus",
            "--concurrency",
            "1",
            "--repetitions",
            "1",
            "--output",
        ])
        .arg(&report)
        .arg("--artifacts")
        .arg(&artifacts)
        .status()
        .unwrap();
    assert!(status.success());
    assert!(report.is_file());
    assert!(artifacts.join("raft.json").is_file());
    assert!(artifacts.join("quorum-ladder-consensus.json").is_file());

    let html = std::fs::read_to_string(report).unwrap();
    assert!(html.contains("raft throughput by concurrency"));
    assert!(html.contains("quorum-ladder-consensus throughput by concurrency"));
    assert!(!html.contains("library-paxos"));
}
