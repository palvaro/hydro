//! Test: service runner operates independently of the dataflow.
//! Proves the separation: dataflow delivers (seq, payload), service runner applies.

#[cfg(feature = "backend_redb")]
#[test]
fn service_runner_applies_in_order() {
    use lego_replicate::service_runner::ServiceRunner;
    use lego_replicate::backends::redb::{RedbService, RedbCommand, RedbResponse};

    let mut runner: ServiceRunner<RedbService> = ServiceRunner::fresh();
    assert_eq!(runner.resume_seq(), 0);

    // Apply 3 commands in order
    let cmd1 = bincode::serialize(&RedbCommand::Put { key: b"x".to_vec(), value: b"1".to_vec() }).unwrap();
    let cmd2 = bincode::serialize(&RedbCommand::Put { key: b"y".to_vec(), value: b"2".to_vec() }).unwrap();
    let cmd3 = bincode::serialize(&RedbCommand::Get { key: b"x".to_vec() }).unwrap();

    assert!(runner.apply(0, &cmd1).is_some());
    assert!(runner.apply(1, &cmd2).is_some());
    let resp = runner.apply(2, &cmd3).unwrap();
    assert_eq!(resp, RedbResponse::Value(Some(b"1".to_vec())));
    assert_eq!(runner.resume_seq(), 3);
}

#[cfg(feature = "backend_redb")]
#[test]
fn service_runner_idempotent_replay() {
    use lego_replicate::service_runner::ServiceRunner;
    use lego_replicate::backends::redb::{RedbService, RedbCommand, RedbResponse};

    let mut runner: ServiceRunner<RedbService> = ServiceRunner::fresh();

    let cmd = bincode::serialize(&RedbCommand::Put { key: b"k".to_vec(), value: b"v".to_vec() }).unwrap();

    // Apply seq 0
    assert!(runner.apply(0, &cmd).is_some());
    assert_eq!(runner.resume_seq(), 1);

    // Replay seq 0 — should be skipped
    assert!(runner.apply(0, &cmd).is_none());
    assert_eq!(runner.resume_seq(), 1);
}

#[cfg(feature = "backend_redb")]
#[test]
fn service_runner_snapshot_restore_resume() {
    use lego_replicate::service_runner::ServiceRunner;
    use lego_replicate::backends::redb::{RedbService, RedbCommand, RedbResponse};
    use lego_replicate::ReplicableService;

    let mut runner: ServiceRunner<RedbService> = ServiceRunner::fresh();

    let cmd = bincode::serialize(&RedbCommand::Put { key: b"a".to_vec(), value: b"1".to_vec() }).unwrap();
    runner.apply(0, &cmd);
    runner.apply(1, &bincode::serialize(&RedbCommand::Put { key: b"b".to_vec(), value: b"2".to_vec() }).unwrap());

    // Snapshot
    let snap = runner.snapshot();
    let resume = runner.resume_seq();

    // Create new runner, restore
    let mut runner2: ServiceRunner<RedbService> = ServiceRunner::new(RedbService::default(), resume);
    runner2.restore(&snap);

    // Verify state survived
    let get_a = bincode::serialize(&RedbCommand::Get { key: b"a".to_vec() }).unwrap();
    let resp = runner2.apply_read(&get_a);
    assert_eq!(resp, RedbResponse::Value(Some(b"1".to_vec())));
    assert_eq!(runner2.resume_seq(), 2);
}
