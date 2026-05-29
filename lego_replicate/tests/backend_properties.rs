//! Property-based tests for backend implementations.
//! Tests that apply/snapshot/restore round-trips correctly.

use lego_replicate::ReplicableService;

#[cfg(feature = "backend_redb")]
mod redb_tests {
    use super::*;
    use lego_replicate::backends::redb::{RedbCommand, RedbResponse, RedbService};

    #[test]
    fn put_get_roundtrip() {
        let mut svc = RedbService::default();
        svc.apply(RedbCommand::Put { key: b"k".to_vec(), value: b"v".to_vec() });
        let resp = svc.apply(RedbCommand::Get { key: b"k".to_vec() });
        assert_eq!(resp, RedbResponse::Value(Some(b"v".to_vec())));
    }

    #[test]
    fn get_missing_key() {
        let mut svc = RedbService::default();
        let resp = svc.apply(RedbCommand::Get { key: b"nope".to_vec() });
        assert_eq!(resp, RedbResponse::Value(None));
    }

    #[test]
    fn snapshot_restore() {
        let mut svc = RedbService::default();
        svc.apply(RedbCommand::Put { key: b"a".to_vec(), value: b"1".to_vec() });
        svc.apply(RedbCommand::Put { key: b"b".to_vec(), value: b"2".to_vec() });
        let snap = svc.snapshot();

        let mut svc2 = RedbService::default();
        svc2.restore(&snap);
        assert_eq!(svc2.apply(RedbCommand::Get { key: b"a".to_vec() }), RedbResponse::Value(Some(b"1".to_vec())));
        assert_eq!(svc2.apply(RedbCommand::Get { key: b"b".to_vec() }), RedbResponse::Value(Some(b"2".to_vec())));
    }

    #[test]
    fn delete_existing() {
        let mut svc = RedbService::default();
        svc.apply(RedbCommand::Put { key: b"x".to_vec(), value: b"y".to_vec() });
        let resp = svc.apply(RedbCommand::Delete { key: b"x".to_vec() });
        assert_eq!(resp, RedbResponse::Deleted(true));
        assert_eq!(svc.apply(RedbCommand::Get { key: b"x".to_vec() }), RedbResponse::Value(None));
    }

    #[test]
    fn is_read_only_classification() {
        assert!(RedbService::is_read_only(&RedbCommand::Get { key: vec![] }));
        assert!(!RedbService::is_read_only(&RedbCommand::Put { key: vec![], value: vec![] }));
        assert!(!RedbService::is_read_only(&RedbCommand::Delete { key: vec![] }));
    }
}

#[cfg(feature = "backend_fjall")]
mod fjall_tests {
    use super::*;
    use lego_replicate::backends::fjall::{FjallCommand, FjallResponse, FjallService};

    #[test]
    fn put_get_roundtrip() {
        let mut svc = FjallService::default();
        svc.apply(FjallCommand::Insert { key: b"k".to_vec(), value: b"v".to_vec() });
        let resp = svc.apply(FjallCommand::Get { key: b"k".to_vec() });
        assert_eq!(resp, FjallResponse::Value(Some(b"v".to_vec())));
    }

    #[test]
    fn snapshot_restore() {
        let mut svc = FjallService::default();
        svc.apply(FjallCommand::Insert { key: b"a".to_vec(), value: b"1".to_vec() });
        let snap = svc.snapshot();

        let mut svc2 = FjallService::default();
        svc2.restore(&snap);
        assert_eq!(svc2.apply(FjallCommand::Get { key: b"a".to_vec() }), FjallResponse::Value(Some(b"1".to_vec())));
    }
}

#[cfg(feature = "backend_rusqlite")]
mod rusqlite_tests {
    use super::*;
    use lego_replicate::backends::rusqlite::{RusqliteService, SqlCommand, SqlResponse};

    #[test]
    fn create_insert_select() {
        let mut svc = RusqliteService::default();
        svc.apply(SqlCommand("CREATE TABLE t (id INTEGER, name TEXT)".into()));
        svc.apply(SqlCommand("INSERT INTO t VALUES (1, 'alice')".into()));
        let resp = svc.apply(SqlCommand("SELECT name FROM t WHERE id = 1".into()));
        match resp {
            SqlResponse::Query { rows, .. } => assert_eq!(rows[0][0], Some("alice".into())),
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn snapshot_restore() {
        let mut svc = RusqliteService::default();
        svc.apply(SqlCommand("CREATE TABLE kv (k TEXT PRIMARY KEY, v TEXT)".into()));
        svc.apply(SqlCommand("INSERT INTO kv VALUES ('x', '42')".into()));
        let snap = svc.snapshot();

        let mut svc2 = RusqliteService::default();
        svc2.restore(&snap);
        let resp = svc2.apply(SqlCommand("SELECT v FROM kv WHERE k = 'x'".into()));
        match resp {
            SqlResponse::Query { rows, .. } => assert_eq!(rows[0][0], Some("42".into())),
            other => panic!("unexpected: {:?}", other),
        }
    }
}
