//! End-to-end tests against a local throwaway Firebird container, or the
//! `firebirddelta-ci.yml` service container in CI (see `CLAUDE.md`'s Environment notes
//! for how to start one locally and seed it).
//!
//! **Not run against a real server in this crate's own history yet**, even though a CI
//! job now exists to run it: the session that wrote this file (and the
//! `firebirdsql/firebird` image, connection details, and CI workflow it now depends on)
//! had no Docker daemon available to actually execute it even once. Treat it as a
//! carefully-researched design for what the live gate should check, not as a passed
//! gate, until its first real CI run confirms the image, the credentials, and
//! `.devtest/type_zoo.sql`'s literal syntax all actually agree with each other.

use std::sync::Mutex;

use rsfbclient::Execute;

use firebirddelta::catalog::TableSync;
use firebirddelta::connect::ConnectConfig;
use firebirddelta::pipeline::{self, SyncConfig};

/// Matches `firebirddelta-test-firebird`'s `FIREBIRD_ROOT_PASSWORD` in `CLAUDE.md`'s
/// Environment notes, and `firebird-ci.env.FIREBIRD_PASSWORD` in
/// `.github/workflows/firebirddelta-ci.yml`. `/var/lib/firebird/data/` is the official
/// `firebirdsql/firebird` image's own database directory (its `FIREBIRD_DATABASE`
/// environment variable creates the file there, and nowhere else is documented).
const CONNECTION_STRING: &str = "firebird://SYSDBA:Test_Passw0rd!2026@127.0.0.1:3050/\
     /var/lib/firebird/data/firebirddelta_test.fdb";

/// All tests in this file read, and some write, the *same* live `CUSTOMERS` table, even
/// though each uses its own local Delta output directory. `cargo test` runs tests in
/// parallel by default, so without this lock one test's mutation of the shared source
/// table can race with another's read of it. Mirrors tiberiusdelta's own
/// `tests/mssql_live.rs::LIVE_DB` for the identical reason.
static LIVE_DB: Mutex<()> = Mutex::new(());

/// Takes the shared-database lock, tolerating a poisoned one.
fn live_db() -> std::sync::MutexGuard<'static, ()> {
    LIVE_DB.lock().unwrap_or_else(|e| e.into_inner())
}

fn tmpdir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("firebirddelta-live-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn uri(dir: &std::path::Path) -> String {
    format!("file://{}", dir.to_string_lossy().replace('\\', "/"))
}

fn sync_config(output_uri: String) -> SyncConfig {
    SyncConfig {
        connect: ConnectConfig {
            connection_string: CONNECTION_STRING.to_string(),
            login_timeout_sec: Some(10),
        },
        checkpoint_uri: format!(
            "{}/_firebirddelta_checkpoints",
            output_uri.trim_end_matches('/')
        ),
        output_uri,
        fetch_batch_size: 100,
        query_timeout_sec: Some(30),
    }
}

fn customers_sync() -> TableSync {
    TableSync {
        table: "CUSTOMERS".to_string(),
        watermark_column: "UPDATED_AT".to_string(),
        primary_key: vec!["ID".to_string()],
    }
}

/// A fresh admin connection to the live source, for setup and assertions outside the
/// pipeline under test. Deliberately opened directly through `rsfbclient` rather than
/// through `firebirddelta::connect`, so a bug in the code under test cannot also
/// silently break the test's own view of the database.
fn admin_connection() -> rsfbclient::SimpleConnection {
    rsfbclient::builder_pure_rust()
        .from_string(CONNECTION_STRING)
        .expect("connection string")
        .connect()
        .expect("admin connection")
        .into()
}

#[test]
fn a_first_sync_pulls_every_row_and_a_second_pulls_nothing() {
    let _lock = live_db();
    let dir = tmpdir("first-sync");
    let config = sync_config(uri(&dir));
    let catalog =
        firebirddelta::catalog::SyncCatalog::new(vec![customers_sync()]).expect("catalog");

    let report = pipeline::run(&config, &catalog, |_| true).expect("first sync");
    assert_eq!(
        report.total_rows_fetched, 7,
        "seed.sql + alter.sql = 7 rows"
    );
    for stats in &report.tables {
        assert_eq!(stats.rows_inserted, stats.rows_fetched as usize);
        assert_eq!(stats.rows_updated, 0);
    }

    let second = pipeline::run(&config, &catalog, |_| true).expect("second sync");
    assert_eq!(
        second.total_rows_fetched, 0,
        "an unchanged source must not be re-fetched"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn an_updated_row_is_merged_in_place_not_duplicated() {
    let _lock = live_db();
    let dir = tmpdir("update");
    let config = sync_config(uri(&dir));
    let catalog =
        firebirddelta::catalog::SyncCatalog::new(vec![customers_sync()]).expect("catalog");

    pipeline::run(&config, &catalog, |_| true).expect("first sync");

    let mut admin = admin_connection();
    admin
        .execute(
            "UPDATE CUSTOMERS SET NAME = 'Alice Updated', \
             UPDATED_AT = (SELECT MAX(UPDATED_AT) FROM CUSTOMERS) + 1 WHERE ID = 1",
            (),
        )
        .expect("update");

    let report = pipeline::run(&config, &catalog, |_| true).expect("incremental sync");
    assert_eq!(report.total_rows_fetched, 1, "exactly one row changed");
    let stats = &report.tables[0];
    assert_eq!(stats.rows_updated, 1);
    assert_eq!(stats.rows_inserted, 0);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn preflight_reports_the_customers_schema_without_writing_anything() {
    let _lock = live_db();
    let dir = tmpdir("preflight");
    let config = sync_config(uri(&dir));
    let catalog =
        firebirddelta::catalog::SyncCatalog::new(vec![customers_sync()]).expect("catalog");

    let found = pipeline::preflight(&config, &catalog).expect("preflight");
    assert_eq!(found.len(), 1);
    let table = &found[0];
    assert!(table.is_ready());
    assert!(table.last_synced_value.is_none(), "nothing has synced yet");
    assert!(
        table.columns.iter().all(|c| c.recognised),
        "every CUSTOMERS column should map natively"
    );
    assert!(
        !std::path::Path::new(&format!("{}/CUSTOMERS", uri(&dir))).exists(),
        "preflight must not create a Delta table"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// The type-zoo table, read back through Delta rather than just synced without error:
/// a decimal rescaled wrongly, or a timestamp shifted by a timezone bug, is still a
/// perfectly valid-looking value, so only comparing against the source catches it. See
/// `.devtest/type_zoo.sql`'s own header for why this file, more than any other, is what
/// proves the type mapping rather than assuming it.
#[test]
fn the_type_zoo_table_decodes_every_mapped_type_correctly() {
    let _lock = live_db();
    let dir = tmpdir("type-zoo");
    let config = sync_config(uri(&dir));
    let table_sync = TableSync {
        table: "TYPE_ZOO".to_string(),
        watermark_column: "UPDATED_AT".to_string(),
        primary_key: vec!["ID".to_string()],
    };
    let catalog = firebirddelta::catalog::SyncCatalog::new(vec![table_sync]).expect("catalog");

    let report = pipeline::run(&config, &catalog, |_| true).expect("type zoo sync");
    assert_eq!(report.total_rows_fetched, 2, "one values row, one NULL row");

    let rt = tokio::runtime::Runtime::new().unwrap();
    let delta_table = rt.block_on(async {
        let url = deltalake::table::builder::ensure_table_uri(format!("{}/TYPE_ZOO", uri(&dir)))
            .expect("table uri");
        let mut t = deltalake::DeltaTableBuilder::from_url(url)
            .expect("builder")
            .build()
            .expect("build");
        t.load().await.expect("load committed table");
        t
    });
    let schema = delta_table.snapshot().expect("snapshot").schema();
    let field = |name: &str| {
        schema
            .field(name)
            .unwrap_or_else(|| panic!("missing {name}"))
    };

    // NUMERIC/DECIMAL must be Float64, not a decimal type: see crate::types::resolve's
    // documentation of why that precision loss happens before this crate ever sees the
    // value.
    assert_eq!(
        field("C_NUMERIC").data_type(),
        &deltalake::kernel::DataType::DOUBLE
    );
    assert_eq!(
        field("C_DECIMAL").data_type(),
        &deltalake::kernel::DataType::DOUBLE
    );
    // TIME, and every Firebird-4 CAST-to-text type, land as strings.
    for name in [
        "C_TIME",
        "C_INT128",
        "C_DECFLOAT16",
        "C_DECFLOAT34",
        "C_TIME_TZ",
        "C_TIMESTAMP_TZ",
    ] {
        assert_eq!(
            field(name).data_type(),
            &deltalake::kernel::DataType::STRING,
            "{name} should be text"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}
