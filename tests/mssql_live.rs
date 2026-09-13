//! End-to-end test against the local throwaway SQL Server container described in
//! `CLAUDE.md`'s Environment notes. Requires that container to be running; not part of a
//! CI gate yet (there is no SQL Server service in CI), so failures here point at the
//! local Docker setup, not necessarily a real regression, until CI grows one.

use std::collections::HashMap;
use std::sync::Mutex;

use tiberius::{Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

use tiberiusdelta::catalog::TableSync;
use tiberiusdelta::connect::ConnectConfig;
use tiberiusdelta::pipeline::{self, SyncConfig};

const CONNECTION_STRING: &str = "Server=tcp:127.0.0.1,14330;Database=tiberiusdelta_test;User Id=sa;Password=Test_Passw0rd!2026;TrustServerCertificate=true";

/// All tests in this file read, and some write, the *same* live `dbo.customers` table,
/// even though each uses its own local Delta output directory. `cargo test` runs tests in
/// parallel by default, so without this lock one test's mutation of the shared source
/// table can race with another's read of it. Held for a whole test's duration: this is a
/// test-isolation concern for a live external resource, not something the pipeline itself
/// needs to guard against, and every test here must remain correct regardless of what
/// state earlier tests left the shared table in.
static LIVE_DB: Mutex<()> = Mutex::new(());

/// Takes the shared-database lock, tolerating a poisoned one.
///
/// The lock orders access to a shared external resource; it guards no invariant that a
/// panicking test could have left half-built. Without this, the first test to fail
/// poisons the mutex and every other test in the file fails with `PoisonError` instead of
/// its own result, which hides the real failure behind three fake ones.
fn live_db() -> std::sync::MutexGuard<'static, ()> {
    LIVE_DB.lock().unwrap_or_else(|e| e.into_inner())
}

fn tmpdir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("tiberiusdelta-live-{tag}-{}", std::process::id()));
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
        checkpoint_uri: format!("{}/_streamer_checkpoints", output_uri.trim_end_matches('/')),
        output_uri,
        fetch_batch_size: 100,
        query_timeout_sec: Some(30),
    }
}

fn customers_sync() -> TableSync {
    TableSync {
        table: "dbo.customers".to_string(),
        watermark_column: "updated_at".to_string(),
        primary_key: vec!["id".to_string()],
    }
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// A fresh admin connection to the live source, for setup and assertions outside the
/// pipeline under test. Deliberately built here rather than through
/// `tiberiusdelta::connect`, so a bug in the code under test cannot also silently break
/// the test's own view of the database.
async fn admin_client() -> Client<Compat<TcpStream>> {
    let config = Config::from_ado_string(CONNECTION_STRING).expect("connection string");
    let tcp = TcpStream::connect(config.get_addr())
        .await
        .expect("tcp connect");
    tcp.set_nodelay(true).unwrap();
    Client::connect(config, tcp.compat_write())
        .await
        .expect("admin connection")
}

/// The source's own greatest `updated_at`, rendered the way a checkpoint records it.
///
/// `crate::builders::render_text` formats a `DATETIME2` through chrono's own `Display`,
/// so reading it back as a `NaiveDateTime` and rendering it the same way compares the
/// value rather than a formatting convention.
async fn max_updated_at() -> String {
    let mut admin = admin_client().await;
    admin
        .query("SELECT MAX(updated_at) FROM dbo.customers", &[])
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .expect("query returned no rows")
        .get::<chrono::NaiveDateTime, _>(0)
        .expect("the source table has no rows to take a maximum of")
        .to_string()
}

/// The source's own current row count, queried directly rather than assumed, so this
/// file's tests stay correct regardless of what earlier tests inserted.
async fn source_row_count() -> i64 {
    let mut admin = admin_client().await;
    admin
        .query("SELECT COUNT(*) FROM dbo.customers", &[])
        .await
        .unwrap()
        .into_row()
        .await
        .unwrap()
        .expect("count returned no row")
        .get::<i32, _>(0)
        .expect("count was NULL")
        .into()
}

async fn execute(sql: &str) {
    admin_client().await.execute(sql, &[]).await.unwrap();
}

#[test]
fn a_first_sync_pulls_every_row_then_an_incremental_sync_pulls_only_the_change() {
    let _guard = live_db();
    let dir = tmpdir("customers");
    let config = sync_config(uri(&dir));

    runtime().block_on(async {
        let expected_rows = source_row_count().await;

        let stats = pipeline::sync_table(&config, &customers_sync())
            .await
            .expect("first sync should succeed");
        assert_eq!(stats.table, "dbo.customers");
        assert_eq!(stats.rows_fetched, expected_rows as u64);
        assert_eq!(stats.rows_inserted as u64, stats.rows_fetched);
        assert_eq!(stats.rows_updated, 0);
        assert!(
            stats.text_fallback_columns.is_empty(),
            "the fixture's own types should all map natively, got {:?}",
            stats.text_fallback_columns
        );

        // A second sync with no source changes must pull nothing: the watermark filter
        // should exclude every already-synced row.
        let stats = pipeline::sync_table(&config, &customers_sync())
            .await
            .expect("second sync should succeed");
        assert_eq!(
            stats.rows_fetched, 0,
            "an unchanged source must not be re-fetched"
        );
    });

    let _ = std::fs::remove_dir_all(&dir);
}

/// The core promise of incremental sync: mutating one row in the live source, after an
/// initial full sync, must cause exactly that row (and no others) to be re-fetched and
/// merged on the next run. Uses a row this test inserts and cleans up itself, rather than
/// mutating a seeded fixture row, so it cannot corrupt what other tests in this file see.
#[test]
fn mutating_one_source_row_causes_only_that_row_to_resync() {
    let _guard = live_db();
    let dir = tmpdir("mutate");
    let config = sync_config(uri(&dir));

    runtime().block_on(async {
        // Watermarks are computed relative to what is already in the table rather than
        // hardcoded. An absolute date silently breaks this test whenever anything else
        // leaves behind a row with a later one (tools/smoke.py does exactly that): the
        // checkpoint then sits ahead of this row, and the strictly-greater-than filter
        // correctly excludes it, so the library looks wrong when it is right. This file's
        // own contract is that every test stays correct regardless of inherited state,
        // and hardcoding broke it.
        execute(
            "DELETE FROM dbo.customers WHERE id = 999; \
             DECLARE @next DATETIME2 = \
               DATEADD(day, 1, (SELECT MAX(updated_at) FROM dbo.customers)); \
             INSERT INTO dbo.customers (id, name, balance, is_active, updated_at) \
             VALUES (999, N'Zed', 1.00, 1, @next)",
        )
        .await;

        let first = pipeline::sync_table(&config, &customers_sync())
            .await
            .unwrap();
        assert!(first.rows_fetched >= 1);

        execute(
            "DECLARE @next DATETIME2 = \
               DATEADD(day, 1, (SELECT MAX(updated_at) FROM dbo.customers)); \
             UPDATE dbo.customers SET balance = 555.55, updated_at = @next WHERE id = 999",
        )
        .await;

        let second = pipeline::sync_table(&config, &customers_sync())
            .await
            .unwrap();
        assert_eq!(
            second.rows_fetched, 1,
            "exactly one mutated row should be re-fetched, got {}",
            second.rows_fetched
        );
        assert_eq!(second.rows_updated, 1);
        assert_eq!(second.rows_inserted, 0);

        // A third sync with no further changes must again pull nothing.
        let third = pipeline::sync_table(&config, &customers_sync())
            .await
            .unwrap();
        assert_eq!(third.rows_fetched, 0);

        execute("DELETE FROM dbo.customers WHERE id = 999").await;
    });

    let _ = std::fs::remove_dir_all(&dir);
}

/// The type mapping, end to end against a real SQL Server rather than against beliefs
/// about how TDS encodes things.
///
/// `dbo.type_zoo` (see `.devtest/type_zoo.sql`) holds one column of every type
/// `crate::types` maps, each nullable, with one row of values and one row of NULLs. That
/// the sync *succeeds at all* is most of the assertion: `crate::builders` returns
/// `UnparsableValue` rather than coercing whenever a decoded value does not match the
/// type its column resolved to, so a single wrong mapping fails this test. The schema
/// assertions then pin down what each type actually became in Delta.
#[test]
fn every_mapped_type_round_trips_from_the_catalog_into_the_delta_schema() {
    let _guard = live_db();
    let dir = tmpdir("type-zoo");
    let config = sync_config(uri(&dir));
    let table_sync = TableSync {
        table: "dbo.type_zoo".to_string(),
        watermark_column: "updated_at".to_string(),
        primary_key: vec!["id".to_string()],
    };

    runtime().block_on(async {
        let stats = pipeline::sync_table(&config, &table_sync)
            .await
            .expect("the type zoo should sync");
        assert_eq!(stats.rows_fetched, 2, "one row of values, one row of NULLs");
        assert_eq!(stats.rows_inserted, 2);
        assert!(
            stats.text_fallback_columns.is_empty(),
            "every type in the zoo should be mapped, got fallbacks: {:?}",
            stats.text_fallback_columns
        );

        let table_uri = format!("{}/dbo/type_zoo", uri(&dir));
        let url = deltalake::table::builder::ensure_table_uri(&table_uri).unwrap();
        let mut table = deltalake::DeltaTableBuilder::from_url(url)
            .unwrap()
            .build()
            .unwrap();
        table.load().await.unwrap();
        let schema = table.snapshot().unwrap().schema();
        let declared: HashMap<String, String> = schema
            .fields()
            .map(|f| (f.name().to_string(), f.data_type().to_string()))
            .collect();

        for (column, expected) in [
            ("id", "integer"),
            ("c_tinyint", "short"),
            ("c_smallint", "short"),
            ("c_bigint", "long"),
            ("c_bit", "boolean"),
            ("c_real", "double"),
            ("c_float", "double"),
            ("c_money", "double"),
            ("c_decimal", "decimal(18,4)"),
            ("c_numeric", "decimal(5,0)"),
            ("c_date", "date"),
            ("c_datetime", "timestamp"),
            ("c_datetime2", "timestamp"),
            ("c_smalldatetime", "timestamp"),
            ("c_datetimeoffset", "timestamp"),
            ("c_varchar", "string"),
            ("c_nvarchar", "string"),
            ("c_varbinary", "binary"),
            ("c_uniqueidentifier", "string"),
            ("c_xml", "string"),
        ] {
            assert_eq!(
                declared.get(column).map(String::as_str),
                Some(expected),
                "{column} should be Delta {expected}, schema was {declared:?}"
            );
        }

        // Reading the committed data back, rather than trusting that a sync which did
        // not error also wrote the right numbers. A decimal rescaled wrongly is still a
        // perfectly valid decimal, so only the value itself proves the conversion.
        let ctx = deltalake::datafusion::prelude::SessionContext::new();
        let provider = deltalake::delta_datafusion::TableProviderBuilder::default()
            .with_log_store(table.log_store())
            .build()
            .await
            .unwrap();
        ctx.register_table("zoo", std::sync::Arc::new(provider))
            .unwrap();
        let rendered = ctx
            .sql(
                "SELECT CAST(c_decimal AS STRING) AS d, CAST(c_numeric AS STRING) AS n, \
                 CAST(c_tinyint AS STRING) AS t, CAST(c_date AS STRING) AS dt, \
                 CAST(c_time AS STRING) AS tm, CAST(c_datetime2 AS STRING) AS ts, \
                 CAST(c_datetimeoffset AS STRING) AS tso, \
                 encode(c_varbinary, 'hex') AS b \
                 FROM zoo WHERE id = 1",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let batch = rendered.first().expect("row id = 1 should be present");
        let value = |name: &str| {
            let idx = batch.schema().index_of(name).unwrap();
            deltalake::arrow::util::display::array_value_to_string(batch.column(idx), 0).unwrap()
        };

        assert_eq!(value("d"), "12345.6789", "DECIMAL(18,4) must be exact");
        assert_eq!(value("n"), "42", "NUMERIC(5,0) must be exact");
        assert_eq!(value("t"), "255", "TINYINT is unsigned and must not wrap");
        assert_eq!(value("dt"), "2026-03-04");
        assert_eq!(value("tm"), "13:45:30.123456700");
        // The trailing Z is load-bearing: a timestamp with no zone is what delta-rs
        // maps to timestamp_ntz, which would break this crate's reader-v1 floor.
        assert_eq!(value("ts"), "2026-03-04T13:45:30.123456Z");
        // Stored at +02:00 in the source; normalised to UTC, not merely relabelled.
        assert_eq!(value("tso"), "2026-03-04T11:45:30.123456Z");
        assert_eq!(value("b"), "0fa0", "VARBINARY must keep its exact bytes");
    });

    let _ = std::fs::remove_dir_all(&dir);
}

/// The bulk-backfill handover, which is the whole point of `source_watermark` and
/// `set_checkpoint`: a table too large to seed a row at a time is loaded by other means,
/// its checkpoint is recorded, and incremental sync then fetches *nothing* rather than
/// re-pulling everything. Getting this wrong is expensive and silent, so it is gated.
#[test]
fn a_recorded_checkpoint_hands_over_to_incremental_sync_without_re_pulling() {
    let _guard = live_db();
    let dir = tmpdir("handover");
    let config = sync_config(uri(&dir));

    // Captured before any "export", exactly as the real procedure requires.
    let watermark = pipeline::source_watermark(&config, &customers_sync())
        .unwrap()
        .expect("the fixture has rows, so it has a watermark");

    // No data is loaded here at all: this asserts the checkpoint alone is what stops the
    // re-pull, so a handover that records the wrong value cannot pass by accident.
    pipeline::set_checkpoint(&config, &customers_sync(), &watermark).unwrap();

    runtime().block_on(async {
        let stats = pipeline::sync_table(&config, &customers_sync())
            .await
            .expect("sync after handover should succeed");
        assert_eq!(
            stats.rows_fetched, 0,
            "a recorded checkpoint must stop the whole table being re-pulled"
        );
    });

    // And the handover must not have broken incremental sync going forward.
    runtime().block_on(async {
        execute(
            "DECLARE @next DATETIME2 =                DATEADD(day, 1, (SELECT MAX(updated_at) FROM dbo.customers));              UPDATE dbo.customers SET name = N'After handover', updated_at = @next              WHERE id = 2",
        )
        .await;
        let stats = pipeline::sync_table(&config, &customers_sync())
            .await
            .unwrap();
        assert_eq!(stats.rows_fetched, 1, "a later change must still be seen");
    });

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn the_checkpoint_advances_to_the_greatest_watermark_seen() {
    let _guard = live_db();
    let dir = tmpdir("checkpoint-advance");
    let config = sync_config(uri(&dir));

    runtime().block_on(async {
        let expected_max = max_updated_at().await;

        pipeline::sync_table(&config, &customers_sync())
            .await
            .unwrap();

        // Each table's checkpoint is its own Delta table, laid out to mirror the data:
        // dbo.customers is checkpointed at <checkpoint_uri>/dbo/customers.
        let at = tiberiusdelta::checkpoint::uri_for(&config.checkpoint_uri, "dbo/customers");
        let recorded = tiberiusdelta::checkpoint::read(&at)
            .await
            .unwrap()
            .expect("a checkpoint must be recorded");
        assert_eq!(
            recorded, expected_max,
            "checkpoint should be the source's own greatest updated_at"
        );
    });

    let _ = std::fs::remove_dir_all(&dir);
}
