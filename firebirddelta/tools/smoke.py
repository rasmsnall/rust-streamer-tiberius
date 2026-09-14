"""Round-trip a live table through the built wheel.

Runs the whole promise end to end against a real Firebird and a real Delta table, using
the installed ``firebirddelta`` wheel rather than the Rust test binary: preflight, a
first sync that pulls everything, a second that pulls nothing, and a third that pulls
exactly the one row that changed. Reads the committed Delta table back with the
``deltalake`` package to check the values actually landed, not merely that nothing
raised. Mirrors tiberiusdelta's own ``tools/smoke.py`` step for step.

Expects ``tools/seed.py`` to have run first. Reads the password from
``FIREBIRD_PASSWORD``, which must match the container's own ``FIREBIRD_ROOT_PASSWORD``.

Usage::

    python tools/smoke.py [host] [port] [path/to/test.fdb]
"""

from __future__ import annotations

import os
import shutil
import sys
import tempfile

import firebird.driver as fdb
from deltalake import DeltaTable

import firebirddelta

TABLES = [
    {"table": "CUSTOMERS", "watermark_column": "UPDATED_AT", "primary_key": ["ID"]},
    {"table": "TYPE_ZOO", "watermark_column": "UPDATED_AT", "primary_key": "ID"},
]


DSN = PASSWORD = None


def source_connection():
    return fdb.connect(DSN, user="SYSDBA", password=PASSWORD)


def query(sql: str) -> list[tuple]:
    connection = source_connection()
    cursor = connection.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    connection.close()
    return rows


def execute(sql: str) -> None:
    connection = source_connection()
    connection.cursor().execute(sql)
    connection.commit()
    connection.close()


def fail(message: str) -> None:
    raise SystemExit(f"smoke test failed: {message}")


def check(condition: bool, message: str) -> None:
    if not condition:
        fail(message)


def main() -> int:
    global DSN, PASSWORD
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = sys.argv[2] if len(sys.argv) > 2 else "3050"
    db_path = (
        sys.argv[3] if len(sys.argv) > 3 else "/var/lib/firebird/data/firebirddelta_test.fdb"
    )
    PASSWORD = os.environ.get("FIREBIRD_PASSWORD")
    if not PASSWORD:
        raise SystemExit("set FIREBIRD_PASSWORD to the instance's SYSDBA password")
    DSN = f"{host}/{port}:{db_path}"

    # Two slashes before an absolute path, not one: rsfbclient's URL parser strips
    # exactly one leading slash off the URL path component when a host is present (so a
    # *relative* db_name round-trips correctly), which would otherwise turn
    # "/var/lib/..." into the relative, wrong "var/lib/...". See tests/firebird_live.rs's
    # own CONNECTION_STRING for the same footgun, and rsfbclient's own conn_string.rs
    # tests, which use exactly this double-slash form for every absolute-path example.
    connection_string = f"firebird://SYSDBA:{PASSWORD}@{host}:{port}/{db_path}"

    output = tempfile.mkdtemp(prefix="firebirddelta-smoke-")
    output_uri = "file://" + output.replace("\\", "/")
    try:
        # 1. Preflight: every configured table must be ready, and the customers fixture's
        #    own types are all mapped, so nothing should fall back to text.
        found = firebirddelta.preflight(connection_string, TABLES, output_uri=output_uri)
        check(len(found) == 2, f"expected two tables, got {len(found)}")
        for table in found:
            check(table.ready, f"{table.table} is not ready: {table!r}")
            check(table.last_synced_value is None, f"{table.table} already has a checkpoint")
        customers = next(t for t in found if t.table == "CUSTOMERS")
        check(
            [c.name for c in customers.columns][:2] == ["ID", "NAME"],
            "columns should come back in RDB$FIELD_POSITION order",
        )
        check(
            all(c.recognised for c in customers.columns),
            "the customers fixture should map every column natively",
        )
        print("preflight ok", flush=True)

        # 2. First sync: everything, inserted rather than updated.
        seen: list[dict] = []
        report = firebirddelta.sync_tables(
            connection_string, output_uri, TABLES, progress=seen.append
        )
        check(report.total_rows_fetched == 9, f"expected 9 rows, got {report.total_rows_fetched}")
        check(len(seen) == 2, f"progress should fire once per table, got {len(seen)}")
        check(seen[-1]["tables_done"] == 2, "progress should count tables done")
        for stats in report.tables:
            check(stats.rows_updated == 0, f"{stats.table}: a first sync updates nothing")
            check(
                stats.rows_inserted == stats.rows_fetched,
                f"{stats.table}: every fetched row should insert on a first sync",
            )
        print(f"first sync ok: {report!r}", flush=True)

        # 3. The committed Delta table must hold the real values, not just the schema.
        #    Compared against what the source actually holds right now rather than
        #    against hardcoded fixture values, since this script mutates CUSTOMERS in
        #    step 5 and a hardcoded expectation would fail the second time this script
        #    runs against state the first run left behind.
        source = dict(query("SELECT ID, NAME FROM CUSTOMERS"))
        table = DeltaTable(f"{output}/CUSTOMERS").to_pyarrow_table()
        check(
            table.num_rows == len(source),
            f"expected {len(source)} committed rows, got {table.num_rows}",
        )
        by_id = dict(zip(table.column("ID").to_pylist(), table.column("NAME").to_pylist()))
        check(by_id == source, f"delta disagrees with the source: {by_id} vs {source}")
        print("delta contents ok", flush=True)

        # 4. Second sync: nothing changed, so nothing is re-fetched.
        report = firebirddelta.sync_tables(connection_string, output_uri, TABLES)
        check(
            report.total_rows_fetched == 0,
            f"an unchanged source must not be re-fetched, got {report.total_rows_fetched}",
        )
        print("incremental no-op ok", flush=True)

        # 5. Change exactly one row; exactly one row should come back. The new
        #    watermark is derived from the table's current maximum rather than
        #    hardcoded, so this works however many times the script has run before.
        marker = f"Alice {os.getpid()}"
        execute(
            "UPDATE CUSTOMERS SET NAME = '"
            + marker.replace("'", "''")
            + "', UPDATED_AT = (SELECT MAX(UPDATED_AT) FROM CUSTOMERS) + 1 WHERE ID = 1"
        )

        report = firebirddelta.sync_tables(connection_string, output_uri, TABLES)
        check(
            report.total_rows_fetched == 1,
            f"exactly one row changed, got {report.total_rows_fetched}",
        )
        changed = next(t for t in report.tables if t.table == "CUSTOMERS")
        check(changed.rows_updated == 1, f"the row should update in place, got {changed!r}")
        check(changed.rows_inserted == 0, f"a merge must not duplicate, got {changed!r}")

        source = dict(query("SELECT ID, NAME FROM CUSTOMERS"))
        table = DeltaTable(f"{output}/CUSTOMERS").to_pyarrow_table()
        check(
            table.num_rows == len(source),
            f"an upsert must not add a row: {table.num_rows} vs {len(source)}",
        )
        by_id = dict(zip(table.column("ID").to_pylist(), table.column("NAME").to_pylist()))
        check(by_id.get(1) == marker, f"row 1 should have updated, got {by_id.get(1)!r}")
        check(by_id == source, f"delta should still match the source: {by_id} vs {source}")
        print("incremental update ok", flush=True)

        print("smoke test passed", flush=True)
        return 0
    finally:
        shutil.rmtree(output, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
