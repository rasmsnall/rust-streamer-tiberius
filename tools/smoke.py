"""Round-trip a live table through the built wheel.

Runs the whole promise end to end against a real SQL Server and a real Delta table, using
the installed ``tiberiusdelta`` wheel rather than the Rust test binary: preflight, a first
sync that pulls everything, a second that pulls nothing, and a third that pulls exactly
the one row that changed. Reads the committed Delta table back with the ``deltalake``
package to check the values actually landed, not merely that nothing raised.

This is what makes the wheel job meaningful. A wheel that imports proves only that the
extension links; it says nothing about whether the code inside it works through the
Python surface, which is the surface anyone will actually use.

Expects ``tools/seed.py`` to have run first. Reads the password from
``MSSQL_SA_PASSWORD``.

Usage::

    python tools/smoke.py [host] [port]
"""

from __future__ import annotations

import os
import shutil
import sys
import tempfile

import pymssql
from deltalake import DeltaTable

import tiberiusdelta

TABLES = [
    {"table": "dbo.customers", "watermark_column": "updated_at", "primary_key": ["id"]},
    {"table": "dbo.type_zoo", "watermark_column": "updated_at", "primary_key": "id"},
]


HOST = PORT = PASSWORD = None


def source_connection():
    connection = pymssql.connect(
        server=HOST, port=PORT, user="sa", password=PASSWORD, database="tiberiusdelta_test"
    )
    connection.autocommit(True)
    return connection


def query(sql: str) -> list[tuple]:
    connection = source_connection()
    cursor = connection.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    connection.close()
    return rows


def execute(sql: str, params: tuple = ()) -> None:
    connection = source_connection()
    connection.cursor().execute(sql, params)
    connection.close()


def fail(message: str) -> None:
    raise SystemExit(f"smoke test failed: {message}")


def check(condition: bool, message: str) -> None:
    if not condition:
        fail(message)


def main() -> int:
    global HOST, PORT, PASSWORD
    host = HOST = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = PORT = sys.argv[2] if len(sys.argv) > 2 else "14330"
    password = PASSWORD = os.environ.get("MSSQL_SA_PASSWORD")
    if not password:
        raise SystemExit("set MSSQL_SA_PASSWORD to the instance's sa password")

    connection_string = (
        f"Server=tcp:{host},{port};Database=tiberiusdelta_test;"
        f"User Id=sa;Password={password};TrustServerCertificate=true"
    )

    output = tempfile.mkdtemp(prefix="tiberiusdelta-smoke-")
    output_uri = "file://" + output.replace("\\", "/")
    try:
        # 1. Preflight: every configured table must be ready, and the customers fixture's
        #    own types are all mapped, so nothing should fall back to text.
        found = tiberiusdelta.preflight(connection_string, TABLES, output_uri=output_uri)
        check(len(found) == 2, f"expected two tables, got {len(found)}")
        for table in found:
            check(table.ready, f"{table.table} is not ready: {table!r}")
            check(table.last_synced_value is None, f"{table.table} already has a checkpoint")
        customers = next(t for t in found if t.table == "dbo.customers")
        check(
            [c.name for c in customers.columns][:2] == ["id", "name"],
            "columns should come back in ordinal order",
        )
        check(
            all(c.recognised for c in customers.columns),
            "the customers fixture should map every column natively",
        )
        print("preflight ok", flush=True)

        # 2. First sync: everything, inserted rather than updated.
        seen: list[dict] = []
        report = tiberiusdelta.sync_tables(
            connection_string, output_uri, TABLES, progress=seen.append
        )
        check(report.total_rows_fetched == 7, f"expected 7 rows, got {report.total_rows_fetched}")
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
        #    Compared against what the source actually holds right now rather than against
        #    hardcoded fixture values: this script mutates dbo.customers in step 5, so a
        #    hardcoded expectation makes the second run of this script fail on state the
        #    first run left behind. Comparing to the source also tests the stronger and
        #    more useful property, that Delta matches SQL Server.
        source = dict(query("SELECT id, name FROM dbo.customers"))
        table = DeltaTable(f"{output}/dbo/customers").to_pyarrow_table()
        check(
            table.num_rows == len(source),
            f"expected {len(source)} committed rows, got {table.num_rows}",
        )
        by_id = dict(zip(table.column("id").to_pylist(), table.column("name").to_pylist()))
        check(by_id == source, f"delta disagrees with the source: {by_id} vs {source}")
        balances = dict(
            zip(table.column("id").to_pylist(), [str(b) for b in table.column("balance").to_pylist()])
        )
        source_balances = {
            i: str(b) for i, b in query("SELECT id, balance FROM dbo.customers")
        }
        check(
            balances == source_balances,
            f"decimals must be exact: {balances} vs {source_balances}",
        )
        print("delta contents ok", flush=True)

        # 4. Second sync: nothing changed, so nothing is re-fetched.
        report = tiberiusdelta.sync_tables(connection_string, output_uri, TABLES)
        check(
            report.total_rows_fetched == 0,
            f"an unchanged source must not be re-fetched, got {report.total_rows_fetched}",
        )
        print("incremental no-op ok", flush=True)

        # 5. Change exactly one row; exactly one row should come back. The new watermark
        #    is derived from the table's current maximum rather than hardcoded, so this
        #    works however many times the script has run before.
        marker = f"Alice {os.getpid()}"
        execute(
            "DECLARE @next DATETIME2 = "
            "DATEADD(day, 1, (SELECT MAX(updated_at) FROM dbo.customers)); "
            "UPDATE dbo.customers SET name = %s, updated_at = @next WHERE id = 1",
            (marker,),
        )

        report = tiberiusdelta.sync_tables(connection_string, output_uri, TABLES)
        check(
            report.total_rows_fetched == 1,
            f"exactly one row changed, got {report.total_rows_fetched}",
        )
        changed = next(t for t in report.tables if t.table == "dbo.customers")
        check(changed.rows_updated == 1, f"the row should update in place, got {changed!r}")
        check(changed.rows_inserted == 0, f"a merge must not duplicate, got {changed!r}")

        source = dict(query("SELECT id, name FROM dbo.customers"))
        table = DeltaTable(f"{output}/dbo/customers").to_pyarrow_table()
        check(
            table.num_rows == len(source),
            f"an upsert must not add a row: {table.num_rows} vs {len(source)}",
        )
        by_id = dict(zip(table.column("id").to_pylist(), table.column("name").to_pylist()))
        check(by_id.get(1) == marker, f"row 1 should have updated, got {by_id.get(1)!r}")
        check(by_id == source, f"delta should still match the source: {by_id} vs {source}")
        print("incremental update ok", flush=True)

        print("smoke test passed", flush=True)
        return 0
    finally:
        shutil.rmtree(output, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
