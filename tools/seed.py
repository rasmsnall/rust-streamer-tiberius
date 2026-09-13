"""Apply the ``.devtest/*.sql`` fixtures to a SQL Server instance.

The same baseline ``tests/mssql_live.rs`` expects: a ``tiberiusdelta_test`` database with
``dbo.customers`` (five rows) and ``dbo.type_zoo`` (one row of values, one of NULLs).

Exists because CI has no ``sqlcmd`` on the runner and the fixtures are batch scripts
separated by ``GO``, which is a client-side directive rather than T-SQL, so they have to
be split before being sent. Running this locally is optional: the Docker container path
in ``CLAUDE.md``'s Environment notes pipes the same files straight into ``sqlcmd``.

Usage::

    python tools/seed.py [host] [port]

Defaults to ``127.0.0.1:14330``, matching the local container and the CI service.
Reads the password from ``MSSQL_SA_PASSWORD``.
"""

from __future__ import annotations

import os
import pathlib
import sys
import time

import pymssql

FIXTURES = ("seed.sql", "alter.sql", "type_zoo.sql")
ROOT = pathlib.Path(__file__).resolve().parent.parent


def batches(script: str) -> list[str]:
    """Split a fixture on its ``GO`` separators.

    ``GO`` is a batch separator understood by sqlcmd, not a T-SQL statement, so a driver
    sending the whole file verbatim gets a syntax error. Only a line that is exactly
    ``GO`` separates; the token can legitimately appear inside a string literal.
    """
    out, current = [], []
    for line in script.splitlines():
        if line.strip().upper() == "GO":
            out.append("\n".join(current))
            current = []
        else:
            current.append(line)
    out.append("\n".join(current))
    return [b for b in (b.strip() for b in out) if b]


def connect(host: str, port: int, password: str, database: str = "master", attempts: int = 30):
    """Connect, waiting for the server to come up.

    A freshly started SQL Server container accepts TCP connections before it accepts
    logins, so a single attempt fails for reasons that are not a real error. Retries with
    a fixed delay rather than failing the job over a race with startup.
    """
    last = None
    for attempt in range(attempts):
        try:
            return pymssql.connect(
                server=host, port=str(port), user="sa", password=password, database=database
            )
        except Exception as exc:  # noqa: BLE001 - any driver error here means "not yet"
            last = exc
            if attempt == 0:
                print(f"waiting for {host}:{port} to accept logins", flush=True)
            time.sleep(2)
    raise SystemExit(f"could not connect to {host}:{port} after {attempts} attempts: {last}")


def main() -> int:
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 14330
    password = os.environ.get("MSSQL_SA_PASSWORD")
    if not password:
        raise SystemExit("set MSSQL_SA_PASSWORD to the instance's sa password")

    connection = connect(host, port, password)
    connection.autocommit(True)
    cursor = connection.cursor()

    for name in FIXTURES:
        path = ROOT / ".devtest" / name
        print(f"applying {path.name}", flush=True)
        for batch in batches(path.read_text(encoding="utf-8")):
            cursor.execute(batch)

    cursor.execute("SELECT COUNT(*) FROM tiberiusdelta_test.dbo.customers")
    customers = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM tiberiusdelta_test.dbo.type_zoo")
    zoo = cursor.fetchone()[0]
    connection.close()

    print(f"seeded: customers={customers} type_zoo={zoo}", flush=True)
    if customers != 5 or zoo != 2:
        raise SystemExit("fixtures did not produce the expected baseline")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
