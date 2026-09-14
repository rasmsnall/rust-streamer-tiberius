"""Apply the ``.devtest/*.sql`` fixtures to a Firebird instance.

The same baseline ``tests/firebird_live.rs`` expects: a database with ``CUSTOMERS``
(five rows, growing to seven after ``alter.sql``) and ``TYPE_ZOO`` (one row of values,
one of NULLs).

Uses ``firebird-driver`` (``pip install firebird-driver``), the official Python client.
Unlike this crate's own Rust connectivity (see ``CLAUDE.md``'s Dependency budget),
``firebird-driver`` wraps the native ``fbclient`` library and needs it installed on
whatever machine runs this script; that is an acceptable cost for a dev-only tool this
crate does not ship, the same way tiberiusdelta's own ``tools/seed.py`` needs
``pymssql``.

Usage::

    python tools/seed.py [host] [port] [path/to/test.fdb]

Defaults to ``127.0.0.1:3050`` and ``/firebird/data/firebirddelta_test.fdb``, matching a
local throwaway container (see ``CLAUDE.md``'s Environment notes). Reads the password
from ``FIREBIRD_PASSWORD`` (falling back to Firebird's own default ``masterkey``, which
is only appropriate for a throwaway local instance, never a real one).
"""

from __future__ import annotations

import os
import pathlib
import sys

import firebird.driver as fdb

FIXTURES = ("seed.sql", "alter.sql", "type_zoo.sql")
ROOT = pathlib.Path(__file__).resolve().parent.parent


def statements(script: str) -> list[str]:
    """Splits a fixture into individual statements on `;` line endings.

    Firebird has no `GO`-style batch separator the way T-SQL does (see
    tiberiusdelta's own `tools/seed.py`): every statement, DDL included, is valid to
    send on its own. This is a plain split on a `;` that ends a line, which is
    sufficient for these fixtures (none of them embeds a `;` inside a string literal)
    without needing a real SQL tokenizer.
    """
    out = []
    current: list[str] = []
    for line in script.splitlines():
        stripped = line.strip()
        if stripped.startswith("--") or not stripped:
            continue
        current.append(line)
        if stripped.endswith(";"):
            out.append("\n".join(current).rstrip(";").strip())
            current = []
    if current:
        out.append("\n".join(current).strip())
    return [s for s in out if s and s.upper() != "COMMIT"]


def main() -> int:
    host = sys.argv[1] if len(sys.argv) > 1 else "127.0.0.1"
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 3050
    db_path = sys.argv[3] if len(sys.argv) > 3 else "/firebird/data/firebirddelta_test.fdb"
    password = os.environ.get("FIREBIRD_PASSWORD", "masterkey")

    dsn = f"{host}/{port}:{db_path}"
    connection = fdb.connect(dsn, user="SYSDBA", password=password)
    cursor = connection.cursor()

    for name in FIXTURES:
        path = ROOT / ".devtest" / name
        print(f"applying {path.name}", flush=True)
        for statement in statements(path.read_text(encoding="utf-8")):
            cursor.execute(statement)
            connection.commit()

    cursor.execute("SELECT COUNT(*) FROM CUSTOMERS")
    customers = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM TYPE_ZOO")
    zoo = cursor.fetchone()[0]
    connection.close()

    print(f"seeded: customers={customers} type_zoo={zoo}", flush=True)
    if customers != 7 or zoo != 2:
        raise SystemExit("fixtures did not produce the expected baseline")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
