"""Stream a live SQL Server database incrementally into Delta Lake tables.

The public surface is :func:`sync_tables`, which connects to SQL Server once, and for
each configured table pulls only the rows new or changed since that table's last
successful run, merging them into a Delta table keyed on its primary key.
:func:`preflight` runs the same catalog and configuration checks against the live source
without writing anything, for a cheap check before pointing a real sync at an unfamiliar
schema.
"""

from ._tiberiusdelta import (
    ColumnPreflight,
    SyncReport,
    TablePreflight,
    TableSyncStats,
    preflight,
    sync_tables,
)

__all__ = [
    "sync_tables",
    "preflight",
    "SyncReport",
    "TableSyncStats",
    "TablePreflight",
    "ColumnPreflight",
]
__version__ = "0.1.0"
