"""Stream a live Firebird database incrementally into Delta Lake tables.

The public surface is :func:`sync_tables`, which connects to Firebird once, and for
each configured table pulls only the rows new or changed since that table's last
successful run, merging them into a Delta table keyed on its primary key.
:func:`preflight` runs the same catalog and configuration checks against the live source
without writing anything, for a cheap check before pointing a real sync at an unfamiliar
schema.

:func:`source_watermark` and :func:`set_checkpoint` are the two halves of a bulk backfill,
for a table too large to seed a row at a time: capture the watermark, load the data by
whatever bulk means is fastest, record the checkpoint, and incremental sync takes over.

``firebirddelta.distributed`` spreads a sync across Spark executors, so throughput scales
with workers rather than code. It imports ``pyspark`` lazily and is not needed otherwise.
"""

from ._firebirddelta import (
    ColumnPreflight,
    ConcurrentWriteError,
    SyncReport,
    TablePreflight,
    TableSyncStats,
    preflight,
    set_checkpoint,
    source_watermark,
    sync_tables,
)

__all__ = [
    "sync_tables",
    "preflight",
    "source_watermark",
    "set_checkpoint",
    "SyncReport",
    "TableSyncStats",
    "TablePreflight",
    "ColumnPreflight",
    "ConcurrentWriteError",
]
__version__ = "0.1.0"
