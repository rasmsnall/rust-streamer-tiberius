"""Type stubs for the ``firebirddelta`` extension module.

A few assumptions are documented on :func:`sync_tables` and worth reading before use: a
zone-less Firebird ``TIMESTAMP`` is assumed to be UTC, a ``NUMERIC``/``DECIMAL`` column
is written as a 64-bit float because that is what actually arrives (not a true decimal),
and a row whose watermark value ties exactly with the current checkpoint is never
re-fetched.
"""

from collections.abc import Callable, Sequence
from typing import Any, TypedDict

__version__: str
__all__: list[str]

class TableConfig(TypedDict):
    """How one table is kept in sync."""

    table: str
    """Table name exactly as Firebird's own catalog names it (``RDB$RELATION_NAME``).
    Firebird has a single flat table namespace per database, no schema qualification, so
    this is always one identifier. An unquoted ``CREATE TABLE`` name is folded to upper
    case by Firebird itself, so this is typically all-caps. Whitespace and brackets are
    rejected rather than quoted."""

    watermark_column: str
    """Column filtered on: ``WHERE <watermark_column> > <last_synced_value>``. Must be
    monotonically non-decreasing with write order for incremental sync to be correct."""

    primary_key: str | Sequence[str]
    """Column, or columns, the merge matches existing rows on. A bare string is accepted
    as a single-column key."""

class ProgressEvent(TypedDict):
    """Payload passed to the ``progress`` callback after each table finishes."""

    table: str
    tables_done: int
    total_tables: int
    rows_fetched: int

class TableSyncStats:
    """What one table's sync produced. Instances are immutable."""

    @property
    def table(self) -> str:
        """Table name."""

    @property
    def rows_fetched(self) -> int:
        """Rows fetched from the source in this run: those new or changed since the last
        checkpoint, or every row on a first sync."""

    @property
    def rows_inserted(self) -> int:
        """Rows the merge inserted, because their key was not already present."""

    @property
    def rows_updated(self) -> int:
        """Rows the merge updated, because their key was already present."""

    @property
    def text_fallback_columns(self) -> list[str]:
        """Columns written as text because their Firebird type has no native mapping.
        Reported, not an error: type uncertainty degrades rather than failing a sync."""

    @property
    def excluded_columns(self) -> list[str]:
        """Columns excluded from the sync entirely: this library's Firebird client
        cannot read them at all, even as text (an exotic ``BLOB`` sub-type is the
        practical case). Distinct from ``text_fallback_columns``, whose columns are
        still synced, only degraded to a string."""

    def __repr__(self) -> str: ...

class SyncReport:
    """What one whole run produced. Instances are immutable."""

    @property
    def tables(self) -> list[TableSyncStats]:
        """Per-table results, in the order the tables were configured."""

    @property
    def total_rows_fetched(self) -> int:
        """Rows fetched from the source across every table."""

    def __repr__(self) -> str: ...

class ColumnPreflight:
    """One column, as :func:`preflight` reports it. Instances are immutable."""

    @property
    def name(self) -> str:
        """Column name, as the source catalog spells it."""

    @property
    def source_type(self) -> str:
        """The Firebird type name, as this library renders the catalog's type code."""

    @property
    def arrow_type(self) -> str | None:
        """The Arrow type this column would be written as, or ``None`` if it would be
        excluded from the sync entirely; see ``excluded``."""

    @property
    def recognised(self) -> bool:
        """False when this column would be written as text because its type is not
        mapped, or excluded outright. Not necessarily a failure; see
        ``TableSyncStats.text_fallback_columns``."""

    @property
    def excluded(self) -> bool:
        """True if this column cannot be synced at all."""

    def __repr__(self) -> str: ...

class TablePreflight:
    """What :func:`preflight` found for one table. Instances are immutable."""

    @property
    def table(self) -> str:
        """Table name, as configured."""

    @property
    def columns(self) -> list[ColumnPreflight]:
        """Every column the source catalog reports for this table, including excluded
        ones."""

    @property
    def watermark_present(self) -> bool:
        """False if the configured watermark column is missing or not selectable, which
        would fail the sync."""

    @property
    def missing_primary_key_columns(self) -> list[str]:
        """Configured primary key columns that are missing or not selectable. Non-empty
        means the sync would fail."""

    @property
    def last_synced_value(self) -> str | None:
        """The value this table has been synced up to, or ``None`` if it has never been
        synced."""

    @property
    def ready(self) -> bool:
        """True if the watermark and every primary key column are selectable, so a sync
        would run. An unrecognised-but-selectable column type does not make a table
        unready: it degrades to text by design."""

    def __repr__(self) -> str: ...

class ConcurrentWriteError(RuntimeError):
    """Another writer committed to the same Delta table at the same time.

    Delta's optimistic concurrency detected the conflict and refused the commit, so
    nothing is corrupted and no checkpoint advanced for the affected table. Re-running is
    safe and re-applies the same rows idempotently.

    Usually means two syncs overlapped: on Databricks, set the job's maximum concurrent
    runs to 1. Subclasses :class:`RuntimeError`, so a handler written before this type
    existed still catches it.
    """

    table: str
    """Name of the table whose commit lost the race."""

def source_watermark(
    connection_string: str,
    table: TableConfig,
    *,
    login_timeout_sec: int | None = 30,
    query_timeout_sec: int | None = 300,
) -> str | None:
    """Read a table's current greatest watermark value, without syncing anything.

    The first half of a bulk backfill, for a table too large to seed a row at a time. The
    data is loaded by whatever bulk means is fastest, and this library then takes over
    incrementally; for that handover to be correct, something has to record how far the
    bulk load got.

    .. warning::
       **Capture this before the export starts, not after.** A watermark read afterwards
       sits ahead of rows written while the export was running, and those rows are then
       skipped forever. Reading it first means such rows are merely re-fetched by the
       first incremental run, which is harmless because the merge is idempotent.

    :returns: The watermark rendered exactly as a checkpoint records it, or ``None`` if
        the table is empty, in which case an ordinary first sync is the right thing.

    :raises ConnectionError: The source could not be reached.
    :raises ValueError: The configuration is wrong, or the watermark column does not
        exist or cannot be selected.
    """

def set_checkpoint(
    output_uri: str,
    table: TableConfig,
    last_value: str,
    *,
    checkpoint_uri: str | None = None,
) -> None:
    """Record how far a table has been synced, without syncing anything.

    The second half of a bulk backfill: once the data is in the table's Delta table by
    whatever means, this hands over to incremental sync, which then fetches only what has
    changed since ``last_value``. Opens no connection to the source.

    :param last_value: What :func:`source_watermark` returned **before** the export ran.

    .. warning::
       This records a checkpoint for data it has not verified, which is the point and
       also the risk. A value ahead of what was actually loaded silently skips the rows in
       between and nothing will report it; a value behind is safe, costing only a
       re-fetch. When unsure, choose the earlier value.

    :raises ValueError: The configuration is wrong, or a name is unsafe to use in a path.
    :raises RuntimeError: The checkpoint could not be written.
    """

def sync_tables(
    connection_string: str,
    output_uri: str,
    tables: Sequence[TableConfig],
    *,
    checkpoint_uri: str | None = None,
    fetch_batch_size: int = 10_000,
    login_timeout_sec: int | None = 30,
    query_timeout_sec: int | None = 300,
    progress: Callable[[ProgressEvent], Any] | None = None,
) -> SyncReport:
    """Sync every configured table, pulling only what changed since its last run.

    Tables are synced sequentially over one connection. Each table's merge commits before
    its checkpoint advances, so an interruption leaves every already-synced table
    correctly checkpointed and the rest untouched, ready for the next run. Re-running is
    always safe: the merge is an upsert, so re-applying a row already merged updates it in
    place rather than duplicating it.

    :param connection_string: A ``firebird://user:pass@host:port/database`` connection
        URL. Pass it from a secret store, never hardcoded. It is scrubbed from every
        error message this module raises.
    :param output_uri: Prefix each table's Delta table is written beneath, for example
        ``CUSTOMERS`` lands at ``<output_uri>/CUSTOMERS``.
    :param tables: One :class:`TableConfig` per table. A table not listed is not synced:
        this library never guesses which tables exist or which columns are suitable.
    :param checkpoint_uri: Where the checkpoint table lives. Defaults to
        ``<output_uri>/_firebirddelta_checkpoints``.
    :param fetch_batch_size: Rows accumulated before each merge into Delta. Bounds peak
        memory; it is a safety limit, not a throughput knob to maximise blindly.
    :param login_timeout_sec: Seconds allowed to establish the connection. Does not
        cancel a login already in flight if it elapses; see ``docs/architecture.md``.
    :param query_timeout_sec: Seconds a table's sync may go without a completed batch of
        up to ``fetch_batch_size`` rows reaching the merge side before the sync gives up
        waiting for it. Coarser than a per-row deadline, and does not cancel the
        underlying fetch either; see ``docs/architecture.md``. ``None`` waits forever.
    :param progress: Called after each table finishes with a :class:`ProgressEvent`.
        Raising from it stops the run and the exception propagates.

    :returns: A :class:`SyncReport`.

    :raises ConnectionError: The source could not be reached, or the login was refused.
    :raises ValueError: The configuration is wrong: a missing key in a table entry, a
        missing watermark column or primary key, a duplicate table, a table that does not
        exist or the account cannot see, a watermark or primary key column that exists
        but cannot be read even as text, a name unsafe to interpolate into SQL, or a
        fetched value that contradicts its column's declared type.
    :raises RuntimeError: A Delta write, a checkpoint write, or an internal invariant
        failed.
    :raises KeyboardInterrupt: Ctrl-C was pressed between tables.

    .. note::
       Firebird's plain ``TIMESTAMP`` carries no timezone, and is written as Delta
       ``timestamp`` (microseconds UTC), so it is **assumed to already be UTC**.

    .. note::
       ``NUMERIC``/``DECIMAL`` columns are written as a 64-bit float, **not** an exact
       decimal: this library's chosen Firebird client decodes every scaled-integer
       column through a wire-level ``DOUBLE`` before this library ever sees the value, so
       the precision loss happens upstream of anything this library could do about it.
       Values whose unscaled integer form exceeds 2**53 can read back off by a cent or
       similar. Where exactness matters, ``CAST`` the column to ``VARCHAR`` in the source
       schema before configuring it here.

    .. warning::
       A row written with exactly the same watermark value as the current checkpoint is
       never re-fetched: the filter is strictly greater-than. Choose a watermark column
       precise enough, or strictly monotonic enough, that genuine ties cannot occur.

    .. warning::
       Deletions are not detected. A row removed from the source simply stops appearing,
       with no signal, and its copy in Delta remains.
    """

def preflight(
    connection_string: str,
    tables: Sequence[TableConfig],
    *,
    output_uri: str = "",
    checkpoint_uri: str | None = None,
    login_timeout_sec: int | None = 30,
    query_timeout_sec: int | None = 300,
) -> list[TablePreflight]:
    """Check the configuration against the live source without writing anything.

    Connects, reads each table's columns from Firebird's own
    ``RDB$RELATION_FIELDS``/``RDB$FIELDS`` catalog, and reports what a sync would do: the
    Arrow type each column would become, which columns would fall back to text or be
    excluded outright, whether the configured watermark and primary key columns actually
    exist and are selectable, and where each table's checkpoint stands. No Delta table is
    created and no checkpoint is advanced.

    :param connection_string: As for :func:`sync_tables`.
    :param tables: As for :func:`sync_tables`.
    :param output_uri: Only used to derive ``checkpoint_uri``. Pass the same value the
        real sync would use to have ``last_synced_value`` reported; leave it empty and no
        checkpoint table is read or derived at all, and every ``last_synced_value`` is
        ``None``.
    :param checkpoint_uri: Where the checkpoint table lives, if not derived from
        ``output_uri``.
    :param login_timeout_sec: As for :func:`sync_tables`.
    :param query_timeout_sec: As for :func:`sync_tables`.

    :returns: One :class:`TablePreflight` per configured table, in order.

    :raises ConnectionError: The source could not be reached, or the login was refused.
    :raises ValueError: The configuration is wrong, or a table does not exist.
    :raises RuntimeError: The checkpoint table exists but could not be read.
    """
