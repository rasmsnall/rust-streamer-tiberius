"""Distribute a sync across Spark executors, so throughput scales with workers.

The sync engine is unchanged and unaware of Spark. This module only decides which worker
syncs which tables, which is possible because the engine already makes every table
independent: no cross-table transaction, one Delta table per source table, and since
checkpoints were unshared, one checkpoint Delta table per source table too. No two
workers ever write the same Delta table, so there is nothing to coordinate and no lock to
take.

The practical consequence is the point of the module: **to go faster, add workers.** The
same code, the same configuration, a bigger cluster.

``pyspark`` is imported lazily and is not a dependency of the package. Importing
``firebirddelta`` does not require Spark; importing ``firebirddelta.distributed`` and
calling it does.

Usage::

    from firebirddelta.distributed import sync_tables_distributed

    report = sync_tables_distributed(
        dbutils.secrets.get(scope="prod", key="firebird_connection_string"),
        "/Volumes/main/raw/firebird/",
        TABLES,
    )
    print(report["total_rows_fetched"], "rows across", len(report["tables"]), "tables")

Measure before reaching for this. See ``docs/operations.md`` for the single-node numbers
this crate has actually measured, and read its caveats: unlike the sibling
``tiberiusdelta`` crate, this one has not yet been measured against a real Firebird
instance at all (see ``CLAUDE.md``'s Open items), so treat any throughput expectation
here as unverified until it has been.
"""

from __future__ import annotations

from typing import Any

__all__ = ["sync_tables_distributed", "plan_partitions"]


def plan_partitions(table_count: int, default_parallelism: int, requested: int | None) -> int:
    """Chooses how many Spark partitions to split `table_count` tables into.

    More partitions than executor cores, deliberately. Tables differ enormously in how
    long they take, and Spark hands a free executor the next waiting partition, so
    oversubscribing lets it balance the load instead of leaving workers idle behind one
    slow partition. Fewer partitions than tables, also deliberately: every table in a
    partition is synced over one shared connection, so a partition of several tables pays
    one Firebird login rather than one each.

    Never more partitions than there are tables, since an empty partition is a scheduled
    task that does nothing.

    :param table_count: How many tables are being synced.
    :param default_parallelism: ``sparkContext.defaultParallelism``, roughly the cluster's
        total executor cores.
    :param requested: An explicit override, or ``None`` to choose.
    :returns: The partition count, always at least 1 when there is any table at all.
    """
    if table_count <= 0:
        return 0
    if requested is not None:
        if requested < 1:
            raise ValueError("num_partitions must be at least 1")
        return min(requested, table_count)
    return max(1, min(table_count, 4 * max(1, default_parallelism)))


def _partition_worker(connection_string: str, output_uri: str, sync_kwargs: dict[str, Any]):
    """Builds the function each executor runs over its share of the tables.

    Returned as a closure over plain strings and dicts only, because Spark pickles it and
    ships it to the executors; anything unpicklable captured here would fail at submit
    time rather than obviously.

    The results are converted to plain dicts inside the worker for the same reason: the
    report objects the extension returns are native classes, not picklable, and would not
    survive the trip back to the driver.
    """

    def run(partition):
        import firebirddelta

        tables = list(partition)
        if not tables:
            return []
        try:
            report = firebirddelta.sync_tables(
                connection_string, output_uri, tables, **sync_kwargs
            )
        except BaseException as exc:  # noqa: BLE001 - reported, not swallowed; see below
            # One partition's failure must not fail the whole job. Every table is synced
            # and checkpointed independently, so the tables that did complete keep their
            # progress and the rest are simply picked up by the next run; a Spark-level
            # abort would throw away the other partitions' work for no reason.
            #
            # The error already names the table it happened on, because the engine
            # annotates every error with one, so attribution survives even though the
            # whole partition stopped at that point.
            return [
                {
                    "tables": [t["table"] for t in tables],
                    "error": f"{type(exc).__name__}: {exc}",
                }
            ]

        return [
            {
                "table": s.table,
                "rows_fetched": s.rows_fetched,
                "rows_inserted": s.rows_inserted,
                "rows_updated": s.rows_updated,
                "text_fallback_columns": list(s.text_fallback_columns),
                "excluded_columns": list(s.excluded_columns),
                "error": None,
            }
            for s in report.tables
        ]

    return run


def sync_tables_distributed(
    connection_string: str,
    output_uri: str,
    tables,
    *,
    spark=None,
    num_partitions: int | None = None,
    raise_on_error: bool = True,
    **sync_kwargs,
) -> dict[str, Any]:
    """Sync every configured table, spread across the cluster's executors.

    Each table is synced exactly as :func:`firebirddelta.sync_tables` would sync it. The
    only difference is which machine does it.

    Spark retries a failed task by default, which is safe here without any special
    handling: a sync is idempotent, so a retried table re-applies the same rows rather
    than duplicating them.

    :param connection_string: As for :func:`firebirddelta.sync_tables`. Note that this is
        shipped to every executor, so the password travels within the cluster; that is
        inherent to distributing the work and is why the cluster should be one you trust
        with it.
    :param output_uri: As for :func:`firebirddelta.sync_tables`.
    :param tables: One table configuration per table, as for
        :func:`firebirddelta.sync_tables`.
    :param spark: The ``SparkSession`` to use. Defaults to the active one, which is what a
        Databricks notebook or job already has.
    :param num_partitions: How many partitions to split the tables into. Defaults to a
        value derived from the cluster's parallelism; see :func:`plan_partitions`.
    :param raise_on_error: Raise if any partition failed, after collecting every result.
        Set ``False`` to inspect the failures yourself and decide.
    :param sync_kwargs: Passed through to :func:`firebirddelta.sync_tables`, for example
        ``fetch_batch_size`` or ``query_timeout_sec``. ``progress`` is not supported here,
        because a callback cannot be shipped to an executor and called back on the driver.

    :returns: A dict with ``tables`` (per-table result dicts), ``failures`` (one entry per
        failed partition, naming the tables it covered and the error), and
        ``total_rows_fetched``.

    :raises RuntimeError: If any partition failed and ``raise_on_error`` is true.
    :raises ValueError: If ``progress`` is passed, or ``num_partitions`` is below 1.
    """
    if "progress" in sync_kwargs:
        raise ValueError(
            "progress is not supported for a distributed sync: the callback would have to "
            "run on the driver while the work runs on executors. Read the returned report "
            "instead, or use firebirddelta.sync_tables for a single-node run."
        )

    tables = list(tables)
    if not tables:
        return {"tables": [], "failures": [], "total_rows_fetched": 0}

    if spark is None:
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is None:
            raise RuntimeError(
                "no active SparkSession; pass spark=... explicitly, or use "
                "firebirddelta.sync_tables for a single-node run"
            )

    context = spark.sparkContext
    partitions = plan_partitions(len(tables), context.defaultParallelism, num_partitions)
    worker = _partition_worker(connection_string, output_uri, sync_kwargs)

    collected = context.parallelize(tables, partitions).mapPartitions(worker).collect()

    results = [r for r in collected if r.get("error") is None]
    failures = [r for r in collected if r.get("error") is not None]
    report = {
        "tables": results,
        "failures": failures,
        "total_rows_fetched": sum(r["rows_fetched"] for r in results),
    }

    if failures and raise_on_error:
        detail = "; ".join(f"{', '.join(f['tables'])}: {f['error']}" for f in failures)
        raise RuntimeError(
            f"{len(failures)} of {partitions} partitions failed. Tables that did complete "
            f"kept their checkpoints, so re-running syncs only what is outstanding. {detail}"
        )
    return report
