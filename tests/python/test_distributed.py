"""Tests for the distribution wrapper's own logic, without Spark or a database.

What is worth testing here is the part that is easy to get quietly wrong: how tables are
split into partitions, and how a failing partition is reported rather than swallowed.
Running an actual Spark job is not, since that tests Spark.

Run with::

    python -m pytest tests/python -q
"""

from __future__ import annotations

import pytest

from tiberiusdelta.distributed import _partition_worker, plan_partitions


class TestPlanPartitions:
    def test_no_tables_needs_no_partitions(self):
        assert plan_partitions(0, 8, None) == 0

    def test_never_more_partitions_than_tables(self):
        # An empty partition is a scheduled task that does nothing.
        assert plan_partitions(3, 64, None) == 3
        assert plan_partitions(3, 64, 100) == 3

    def test_oversubscribes_the_cluster_to_let_spark_balance(self):
        # Tables differ enormously in duration, so more partitions than cores lets a free
        # executor take the next one instead of idling behind a slow partition.
        assert plan_partitions(1000, 8, None) == 32

    def test_groups_tables_so_a_partition_shares_one_connection(self):
        # Fewer partitions than tables: each partition is one TDS login for several
        # tables, rather than one login each.
        assert plan_partitions(1000, 8, None) < 1000

    def test_an_explicit_request_is_honoured(self):
        assert plan_partitions(100, 8, 4) == 4

    def test_a_nonsense_request_is_rejected(self):
        with pytest.raises(ValueError):
            plan_partitions(10, 8, 0)

    def test_a_cluster_reporting_no_parallelism_still_gets_a_partition(self):
        assert plan_partitions(5, 0, None) >= 1


class TestPartitionWorker:
    """The worker runs on an executor, so it must return picklable plain data and must
    not let one partition's failure abort the whole job."""

    def test_an_empty_partition_does_no_work(self, monkeypatch):
        called = []
        monkeypatch.setattr(
            "tiberiusdelta.sync_tables", lambda *a, **k: called.append(a), raising=False
        )
        worker = _partition_worker("Server=x", "file:///out", {})
        assert worker(iter([])) == []
        assert called == []

    def test_results_come_back_as_plain_dicts(self, monkeypatch):
        # The report objects the extension returns are native classes and are not
        # picklable, so they would not survive the trip back to the driver.
        class Stats:
            table = "dbo.customers"
            rows_fetched = 7
            rows_inserted = 5
            rows_updated = 2
            text_fallback_columns = ["shape"]

        class Report:
            tables = [Stats()]

        monkeypatch.setattr(
            "tiberiusdelta.sync_tables", lambda *a, **k: Report(), raising=False
        )
        worker = _partition_worker("Server=x", "file:///out", {})
        out = worker(iter([{"table": "dbo.customers"}]))
        assert out == [
            {
                "table": "dbo.customers",
                "rows_fetched": 7,
                "rows_inserted": 5,
                "rows_updated": 2,
                "text_fallback_columns": ["shape"],
                "error": None,
            }
        ]

    def test_a_failing_partition_is_reported_not_raised(self, monkeypatch):
        # A raise here would fail the Spark job and throw away every other partition's
        # work, for a failure that only affects this partition's tables.
        def boom(*_a, **_k):
            raise ValueError("on table dbo.invoices: query error")

        monkeypatch.setattr("tiberiusdelta.sync_tables", boom, raising=False)
        worker = _partition_worker("Server=x", "file:///out", {})
        out = worker(iter([{"table": "dbo.invoices"}, {"table": "dbo.lines"}]))

        assert len(out) == 1
        assert out[0]["tables"] == ["dbo.invoices", "dbo.lines"]
        assert "dbo.invoices" in out[0]["error"], "the error names the table that failed"
        assert out[0]["error"].startswith("ValueError")
