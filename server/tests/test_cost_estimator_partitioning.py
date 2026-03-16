"""Tests for engine.cost_estimator partition pruning logic."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from engine.cost_estimator import CostEstimator, _has_partition_filter
from catalog.schema_cache import SchemaCache, TableMeta, ColumnMeta, PartitionMeta


@pytest.fixture
def mock_cache():
    return MagicMock(spec=SchemaCache)


@pytest.fixture
def mock_config():
    cfg = MagicMock()
    cfg.engine.duckdb_max_scan_bytes = 10 * 1024**3
    cfg.cost_gates.warn_threshold_usd = 0.10
    cfg.cost_gates.block_threshold_usd = 1.00
    return cfg


def test_has_partition_filter_basic_equality():
    partition_cols = ["dt", "region"]
    sql = "SELECT * FROM orders WHERE dt = '2025-01-01'"
    assert _has_partition_filter(sql, partition_cols) is True


def test_has_partition_filter_case_insensitive():
    partition_cols = ["dt"]
    sql = "SELECT * FROM orders WHERE DT = '2025-01-01'"
    assert _has_partition_filter(sql, partition_cols) is True


def test_has_partition_filter_multiple_columns():
    partition_cols = ["dt", "region"]
    sql = "SELECT * FROM orders WHERE region = 'US' AND amount > 100"
    assert _has_partition_filter(sql, partition_cols) is True


def test_has_partition_filter_range():
    partition_cols = ["dt"]
    sql = "SELECT * FROM orders WHERE dt >= '2025-01-01' AND dt <= '2025-01-31'"
    assert _has_partition_filter(sql, partition_cols) is True


def test_has_partition_filter_no_where():
    partition_cols = ["dt"]
    sql = "SELECT * FROM orders"
    assert _has_partition_filter(sql, partition_cols) is False


def test_has_partition_filter_other_columns_only():
    partition_cols = ["dt"]
    sql = "SELECT * FROM orders WHERE customer_id = 123"
    assert _has_partition_filter(sql, partition_cols) is False


def test_has_partition_filter_in_clause():
    partition_cols = ["region"]
    sql = "SELECT * FROM orders WHERE region IN ('US', 'EU')"
    assert _has_partition_filter(sql, partition_cols) is True


def test_has_partition_filter_or_condition():
    partition_cols = ["region"]
    sql = "SELECT * FROM orders WHERE region = 'US' OR customer_id = 1"
    assert _has_partition_filter(sql, partition_cols) is True


def test_has_partition_filter_complex_expression():
    partition_cols = ["dt"]
    # Testing if sqlglot handles complex expressions in WHERE
    sql = (
        "SELECT * FROM orders WHERE (dt = '2025-01-01' OR region = 'US') AND amount > 0"
    )
    assert _has_partition_filter(sql, partition_cols) is True


def test_has_partition_filter_fallback_string_search():
    # Test the fallback logic when sqlglot fails (by providing invalid SQL)
    partition_cols = ["dt"]
    invalid_sql = "SELECT * FROM orders WHERE dt = '2025-01-01' SOME INVALID SYNTAX"
    assert _has_partition_filter(invalid_sql, partition_cols) is True


def test_estimate_applies_partition_fraction(mock_config, mock_cache):
    estimator = CostEstimator(mock_config, mock_cache)

    meta = TableMeta(
        table_name="orders",
        s3_path="s3://bucket/orders/",
        format="parquet",
        columns=[ColumnMeta(name="id", dtype="BIGINT", estimated_bytes_per_row=8)],
        partition_columns=[PartitionMeta(name="dt", dtype="string")],
        total_rows=1000000,
        total_bytes=100 * 1024**2,  # 100 MB
        total_files=10,
        total_partitions=10,
        avg_row_groups_per_file=4,
        last_refreshed=datetime.now(tz=timezone.utc),
    )
    mock_cache.get.return_value = meta

    # Query WITHOUT partition filter
    sql_no_filter = "SELECT * FROM orders"
    est_no_filter = estimator.estimate("orders", sql_no_filter)

    # Query WITH partition filter
    sql_filter = "SELECT * FROM orders WHERE dt = '2025-01-01'"
    est_filter = estimator.estimate("orders", sql_filter)

    # partition_fraction is 0.1 in current implementation
    assert est_filter.estimated_bytes == int(est_no_filter.estimated_bytes * 0.1)
    assert est_filter.estimated_files == max(
        1, int(est_no_filter.estimated_files * 0.1)
    )
    assert est_filter.partition_filter_detected is True
    assert est_no_filter.partition_filter_detected is False
