"""Tests for flat-file-specific cost estimation behaviour."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from catalog.schema_cache import ColumnMeta, TableMeta
from engine.cost_estimator import CostEstimator


def _make_config(max_scan_bytes=10 * 1024**3, warn=0.10, block=1.00):
    cfg = MagicMock()
    cfg.engine.duckdb_max_scan_bytes = max_scan_bytes
    cfg.cost_gates.warn_threshold_usd = warn
    cfg.cost_gates.block_threshold_usd = block
    return cfg


def _make_cache(meta: TableMeta):
    cache = MagicMock()
    cache.get.return_value = meta
    return cache


def _make_meta(
    fmt="csv", total_bytes=1_000_000, total_files=10, bytes_per_row_estimate=100.0
):
    return TableMeta(
        table_name="test",
        s3_path="s3://bucket/test/",
        format=fmt,
        columns=[
            ColumnMeta(name="id", dtype="BIGINT", estimated_bytes_per_row=8.0),
            ColumnMeta(name="name", dtype="VARCHAR", estimated_bytes_per_row=32.0),
        ],
        partition_columns=[],
        total_rows=0,
        total_bytes=total_bytes,
        total_files=total_files,
        total_partitions=1,
        avg_row_groups_per_file=4,
        last_refreshed=datetime.now(tz=timezone.utc),
        bytes_per_row_estimate=bytes_per_row_estimate,
    )


class TestFlatFileColumnFraction:
    @pytest.mark.parametrize("fmt", ["csv", "json", "ndjson", "txt"])
    def test_col_fraction_always_one_for_flat_files(self, fmt):
        meta = _make_meta(fmt=fmt)
        cfg = _make_config()
        estimator = CostEstimator(cfg, _make_cache(meta))

        # SELECT only one of the two columns — fraction should still be 1.0
        estimate = estimator.estimate("test", "SELECT id FROM t")
        assert estimate.column_filter_fraction == 1.0

    def test_col_fraction_respects_columns_for_parquet(self):
        meta = _make_meta(fmt="parquet")
        cfg = _make_config()
        estimator = CostEstimator(cfg, _make_cache(meta))

        estimate = estimator.estimate("test", "SELECT id FROM t")
        # id = 8 bytes out of 8+32 = 40 total → ~0.2
        assert estimate.column_filter_fraction < 1.0


class TestFlatFileAvgRowGroups:
    @pytest.mark.parametrize("fmt", ["csv", "json", "ndjson", "txt"])
    def test_s3_gets_equals_file_count_for_flat_files(self, fmt):
        meta = _make_meta(fmt=fmt, total_files=5)
        cfg = _make_config()
        estimator = CostEstimator(cfg, _make_cache(meta))

        estimate = estimator.estimate("test", "SELECT id, name FROM t")
        # avg_row_groups=1 → s3_gets = files * 1 = 5
        assert estimate.s3_get_requests == 5

    def test_s3_gets_uses_row_groups_for_parquet(self):
        meta = _make_meta(fmt="parquet", total_files=5)
        meta.avg_row_groups_per_file = 4
        cfg = _make_config()
        estimator = CostEstimator(cfg, _make_cache(meta))

        estimate = estimator.estimate("test", "SELECT id FROM t")
        # avg_row_groups=4 → s3_gets = 5 * 4 = 20
        assert estimate.s3_get_requests == 20


class TestFlatFileEstimatedBytes:
    def test_estimated_bytes_equals_total_bytes_no_partitions(self):
        meta = _make_meta(fmt="csv", total_bytes=500_000, total_files=3)
        cfg = _make_config()
        estimator = CostEstimator(cfg, _make_cache(meta))

        estimate = estimator.estimate("test", "SELECT id, name FROM t")
        # col_fraction=1.0, partition_fraction=1.0 → estimated = total_bytes
        assert estimate.estimated_bytes == 500_000

    def test_engine_recommendation_duckdb_for_small_files(self):
        meta = _make_meta(fmt="csv", total_bytes=1_000_000)
        cfg = _make_config(max_scan_bytes=10 * 1024**3)
        estimator = CostEstimator(cfg, _make_cache(meta))

        estimate = estimator.estimate("test", "SELECT id FROM t")
        assert estimate.recommended_engine == "duckdb"

    def test_engine_recommendation_athena_for_large_files(self):
        meta = _make_meta(fmt="ndjson", total_bytes=20 * 1024**3)  # 20 GB
        cfg = _make_config(max_scan_bytes=10 * 1024**3)
        estimator = CostEstimator(cfg, _make_cache(meta))

        estimate = estimator.estimate("test", "SELECT id FROM t")
        assert estimate.recommended_engine == "athena"
