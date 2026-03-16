"""Tests for query helpers."""

from __future__ import annotations

from unittest.mock import MagicMock
from tools.query import _translate_to_athena, current_query_cost


def test_translate_to_athena():
    aws_cfg = MagicMock()
    aws_cfg.glue_database = "my_db"

    table_cfg = MagicMock()
    table_cfg.name = "my-table"

    # Test Parquet translation
    sql_pq = "SELECT * FROM read_parquet('s3://bucket/path/**/*.parquet', hive_partitioning=true) LIMIT 10"
    translated = _translate_to_athena(sql_pq, table_cfg, aws_cfg)
    assert translated == 'SELECT * FROM "my_db"."my_table" LIMIT 10'

    # Test Iceberg translation
    sql_ice = "SELECT count(*) FROM iceberg_scan('s3://bucket/iceberg/')"
    translated = _translate_to_athena(sql_ice, table_cfg, aws_cfg)
    assert translated == 'SELECT count(*) FROM "my_db"."my_table"'

    # Test CSV translation
    sql_csv = "SELECT * FROM read_csv('s3://bucket/data.csv', auto_detect=true)"
    translated = _translate_to_athena(sql_csv, table_cfg, aws_cfg)
    assert translated == 'SELECT * FROM "my_db"."my_table"'


def test_current_query_cost_contextvar():
    current_query_cost.set(7.89)
    assert current_query_cost.get() == 7.89
    current_query_cost.set(0.0)
    assert current_query_cost.get() == 0.0
