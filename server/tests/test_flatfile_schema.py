"""Tests for DuckDBEngine.get_flat_file_schema."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from engine.duckdb_engine import DuckDBEngine, QueryError


def _make_engine():
    cfg = MagicMock()
    cfg.aws.region = "us-east-1"
    cfg.aws.access_key_id = ""
    cfg.aws.secret_access_key = ""
    cfg.engine.default_row_limit = 1000
    cfg.engine.query_timeout_seconds = 120
    with patch("engine.duckdb_engine.duckdb") as mock_duckdb:
        con = MagicMock()
        mock_duckdb.connect.return_value = con
        engine = DuckDBEngine.__new__(DuckDBEngine)
        engine.config = cfg
        engine._con = con
        return engine, con


class TestGetFlatFileSchema:
    def test_txt_returns_hardcoded_schema_without_querying(self):
        engine, con = _make_engine()
        cols, bpr = engine.get_flat_file_schema("s3://bucket/path/", "txt")
        assert cols == [{"name": "line", "type": "VARCHAR"}]
        assert bpr == 80.0
        con.execute.assert_not_called()

    def test_csv_calls_describe_and_sample(self):
        engine, con = _make_engine()
        # DESCRIBE result: two columns
        describe_rel = MagicMock()
        describe_rel.fetchall.return_value = [("id", "INTEGER"), ("name", "VARCHAR")]
        # Sample result: 100 rows, 5000 total bytes
        sample_rel = MagicMock()
        sample_rel.fetchone.return_value = (100, 5000)

        con.execute.side_effect = [describe_rel, sample_rel]
        cols, bpr = engine.get_flat_file_schema("s3://bucket/path/", "csv")

        assert cols == [
            {"name": "id", "type": "INTEGER"},
            {"name": "name", "type": "VARCHAR"},
        ]
        assert bpr == pytest.approx(50.0)
        assert con.execute.call_count == 2
        # First call should be a DESCRIBE
        first_sql = con.execute.call_args_list[0][0][0]
        assert "DESCRIBE" in first_sql
        assert "read_csv" in first_sql
        assert "**/*.csv" in first_sql

    def test_json_uses_read_json(self):
        engine, con = _make_engine()
        describe_rel = MagicMock()
        describe_rel.fetchall.return_value = [("event", "VARCHAR")]
        sample_rel = MagicMock()
        sample_rel.fetchone.return_value = (50, 3000)

        con.execute.side_effect = [describe_rel, sample_rel]
        cols, bpr = engine.get_flat_file_schema("s3://bucket/path/", "json")

        first_sql = con.execute.call_args_list[0][0][0]
        assert "read_json" in first_sql
        assert "**/*.json" in first_sql
        assert "format='auto'" in first_sql

    def test_ndjson_uses_newline_delimited(self):
        engine, con = _make_engine()
        describe_rel = MagicMock()
        describe_rel.fetchall.return_value = [("ts", "TIMESTAMP")]
        sample_rel = MagicMock()
        sample_rel.fetchone.return_value = (200, 8000)

        con.execute.side_effect = [describe_rel, sample_rel]
        engine.get_flat_file_schema("s3://bucket/path/", "ndjson")

        first_sql = con.execute.call_args_list[0][0][0]
        assert "newline_delimited" in first_sql

    def test_csv_respects_delimiter(self):
        engine, con = _make_engine()
        describe_rel = MagicMock()
        describe_rel.fetchall.return_value = [("col", "VARCHAR")]
        sample_rel = MagicMock()
        sample_rel.fetchone.return_value = (10, 100)

        con.execute.side_effect = [describe_rel, sample_rel]
        engine.get_flat_file_schema("s3://bucket/path/", "csv", delimiter="|")

        first_sql = con.execute.call_args_list[0][0][0]
        assert "delim='|'" in first_sql

    def test_glob_pattern_override(self):
        engine, con = _make_engine()
        describe_rel = MagicMock()
        describe_rel.fetchall.return_value = [("col", "VARCHAR")]
        sample_rel = MagicMock()
        sample_rel.fetchone.return_value = (10, 500)

        con.execute.side_effect = [describe_rel, sample_rel]
        engine.get_flat_file_schema(
            "s3://bucket/path/", "csv", glob_pattern="data/*.csv"
        )

        first_sql = con.execute.call_args_list[0][0][0]
        assert "data/*.csv" in first_sql

    def test_sample_zero_rows_returns_fallback_bpr(self):
        engine, con = _make_engine()
        describe_rel = MagicMock()
        describe_rel.fetchall.return_value = [("col", "VARCHAR")]
        sample_rel = MagicMock()
        sample_rel.fetchone.return_value = (0, 0)

        con.execute.side_effect = [describe_rel, sample_rel]
        _, bpr = engine.get_flat_file_schema("s3://bucket/path/", "csv")

        assert bpr == 200.0

    def test_sample_error_returns_fallback_bpr(self):
        import duckdb

        engine, con = _make_engine()
        describe_rel = MagicMock()
        describe_rel.fetchall.return_value = [("col", "VARCHAR")]

        con.execute.side_effect = [describe_rel, duckdb.Error("network error")]
        _, bpr = engine.get_flat_file_schema("s3://bucket/path/", "csv")

        assert bpr == 200.0

    def test_describe_error_raises_query_error(self):
        import duckdb

        engine, con = _make_engine()
        con.execute.side_effect = duckdb.Error("access denied")

        with pytest.raises(QueryError):
            engine.get_flat_file_schema("s3://bucket/path/", "csv")
