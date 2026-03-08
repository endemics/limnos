"""Tests for catalog.result_cache — cache key, SQLite backend, DuckDB backend, factory."""

from __future__ import annotations

import sqlite3
import time
from datetime import datetime, timezone, timedelta

import pytest

from catalog.result_cache import (
    make_cache_key,
    make_result_cache,
    SQLiteResultCache,
    DuckDBResultCache,
)


# ── make_cache_key ─────────────────────────────────────────────────────────────

class TestMakeCacheKey:
    def test_deterministic(self):
        k1 = make_cache_key("orders", "SELECT count(*) FROM t", 1000)
        k2 = make_cache_key("orders", "SELECT count(*) FROM t", 1000)
        assert k1 == k2

    def test_normalizes_whitespace(self):
        k1 = make_cache_key("orders", "SELECT  count(*)  FROM   t", 1000)
        k2 = make_cache_key("orders", "SELECT count(*) FROM t", 1000)
        assert k1 == k2

    def test_normalizes_case(self):
        k1 = make_cache_key("orders", "SELECT COUNT(*) FROM T", 1000)
        k2 = make_cache_key("orders", "select count(*) from t", 1000)
        assert k1 == k2

    def test_strips_trailing_semicolon(self):
        k1 = make_cache_key("orders", "SELECT 1;", 1000)
        k2 = make_cache_key("orders", "SELECT 1", 1000)
        assert k1 == k2

    def test_different_row_limit_gives_different_key(self):
        k1 = make_cache_key("orders", "SELECT 1", 100)
        k2 = make_cache_key("orders", "SELECT 1", 500)
        assert k1 != k2

    def test_different_table_gives_different_key(self):
        k1 = make_cache_key("orders", "SELECT 1", 100)
        k2 = make_cache_key("events", "SELECT 1", 100)
        assert k1 != k2

    def test_returns_hex_string(self):
        key = make_cache_key("t", "SELECT 1", 10)
        assert len(key) == 64
        int(key, 16)  # must be valid hex


# ── Shared backend contract ────────────────────────────────────────────────────

def _run_backend_tests(cache):
    """Exercise the full ResultCacheBackend contract on any backend instance."""
    key = make_cache_key("sales", "SELECT 1", 100)

    # Miss before any put
    assert cache.get(key) is None

    # Hit after put
    cache.put(key, "sales", "SELECT 1", "## Results\n| id |\n|---|\n| 1 |", 1, ttl_seconds=3600)
    assert cache.get(key) == "## Results\n| id |\n|---|\n| 1 |"

    # Overwrite (INSERT OR REPLACE)
    cache.put(key, "sales", "SELECT 1", "## Updated", 1, ttl_seconds=3600)
    assert cache.get(key) == "## Updated"

    # invalidate_table removes only that table's entries
    key2 = make_cache_key("orders", "SELECT 2", 100)
    cache.put(key2, "orders", "SELECT 2", "## Orders", 5, ttl_seconds=3600)
    cache.invalidate_table("sales")
    assert cache.get(key) is None      # gone
    assert cache.get(key2) == "## Orders"  # untouched

    # close does not raise
    cache.close()


# ── SQLite backend ─────────────────────────────────────────────────────────────

class TestSQLiteResultCache:
    def test_basic_contract(self, tmp_path):
        cache = SQLiteResultCache(str(tmp_path / "test.db"), ttl_seconds=3600)
        _run_backend_tests(cache)

    def test_expired_entry_returns_none(self, tmp_path):
        db_path = str(tmp_path / "ttl.db")
        cache = SQLiteResultCache(db_path, ttl_seconds=3600)
        key = make_cache_key("t", "SELECT 1", 10)
        cache.put(key, "t", "SELECT 1", "response", 1, ttl_seconds=1)

        # Back-date cached_at so the entry appears expired
        past = (datetime.now(tz=timezone.utc) - timedelta(seconds=10)).isoformat()
        con = sqlite3.connect(db_path)
        con.execute("UPDATE query_results SET cached_at = ? WHERE cache_key = ?", (past, key))
        con.commit()
        con.close()

        assert cache.get(key) is None
        cache.close()

    def test_unexpired_entry_is_returned(self, tmp_path):
        cache = SQLiteResultCache(str(tmp_path / "test.db"), ttl_seconds=3600)
        key = make_cache_key("t", "SELECT 1", 10)
        cache.put(key, "t", "SELECT 1", "fresh", 1, ttl_seconds=3600)
        assert cache.get(key) == "fresh"
        cache.close()

    def test_multiple_tables_invalidated_independently(self, tmp_path):
        cache = SQLiteResultCache(str(tmp_path / "test.db"), ttl_seconds=3600)
        k_a = make_cache_key("a", "SELECT 1", 10)
        k_b = make_cache_key("b", "SELECT 1", 10)
        cache.put(k_a, "a", "SELECT 1", "resp-a", 1, ttl_seconds=3600)
        cache.put(k_b, "b", "SELECT 1", "resp-b", 1, ttl_seconds=3600)
        cache.invalidate_table("a")
        assert cache.get(k_a) is None
        assert cache.get(k_b) == "resp-b"
        cache.close()


# ── DuckDB backend ─────────────────────────────────────────────────────────────

class TestDuckDBResultCache:
    def test_basic_contract(self, tmp_path):
        cache = DuckDBResultCache(str(tmp_path / "test.duckdb"), ttl_seconds=3600)
        _run_backend_tests(cache)

    def test_expired_entry_returns_none(self, tmp_path):
        import duckdb
        db_path = str(tmp_path / "ttl.duckdb")
        cache = DuckDBResultCache(db_path, ttl_seconds=3600)
        key = make_cache_key("t", "SELECT 1", 10)
        cache.put(key, "t", "SELECT 1", "response", 1, ttl_seconds=1)
        cache.close()

        # Back-date cached_at so the entry appears expired
        past = (datetime.now(tz=timezone.utc) - timedelta(seconds=10)).isoformat()
        con = duckdb.connect(db_path)
        con.execute("UPDATE query_results SET cached_at = ? WHERE cache_key = ?", [past, key])
        con.close()

        cache2 = DuckDBResultCache(db_path, ttl_seconds=3600)
        assert cache2.get(key) is None
        cache2.close()

    def test_unexpired_entry_is_returned(self, tmp_path):
        cache = DuckDBResultCache(str(tmp_path / "test.duckdb"), ttl_seconds=3600)
        key = make_cache_key("t", "SELECT 1", 10)
        cache.put(key, "t", "SELECT 1", "fresh", 1, ttl_seconds=3600)
        assert cache.get(key) == "fresh"
        cache.close()

    def test_multiple_tables_invalidated_independently(self, tmp_path):
        cache = DuckDBResultCache(str(tmp_path / "test.duckdb"), ttl_seconds=3600)
        k_a = make_cache_key("a", "SELECT 1", 10)
        k_b = make_cache_key("b", "SELECT 1", 10)
        cache.put(k_a, "a", "SELECT 1", "resp-a", 1, ttl_seconds=3600)
        cache.put(k_b, "b", "SELECT 1", "resp-b", 1, ttl_seconds=3600)
        cache.invalidate_table("a")
        assert cache.get(k_a) is None
        assert cache.get(k_b) == "resp-b"
        cache.close()


# ── Factory ────────────────────────────────────────────────────────────────────

class TestMakeResultCache:
    def test_default_returns_sqlite(self, tmp_path):
        cache = make_result_cache(
            backend="sqlite",
            db_path=str(tmp_path / "s.db"),
            result_cache_db_path=str(tmp_path / "d.duckdb"),
            redis_url="redis://localhost:6379",
            ttl_seconds=3600,
        )
        assert isinstance(cache, SQLiteResultCache)
        cache.close()

    def test_unknown_backend_returns_sqlite(self, tmp_path):
        cache = make_result_cache(
            backend="unknown",
            db_path=str(tmp_path / "s.db"),
            result_cache_db_path=str(tmp_path / "d.duckdb"),
            redis_url="redis://localhost:6379",
            ttl_seconds=3600,
        )
        assert isinstance(cache, SQLiteResultCache)
        cache.close()

    def test_duckdb_backend(self, tmp_path):
        cache = make_result_cache(
            backend="duckdb",
            db_path=str(tmp_path / "s.db"),
            result_cache_db_path=str(tmp_path / "d.duckdb"),
            redis_url="redis://localhost:6379",
            ttl_seconds=3600,
        )
        assert isinstance(cache, DuckDBResultCache)
        cache.close()

    def test_redis_backend_raises_without_redis_py(self, tmp_path, monkeypatch):
        import builtins
        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "redis":
                raise ImportError("No module named 'redis'")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", mock_import)
        with pytest.raises(ImportError, match="redis-py"):
            make_result_cache(
                backend="redis",
                db_path=str(tmp_path / "s.db"),
                result_cache_db_path=str(tmp_path / "d.duckdb"),
                redis_url="redis://localhost:6379",
                ttl_seconds=3600,
            )
