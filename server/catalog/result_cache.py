"""
Pluggable query result cache.

Caches the formatted Markdown response for a (table, sql, row_limit) triple
so repeated identical queries are served instantly without re-hitting S3.

Backends:
  sqlite  — same db file as SchemaCache; zero new infra; single-machine
  duckdb  — persistent .db file; survives reboots; columnar; single-machine
  redis   — shared across all workers on all machines; use for multi-node/prod

Cache key: SHA256(table_name | normalize(sql) | row_limit)
TTL:       configurable (default 1 h); lazy eviction for SQLite/DuckDB
Invalidation: call invalidate_table(table_name) after datalake_refresh_schema
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Optional


# ── Cache key ─────────────────────────────────────────────────────────────────

def make_cache_key(table: str, sql: str, row_limit: int) -> str:
    """Stable SHA-256 key from (table, normalised SQL, row_limit)."""
    normalized = re.sub(r"\s+", " ", sql.strip().rstrip(";").lower())
    payload = f"{table}|{normalized}|{row_limit}"
    return hashlib.sha256(payload.encode()).hexdigest()


# ── Abstract interface ─────────────────────────────────────────────────────────

class ResultCacheBackend(ABC):
    @abstractmethod
    def get(self, key: str) -> Optional[str]:
        """Return cached response string, or None on miss/expiry."""

    @abstractmethod
    def put(
        self,
        key: str,
        table_name: str,
        sql_executed: str,
        response: str,
        row_count: int,
        ttl_seconds: int,
    ) -> None:
        """Store a response. Overwrites any existing entry for key."""

    @abstractmethod
    def invalidate_table(self, table_name: str) -> None:
        """Delete all cached entries for a given table."""

    @abstractmethod
    def close(self) -> None:
        """Release resources."""


# ── SQLite backend ─────────────────────────────────────────────────────────────

_SQLITE_SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS query_results (
    cache_key    TEXT PRIMARY KEY,
    table_name   TEXT NOT NULL,
    sql_executed TEXT NOT NULL,
    response     TEXT NOT NULL,
    row_count    INTEGER NOT NULL DEFAULT 0,
    cached_at    TEXT NOT NULL,
    ttl_seconds  INTEGER NOT NULL DEFAULT 3600
);
CREATE INDEX IF NOT EXISTS idx_qr_table ON query_results(table_name);
"""


class SQLiteResultCache(ResultCacheBackend):
    """SQLite-backed result cache. Persistent, single-machine."""

    def __init__(self, db_path: str, ttl_seconds: int = 3600):
        self._ttl = ttl_seconds
        self._con = sqlite3.connect(db_path, check_same_thread=False)
        self._con.executescript(_SQLITE_SCHEMA)
        self._con.commit()

    def get(self, key: str) -> Optional[str]:
        row = self._con.execute(
            "SELECT response, cached_at, ttl_seconds FROM query_results WHERE cache_key = ?",
            (key,),
        ).fetchone()
        if not row:
            return None
        response, cached_at_str, ttl = row
        cached_at = datetime.fromisoformat(cached_at_str)
        age = (datetime.now(tz=timezone.utc) - cached_at.replace(tzinfo=timezone.utc)).total_seconds()
        if age > ttl:
            self._con.execute("DELETE FROM query_results WHERE cache_key = ?", (key,))
            self._con.commit()
            return None
        return response

    def put(self, key, table_name, sql_executed, response, row_count, ttl_seconds):
        now = datetime.now(tz=timezone.utc).isoformat()
        self._con.execute(
            """
            INSERT OR REPLACE INTO query_results
            (cache_key, table_name, sql_executed, response, row_count, cached_at, ttl_seconds)
            VALUES (?,?,?,?,?,?,?)
            """,
            (key, table_name, sql_executed, response, row_count, now, ttl_seconds),
        )
        self._con.commit()

    def invalidate_table(self, table_name: str) -> None:
        self._con.execute("DELETE FROM query_results WHERE table_name = ?", (table_name,))
        self._con.commit()

    def close(self) -> None:
        self._con.close()


# ── DuckDB backend ─────────────────────────────────────────────────────────────

_DUCKDB_SCHEMA = """
CREATE TABLE IF NOT EXISTS query_results (
    cache_key    VARCHAR PRIMARY KEY,
    table_name   VARCHAR NOT NULL,
    sql_executed VARCHAR NOT NULL,
    response     VARCHAR NOT NULL,
    row_count    INTEGER NOT NULL DEFAULT 0,
    cached_at    VARCHAR NOT NULL,
    ttl_seconds  INTEGER NOT NULL DEFAULT 3600
);
CREATE INDEX IF NOT EXISTS idx_qr_table ON query_results(table_name);
"""


class DuckDBResultCache(ResultCacheBackend):
    """DuckDB persistent file cache. Survives reboots. Single-machine."""

    def __init__(self, db_path: str, ttl_seconds: int = 3600):
        import duckdb  # already a project dependency
        self._ttl = ttl_seconds
        self._con = duckdb.connect(db_path)
        self._con.execute(_DUCKDB_SCHEMA)

    def get(self, key: str) -> Optional[str]:
        row = self._con.execute(
            "SELECT response, cached_at, ttl_seconds FROM query_results WHERE cache_key = ?",
            [key],
        ).fetchone()
        if not row:
            return None
        response, cached_at_str, ttl = row
        cached_at = datetime.fromisoformat(cached_at_str)
        age = (datetime.now(tz=timezone.utc) - cached_at.replace(tzinfo=timezone.utc)).total_seconds()
        if age > ttl:
            self._con.execute("DELETE FROM query_results WHERE cache_key = ?", [key])
            return None
        return response

    def put(self, key, table_name, sql_executed, response, row_count, ttl_seconds):
        now = datetime.now(tz=timezone.utc).isoformat()
        self._con.execute(
            """
            INSERT OR REPLACE INTO query_results
            (cache_key, table_name, sql_executed, response, row_count, cached_at, ttl_seconds)
            VALUES (?,?,?,?,?,?,?)
            """,
            [key, table_name, sql_executed, response, row_count, now, ttl_seconds],
        )

    def invalidate_table(self, table_name: str) -> None:
        self._con.execute("DELETE FROM query_results WHERE table_name = ?", [table_name])

    def close(self) -> None:
        self._con.close()


# ── Redis backend ──────────────────────────────────────────────────────────────

class RedisResultCache(ResultCacheBackend):
    """
    Redis-backed result cache. Shared across all workers on all machines.
    For production, point redis_url at an ElastiCache endpoint.

    Keys:
      limnos:result:{cache_key}      → JSON payload (response, row_count, sql_executed)
      limnos:table:{table_name}      → Redis Set of cache_keys (for invalidation)
    """

    def __init__(self, redis_url: str, ttl_seconds: int = 3600):
        try:
            import redis as redis_lib
        except ImportError as e:
            raise ImportError(
                "redis-py is required for the Redis cache backend. "
                "Install with: pip install redis"
            ) from e
        self._ttl = ttl_seconds
        self._client = redis_lib.from_url(redis_url, decode_responses=True)

    def _result_key(self, key: str) -> str:
        return f"limnos:result:{key}"

    def _table_key(self, table_name: str) -> str:
        return f"limnos:table:{table_name}"

    def get(self, key: str) -> Optional[str]:
        raw = self._client.get(self._result_key(key))
        if not raw:
            return None
        return json.loads(raw)["response"]

    def put(self, key, table_name, sql_executed, response, row_count, ttl_seconds):
        payload = json.dumps({"response": response, "row_count": row_count, "sql_executed": sql_executed})
        rk = self._result_key(key)
        self._client.set(rk, payload, ex=ttl_seconds)
        # Track key under table set (set TTL slightly longer so it outlives the result)
        tk = self._table_key(table_name)
        self._client.sadd(tk, key)
        self._client.expire(tk, ttl_seconds + 60)

    def invalidate_table(self, table_name: str) -> None:
        tk = self._table_key(table_name)
        keys = self._client.smembers(tk)
        if keys:
            self._client.delete(*[self._result_key(k) for k in keys])
        self._client.delete(tk)

    def close(self) -> None:
        self._client.close()


# ── Factory ────────────────────────────────────────────────────────────────────

def make_result_cache(
    backend: str,
    db_path: str,
    result_cache_db_path: str,
    redis_url: str,
    ttl_seconds: int,
) -> ResultCacheBackend:
    """Instantiate the configured result cache backend."""
    if backend == "redis":
        return RedisResultCache(redis_url, ttl_seconds)
    if backend == "duckdb":
        return DuckDBResultCache(result_cache_db_path, ttl_seconds)
    return SQLiteResultCache(db_path, ttl_seconds)
