"""
DuckDB query engine — primary query path for S3 Parquet/Iceberg data.

Uses DuckDB's httpfs extension to query S3 directly.
Columnar + predicate pushdown means only needed bytes are transferred.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import duckdb
import structlog

from config import Config

logger = structlog.get_logger()


@dataclass
class QueryResult:
    rows: List[Dict[str, Any]]
    columns: List[str]
    row_count: int
    bytes_scanned: int  # -1 if not measurable
    duration_ms: int
    engine: str = "duckdb"
    sql_executed: str = ""
    truncated: bool = False  # True if row_limit was applied


class DuckDBEngine:
    """
    Wraps a DuckDB in-memory connection configured for S3 access.

    Thread safety: DuckDB connections are NOT thread-safe.
    For concurrent use (HTTP transport), instantiate one engine per worker.
    """

    def __init__(self, config: Config):
        self.config = config
        self._con = duckdb.connect(database=":memory:")
        self._setup()

    def _setup(self) -> None:
        """Install extensions and configure S3 credentials."""
        self._con.execute("INSTALL httpfs; LOAD httpfs;")
        self._con.execute("INSTALL iceberg; LOAD iceberg;")

        aws = self.config.aws
        region = aws.region or "us-east-1"
        self._con.execute(f"SET s3_region = '{region}';")

        if aws.access_key_id and aws.secret_access_key:
            self._con.execute(f"SET s3_access_key_id = '{aws.access_key_id}';")
            self._con.execute(f"SET s3_secret_access_key = '{aws.secret_access_key}';")
        # If credentials are empty, DuckDB will use the AWS credential chain
        # (env vars, instance profile, etc.) — preferred for production.

        self._con.execute("SET s3_use_ssl = true;")
        # Enable parallel S3 downloads
        self._con.execute("SET threads = 4;")
        logger.info("duckdb_engine_ready", region=region)

    def query(
        self,
        sql: str,
        row_limit: Optional[int] = None,
        timeout_seconds: Optional[int] = None,
    ) -> QueryResult:
        """Execute SQL and return structured results."""
        limit = row_limit or self.config.engine.default_row_limit
        _ = timeout_seconds or self.config.engine.query_timeout_seconds

        # Inject LIMIT if not already present
        final_sql = _inject_limit(sql, limit)
        truncated = "LIMIT" not in sql.upper()

        t0 = time.monotonic()
        try:
            rel = self._con.execute(final_sql)
            rows_raw = rel.fetchall()
            cols = [d[0] for d in rel.description]
        except duckdb.Error as e:
            raise QueryError(f"DuckDB query failed: {e}") from e

        elapsed_ms = int((time.monotonic() - t0) * 1000)
        rows = [dict(zip(cols, r)) for r in rows_raw]

        logger.info(
            "duckdb_query_complete",
            rows=len(rows),
            duration_ms=elapsed_ms,
            truncated=truncated,
        )
        return QueryResult(
            rows=rows,
            columns=cols,
            row_count=len(rows),
            bytes_scanned=-1,  # DuckDB doesn't expose this directly yet
            duration_ms=elapsed_ms,
            engine="duckdb",
            sql_executed=final_sql,
            truncated=truncated,
        )

    def explain(self, sql: str) -> str:
        """Return EXPLAIN output for a query (no execution)."""
        try:
            result = self._con.execute(f"EXPLAIN {sql}").fetchall()
            return "\n".join(str(r[1]) for r in result)
        except duckdb.Error as e:
            return f"EXPLAIN failed: {e}"

    def get_parquet_schema(self, s3_path: str) -> List[Dict[str, str]]:
        """Read column names and types from a Parquet file/prefix footer."""
        # Use DESCRIBE on a glob pattern to read schema without fetching data
        glob = s3_path.rstrip("/") + "/**/*.parquet"
        try:
            rel = self._con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{glob}', hive_partitioning=true) LIMIT 0"
            )
            return [{"name": r[0], "type": r[1]} for r in rel.fetchall()]
        except duckdb.Error as e:
            raise QueryError(
                f"Failed to read Parquet schema from {s3_path}: {e}"
            ) from e

    def estimate_row_count(self, s3_path: str, fmt: str = "parquet") -> int:
        """Cheap row count estimate using Parquet footer metadata."""
        glob = s3_path.rstrip("/") + "/**/*.parquet"
        try:
            if fmt == "parquet":
                rel = self._con.execute(
                    f"SELECT COUNT(*) FROM read_parquet('{glob}', hive_partitioning=true)"
                )
            else:
                rel = self._con.execute(
                    f"SELECT COUNT(*) FROM iceberg_scan('{s3_path}')"
                )
            result = rel.fetchone()
            return result[0] if result else 0
        except duckdb.Error:
            return -1

    def get_flat_file_schema(
        self,
        s3_path: str,
        fmt: str,
        delimiter: str = ",",
        has_header: bool = True,
        json_format: str = "auto",
        glob_pattern: Optional[str] = None,
    ) -> tuple[List[Dict[str, str]], float]:
        """Detect schema and estimate bytes-per-row for a flat file table.

        Returns (columns, bytes_per_row_estimate).
        TXT returns a hardcoded single-column schema without querying S3.
        CSV/JSON/NDJSON run a DESCRIBE + 10k-row sample against S3.
        """
        if fmt == "txt":
            return [{"name": "line", "type": "VARCHAR"}], 80.0

        base = s3_path.rstrip("/")
        if glob_pattern:
            glob = f"{base}/{glob_pattern.lstrip('/')}"
        elif fmt == "csv":
            glob = f"{base}/**/*.csv"
        elif fmt == "json":
            glob = f"{base}/**/*.json"
        else:  # ndjson
            glob = f"{base}/**/*.ndjson"

        if fmt == "csv":
            header_opt = "true" if has_header else "false"
            source = (
                f"read_csv('{glob}', auto_detect=true, "
                f"delim='{delimiter}', header={header_opt})"
            )
        elif fmt == "ndjson":
            source = f"read_json('{glob}', format='newline_delimited')"
        else:  # json
            source = f"read_json('{glob}', format='{json_format}')"
        row_source = source

        try:
            rel = self._con.execute(f"DESCRIBE SELECT * FROM {source} LIMIT 0")
            columns = [{"name": r[0], "type": r[1]} for r in rel.fetchall()]
        except duckdb.Error as e:
            raise QueryError(f"Failed to read {fmt} schema from {s3_path}: {e}") from e

        # One-time bytes-per-row sample (10k rows)
        try:
            # STRUCT_PACK(*) converts a row to a struct; casting to VARCHAR gives
            # a JSON-like string whose length is a reasonable bytes-per-row proxy.
            sample_sql = (
                f"SELECT COUNT(*) AS n, "
                f"SUM(LENGTH(CAST(STRUCT_PACK(*) AS VARCHAR))) AS b "
                f"FROM (SELECT * FROM {row_source} LIMIT 10000)"
            )
            result = self._con.execute(sample_sql).fetchone()
            n, b = result if result else (0, 0)
            bytes_per_row = float(b) / n if n and n > 0 else 200.0
        except duckdb.Error:
            bytes_per_row = 200.0  # safe fallback

        return columns, bytes_per_row

    def close(self) -> None:
        self._con.close()


class QueryError(Exception):
    """Raised when a DuckDB query fails."""


def _inject_limit(sql: str, limit: int) -> str:
    """Append LIMIT to SQL if not already present."""
    stripped = sql.strip().rstrip(";")
    if "LIMIT" not in stripped.upper():
        return f"{stripped}\nLIMIT {limit}"
    return stripped
