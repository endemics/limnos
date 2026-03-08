"""
Pre-query cost estimator.

Estimates bytes scanned and USD cost BEFORE running a query, so the MCP
server can warn or block expensive queries.

Estimation model:
  1. Partition pruning  — what fraction of partitions does the WHERE clause touch?
  2. Column pruning     — Parquet is columnar; only selected columns are read.
  3. Athena billing     — (billable_bytes / 1 TB) * $5.00, min 10 MB per query.
  4. S3 GET cost        — ~1 GET per row group per file touched.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import sqlglot
import sqlglot.expressions as exp
import structlog

from catalog.schema_cache import SchemaCache, TableMeta
from config import Config

logger = structlog.get_logger()

# AWS pricing constants (us-east-1, as of 2025)
ATHENA_PRICE_PER_TB_USD = 5.00
ATHENA_MIN_BYTES = 10 * 1024 * 1024  # 10 MB minimum per query
S3_GET_PRICE_PER_1000_USD = 0.0004
S3_STORAGE_PRICE_PER_GB = 0.023  # Standard storage / month


@dataclass
class CostEstimate:
    recommended_engine: str  # "duckdb" or "athena"
    estimated_bytes: int  # Bytes DuckDB/Athena would scan
    estimated_files: int  # S3 files touched
    s3_get_requests: int
    athena_cost_usd: float
    s3_get_cost_usd: float
    total_cost_usd: float
    confidence: str  # "high" | "medium" | "low"
    partition_filter_detected: bool
    column_filter_fraction: float  # 0.0–1.0
    warning: Optional[str] = None
    block: bool = False  # True if above block_threshold

    def summary_line(self) -> str:
        engine_label = (
            f"{'🦆 DuckDB' if self.recommended_engine == 'duckdb' else '☁️  Athena'}"
        )
        size_label = _human_bytes(self.estimated_bytes)
        return (
            f"{engine_label} | ~{size_label} scan | "
            f"est. ${self.total_cost_usd:.5f} ({self.confidence} confidence)"
        )


class CostEstimator:
    def __init__(self, config: Config, cache: SchemaCache):
        self.config = config
        self.cache = cache

    def estimate(self, table_name: str, sql: str) -> CostEstimate:
        """
        Estimate cost for a SQL query against a registered table.

        Returns a CostEstimate regardless of whether metadata is available
        (confidence will be 'low' if metadata is missing or stale).
        """
        meta = self.cache.get(table_name)
        if meta is None:
            return self._unknown_estimate(table_name)

        confidence = (
            "high"
            if meta.freshness_hours < 24
            else "medium"
            if meta.freshness_hours < 72
            else "low"
        )

        # ── Column pruning ──────────────────────────────────────────────────
        selected_cols = _extract_selected_columns(sql)
        col_fraction = _column_size_fraction(selected_cols, meta)

        # ── Partition pruning ───────────────────────────────────────────────
        partition_col_names = [p.name for p in meta.partition_columns]
        has_partition_filter = _has_partition_filter(sql, partition_col_names)
        partition_fraction = 0.1 if has_partition_filter else 1.0

        # ── Byte estimate ───────────────────────────────────────────────────
        estimated_bytes = int(meta.total_bytes * col_fraction * partition_fraction)
        estimated_files = max(1, int(meta.total_files * partition_fraction))

        # ── S3 GET cost ─────────────────────────────────────────────────────
        avg_row_groups = meta.avg_row_groups_per_file or 4
        s3_gets = estimated_files * avg_row_groups
        s3_get_cost = (s3_gets / 1000) * S3_GET_PRICE_PER_1000_USD

        # ── Athena billing (for reference / fallback path) ──────────────────
        billable = max(estimated_bytes, ATHENA_MIN_BYTES)
        athena_cost = (billable / 1e12) * ATHENA_PRICE_PER_TB_USD

        # ── Engine recommendation ────────────────────────────────────────────
        max_duckdb = self.config.engine.duckdb_max_scan_bytes
        recommended = "duckdb" if estimated_bytes <= max_duckdb else "athena"

        # For DuckDB path, S3 GET is the real cost (no per-TB charge)
        total_cost = (
            s3_get_cost if recommended == "duckdb" else athena_cost + s3_get_cost
        )

        # ── Warning / block gates ────────────────────────────────────────────
        warn_thresh = self.config.cost_gates.warn_threshold_usd
        block_thresh = self.config.cost_gates.block_threshold_usd
        warning = None
        block = False

        if recommended == "athena":
            warning = (
                f"Estimated scan ({_human_bytes(estimated_bytes)}) exceeds DuckDB threshold "
                f"({_human_bytes(max_duckdb)}). Will route to Athena "
                f"(est. ${athena_cost:.4f})."
            )
        elif total_cost > block_thresh:
            warning = (
                f"Estimated cost ${total_cost:.4f} exceeds block threshold "
                f"${block_thresh:.2f}. Pass force=true to proceed."
            )
            block = True
        elif total_cost > warn_thresh:
            warning = (
                f"Estimated cost ${total_cost:.4f} exceeds warn threshold "
                f"${warn_thresh:.2f}. Consider adding partition filters."
            )

        logger.debug(
            "cost_estimated",
            table=table_name,
            bytes=estimated_bytes,
            engine=recommended,
            cost_usd=total_cost,
        )

        return CostEstimate(
            recommended_engine=recommended,
            estimated_bytes=estimated_bytes,
            estimated_files=estimated_files,
            s3_get_requests=s3_gets,
            athena_cost_usd=athena_cost,
            s3_get_cost_usd=s3_get_cost,
            total_cost_usd=total_cost,
            confidence=confidence,
            partition_filter_detected=has_partition_filter,
            column_filter_fraction=col_fraction,
            warning=warning,
            block=block,
        )

    def _unknown_estimate(self, table_name: str) -> CostEstimate:
        return CostEstimate(
            recommended_engine="athena",
            estimated_bytes=-1,
            estimated_files=-1,
            s3_get_requests=-1,
            athena_cost_usd=-1.0,
            s3_get_cost_usd=-1.0,
            total_cost_usd=-1.0,
            confidence="low",
            partition_filter_detected=False,
            column_filter_fraction=1.0,
            warning=(
                f"No metadata found for '{table_name}'. "
                f"Run datalake_refresh_schema first to enable cost estimation."
            ),
        )


# ─── SQL parsing helpers ──────────────────────────────────────────────────────


def _extract_selected_columns(sql: str) -> List[str]:
    """Return list of column names referenced in SELECT clause, or ['*']."""
    try:
        tree = sqlglot.parse_one(sql, read="duckdb")
        select = tree.find(exp.Select)
        if not select:
            return ["*"]
        cols = []
        for expr in select.expressions:
            if isinstance(expr, exp.Star):
                return ["*"]
            if isinstance(expr, exp.Column):
                cols.append(expr.name)
            elif isinstance(expr, exp.Alias):
                inner = expr.this
                if isinstance(inner, exp.Column):
                    cols.append(inner.name)
        return cols or ["*"]
    except Exception:
        return ["*"]


def _has_partition_filter(sql: str, partition_cols: List[str]) -> bool:
    """Return True if the WHERE clause references any partition column."""
    if not partition_cols:
        return False
    try:
        tree = sqlglot.parse_one(sql, read="duckdb")
        where = tree.find(exp.Where)
        if not where:
            return False
        sql_cols = {col.name.lower() for col in where.find_all(exp.Column)}
        return any(p.lower() in sql_cols for p in partition_cols)
    except Exception:
        # Fallback: simple string search
        sql_upper = sql.upper()
        return any(p.upper() in sql_upper for p in partition_cols)


def _column_size_fraction(selected: List[str], meta: TableMeta) -> float:
    """Fraction of total bytes that selected columns represent."""
    if not selected or "*" in selected:
        return 1.0
    total_bytes_per_row = sum(c.estimated_bytes_per_row for c in meta.columns) or 1.0
    selected_set = {s.lower() for s in selected}
    selected_bytes = sum(
        c.estimated_bytes_per_row
        for c in meta.columns
        if c.name.lower() in selected_set
    )
    return min(selected_bytes / total_bytes_per_row, 1.0) if selected_bytes else 1.0


def _human_bytes(b: int) -> str:
    if b < 0:
        return "unknown"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if b < 1024:
            return f"{b:.1f} {unit}"
        b //= 1024
    return f"{b:.1f} PB"
