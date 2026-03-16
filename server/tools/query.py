"""datalake_query — execute natural language or SQL queries."""

from __future__ import annotations

import re
from contextvars import ContextVar
from typing import Optional

from mcp.server.fastmcp import FastMCP, Context
from pydantic import BaseModel, ConfigDict, Field

from catalog.result_cache import make_cache_key
from tools.formatting import format_query_result
from tools.sample_data import _nl_to_sql

# ContextVar to communicate cost to the HTTP response headers in the middleware
current_query_cost: ContextVar[float] = ContextVar("current_query_cost", default=0.0)


class QueryInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    table: str = Field(..., description="Target table name.")
    question: str = Field(
        ...,
        description="Natural language question or SQL SELECT query.",
        min_length=1,
    )
    row_limit: Optional[int] = Field(
        default=None,
        description="Maximum rows to return (overrides config).",
    )
    force: bool = Field(
        default=False,
        description="Bypass cost gate warnings.",
    )
    explain_only: bool = Field(
        default=False,
        description="Return generated SQL and cost estimate without executing.",
    )


def register(mcp: FastMCP) -> None:

    @mcp.tool(
        name="datalake_query",
        annotations={
            "title": "Query Data Lake",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": True,
        },
    )
    async def datalake_query(params: QueryInput, ctx: Context) -> str:
        """Execute a natural language or SQL query against a data lake table."""
        state = ctx.request_context.lifespan_state
        config = state["config"]
        cache = state["cache"]
        result_cache = state["result_cache"]
        duckdb_engine = state["duckdb_engine"]
        athena_engine = state["athena_engine"]
        cost_estimator = state["cost_estimator"]

        table_cfg = config.get_table(params.table)
        if not table_cfg:
            return f"❌ Table '{params.table}' not found."

        meta = cache.get(params.table)
        if meta is None and config.cache.auto_refresh:
            from tools.describe_table import _scan_metadata

            meta = await _scan_metadata(table_cfg, duckdb_engine, cache, config)

        # ── Step 1: NL → SQL ────────────────────────────────────────────────
        is_sql = params.question.strip().upper().startswith("SELECT")
        if is_sql:
            sql = params.question.strip()
        else:
            sql = (
                await _nl_to_sql(params.question, meta, table_cfg)
                if meta
                else _fallback_sql(params.question, table_cfg)
            )

        # ── Step 2: Cost estimation ─────────────────────────────────────────
        estimate = cost_estimator.estimate(params.table, sql)
        current_query_cost.set(estimate.total_cost_usd)

        if params.explain_only:
            return _format_explain(sql, estimate)

        # ── Step 3: Cost gate ────────────────────────────────────────────────
        if estimate.block and not params.force:
            return f"🚫 **Query blocked** — estimated cost exceeds threshold.\n\n{estimate.summary_line()}"

        # ── Step 4: Result cache check ───────────────────────────────────────
        effective_row_limit = params.row_limit or config.engine.default_row_limit
        cache_key = make_cache_key(params.table, sql, effective_row_limit)
        if result_cache:
            cached = result_cache.get(cache_key)
            if cached:
                return cached

        # ── Step 5: Execute ──────────────────────────────────────────────────
        try:
            if estimate.recommended_engine == "duckdb":
                result = duckdb_engine.query(sql, row_limit=params.row_limit)
            else:
                result = athena_engine.query(sql)
        except Exception as e:
            return f"❌ **Query failed**\n\n```\n{e}\n```"

        # ── Step 6: Format & return ──────────────────────────────────────────
        response = format_query_result(result, estimate.summary_line())
        if result_cache:
            result_cache.put(
                cache_key,
                params.table,
                sql,
                response,
                result.row_count,
                config.cache.result_cache_ttl_seconds,
            )

        return response


def _fallback_sql(question: str, table_cfg) -> str:
    source = (
        f"read_parquet('{table_cfg.s3_path}**/*.parquet')"
        if table_cfg.format == "parquet"
        else f"iceberg_scan('{table_cfg.s3_path}')"
    )
    return f"SELECT * FROM {source} LIMIT 100"


def _format_explain(sql: str, estimate) -> str:
    return f"## Query Plan\n\n**{estimate.summary_line()}**\n\n```sql\n{sql}\n```"
