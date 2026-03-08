"""datalake_query — execute a natural language or SQL query against the data lake."""

from __future__ import annotations

import json
from typing import Optional

from mcp.server.fastmcp import FastMCP, Context
from pydantic import BaseModel, ConfigDict, Field

from catalog.result_cache import make_cache_key
from engine.duckdb_engine import QueryError
from tools import format_query_result
from tools.sample_data import _nl_to_sql


NL_TO_SQL_SYSTEM = """\
You are a SQL expert. Convert the user's question to a single DuckDB SQL query.

Rules:
- Use partition columns in WHERE clauses whenever relevant to the question
- Never use SELECT * — select only the columns needed to answer the question
- For aggregation questions, do NOT add LIMIT
- For row-level questions, add LIMIT 1000
- Reference Parquet tables with: read_parquet('{s3_path}**/*.parquet', hive_partitioning=true)
- Reference Iceberg tables with: iceberg_scan('{s3_path}')
- Respond with ONLY the SQL query — no explanation, no markdown fences
"""


class QueryInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    table: str = Field(
        ...,
        description="Table name as returned by datalake_list_datasets.",
        min_length=1,
    )
    question: str = Field(
        ...,
        description=(
            "Natural language question (e.g. 'total orders by region last month') "
            "or a raw SQL SELECT statement."
        ),
        min_length=1,
    )
    row_limit: Optional[int] = Field(
        default=None,
        description="Override the default row limit (default: from config). Max: 50000.",
        ge=1,
        le=50_000,
    )
    force: bool = Field(
        default=False,
        description="Set true to proceed even when a cost warning or block is raised.",
    )
    explain_only: bool = Field(
        default=False,
        description="Return the generated SQL and cost estimate without executing.",
    )


def register(mcp: FastMCP) -> None:

    @mcp.tool(
        name="datalake_query",
        annotations={
            "title": "Query Data Lake Table",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": False,
            "openWorldHint": False,
        },
    )
    async def datalake_query(params: QueryInput, ctx: Context) -> str:
        """Execute a natural language or SQL query against a data lake table.

        Workflow:
        1. Converts natural language to SQL using Claude (if not already SQL).
        2. Estimates cost and bytes scanned before executing.
        3. Warns or blocks if cost exceeds configured thresholds.
        4. Routes to DuckDB (cheap, fast) or Athena (large scans) automatically.
        5. Returns results as a Markdown table with cost/performance metadata.

        Always call datalake_describe_table first to understand the schema.
        Use partition columns in your question to reduce cost.

        Args:
            params (QueryInput): Input parameters containing:
                - table (str): Target table name
                - question (str): Natural language question or SQL SELECT
                - row_limit (Optional[int]): Max rows (default from config)
                - force (bool): Bypass cost warnings
                - explain_only (bool): Return SQL + estimate without executing

        Returns:
            str: Markdown response with query, cost metadata, and result table.
                 If blocked by cost gate, returns warning and generated SQL instead.
        """
        state          = ctx.request_context.lifespan_state
        config         = state["config"]
        cache          = state["cache"]
        result_cache   = state.get("result_cache")
        duckdb_engine  = state["duckdb_engine"]
        athena_engine  = state["athena_engine"]
        cost_estimator = state["cost_estimator"]

        table_cfg = config.get_table(params.table)
        if not table_cfg:
            return (
                f"❌ Table '{params.table}' not found.\n\n"
                f"Available tables: {', '.join(config.table_names)}"
            )

        meta = cache.get(params.table)
        if meta is None and config.cache.auto_refresh:
            await ctx.report_progress(0.05, "Refreshing schema cache...")
            from tools.describe_table import _scan_metadata
            meta = await _scan_metadata(table_cfg, duckdb_engine, cache)

        # ── Step 1: NL → SQL ────────────────────────────────────────────────
        is_sql = params.question.strip().upper().startswith("SELECT")
        if is_sql:
            sql = params.question.strip()
        else:
            await ctx.report_progress(0.2, "Generating SQL from your question...")
            sql = await _nl_to_sql(params.question, meta, table_cfg) if meta \
                else _fallback_sql(params.question, table_cfg)

        # ── Step 2: Cost estimation ─────────────────────────────────────────
        estimate = cost_estimator.estimate(params.table, sql)

        if params.explain_only:
            return _format_explain(sql, estimate)

        # ── Step 3: Cost gate ────────────────────────────────────────────────
        if estimate.block and not params.force:
            return (
                f"🚫 **Query blocked** — estimated cost exceeds threshold.\n\n"
                f"{estimate.summary_line()}\n\n"
                f"**Warning:** {estimate.warning}\n\n"
                f"**Generated SQL:**\n```sql\n{sql}\n```\n\n"
                f"To proceed anyway, call `datalake_query` with `force=true`."
            )

        if estimate.warning and not params.force:
            # Soft warning — still execute, but surface the warning
            await ctx.log_info(f"Cost warning: {estimate.warning}")

        # ── Step 4: Result cache check ───────────────────────────────────────
        effective_row_limit = params.row_limit or config.engine.default_row_limit
        cache_key = None
        skip_cache = (
            not config.cache.result_cache_enabled
            or params.force
            or (estimate.confidence == "low" and config.cache.result_cache_skip_low_confidence)
        )
        if result_cache and not skip_cache:
            cache_key = make_cache_key(params.table, sql, effective_row_limit)
            cached_response = result_cache.get(cache_key)
            if cached_response is not None:
                await ctx.log_info("cache_hit", table=params.table)
                return cached_response

        # ── Step 5: Execute ──────────────────────────────────────────────────
        await ctx.report_progress(0.5, f"Running query via {estimate.recommended_engine}...")

        try:
            if estimate.recommended_engine == "duckdb":
                result = duckdb_engine.query(sql, row_limit=params.row_limit)
            else:
                result = athena_engine.query(sql)
        except QueryError as e:
            return (
                f"❌ **Query failed** ({estimate.recommended_engine})\n\n"
                f"```\n{e}\n```\n\n"
                f"**SQL attempted:**\n```sql\n{sql}\n```\n\n"
                f"Tip: Run `datalake_describe_table` to check the schema, "
                f"then try again with more specific column names."
            )
        except Exception as e:
            return f"❌ Unexpected error: {type(e).__name__}: {e}"

        await ctx.report_progress(1.0, "Done")

        # ── Step 6: Format & return ──────────────────────────────────────────
        response = format_query_result(result, estimate.summary_line())

        if estimate.warning:
            response = f"⚠️ {estimate.warning}\n\n{response}"

        # Store in result cache (skip on errors — we only reach here on success)
        if result_cache and cache_key and not skip_cache:
            result_cache.put(
                key=cache_key,
                table_name=params.table,
                sql_executed=result.sql_executed,
                response=response,
                row_count=result.row_count,
                ttl_seconds=config.cache.result_cache_ttl_seconds,
            )

        return response


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _fallback_sql(question: str, table_cfg) -> str:
    """Last-resort SQL when no metadata is available — SELECT * with limit."""
    if table_cfg.format == "parquet":
        source = f"read_parquet('{table_cfg.s3_path}**/*.parquet', hive_partitioning=true)"
    else:
        source = f"iceberg_scan('{table_cfg.s3_path}')"
    return f"-- Could not generate SQL from NL (no cached schema). Returning sample.\nSELECT * FROM {source} LIMIT 100"


def _format_explain(sql: str, estimate) -> str:
    return (
        f"## Query Plan (explain_only=true)\n\n"
        f"**{estimate.summary_line()}**\n\n"
        f"{'⚠️ ' + estimate.warning if estimate.warning else ''}\n\n"
        f"```sql\n{sql}\n```\n\n"
        f"| Metric | Value |\n"
        f"|--------|-------|\n"
        f"| Engine | {estimate.recommended_engine} |\n"
        f"| Estimated bytes | {_human_bytes(estimate.estimated_bytes)} |\n"
        f"| Estimated files | {estimate.estimated_files} |\n"
        f"| S3 GET requests | {estimate.s3_get_requests} |\n"
        f"| Athena cost | ${estimate.athena_cost_usd:.6f} |\n"
        f"| S3 GET cost | ${estimate.s3_get_cost_usd:.6f} |\n"
        f"| **Total cost** | **${estimate.total_cost_usd:.6f}** |\n"
        f"| Confidence | {estimate.confidence} |\n"
        f"| Partition filter | {'✅ yes' if estimate.partition_filter_detected else '❌ no (full scan)'} |\n"
    )


def _human_bytes(b: int) -> str:
    if b < 0:
        return "unknown"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if b < 1024:
            return f"{b:.1f} {unit}"
        b //= 1024
    return f"{b:.1f} PB"
