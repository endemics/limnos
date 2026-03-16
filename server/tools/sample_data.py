"""datalake_sample_data — return N rows cheaply without a full scan."""

from __future__ import annotations

import json

from mcp.server.fastmcp import FastMCP, Context
from pydantic import BaseModel, ConfigDict, Field

from tools.formatting import format_table


def _duckdb_source(table_cfg) -> str:
    """Return the DuckDB table-valued function for a given format."""
    p = table_cfg.s3_path
    fmt = table_cfg.format
    if fmt == "parquet":
        return f"read_parquet('{p}**/*.parquet', hive_partitioning=true)"
    if fmt == "iceberg":
        return f"iceberg_scan('{p}')"
    if fmt == "csv":
        glob = table_cfg.glob_pattern or "**/*.csv"
        return (
            f"read_csv('{p}{glob.lstrip('/')}', auto_detect=true, "
            f"delim='{table_cfg.delimiter}', "
            f"header={'true' if table_cfg.has_header else 'false'})"
        )
    if fmt == "json":
        glob = table_cfg.glob_pattern or "**/*.json"
        return f"read_json('{p}{glob.lstrip('/')}', format='{table_cfg.json_format}')"
    if fmt == "ndjson":
        glob = table_cfg.glob_pattern or "**/*.ndjson"
        return f"read_json('{p}{glob.lstrip('/')}', format='newline_delimited')"
    if fmt == "txt":
        glob = table_cfg.glob_pattern or "**/*.txt"
        return (
            f"read_csv('{p}{glob.lstrip('/')}', sep='\\n', header=false, "
            f"columns={{'line': 'VARCHAR'}})"
        )
    return f"read_parquet('{p}**/*.parquet', hive_partitioning=true)"


class SampleDataInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    table: str = Field(
        ..., description="Table name from datalake_list_datasets.", min_length=1
    )
    n: int = Field(default=10, description="Number of rows to return.", ge=1, le=500)
    columns: list[str] = Field(
        default_factory=list,
        description="Specific columns to fetch. Empty = all columns.",
    )


class EstimateQueryInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    table: str = Field(
        ..., description="Table name from datalake_list_datasets.", min_length=1
    )
    question: str = Field(
        ...,
        description="Natural language question or SQL query to estimate.",
        min_length=1,
    )


class RefreshSchemaInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    table: str = Field(..., description="Table name to refresh.", min_length=1)


def register(mcp: FastMCP) -> None:

    @mcp.tool(
        name="datalake_sample_data",
        annotations={
            "title": "Sample Data Lake Table",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
    )
    async def datalake_sample_data(params: SampleDataInput, ctx: Context) -> str:
        """Return a small sample of rows from a table.

        Uses LIMIT to fetch rows cheaply without scanning the whole table.
        Ideal for understanding data shape, value formats, and column contents
        before writing a real query.

        Args:
            params (SampleDataInput): Input parameters containing:
                - table (str): Table name
                - n (int): Number of rows (default 10, max 500)
                - columns (list[str]): Columns to include (default: all)

        Returns:
            str: Markdown table with sample rows and query metadata.
        """
        state = ctx.request_context.lifespan_state
        config = state["config"]
        engine = state["duckdb_engine"]

        table_cfg = config.get_table(params.table)
        if not table_cfg:
            return f"Table '{params.table}' not found. Use datalake_list_datasets."

        col_clause = ", ".join(params.columns) if params.columns else "*"
        source = _duckdb_source(table_cfg)
        sql = f"SELECT {col_clause} FROM {source} LIMIT {params.n}"

        result = engine.query(sql, row_limit=params.n)
        table_md = format_table(result.rows, result.columns)
        return f"**Sample: {params.table}** ({params.n} rows, {result.duration_ms}ms)\n\n{table_md}"

    # ──────────────────────────────────────────────────────────────────────────

    @mcp.tool(
        name="datalake_estimate_query",
        annotations={
            "title": "Estimate Query Cost",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
    )
    async def datalake_estimate_query(params: EstimateQueryInput, ctx: Context) -> str:
        """Estimate cost and bytes scanned for a query BEFORE running it.

        Returns estimated bytes scanned, engine recommendation (DuckDB vs Athena),
        and estimated USD cost. Call this before datalake_query when working with
        large tables or when cost is a concern.

        Args:
            params (EstimateQueryInput): Input parameters containing:
                - table (str): Target table name
                - question (str): Natural language question or raw SQL

        Returns:
            str: JSON with keys: recommended_engine, estimated_bytes,
                 estimated_files, athena_cost_usd, s3_get_cost_usd,
                 total_cost_usd, confidence, partition_filter_detected,
                 warning (optional).
        """
        state = ctx.request_context.lifespan_state
        config = state["config"]
        cache = state["cache"]
        cost_estimator = state["cost_estimator"]

        table_cfg = config.get_table(params.table)
        if not table_cfg:
            return json.dumps({"error": f"Table '{params.table}' not found."})

        # If NL, do a lightweight SQL generation (no execution)
        sql = params.question
        if not sql.strip().upper().startswith("SELECT"):
            meta = cache.get(params.table)
            if meta:
                sql = await _nl_to_sql(params.question, meta, table_cfg)
            else:
                return json.dumps(
                    {
                        "warning": "No metadata cached; run datalake_describe_table first.",
                        "sql_generated": None,
                    }
                )

        estimate = cost_estimator.estimate(params.table, sql)
        return json.dumps(
            {
                "sql_generated": sql,
                "recommended_engine": estimate.recommended_engine,
                "estimated_bytes": estimate.estimated_bytes,
                "estimated_bytes_human": _human_bytes(estimate.estimated_bytes),
                "estimated_files": estimate.estimated_files,
                "s3_get_requests": estimate.s3_get_requests,
                "athena_cost_usd": round(estimate.athena_cost_usd, 6),
                "s3_get_cost_usd": round(estimate.s3_get_cost_usd, 6),
                "total_cost_usd": round(estimate.total_cost_usd, 6),
                "confidence": estimate.confidence,
                "partition_filter_detected": estimate.partition_filter_detected,
                "column_fraction": round(estimate.column_filter_fraction, 3),
                "warning": estimate.warning,
                "will_block": estimate.block,
            },
            indent=2,
        )

    # ──────────────────────────────────────────────────────────────────────────

    @mcp.tool(
        name="datalake_refresh_schema",
        annotations={
            "title": "Refresh Table Schema Cache",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
    )
    async def datalake_refresh_schema(params: RefreshSchemaInput, ctx: Context) -> str:
        """Force a re-scan of S3 metadata for a table and update the local cache.

        Use this after a table's schema changes, after new partitions are added,
        or when cost estimates seem inaccurate due to stale metadata.

        Args:
            params (RefreshSchemaInput): Input parameters containing:
                - table (str): Table name to refresh

        Returns:
            str: Summary of what was refreshed (row count, size, partition count).
        """
        from tools.describe_table import _scan_metadata

        state = ctx.request_context.lifespan_state
        config = state["config"]
        cache = state["cache"]
        result_cache = state.get("result_cache")
        engine = state["duckdb_engine"]

        table_cfg = config.get_table(params.table)
        if not table_cfg:
            return f"Table '{params.table}' not found."

        await ctx.report_progress(0.1, f"Scanning S3 metadata for '{params.table}'...")
        meta = await _scan_metadata(table_cfg, engine, cache, config)

        # Invalidate cached query results — schema changed, old results may be stale
        if result_cache:
            result_cache.invalidate_table(params.table)

        await ctx.report_progress(1.0, "Done")

        return (
            f"✅ Schema cache refreshed for **{params.table}**\n\n"
            f"- Format: {meta.format}\n"
            f"- Columns: {len(meta.columns)}\n"
            f"- Partition columns: {[p.name for p in meta.partition_columns]}\n"
            f"- Total size: {meta.size_human}\n"
            f"- Total files: {meta.total_files}\n"
            f"- Total partitions: {meta.total_partitions}\n"
            f"- Total rows: {meta.total_rows or 'not counted (expensive for Parquet)'}\n"
        )


# ─── Shared helper ────────────────────────────────────────────────────────────


async def _nl_to_sql(question: str, meta, table_cfg) -> str:
    """Call Claude to convert a natural language question to SQL."""
    import anthropic

    schema_lines = "\n".join(f"  {c.name} {c.dtype}" for c in meta.columns)
    partition_info = ", ".join(p.name for p in meta.partition_columns) or "none"

    source = _duckdb_source(table_cfg)
    system = (
        "You are a SQL expert. Convert the user's question to a single DuckDB SQL query.\n"
        "Rules:\n"
        "- Use the exact DuckDB source expression provided in the prompt as the FROM clause\n"
        "- Use partition columns in WHERE clauses whenever relevant\n"
        "- Never use SELECT * — only select needed columns\n"
        "- Add LIMIT 1000 unless the question asks for aggregates\n"
        "- Respond with ONLY the SQL, no explanation or markdown fences\n"
    )
    prompt = (
        f"Table: {table_cfg.name}\n"
        f"S3 path: {table_cfg.s3_path}\n"
        f"Format: {table_cfg.format}\n"
        f"DuckDB source (use as FROM clause): {source}\n"
        f"Partition columns: {partition_info}\n"
        f"Columns:\n{schema_lines}\n\n"
        f"Question: {question}"
    )

    client = anthropic.AsyncAnthropic()
    msg = await client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=512,
        system=system,
        messages=[{"role": "user", "content": prompt}],
    )
    return msg.content[0].text.strip()


def _human_bytes(b: int) -> str:
    if b < 0:
        return "unknown"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if b < 1024:
            return f"{b:.1f} {unit}"
        b //= 1024
    return f"{b:.1f} PB"
