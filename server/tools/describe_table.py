"""datalake_describe_table — schema, partitions, and size for a table."""

from __future__ import annotations

import json

from mcp.server.fastmcp import FastMCP, Context
from pydantic import BaseModel, ConfigDict, Field

from catalog.schema_cache import SchemaCache, TableMeta, ColumnMeta, PartitionMeta
from catalog.hive import discover_partitions
from catalog.iceberg import read_iceberg_metadata


class DescribeTableInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    table: str = Field(
        ...,
        description="Table name as returned by datalake_list_datasets.",
        min_length=1,
    )
    force_refresh: bool = Field(
        default=False,
        description="Re-scan S3 metadata even if cached data is fresh.",
    )


def register(mcp: FastMCP) -> None:

    @mcp.tool(
        name="datalake_describe_table",
        annotations={
            "title": "Describe Data Lake Table",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
    )
    async def datalake_describe_table(params: DescribeTableInput, ctx: Context) -> str:
        """Get schema, partition info, and size statistics for a table.

        Reads metadata from the local cache when available (fast, free).
        Falls back to scanning S3 metadata files (Parquet footers or Iceberg
        manifests) when cache is missing or force_refresh=true.

        Call this before datalake_query to understand the table structure
        and choose appropriate partition filters.

        Args:
            params (DescribeTableInput): Input parameters containing:
                - table (str): Table name from datalake_list_datasets
                - force_refresh (bool): Re-scan S3 even if cache is fresh

        Returns:
            str: JSON with keys: name, format, s3_path, description,
                 columns (list of {name, dtype, estimated_bytes_per_row}),
                 partition_columns (list of {name, dtype}),
                 total_rows, total_bytes, total_files, total_partitions,
                 metadata_age_hours, sql_hint (example partition-filtered query).
        """
        state  = ctx.request_context.lifespan_state
        config = state["config"]
        cache: SchemaCache = state["cache"]
        engine = state["duckdb_engine"]

        table_cfg = config.get_table(params.table)
        if not table_cfg:
            return json.dumps({
                "error": f"Table '{params.table}' not found.",
                "available_tables": config.table_names,
            })

        # Check cache freshness
        meta = cache.get(params.table)
        needs_refresh = (
            params.force_refresh
            or meta is None
            or (meta.freshness_hours > config.cache.stale_threshold_hours
                and config.cache.auto_refresh)
        )

        if needs_refresh:
            await ctx.report_progress(0.1, f"Scanning {table_cfg.format} metadata from S3...")
            meta = await _scan_metadata(table_cfg, engine, cache)
            await ctx.report_progress(1.0, "Done")

        # Build SQL hint showing how to use partition filters
        sql_hint = _build_sql_hint(meta)

        return json.dumps({
            "name":             meta.table_name,
            "format":           meta.format,
            "s3_path":          meta.s3_path,
            "description":      meta.description,
            "columns": [
                {
                    "name":  c.name,
                    "dtype": c.dtype,
                    "estimated_bytes_per_row": c.estimated_bytes_per_row,
                    "null_fraction": c.null_fraction,
                }
                for c in meta.columns
            ],
            "partition_columns": [
                {"name": p.name, "dtype": p.dtype}
                for p in meta.partition_columns
            ],
            "total_rows":        meta.total_rows,
            "total_bytes":       meta.total_bytes,
            "total_files":       meta.total_files,
            "total_partitions":  meta.total_partitions,
            "size_human":        meta.size_human,
            "metadata_age_hours": round(meta.freshness_hours, 1),
            "sql_hint":          sql_hint,
        }, indent=2)


async def _scan_metadata(table_cfg, engine, cache: SchemaCache) -> TableMeta:
    """Refresh metadata by reading from S3."""
    import asyncio

    if table_cfg.format == "iceberg":
        iceberg_meta = await asyncio.to_thread(
            read_iceberg_metadata, table_cfg.s3_path
        )
        columns = [
            ColumnMeta(
                name=c.name,
                dtype=c.dtype,
                estimated_bytes_per_row=_dtype_to_bytes(c.dtype),
            )
            for c in iceberg_meta.schema_columns
        ]
        partition_cols = [
            PartitionMeta(name=p.name, dtype="string")
            for p in iceberg_meta.partition_fields
        ]
        meta = TableMeta(
            table_name=table_cfg.name,
            s3_path=table_cfg.s3_path,
            format="iceberg",
            columns=columns,
            partition_columns=partition_cols,
            total_rows=iceberg_meta.total_rows,
            total_bytes=iceberg_meta.total_bytes,
            total_files=iceberg_meta.total_files,
            total_partitions=max(len(partition_cols), 1),
            avg_row_groups_per_file=4,
            description=table_cfg.description,
        )
    else:
        # Parquet — read schema via DuckDB, discover partitions via S3
        raw_schema = await asyncio.to_thread(
            engine.get_parquet_schema, table_cfg.s3_path
        )
        columns = [
            ColumnMeta(
                name=c["name"],
                dtype=c["type"],
                estimated_bytes_per_row=_dtype_to_bytes(c["type"]),
            )
            for c in raw_schema
        ]

        partitions, discovered_partition_cols = await asyncio.to_thread(
            discover_partitions, table_cfg.s3_path
        )
        # Prefer config-defined partition columns; fall back to discovered
        if table_cfg.partition_columns:
            partition_cols = [
                PartitionMeta(name=p.name, dtype=p.type)
                for p in table_cfg.partition_columns
            ]
        else:
            partition_cols = [
                PartitionMeta(name=name, dtype="string")
                for name in discovered_partition_cols
            ]

        total_bytes = sum(p.total_bytes for p in partitions)
        total_files = sum(p.file_count for p in partitions)

        meta = TableMeta(
            table_name=table_cfg.name,
            s3_path=table_cfg.s3_path,
            format="parquet",
            columns=columns,
            partition_columns=partition_cols,
            total_rows=0,       # too expensive to count here
            total_bytes=total_bytes,
            total_files=total_files,
            total_partitions=len(partitions),
            avg_row_groups_per_file=4,
            description=table_cfg.description,
        )

    cache.upsert(meta)
    return meta


def _build_sql_hint(meta: TableMeta) -> str:
    col_names = ", ".join(c.name for c in meta.columns[:6])
    if len(meta.columns) > 6:
        col_names += ", ..."

    if meta.partition_columns:
        first_part = meta.partition_columns[0]
        filter_example = f"WHERE {first_part.name} = '<value>'"
    else:
        filter_example = "-- (no partition columns — full scan required)"

    return (
        f"SELECT {col_names}\n"
        f"FROM read_parquet('{meta.s3_path}**/*.parquet', hive_partitioning=true)\n"
        f"{filter_example}\n"
        f"LIMIT 1000"
    )


def _dtype_to_bytes(dtype: str) -> float:
    """Rough estimated bytes per row for common types."""
    dtype_upper = dtype.upper()
    if any(t in dtype_upper for t in ("BIGINT", "DOUBLE", "TIMESTAMP", "DATE")):
        return 8.0
    if any(t in dtype_upper for t in ("INTEGER", "FLOAT")):
        return 4.0
    if "BOOLEAN" in dtype_upper:
        return 1.0
    if any(t in dtype_upper for t in ("VARCHAR", "STRING", "TEXT")):
        return 32.0  # rough average
    return 8.0
