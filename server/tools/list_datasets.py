"""datalake_list_datasets — browse registered tables."""

from __future__ import annotations

import json
from typing import Optional

from mcp.server.fastmcp import FastMCP, Context
from pydantic import BaseModel, ConfigDict, Field


class ListDatasetsInput(BaseModel):
    model_config = ConfigDict(str_strip_whitespace=True, extra="forbid")

    filter: Optional[str] = Field(
        default=None,
        description="Optional substring filter on table name or description.",
    )


def register(mcp: FastMCP) -> None:

    @mcp.tool(
        name="datalake_list_datasets",
        annotations={
            "title": "List Data Lake Datasets",
            "readOnlyHint": True,
            "destructiveHint": False,
            "idempotentHint": True,
            "openWorldHint": False,
        },
    )
    async def datalake_list_datasets(params: ListDatasetsInput, ctx: Context) -> str:
        """List all tables registered in the data lake.

        Returns table names, S3 paths, formats (parquet/iceberg),
        partition columns, and descriptions. Use this as the first step
        when exploring the data lake to discover what tables are available.

        Args:
            params (ListDatasetsInput): Input parameters containing:
                - filter (Optional[str]): Substring to filter table names/descriptions

        Returns:
            str: JSON with keys 'tables' (list) and 'total' (int).
                 Each table entry has: name, s3_path, format, description,
                 partition_columns, cached_metadata (bool).
        """
        state = ctx.request_context.lifespan_state
        config = state["config"]
        cache  = state["cache"]

        tables = []
        for table_cfg in config.tables:
            if params.filter and params.filter.lower() not in (
                table_cfg.name + table_cfg.description
            ).lower():
                continue

            meta = cache.get(table_cfg.name)
            tables.append({
                "name":              table_cfg.name,
                "s3_path":           table_cfg.s3_path,
                "format":            table_cfg.format,
                "description":       table_cfg.description,
                "partition_columns": [p.name for p in table_cfg.partition_columns],
                "cached_metadata":   meta is not None,
                "total_rows":        meta.total_rows if meta else None,
                "total_size":        meta.size_human if meta else None,
                "metadata_age_h":    round(meta.freshness_hours, 1) if meta else None,
            })

        return json.dumps({"tables": tables, "total": len(tables)}, indent=2)
