"""
Limnos MCP Server — entry point.

Usage:
    python main.py                          # stdio (Claude Desktop)
    python main.py --transport http         # HTTP on port 8000
    python main.py --transport http --port 9000
"""

import argparse
import os
from contextlib import asynccontextmanager

import structlog
from mcp.server.fastmcp import FastMCP

from catalog.schema_cache import SchemaCache
from catalog.result_cache import make_result_cache
from engine.duckdb_engine import DuckDBEngine
from engine.athena_engine import AthenaEngine
from engine.cost_estimator import CostEstimator
from tools import (
    list_datasets,
    describe_table,
    sample_data,
    query,
)
from config import load_config

logger = structlog.get_logger()

# ─── Lifespan ────────────────────────────────────────────────────────────────


@asynccontextmanager
async def app_lifespan(server: FastMCP):
    """Initialize shared resources for the server's lifetime."""
    config_path = os.environ.get("CONFIG_PATH", "config/config.yaml")
    cfg = load_config(config_path)

    structlog.configure(
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(__import__("logging"), cfg.logging.level)
        )
    )
    logger.info("s3_mcp_starting", config_path=config_path)

    cache = SchemaCache(cfg.cache.db_path)
    duckdb_engine = DuckDBEngine(cfg)
    athena_engine = AthenaEngine(cfg)
    cost_estimator = CostEstimator(cfg, cache)
    result_cache = make_result_cache(
        backend=cfg.cache.result_cache_backend,
        db_path=cfg.cache.db_path,
        result_cache_db_path=cfg.cache.result_cache_db_path,
        redis_url=cfg.cache.redis_url,
        ttl_seconds=cfg.cache.result_cache_ttl_seconds,
    )

    yield {
        "config": cfg,
        "cache": cache,
        "result_cache": result_cache,
        "duckdb_engine": duckdb_engine,
        "athena_engine": athena_engine,
        "cost_estimator": cost_estimator,
    }

    duckdb_engine.close()
    cache.close()
    result_cache.close()
    logger.info("s3_mcp_stopped")


# ─── Server ───────────────────────────────────────────────────────────────────

mcp = FastMCP(
    "limnos",
    lifespan=app_lifespan,
    instructions="""
You have access to an S3 data lake containing Parquet and Iceberg tables.

Workflow for answering data questions:
1. Call datalake_list_datasets to discover available tables.
2. Call datalake_describe_table on the relevant table(s) to understand schema and partitions.
3. Call datalake_estimate_query to preview cost before executing.
4. Call datalake_query with your natural language question or SQL.

Always filter on partition columns when possible — this dramatically reduces cost and latency.
If a query is estimated to be expensive, explain the cost to the user and ask for confirmation.
""",
)

# ─── Tool registration ────────────────────────────────────────────────────────

list_datasets.register(mcp)
describe_table.register(mcp)
sample_data.register(mcp)
query.register(mcp)


# ─── CLI ──────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Limnos MCP Server")
    parser.add_argument(
        "--transport",
        choices=["stdio", "http"],
        default="stdio",
        help="Transport to use (default: stdio for Claude Desktop)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Port for HTTP transport (default: 8000)",
    )
    args = parser.parse_args()

    if args.transport == "stdio":
        mcp.run(transport="stdio")
    else:
        # Get the underlying FastAPI/Starlette app
        app = mcp.sse_app()

        # Add health check for the Go gateway
        from starlette.responses import Response

        @app.route("/health")
        async def health(request):
            return Response(status_code=200)

        # Middleware to inject spend tracking headers
        from tools.query import current_query_cost

        @app.middleware("http")
        async def inject_cost_header(request, call_next):
            response = await call_next(request)
            cost = current_query_cost.get()
            if cost > 0:
                response.headers["X-Limnos-Cost-USD"] = str(cost)
                # Reset for next request in this worker
                current_query_cost.set(0.0)
            return response

        import uvicorn

        uvicorn.run(app, host="0.0.0.0", port=args.port)


if __name__ == "__main__":
    main()
