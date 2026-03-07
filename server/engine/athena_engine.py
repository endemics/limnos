"""
AWS Athena fallback engine.

Used when DuckDB estimated scan exceeds config.engine.duckdb_max_scan_bytes.
Submits queries to Athena, polls for completion, and retrieves results via S3.
"""

from __future__ import annotations

import csv
import io
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import boto3
import structlog

from config import Config
from engine.duckdb_engine import QueryResult

logger = structlog.get_logger()

POLL_INTERVAL_SECONDS = 1.0
MAX_POLL_SECONDS = 300


@dataclass
class AthenaQueryMeta:
    query_execution_id: str
    state: str
    bytes_scanned: int
    duration_ms: int
    cost_usd: float


class AthenaEngine:
    """Submits queries to Athena and retrieves results from S3."""

    ATHENA_PRICE_PER_TB = 5.00

    def __init__(self, config: Config):
        self.config = config
        self._athena = boto3.client("athena", region_name=config.aws.region)
        self._s3 = boto3.client("s3", region_name=config.aws.region)

    def query(self, sql: str, database: str = "default") -> QueryResult:
        """
        Submit a query to Athena and block until results are ready.

        Returns a QueryResult compatible with DuckDBEngine output.
        """
        output_location = self.config.aws.athena_output_location
        workgroup = self.config.aws.athena_workgroup

        logger.info("athena_query_start", sql_preview=sql[:200])

        # Start execution
        resp = self._athena.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": database},
            ResultConfiguration={"OutputLocation": output_location},
            WorkGroup=workgroup,
        )
        execution_id = resp["QueryExecutionId"]

        # Poll for completion
        meta = self._poll(execution_id)

        if meta.state != "SUCCEEDED":
            raise AthenaQueryError(
                f"Athena query {execution_id} ended with state: {meta.state}"
            )

        # Fetch results
        rows, columns = self._fetch_results(execution_id)

        logger.info(
            "athena_query_complete",
            execution_id=execution_id,
            bytes_scanned=meta.bytes_scanned,
            cost_usd=meta.cost_usd,
            rows=len(rows),
        )

        return QueryResult(
            rows=rows,
            columns=columns,
            row_count=len(rows),
            bytes_scanned=meta.bytes_scanned,
            duration_ms=meta.duration_ms,
            engine="athena",
            sql_executed=sql,
            truncated=False,
        )

    def _poll(self, execution_id: str) -> AthenaQueryMeta:
        """Poll Athena until query finishes. Returns execution metadata."""
        start = time.monotonic()
        while True:
            resp = self._athena.get_query_execution(QueryExecutionId=execution_id)
            exec_info = resp["QueryExecution"]
            state = exec_info["Status"]["State"]

            if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
                stats = exec_info.get("Statistics", {})
                bytes_scanned = stats.get("DataScannedInBytes", 0)
                duration_ms = stats.get("TotalExecutionTimeInMillis", 0)
                # Athena minimum billing is 10 MB
                billable = max(bytes_scanned, 10 * 1024 * 1024)
                cost_usd = (billable / 1e12) * self.ATHENA_PRICE_PER_TB
                return AthenaQueryMeta(
                    query_execution_id=execution_id,
                    state=state,
                    bytes_scanned=bytes_scanned,
                    duration_ms=duration_ms,
                    cost_usd=cost_usd,
                )

            elapsed = time.monotonic() - start
            if elapsed > MAX_POLL_SECONDS:
                self._athena.stop_query_execution(QueryExecutionId=execution_id)
                raise AthenaQueryError(
                    f"Athena query timed out after {MAX_POLL_SECONDS}s"
                )

            time.sleep(POLL_INTERVAL_SECONDS)

    def _fetch_results(self, execution_id: str) -> tuple[List[Dict[str, Any]], List[str]]:
        """Download result CSV from S3 and parse into rows."""
        output_location = self.config.aws.athena_output_location.rstrip("/")
        bucket = output_location.removeprefix("s3://").split("/")[0]
        prefix = "/".join(output_location.removeprefix("s3://").split("/")[1:])
        key = f"{prefix}/{execution_id}.csv"

        obj = self._s3.get_object(Bucket=bucket, Key=key)
        content = obj["Body"].read().decode("utf-8")

        reader = csv.DictReader(io.StringIO(content))
        columns = reader.fieldnames or []
        rows = [dict(row) for row in reader]
        return rows, list(columns)


class AthenaQueryError(Exception):
    """Raised when an Athena query fails or times out."""
