"""
Hive-style partition discovery for Parquet tables.

Scans S3 prefixes to discover partitions in the format:
  s3://bucket/prefix/col=value/col2=value2/*.parquet
"""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import boto3
import structlog

logger = structlog.get_logger()

# Matches Hive-style partition path segments: col=value
_PARTITION_RE = re.compile(r"^([^=]+)=(.+)$")


@dataclass
class DiscoveredPartition:
    path: str
    values: Dict[str, str]     # col -> value
    file_count: int
    total_bytes: int


def discover_partitions(
    s3_path: str,
    s3_client=None,
    max_keys: int = 10_000,
) -> Tuple[List[DiscoveredPartition], List[str]]:
    """
    Scan an S3 prefix and return discovered partitions + partition column names.

    Returns:
        (partitions, partition_column_names)
    """
    if s3_client is None:
        s3_client = boto3.client("s3")

    bucket, prefix = _parse_s3_path(s3_path)
    if prefix and not prefix.endswith("/"):
        prefix += "/"

    # Collect all .parquet files
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix, PaginationConfig={"MaxItems": max_keys})

    # Group files by their partition path
    partition_map: Dict[str, Dict] = defaultdict(lambda: {"files": 0, "bytes": 0, "values": {}})

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue
            relative = key[len(prefix):]
            parts = relative.split("/")[:-1]  # drop filename

            partition_values: Dict[str, str] = {}
            for part in parts:
                m = _PARTITION_RE.match(part)
                if m:
                    partition_values[m.group(1)] = m.group(2)

            if not partition_values:
                # Flat (non-partitioned) table — treat as single partition
                partition_path = prefix
                partition_map[partition_path]["files"] += 1
                partition_map[partition_path]["bytes"] += obj["Size"]
            else:
                partition_path = "/".join(parts)
                partition_map[partition_path]["files"] += 1
                partition_map[partition_path]["bytes"] += obj["Size"]
                partition_map[partition_path]["values"] = partition_values

    partitions = [
        DiscoveredPartition(
            path=path,
            values=info["values"],
            file_count=info["files"],
            total_bytes=info["bytes"],
        )
        for path, info in partition_map.items()
    ]

    # Derive partition column names from the first partition that has values
    partition_cols: List[str] = []
    for p in partitions:
        if p.values:
            partition_cols = list(p.values.keys())
            break

    logger.info(
        "partitions_discovered",
        s3_path=s3_path,
        partition_count=len(partitions),
        partition_cols=partition_cols,
    )
    return partitions, partition_cols


def estimate_scan_fraction(
    partition_cols: List[str],
    all_partitions: List[DiscoveredPartition],
    sql: str,
) -> float:
    """
    Heuristic: estimate what fraction of partitions a SQL query touches.

    Returns a float in [0.0, 1.0].
    0.1 = 10% of partitions touched (query has useful partition filters).
    1.0 = full scan.
    """
    if not partition_cols or not all_partitions:
        return 1.0

    sql_upper = sql.upper()
    filtered_cols = [col for col in partition_cols if col.upper() in sql_upper]

    if not filtered_cols:
        return 1.0  # no partition filter found → full scan

    # Rough heuristic: each partition filter narrows by ~10x
    # (e.g. date=2024-01-15 out of 365 date partitions ≈ 1/365 ≈ 0.003,
    # but we conservatively say 10% to account for range queries)
    fraction = 0.1 ** len(filtered_cols)
    return max(fraction, 1.0 / max(len(all_partitions), 1))


def _parse_s3_path(s3_path: str) -> Tuple[str, str]:
    """Parse s3://bucket/prefix into (bucket, prefix)."""
    without_scheme = s3_path.removeprefix("s3://")
    parts = without_scheme.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    return bucket, prefix
