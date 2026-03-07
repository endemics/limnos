"""
Iceberg table metadata reader — direct S3 approach (no catalog server required).

Reads Iceberg metadata files from S3:
  metadata/version-hint.text  -> current metadata version
  metadata/v{N}.metadata.json -> schema, partition spec, snapshots

For production, consider using PyIceberg with a Glue or REST catalog instead.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import boto3
import structlog

logger = structlog.get_logger()

_ICEBERG_TYPE_MAP = {
    "boolean": "BOOLEAN", "int": "INTEGER", "long": "BIGINT",
    "float": "FLOAT", "double": "DOUBLE", "date": "DATE",
    "time": "TIME", "timestamp": "TIMESTAMP", "timestamptz": "TIMESTAMPTZ",
    "string": "VARCHAR", "uuid": "UUID", "binary": "BLOB",
}


@dataclass
class IcebergColumn:
    field_id: int
    name: str
    dtype: str
    required: bool = False
    doc: str = ""


@dataclass
class IcebergPartitionField:
    source_id: int
    field_id: int
    name: str
    transform: str


@dataclass
class IcebergSnapshot:
    snapshot_id: int
    parent_id: Optional[int]
    timestamp_ms: int
    manifest_list: str
    total_rows: int = 0
    total_bytes: int = 0


@dataclass
class IcebergTableMeta:
    table_uuid: str
    location: str
    schema_columns: List[IcebergColumn]
    partition_fields: List[IcebergPartitionField]
    current_snapshot: Optional[IcebergSnapshot]
    total_files: int = 0
    total_bytes: int = 0
    total_rows: int = 0


def read_iceberg_metadata(s3_path: str, s3_client=None) -> IcebergTableMeta:
    """Read Iceberg table metadata directly from S3 without a catalog server."""
    if s3_client is None:
        s3_client = boto3.client("s3")

    bucket, prefix = _parse_s3_path(s3_path)
    meta_prefix = f"{prefix}metadata/"

    version = _read_version_hint(s3_client, bucket, meta_prefix)
    meta = _read_json(s3_client, bucket, f"{meta_prefix}v{version}.metadata.json")

    current_schema_id = meta.get("current-schema-id", 0)
    schemas = {s["schema-id"]: s for s in meta.get("schemas", [meta.get("schema", {})])}
    schema = schemas.get(current_schema_id, next(iter(schemas.values())))
    columns = _parse_schema(schema)

    current_spec_id = meta.get("default-spec-id", 0)
    specs = {s["spec-id"]: s for s in meta.get("partition-specs", [])}
    partition_fields = _parse_partition_spec(specs.get(current_spec_id, {}))

    current_snap_id = meta.get("current-snapshot-id")
    snapshots = {s["snapshot-id"]: s for s in meta.get("snapshots", [])}
    current_snap = snapshots.get(current_snap_id)

    snapshot_meta = None
    total_files = total_rows = total_bytes = 0

    if current_snap:
        summary = current_snap.get("summary", {})
        total_files = int(summary.get("total-data-files", 0))
        total_rows = int(summary.get("total-records", 0))
        total_bytes = int(summary.get("total-files-size", 0))
        snapshot_meta = IcebergSnapshot(
            snapshot_id=current_snap["snapshot-id"],
            parent_id=current_snap.get("parent-snapshot-id"),
            timestamp_ms=current_snap.get("timestamp-ms", 0),
            manifest_list=current_snap.get("manifest-list", ""),
            total_rows=total_rows,
            total_bytes=total_bytes,
        )

    return IcebergTableMeta(
        table_uuid=meta.get("table-uuid", ""),
        location=meta.get("location", s3_path),
        schema_columns=columns,
        partition_fields=partition_fields,
        current_snapshot=snapshot_meta,
        total_files=total_files,
        total_bytes=total_bytes,
        total_rows=total_rows,
    )


def _read_version_hint(s3_client, bucket: str, meta_prefix: str) -> int:
    try:
        obj = s3_client.get_object(Bucket=bucket, Key=f"{meta_prefix}version-hint.text")
        return int(obj["Body"].read().decode().strip())
    except Exception:
        resp = s3_client.list_objects_v2(Bucket=bucket, Prefix=meta_prefix)
        versions = []
        for obj in resp.get("Contents", []):
            key = obj["Key"].split("/")[-1]
            if key.startswith("v") and key.endswith(".metadata.json"):
                try:
                    versions.append(int(key[1:].replace(".metadata.json", "")))
                except ValueError:
                    pass
        if not versions:
            raise FileNotFoundError(f"No Iceberg metadata found at s3://{bucket}/{meta_prefix}")
        return max(versions)


def _read_json(s3_client, bucket: str, key: str) -> Dict[str, Any]:
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read())


def _parse_schema(schema: Dict[str, Any]) -> List[IcebergColumn]:
    columns = []
    for f in schema.get("fields", []):
        dtype_raw = f.get("type", "string")
        dtype = _ICEBERG_TYPE_MAP.get(dtype_raw, dtype_raw.upper()) if isinstance(dtype_raw, str) else "STRUCT"
        columns.append(IcebergColumn(
            field_id=f.get("id", 0), name=f.get("name", ""),
            dtype=dtype, required=f.get("required", False), doc=f.get("doc", ""),
        ))
    return columns


def _parse_partition_spec(spec: Dict[str, Any]) -> List[IcebergPartitionField]:
    return [
        IcebergPartitionField(
            source_id=f.get("source-id", 0), field_id=f.get("field-id", 0),
            name=f.get("name", ""), transform=f.get("transform", "identity"),
        )
        for f in spec.get("fields", [])
    ]


def _parse_s3_path(s3_path: str) -> Tuple[str, str]:
    without_scheme = s3_path.removeprefix("s3://")
    parts = without_scheme.split("/", 1)
    bucket = parts[0]
    prefix = parts[1].rstrip("/") + "/" if len(parts) > 1 else ""
    return bucket, prefix
