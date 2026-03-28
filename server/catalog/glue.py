"""
Glue auto-provisioner for data lake tables.

Creates or updates AWS Glue external tables so Athena can query flat files
(CSV, JSON, NDJSON) and Parquet tables without manual catalog setup.
Called once during describe_table when metadata is first scanned.

Iceberg tables are excluded (they use their own catalog).
TXT tables are excluded (no Athena support).
"""

from __future__ import annotations

import boto3
import structlog
from botocore.exceptions import ClientError

from catalog.schema_cache import ColumnMeta, PartitionMeta

logger = structlog.get_logger()

_GLUE_TYPE_MAP: dict[str, str] = {
    "VARCHAR": "string",
    "TEXT": "string",
    "BIGINT": "bigint",
    "INTEGER": "int",
    "INT": "int",
    "DOUBLE": "double",
    "FLOAT": "float",
    "BOOLEAN": "boolean",
    "DATE": "date",
    "TIMESTAMP": "timestamp",
}

# (InputFormat, OutputFormat, SerDe library) per format
_SERDE: dict[str, tuple[str, str, str]] = {
    "csv": (
        "org.apache.hadoop.mapred.TextInputFormat",
        "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
    ),
    "json": (
        "org.apache.hadoop.mapred.TextInputFormat",
        "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        "org.openx.data.jsonserde.JsonSerDe",
    ),
    "ndjson": (
        "org.apache.hadoop.mapred.TextInputFormat",
        "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        "org.openx.data.jsonserde.JsonSerDe",
    ),
    "parquet": (
        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
    ),
}


class GlueProvisioner:
    """Create or update Glue external tables for data lake tables."""

    def __init__(self, config) -> None:
        self._glue = boto3.client("glue", region_name=config.aws.region)
        self._database = config.aws.glue_database

    def sync_table(
        self,
        table_cfg,
        columns: list[ColumnMeta],
        partition_cols: list[PartitionMeta],
    ) -> None:
        """Create or update a Glue external table. Idempotent."""
        if table_cfg.format not in _SERDE:
            return

        input_fmt, output_fmt, serde_lib = _SERDE[table_cfg.format]

        serde_params: dict[str, str] = {}
        if table_cfg.format == "csv":
            serde_params["field.delim"] = table_cfg.delimiter
            if table_cfg.has_header:
                serde_params["skip.header.line.count"] = "1"
        elif table_cfg.format == "parquet":
            serde_params["serialization.format"] = "1"

        glue_cols = [
            {
                "Name": c.name,
                "Type": _GLUE_TYPE_MAP.get(c.dtype.upper(), c.dtype.lower()),
            }
            for c in columns
        ]
        glue_partitions = [{"Name": p.name, "Type": "string"} for p in partition_cols]

        table_input = {
            "Name": table_cfg.name.replace("-", "_"),
            "StorageDescriptor": {
                "Columns": glue_cols,
                "Location": table_cfg.s3_path,
                "InputFormat": input_fmt,
                "OutputFormat": output_fmt,
                "SerdeInfo": {
                    "SerializationLibrary": serde_lib,
                    "Parameters": serde_params,
                },
            },
            "PartitionKeys": glue_partitions,
            "TableType": "EXTERNAL_TABLE",
            "Parameters": {"classification": table_cfg.format},
        }

        try:
            self._glue.create_table(DatabaseName=self._database, TableInput=table_input)
            logger.info(
                "glue_table_created",
                table=table_cfg.name,
                database=self._database,
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "AlreadyExistsException":
                self._glue.update_table(
                    DatabaseName=self._database, TableInput=table_input
                )
                logger.info(
                    "glue_table_updated",
                    table=table_cfg.name,
                    database=self._database,
                )
            else:
                raise
