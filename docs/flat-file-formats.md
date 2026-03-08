# Flat File Format Support — Implementation Specification

> **Status: Planned.** See [limnos.md § 11](limnos.md#11-planned-flat-file-format-support-csv-json-ndjson-txt) for the architectural overview.

This document is the detailed implementation reference for adding CSV, JSON, NDJSON, and TXT support to Limnos.

---

## 1. DuckDB SQL Source per Format

| Format | DuckDB SQL |
|--------|-----------|
| `csv` | `read_csv('s3://…/**/*.csv', hive_partitioning=true, auto_detect=true, delim=',')` |
| `json` | `read_json('s3://…/**/*.json', format='auto')` |
| `ndjson` | `read_json('s3://…/**/*.ndjson', format='newline_delimited')` |
| `txt` | `read_csv('s3://…/**/*.txt', sep='\n', header=false, columns={'line': 'VARCHAR'})` |

---

## 2. Athena Fallback via Glue Auto-Provisioning

Athena can query flat files if a Glue external table exists. Since schema, partition columns, and S3 path are all known after `describe_table`, Limnos can auto-provision this table — enabling Athena fallback for large flat-file scans with no manual setup.

### 2.1 New module: `server/catalog/glue.py`

```python
import boto3
from botocore.exceptions import ClientError
from catalog.schema_cache import ColumnMeta, PartitionMeta

_GLUE_TYPE_MAP = {
    "VARCHAR": "string", "TEXT": "string",
    "BIGINT": "bigint", "INTEGER": "int", "INT": "int",
    "DOUBLE": "double", "FLOAT": "float",
    "BOOLEAN": "boolean", "DATE": "date", "TIMESTAMP": "timestamp",
}

_SERDE = {
    "csv":   ("org.apache.hadoop.mapred.TextInputFormat",
               "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"),
    "json":  ("org.apache.hadoop.mapred.TextInputFormat",
               "org.openx.data.jsonserde.JsonSerDe"),
    "ndjson":("org.apache.hadoop.mapred.TextInputFormat",
               "org.openx.data.jsonserde.JsonSerDe"),
    "txt":   ("org.apache.hadoop.mapred.TextInputFormat",
               "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"),
}

class GlueProvisioner:
    def __init__(self, config):
        self._glue = boto3.client("glue", region_name=config.aws.region)
        self._database = config.aws.glue_database  # new AWSConfig field, default "default"

    def sync_table(self, table_cfg, columns: list[ColumnMeta],
                   partition_cols: list[PartitionMeta]) -> None:
        """Create or update Glue external table. Idempotent."""
        input_fmt, serde_lib = _SERDE[table_cfg.format]
        serde_params = {}
        if table_cfg.format == "csv":
            serde_params["field.delim"] = table_cfg.delimiter
            if table_cfg.has_header:
                serde_params["skip.header.line.count"] = "1"

        glue_cols = [
            {"Name": c.name, "Type": _GLUE_TYPE_MAP.get(c.dtype.upper(), c.dtype.lower())}
            for c in columns
        ]
        glue_partitions = [
            {"Name": p.name, "Type": "string"}
            for p in partition_cols
        ]
        table_input = {
            "Name": table_cfg.name.replace("-", "_"),
            "StorageDescriptor": {
                "Columns": glue_cols,
                "Location": table_cfg.s3_path,
                "InputFormat": input_fmt,
                "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                "SerdeInfo": {"SerializationLibrary": serde_lib, "Parameters": serde_params},
            },
            "PartitionKeys": glue_partitions,
            "TableType": "EXTERNAL_TABLE",
        }
        try:
            self._glue.create_table(DatabaseName=self._database, TableInput=table_input)
        except ClientError as e:
            if e.response["Error"]["Code"] == "AlreadyExistsException":
                self._glue.update_table(DatabaseName=self._database, TableInput=table_input)
            else:
                raise
```

### 2.2 Glue SerDe reference

| Format | InputFormat | SerDe library | Key parameters |
|--------|-------------|---------------|----------------|
| `csv` | `TextInputFormat` | `LazySimpleSerDe` | `field.delim`, `skip.header.line.count` |
| `json` / `ndjson` | `TextInputFormat` | `openx.JsonSerDe` | — |
| `txt` | `TextInputFormat` | `LazySimpleSerDe` | `field.delim=\n` |

### 2.3 Type mapping: `ColumnMeta.dtype` → Glue

`ColumnMeta.dtype` already stores Hive-compatible SQL-style strings (e.g. `"BIGINT"`, `"VARCHAR"`). Only a case and alias mapping is needed:

| ColumnMeta.dtype | Glue type |
|---|---|
| `VARCHAR`, `TEXT` | `string` |
| `BIGINT` | `bigint` |
| `INTEGER`, `INT` | `int` |
| `DOUBLE` | `double` |
| `FLOAT` | `float` |
| `BOOLEAN` | `boolean` |
| `DATE` | `date` |
| `TIMESTAMP` | `timestamp` |
| anything else | `.lower()` passthrough |

### 2.4 IAM requirements

In addition to the existing read-only Glue permissions, the IAM role needs:

```json
{
  "Sid": "GlueTableProvision",
  "Effect": "Allow",
  "Action": ["glue:CreateTable", "glue:UpdateTable"],
  "Resource": "arn:aws:glue:*:*:table/your-database/*"
}
```

---

## 3. Schema Cache Changes (`server/catalog/schema_cache.py`)

Add `bytes_per_row_estimate: Optional[float] = None` to `TableMeta`. Store in SQLite with an `ALTER TABLE … ADD COLUMN` migration (add on `__init__` with `IF NOT EXISTS`-style guard, since SQLite supports adding nullable columns).

Used by the cost estimator to derive:
```
estimated_rows ≈ total_bytes / bytes_per_row_estimate
```
without re-reading any files after the first `describe_table`.

---

## 4. Schema Detection: `server/engine/duckdb_engine.py`

New method `get_flat_file_schema(s3_path, fmt, **opts) -> tuple[list[dict], float]`:

```python
# Step 1 — schema (reads only header or first object):
# CSV:    DESCRIBE SELECT * FROM read_csv('{glob}', auto_detect=true) LIMIT 0
# JSON:   DESCRIBE SELECT * FROM read_json('{glob}', format='auto') LIMIT 0
# NDJSON: same with format='newline_delimited'
# TXT:    return [{"name": "line", "type": "VARCHAR"}], 80.0  # no introspection

# Step 2 — bytes-per-row sample (run once; result stored in cache):
# SELECT COUNT(*) AS n,
#        SUM(LENGTH(CAST(row AS VARCHAR))) AS b
# FROM (SELECT * FROM {source} LIMIT 10000)
# bytes_per_row = b / n
```

Returns `(columns, bytes_per_row_estimate)`.

---

## 5. `server/tools/describe_table.py` — `_scan_metadata()`

After the existing `if format == "iceberg"` / `else` (parquet) branches, add:

```python
elif table_cfg.format in ("csv", "json", "ndjson", "txt"):
    columns, bpr = await asyncio.to_thread(
        engine.get_flat_file_schema, table_cfg.s3_path, table_cfg.format, ...
    )
    partitions, partition_cols = await asyncio.to_thread(
        discover_partitions, table_cfg.s3_path
    )
    total_bytes = sum(p.total_bytes for p in partitions)
    total_files = sum(p.file_count for p in partitions)
    meta = TableMeta(
        ...,
        bytes_per_row_estimate=bpr,
    )
    # Auto-provision Glue table so Athena fallback works:
    if table_cfg.format != "txt":  # TXT: Athena not useful for unstructured text
        GlueProvisioner(config).sync_table(table_cfg, columns, partition_cols)
```

---

## 6. `server/tools/sample_data.py` & `query.py`

Add format branches using the SQL sources from Section 1. Update `NL_TO_SQL_SYSTEM` prompt with the new `read_csv`/`read_json` references.

---

## 7. `server/engine/cost_estimator.py`

- `column_filter_fraction = 1.0` for all flat file formats (no columnar pruning)
- Row count: `int(meta.total_bytes / meta.bytes_per_row_estimate)` when `meta.total_rows == 0`
- Athena is available (Glue table provisioned) — no special-casing needed

---

## 8. Config changes

### `server/config.py`
```python
class TableConfig(BaseModel):
    ...
    delimiter: str = ","
    has_header: bool = True
    json_format: str = "auto"       # "records" | "array" | "newline_delimited" | "auto"
    glob_pattern: Optional[str] = None  # override default **/*.{ext}

class AWSConfig(BaseModel):
    ...
    glue_database: str = "default"
```

Format validator accepts: `"parquet"`, `"iceberg"`, `"csv"`, `"json"`, `"ndjson"`, `"txt"`.

### `config/config.example.yaml`
```yaml
aws:
  glue_database: "my_datalake"

tables:
  - name: orders_csv
    s3_path: "s3://my-bucket/exports/orders/"
    format: csv
    delimiter: ","

  - name: events_ndjson
    s3_path: "s3://my-bucket/logs/events/"
    format: ndjson
```

---

## 9. Files to Modify

| File | Change |
|---|---|
| `server/config.py` | Format validator; `delimiter`, `has_header`, `json_format`, `glob_pattern` on `TableConfig`; `glue_database` on `AWSConfig` |
| `server/engine/duckdb_engine.py` | `get_flat_file_schema()` + `estimate_row_count()` |
| `server/catalog/schema_cache.py` | `bytes_per_row_estimate` field + SQLite migration |
| `server/catalog/glue.py` | **New** — `GlueProvisioner` |
| `server/tools/describe_table.py` | Flat file branches + `GlueProvisioner` call |
| `server/tools/sample_data.py` | SQL source strings |
| `server/tools/query.py` | Prompt + fallback SQL |
| `server/engine/cost_estimator.py` | `column_filter_fraction`, row count from estimate |
| `server/tests/` | Unit tests for `get_flat_file_schema()`, `GlueProvisioner.sync_table()` (mock boto3), cost estimator |
| `config/config.example.yaml` | Example CSV/NDJSON entries; `glue_database` |

---

## 10. Verification

1. Add CSV + NDJSON tables to `config/config.example.yaml`
2. `datalake_describe_table` — schema detected, Glue table created, metadata cached in SQLite
3. `datalake_sample_data` — rows returned via DuckDB from S3 flat file
4. `datalake_query` with NL — valid SQL generated and executed via DuckDB
5. Large file: `datalake_estimate_query` — Athena recommended; runs via Athena against Glue table
6. Second `datalake_describe_table` — served from SQLite, no S3 file read
7. Unit tests pass: schema detection, Glue provisioning (mocked), cost row count derivation
