# Limnos: Natural Language Query of S3 Data Lakes via MCP
## Architecture, Implementation & Cost Analysis

---

## 1. Problem Statement

Modern data lakes store large volumes of data in S3 using open formats (Parquet, Iceberg) independently of the applications that produce them. While tools like AWS Athena can query this data, they present two friction points:

- **Cost**: Athena charges $5/TB scanned — exploratory, conversational queries add up quickly
- **Knowledge barrier**: Users must know SQL, understand table schemas, and be aware of partitioning to query efficiently

An MCP (Model Context Protocol) server sitting in front of the data lake, backed by a local query engine, can eliminate both problems.

---

## 2. Architecture

```
User (Claude Desktop / IDE)
        │
        ▼
   MCP Client (Claude)
        │  natural language
        ▼
   ┌─────────────────────────────────────┐
   │  Go Gateway (Phase 2 — multi-user)  │
   │  API key auth, per-user budgets,    │
   │  load balancing, health checks      │
   └──────────────┬──────────────────────┘
                  │ HTTP proxy
        ┌─────────┴─────────┐
        │                   │
        ▼                   ▼
   MCP Server          MCP Server      (worker pool)
   ┌──────────────────────────────────────┐
   │  Tool Layer (FastMCP)                │
   │  datalake_list_datasets              │
   │  datalake_describe_table             │
   │  datalake_sample_data                │
   │  datalake_estimate_query             │
   │  datalake_query                      │
   │  datalake_refresh_schema             │
   │                                      │
   │  Schema Cache (SQLite)               │
   │  Partition Metadata Index            │
   │                                      │
   │  Query Engine: DuckDB                │
   │  (httpfs extension → S3)             │
   └──────────────────────────────────────┘
        │
        │ columnar reads (only needed columns + row groups)
        ▼
   S3 (Parquet / Iceberg)
        │
        │  fallback for very large scans
        ▼
   AWS Athena
```

### Design Principles

1. **DuckDB as primary engine** — queries S3 directly via `httpfs`, runs in-process, no infrastructure
2. **Athena as fallback** — triggered only when estimated scan exceeds configurable threshold (e.g. 10 GB)
3. **Schema cache** — partition metadata, column stats, and row counts stored locally in SQLite; refreshed on demand
4. **Cost-first query planning** — every query is estimated before execution; user approves if cost exceeds threshold
5. **Predicate pushdown** — partition filters are injected automatically based on WHERE clause analysis
6. **Go gateway** — optional HTTP proxy layer for multi-user deployments with API key auth and per-user spend budgets

---

## 3. MCP Tools Exposed

| Tool | Description | When Used |
|------|-------------|-----------|
| `datalake_list_datasets` | Browse S3 prefixes registered as tables | Orientation / discovery |
| `datalake_describe_table` | Schema, partitions, row count, size estimate | Before writing queries |
| `datalake_sample_data` | Return N rows cheaply (uses LIMIT) | Understand data shape |
| `datalake_estimate_query` | Return cost + bytes estimate before running | Always called before `query` |
| `datalake_query` | Run NL or SQL query; supports `explain_only=true` to preview SQL without executing | Main workhorse |
| `datalake_refresh_schema` | Force re-scan of S3 metadata for a table | After schema changes |

---

## 4. Implementation

### 4.1 Project Structure

```
limnos/
├── server/
│   ├── main.py                  # MCP server entry point (FastMCP)
│   ├── config.py                # Config schema + loader (Pydantic)
│   ├── requirements.txt         # Python dependencies
│   ├── engine/
│   │   ├── duckdb_engine.py     # DuckDB query execution + S3 setup
│   │   ├── athena_engine.py     # Athena fallback
│   │   └── cost_estimator.py    # Pre-query cost estimation
│   ├── catalog/
│   │   ├── schema_cache.py      # SQLite-backed schema/partition cache
│   │   ├── result_cache.py      # Query result cache (SQLite / DuckDB / Redis backends)
│   │   ├── iceberg.py           # Direct S3 Iceberg metadata reader
│   │   └── hive.py              # Hive-style partition discovery
│   ├── tests/
│   │   └── test_result_cache.py # Unit tests for result cache backends
│   └── tools/
│       ├── list_datasets.py     # datalake_list_datasets
│       ├── describe_table.py    # datalake_describe_table
│       ├── sample_data.py       # datalake_sample_data, datalake_estimate_query, datalake_refresh_schema
│       └── query.py             # datalake_query (NL→SQL via Claude + execution)
├── gateway/
│   ├── cmd/gateway/main.go      # Go gateway entry point
│   └── internal/
│       ├── auth/apikey.go       # API key auth + per-user spend tracking
│       ├── mcp/proxy.go         # HTTP reverse proxy to worker pool
│       └── queue/worker_pool.go # Python worker process lifecycle + load balancing
├── config/
│   └── config.example.yaml      # Table registry, thresholds, AWS config
├── docker-compose.yml
└── Makefile
```

### 4.2 Server Entry Point

The server uses [FastMCP](https://github.com/jlowin/fastmcp) with a lifespan context to share engine and cache instances across tool calls.

```python
# server/main.py
from mcp.server.fastmcp import FastMCP
from contextlib import asynccontextmanager

@asynccontextmanager
async def app_lifespan(server: FastMCP):
    cfg = load_config(os.environ.get("CONFIG_PATH", "config/config.yaml"))
    cache = SchemaCache(cfg.cache.db_path)
    yield {
        "config":         cfg,
        "cache":          cache,
        "duckdb_engine":  DuckDBEngine(cfg),
        "athena_engine":  AthenaEngine(cfg),
        "cost_estimator": CostEstimator(cfg, cache),
        "result_cache":   make_result_cache(...),
    }

mcp = FastMCP("limnos", lifespan=app_lifespan)

list_datasets.register(mcp)
describe_table.register(mcp)
sample_data.register(mcp)
estimate_query.register(mcp)
refresh_schema.register(mcp)
query.register(mcp)

# Usage:
#   python main.py                         # stdio (Claude Desktop)
#   python main.py --transport http        # streamable HTTP on port 8000
```

### 4.3 DuckDB Engine

```python
# server/engine/duckdb_engine.py
import duckdb

class DuckDBEngine:
    def __init__(self, config: Config):
        self._con = duckdb.connect(database=":memory:")
        self._con.execute("INSTALL httpfs; LOAD httpfs;")
        self._con.execute("INSTALL iceberg; LOAD iceberg;")
        aws = config.aws
        self._con.execute(f"SET s3_region = '{aws.region}';")
        if aws.access_key_id and aws.secret_access_key:
            self._con.execute(f"SET s3_access_key_id = '{aws.access_key_id}';")
            self._con.execute(f"SET s3_secret_access_key = '{aws.secret_access_key}';")
        # If credentials are empty, DuckDB uses the AWS credential chain

    def query(self, sql: str, row_limit: int = 1000) -> QueryResult: ...
    def explain(self, sql: str) -> str: ...
    def get_parquet_schema(self, s3_path: str) -> list[dict]: ...
    def estimate_row_count(self, s3_path: str, fmt: str) -> int: ...
```

> **Note:** DuckDB connections are not thread-safe. In HTTP mode, each worker process gets its own `DuckDBEngine` instance.

### 4.4 Cost Estimator

```python
# server/engine/cost_estimator.py

# AWS Athena pricing (us-east-1, March 2026)
ATHENA_PRICE_PER_TB    = 5.00         # USD per TB scanned
ATHENA_MIN_BYTES       = 10 * 1024**2 # 10 MB minimum per query
S3_GET_PRICE_PER_1000  = 0.0004       # USD per 1000 GET requests

@dataclass
class CostEstimate:
    recommended_engine: str        # "duckdb" or "athena"
    estimated_bytes: int
    estimated_files: int
    s3_get_requests: int
    athena_cost_usd: float
    s3_get_cost_usd: float
    total_cost_usd: float
    confidence: str                # "high" / "medium" / "low"
    partition_filter_detected: bool
    column_filter_fraction: float
    warning: Optional[str]
    block: bool                    # True when cost exceeds block_threshold_usd

class CostEstimator:
    def estimate(self, table_name: str, sql: str) -> CostEstimate: ...
```

### 4.5 Schema Cache

```python
# server/catalog/schema_cache.py
# SQLite-backed store for table metadata (columns, partitions, sizes).
# Populated by datalake_describe_table; auto-refreshed when stale.

@dataclass
class TableMeta:
    table_name: str
    s3_path: str
    format: str                          # "parquet" or "iceberg"
    columns: list[ColumnMeta]
    partition_columns: list[PartitionColumnMeta]
    total_files: int
    total_rows: Optional[int]
    total_bytes: int
    total_partitions: int
    size_human: str
    description: str
    freshness_hours: float               # property derived from last_refreshed
    last_refreshed: datetime

class SchemaCache:
    def get(self, table_name: str) -> Optional[TableMeta]: ...
    def put(self, meta: TableMeta) -> None: ...
    def list_tables(self) -> list[str]: ...
    def delete(self, table_name: str) -> None: ...
    def close(self) -> None: ...
```

### 4.6 NL → SQL (Query Tool)

Natural language questions are converted to SQL by calling the Claude API before execution:

```python
# server/tools/query.py  (simplified)
import anthropic

async def _nl_to_sql(question: str, meta: TableMeta, table_cfg: TableConfig) -> str:
    client = anthropic.AsyncAnthropic()
    msg = await client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=512,
        system="You are a SQL expert. Convert the user's question to a single DuckDB SQL query...",
        messages=[{"role": "user", "content": prompt}],
    )
    return msg.content[0].text.strip()
```

### 4.7 Go Gateway (Phase 2)

The Go gateway (`gateway/`) is an optional HTTP proxy for multi-user deployments:

```
gateway/
├── cmd/gateway/main.go          # --config, --workers, --port flags
└── internal/
    ├── auth/apikey.go           # X-API-Key / Bearer auth; daily per-user USD budget
    ├── mcp/proxy.go             # Reverse proxy; SSE-aware (disables buffering)
    └── queue/worker_pool.go     # Spawns N Python workers on ports 9100–910N
                                 # Health checks every 10s; auto-restarts crashed workers
```

HTTP endpoints:
- `POST /mcp` — MCP streamable HTTP (requires `X-API-Key`)
- `GET /health` — health check (no auth)
- `GET /metrics` — worker pool status JSON (no auth)

---

## 5. Cost Deep Dive

### 5.1 AWS Pricing Components

| Service | Pricing Unit | Rate (us-east-1) | Notes |
|---------|-------------|-------------------|-------|
| Athena queries | Per TB scanned | $5.00 / TB | 10 MB minimum per query |
| S3 Standard storage | Per GB/month | $0.023 / GB | First 50 TB |
| S3 GET requests | Per 1,000 | $0.0004 | Each Parquet file open = 1 GET |
| S3 data transfer | Per GB (out to internet) | $0.09 / GB | Free within same region |
| DuckDB (self-hosted) | EC2 / compute | Varies | Or runs on local machine: $0 |

### 5.2 Cost Scenarios

**Scenario A — Exploratory session, 10 queries/day, 100 GB table**

| Approach | Daily Cost | Monthly Cost |
|----------|-----------|--------------|
| Athena (full scans) | ~$5.00 | ~$150 |
| Athena (with partition filters) | ~$0.25 | ~$7.50 |
| DuckDB via MCP (filtered) | ~$0.001 | ~$0.03 |
| DuckDB via MCP (full scan) | ~$0.04 | ~$1.20 |

_DuckDB cost is S3 GET requests only; the query engine is free._

**Scenario B — Large table, 10 TB, daily aggregation queries**

| Approach | Per-query Cost |
|----------|---------------|
| Athena (full scan) | $50.00 |
| Athena (date partition filtered, 1 day) | $0.14 |
| DuckDB (1 day partition) | $0.002 |
| DuckDB (full table) | Not recommended (memory/time) |

### 5.3 Pre-query Cost Prediction Model

The estimator works in three steps:

**Step 1 — Partition pruning estimate**
```
partitions_accessed = total_partitions × filter_selectivity
bytes_after_partition = total_bytes × (partitions_accessed / total_partitions)
```

**Step 2 — Column pruning estimate**
```
# Each column's byte contribution is proportional to its average value size
col_fraction = Σ(size_of_selected_columns) / Σ(size_of_all_columns)
bytes_after_column_pruning = bytes_after_partition × col_fraction
```

**Step 3 — Athena billing calculation**
```
billable_bytes = max(bytes_after_column_pruning, 10_485_760)  # 10 MB min
athena_cost = (billable_bytes / 1_099_511_627_776) × 5.00     # per TB
```

### 5.4 Making Estimates Accurate

Accurate estimation requires up-to-date table statistics. The schema cache stores:

- **Parquet**: Row group metadata (min/max values per column, row count, compressed size) — readable from file footer, zero-cost
- **Iceberg**: Manifest files contain file-level stats — read from `metadata/` prefix in S3
- **Hive-partitioned Parquet**: Infer partition list from S3 `ListObjects` calls (priced at $0.005/1000 requests)

To keep estimates fresh without constant re-scanning, implement a **lazy refresh** strategy: re-read metadata only when a partition's last-modified timestamp in S3 changes.

---

## 6. Iceberg Integration

For Iceberg tables, the MCP server reads metadata directly from S3 without requiring a catalog server (`catalog/iceberg.py`):

```
s3://bucket/prefix/
└── metadata/
    ├── version-hint.text           # → current version number
    ├── v{N}.metadata.json          # schema, partition spec, snapshots
    └── snap-*.avro                 # manifest lists (file-level stats)
```

For production, consider using PyIceberg with a Glue or REST catalog instead, which provides richer metadata and transactional guarantees.

Hive-style partition discovery (`catalog/hive.py`) scans S3 prefixes and parses `col=value` path segments to build a partition index used by the cost estimator.

---

## 7. Security & IAM

The MCP server needs a tightly-scoped IAM role:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3ReadOnly",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"],
      "Resource": [
        "arn:aws:s3:::your-datalake-bucket",
        "arn:aws:s3:::your-datalake-bucket/*"
      ]
    },
    {
      "Sid": "AthenaFallback",
      "Effect": "Allow",
      "Action": [
        "athena:StartQueryExecution",
        "athena:GetQueryExecution",
        "athena:GetQueryResults"
      ],
      "Resource": "*"
    },
    {
      "Sid": "GlueSchemaRead",
      "Effect": "Allow",
      "Action": ["glue:GetTable", "glue:GetDatabase", "glue:GetPartitions"],
      "Resource": "*"
    },
    {
      "Sid": "GlueTableProvision",
      "Effect": "Allow",
      "Action": ["glue:CreateTable", "glue:UpdateTable"],
      "Resource": "arn:aws:glue:*:*:table/your-database/*",
      "Condition": {"StringEquals": {"glue:resourceTag/ManagedBy": "limnos"}}
    }
  ]
}
```

> **Note:** `GlueTableProvision` is only needed when using flat file formats (CSV/JSON/NDJSON/TXT). The `ManagedBy` condition limits scope to tables tagged by Limnos. For Parquet/Iceberg-only deployments this statement can be omitted.

Additional recommendations:
- Run the MCP server as an ECS task or local process using an **instance profile** — no long-lived credentials
- Restrict `s3:GetObject` to specific S3 prefixes matching the registered tables
- Log all query executions with the user identity for audit purposes
- In Phase 2 (gateway), API keys map to `UserInfo` with a `budget_usd` field; spend is tracked in-memory and reset daily

---

## 8. Deployment Options

| Option | Latency | Cost | Complexity | Best For |
|--------|---------|------|------------|----------|
| Local process (Claude Desktop) | Low | Free | Low | Individual analyst |
| Docker on EC2 | Medium | ~$10–50/mo | Medium | Small team |
| AWS Lambda (MCP over HTTP) | High (cold start) | Very low | Medium | Infrequent use |
| ECS Fargate service | Low | ~$30–100/mo | Medium-high | Team / production |

For most teams, **Docker on a small EC2 instance** (t3.medium, ~$30/mo) is the right starting point. The DuckDB engine benefits from having local RAM for intermediate results, so Lambda is a poor fit for complex queries.

**Docker Compose** (`docker-compose.yml`) supports two profiles:
- Default: Python MCP server only (`make docker-up`)
- `gateway` profile: full stack with Go gateway in front (`make docker-up-gateway`)

---

## 9. Implementation Roadmap

### Phase 1 — Proof of Concept ✅
- [x] MCP server scaffold (Python, FastMCP, stdio transport)
- [x] DuckDB + httpfs wired to S3
- [x] Static table registry (YAML config)
- [x] `datalake_list_datasets`, `datalake_describe_table`, `datalake_sample_data` tools
- [x] Basic NL→SQL using Claude API (`claude-sonnet-4-6`)

### Phase 2 — Cost Awareness ✅
- [x] Schema cache (SQLite)
- [x] Partition metadata indexing (Hive-style)
- [x] Cost estimator with pre-query gate (warn + block thresholds)
- [x] `datalake_estimate_query` tool exposed to MCP client

### Phase 3 — Production Hardening ✅ (partial)
- [x] Athena fallback for large scans
- [x] Iceberg metadata integration (direct S3 reads)
- [x] Query result cache (SQLite / DuckDB / Redis backends)
- [ ] Schema auto-refresh (EventBridge cron)
- [ ] Flat file formats (CSV, JSON, NDJSON, TXT) — see Section 11

### Phase 4 — Multi-user / Team ✅ (partial)
- [x] MCP over HTTP/SSE transport (streamable HTTP)
- [x] Go gateway with API key auth and per-user query budgets
- [x] Worker pool with health checks and auto-restart
- [ ] Audit log to S3 / CloudWatch
- [ ] Web UI for schema browsing (optional)

---

## 10. Key Trade-offs Summary

| Factor | Reality |
|--------|---------|
| **Query cost** | Near-zero for filtered queries via DuckDB; S3 GET costs only |
| **Performance** | Excellent for partition-filtered queries; degrades on TB-scale full scans |
| **Scale ceiling** | DuckDB is single-node; ~100–500 GB per query is practical limit |
| **Iceberg support** | DuckDB Iceberg extension is capable but still maturing |
| **Accuracy of estimates** | High when metadata is fresh; low for infrequently-refreshed tables |
| **Security** | Read-only IAM scope; no data leaves S3 region |
| **Operational burden** | Minimal — one Python process (stdio) or Go gateway + worker pool (HTTP) |

---

---

## 11. Planned: Flat File Format Support (CSV, JSON, NDJSON, TXT)

> See [docs/flat-file-formats.md](flat-file-formats.md) for the full implementation specification.

Many data lakes contain raw flat files alongside Parquet/Iceberg tables — CSVs exported from operational systems, JSON API dumps, NDJSON log streams, and plain-text files. DuckDB natively reads all of these, so adding support is primarily plumbing across the existing format-dispatch points.

### 11.1 DuckDB Table Source per Format

| Format | DuckDB SQL source |
|--------|-------------------|
| `csv`  | `read_csv('s3://…/**/*.csv', hive_partitioning=true, auto_detect=true, delim=',')` |
| `json` | `read_json('s3://…/**/*.json', format='auto')` |
| `ndjson` | `read_json('s3://…/**/*.ndjson', format='newline_delimited')` |
| `txt`  | `read_csv('s3://…/**/*.txt', sep='\n', header=false, columns={'line': 'VARCHAR'})` |

### 11.2 Schema Detection & Caching Strategy

Unlike Parquet (which exposes schema in the file footer at zero read cost), flat files must be partially read to determine column names and types. To avoid re-reading on every query, schema detection runs **once** during the first `datalake_describe_table` call and stores results in the existing SQLite schema cache:

1. **Schema detection** — DuckDB's `DESCRIBE SELECT * FROM read_csv/read_json(…) LIMIT 0` reads only the file header or first JSON object. Result stored in `TableMeta.columns`.
2. **Bytes-per-row sample** — A `SELECT COUNT(*) … LIMIT 10000` scan estimates average row size. Stored as `TableMeta.bytes_per_row_estimate` (new field). Used by the cost estimator to derive `estimated_rows ≈ total_bytes / bytes_per_row_estimate` without re-reading files.
3. **File inventory** — `discover_partitions()` in `catalog/hive.py` (already format-agnostic) provides file count and total bytes.

All subsequent `datalake_query`, `datalake_estimate_query`, and `datalake_sample_data` calls read only from the SQLite cache — no S3 file access for metadata.

### 11.3a Athena Fallback via Glue Auto-Provisioning

Athena CAN query flat files — it just needs a Glue external table definition. Since schema detection (Section 11.2) already produces column names, types, and partition columns from `ColumnMeta` objects (which already store Hive-compatible type strings), a new `GlueProvisioner` class in `server/catalog/glue.py` can create or update a Glue table at the end of `_scan_metadata()`. This is idempotent and runs only on `describe_table` — not on every query.

The correct SerDe is selected by format (`LazySimpleSerDe` for CSV/TXT, `openx.JsonSerDe` for JSON/NDJSON). After provisioning, Athena queries flat files exactly as it does Parquet. Requires `glue:CreateTable` and `glue:UpdateTable` IAM permissions (see Section 7).

### 11.3b Configuration Changes (`server/config.py`)

New optional fields on `TableConfig`:

```yaml
tables:
  - name: orders_csv
    s3_path: "s3://my-bucket/exports/orders/"
    format: csv
    delimiter: ","          # optional, default ","
    has_header: true         # optional, default true

  - name: events_ndjson
    s3_path: "s3://my-bucket/logs/events/"
    format: ndjson

  - name: api_responses
    s3_path: "s3://my-bucket/api/responses/"
    format: json
    json_format: "records"   # "records" | "array" | "auto"

  - name: audit_log
    s3_path: "s3://my-bucket/audit/"
    format: txt
```

### 11.4 Limitations vs Parquet/Iceberg

| Limitation | Detail |
|---|---|
| **No columnar pruning** | Entire file is read even for single-column queries; `column_filter_fraction` is always `1.0` |
| **Row count is estimated** | Derived from `total_bytes / bytes_per_row_estimate` after a one-time 10k-row sample; not exact |
| **Glue required for Athena** | Glue external table auto-provisioned during first `describe_table`; requires `glue:CreateTable`/`glue:UpdateTable` IAM — see Section 7 |
| **Schema detection reads data** | First `describe_table` call reads file content (header + 10k rows); subsequent calls are cache-only |
| **Cost estimate confidence** | Always `"medium"` at best — no row-group statistics available |
| **Partition pushdown** | Hive-style path partitioning (`col=value/`) works for CSV; JSON/NDJSON files rarely use it |

### 11.5 Files to Modify

| File | Change |
|---|---|
| `server/config.py` | Add `csv`, `json`, `ndjson`, `txt` to format validator; add `delimiter`, `has_header`, `json_format`, `glob_pattern` on `TableConfig`; add `glue_database` to `AWSConfig` |
| `server/engine/duckdb_engine.py` | Add `get_flat_file_schema(s3_path, fmt, **opts)` → `(columns, bytes_per_row)`; update `estimate_row_count()` |
| `server/catalog/schema_cache.py` | Add `bytes_per_row_estimate: Optional[float]` to `TableMeta`; add column to SQLite schema |
| `server/catalog/glue.py` | **New** — `GlueProvisioner.sync_table()` (create/update Glue external table) |
| `server/tools/describe_table.py` | Add format branches for CSV/JSON/NDJSON/TXT; call `GlueProvisioner.sync_table()` |
| `server/tools/sample_data.py` | Add SQL source strings for each new format |
| `server/tools/query.py` | Update `NL_TO_SQL_SYSTEM` prompt and `_fallback_sql()` with new format references |
| `server/engine/cost_estimator.py` | Set `column_filter_fraction=1.0`; derive row count from `bytes_per_row_estimate` |
| `server/tests/` | Unit tests for `get_flat_file_schema()`, `GlueProvisioner` (mocked boto3), cost estimation |
| `config/config.example.yaml` | Add example CSV/NDJSON entries; add `glue_database` |

---

*Last updated: March 2026*
