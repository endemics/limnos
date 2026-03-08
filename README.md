# Limnos

Natural language querying of S3 Parquet/Iceberg data lakes via the Model Context Protocol (MCP). Ask Claude questions about your data; Limnos handles schema discovery, query planning, execution, and cost control — no SQL required.

## Cost control

Every query goes through a cost estimator before it runs. Limnos calculates the bytes to be scanned and the estimated USD cost, then applies configurable gates:

- **Warn threshold** (default $0.10) — Claude is told the cost and can decide whether to proceed
- **Block threshold** (default $1.00) — query is refused unless explicitly overridden with `force=true`

Queries are executed locally via DuckDB wherever possible (fast, free). Only queries that exceed the configured scan limit escalate to Athena, which incurs AWS charges. This keeps day-to-day exploratory queries essentially free.

A query result cache (SQLite, DuckDB, or Redis) means repeated identical queries are served instantly without re-hitting S3 or Athena at all.

## Available tools

These MCP tools are exposed to Claude:

| Tool | Description |
|------|-------------|
| `datalake_list_datasets` | Browse registered tables and S3 paths |
| `datalake_describe_table` | Schema, partitions, row count, size |
| `datalake_sample_data` | Return N rows without a full scan |
| `datalake_estimate_query` | Cost and bytes estimate before running |
| `datalake_query` | Execute a natural language or SQL query with cost gate |
| `datalake_refresh_schema` | Force re-scan of table metadata |

## How it works

```
Claude Desktop / IDE
        │
        ▼
  Python MCP Server          ← schema cache (SQLite)
        │                    ← result cache (SQLite / DuckDB / Redis)
        ├── DuckDB (local, primary engine)
        └── Athena (fallback for large scans)
                │
                ▼
               S3 (Parquet / Iceberg)
```

For multi-user deployments, a Go gateway sits in front of the Python server and adds API-key authentication, per-user spend budgets, and load balancing across a worker pool. See [Deployment: team with access controls](#team-with-access-controls) below.

---

## Deployment

### Individual

Run the Python MCP server locally in stdio mode and connect it to Claude Desktop. Queries run entirely on your machine via DuckDB; Athena is only used as a fallback.

**Install:**

```bash
cd server
pip install -r requirements.txt
cp ../config/config.example.yaml ../config/config.yaml
# Edit config.yaml: add your S3 paths and AWS credentials
```

**Claude Desktop config** (`~/Library/Application Support/Claude/claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "limnos": {
      "command": "python",
      "args": ["/path/to/limnos/server/main.py"],
      "env": {
        "AWS_REGION": "us-east-1",
        "CONFIG_PATH": "/path/to/limnos/config/config.yaml"
      }
    }
  }
}
```

Claude Desktop starts the server automatically when you open a conversation.

---

### Shared server (small team)

Run the Python MCP server in HTTP mode on a shared host. All team members point their Claude Desktop or IDE at the same URL. Suitable for small trusted teams — there is no per-user authentication at this level.

```bash
cd server
python main.py --transport http --port 8000
```

Each user's Claude Desktop config:

```json
{
  "mcpServers": {
    "limnos": {
      "url": "http://your-server:8000/mcp"
    }
  }
}
```

Tune `engine.worker_pool_size` in `config.yaml` to match concurrent users.

---

### Team with access controls

For organisations that need per-user API keys, daily spend caps, and audit logging, the Go gateway wraps a pool of Python workers and adds a proper auth layer.

```bash
# Build
make build   # outputs build/gateway

# Configure API keys
export GATEWAY_API_KEYS='{
  "sk-alice": {"user_id": "alice", "budget_usd": 5.0},
  "sk-bob":   {"user_id": "bob",   "budget_usd": 10.0}
}'

# Run (spawns 4 Python workers internally)
./build/gateway --config config/config.yaml --workers 4 --port 8080
```

Each user's Claude Desktop config:

```json
{
  "mcpServers": {
    "limnos": {
      "url": "http://your-gateway:8080/mcp",
      "headers": { "X-API-Key": "sk-alice" }
    }
  }
}
```

The gateway health-checks workers every 10 seconds and restarts any that crash. All requests are logged as structured JSON with user, cost, and duration for audit purposes.

See [docs/gateway.md](docs/gateway.md) for the full reference: flags, timeout settings, systemd example, and metrics endpoint.

---

## Configuration

See [`config/config.example.yaml`](config/config.example.yaml) for the annotated full reference. Key sections:

**Tables** — register S3 paths and their format:

```yaml
tables:
  - name: orders
    s3_path: "s3://your-datalake/warehouse/orders/"
    format: parquet
    partition_columns:
      - {name: order_date, type: date}
      - {name: region,     type: string}
    description: "Customer orders, partitioned by date and region"

  - name: events
    s3_path: "s3://your-datalake/warehouse/events/"
    format: iceberg
```

**Cost gates:**

```yaml
cost_gates:
  warn_threshold_usd: 0.10   # Claude is informed; can choose to proceed
  block_threshold_usd: 1.00  # Hard block; requires force=true to override
```

**Query engine:**

```yaml
engine:
  duckdb_max_scan_bytes: 10_737_418_240  # 10 GB — above this, falls back to Athena
  default_row_limit: 1000
  query_timeout_seconds: 120
```

**Result cache:**

```yaml
cache:
  result_cache_enabled: true
  result_cache_ttl_seconds: 3600
  result_cache_backend: sqlite   # sqlite | duckdb | redis
  # redis_url: "redis://localhost:6379"  # for multi-node deployments
```

### Supported table formats

| Format | Status | Notes |
|--------|--------|-------|
| `parquet` | Supported | Hive-style partitioning, columnar pruning |
| `iceberg` | Supported | Direct S3 metadata, exact row counts |
| `csv` | Planned | Auto-detect schema; configurable delimiter |
| `json` | Planned | Records, array, or auto format |
| `ndjson` | Planned | Newline-delimited JSON (log files) |
| `txt` | Planned | Single-column `line VARCHAR` |

Flat file formats (CSV, JSON, NDJSON, TXT) detect schema once on first `describe_table` and cache it — subsequent queries use the SQLite cache with no re-scanning. Athena fallback is supported via auto-provisioned Glue external tables. See [docs/flat-file-formats.md](docs/flat-file-formats.md) for the full design.

---

## Development

### Prerequisites

- Python 3.11+
- Go 1.21+
- [golangci-lint](https://golangci-lint.run/) (installed by `make dev-tools`)

### First-time setup

```bash
make dev-tools
pip install -r server/requirements.txt pytest pytest-cov ruff
```

### Common tasks

| Command | Description |
|---------|-------------|
| `make test` | Run all tests (Go + Python) |
| `make test-gateway` | Go tests only |
| `make test-server` | Python tests only |
| `make test-coverage` | Tests + coverage reports for both |
| `make lint` | Lint Go (golangci-lint) + Python (ruff) |
| `make fmt` | Format Go + Python |
| `make check` | fmt + vet + lint + test (full quality gate) |
| `make build` | Build the Go gateway binary |

### Pre-commit hook

`make dev-tools` (or `make install-hooks`) configures git to run `make check` before every commit:

```bash
make install-hooks   # one-time; re-run after cloning
```

To bypass for a work-in-progress commit:

```bash
git commit --no-verify -m "wip: ..."
```

### CI

GitHub Actions runs the same `make check` quality gate on every push and pull request to `main`, with coverage artifacts uploaded for each run.

---

## Project structure

```
limnos/
├── server/                    # Python MCP server
│   ├── main.py                # Entry point (stdio or HTTP transport)
│   ├── catalog/
│   │   ├── schema_cache.py    # SQLite-backed metadata cache
│   │   ├── result_cache.py    # Query result cache (SQLite/DuckDB/Redis)
│   │   ├── iceberg.py         # Iceberg metadata reader
│   │   └── hive.py            # Hive partition discovery
│   ├── engine/
│   │   ├── duckdb_engine.py   # DuckDB query execution
│   │   ├── athena_engine.py   # Athena fallback
│   │   └── cost_estimator.py  # Pre-query cost estimation
│   ├── tools/                 # MCP tool implementations
│   └── requirements.txt
├── gateway/                   # Go HTTP gateway (multi-user deployments)
│   ├── cmd/gateway/main.go
│   └── internal/
│       ├── auth/              # API key auth + budget enforcement
│       ├── mcp/               # MCP protocol proxy
│       └── queue/             # Worker pool + health checks
├── docs/
│   ├── gateway.md             # Gateway reference
│   ├── flat-file-formats.md   # Flat file format design
│   └── limnos.md              # Extended design notes
├── config/
│   └── config.example.yaml    # Annotated configuration reference
├── .github/workflows/ci.yml
└── Makefile
```
