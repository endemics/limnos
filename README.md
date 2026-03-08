# Limnos

Natural language querying of S3 Parquet/Iceberg data lakes via the Model Context Protocol.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Phase 1: Python MCP Server (stdio / streamable HTTP)       │
│                                                             │
│  Claude Desktop / IDE ──► Python MCP Server ──► S3          │
│                               │                             │
│                           DuckDB (primary)                  │
│                           Athena (fallback)                 │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  Phase 2: Go HTTP Gateway (optional, for teams)             │
│                                                             │
│  Claude (user A) ──┐                                        │
│  Claude (user B) ──┼──► Go Gateway ──► Python Workers ──► S3│
│  Claude (user C) ──┘   (auth, queue,   (DuckDB pool)        │
│                         rate limits)                        │
└─────────────────────────────────────────────────────────────┘
```

## Phase 1: Python MCP Server

### Quickstart

```bash
cd server
pip install -r requirements.txt

# Configure
cp ../config/config.example.yaml ../config/config.yaml
# Edit config.yaml with your S3 paths and AWS credentials

# Run (stdio, for Claude Desktop)
python main.py

# Run (HTTP, for shared access)
python main.py --transport http --port 8000
```

### Claude Desktop Configuration

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

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

### Tools Available

| Tool | Description |
|------|-------------|
| `datalake_list_datasets` | Browse registered tables and S3 paths |
| `datalake_describe_table` | Schema, partitions, row count, size |
| `datalake_sample_data` | Return N rows cheaply |
| `datalake_estimate_query` | Cost + bytes estimate before running |
| `datalake_query` | Execute NL or SQL query with cost gate |
| `datalake_refresh_schema` | Force re-scan of table metadata |

## Phase 2: Go HTTP Gateway

For teams: wraps the Python workers behind a production HTTP layer.

```bash
cd gateway
go build -o bin/gateway ./cmd/gateway
./bin/gateway --config ../config/config.yaml --workers 4
```

See `gateway/README.md` for full setup.

## Configuration

See `config/config.example.yaml` for all options.

### Supported Table Formats

| Format | Status | Notes |
|--------|--------|-------|
| `parquet` | ✅ Supported | Hive-style partitioning, columnar pruning |
| `iceberg` | ✅ Supported | Direct S3 metadata, exact row counts |
| `csv` | 🔜 Planned | Auto-detect schema; configurable delimiter |
| `json` | 🔜 Planned | Records, array, or auto format |
| `ndjson` | 🔜 Planned | Newline-delimited JSON (log files) |
| `txt` | 🔜 Planned | Single-column `line VARCHAR` |

> **Flat file formats (CSV, JSON, NDJSON, TXT)** are planned. Schema is detected once on first `describe_table` and cached — subsequent queries use the SQLite cache with no re-scanning. A one-time 10k-row sample is used to estimate rows-per-byte for cost prediction. See [docs/limnos.md](docs/limnos.md) for the full design.

## Development

### Prerequisites

- Python 3.11+
- Go 1.21+
- [golangci-lint](https://golangci-lint.run/) (installed by `make dev-tools`)

### First-time setup

```bash
# Install Go tools, Python dev dependencies, and the pre-commit hook
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

`make dev-tools` (or `make install-hooks`) configures git to run `make check`
before every commit, keeping lint and tests green locally:

```bash
make install-hooks   # one-time setup; re-run after cloning
```

To bypass in exceptional cases (e.g. a work-in-progress commit):

```bash
git commit --no-verify -m "wip: ..."
```

### CI

GitHub Actions runs the same `make check` quality gate on every push and pull
request to `main`, with coverage artifacts uploaded for each run:

- **Python coverage** → `coverage-python.xml`
- **Go coverage** → `coverage.txt` (view locally with `make test-coverage-html`)

## Project Structure

```
limnos/
├── server/                    # Phase 1: Python MCP server
│   ├── main.py                # Entry point
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
│   │   ├── list_datasets.py
│   │   ├── describe_table.py
│   │   ├── sample_data.py
│   │   ├── estimate_query.py
│   │   ├── query.py
│   │   └── refresh_schema.py
│   ├── tests/                 # Python unit tests
│   └── requirements.txt
├── gateway/                   # Phase 2: Go HTTP gateway
│   ├── cmd/gateway/main.go
│   └── internal/
│       ├── auth/              # API key auth + budget enforcement
│       ├── mcp/               # MCP protocol proxy
│       └── queue/             # Worker pool + health checks
├── .githooks/
│   └── pre-commit             # Runs make check before every commit
├── .github/workflows/
│   └── ci.yml                 # CI: lint + test + coverage on push/PR
├── config/
│   └── config.example.yaml
└── Makefile
```
