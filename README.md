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

## Project Structure

```
limnos/
├── server/                    # Phase 1: Python MCP server
│   ├── main.py                # Entry point
│   ├── catalog/
│   │   ├── schema_cache.py    # SQLite-backed metadata cache
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
│   └── requirements.txt
├── gateway/                   # Phase 2: Go HTTP gateway
│   ├── cmd/gateway/main.go
│   └── internal/
│       ├── mcp/               # MCP protocol proxy
│       ├── queue/             # Worker queue
│       └── auth/              # Auth middleware
├── config/
│   └── config.example.yaml
├── tests/
└── docs/
```
