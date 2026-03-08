# Limnos Gateway

The Limnos gateway is a Go HTTP server that sits between Claude clients and a pool of Python MCP workers. It adds authentication, per-user cost budgets, and automatic load balancing so multiple users can share a single Limnos deployment safely.

## When to use it

Use the gateway when you need any of the following:

- Multiple users querying the same data lake via a shared server
- Per-user API keys and daily spend budgets
- Centralised audit logging (every query logged with user, cost, duration)
- Automatic worker restart if a Python process crashes

For a single user or a small trusted team comfortable sharing credentials, running the Python server directly in HTTP mode is simpler — see the main README.

## Architecture

```
Claude (user A) ──┐
Claude (user B) ──┼──► Go Gateway ──► Python Worker 0 ──► S3
Claude (user C) ──┘   (auth, budget   Python Worker 1 ──► S3
                        round-robin)  Python Worker N ──► S3
```

Each Python worker is an independent MCP server process. The gateway spawns them on startup, health-checks them every 10 seconds, and automatically restarts any that become unhealthy.

## Build

```bash
# From the repo root
make build           # outputs build/gateway
# or
cd gateway && go build -o ../build/gateway ./cmd/gateway
```

## Run

```bash
./build/gateway \
  --config config/config.yaml \
  --workers 4 \
  --port 8080 \
  --log-level info
```

| Flag | Default | Description |
|------|---------|-------------|
| `--config` | `config/config.yaml` | Config file path; passed to each Python worker via `CONFIG_PATH` |
| `--workers` | `4` | Number of Python MCP worker processes to spawn |
| `--port` | `8080` | HTTP listen port |
| `--log-level` | `info` | Log verbosity: `debug`, `info`, `warn`, `error` |

Workers start on internal ports `9100, 9101, …` and are never exposed directly.

## API keys

API keys are configured via the `GATEWAY_API_KEYS` environment variable as a JSON object mapping each key to its user settings:

```bash
export GATEWAY_API_KEYS='{
  "sk-alice-secret": {"user_id": "alice", "budget_usd": 5.0,  "rate_limit": 60},
  "sk-bob-secret":   {"user_id": "bob",   "budget_usd": 10.0, "rate_limit": 120},
  "sk-readonly":     {"user_id": "ci",    "budget_usd": 0,    "rate_limit": 0}
}'
```

| Field | Description |
|-------|-------------|
| `user_id` | Identifier used in logs and spend tracking |
| `budget_usd` | Daily spend cap in USD; `0` = unlimited |
| `rate_limit` | Requests per minute; `0` = unlimited |

Budgets reset at midnight UTC. Spend is tracked in memory and lost on restart — if persistence matters, record spend from the structured logs.

Clients authenticate using either header format:

```
X-API-Key: sk-alice-secret
# or
Authorization: Bearer sk-alice-secret
```

## Endpoints

| Endpoint | Auth | Description |
|----------|------|-------------|
| `POST /mcp` | Required | MCP protocol proxy; supports SSE streaming |
| `GET /health` | None | Returns `{"status":"ok"}` |
| `GET /metrics` | None | Worker pool status and request counts |

## HTTP response codes

| Code | Cause |
|------|-------|
| 200 | Request routed successfully |
| 401 | Missing or invalid API key |
| 429 | User's daily budget exceeded |
| 503 | All Python workers unhealthy |

## Logging

All logs are structured JSON on stdout:

```json
{"time":"…","level":"INFO","msg":"worker_started","id":0,"port":9100}
{"time":"…","level":"INFO","msg":"mcp_request","user_id":"alice","worker_id":0,"duration_ms":340}
{"time":"…","level":"WARN","msg":"budget_exceeded","user_id":"alice","spent_usd":5.0,"budget_usd":5.0}
```

Ship these to your log aggregator to get per-user query history and cost attribution.

## Timeouts

| Timeout | Value | Notes |
|---------|-------|-------|
| Read | 30 s | Time to receive full request |
| Write | 5 min | Allows long-running or streaming queries |
| Idle | 120 s | Keep-alive connection lifetime |
| Graceful shutdown | 30 s | In-flight requests complete before workers are killed |

## Python interpreter

The gateway defaults to `python3`. Override with the `PYTHON_PATH` environment variable:

```bash
export PYTHON_PATH=/usr/bin/python3.11
./build/gateway --config config/config.yaml
```

## Systemd example

```ini
[Unit]
Description=Limnos Gateway
After=network.target

[Service]
WorkingDirectory=/opt/limnos
ExecStart=/opt/limnos/build/gateway --config /opt/limnos/config/config.yaml --workers 4 --port 8080
EnvironmentFile=/etc/limnos/gateway.env
Restart=on-failure
StandardOutput=journal

[Install]
WantedBy=multi-user.target
```

`/etc/limnos/gateway.env` holds `GATEWAY_API_KEYS` and any AWS credentials.
