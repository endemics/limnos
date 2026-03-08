// gateway/cmd/gateway/main.go
//
// Phase 2: Go HTTP gateway for multi-user deployments.
//
// Responsibilities:
//   - Accept MCP-over-HTTP (streamable HTTP / SSE) connections from multiple Claude clients
//   - Authenticate requests (API key or IAM-signed headers)
//   - Enforce per-user query budgets and rate limits
//   - Queue requests and fan out to a pool of Python MCP worker processes
//   - Proxy responses back to clients with audit logging
//
// Usage:
//
//	./gateway --config ../../config/config.yaml --workers 4 --port 8080

package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/endemics/limnos/gateway/internal/auth"
	"github.com/endemics/limnos/gateway/internal/mcp"
	"github.com/endemics/limnos/gateway/internal/queue"
)

func main() {
	// ── Flags ──────────────────────────────────────────────────────────────
	configPath := flag.String("config", "config/config.yaml", "Path to config.yaml")
	workers := flag.Int("workers", 4, "Number of Python MCP worker processes")
	port := flag.Int("port", 8080, "HTTP listen port")
	logLevel := flag.String("log-level", "info", "Log level: debug|info|warn|error")
	flag.Parse()

	// ── Logging ────────────────────────────────────────────────────────────
	var level slog.Level
	if err := level.UnmarshalText([]byte(*logLevel)); err != nil {
		level = slog.LevelInfo
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))
	slog.SetDefault(logger)

	slog.Info("gateway_starting",
		"port", *port,
		"workers", *workers,
		"config", *configPath,
	)

	// ── Worker pool ─────────────────────────────────────────────────────────
	// Each worker is a Python MCP server process running in HTTP mode.
	// The gateway starts them as subprocesses and load-balances across them.
	pool, err := queue.NewWorkerPool(queue.WorkerPoolConfig{
		Size:         *workers,
		PythonPath:   pythonPath(),
		ServerScript: serverScriptPath(*configPath),
		ConfigPath:   *configPath,
		StartPort:    9100, // workers listen on 9100, 9101, 9102, ...
		StartTimeout: 15 * time.Second,
	})
	if err != nil {
		slog.Error("worker_pool_failed", "error", err)
		os.Exit(1)
	}
	defer pool.Shutdown()

	// ── Auth middleware ─────────────────────────────────────────────────────
	authn := auth.NewAPIKeyAuth(auth.APIKeyAuthConfig{
		// In production: load keys from AWS Secrets Manager or env
		Keys: loadAPIKeys(),
	})

	// ── HTTP router ─────────────────────────────────────────────────────────
	proxy := mcp.NewProxy(pool, logger)

	mux := http.NewServeMux()
	mux.Handle("/mcp", authn.Middleware(proxy))                   // MCP streamable HTTP endpoint
	mux.Handle("/health", http.HandlerFunc(healthHandler))        // Health check (no auth)
	mux.Handle("/metrics", http.HandlerFunc(pool.MetricsHandler)) // Worker pool metrics

	server := &http.Server{
		Addr:         fmt.Sprintf(":%d", *port),
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 5 * time.Minute, // Long timeout for streaming query responses
		IdleTimeout:  120 * time.Second,
	}

	// ── Graceful shutdown ────────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		slog.Info("gateway_listening", "addr", server.Addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server_error", "error", err)
			os.Exit(1)
		}
	}()

	<-quit
	slog.Info("gateway_shutting_down")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		slog.Error("shutdown_error", "error", err)
	}
	slog.Info("gateway_stopped")
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintf(w, `{"status":"ok","time":"%s"}`, time.Now().UTC().Format(time.RFC3339))
}

func pythonPath() string {
	if p := os.Getenv("PYTHON_PATH"); p != "" {
		return p
	}
	return "python3"
}

func serverScriptPath(configPath string) string {
	// Resolve relative to the gateway binary's working directory
	return "../server/main.py"
}

func loadAPIKeys() map[string]auth.UserInfo {
	// In production, load from AWS Secrets Manager / env.
	// For development: read from GATEWAY_API_KEYS env var as JSON.
	// Format: {"key1": {"user_id": "alice", "budget_usd": 5.0}, ...}
	raw := os.Getenv("GATEWAY_API_KEYS")
	if raw == "" {
		slog.Warn("no_api_keys_configured — all requests will be rejected")
		return map[string]auth.UserInfo{}
	}
	keys, err := auth.ParseAPIKeys(raw)
	if err != nil {
		slog.Error("invalid_api_keys", "error", err)
		return map[string]auth.UserInfo{}
	}
	return keys
}
