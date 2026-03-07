// gateway/internal/mcp/proxy.go
//
// HTTP reverse proxy that routes MCP requests to Python worker processes.
// Handles both regular HTTP and SSE (Server-Sent Events) streaming responses.

package mcp

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/endemics/limnos/gateway/internal/auth"
	"github.com/endemics/limnos/gateway/internal/queue"
)

// Proxy routes incoming MCP HTTP requests to available Python workers.
type Proxy struct {
	pool   *queue.WorkerPool
	logger *slog.Logger
}

func NewProxy(pool *queue.WorkerPool, logger *slog.Logger) *Proxy {
	return &Proxy{pool: pool, logger: logger}
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	// Extract user from context (set by auth middleware)
	user, _ := r.Context().Value(auth.UserInfoKey).(auth.UserInfo)
	userID := user.UserID
	if userID == "" {
		userID = "anonymous"
	}

	// Select next healthy worker
	worker, ok := p.pool.Next()
	if !ok {
		p.logger.Error("no_healthy_workers", "user_id", userID)
		http.Error(w, `{"error":"no workers available, try again shortly"}`,
			http.StatusServiceUnavailable)
		return
	}

	worker.ReqCount.Add(1)

	p.logger.Info("mcp_request",
		"user_id", userID,
		"worker_id", worker.ID,
		"method", r.Method,
		"path", r.URL.Path,
	)

	// Detect SSE / streaming response (MCP uses text/event-stream)
	isSSE := r.Header.Get("Accept") == "text/event-stream"
	if isSSE {
		// For SSE: disable response buffering so events stream through immediately
		w.Header().Set("X-Accel-Buffering", "no")
	}

	// Proxy the request
	worker.Proxy.ServeHTTP(w, r)

	p.logger.Info("mcp_response",
		"user_id", userID,
		"worker_id", worker.ID,
		"duration_ms", time.Since(start).Milliseconds(),
	)
}
