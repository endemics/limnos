// gateway/internal/mcp/proxy.go
//
// HTTP reverse proxy that routes MCP requests to Python worker processes.
// Handles both regular HTTP and SSE (Server-Sent Events) streaming responses.

package mcp

import (
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"time"

	"github.com/endemics/limnos/gateway/internal/auth"
	"github.com/endemics/limnos/gateway/internal/queue"
)

// Proxy routes incoming MCP HTTP requests to available Python workers.
type Proxy struct {
	pool   *queue.WorkerPool
	auth   *auth.APIKeyAuth
	logger *slog.Logger
}

func NewProxy(pool *queue.WorkerPool, auth *auth.APIKeyAuth, logger *slog.Logger) *Proxy {
	return &Proxy{pool: pool, auth: auth, logger: logger}
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

	// Strip /mcp prefix if present
	originalPath := r.URL.Path
	if len(r.URL.Path) >= 4 && r.URL.Path[:4] == "/mcp" {
		r.URL.Path = r.URL.Path[4:]
		if r.URL.Path == "" {
			r.URL.Path = "/"
		}
	}

	p.logger.Info("mcp_request",
		"user_id", userID,
		"worker_id", worker.ID,
		"method", r.Method,
		"original_path", originalPath,
		"proxied_path", r.URL.Path,
	)

	// Detect SSE / streaming response (MCP uses text/event-stream)
	isSSE := r.Header.Get("Accept") == "text/event-stream"
	if isSSE {
		// For SSE: disable response buffering so events stream through immediately
		w.Header().Set("X-Accel-Buffering", "no")
		worker.Proxy.ServeHTTP(w, r)
	} else {
		// For tool calls (usually POST /messages), capture response to record spend
		recorder := &bodyRecorder{ResponseWriter: w}
		worker.Proxy.ServeHTTP(recorder, r)

		if userID != "anonymous" {
			// 1. Try to get cost from header (most reliable)
			costStr := recorder.Header().Get("X-Limnos-Cost-USD")
			cost, _ := strconv.ParseFloat(costStr, 64)

			// 2. Fallback to scraping body (backwards compat)
			if cost <= 0 {
				cost = p.extractCost(recorder.body)
			}

			if cost > 0 {
				source := "scrape"
				if costStr != "" {
					source = "header"
				}
				p.auth.RecordSpend(userID, cost)
				p.logger.Info("spend_recorded",
					"user_id", userID,
					"cost_usd", cost,
					"source", source,
				)
			}
		}
	}

	p.logger.Info("mcp_response",
		"user_id", userID,
		"worker_id", worker.ID,
		"duration_ms", time.Since(start).Milliseconds(),
	)
}

// ── Helpers ──────────────────────────────────────────────────────────────────

type bodyRecorder struct {
	http.ResponseWriter
	body []byte
}

func (b *bodyRecorder) Write(p []byte) (int, error) {
	b.body = append(b.body, p...)
	return b.ResponseWriter.Write(p)
}

func (b *bodyRecorder) Flush() {
	if flusher, ok := b.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

var costRegex = regexp.MustCompile(`est\. \$([0-9.]+)`)

func (p *Proxy) extractCost(body []byte) float64 {
	match := costRegex.FindSubmatch(body)
	if len(match) < 2 {
		return 0
	}
	cost, _ := strconv.ParseFloat(string(match[1]), 64)
	return cost
}
