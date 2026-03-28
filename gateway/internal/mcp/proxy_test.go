// gateway/internal/mcp/proxy_test.go

package mcp_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/endemics/limnos/gateway/internal/auth"
	"github.com/endemics/limnos/gateway/internal/mcp"
	"github.com/endemics/limnos/gateway/internal/queue"
)

func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError + 1}))
}

func dummyAuth() *auth.APIKeyAuth {
	return auth.NewAPIKeyAuth(auth.APIKeyAuthConfig{})
}

func emptyPool() *queue.WorkerPool {
	pool, _ := queue.NewWorkerPool(queue.WorkerPoolConfig{
		Size: 0,
	})
	return pool
}

// ── No healthy workers ─────────────────────────────────────────────────────────

func TestProxy_NoWorkers_Returns503(t *testing.T) {
	proxy := mcp.NewProxy(emptyPool(), dummyAuth(), silentLogger())

	r := httptest.NewRequest("POST", "/mcp", nil)
	w := httptest.NewRecorder()

	proxy.ServeHTTP(w, r)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", w.Code)
	}
}

func TestProxy_NoWorkers_BodyMentionsWorkers(t *testing.T) {
	proxy := mcp.NewProxy(emptyPool(), dummyAuth(), silentLogger())

	r := httptest.NewRequest("POST", "/mcp", nil)
	w := httptest.NewRecorder()

	proxy.ServeHTTP(w, r)

	if !strings.Contains(w.Body.String(), "no workers available") {
		t.Errorf("expected error message in body, got %s", w.Body.String())
	}
}

func TestProxy_NoContext_AnonymousFallback_Returns503(t *testing.T) {
	// No UserInfo in context → proxy treats as "anonymous" and still returns 503
	// (no workers), not an auth error.
	proxy := mcp.NewProxy(emptyPool(), dummyAuth(), silentLogger())

	r := httptest.NewRequest("GET", "/mcp", nil)
	w := httptest.NewRecorder()

	proxy.ServeHTTP(w, r)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", w.Code)
	}
}

func TestProxy_AuthenticatedUser_NoWorkers_Returns503(t *testing.T) {
	// Valid user in context, but still no workers → 503.
	proxy := mcp.NewProxy(emptyPool(), dummyAuth(), silentLogger())

	r := httptest.NewRequest("POST", "/mcp", nil)
	ctx := context.WithValue(r.Context(), auth.UserInfoKey, auth.UserInfo{UserID: "alice", BudgetUSD: 10})
	r = r.WithContext(ctx)

	w := httptest.NewRecorder()

	proxy.ServeHTTP(w, r)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", w.Code)
	}
}
