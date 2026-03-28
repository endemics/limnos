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

// silentLogger discards all log output during tests.
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
		t.Errorf("code = %d, want 503", w.Code)
	}
}

func TestProxy_NoWorkers_BodyMentionsWorkers(t *testing.T) {
	proxy := mcp.NewProxy(emptyPool(), dummyAuth(), silentLogger())

	r := httptest.NewRequest("POST", "/mcp", nil)
	w := httptest.NewRecorder()
	proxy.ServeHTTP(w, r)

	body := w.Body.String()
	if !strings.Contains(body, "workers") {
		t.Errorf("body %q should mention 'workers'", body)
	}
}

// ── Anonymous vs authenticated user ───────────────────────────────────────────

func TestProxy_NoContext_AnonymousFallback_Returns503(t *testing.T) {
	// No UserInfo in context → proxy treats as "anonymous" and still returns 503
	// (no workers), not an auth error.
	proxy := mcp.NewProxy(emptyPool(), dummyAuth(), silentLogger())

	r := httptest.NewRequest("GET", "/mcp", nil)
	w := httptest.NewRecorder()
	proxy.ServeHTTP(w, r)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("code = %d, want 503 for anonymous user with no workers", w.Code)
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
		t.Errorf("code = %d, want 503", w.Code)
	}
}

// ── Auth middleware integration ────────────────────────────────────────────────

func TestProxy_AuthMiddleware_MissingKey_Returns401(t *testing.T) {
	// Auth middleware sits in front of the proxy; missing key → 401 before
	// the proxy even runs (no workers needed for this code path).
	authn := auth.NewAPIKeyAuth(auth.APIKeyAuthConfig{
		Keys: map[string]auth.UserInfo{
			"valid-key": {UserID: "alice"},
		},
	})
	handler := authn.Middleware(mcp.NewProxy(emptyPool(), authn, silentLogger()))

	r := httptest.NewRequest("POST", "/mcp", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("code = %d, want 401", w.Code)
	}
}

func TestProxy_AuthMiddleware_ValidKey_ThenNoWorkers_Returns503(t *testing.T) {
	// Auth passes → reaches proxy → no workers → 503.
	authn := auth.NewAPIKeyAuth(auth.APIKeyAuthConfig{
		Keys: map[string]auth.UserInfo{
			"valid-key": {UserID: "alice"},
		},
	})
	handler := authn.Middleware(mcp.NewProxy(emptyPool(), authn, silentLogger()))

	r := httptest.NewRequest("POST", "/mcp", nil)
	r.Header.Set("X-API-Key", "valid-key")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusServiceUnavailable {
		t.Errorf("code = %d, want 503 (auth OK, no workers)", w.Code)
	}
}

func TestProxy_AuthMiddleware_BudgetExceeded_Returns429(t *testing.T) {
	authn := auth.NewAPIKeyAuth(auth.APIKeyAuthConfig{
		Keys: map[string]auth.UserInfo{
			"k": {UserID: "alice", BudgetUSD: 1.0},
		},
	})
	authn.RecordSpend("alice", 1.0) // exhaust budget
	handler := authn.Middleware(mcp.NewProxy(emptyPool(), authn, silentLogger()))

	r := httptest.NewRequest("POST", "/mcp", nil)
	r.Header.Set("X-API-Key", "k")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)

	if w.Code != http.StatusTooManyRequests {
		t.Errorf("code = %d, want 429", w.Code)
	}
}
