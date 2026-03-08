package auth

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// ── Helpers ────────────────────────────────────────────────────────────────────

func newAuth(keys map[string]UserInfo) *APIKeyAuth {
	return NewAPIKeyAuth(APIKeyAuthConfig{Keys: keys})
}

// okHandler records that it was called and returns 200.
func okHandler(called *bool) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if called != nil {
			*called = true
		}
		w.WriteHeader(http.StatusOK)
	})
}

func do(h http.Handler, r *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

// ── ParseAPIKeys ───────────────────────────────────────────────────────────────

func TestParseAPIKeys_Valid(t *testing.T) {
	raw := `{"key1":{"user_id":"alice","budget_usd":5.0,"rate_limit":60}}`
	keys, err := ParseAPIKeys(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	u, ok := keys["key1"]
	if !ok {
		t.Fatal("key1 not found")
	}
	if u.UserID != "alice" {
		t.Errorf("UserID = %q, want alice", u.UserID)
	}
	if u.BudgetUSD != 5.0 {
		t.Errorf("BudgetUSD = %f, want 5.0", u.BudgetUSD)
	}
	if u.RateLimit != 60 {
		t.Errorf("RateLimit = %d, want 60", u.RateLimit)
	}
}

func TestParseAPIKeys_MultipleKeys(t *testing.T) {
	raw := `{"k1":{"user_id":"a"},"k2":{"user_id":"b"}}`
	keys, err := ParseAPIKeys(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(keys) != 2 {
		t.Errorf("len = %d, want 2", len(keys))
	}
}

func TestParseAPIKeys_InvalidJSON(t *testing.T) {
	_, err := ParseAPIKeys("not-json")
	if err == nil {
		t.Error("expected error for invalid JSON, got nil")
	}
}

func TestParseAPIKeys_Empty(t *testing.T) {
	keys, err := ParseAPIKeys("{}")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(keys) != 0 {
		t.Errorf("expected empty map, got %d keys", len(keys))
	}
}

// ── Middleware: authentication ─────────────────────────────────────────────────

func TestMiddleware_MissingKey_Returns401(t *testing.T) {
	a := newAuth(map[string]UserInfo{"k": {UserID: "alice"}})
	r := httptest.NewRequest("POST", "/mcp", nil)
	w := do(a.Middleware(okHandler(nil)), r)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("code = %d, want 401", w.Code)
	}
}

func TestMiddleware_InvalidKey_Returns401(t *testing.T) {
	a := newAuth(map[string]UserInfo{"k": {UserID: "alice"}})
	r := httptest.NewRequest("POST", "/mcp", nil)
	r.Header.Set("X-API-Key", "wrong")
	w := do(a.Middleware(okHandler(nil)), r)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("code = %d, want 401", w.Code)
	}
}

func TestMiddleware_ValidXAPIKey_Passes(t *testing.T) {
	a := newAuth(map[string]UserInfo{"secret": {UserID: "alice"}})
	r := httptest.NewRequest("POST", "/mcp", nil)
	r.Header.Set("X-API-Key", "secret")
	w := do(a.Middleware(okHandler(nil)), r)
	if w.Code != http.StatusOK {
		t.Errorf("code = %d, want 200", w.Code)
	}
}

func TestMiddleware_BearerToken_Accepted(t *testing.T) {
	a := newAuth(map[string]UserInfo{"secret": {UserID: "bob"}})
	r := httptest.NewRequest("POST", "/mcp", nil)
	r.Header.Set("Authorization", "Bearer secret")
	w := do(a.Middleware(okHandler(nil)), r)
	if w.Code != http.StatusOK {
		t.Errorf("code = %d, want 200", w.Code)
	}
}

func TestMiddleware_BearerToken_TooShort_Returns401(t *testing.T) {
	// "Bearer " is exactly 7 chars; less than that should not be treated as bearer
	a := newAuth(map[string]UserInfo{"secret": {UserID: "alice"}})
	r := httptest.NewRequest("POST", "/mcp", nil)
	r.Header.Set("Authorization", "Bear x") // only 6 chars before key
	w := do(a.Middleware(okHandler(nil)), r)
	if w.Code != http.StatusUnauthorized {
		t.Errorf("code = %d, want 401", w.Code)
	}
}

func TestMiddleware_UserInfoInjectedInContext(t *testing.T) {
	a := newAuth(map[string]UserInfo{"k": {UserID: "carol", BudgetUSD: 10}})

	var got UserInfo
	inner := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r.Context().Value(UserInfoKey).(UserInfo)
		w.WriteHeader(http.StatusOK)
	})

	r := httptest.NewRequest("POST", "/mcp", nil)
	r.Header.Set("X-API-Key", "k")
	do(a.Middleware(inner), r)

	if got.UserID != "carol" {
		t.Errorf("UserID = %q, want carol", got.UserID)
	}
	if got.BudgetUSD != 10 {
		t.Errorf("BudgetUSD = %f, want 10", got.BudgetUSD)
	}
}

// ── Middleware: budget enforcement ─────────────────────────────────────────────

func TestMiddleware_UnlimitedBudget_AlwaysPasses(t *testing.T) {
	// BudgetUSD = 0 means unlimited; spend should never block
	a := newAuth(map[string]UserInfo{"k": {UserID: "alice", BudgetUSD: 0}})
	a.RecordSpend("alice", 1_000_000)

	r := httptest.NewRequest("POST", "/mcp", nil)
	r.Header.Set("X-API-Key", "k")
	w := do(a.Middleware(okHandler(nil)), r)
	if w.Code != http.StatusOK {
		t.Errorf("code = %d, want 200 (unlimited budget)", w.Code)
	}
}

func TestMiddleware_BudgetExceeded_Returns429(t *testing.T) {
	a := newAuth(map[string]UserInfo{"k": {UserID: "alice", BudgetUSD: 1.0}})
	a.RecordSpend("alice", 1.0) // hit the cap

	r := httptest.NewRequest("POST", "/mcp", nil)
	r.Header.Set("X-API-Key", "k")
	w := do(a.Middleware(okHandler(nil)), r)
	if w.Code != http.StatusTooManyRequests {
		t.Errorf("code = %d, want 429", w.Code)
	}
	if !strings.Contains(w.Body.String(), "budget") {
		t.Errorf("body %q should mention 'budget'", w.Body.String())
	}
}

func TestMiddleware_BudgetNotYetExceeded_Passes(t *testing.T) {
	a := newAuth(map[string]UserInfo{"k": {UserID: "alice", BudgetUSD: 5.0}})
	a.RecordSpend("alice", 4.99) // just under

	r := httptest.NewRequest("POST", "/mcp", nil)
	r.Header.Set("X-API-Key", "k")
	w := do(a.Middleware(okHandler(nil)), r)
	if w.Code != http.StatusOK {
		t.Errorf("code = %d, want 200", w.Code)
	}
}

// ── RecordSpend ────────────────────────────────────────────────────────────────

func TestRecordSpend_AccumulatesAcrossCalls(t *testing.T) {
	a := newAuth(map[string]UserInfo{"k": {UserID: "alice", BudgetUSD: 5.0}})
	a.RecordSpend("alice", 2.0)
	a.RecordSpend("alice", 2.0) // total 4.0, still under 5.0

	r := httptest.NewRequest("POST", "/mcp", nil)
	r.Header.Set("X-API-Key", "k")
	w := do(a.Middleware(okHandler(nil)), r)
	if w.Code != http.StatusOK {
		t.Errorf("code = %d, want 200 (spend 4.0 < budget 5.0)", w.Code)
	}

	a.RecordSpend("alice", 2.0) // total 6.0, over budget

	r2 := httptest.NewRequest("POST", "/mcp", nil)
	r2.Header.Set("X-API-Key", "k")
	w2 := do(a.Middleware(okHandler(nil)), r2)
	if w2.Code != http.StatusTooManyRequests {
		t.Errorf("code = %d, want 429 (spend 6.0 > budget 5.0)", w2.Code)
	}
}

func TestRecordSpend_UnknownUser_DoesNotPanic(t *testing.T) {
	a := newAuth(map[string]UserInfo{})
	a.RecordSpend("nobody", 99.0) // no-op; user has no budget entry
}
