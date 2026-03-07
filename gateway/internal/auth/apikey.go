// gateway/internal/auth/apikey.go
//
// API key authentication middleware for the Go gateway.
// Keys are loaded from env (GATEWAY_API_KEYS JSON) or Secrets Manager.
// Each key maps to a UserInfo with a daily spend budget.

package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

type UserInfo struct {
	UserID    string  `json:"user_id"`
	BudgetUSD float64 `json:"budget_usd"` // daily budget, 0 = unlimited
	RateLimit int     `json:"rate_limit"` // requests per minute, 0 = unlimited
}

type APIKeyAuthConfig struct {
	Keys map[string]UserInfo
}

type APIKeyAuth struct {
	keys  map[string]UserInfo
	spend map[string]*userSpend
	mu    sync.RWMutex
}

type userSpend struct {
	dailyUSD float64
	resetAt  time.Time
	mu       sync.Mutex
}

type contextKey string

const UserInfoKey contextKey = "user_info"

func NewAPIKeyAuth(cfg APIKeyAuthConfig) *APIKeyAuth {
	return &APIKeyAuth{
		keys:  cfg.Keys,
		spend: make(map[string]*userSpend),
	}
}

// Middleware validates the X-API-Key header and injects UserInfo into context.
func (a *APIKeyAuth) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("X-API-Key")
		if key == "" {
			// Also accept Bearer token format
			auth := r.Header.Get("Authorization")
			if len(auth) > 7 && auth[:7] == "Bearer " {
				key = auth[7:]
			}
		}

		if key == "" {
			http.Error(w, `{"error":"missing X-API-Key header"}`, http.StatusUnauthorized)
			return
		}

		a.mu.RLock()
		user, ok := a.keys[key]
		a.mu.RUnlock()

		if !ok {
			slog.Warn("invalid_api_key", "remote_addr", r.RemoteAddr)
			http.Error(w, `{"error":"invalid API key"}`, http.StatusUnauthorized)
			return
		}

		// Budget check
		if user.BudgetUSD > 0 {
			spend := a.getOrCreateSpend(user.UserID)
			spend.mu.Lock()
			if time.Now().After(spend.resetAt) {
				spend.dailyUSD = 0
				spend.resetAt = tomorrow()
			}
			if spend.dailyUSD >= user.BudgetUSD {
				spend.mu.Unlock()
				slog.Warn("budget_exceeded",
					"user_id", user.UserID,
					"spent_usd", spend.dailyUSD,
					"budget_usd", user.BudgetUSD,
				)
				http.Error(w,
					fmt.Sprintf(`{"error":"daily budget $%.2f exceeded"}`, user.BudgetUSD),
					http.StatusTooManyRequests,
				)
				return
			}
			spend.mu.Unlock()
		}

		ctx := context.WithValue(r.Context(), UserInfoKey, user)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// RecordSpend records query cost against a user's daily budget.
func (a *APIKeyAuth) RecordSpend(userID string, usd float64) {
	spend := a.getOrCreateSpend(userID)
	spend.mu.Lock()
	defer spend.mu.Unlock()
	if time.Now().After(spend.resetAt) {
		spend.dailyUSD = 0
		spend.resetAt = tomorrow()
	}
	spend.dailyUSD += usd
}

func (a *APIKeyAuth) getOrCreateSpend(userID string) *userSpend {
	a.mu.Lock()
	defer a.mu.Unlock()
	if s, ok := a.spend[userID]; ok {
		return s
	}
	s := &userSpend{resetAt: tomorrow()}
	a.spend[userID] = s
	return s
}

func tomorrow() time.Time {
	now := time.Now()
	return time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, now.Location())
}

// ParseAPIKeys parses a JSON string into a key->UserInfo map.
func ParseAPIKeys(raw string) (map[string]UserInfo, error) {
	var keys map[string]UserInfo
	if err := json.Unmarshal([]byte(raw), &keys); err != nil {
		return nil, fmt.Errorf("invalid API keys JSON: %w", err)
	}
	return keys, nil
}
