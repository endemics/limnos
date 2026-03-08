// gateway/internal/queue/worker_pool.go
//
// Manages a pool of Python MCP server worker processes.
// Each worker runs: python main.py --transport http --port <N>
// The pool load-balances incoming requests across healthy workers
// and restarts crashed workers automatically.

package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"sync"
	"sync/atomic"
	"time"
)

// WorkerPoolConfig configures the Python worker process pool.
type WorkerPoolConfig struct {
	Size         int
	PythonPath   string
	ServerScript string
	ConfigPath   string
	StartPort    int
	StartTimeout time.Duration
}

// Worker represents one Python MCP server process.
type Worker struct {
	ID       int
	Port     int
	URL      *url.URL
	Proxy    *httputil.ReverseProxy
	cmd      *exec.Cmd
	healthy  atomic.Bool
	ReqCount atomic.Int64
}

// WorkerPool manages N Python worker processes with load balancing.
type WorkerPool struct {
	cfg     WorkerPoolConfig
	workers []*Worker
	mu      sync.RWMutex
	next    atomic.Uint64
}

func NewWorkerPool(cfg WorkerPoolConfig) (*WorkerPool, error) {
	pool := &WorkerPool{cfg: cfg}
	pool.workers = make([]*Worker, cfg.Size)

	for i := 0; i < cfg.Size; i++ {
		w, err := pool.startWorker(i)
		if err != nil {
			pool.Shutdown()
			return nil, fmt.Errorf("failed to start worker %d: %w", i, err)
		}
		pool.workers[i] = w
		slog.Info("worker_started", "id", i, "port", w.Port)
	}

	// Start health check loop
	go pool.healthCheckLoop()

	return pool, nil
}

// Next returns the next healthy worker using round-robin.
func (p *WorkerPool) Next() (*Worker, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	n := len(p.workers)
	if n == 0 {
		return nil, false
	}

	// Try up to N times to find a healthy worker
	for i := 0; i < n; i++ {
		idx := int(p.next.Add(1)-1) % n
		w := p.workers[idx]
		if w.healthy.Load() {
			return w, true
		}
	}
	return nil, false
}

// MetricsHandler returns a JSON snapshot of worker health and request counts.
func (p *WorkerPool) MetricsHandler(w http.ResponseWriter, r *http.Request) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	type workerMetric struct {
		ID       int   `json:"id"`
		Port     int   `json:"port"`
		Healthy  bool  `json:"healthy"`
		Requests int64 `json:"total_requests"`
	}
	metrics := make([]workerMetric, len(p.workers))
	for i, wk := range p.workers {
		metrics[i] = workerMetric{
			ID:       wk.ID,
			Port:     wk.Port,
			Healthy:  wk.healthy.Load(),
			Requests: wk.ReqCount.Load(),
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"workers": metrics})
}

// Shutdown kills all worker processes.
func (p *WorkerPool) Shutdown() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, w := range p.workers {
		if w.cmd != nil && w.cmd.Process != nil {
			slog.Info("stopping_worker", "id", w.ID)
			_ = w.cmd.Process.Kill()
		}
	}
}

// ── Private ────────────────────────────────────────────────────────────────

func (p *WorkerPool) startWorker(id int) (*Worker, error) {
	port := p.cfg.StartPort + id
	workerURL, _ := url.Parse(fmt.Sprintf("http://localhost:%d", port))

	cmd := exec.Command(
		p.cfg.PythonPath,
		p.cfg.ServerScript,
		"--transport", "http",
		"--port", fmt.Sprintf("%d", port),
	)
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("CONFIG_PATH=%s", p.cfg.ConfigPath),
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("cmd.Start: %w", err)
	}

	proxy := httputil.NewSingleHostReverseProxy(workerURL)

	w := &Worker{
		ID:    id,
		Port:  port,
		URL:   workerURL,
		Proxy: proxy,
		cmd:   cmd,
	}

	// Wait for worker to become healthy
	if err := p.waitHealthy(w, p.cfg.StartTimeout); err != nil {
		_ = cmd.Process.Kill()
		return nil, fmt.Errorf("worker %d never became healthy: %w", id, err)
	}
	w.healthy.Store(true)
	return w, nil
}

func (p *WorkerPool) waitHealthy(w *Worker, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	client := &http.Client{Timeout: 1 * time.Second}
	healthURL := fmt.Sprintf("http://localhost:%d/health", w.Port)

	for time.Now().Before(deadline) {
		resp, err := client.Get(healthURL)
		if err == nil && resp.StatusCode == http.StatusOK {
			_ = resp.Body.Close()
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("timeout after %s", timeout)
}

func (p *WorkerPool) healthCheckLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	client := &http.Client{Timeout: 2 * time.Second}

	for range ticker.C {
		p.mu.RLock()
		workers := p.workers
		p.mu.RUnlock()

		for _, w := range workers {
			go func(w *Worker) {
				healthURL := fmt.Sprintf("http://localhost:%d/health", w.Port)
				resp, err := client.Get(healthURL)
				if err != nil || resp.StatusCode != http.StatusOK {
					if w.healthy.Swap(false) {
						slog.Warn("worker_unhealthy", "id", w.ID, "port", w.Port)
						go p.restartWorker(w)
					}
				} else {
					w.healthy.Store(true)
					_ = resp.Body.Close()
				}
			}(w)
		}
	}
}

func (p *WorkerPool) restartWorker(w *Worker) {
	slog.Info("restarting_worker", "id", w.ID)
	if w.cmd != nil && w.cmd.Process != nil {
		_ = w.cmd.Process.Kill()
		_ = w.cmd.Wait()
	}

	ctx, cancel := context.WithTimeout(context.Background(), p.cfg.StartTimeout)
	defer cancel()
	_ = ctx

	newWorker, err := p.startWorker(w.ID)
	if err != nil {
		slog.Error("worker_restart_failed", "id", w.ID, "error", err)
		return
	}

	p.mu.Lock()
	p.workers[w.ID] = newWorker
	p.mu.Unlock()
	slog.Info("worker_restarted", "id", w.ID)
}
