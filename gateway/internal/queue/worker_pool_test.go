// White-box tests for WorkerPool — run as package queue so we can directly
// construct Worker/WorkerPool structs without spawning real Python processes.
package queue

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os/exec"
	"testing"
)

// ── Helpers ────────────────────────────────────────────────────────────────────

// newTestWorker creates a Worker without a real subprocess. The reverse proxy
// target doesn't matter for tests that only check pool logic (Next, Metrics).
func newTestWorker(id int, healthy bool) *Worker {
	u, _ := url.Parse("http://localhost:19999")
	w := &Worker{
		ID:    id,
		Port:  9100 + id,
		URL:   u,
		Proxy: httputil.NewSingleHostReverseProxy(u),
	}
	w.healthy.Store(healthy)
	return w
}

// newTestPool wires workers directly into a WorkerPool without starting
// subprocesses or health-check goroutines.
func newTestPool(workers ...*Worker) *WorkerPool {
	p := &WorkerPool{}
	p.workers = workers
	return p
}

// ── Next ───────────────────────────────────────────────────────────────────────

func TestNext_EmptyPool_ReturnsFalse(t *testing.T) {
	p := &WorkerPool{}
	_, ok := p.Next()
	if ok {
		t.Error("Next() on empty pool should return false")
	}
}

func TestNext_AllUnhealthy_ReturnsFalse(t *testing.T) {
	p := newTestPool(newTestWorker(0, false), newTestWorker(1, false))
	_, ok := p.Next()
	if ok {
		t.Error("Next() with all unhealthy workers should return false")
	}
}

func TestNext_SingleHealthyWorker(t *testing.T) {
	p := newTestPool(newTestWorker(0, true))
	got, ok := p.Next()
	if !ok {
		t.Fatal("Next() returned false, expected healthy worker")
	}
	if got.ID != 0 {
		t.Errorf("ID = %d, want 0", got.ID)
	}
}

func TestNext_SkipsUnhealthyWorkers(t *testing.T) {
	// Only worker 1 is healthy; Next() must skip workers 0 and 2.
	p := newTestPool(
		newTestWorker(0, false),
		newTestWorker(1, true),
		newTestWorker(2, false),
	)
	got, ok := p.Next()
	if !ok {
		t.Fatal("Next() returned false, expected a healthy worker")
	}
	if !got.healthy.Load() {
		t.Errorf("returned worker %d is not healthy", got.ID)
	}
}

func TestNext_RoundRobin_EvenDistribution(t *testing.T) {
	p := newTestPool(newTestWorker(0, true), newTestWorker(1, true))

	counts := map[int]int{}
	for i := 0; i < 10; i++ {
		w, ok := p.Next()
		if !ok {
			t.Fatal("Next() returned false unexpectedly")
		}
		counts[w.ID]++
	}
	if counts[0] != 5 || counts[1] != 5 {
		t.Errorf("uneven distribution: w0=%d, w1=%d, want 5/5", counts[0], counts[1])
	}
}

func TestNext_RoundRobin_SkipsUnhealthy(t *testing.T) {
	// Workers 0 and 2 are healthy; worker 1 is unhealthy.
	// All 6 calls should land on worker 0 or 2, never on 1.
	p := newTestPool(
		newTestWorker(0, true),
		newTestWorker(1, false),
		newTestWorker(2, true),
	)
	for i := 0; i < 6; i++ {
		w, ok := p.Next()
		if !ok {
			t.Fatalf("call %d: Next() returned false unexpectedly", i)
		}
		if w.ID == 1 {
			t.Errorf("call %d: Next() returned unhealthy worker 1", i)
		}
	}
}

// ── MetricsHandler ─────────────────────────────────────────────────────────────

func TestMetricsHandler_ReturnsJSON(t *testing.T) {
	w0 := newTestWorker(0, true)
	w0.ReqCount.Store(42)
	w1 := newTestWorker(1, false)
	w1.ReqCount.Store(7)
	p := newTestPool(w0, w1)

	r := httptest.NewRequest("GET", "/metrics", nil)
	rw := httptest.NewRecorder()
	p.MetricsHandler(rw, r)

	if rw.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rw.Code)
	}
	if ct := rw.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	var resp struct {
		Workers []struct {
			ID       int   `json:"id"`
			Port     int   `json:"port"`
			Healthy  bool  `json:"healthy"`
			Requests int64 `json:"total_requests"`
		} `json:"workers"`
	}
	if err := json.NewDecoder(rw.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if len(resp.Workers) != 2 {
		t.Fatalf("len(workers) = %d, want 2", len(resp.Workers))
	}

	m0 := resp.Workers[0]
	if m0.ID != 0 || !m0.Healthy || m0.Requests != 42 {
		t.Errorf("worker[0] = %+v, want id=0 healthy=true requests=42", m0)
	}
	m1 := resp.Workers[1]
	if m1.ID != 1 || m1.Healthy || m1.Requests != 7 {
		t.Errorf("worker[1] = %+v, want id=1 healthy=false requests=7", m1)
	}
}

func TestMetricsHandler_EmptyPool(t *testing.T) {
	p := &WorkerPool{}
	r := httptest.NewRequest("GET", "/metrics", nil)
	rw := httptest.NewRecorder()
	p.MetricsHandler(rw, r)

	if rw.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rw.Code)
	}
	var resp struct {
		Workers []interface{} `json:"workers"`
	}
	if err := json.NewDecoder(rw.Body).Decode(&resp); err != nil {
		t.Fatalf("decode error: %v", err)
	}
	if len(resp.Workers) != 0 {
		t.Errorf("expected empty workers array, got %d", len(resp.Workers))
	}
}

// ── Shutdown ───────────────────────────────────────────────────────────────────

func TestShutdown_NilCmd_DoesNotPanic(t *testing.T) {
	// Workers without a cmd (e.g. in tests) must not crash Shutdown.
	w := &Worker{ID: 0}
	p := newTestPool(w)
	p.Shutdown() // must not panic
}

func TestShutdown_KillsProcess(t *testing.T) {
	// Start a harmless long-lived process and verify Shutdown kills it.
	cmd := exec.Command("sleep", "60")
	if err := cmd.Start(); err != nil {
		t.Skipf("cannot start sleep: %v", err)
	}
	t.Cleanup(func() {
		// Safety net in case Shutdown didn't kill it (test failure path).
		if cmd.Process != nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	})

	w := &Worker{ID: 0, cmd: cmd}
	p := newTestPool(w)
	p.Shutdown() // must kill the process without panicking

	// Confirm the process has exited (Wait returns an error for killed processes).
	err := cmd.Wait()
	if err == nil {
		t.Error("expected non-nil error from Wait() after Shutdown killed the process")
	}
}
