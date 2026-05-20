package metrics_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/yourusername/xyllo/internal/metrics"
)

// ── nopProvider / default ──────────────────────────────────────────────────────

func TestDefaultProvider_AllDelegationFunctionsNoOp(t *testing.T) {
	// All delegation functions must route to the default nopProvider without panic.
	metrics.RecordEventIngested("src")
	metrics.RecordEventRejected("src", "reason")
	metrics.RecordDLQEnqueued()
	metrics.RecordBatcherFlush()
	metrics.ObserveWorkerPoolDepth(3.0)
	metrics.RecordRateLimited("src")
	metrics.RecordPanicRecovered()
}

func TestDefaultProvider_HandlerNotNil(t *testing.T) {
	h := metrics.Handler()
	if h == nil {
		t.Fatal("Handler() must not return nil with the default nopProvider")
	}
}

func TestNewNopProvider_AllMethodsNoOp(t *testing.T) {
	nop := metrics.NewNopProvider()
	nop.RecordEventIngested("s")
	nop.RecordEventRejected("s", "r")
	nop.RecordDLQEnqueued()
	nop.RecordBatcherFlush()
	nop.ObserveWorkerPoolDepth(1.0)
	nop.RecordRateLimited("s")
	nop.RecordPanicRecovered()

	if h := nop.Handler(); h == nil {
		t.Fatal("nopProvider.Handler() must not return nil")
	}
}

// ── SetProvider / spy ──────────────────────────────────────────────────────────

type spyProvider struct {
	ingested    int
	rejected    int
	dlq         int
	flushes     int
	depth       float64
	rateLimited int
	panics      int
	handlerHits int
}

func (s *spyProvider) RecordEventIngested(string)         { s.ingested++ }
func (s *spyProvider) RecordEventRejected(string, string) { s.rejected++ }
func (s *spyProvider) RecordDLQEnqueued()                 { s.dlq++ }
func (s *spyProvider) RecordBatcherFlush()                { s.flushes++ }
func (s *spyProvider) ObserveWorkerPoolDepth(n float64)   { s.depth = n }
func (s *spyProvider) RecordRateLimited(string)           { s.rateLimited++ }
func (s *spyProvider) RecordPanicRecovered()              { s.panics++ }
func (s *spyProvider) Handler() http.Handler {
	s.handlerHits++
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {})
}

func TestSetProvider_RoutesCallsToCustomProvider(t *testing.T) {
	spy := &spyProvider{}
	metrics.SetProvider(spy)
	t.Cleanup(func() { metrics.SetProvider(metrics.NewNopProvider()) })

	metrics.RecordEventIngested("s")
	metrics.RecordEventRejected("s", "r")
	metrics.RecordDLQEnqueued()
	metrics.RecordBatcherFlush()
	metrics.ObserveWorkerPoolDepth(7.0)
	metrics.RecordRateLimited("s")
	metrics.RecordPanicRecovered()
	_ = metrics.Handler()

	if spy.ingested != 1 {
		t.Errorf("ingested: want 1, got %d", spy.ingested)
	}
	if spy.rejected != 1 {
		t.Errorf("rejected: want 1, got %d", spy.rejected)
	}
	if spy.dlq != 1 {
		t.Errorf("dlq: want 1, got %d", spy.dlq)
	}
	if spy.flushes != 1 {
		t.Errorf("flushes: want 1, got %d", spy.flushes)
	}
	if spy.depth != 7.0 {
		t.Errorf("depth: want 7.0, got %f", spy.depth)
	}
	if spy.rateLimited != 1 {
		t.Errorf("rateLimited: want 1, got %d", spy.rateLimited)
	}
	if spy.panics != 1 {
		t.Errorf("panics: want 1, got %d", spy.panics)
	}
	if spy.handlerHits != 1 {
		t.Errorf("handler calls: want 1, got %d", spy.handlerHits)
	}
}

// ── PrometheusProvider ─────────────────────────────────────────────────────────

func TestPrometheusProvider_AllMethodsNoOp(t *testing.T) {
	p := metrics.NewPrometheusProvider()
	p.RecordEventIngested("src")
	p.RecordEventRejected("src", "validation")
	p.RecordDLQEnqueued()
	p.RecordBatcherFlush()
	p.ObserveWorkerPoolDepth(5.0)
	p.RecordRateLimited("src")
	p.RecordPanicRecovered()
}

func TestPrometheusProvider_HandlerServesOK(t *testing.T) {
	p := metrics.NewPrometheusProvider()
	p.RecordEventIngested("src") // record something so output is non-empty

	h := p.Handler()
	if h == nil {
		t.Fatal("Handler() must not return nil")
	}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", rec.Code)
	}
}

func TestPrometheusProvider_MultipleInstancesNoConflict(t *testing.T) {
	// Private registry per instance — creating two must not panic with
	// "already registered" errors.
	p1 := metrics.NewPrometheusProvider()
	p2 := metrics.NewPrometheusProvider()
	p1.RecordEventIngested("s1")
	p2.RecordEventIngested("s2")
}
