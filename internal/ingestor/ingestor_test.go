// Tests for the ingestor HTTP routes.
//
// Route logic is tested via Fiber's built-in app.Test helper, which exercises
// the full handler stack without binding a real TCP port. Start() and its port
// management are covered by integration tests in tests/.
package ingestor

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/yourusername/xyllo/config"
	"github.com/yourusername/xyllo/internal/batcher"
	"github.com/yourusername/xyllo/internal/dispatcher"
	"github.com/yourusername/xyllo/internal/dlq"
	"github.com/yourusername/xyllo/internal/middleware"
	"github.com/yourusername/xyllo/internal/translator"
)

// newTestServer creates a fully wired Server suitable for route testing.
//
// bufSize controls the dispatcher channel capacity:
//   - bufSize > 0 — Submit succeeds for the first bufSize payloads (happy path).
//   - bufSize = 0 — Submit always returns false (tests 429 backpressure path).
func newTestServer(t *testing.T, bufSize int) *Server {
	t.Helper()
	chain := middleware.NewChain()
	bat := batcher.New(config.BatcherConfig{
		MaxSize:       10,
		FlushInterval: time.Second,
	})
	dlqSink, err := dlq.New(config.DLQConfig{Backend: "file", Target: ""})
	if err != nil {
		t.Fatalf("dlq.New: %v", err)
	}
	disp := dispatcher.New(
		config.DispatcherConfig{Workers: 0, BufferSize: bufSize},
		chain, bat, dlqSink,
	)
	reg := translator.NewRegistry()
	cfg := &config.Config{
		Observability: config.ObservabilityConfig{MetricsPath: "/metrics"},
	}
	return New("0", cfg, disp, reg)
}

// fiberTest sends req to the server's Fiber app and returns the response.
// It is a thin wrapper around app.Test that calls t.Fatal on internal errors.
func fiberTest(t *testing.T, srv *Server, req *http.Request) *http.Response {
	t.Helper()
	resp, err := srv.buildApp().Test(req)
	if err != nil {
		t.Fatalf("app.Test: unexpected error: %v", err)
	}
	return resp
}

// decodeBody reads resp.Body and unmarshals the JSON into dst.
func decodeBody(t *testing.T, resp *http.Response, dst any) {
	t.Helper()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response body: %v", err)
	}
	if err := json.Unmarshal(b, dst); err != nil {
		t.Fatalf("unmarshal response body %q: %v", b, err)
	}
}

// --- /healthz ---

func TestServer_Healthz_Returns200(t *testing.T) {
	srv := newTestServer(t, 1)
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)

	resp := fiberTest(t, srv, req)

	if resp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", resp.StatusCode)
	}

	var body map[string]string
	decodeBody(t, resp, &body)
	if body["status"] != "ok" {
		t.Errorf("want status=ok, got %q", body["status"])
	}
}

// --- /readyz ---

func TestServer_Readyz_Returns200(t *testing.T) {
	srv := newTestServer(t, 1)
	req := httptest.NewRequest(http.MethodGet, "/readyz", nil)

	resp := fiberTest(t, srv, req)

	if resp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", resp.StatusCode)
	}

	var body map[string]string
	decodeBody(t, resp, &body)
	if body["status"] != "ok" {
		t.Errorf("want status=ok, got %q", body["status"])
	}
}

// --- POST /v1/ingest ---

func TestServer_Ingest_Returns400OnEmptyBody(t *testing.T) {
	srv := newTestServer(t, 1)
	req := httptest.NewRequest(http.MethodPost, "/v1/ingest", nil)
	req.Header.Set("Content-Type", "application/json")

	resp := fiberTest(t, srv, req)

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("want 400, got %d", resp.StatusCode)
	}

	var body errorResponse
	decodeBody(t, resp, &body)
	if body.Error == "" {
		t.Error("want non-empty error message in response body")
	}
}

func TestServer_Ingest_Returns422OnMalformedJSON(t *testing.T) {
	srv := newTestServer(t, 1)
	req := httptest.NewRequest(http.MethodPost, "/v1/ingest",
		strings.NewReader(`not valid json`))
	req.Header.Set("Content-Type", "application/json")

	resp := fiberTest(t, srv, req)

	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Errorf("want 422, got %d", resp.StatusCode)
	}

	var body errorResponse
	decodeBody(t, resp, &body)
	if body.Error == "" {
		t.Error("want non-empty error message in response body")
	}
}

func TestServer_Ingest_AcceptsValidPayload(t *testing.T) {
	srv := newTestServer(t, 10)
	payload := `{"event_type":"pageview","timestamp_ms":1700000000000,"body":{"page":"/home"}}`
	req := httptest.NewRequest(http.MethodPost, "/v1/ingest",
		strings.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Source-ID", "test-client")

	resp := fiberTest(t, srv, req)

	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("want 202, got %d", resp.StatusCode)
	}

	var body ingestResponse
	decodeBody(t, resp, &body)
	if body.Status != "accepted" {
		t.Errorf("want status=accepted, got %q", body.Status)
	}
	if body.ID == "" {
		t.Error("want non-empty event ID in response body")
	}
}

func TestServer_Ingest_AssignsUniqueIDsPerRequest(t *testing.T) {
	srv := newTestServer(t, 10)
	payload := `{"event_type":"click","timestamp_ms":1700000000000}`

	send := func() string {
		req := httptest.NewRequest(http.MethodPost, "/v1/ingest",
			strings.NewReader(payload))
		req.Header.Set("Content-Type", "application/json")
		resp := fiberTest(t, srv, req)
		var body ingestResponse
		decodeBody(t, resp, &body)
		return body.ID
	}

	id1 := send()
	id2 := send()

	if id1 == "" || id2 == "" {
		t.Fatal("want non-empty IDs from both requests")
	}
	if id1 == id2 {
		t.Errorf("want unique IDs per request; both requests returned %q", id1)
	}
}

func TestServer_Ingest_DefaultsSourceToUnknown(t *testing.T) {
	// When X-Source-ID is absent the ingestor must not error; it should default
	// to "unknown" and still accept the payload.
	srv := newTestServer(t, 10)
	payload := `{"event_type":"metric","timestamp_ms":1700000000000}`
	req := httptest.NewRequest(http.MethodPost, "/v1/ingest",
		strings.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	// Deliberately omit X-Source-ID.

	resp := fiberTest(t, srv, req)

	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("want 202 when X-Source-ID is absent, got %d", resp.StatusCode)
	}
}

func TestServer_Ingest_Returns429WhenBufferFull(t *testing.T) {
	// bufSize=0 produces an unbuffered channel; every non-blocking Submit
	// returns false immediately, simulating a saturated dispatcher buffer.
	srv := newTestServer(t, 0)
	payload := `{"event_type":"pageview","timestamp_ms":1700000000000}`
	req := httptest.NewRequest(http.MethodPost, "/v1/ingest",
		strings.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")

	resp := fiberTest(t, srv, req)

	if resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("want 429, got %d", resp.StatusCode)
	}

	var body errorResponse
	decodeBody(t, resp, &body)
	if body.Error == "" {
		t.Error("want non-empty error message in 429 response body")
	}
}

func TestServer_Ingest_Returns404ForUnknownRoute(t *testing.T) {
	srv := newTestServer(t, 1)
	req := httptest.NewRequest(http.MethodGet, "/does/not/exist", nil)

	resp := fiberTest(t, srv, req)

	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("want 404 for unknown route, got %d", resp.StatusCode)
	}
}
