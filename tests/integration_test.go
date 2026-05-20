// Package tests wires the full Xyllo pipeline together and exercises it
// end-to-end using Fiber's in-process app.Test helper — no real TCP port is
// bound, making these tests fast and hermetic.
//
// Run with: go test ./tests/...
package tests

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/golang-jwt/jwt/v5"
	"github.com/yourusername/xyllo/config"
	"github.com/yourusername/xyllo/internal/batcher"
	"github.com/yourusername/xyllo/internal/dispatcher"
	"github.com/yourusername/xyllo/internal/dlq"
	"github.com/yourusername/xyllo/internal/ingestor"
	"github.com/yourusername/xyllo/internal/middleware"
	"github.com/yourusername/xyllo/internal/translator"
)

// ── Stack construction ─────────────────────────────────────────────────────────

// stackOpts parameterises the test pipeline. Zero values produce a safe,
// permissive default (auth=none, rate limiting disabled, buffer=100).
type stackOpts struct {
	authCfg   config.AuthConfig
	rlCfg     config.RateLimitConfig // only applied when rlCfg.Enabled == true
	workers   int
	bufSize   int
	dlqTarget string // non-empty writes DLQ entries to a file for inspection
}

func defaultOpts() stackOpts {
	return stackOpts{
		authCfg: config.AuthConfig{Mode: "none"},
		workers: 2,
		bufSize: 100,
	}
}

// testStack holds the running pipeline and a cleanup function.
type testStack struct {
	app     *fiber.App
	cleanup func() // idempotent via sync.Once
}

func newStack(t *testing.T, opts stackOpts) *testStack {
	t.Helper()

	dlqSink, err := dlq.New(config.DLQConfig{Backend: "file", Target: opts.dlqTarget})
	if err != nil {
		t.Fatalf("dlq.New: %v", err)
	}

	// Build the middleware chain. Rate limiter is prepended when enabled so
	// that per-source quotas are checked before heavier schema validation.
	var handlers []middleware.Handler
	if opts.rlCfg.Enabled {
		rl := middleware.NewRateLimiter(opts.rlCfg)
		handlers = append(handlers, rl.Middleware())
	}
	handlers = append(handlers, middleware.SchemaValidator(), middleware.TypeChecker())
	chain := middleware.NewChain(handlers...)

	bat := batcher.New(config.BatcherConfig{
		MaxSize:       100,
		FlushInterval: time.Minute, // no timer-driven flush during tests
	})
	disp := dispatcher.New(
		config.DispatcherConfig{Workers: opts.workers, BufferSize: opts.bufSize},
		chain, bat, dlqSink,
	)

	reg := translator.NewRegistry()
	cfg := &config.Config{
		Auth:          opts.authCfg,
		Observability: config.ObservabilityConfig{MetricsPath: "/metrics"},
	}
	srv := ingestor.New("0", cfg, disp, reg)
	app := srv.BuildApp()

	// Cleanup is made idempotent so tests that call it early (to drain workers
	// before inspection) can still defer it safely without a double-close panic.
	var once sync.Once
	cleanup := func() {
		once.Do(func() {
			disp.Shutdown()
			bat.Stop()
			dlqSink.Close()
		})
	}

	return &testStack{app: app, cleanup: cleanup}
}

// ── Request helpers ────────────────────────────────────────────────────────────

// doRequest builds and sends a request to the test app, returning the response.
func doRequest(t *testing.T, app *fiber.App, method, path, body string, headers map[string]string) *http.Response {
	t.Helper()
	var rb io.Reader
	if body != "" {
		rb = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, rb)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	return resp
}

// readJSON drains resp.Body and unmarshals it as a JSON object.
func readJSON(t *testing.T, resp *http.Response) map[string]any {
	t.Helper()
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		t.Fatalf("unmarshal body %q: %v", b, err)
	}
	return m
}

// signJWT produces a signed HS256 JWT for the given claims.
func signJWT(t *testing.T, secret string, claims jwt.MapClaims) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	s, err := tok.SignedString([]byte(secret))
	if err != nil {
		t.Fatalf("signJWT: %v", err)
	}
	return s
}

func validJWTClaims() jwt.MapClaims {
	return jwt.MapClaims{
		"sub": "integration-test",
		"iat": time.Now().Unix(),
		"exp": time.Now().Add(time.Hour).Unix(),
	}
}

// waitForDLQEntry polls path until at least one valid JSON entry appears or
// the deadline expires. Returns the first entry found.
func waitForDLQEntry(t *testing.T, path string, timeout time.Duration) dlq.Entry {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		data, err := os.ReadFile(path)
		if err == nil {
			for _, line := range bytes.Split(bytes.TrimSpace(data), []byte("\n")) {
				if len(bytes.TrimSpace(line)) == 0 {
					continue
				}
				var entry dlq.Entry
				if jsonErr := json.Unmarshal(line, &entry); jsonErr == nil {
					return entry
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for DLQ entry in %q", path)
	return dlq.Entry{}
}

// validEventBody passes Passthrough translation and the full middleware chain.
const validEventBody = `{"event_type":"pageview","timestamp_ms":1700000000000,"body":{"page":"/home"}}`

const testJWTSecret = "integration-test-secret"

// ── Probe routes ───────────────────────────────────────────────────────────────

func TestPipeline_Healthz_Returns200(t *testing.T) {
	s := newStack(t, defaultOpts())
	defer s.cleanup()

	resp := doRequest(t, s.app, http.MethodGet, "/healthz", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", resp.StatusCode)
	}
	body := readJSON(t, resp)
	if body["status"] != "ok" {
		t.Errorf("want status=ok, got %v", body["status"])
	}
}

func TestPipeline_Readyz_Returns200(t *testing.T) {
	s := newStack(t, defaultOpts())
	defer s.cleanup()

	resp := doRequest(t, s.app, http.MethodGet, "/readyz", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("want 200, got %d", resp.StatusCode)
	}
}

// Probe routes are intentionally exempt from auth — the load balancer must
// always be able to reach them.
func TestPipeline_Healthz_BypassesAuthMiddleware(t *testing.T) {
	opts := defaultOpts()
	opts.authCfg = config.AuthConfig{Mode: "apikey", APIKey: "secret"}
	s := newStack(t, opts)
	defer s.cleanup()

	// No X-API-Key header — healthz must still return 200.
	resp := doRequest(t, s.app, http.MethodGet, "/healthz", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("healthz must bypass auth middleware; want 200, got %d", resp.StatusCode)
	}
}

func TestPipeline_Readyz_BypassesAuthMiddleware(t *testing.T) {
	opts := defaultOpts()
	opts.authCfg = config.AuthConfig{Mode: "jwt", JWTSecret: testJWTSecret}
	s := newStack(t, opts)
	defer s.cleanup()

	resp := doRequest(t, s.app, http.MethodGet, "/readyz", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("readyz must bypass auth middleware; want 200, got %d", resp.StatusCode)
	}
}

// ── Ingest — HTTP contract ─────────────────────────────────────────────────────

func TestPipeline_Ingest_ValidEvent_Returns202WithIDAndStatus(t *testing.T) {
	s := newStack(t, defaultOpts())
	defer s.cleanup()

	resp := doRequest(t, s.app, http.MethodPost, "/v1/ingest", validEventBody,
		map[string]string{"X-Source-ID": "test-source"})

	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("want 202, got %d", resp.StatusCode)
	}
	body := readJSON(t, resp)
	if body["status"] != "accepted" {
		t.Errorf("want status=accepted, got %v", body["status"])
	}
	if id, ok := body["id"].(string); !ok || id == "" {
		t.Error("want non-empty string id in response body")
	}
}

func TestPipeline_Ingest_TwoEvents_AssignDifferentIDs(t *testing.T) {
	s := newStack(t, defaultOpts())
	defer s.cleanup()

	send := func() string {
		resp := doRequest(t, s.app, http.MethodPost, "/v1/ingest", validEventBody, nil)
		body := readJSON(t, resp)
		id, _ := body["id"].(string)
		return id
	}

	if id1, id2 := send(), send(); id1 == "" || id1 == id2 {
		t.Errorf("want unique non-empty IDs per request; got id1=%q id2=%q", id1, id2)
	}
}

func TestPipeline_Ingest_MissingSourceIDDefaultsToUnknown(t *testing.T) {
	s := newStack(t, defaultOpts())
	defer s.cleanup()

	// No X-Source-ID header — must not error, defaults to "unknown".
	resp := doRequest(t, s.app, http.MethodPost, "/v1/ingest", validEventBody, nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("want 202 when X-Source-ID is absent, got %d", resp.StatusCode)
	}
}

func TestPipeline_Ingest_EmptyBody_Returns400(t *testing.T) {
	s := newStack(t, defaultOpts())
	defer s.cleanup()

	resp := doRequest(t, s.app, http.MethodPost, "/v1/ingest", "", nil)
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("want 400, got %d", resp.StatusCode)
	}
	body := readJSON(t, resp)
	if body["error"] == "" {
		t.Error("want non-empty error field in 400 body")
	}
}

func TestPipeline_Ingest_MalformedJSON_Returns422(t *testing.T) {
	s := newStack(t, defaultOpts())
	defer s.cleanup()

	resp := doRequest(t, s.app, http.MethodPost, "/v1/ingest", "not-json", nil)
	if resp.StatusCode != http.StatusUnprocessableEntity {
		t.Errorf("want 422, got %d", resp.StatusCode)
	}
}

func TestPipeline_Ingest_BackpressureReturns429(t *testing.T) {
	opts := defaultOpts()
	// bufSize=0 → every non-blocking Submit returns false immediately.
	opts.bufSize = 0
	opts.workers = 0
	s := newStack(t, opts)
	defer s.cleanup()

	resp := doRequest(t, s.app, http.MethodPost, "/v1/ingest", validEventBody, nil)
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Errorf("want 429 when buffer is full, got %d", resp.StatusCode)
	}
	body := readJSON(t, resp)
	if body["error"] == "" {
		t.Error("want non-empty error field in 429 body")
	}
}

// ── Auth — mode none ───────────────────────────────────────────────────────────

func TestPipeline_Auth_None_NoCredentials_Passes(t *testing.T) {
	s := newStack(t, defaultOpts()) // mode=none
	defer s.cleanup()

	resp := doRequest(t, s.app, http.MethodPost, "/v1/ingest", validEventBody, nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("mode=none: want 202, got %d", resp.StatusCode)
	}
}

// ── Auth — mode apikey ─────────────────────────────────────────────────────────

func TestPipeline_Auth_APIKey_ValidKey_Returns202(t *testing.T) {
	opts := defaultOpts()
	opts.authCfg = config.AuthConfig{Mode: "apikey", APIKey: "correct-key"}
	s := newStack(t, opts)
	defer s.cleanup()

	resp := doRequest(t, s.app, http.MethodPost, "/v1/ingest", validEventBody,
		map[string]string{"X-API-Key": "correct-key"})
	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("want 202, got %d", resp.StatusCode)
	}
}

func TestPipeline_Auth_APIKey_MissingHeader_Returns401(t *testing.T) {
	opts := defaultOpts()
	opts.authCfg = config.AuthConfig{Mode: "apikey", APIKey: "correct-key"}
	s := newStack(t, opts)
	defer s.cleanup()

	resp := doRequest(t, s.app, http.MethodPost, "/v1/ingest", validEventBody, nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("want 401 for missing API key, got %d", resp.StatusCode)
	}
}

func TestPipeline_Auth_APIKey_WrongKey_Returns401(t *testing.T) {
	opts := defaultOpts()
	opts.authCfg = config.AuthConfig{Mode: "apikey", APIKey: "correct-key"}
	s := newStack(t, opts)
	defer s.cleanup()

	resp := doRequest(t, s.app, http.MethodPost, "/v1/ingest", validEventBody,
		map[string]string{"X-API-Key": "wrong-key"})
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("want 401 for wrong API key, got %d", resp.StatusCode)
	}
}

// ── Auth — mode jwt ────────────────────────────────────────────────────────────

func TestPipeline_Auth_JWT_ValidToken_Returns202(t *testing.T) {
	opts := defaultOpts()
	opts.authCfg = config.AuthConfig{Mode: "jwt", JWTSecret: testJWTSecret}
	s := newStack(t, opts)
	defer s.cleanup()

	tok := signJWT(t, testJWTSecret, validJWTClaims())
	resp := doRequest(t, s.app, http.MethodPost, "/v1/ingest", validEventBody,
		map[string]string{"Authorization": "Bearer " + tok})
	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("want 202, got %d", resp.StatusCode)
	}
}

func TestPipeline_Auth_JWT_MissingHeader_Returns401(t *testing.T) {
	opts := defaultOpts()
	opts.authCfg = config.AuthConfig{Mode: "jwt", JWTSecret: testJWTSecret}
	s := newStack(t, opts)
	defer s.cleanup()

	resp := doRequest(t, s.app, http.MethodPost, "/v1/ingest", validEventBody, nil)
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("want 401 for missing JWT, got %d", resp.StatusCode)
	}
}

func TestPipeline_Auth_JWT_ExpiredToken_Returns401(t *testing.T) {
	opts := defaultOpts()
	opts.authCfg = config.AuthConfig{Mode: "jwt", JWTSecret: testJWTSecret}
	s := newStack(t, opts)
	defer s.cleanup()

	claims := jwt.MapClaims{
		"sub": "test",
		"iat": time.Now().Add(-2 * time.Hour).Unix(),
		"exp": time.Now().Add(-1 * time.Hour).Unix(),
	}
	tok := signJWT(t, testJWTSecret, claims)
	resp := doRequest(t, s.app, http.MethodPost, "/v1/ingest", validEventBody,
		map[string]string{"Authorization": "Bearer " + tok})
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("want 401 for expired token, got %d", resp.StatusCode)
	}
}

func TestPipeline_Auth_JWT_WrongSecret_Returns401(t *testing.T) {
	opts := defaultOpts()
	opts.authCfg = config.AuthConfig{Mode: "jwt", JWTSecret: testJWTSecret}
	s := newStack(t, opts)
	defer s.cleanup()

	tok := signJWT(t, "different-secret", validJWTClaims())
	resp := doRequest(t, s.app, http.MethodPost, "/v1/ingest", validEventBody,
		map[string]string{"Authorization": "Bearer " + tok})
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("want 401 for wrong JWT secret, got %d", resp.StatusCode)
	}
}

func TestPipeline_Auth_JWT_WrongIssuer_Returns401(t *testing.T) {
	opts := defaultOpts()
	opts.authCfg = config.AuthConfig{Mode: "jwt", JWTSecret: testJWTSecret, JWTIssuer: "xyllo"}
	s := newStack(t, opts)
	defer s.cleanup()

	claims := validJWTClaims()
	claims["iss"] = "other-service"
	tok := signJWT(t, testJWTSecret, claims)
	resp := doRequest(t, s.app, http.MethodPost, "/v1/ingest", validEventBody,
		map[string]string{"Authorization": "Bearer " + tok})
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("want 401 for wrong JWT issuer, got %d", resp.StatusCode)
	}
}

func TestPipeline_Auth_JWT_CorrectIssuer_Returns202(t *testing.T) {
	opts := defaultOpts()
	opts.authCfg = config.AuthConfig{Mode: "jwt", JWTSecret: testJWTSecret, JWTIssuer: "xyllo"}
	s := newStack(t, opts)
	defer s.cleanup()

	claims := validJWTClaims()
	claims["iss"] = "xyllo"
	tok := signJWT(t, testJWTSecret, claims)
	resp := doRequest(t, s.app, http.MethodPost, "/v1/ingest", validEventBody,
		map[string]string{"Authorization": "Bearer " + tok})
	if resp.StatusCode != http.StatusAccepted {
		t.Errorf("want 202 for correct issuer, got %d", resp.StatusCode)
	}
}

// ── Middleware chain — DLQ routing ─────────────────────────────────────────────
//
// Validation runs asynchronously inside dispatcher workers. Tests that need
// to verify DLQ state call s.cleanup() directly (which drains workers) before
// reading the file. The deferred call is a no-op thanks to sync.Once.

func TestPipeline_Middleware_ValidEventDoesNotWriteToDLQ(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "dlq-*.jsonl")
	if err != nil {
		t.Fatalf("create temp dlq file: %v", err)
	}
	f.Close()

	opts := defaultOpts()
	opts.dlqTarget = f.Name()
	s := newStack(t, opts)
	defer s.cleanup()

	resp := doRequest(t, s.app, http.MethodPost, "/v1/ingest", validEventBody,
		map[string]string{"X-Source-ID": "clean-source"})
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("setup: want 202, got %d", resp.StatusCode)
	}

	// Drain all workers synchronously before reading the DLQ file.
	s.cleanup()

	data, _ := os.ReadFile(f.Name())
	if len(bytes.TrimSpace(data)) != 0 {
		t.Errorf("DLQ must be empty for a valid event; got: %s", data)
	}
}

// Events with a missing event_type field pass Passthrough translation (valid
// JSON) but fail SchemaValidator because EventType is empty in the canonical
// struct. They should be routed to the DLQ.
func TestPipeline_Middleware_MissingEventType_WritesToDLQ(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "dlq-*.jsonl")
	if err != nil {
		t.Fatalf("create temp dlq file: %v", err)
	}
	f.Close()

	opts := defaultOpts()
	opts.dlqTarget = f.Name()
	s := newStack(t, opts)
	defer s.cleanup()

	// No event_type → Passthrough sets EventType="" → SchemaValidator rejects.
	body := `{"timestamp_ms":1700000000000}`
	resp := doRequest(t, s.app, http.MethodPost, "/v1/ingest", body,
		map[string]string{"X-Source-ID": "bad-source"})
	// The ingestor returns 202 — validation is asynchronous.
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("ingestor should accept (validation is async); want 202, got %d", resp.StatusCode)
	}

	entry := waitForDLQEntry(t, f.Name(), 2*time.Second)
	if entry.Source != "bad-source" {
		t.Errorf("DLQ entry source: want %q, got %q", "bad-source", entry.Source)
	}
	if entry.Reason == "" {
		t.Error("DLQ entry reason must be non-empty")
	}
}

// Events with an explicitly empty event_type string fail SchemaValidator's
// empty-string check. Note: timestamp_ms=0 is NOT tested here because
// Passthrough coerces 0 → time.Now(), so it never reaches SchemaValidator.
func TestPipeline_Middleware_EmptyEventType_WritesToDLQ(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "dlq-*.jsonl")
	if err != nil {
		t.Fatalf("create temp dlq file: %v", err)
	}
	f.Close()

	opts := defaultOpts()
	opts.dlqTarget = f.Name()
	s := newStack(t, opts)
	defer s.cleanup()

	// event_type is present but explicitly empty — SchemaValidator rejects it.
	body := `{"event_type":"","timestamp_ms":1700000000000}`
	doRequest(t, s.app, http.MethodPost, "/v1/ingest", body,
		map[string]string{"X-Source-ID": "empty-type-source"})

	entry := waitForDLQEntry(t, f.Name(), 2*time.Second)
	if entry.Source != "empty-type-source" {
		t.Errorf("DLQ entry source: want %q, got %q", "empty-type-source", entry.Source)
	}
}

// ── Rate limiter — DLQ routing ─────────────────────────────────────────────────

// With a burst of 1 and 1 RPS, the second immediate request from the same
// source exhausts the token bucket and is routed to the DLQ.
func TestPipeline_RateLimit_ExceededEvent_WritesToDLQ(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "dlq-*.jsonl")
	if err != nil {
		t.Fatalf("create temp dlq file: %v", err)
	}
	f.Close()

	opts := defaultOpts()
	opts.dlqTarget = f.Name()
	opts.rlCfg = config.RateLimitConfig{
		Enabled:           true,
		RequestsPerSecond: 1,
		BurstSize:         1,
	}
	s := newStack(t, opts)
	defer s.cleanup()

	const source = "rate-limited-source"
	headers := map[string]string{"X-Source-ID": source}

	// First request consumes the only token.
	doRequest(t, s.app, http.MethodPost, "/v1/ingest", validEventBody, headers)
	// Second request immediately after — bucket is empty, rate limiter rejects.
	doRequest(t, s.app, http.MethodPost, "/v1/ingest", validEventBody, headers)

	entry := waitForDLQEntry(t, f.Name(), 2*time.Second)
	if entry.Source != source {
		t.Errorf("DLQ entry source: want %q, got %q", source, entry.Source)
	}
	if !strings.Contains(entry.Reason, "rate limit") {
		t.Errorf("DLQ reason should mention rate limit; got %q", entry.Reason)
	}
}

// ── Benchmark ─────────────────────────────────────────────────────────────────

func BenchmarkIngest(b *testing.B) {
	dlqSink, _ := dlq.New(config.DLQConfig{})
	chain := middleware.NewChain(middleware.SchemaValidator(), middleware.TypeChecker())
	bat := batcher.New(config.BatcherConfig{MaxSize: 1000, FlushInterval: time.Minute})
	disp := dispatcher.New(
		config.DispatcherConfig{Workers: 4, BufferSize: 10000},
		chain, bat, dlqSink,
	)
	reg := translator.NewRegistry()
	cfg := &config.Config{
		Auth:          config.AuthConfig{Mode: "none"},
		Observability: config.ObservabilityConfig{MetricsPath: "/metrics"},
	}
	srv := ingestor.New("0", cfg, disp, reg)
	app := srv.BuildApp()
	defer func() { disp.Shutdown(); bat.Stop() }()

	body := []byte(validEventBody)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			req := httptest.NewRequest(http.MethodPost, "/v1/ingest", bytes.NewReader(body))
			req.Header.Set("Content-Type", "application/json")
			resp, err := app.Test(req)
			if err != nil {
				b.Fatal(err)
			}
			resp.Body.Close()
		}
	})
}
