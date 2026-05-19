package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// writeYAML writes content to a temp file and returns its path.
func writeYAML(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0600); err != nil {
		t.Fatalf("writeYAML: %v", err)
	}
	return path
}

// ── Load: empty path returns defaults ────────────────────────────────────────

func TestLoad_EmptyPath_ReturnsDefaults(t *testing.T) {
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load with empty path: %v", err)
	}
	if cfg.Dispatcher.Workers != 8 {
		t.Errorf("default Workers: want 8, got %d", cfg.Dispatcher.Workers)
	}
	if cfg.Batcher.FlushInterval != 2*time.Second {
		t.Errorf("default FlushInterval: want 2s, got %v", cfg.Batcher.FlushInterval)
	}
	if cfg.Auth.Mode != "none" {
		t.Errorf("default Auth.Mode: want %q, got %q", "none", cfg.Auth.Mode)
	}
}

// ── Load: reads YAML file ─────────────────────────────────────────────────────

func TestLoad_ReadsWorkersFromYAML(t *testing.T) {
	path := writeYAML(t, `
dispatcher:
  workers: 16
  buffer_size: 8192
batcher:
  max_size: 200
  flush_interval: 500ms
dlq:
  backend: file
  target: /tmp/test.log
observability:
  metrics_path: /metrics
auth:
  mode: none
rate_limit:
  enabled: false
  requests_per_second: 100
  burst_size: 200
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Dispatcher.Workers != 16 {
		t.Errorf("Workers: want 16, got %d", cfg.Dispatcher.Workers)
	}
	if cfg.Dispatcher.BufferSize != 8192 {
		t.Errorf("BufferSize: want 8192, got %d", cfg.Dispatcher.BufferSize)
	}
}

func TestLoad_ParsesDurationString(t *testing.T) {
	path := writeYAML(t, `
dispatcher:
  workers: 4
  buffer_size: 100
batcher:
  max_size: 50
  flush_interval: 750ms
auth:
  mode: none
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Batcher.FlushInterval != 750*time.Millisecond {
		t.Errorf("FlushInterval: want 750ms, got %v", cfg.Batcher.FlushInterval)
	}
}

func TestLoad_RejectsInvalidDurationString(t *testing.T) {
	path := writeYAML(t, `
dispatcher:
  workers: 4
  buffer_size: 100
batcher:
  max_size: 50
  flush_interval: "not-a-duration"
auth:
  mode: none
`)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for invalid duration string, got nil")
	}
}

func TestLoad_ReturnsErrorForMissingFile(t *testing.T) {
	_, err := Load("/nonexistent/path/config.yaml")
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func TestLoad_ReturnsErrorForMalformedYAML(t *testing.T) {
	path := writeYAML(t, `{this is not valid yaml: [}`)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for malformed YAML, got nil")
	}
}

// ── Environment variable overrides ───────────────────────────────────────────

func TestLoad_EnvOverride_APIKey(t *testing.T) {
	t.Setenv("XYLLO_API_KEY", "test-secret-key")
	path := writeYAML(t, `
dispatcher:
  workers: 4
  buffer_size: 100
batcher:
  max_size: 10
  flush_interval: 1s
auth:
  mode: apikey
  api_key: ""
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Auth.APIKey != "test-secret-key" {
		t.Errorf("APIKey: want %q, got %q", "test-secret-key", cfg.Auth.APIKey)
	}
}

func TestLoad_EnvOverride_JWTSecret(t *testing.T) {
	t.Setenv("XYLLO_JWT_SECRET", "super-secret")
	path := writeYAML(t, `
dispatcher:
  workers: 4
  buffer_size: 100
batcher:
  max_size: 10
  flush_interval: 1s
auth:
  mode: jwt
  jwt_secret: ""
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Auth.JWTSecret != "super-secret" {
		t.Errorf("JWTSecret: want %q, got %q", "super-secret", cfg.Auth.JWTSecret)
	}
}

func TestLoad_EnvOverride_DLQTarget(t *testing.T) {
	t.Setenv("XYLLO_DLQ_TARGET", "/var/log/xyllo/dlq.log")
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.DLQ.Target != "/var/log/xyllo/dlq.log" {
		t.Errorf("DLQ.Target: want %q, got %q", "/var/log/xyllo/dlq.log", cfg.DLQ.Target)
	}
}

func TestLoad_EnvOverride_TakesPrecedenceOverFile(t *testing.T) {
	t.Setenv("XYLLO_API_KEY", "env-wins")
	path := writeYAML(t, `
dispatcher:
  workers: 4
  buffer_size: 100
batcher:
  max_size: 10
  flush_interval: 1s
auth:
  mode: apikey
  api_key: file-value
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Auth.APIKey != "env-wins" {
		t.Errorf("env override should win over file value; got %q", cfg.Auth.APIKey)
	}
}

// ── Validation ────────────────────────────────────────────────────────────────

func TestValidate_RejectsZeroWorkers(t *testing.T) {
	cfg := defaults()
	cfg.Dispatcher.Workers = 0
	if err := validate(cfg); err == nil {
		t.Error("expected error for Workers=0")
	}
}

func TestValidate_RejectsZeroBufferSize(t *testing.T) {
	cfg := defaults()
	cfg.Dispatcher.BufferSize = 0
	if err := validate(cfg); err == nil {
		t.Error("expected error for BufferSize=0")
	}
}

func TestValidate_RejectsZeroMaxSize(t *testing.T) {
	cfg := defaults()
	cfg.Batcher.MaxSize = 0
	if err := validate(cfg); err == nil {
		t.Error("expected error for MaxSize=0")
	}
}

func TestValidate_RejectsZeroFlushInterval(t *testing.T) {
	cfg := defaults()
	cfg.Batcher.FlushInterval = 0
	if err := validate(cfg); err == nil {
		t.Error("expected error for FlushInterval=0")
	}
}

func TestValidate_RejectsUnknownAuthMode(t *testing.T) {
	cfg := defaults()
	cfg.Auth.Mode = "oauth2"
	if err := validate(cfg); err == nil {
		t.Error("expected error for unknown auth mode")
	}
}

func TestValidate_RejectsAPIKeyModeWithEmptyKey(t *testing.T) {
	cfg := defaults()
	cfg.Auth.Mode = "apikey"
	cfg.Auth.APIKey = ""
	if err := validate(cfg); err == nil {
		t.Error("expected error for apikey mode with empty APIKey")
	}
}

func TestValidate_RejectsJWTModeWithEmptySecret(t *testing.T) {
	cfg := defaults()
	cfg.Auth.Mode = "jwt"
	cfg.Auth.JWTSecret = ""
	if err := validate(cfg); err == nil {
		t.Error("expected error for jwt mode with empty JWTSecret")
	}
}

func TestValidate_RejectsUnknownDLQBackend(t *testing.T) {
	cfg := defaults()
	cfg.DLQ.Backend = "sqs"
	if err := validate(cfg); err == nil {
		t.Error("expected error for unknown DLQ backend")
	}
}

func TestValidate_RejectsRateLimitWithZeroRPS(t *testing.T) {
	cfg := defaults()
	cfg.RateLimit.Enabled = true
	cfg.RateLimit.RequestsPerSecond = 0
	if err := validate(cfg); err == nil {
		t.Error("expected error for RateLimit enabled with RPS=0")
	}
}

func TestValidate_RejectsRateLimitWithZeroBurst(t *testing.T) {
	cfg := defaults()
	cfg.RateLimit.Enabled = true
	cfg.RateLimit.BurstSize = 0
	if err := validate(cfg); err == nil {
		t.Error("expected error for RateLimit enabled with BurstSize=0")
	}
}

func TestValidate_AllowsRateLimitDisabledWithZeroValues(t *testing.T) {
	cfg := defaults()
	cfg.RateLimit.Enabled = false
	cfg.RateLimit.RequestsPerSecond = 0
	cfg.RateLimit.BurstSize = 0
	if err := validate(cfg); err != nil {
		t.Errorf("disabled rate limiter should not require non-zero values: %v", err)
	}
}

func TestValidate_AcceptsValidConfig(t *testing.T) {
	cfg := defaults()
	if err := validate(cfg); err != nil {
		t.Errorf("defaults should pass validation: %v", err)
	}
}

// ── defaults() ────────────────────────────────────────────────────────────────

func TestDefaults_AreValid(t *testing.T) {
	cfg := defaults()
	if err := validate(cfg); err != nil {
		t.Errorf("defaults() must produce a valid config, got: %v", err)
	}
}
