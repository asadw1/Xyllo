// Package config handles loading and validating Xyllo's runtime configuration
// from a YAML file and/or environment variable overrides.
package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the top-level configuration structure.
type Config struct {
	Dispatcher    DispatcherConfig    `yaml:"dispatcher"`
	Batcher       BatcherConfig       `yaml:"batcher"`
	DLQ           DLQConfig           `yaml:"dlq"`
	Observability ObservabilityConfig `yaml:"observability"`
	TLS           TLSConfig           `yaml:"tls"`
	Auth          AuthConfig          `yaml:"auth"`
	RateLimit     RateLimitConfig     `yaml:"rate_limit"`
}

// DispatcherConfig controls the worker pool and buffer.
type DispatcherConfig struct {
	// Workers is the number of goroutines in the pool.
	Workers int `yaml:"workers"`
	// BufferSize is the capacity of the inbound payload channel.
	BufferSize int `yaml:"buffer_size"`
}

// BatcherConfig controls aggregation before upstream delivery.
type BatcherConfig struct {
	// MaxSize triggers an immediate flush when the batch reaches this count.
	MaxSize int `yaml:"max_size"`
	// FlushInterval is the maximum time between flushes.
	FlushInterval time.Duration `yaml:"flush_interval"`
}

// DLQConfig describes the dead-letter backend.
type DLQConfig struct {
	// Backend selects the storage driver: "file", "redis", "kafka".
	Backend string `yaml:"backend"`
	// Target is a backend-specific address (file path, DSN, broker list, etc.).
	Target string `yaml:"target"`
}

// ObservabilityConfig holds Prometheus scrape settings.
type ObservabilityConfig struct {
	// MetricsPath is the HTTP path that exposes Prometheus metrics.
	MetricsPath string `yaml:"metrics_path"`
	// MetricsPort is the port on which the Prometheus /metrics endpoint is served.
	// Keeping metrics on a dedicated port allows network policy rules to restrict
	// scrape access independently of ingest traffic. Defaults to "9091".
	MetricsPort string `yaml:"metrics_port"`
}

// TLSConfig controls TLS for the HTTP and gRPC listeners.
type TLSConfig struct {
	// Enabled toggles TLS. When false the server runs in plain-text mode.
	Enabled bool `yaml:"enabled"`
	// CertFile is the path to the PEM-encoded TLS certificate.
	CertFile string `yaml:"cert_file"`
	// KeyFile is the path to the PEM-encoded private key.
	KeyFile string `yaml:"key_file"`
}

// AuthConfig controls API Key and JWT authentication.
type AuthConfig struct {
	// Mode selects the auth strategy: "apikey", "jwt", or "none".
	Mode string `yaml:"mode"`
	// APIKey is the static key checked when Mode is "apikey".
	APIKey string `yaml:"api_key"`
	// JWTSecret is the HMAC secret used to verify tokens when Mode is "jwt".
	JWTSecret string `yaml:"jwt_secret"`
	// JWTIssuer is the expected `iss` claim value.
	JWTIssuer string `yaml:"jwt_issuer"`
}

// RateLimitConfig controls the per-source Token Bucket limiter.
type RateLimitConfig struct {
	// Enabled toggles the rate limiter.
	Enabled bool `yaml:"enabled"`
	// RequestsPerSecond is the steady-state refill rate per source.
	RequestsPerSecond int `yaml:"requests_per_second"`
	// BurstSize is the maximum number of tokens a bucket can accumulate.
	BurstSize int `yaml:"burst_size"`
}

// UnmarshalYAML implements yaml.Unmarshaler so that FlushInterval can be
// expressed as a human-readable duration string (e.g. "2s", "500ms") in the
// YAML file while remaining a time.Duration in Go.
func (b *BatcherConfig) UnmarshalYAML(value *yaml.Node) error {
	type plain struct {
		MaxSize       int    `yaml:"max_size"`
		FlushInterval string `yaml:"flush_interval"`
	}
	var p plain
	if err := value.Decode(&p); err != nil {
		return err
	}
	b.MaxSize = p.MaxSize
	if p.FlushInterval != "" {
		d, err := time.ParseDuration(p.FlushInterval)
		if err != nil {
			return fmt.Errorf("batcher.flush_interval: invalid duration %q: %w", p.FlushInterval, err)
		}
		b.FlushInterval = d
	}
	return nil
}

// Load reads the YAML file at path, applies environment variable overrides for
// sensitive fields, and validates the resulting Config.
//
// Environment overrides (take precedence over the YAML file):
//   - XYLLO_API_KEY    → Auth.APIKey
//   - XYLLO_JWT_SECRET → Auth.JWTSecret
//   - XYLLO_DLQ_TARGET → DLQ.Target
//
// If path is empty, Load returns the built-in defaults without reading any file.
func Load(path string) (*Config, error) {
	cfg := defaults()

	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("config: read %q: %w", path, err)
		}
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("config: parse %q: %w", path, err)
		}
	}

	// Environment overrides — secrets must never live in YAML files checked
	// into source control. Env vars always win over file values.
	if v := os.Getenv("XYLLO_API_KEY"); v != "" {
		cfg.Auth.APIKey = v
	}
	if v := os.Getenv("XYLLO_JWT_SECRET"); v != "" {
		cfg.Auth.JWTSecret = v
	}
	if v := os.Getenv("XYLLO_DLQ_TARGET"); v != "" {
		cfg.DLQ.Target = v
	}

	if err := validate(cfg); err != nil {
		return nil, fmt.Errorf("config: validation failed: %w", err)
	}
	return cfg, nil
}

// defaults returns a Config populated with safe, runnable built-in values.
// These are used when no YAML file is provided and as the baseline before
// file values are merged in.
func defaults() *Config {
	return &Config{
		Dispatcher:    DispatcherConfig{Workers: 8, BufferSize: 4096},
		Batcher:       BatcherConfig{MaxSize: 500, FlushInterval: 2 * time.Second},
		DLQ:           DLQConfig{Backend: "file", Target: "dlq.log"},
		Observability: ObservabilityConfig{MetricsPath: "/metrics"},
		TLS:           TLSConfig{Enabled: false},
		Auth:          AuthConfig{Mode: "none"},
		RateLimit:     RateLimitConfig{Enabled: true, RequestsPerSecond: 1000, BurstSize: 2000},
	}
}

// validate checks that cfg contains only coherent, safe values.
func validate(cfg *Config) error {
	if cfg.Dispatcher.Workers < 1 {
		return fmt.Errorf("dispatcher.workers must be >= 1, got %d", cfg.Dispatcher.Workers)
	}
	if cfg.Dispatcher.BufferSize < 1 {
		return fmt.Errorf("dispatcher.buffer_size must be >= 1, got %d", cfg.Dispatcher.BufferSize)
	}
	if cfg.Batcher.MaxSize < 1 {
		return fmt.Errorf("batcher.max_size must be >= 1, got %d", cfg.Batcher.MaxSize)
	}
	if cfg.Batcher.FlushInterval <= 0 {
		return fmt.Errorf("batcher.flush_interval must be > 0, got %v", cfg.Batcher.FlushInterval)
	}

	switch cfg.Auth.Mode {
	case "none", "apikey", "jwt":
		// valid
	default:
		return fmt.Errorf("auth.mode must be one of [none apikey jwt], got %q", cfg.Auth.Mode)
	}
	if cfg.Auth.Mode == "apikey" && cfg.Auth.APIKey == "" {
		return fmt.Errorf("auth.api_key must be set when auth.mode is %q", cfg.Auth.Mode)
	}
	if cfg.Auth.Mode == "jwt" && cfg.Auth.JWTSecret == "" {
		return fmt.Errorf("auth.jwt_secret must be set when auth.mode is %q", cfg.Auth.Mode)
	}

	validDLQBackends := map[string]bool{"": true, "file": true, "redis": true, "kafka": true, "log": true}
	if !validDLQBackends[cfg.DLQ.Backend] {
		return fmt.Errorf("dlq.backend must be one of [file redis kafka log], got %q", cfg.DLQ.Backend)
	}

	if cfg.RateLimit.Enabled {
		if cfg.RateLimit.RequestsPerSecond < 1 {
			return fmt.Errorf("rate_limit.requests_per_second must be >= 1 when rate limiting is enabled, got %d", cfg.RateLimit.RequestsPerSecond)
		}
		if cfg.RateLimit.BurstSize < 1 {
			return fmt.Errorf("rate_limit.burst_size must be >= 1 when rate limiting is enabled, got %d", cfg.RateLimit.BurstSize)
		}
	}

	return nil
}
