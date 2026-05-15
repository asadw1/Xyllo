// Package config handles loading and validating Xyllo's runtime configuration
// from a YAML file and/or environment variable overrides.
package config

import "time"

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

// Load reads the YAML file at path and returns a validated Config.
//
// TODO: Implement YAML unmarshal + env-override + struct validation.
func Load(path string) (*Config, error) {
	// Placeholder — return sensible defaults until file loading is implemented.
	return &Config{
		Dispatcher:    DispatcherConfig{Workers: 8, BufferSize: 4096},
		Batcher:       BatcherConfig{MaxSize: 500, FlushInterval: 2 * time.Second},
		DLQ:           DLQConfig{Backend: "file", Target: "dlq.log"},
		Observability: ObservabilityConfig{MetricsPath: "/metrics"},
		TLS:           TLSConfig{Enabled: false},
		Auth:          AuthConfig{Mode: "none"},
		RateLimit:     RateLimitConfig{Enabled: true, RequestsPerSecond: 1000, BurstSize: 2000},
	}, nil
}
