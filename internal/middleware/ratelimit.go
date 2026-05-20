// ratelimit.go — Token Bucket rate limiter middleware for Xyllo.
// Each unique Source value gets its own independent bucket, enforcing a
// per-client ingestion ceiling to protect downstream systems during spikes.
package middleware

import (
	"fmt"
	"sync"
	"time"

	"github.com/yourusername/xyllo/config"
	"github.com/yourusername/xyllo/internal/metrics"
)

// bucket is a single token-bucket for one source.
type bucket struct {
	mu       sync.Mutex
	tokens   float64
	capacity float64
	rate     float64 // tokens added per second
	lastFill time.Time
}

func newBucket(cfg config.RateLimitConfig) *bucket {
	cap := float64(cfg.BurstSize)
	return &bucket{
		tokens:   cap,
		capacity: cap,
		rate:     float64(cfg.RequestsPerSecond),
		lastFill: time.Now(),
	}
}

// allow refills the bucket based on elapsed time and returns true if a token
// was consumed successfully.
func (b *bucket) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(b.lastFill).Seconds()
	b.lastFill = now

	b.tokens += elapsed * b.rate
	if b.tokens > b.capacity {
		b.tokens = b.capacity
	}

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// RateLimiter holds per-source token buckets.
type RateLimiter struct {
	cfg     config.RateLimitConfig
	mu      sync.Mutex
	buckets map[string]*bucket
}

// NewRateLimiter creates a RateLimiter from the provided config.
func NewRateLimiter(cfg config.RateLimitConfig) *RateLimiter {
	return &RateLimiter{
		cfg:     cfg,
		buckets: make(map[string]*bucket),
	}
}

// Middleware returns a Handler that enforces the token-bucket limit per source.
func (rl *RateLimiter) Middleware() Handler {
	return func(r *Result) error {
		b := rl.bucketFor(r.Source)
		if !b.allow() {
			metrics.RecordRateLimited(r.Source)
			return fmt.Errorf("rate limit exceeded for source %q", r.Source)
		}
		return nil
	}
}

// bucketFor retrieves the bucket for a source, creating it on first access.
func (rl *RateLimiter) bucketFor(source string) *bucket {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	if b, ok := rl.buckets[source]; ok {
		return b
	}
	b := newBucket(rl.cfg)
	rl.buckets[source] = b
	return b
}
