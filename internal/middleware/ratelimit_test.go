package middleware

import (
	"strings"
	"testing"
	"time"

	"github.com/yourusername/xyllo/config"
)

func rateLimitCfg(rps, burst int) config.RateLimitConfig {
	return config.RateLimitConfig{
		Enabled:           true,
		RequestsPerSecond: rps,
		BurstSize:         burst,
	}
}

// ── bucket ───────────────────────────────────────────────────────────────────

func TestBucket_AllowsUpToBurstSize(t *testing.T) {
	b := newBucket(rateLimitCfg(10, 5))
	for i := 0; i < 5; i++ {
		if !b.allow() {
			t.Fatalf("token %d should be allowed within burst size", i+1)
		}
	}
}

func TestBucket_DeniesWhenTokensExhausted(t *testing.T) {
	b := newBucket(rateLimitCfg(10, 3))
	// Drain the bucket.
	for i := 0; i < 3; i++ {
		b.allow()
	}
	if b.allow() {
		t.Error("expected denial after burst tokens exhausted")
	}
}

func TestBucket_RefillsOverTime(t *testing.T) {
	// 100 tokens/s = 1 token per 10ms.  Burst = 1 so starts with 1 token.
	b := newBucket(rateLimitCfg(100, 1))
	b.allow() // drain the single token

	// Wait long enough for at least one token to refill.
	time.Sleep(20 * time.Millisecond)

	if !b.allow() {
		t.Error("expected token to be available after refill interval")
	}
}

func TestBucket_DoesNotExceedCapacity(t *testing.T) {
	b := newBucket(rateLimitCfg(1000, 5))
	// Sleep so the refill would theoretically overflow if uncapped.
	time.Sleep(20 * time.Millisecond)

	// Should only be able to consume capacity (5), not more.
	allowed := 0
	for i := 0; i < 10; i++ {
		if b.allow() {
			allowed++
		}
	}
	if allowed > 5 {
		t.Errorf("tokens should not exceed burst capacity 5, got %d allowed", allowed)
	}
}

// ── RateLimiter ──────────────────────────────────────────────────────────────

func TestRateLimiter_AllowsRequestWithinLimit(t *testing.T) {
	rl := NewRateLimiter(rateLimitCfg(100, 10))
	h := rl.Middleware()
	if err := h(result(validPayload())); err != nil {
		t.Fatalf("expected request to be allowed, got: %v", err)
	}
}

func TestRateLimiter_RejectsWhenBucketExhausted(t *testing.T) {
	rl := NewRateLimiter(rateLimitCfg(1, 1))
	h := rl.Middleware()

	r := &Result{Source: "test-source", Data: validPayload()}
	h(r) // consume the single token

	err := h(r)
	if err == nil {
		t.Fatal("expected rate limit error, got nil")
	}
	if !strings.Contains(err.Error(), "rate limit exceeded") {
		t.Errorf("error should mention 'rate limit exceeded', got: %v", err)
	}
	if !strings.Contains(err.Error(), "test-source") {
		t.Errorf("error should mention the source, got: %v", err)
	}
}

func TestRateLimiter_IndependentBucketsPerSource(t *testing.T) {
	rl := NewRateLimiter(rateLimitCfg(1, 1))
	h := rl.Middleware()

	srcA := &Result{Source: "source-a", Data: validPayload()}
	srcB := &Result{Source: "source-b", Data: validPayload()}

	// First request for each source should be allowed.
	if err := h(srcA); err != nil {
		t.Fatalf("source-a first request: %v", err)
	}
	if err := h(srcB); err != nil {
		t.Fatalf("source-b first request: %v", err)
	}

	// Second request for source-a should be denied (its bucket is empty).
	if err := h(srcA); err == nil {
		t.Error("source-a second request should be denied")
	}
	// source-b bucket is also now empty.
	if err := h(srcB); err == nil {
		t.Error("source-b second request should be denied")
	}
}

func TestRateLimiter_BucketFor_CreatesOnFirstAccess(t *testing.T) {
	rl := NewRateLimiter(rateLimitCfg(10, 5))

	if len(rl.buckets) != 0 {
		t.Fatalf("expected 0 buckets before any request, got %d", len(rl.buckets))
	}
	rl.bucketFor("new-source")
	if len(rl.buckets) != 1 {
		t.Errorf("expected 1 bucket after first access, got %d", len(rl.buckets))
	}
}

func TestRateLimiter_BucketFor_ReusesSameBucket(t *testing.T) {
	rl := NewRateLimiter(rateLimitCfg(10, 5))

	b1 := rl.bucketFor("reuse-source")
	b2 := rl.bucketFor("reuse-source")
	if b1 != b2 {
		t.Error("expected the same bucket pointer on repeated calls for same source")
	}
	if len(rl.buckets) != 1 {
		t.Errorf("expected 1 bucket, got %d", len(rl.buckets))
	}
}

func TestRateLimiter_IntegratesWithChain(t *testing.T) {
	rl := NewRateLimiter(rateLimitCfg(100, 5))
	chain := NewChain(SchemaValidator(), TypeChecker(), rl.Middleware())

	// Five valid requests should all pass.
	for i := 0; i < 5; i++ {
		if err := chain.Run(result(validPayload())); err != nil {
			t.Fatalf("request %d failed: %v", i+1, err)
		}
	}
}
