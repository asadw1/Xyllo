// Package batcher accumulates validated payloads and flushes them to upstream
// analytics in efficient batches, reducing per-event write overhead.
package batcher

import (
	"sync"
	"time"

	"github.com/yourusername/xyllo/config"
)

// Batch is a slice of validated raw payloads ready for upstream delivery.
type Batch [][]byte

// Batcher buffers validated payloads and periodically flushes them.
type Batcher struct {
	cfg    config.BatcherConfig
	mu     sync.Mutex
	buf    Batch
	ticker *time.Ticker
}

// New creates a Batcher and starts the periodic flush ticker.
//
// TODO: Start a goroutine that calls Flush() on each ticker tick.
func New(cfg config.BatcherConfig) *Batcher {
	return &Batcher{
		cfg:    cfg,
		ticker: time.NewTicker(cfg.FlushInterval),
	}
}

// Add appends a validated payload to the internal buffer.
// Triggers an immediate flush when the batch size limit is reached.
func (b *Batcher) Add(data []byte) {
	b.mu.Lock()
	b.buf = append(b.buf, data)
	full := len(b.buf) >= b.cfg.MaxSize
	b.mu.Unlock()

	if full {
		b.Flush()
	}
}

// Flush drains the buffer and sends the batch to the upstream exporter.
//
// TODO: Implement the upstream HTTP/gRPC export call.
func (b *Batcher) Flush() {
	b.mu.Lock()
	if len(b.buf) == 0 {
		b.mu.Unlock()
		return
	}
	batch := b.buf
	b.buf = nil
	b.mu.Unlock()

	// Placeholder — replace with real upstream delivery.
	_ = batch
}

// Stop cancels the flush ticker.
func (b *Batcher) Stop() {
	b.ticker.Stop()
}
