// Package dlq implements the Dead Letter Queue — the sink for payloads that
// fail validation.  Rejected payloads are serialised and written to a
// configurable backend (local file, Redis stream, etc.) for offline inspection.
package dlq

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/yourusername/xyllo/config"
)

// Entry is a single record stored in the DLQ.
type Entry struct {
	ReceivedAt time.Time `json:"received_at"`
	Source     string    `json:"source"`
	Reason     string    `json:"reason"`
	Raw        []byte    `json:"raw"`
}

// DLQ routes rejected payloads to the configured backend.
// When Backend is "file" and Target is a non-empty path, entries are written
// as newline-delimited JSON to that file (append, created if absent, mode 0600).
// When Target is empty the DLQ falls back to log output — safe for tests and
// development environments where no file path is configured.
// DLQ is safe for concurrent use by multiple goroutines.
type DLQ struct {
	cfg  config.DLQConfig
	mu   sync.Mutex
	file *os.File // non-nil only when backend is "file" with a real target path
}

// New creates a DLQ from cfg.
// Returns an error only when Backend is "file" and Target is non-empty but the
// file cannot be opened or created.
// The returned DLQ is ready to use immediately; no separate Init step required.
func New(cfg config.DLQConfig) (*DLQ, error) {
	d := &DLQ{cfg: cfg}
	if cfg.Backend == "file" && cfg.Target != "" {
		f, err := os.OpenFile(cfg.Target, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			return nil, fmt.Errorf("dlq: open file %q: %w", cfg.Target, err)
		}
		d.file = f
	}
	return d, nil
}

// Push stores a rejected payload entry.
// It is safe for concurrent use by multiple goroutines.
// Write errors are logged rather than returned — Push is called from the
// dispatcher's hot path and must never block or propagate failure back up the
// pipeline.
func (d *DLQ) Push(source, reason string, raw []byte) {
	entry := Entry{
		ReceivedAt: time.Now().UTC(),
		Source:     source,
		Reason:     reason,
		Raw:        raw,
	}
	b, err := json.Marshal(entry)
	if err != nil {
		// Entry is a plain struct with no custom marshalling; this path is
		// defensive and should never be reached in practice.
		log.Printf("[DLQ] marshal error source=%q reason=%q: %v", source, reason, err)
		return
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.file != nil {
		if _, err := fmt.Fprintf(d.file, "%s\n", b); err != nil {
			log.Printf("[DLQ] write error source=%q: %v", source, err)
		}
		return
	}
	log.Printf("[DLQ] %s", b)
}

// Close flushes and closes the underlying file, if one is open.
// After Close, further Push calls fall back to log output.
// Close is idempotent and safe to call multiple times.
func (d *DLQ) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.file == nil {
		return nil
	}
	err := d.file.Close()
	d.file = nil
	return err
}
