// Package dlq implements the Dead Letter Queue — the sink for payloads that
// fail validation.  Rejected payloads are serialised and written to a
// configurable backend (local file, Redis stream, etc.) for offline inspection.
package dlq

import (
	"encoding/json"
	"log"
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
type DLQ struct {
	cfg config.DLQConfig
}

// New creates a DLQ with the provided configuration.
func New(cfg config.DLQConfig) *DLQ {
	return &DLQ{cfg: cfg}
}

// Push stores a rejected payload entry.
//
// TODO: Replace the log.Printf stub with actual persistence (file writer,
// Redis XADD, Kafka producer, etc.) driven by cfg.Backend.
func (d *DLQ) Push(source, reason string, raw []byte) {
	entry := Entry{
		ReceivedAt: time.Now().UTC(),
		Source:     source,
		Reason:     reason,
		Raw:        raw,
	}
	b, _ := json.Marshal(entry)
	log.Printf("[DLQ] %s", b)
}
