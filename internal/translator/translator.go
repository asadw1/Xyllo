// Package translator implements Xyllo's Anti-Corruption Layer (ACL).
// It defines the canonical Event — the single internal domain model used by
// every pipeline stage — and the InboundTranslator / OutboundTranslator
// interfaces that isolate the pipeline from the schemas of external sources
// and upstream analytics consumers.
//
// Architecture note: raw source bytes must never cross the ingestor boundary
// into the dispatcher or middleware chain.  All internal stages operate
// exclusively on *Event.
package translator

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// Event is Xyllo's canonical internal representation of a telemetry event.
// All inbound source formats are normalised into this struct by an
// InboundTranslator before entering the processing pipeline.
type Event struct {
	// ID is a unique identifier assigned at ingestion time (UUID v4 recommended).
	ID string `json:"id"`
	// Source identifies the originating client or service.
	Source string `json:"source"`
	// EventType classifies the event (e.g. "click", "pageview", "metric").
	EventType string `json:"event_type"`
	// TimestampMS is the event time in Unix milliseconds (UTC).
	// Normalised from whatever the source provides (Unix seconds, RFC3339, etc.)
	TimestampMS int64 `json:"timestamp_ms"`
	// ReceivedAt is the wall-clock time Xyllo accepted the event.
	ReceivedAt time.Time `json:"received_at"`
	// Body holds the source-agnostic event payload as raw JSON.
	// Using json.RawMessage avoids a re-serialise round-trip for pass-through cases.
	Body json.RawMessage `json:"body"`
}

// InboundTranslator converts a raw source payload into a canonical Event.
// Each external source schema should have its own implementation registered
// in the Registry.  Implementations must not retain a reference to raw after
// returning.
type InboundTranslator interface {
	Translate(source string, raw []byte) (*Event, error)
}

// OutboundTranslator converts a canonical Event into the wire format expected
// by a specific upstream analytics system.
type OutboundTranslator interface {
	// Marshal serialises e into the upstream's expected byte format.
	Marshal(e *Event) ([]byte, error)
}

// Registry maps source identifiers to their InboundTranslator.
// Unknown sources fall through to the Passthrough fallback.
// All methods are safe for concurrent use.
type Registry struct {
	mu          sync.RWMutex
	translators map[string]InboundTranslator
	fallback    InboundTranslator
}

// NewRegistry creates a Registry with Passthrough as the default fallback.
func NewRegistry() *Registry {
	return &Registry{
		translators: make(map[string]InboundTranslator),
		fallback:    Passthrough{},
	}
}

// Register associates a source name with an InboundTranslator.
// Call this at startup for each known source that sends a non-canonical schema.
func (r *Registry) Register(source string, t InboundTranslator) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.translators[source] = t
}

// Translate selects the translator for source and converts raw into a canonical
// Event.  Falls back to Passthrough when no specific translator is registered.
func (r *Registry) Translate(source string, raw []byte) (*Event, error) {
	r.mu.RLock()
	t, ok := r.translators[source]
	r.mu.RUnlock()
	if !ok {
		t = r.fallback
	}
	return t.Translate(source, raw)
}

// Passthrough is the default InboundTranslator. It expects raw to already use
// a near-canonical structure and performs only minimal normalisation: extracting
// well-known top-level fields and stamping ReceivedAt.
//
// Replace with a source-specific translator for any client that sends a
// substantially different schema (different field names, timestamp format,
// nested wrapper objects, etc.).
type Passthrough struct{}

func (Passthrough) Translate(source string, raw []byte) (*Event, error) {
	var envelope struct {
		EventType   string          `json:"event_type"`
		TimestampMS int64           `json:"timestamp_ms"`
		Body        json.RawMessage `json:"body"`
	}
	if err := json.Unmarshal(raw, &envelope); err != nil {
		return nil, fmt.Errorf("passthrough translator: malformed JSON from source %q: %w", source, err)
	}

	ts := envelope.TimestampMS
	if ts == 0 {
		ts = time.Now().UnixMilli()
	}

	body := envelope.Body
	if len(body) == 0 {
		// Treat the entire payload as the body if no envelope wrapper is present.
		body = raw
	}

	return &Event{
		Source:      source,
		EventType:   envelope.EventType,
		TimestampMS: ts,
		ReceivedAt:  time.Now().UTC(),
		Body:        body,
	}, nil
}

// JSONOutbound is the default OutboundTranslator. It marshals the canonical
// Event directly to JSON for delivery to an upstream HTTP/gRPC endpoint.
//
// TODO: Implement upstream-specific translators as needed (Avro, Parquet,
// custom upstream schema, etc.).
type JSONOutbound struct{}

func (JSONOutbound) Marshal(e *Event) ([]byte, error) {
	return json.Marshal(e)
}
