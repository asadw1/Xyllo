package translator_test

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/yourusername/xyllo/internal/translator"
)

// ── Registry ──────────────────────────────────────────────────────────────────

func TestRegistry_NewRegistry(t *testing.T) {
	r := translator.NewRegistry()
	if r == nil {
		t.Fatal("NewRegistry returned nil")
	}
}

func TestRegistry_FallbackToPassthrough(t *testing.T) {
	r := translator.NewRegistry()
	event, err := r.Translate("unknown", []byte(`{"event_type":"click","timestamp_ms":1000,"body":{}}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if event.EventType != "click" {
		t.Errorf("expected event_type=click, got %q", event.EventType)
	}
}

// stubTranslator is a minimal InboundTranslator for testing registry dispatch.
type stubTranslator struct{ called bool }

func (s *stubTranslator) Translate(source string, _ []byte) (*translator.Event, error) {
	s.called = true
	return &translator.Event{Source: source, EventType: "stub"}, nil
}

func TestRegistry_RegisteredTranslatorUsed(t *testing.T) {
	r := translator.NewRegistry()
	stub := &stubTranslator{}
	r.Register("my-source", stub)

	event, err := r.Translate("my-source", []byte(`{}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !stub.called {
		t.Fatal("expected registered translator to be called")
	}
	if event.EventType != "stub" {
		t.Errorf("expected event_type=stub, got %q", event.EventType)
	}
}

func TestRegistry_UnregisteredSourceFallsThrough(t *testing.T) {
	r := translator.NewRegistry()
	r.Register("source-a", &stubTranslator{})

	// "source-b" has no registration — must fall back to Passthrough without error.
	_, err := r.Translate("source-b", []byte(`{"event_type":"fallback","timestamp_ms":1}`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRegistry_ConcurrentRegisterTranslate(t *testing.T) {
	r := translator.NewRegistry()
	sources := []string{"a", "b", "c", "d", "e"}

	var wg sync.WaitGroup
	for _, src := range sources {
		src := src
		wg.Add(2)
		go func() {
			defer wg.Done()
			r.Register(src, &stubTranslator{})
		}()
		go func() {
			defer wg.Done()
			_, _ = r.Translate(src, []byte(`{"event_type":"x","timestamp_ms":1}`))
		}()
	}
	wg.Wait()
}

// ── Passthrough ────────────────────────────────────────────────────────────────

func TestPassthrough_ValidEnvelope(t *testing.T) {
	p := translator.Passthrough{}
	raw := []byte(`{"event_type":"pageview","timestamp_ms":1234567890000,"body":{"url":"/home"}}`)

	event, err := p.Translate("test-source", raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if event.Source != "test-source" {
		t.Errorf("expected source=test-source, got %q", event.Source)
	}
	if event.EventType != "pageview" {
		t.Errorf("expected event_type=pageview, got %q", event.EventType)
	}
	if event.TimestampMS != 1234567890000 {
		t.Errorf("expected timestamp_ms=1234567890000, got %d", event.TimestampMS)
	}
}

func TestPassthrough_ZeroTimestampCoercedToNow(t *testing.T) {
	p := translator.Passthrough{}
	before := time.Now().UnixMilli()
	event, err := p.Translate("src", []byte(`{"event_type":"t","timestamp_ms":0}`))
	after := time.Now().UnixMilli()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if event.TimestampMS < before || event.TimestampMS > after {
		t.Errorf("timestamp_ms %d not within [%d, %d]", event.TimestampMS, before, after)
	}
}

func TestPassthrough_NoBodyFieldUsesWholePayload(t *testing.T) {
	p := translator.Passthrough{}
	raw := []byte(`{"event_type":"raw","timestamp_ms":1}`)

	event, err := p.Translate("src", raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(event.Body) != string(raw) {
		t.Errorf("expected body=%s, got %s", raw, event.Body)
	}
}

func TestPassthrough_InvalidJSON(t *testing.T) {
	p := translator.Passthrough{}
	_, err := p.Translate("src", []byte(`not json`))
	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
}

func TestPassthrough_ReceivedAtIsPopulated(t *testing.T) {
	p := translator.Passthrough{}
	before := time.Now().UTC()
	event, err := p.Translate("src", []byte(`{"event_type":"t","timestamp_ms":1}`))
	after := time.Now().UTC()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if event.ReceivedAt.Before(before) || event.ReceivedAt.After(after) {
		t.Errorf("ReceivedAt %v not within expected range [%v, %v]", event.ReceivedAt, before, after)
	}
}

// ── JSONOutbound ───────────────────────────────────────────────────────────────

func TestJSONOutbound_Marshal(t *testing.T) {
	j := translator.JSONOutbound{}
	event := &translator.Event{
		ID:          "abc-123",
		Source:      "test",
		EventType:   "click",
		TimestampMS: 999,
	}

	data, err := j.Marshal(event)
	if err != nil {
		t.Fatalf("Marshal error: %v", err)
	}

	var out translator.Event
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("round-trip unmarshal error: %v", err)
	}
	if out.ID != "abc-123" || out.EventType != "click" {
		t.Errorf("round-trip mismatch: %+v", out)
	}
}
