package middleware

import (
	"fmt"
	"strings"
	"testing"
)

// validPayload returns a well-formed canonical event JSON for use in tests.
func validPayload() []byte {
	return []byte(`{
		"id":           "550e8400-e29b-41d4-a716-446655440000",
		"source":       "web",
		"event_type":   "pageview",
		"timestamp_ms": 1700000000000,
		"received_at":  "2023-11-14T22:13:20Z",
		"body":         {}
	}`)
}

// result builds a *Result seeded with the given data.
func result(data []byte) *Result {
	return &Result{Source: "web", Data: data}
}

// ── SchemaValidator ──────────────────────────────────────────────────────────

func TestSchemaValidator_PassesValidPayload(t *testing.T) {
	h := SchemaValidator()
	if err := h(result(validPayload())); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSchemaValidator_RejectsInvalidJSON(t *testing.T) {
	h := SchemaValidator()
	err := h(result([]byte(`not json`)))
	if err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
	if !strings.Contains(err.Error(), "invalid JSON") {
		t.Errorf("error should mention 'invalid JSON', got: %v", err)
	}
}

func TestSchemaValidator_RejectsMissingID(t *testing.T) {
	payload := []byte(`{"source":"web","event_type":"click","timestamp_ms":1700000000000}`)
	h := SchemaValidator()
	err := h(result(payload))
	if err == nil {
		t.Fatal("expected error for missing id")
	}
	if !strings.Contains(err.Error(), `"id"`) {
		t.Errorf("error should mention field 'id', got: %v", err)
	}
}

func TestSchemaValidator_RejectsMissingSource(t *testing.T) {
	payload := []byte(`{"id":"abc","event_type":"click","timestamp_ms":1700000000000}`)
	h := SchemaValidator()
	err := h(result(payload))
	if err == nil {
		t.Fatal("expected error for missing source")
	}
	if !strings.Contains(err.Error(), `"source"`) {
		t.Errorf("error should mention field 'source', got: %v", err)
	}
}

func TestSchemaValidator_RejectsMissingEventType(t *testing.T) {
	payload := []byte(`{"id":"abc","source":"web","timestamp_ms":1700000000000}`)
	h := SchemaValidator()
	err := h(result(payload))
	if err == nil {
		t.Fatal("expected error for missing event_type")
	}
	if !strings.Contains(err.Error(), `"event_type"`) {
		t.Errorf("error should mention field 'event_type', got: %v", err)
	}
}

func TestSchemaValidator_RejectsMissingTimestamp(t *testing.T) {
	payload := []byte(`{"id":"abc","source":"web","event_type":"click"}`)
	h := SchemaValidator()
	err := h(result(payload))
	if err == nil {
		t.Fatal("expected error for missing timestamp_ms")
	}
	if !strings.Contains(err.Error(), `"timestamp_ms"`) {
		t.Errorf("error should mention field 'timestamp_ms', got: %v", err)
	}
}

func TestSchemaValidator_RejectsEmptyStringField(t *testing.T) {
	payload := []byte(`{"id":"","source":"web","event_type":"click","timestamp_ms":1700000000000}`)
	h := SchemaValidator()
	err := h(result(payload))
	if err == nil {
		t.Fatal("expected error for empty id")
	}
	if !strings.Contains(err.Error(), "must not be empty") {
		t.Errorf("error should mention 'must not be empty', got: %v", err)
	}
}

func TestSchemaValidator_RejectsZeroTimestamp(t *testing.T) {
	payload := []byte(`{"id":"abc","source":"web","event_type":"click","timestamp_ms":0}`)
	h := SchemaValidator()
	err := h(result(payload))
	if err == nil {
		t.Fatal("expected error for zero timestamp_ms")
	}
	if !strings.Contains(err.Error(), "must not be zero") {
		t.Errorf("error should mention 'must not be zero', got: %v", err)
	}
}

func TestSchemaValidator_RejectsNullField(t *testing.T) {
	payload := []byte(`{"id":null,"source":"web","event_type":"click","timestamp_ms":1700000000000}`)
	h := SchemaValidator()
	err := h(result(payload))
	if err == nil {
		t.Fatal("expected error for null id")
	}
}

// ── TypeChecker ──────────────────────────────────────────────────────────────

func TestTypeChecker_PassesValidPayload(t *testing.T) {
	h := TypeChecker()
	if err := h(result(validPayload())); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestTypeChecker_RejectsInvalidJSON(t *testing.T) {
	h := TypeChecker()
	err := h(result([]byte(`{bad}`)))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestTypeChecker_RejectsNumericID(t *testing.T) {
	payload := []byte(`{"id":42,"source":"web","event_type":"click","timestamp_ms":1700000000000}`)
	h := TypeChecker()
	err := h(result(payload))
	if err == nil {
		t.Fatal("expected error when id is a number")
	}
	if !strings.Contains(err.Error(), `"id"`) || !strings.Contains(err.Error(), "string") {
		t.Errorf("error should mention field 'id' and 'string', got: %v", err)
	}
}

func TestTypeChecker_RejectsNumericSource(t *testing.T) {
	payload := []byte(`{"id":"abc","source":123,"event_type":"click","timestamp_ms":1700000000000}`)
	h := TypeChecker()
	err := h(result(payload))
	if err == nil {
		t.Fatal("expected error when source is a number")
	}
	if !strings.Contains(err.Error(), `"source"`) {
		t.Errorf("error should mention field 'source', got: %v", err)
	}
}

func TestTypeChecker_RejectsStringTimestamp(t *testing.T) {
	payload := []byte(`{"id":"abc","source":"web","event_type":"click","timestamp_ms":"not-a-number"}`)
	h := TypeChecker()
	err := h(result(payload))
	if err == nil {
		t.Fatal("expected error when timestamp_ms is a string")
	}
	if !strings.Contains(err.Error(), `"timestamp_ms"`) || !strings.Contains(err.Error(), "number") {
		t.Errorf("error should mention 'timestamp_ms' and 'number', got: %v", err)
	}
}

func TestTypeChecker_RejectsBooleanEventType(t *testing.T) {
	payload := []byte(`{"id":"abc","source":"web","event_type":true,"timestamp_ms":1700000000000}`)
	h := TypeChecker()
	err := h(result(payload))
	if err == nil {
		t.Fatal("expected error when event_type is a boolean")
	}
	if !strings.Contains(err.Error(), `"event_type"`) {
		t.Errorf("error should mention field 'event_type', got: %v", err)
	}
}

// ── Chain integration ────────────────────────────────────────────────────────

func TestChain_RunsHandlersInOrder(t *testing.T) {
	order := []int{}
	h1 := func(r *Result) error { order = append(order, 1); return nil }
	h2 := func(r *Result) error { order = append(order, 2); return nil }
	h3 := func(r *Result) error { order = append(order, 3); return nil }

	chain := NewChain(h1, h2, h3)
	if err := chain.Run(result(validPayload())); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(order) != 3 || order[0] != 1 || order[1] != 2 || order[2] != 3 {
		t.Errorf("handlers ran out of order: %v", order)
	}
}

func TestChain_StopsOnFirstError(t *testing.T) {
	ran := 0
	h1 := func(r *Result) error { ran++; return nil }
	h2 := func(r *Result) error { ran++; return errSentinel }
	h3 := func(r *Result) error { ran++; return nil }

	chain := NewChain(h1, h2, h3)
	err := chain.Run(result(validPayload()))
	if err == nil {
		t.Fatal("expected error from chain, got nil")
	}
	if ran != 2 {
		t.Errorf("expected 2 handlers to run before stopping, ran %d", ran)
	}
}

func TestChain_SchemaAndTypeCheckersPass(t *testing.T) {
	chain := NewChain(SchemaValidator(), TypeChecker())
	if err := chain.Run(result(validPayload())); err != nil {
		t.Fatalf("valid payload failed chain: %v", err)
	}
}

func TestChain_SchemaValidatorBlocksBeforeTypeChecker(t *testing.T) {
	// Payload with missing id — SchemaValidator should fire, TypeChecker never reached.
	payload := []byte(`{"source":"web","event_type":"click","timestamp_ms":1700000000000}`)
	chain := NewChain(SchemaValidator(), TypeChecker())
	err := chain.Run(result(payload))
	if err == nil {
		t.Fatal("expected chain to reject payload missing id")
	}
	if !strings.Contains(err.Error(), "SchemaValidator") {
		t.Errorf("error should originate from SchemaValidator, got: %v", err)
	}
}

// errSentinel is a named error used in chain tests.
var errSentinel = fmt.Errorf("sentinel error")
