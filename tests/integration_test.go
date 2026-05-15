// Package tests contains integration and load-test helpers for Xyllo.
// Run with:  go test ./tests/... -tags integration
package tests

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestIngestEndpoint_ValidPayload verifies that a well-formed event is accepted
// with HTTP 202 and an "accepted:true" body.
//
// TODO: Spin up a real ingestor.Server (test mode) and assert full pipeline
// execution including batcher accumulation.
func TestIngestEndpoint_ValidPayload(t *testing.T) {
	t.Skip("placeholder — implement once ingestor HTTP handler is wired up")

	payload := []byte(`{"source":"test","event_type":"click","payload":{}}`)
	req := httptest.NewRequest(http.MethodPost, "/ingest", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")

	// TODO: replace with real handler call
	_ = req
}

// TestIngestEndpoint_MalformedPayload verifies that a malformed event is
// rejected with HTTP 422 and routed to the DLQ.
//
// TODO: Assert DLQ entry was created after rejection.
func TestIngestEndpoint_MalformedPayload(t *testing.T) {
	t.Skip("placeholder — implement once DLQ is wired up")
}

// TestDispatcher_Backpressure verifies that Submit returns false when the
// buffer channel is full.
//
// TODO: Fill the buffer and assert false return from dispatcher.Submit.
func TestDispatcher_Backpressure(t *testing.T) {
	t.Skip("placeholder — implement dispatcher backpressure test")
}

// BenchmarkIngest measures raw ingestion throughput (events/sec).
//
// TODO: Implement end-to-end benchmark using a loopback HTTP client.
func BenchmarkIngest(b *testing.B) {
	b.Skip("placeholder — implement end-to-end ingest benchmark")
}
