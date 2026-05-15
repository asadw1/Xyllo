// Package pool provides a sync.Pool-backed allocator for Payload objects,
// reducing GC pressure under high-throughput ingestion by reusing allocations.
package pool

import "sync"

// Payload is the pooled unit of work that travels through the pipeline.
// Fields are reset in Put before the object is returned to the pool.
type Payload struct {
	// Source identifies the originating client or service.
	Source string
	// Data holds the raw event bytes. The slice is reused across calls;
	// callers must not retain a reference after calling Put.
	Data []byte
}

var payloadPool = sync.Pool{
	New: func() any {
		return &Payload{
			// Pre-allocate a 4 KB backing array to avoid small re-allocations
			// for typical telemetry payloads.
			Data: make([]byte, 0, 4096),
		}
	},
}

// Get retrieves a Payload from the pool.  The returned object is zeroed and
// ready for use.
func Get() *Payload {
	return payloadPool.Get().(*Payload)
}

// Put resets p and returns it to the pool.  Callers must not use p after
// calling Put.
func Put(p *Payload) {
	p.Source = ""
	p.Data = p.Data[:0] // retain underlying array, reset length
	payloadPool.Put(p)
}
