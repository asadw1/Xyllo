// Package dispatcher manages the bounded inbound channel and the worker pool
// that drains it. It is responsible for fan-out and failure routing only —
// validation is delegated to internal/middleware and persistence to
// internal/batcher and internal/dlq.
//
// Concurrency model: New launches cfg.Workers goroutines, each running the
// process loop. All goroutines are tracked in a sync.WaitGroup so that
// Shutdown() can guarantee a clean drain before returning.
package dispatcher

import (
	"fmt"
	"log"
	"runtime"
	"sync"

	"github.com/yourusername/xyllo/config"
	"github.com/yourusername/xyllo/internal/batcher"
	"github.com/yourusername/xyllo/internal/dlq"
	"github.com/yourusername/xyllo/internal/metrics"
	"github.com/yourusername/xyllo/internal/middleware"
	"github.com/yourusername/xyllo/internal/xlerr"
)

// Payload is the unit of work that travels through the pipeline from the
// ingestor to the worker pool.
type Payload struct {
	// Source identifies the originating client or service.
	Source string
	// Data holds the serialised canonical Event bytes produced by the ingestor's
	// ACL translation step. Workers must treat this slice as read-only.
	Data []byte
}

// Dispatcher owns the worker pool and the inbound buffered channel.
//
// The zero value is not usable; construct with New.
type Dispatcher struct {
	cfg     config.DispatcherConfig
	chain   *middleware.Chain
	batcher *batcher.Batcher
	dlq     *dlq.DLQ
	// buf is the single congestion point of the pipeline. When it is full,
	// Submit returns false and the ingestor responds with HTTP 429.
	// Protected by its own channel semantics — no additional lock required.
	buf chan Payload
	// wg tracks all live worker goroutines so Shutdown can block until the
	// buffer is fully drained.
	wg sync.WaitGroup
}

// New creates a Dispatcher, allocates the inbound channel, and immediately
// launches cfg.Workers worker goroutines. The workers begin draining the
// channel as soon as New returns.
//
// Call Shutdown to stop all workers after signalling cancellation upstream.
func New(
	cfg config.DispatcherConfig,
	chain *middleware.Chain,
	bat *batcher.Batcher,
	dlqSink *dlq.DLQ,
) *Dispatcher {
	d := &Dispatcher{
		cfg:     cfg,
		chain:   chain,
		batcher: bat,
		dlq:     dlqSink,
		buf:     make(chan Payload, cfg.BufferSize),
	}
	for i := 0; i < cfg.Workers; i++ {
		d.wg.Add(1)
		go d.process()
	}
	return d
}

// Submit enqueues p for asynchronous processing by the worker pool.
// It is safe to call from multiple goroutines concurrently.
//
// Returns false — and does not block — when the internal buffer is at
// capacity. The caller must treat a false return as a backpressure signal
// and respond with HTTP 429 / gRPC RESOURCE_EXHAUSTED (architecture.md §3.3).
func (d *Dispatcher) Submit(p Payload) bool {
	// Non-blocking select — intentional. Backpressure is the caller's
	// responsibility (architecture.md §3.3 Dispatcher & Worker Pool).
	select {
	case d.buf <- p:
		metrics.WorkerPoolDepth.Inc()
		return true
	default:
		return false
	}
}

// process is the inner loop executed by each worker goroutine. It ranges over
// the inbound channel until it is closed by Shutdown, processing every queued
// payload before exiting. Using range guarantees the buffer is fully drained
// before the goroutine terminates.
func (d *Dispatcher) process() {
	defer d.wg.Done()
	for p := range d.buf {
		d.processOne(p)
	}
}

// processOne handles a single payload inside a deferred recover guard.
// A panic in any middleware handler is caught here, logged with a stack trace,
// routed to the DLQ, and counted in metrics — the worker goroutine then
// continues processing subsequent payloads uninterrupted.
func (d *Dispatcher) processOne(p Payload) {
	defer func() {
		if rec := recover(); rec != nil {
			stack := make([]byte, 4096)
			n := runtime.Stack(stack, false)
			pe := xlerr.New(xlerr.StageDispatch, xlerr.CodePanic, p.Source,
				fmt.Errorf("%v", rec))
			log.Printf("[dispatcher] worker panic recovered: %v\n%s", pe, stack[:n])
			metrics.PanicsRecovered.Inc()
			metrics.DLQEnqueued.Inc()
			d.dlq.Push(p.Source, pe.Error(), p.Data)
		}
	}()

	// Decrement the depth gauge now that we have taken ownership of p.
	metrics.WorkerPoolDepth.Dec()

	r := &middleware.Result{
		Source: p.Source,
		Data:   p.Data,
	}

	if err := d.chain.Run(r); err != nil {
		// Validation failed — wrap with structured context, route to DLQ.
		// The worker continues processing subsequent payloads; a single
		// bad payload must never stall the pool (architecture.md §3.4).
		pe := xlerr.New(xlerr.StageValidation, xlerr.CodeValidation, p.Source, err)
		metrics.EventsRejected.WithLabelValues(p.Source, string(xlerr.CodeValidation)).Inc()
		metrics.DLQEnqueued.Inc()
		d.dlq.Push(p.Source, pe.Error(), p.Data)
		return
	}

	// Payload passed validation — hand it to the batcher for upstream delivery.
	d.batcher.Add(p.Data)
}

// Shutdown closes the inbound channel and blocks until every worker goroutine
// has finished processing all remaining buffered payloads. It is safe to call
// exactly once after all inbound sources have stopped submitting.
//
// Calling Submit after Shutdown will panic (send on closed channel).
func (d *Dispatcher) Shutdown() {
	close(d.buf)
	d.wg.Wait()
}
