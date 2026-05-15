// Package dispatcher manages the buffered channel and the worker pool that
// pulls payloads off the channel, runs them through the middleware chain, and
// routes results to either the batcher or the DLQ.
package dispatcher

import (
	"sync"

	"github.com/yourusername/xyllo/config"
	"github.com/yourusername/xyllo/internal/batcher"
	"github.com/yourusername/xyllo/internal/dlq"
	"github.com/yourusername/xyllo/internal/middleware"
)

// Payload is the unit of work travelling through the pipeline.
type Payload struct {
	// Source identifies the originating client.
	Source string
	// Data holds the raw event bytes.
	Data []byte
}

// Dispatcher owns the worker pool and the inbound buffered channel.
type Dispatcher struct {
	cfg     config.DispatcherConfig
	chain   *middleware.Chain
	batcher *batcher.Batcher
	dlq     *dlq.DLQ
	buf     chan Payload
	wg      sync.WaitGroup
}

// New creates a Dispatcher and starts the worker goroutines.
//
// TODO: Start worker goroutines inside New using d.cfg.Workers count.
// TODO: Implement backpressure signalling when buf reaches capacity.
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
	// TODO: launch d.cfg.Workers goroutines calling d.process()
	return d
}

// Submit enqueues a payload for asynchronous processing.
// Returns false if the buffer is full (backpressure signal).
func (d *Dispatcher) Submit(p Payload) bool {
	select {
	case d.buf <- p:
		return true
	default:
		return false
	}
}

// process is the inner loop executed by each worker goroutine.
//
// TODO: Implement — pull from d.buf, run d.chain.Run(payload), route to
// d.batcher or d.dlq depending on the result.
func (d *Dispatcher) process() {
	// Placeholder
}

// Shutdown drains the buffer and waits for all workers to finish.
func (d *Dispatcher) Shutdown() {
	close(d.buf)
	d.wg.Wait()
}
