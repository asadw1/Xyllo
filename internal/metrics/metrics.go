// Package metrics registers and exposes Prometheus metrics for the Xyllo
// pipeline.  All counters and gauges are created here and imported by the
// packages that record observations.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	// EventsIngested counts every payload accepted by the ingestor.
	EventsIngested = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "xyllo_events_ingested_total",
		Help: "Total number of events received by the ingestor.",
	}, []string{"source"})

	// EventsRejected counts payloads that failed middleware validation.
	EventsRejected = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "xyllo_events_rejected_total",
		Help: "Total number of events rejected by the validation middleware.",
	}, []string{"source", "reason"})

	// DLQEnqueued counts payloads written to the Dead Letter Queue.
	DLQEnqueued = promauto.NewCounter(prometheus.CounterOpts{
		Name: "xyllo_dlq_enqueued_total",
		Help: "Total number of payloads routed to the Dead Letter Queue.",
	})

	// BatcherFlushes counts upstream batch flush operations.
	BatcherFlushes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "xyllo_batcher_flushes_total",
		Help: "Total number of batch flushes sent to the upstream exporter.",
	})

	// WorkerPoolDepth tracks the current number of items waiting in the
	// dispatcher buffer channel.
	WorkerPoolDepth = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "xyllo_worker_pool_buffer_depth",
		Help: "Current number of payloads queued in the dispatcher buffer.",
	})

	// RateLimitedRequests counts requests dropped by the rate limiter.
	RateLimitedRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "xyllo_rate_limited_requests_total",
		Help: "Total number of requests dropped by the per-source rate limiter.",
	}, []string{"source"})

	// PanicsRecovered counts goroutine panics caught by the dispatcher's
	// per-payload recover guard. A non-zero value warrants immediate investigation.
	PanicsRecovered = promauto.NewCounter(prometheus.CounterOpts{
		Name: "xyllo_worker_panics_recovered_total",
		Help: "Total number of panics recovered by the dispatcher worker pool.",
	})
)

// Handler returns the Prometheus HTTP handler for the /metrics endpoint.
func Handler() http.Handler {
	return promhttp.Handler()
}
