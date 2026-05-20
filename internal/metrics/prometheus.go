// Package metrics — Prometheus implementation of the Provider interface.
// Register it at startup: metrics.SetProvider(metrics.NewPrometheusProvider())
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// PrometheusProvider implements Provider using prometheus/client_golang.
// Each instance owns a private prometheus.Registry so multiple instances can
// coexist in the same process (e.g. during test isolation) without triggering
// "already registered" panics.
type PrometheusProvider struct {
	reg             *prometheus.Registry
	eventsIngested  *prometheus.CounterVec
	eventsRejected  *prometheus.CounterVec
	dlqEnqueued     prometheus.Counter
	batcherFlushes  prometheus.Counter
	workerPoolDepth prometheus.Gauge
	rateLimited     *prometheus.CounterVec
	panicsRecovered prometheus.Counter
}

// NewPrometheusProvider creates and registers all Xyllo metrics in a private
// Prometheus registry. Call once and pass to SetProvider.
func NewPrometheusProvider() *PrometheusProvider {
	reg := prometheus.NewRegistry()

	p := &PrometheusProvider{
		reg: reg,

		eventsIngested: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "xyllo_events_ingested_total",
			Help: "Total number of events received by the ingestor.",
		}, []string{"source"}),

		eventsRejected: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "xyllo_events_rejected_total",
			Help: "Total number of events rejected by the validation middleware.",
		}, []string{"source", "reason"}),

		dlqEnqueued: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "xyllo_dlq_enqueued_total",
			Help: "Total number of payloads routed to the Dead Letter Queue.",
		}),

		batcherFlushes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "xyllo_batcher_flushes_total",
			Help: "Total number of batch flushes sent to the upstream exporter.",
		}),

		workerPoolDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "xyllo_worker_pool_buffer_depth",
			Help: "Current number of payloads queued in the dispatcher buffer.",
		}),

		rateLimited: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "xyllo_rate_limited_requests_total",
			Help: "Total number of requests dropped by the per-source rate limiter.",
		}, []string{"source"}),

		panicsRecovered: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "xyllo_worker_panics_recovered_total",
			Help: "Total number of panics recovered by the dispatcher worker pool.",
		}),
	}

	reg.MustRegister(
		p.eventsIngested,
		p.eventsRejected,
		p.dlqEnqueued,
		p.batcherFlushes,
		p.workerPoolDepth,
		p.rateLimited,
		p.panicsRecovered,
		prometheus.NewGoCollector(),
		prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}),
	)

	return p
}

func (p *PrometheusProvider) RecordEventIngested(source string) {
	p.eventsIngested.WithLabelValues(source).Inc()
}

func (p *PrometheusProvider) RecordEventRejected(source, reason string) {
	p.eventsRejected.WithLabelValues(source, reason).Inc()
}

func (p *PrometheusProvider) RecordDLQEnqueued() {
	p.dlqEnqueued.Inc()
}

func (p *PrometheusProvider) RecordBatcherFlush() {
	p.batcherFlushes.Inc()
}

func (p *PrometheusProvider) ObserveWorkerPoolDepth(n float64) {
	p.workerPoolDepth.Set(n)
}

func (p *PrometheusProvider) RecordRateLimited(source string) {
	p.rateLimited.WithLabelValues(source).Inc()
}

func (p *PrometheusProvider) RecordPanicRecovered() {
	p.panicsRecovered.Inc()
}

// Handler returns a promhttp handler scoped to this provider's private registry.
func (p *PrometheusProvider) Handler() http.Handler {
	return promhttp.HandlerFor(p.reg, promhttp.HandlerOpts{})
}
