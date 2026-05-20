// Package metrics defines the Provider interface through which all pipeline
// packages emit metric observations. A concrete implementation is registered
// once at startup via SetProvider. The default is a no-op provider so that
// packages that don't call SetProvider (e.g. unit tests) are safe to use
// without any initialisation overhead or external dependencies.
package metrics

import "net/http"

// Provider is the single surface through which the pipeline records metrics.
// Inject a concrete implementation at startup with SetProvider; swap backends
// without touching any pipeline code.
type Provider interface {
	// RecordEventIngested increments the ingested-events counter for source.
	RecordEventIngested(source string)
	// RecordEventRejected increments the rejected-events counter for source and reason.
	RecordEventRejected(source, reason string)
	// RecordDLQEnqueued increments the DLQ enqueued counter.
	RecordDLQEnqueued()
	// RecordBatcherFlush increments the batcher flush counter.
	RecordBatcherFlush()
	// ObserveWorkerPoolDepth records the current number of payloads waiting in
	// the dispatcher buffer. Pass float64(len(buf)) at the point of observation.
	ObserveWorkerPoolDepth(n float64)
	// RecordRateLimited increments the rate-limited-requests counter for source.
	RecordRateLimited(source string)
	// RecordPanicRecovered increments the panic-recovered counter.
	RecordPanicRecovered()
	// Handler returns an HTTP handler that exposes collected metrics for scraping.
	// Push-based implementations (OTLP, statsd) should return http.NotFoundHandler.
	Handler() http.Handler
}

// active is the registered Provider. Defaults to nopProvider so that packages
// imported without a SetProvider call do not panic.
var active Provider = nopProvider{}

// SetProvider registers p as the active metrics Provider. Call once from main
// before starting any pipeline components.
func SetProvider(p Provider) { active = p }

// NewNopProvider returns a Provider that silently discards all observations.
// Useful in tests that need to restore a neutral provider after calling SetProvider.
func NewNopProvider() Provider { return nopProvider{} }

// Package-level delegation functions — these are the only symbols pipeline
// packages should call. No pipeline code should import Prometheus directly.

func RecordEventIngested(source string)         { active.RecordEventIngested(source) }
func RecordEventRejected(source, reason string) { active.RecordEventRejected(source, reason) }
func RecordDLQEnqueued()                        { active.RecordDLQEnqueued() }
func RecordBatcherFlush()                       { active.RecordBatcherFlush() }
func ObserveWorkerPoolDepth(n float64)          { active.ObserveWorkerPoolDepth(n) }
func RecordRateLimited(source string)           { active.RecordRateLimited(source) }
func RecordPanicRecovered()                     { active.RecordPanicRecovered() }

// Handler returns the active provider's HTTP metrics handler.
func Handler() http.Handler { return active.Handler() }

// ── nopProvider ────────────────────────────────────────────────────────────────

// nopProvider silently discards all observations. Used as the default so that
// packages work correctly without explicit initialisation (e.g. in tests).
type nopProvider struct{}

func (nopProvider) RecordEventIngested(string)         {}
func (nopProvider) RecordEventRejected(string, string) {}
func (nopProvider) RecordDLQEnqueued()                 {}
func (nopProvider) RecordBatcherFlush()                {}
func (nopProvider) ObserveWorkerPoolDepth(float64)     {}
func (nopProvider) RecordRateLimited(string)           {}
func (nopProvider) RecordPanicRecovered()              {}
func (nopProvider) Handler() http.Handler              { return http.NotFoundHandler() }
