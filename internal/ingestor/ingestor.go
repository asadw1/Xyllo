// Package ingestor provides the HTTP listener that forms the network boundary
// of the Xyllo pipeline. It accepts raw telemetry payloads, passes them
// through the Anti-Corruption Layer (translator), and queues canonical Events
// onto the dispatcher's buffered channel.
//
// This package owns the HTTP (Fiber) surface only. gRPC will be added in a
// future phase once proto stubs are generated (see proto/event.proto).
package ingestor

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"github.com/yourusername/xyllo/config"
	"github.com/yourusername/xyllo/internal/auth"
	"github.com/yourusername/xyllo/internal/dispatcher"
	"github.com/yourusername/xyllo/internal/metrics"
	"github.com/yourusername/xyllo/internal/pool"
	"github.com/yourusername/xyllo/internal/translator"
)

// Server wraps the HTTP (Fiber) ingest listener and the Prometheus metrics
// server. Create one with New and start it with Start.
type Server struct {
	port string
	cfg  *config.Config
	disp *dispatcher.Dispatcher
	reg  *translator.Registry
}

// ingestResponse is the JSON body returned on a successful 202 Accepted.
type ingestResponse struct {
	Status string `json:"status"`
	ID     string `json:"id"`
}

// errorResponse is the JSON body returned when the request cannot be fulfilled.
type errorResponse struct {
	Error string `json:"error"`
}

// New constructs a Server with the given port, config, dispatcher, and
// translator registry. It does not start any listeners; call Start to do that.
func New(port string, cfg *config.Config, disp *dispatcher.Dispatcher, reg *translator.Registry) *Server {
	return &Server{port: port, cfg: cfg, disp: disp, reg: reg}
}

// Start launches the Fiber HTTP ingest listener on s.port and a standard
// net/http metrics listener on metricsPort. It blocks until ctx is cancelled,
// then performs a graceful shutdown with a 10-second drain window.
//
// It is safe to call Start exactly once per Server instance.
func (s *Server) Start(ctx context.Context) error {
	app := s.buildApp()

	// Start the Prometheus metrics server on its own port so that network
	// policies can restrict scrape access independently of ingest traffic.
	metricsMux := http.NewServeMux()
	metricsMux.Handle(s.cfg.Observability.MetricsPath, metrics.Handler())
	metricsPort := s.cfg.Observability.MetricsPort
	if metricsPort == "" {
		metricsPort = "9091"
	}
	metricsSrv := &http.Server{
		Addr:         ":" + metricsPort,
		Handler:      metricsMux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	go func() {
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			// Metrics unavailability is non-fatal to the ingest pipeline.
			log.Printf("[ingestor] metrics server error: %v", err)
		}
	}()

	// Run the Fiber listener in a goroutine so we can race it against ctx.
	listenErr := make(chan error, 1)
	go func() {
		if s.cfg.TLS.Enabled {
			listenErr <- app.ListenTLS(":"+s.port, s.cfg.TLS.CertFile, s.cfg.TLS.KeyFile)
		} else {
			listenErr <- app.Listen(":" + s.port)
		}
	}()

	select {
	case <-ctx.Done():
		// Drain the metrics server first (fast), then the ingest server.
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = metricsSrv.Shutdown(shutCtx)
		return app.ShutdownWithTimeout(10 * time.Second)
	case err := <-listenErr:
		return err
	}
}

// BuildApp constructs the Fiber application for use in tests without binding a
// port. It is the exported equivalent of buildApp, allowing integration tests
// in external packages to exercise routes via app.Test.
func (s *Server) BuildApp() *fiber.App {
	return s.buildApp()
}

// buildApp constructs and returns the configured Fiber application with all
// routes registered. It is intentionally decoupled from Start so that tests
// can exercise routes directly via app.Test without binding a real port.
func (s *Server) buildApp() *fiber.App {
	app := fiber.New(fiber.Config{
		// Suppress Fiber's ASCII banner; Xyllo owns its own startup log output.
		DisableStartupMessage: true,
		ReadTimeout:           5 * time.Second,
		WriteTimeout:          10 * time.Second,
		// Return a JSON errorResponse body for unhandled errors instead of
		// Fiber's default HTML error page.
		ErrorHandler: func(c *fiber.Ctx, err error) error {
			code := fiber.StatusInternalServerError
			var fe *fiber.Error
			if errors.As(err, &fe) {
				code = fe.Code
			}
			return c.Status(code).JSON(errorResponse{Error: err.Error()})
		},
	})

	app.Get("/healthz", s.handleHealthz)
	app.Get("/readyz", s.handleReadyz)
	// Auth middleware is applied per-route so that healthz/readyz remain
	// publicly accessible for load-balancer and Kubernetes probes.
	app.Post("/v1/ingest", auth.Middleware(s.cfg.Auth), s.handleIngest)

	return app
}

// handleHealthz responds to Kubernetes liveness probe requests.
// It returns 200 OK as long as the process is alive and the HTTP server is
// responding; no downstream health is checked here.
func (s *Server) handleHealthz(c *fiber.Ctx) error {
	return c.Status(fiber.StatusOK).JSON(fiber.Map{"status": "ok"})
}

// handleReadyz responds to Kubernetes readiness probe requests.
// Returns 200 OK when the server is initialised and ready for traffic.
//
// TODO(ingestor): Expand to inspect dispatcher buffer depth and batcher state
// before advertising readiness so that a saturated pipeline cannot be targeted
// by the load balancer.
func (s *Server) handleReadyz(c *fiber.Ctx) error {
	return c.Status(fiber.StatusOK).JSON(fiber.Map{"status": "ok"})
}

// handleIngest is the primary HTTP handler for POST /v1/ingest.
//
// Request contract:
//   - Header X-Source-ID (optional): identifies the originating client.
//     Defaults to "unknown" when absent.
//   - Body: JSON payload conforming to the registered source schema.
//
// Response codes:
//   - 202 Accepted:            event queued successfully; body contains the assigned ID.
//   - 400 Bad Request:         request body is empty.
//   - 422 Unprocessable Entity: payload failed ACL translation.
//   - 429 Too Many Requests:   dispatcher buffer is at capacity; caller should back off.
func (s *Server) handleIngest(c *fiber.Ctx) error {
	body := c.Body()
	if len(body) == 0 {
		return c.Status(fiber.StatusBadRequest).JSON(errorResponse{Error: "request body is required"})
	}

	source := c.Get("X-Source-ID", "unknown")

	// Obtain a pooled scratch buffer and copy the Fiber-owned body bytes into
	// it. This avoids a heap allocation on the hot ingest path (architecture.md §4.3).
	// The pool buffer must NOT be returned before json.Marshal(event) completes
	// because Passthrough.Translate may set event.Body to a sub-slice of p.Data.
	p := pool.Get()
	p.Data = append(p.Data, body...)

	event, err := s.reg.Translate(source, p.Data)
	if err != nil {
		pool.Put(p) // No event was created; p.Data is no longer referenced.
		metrics.RecordEventRejected(source, "translation_error")
		return c.Status(fiber.StatusUnprocessableEntity).JSON(errorResponse{Error: err.Error()})
	}

	// The ingestor owns canonical ID assignment (architecture.md §3.1).
	// Translators intentionally leave ID empty so ingestion time is authoritative.
	if event.ID == "" {
		event.ID = uuid.NewString()
	}

	// Serialise the canonical Event before releasing p; event.Body may point
	// into p.Data and must be fully copied first.
	data, err := json.Marshal(event)
	pool.Put(p) // event.Body is now captured in data; safe to release.
	if err != nil {
		// A well-formed *translator.Event should never fail json.Marshal.
		// Degrade gracefully rather than panic in a live request goroutine.
		return c.Status(fiber.StatusInternalServerError).JSON(errorResponse{Error: "internal serialisation error"})
	}

	// Non-blocking submit — returns false immediately when the buffer is full.
	// The caller is responsible for retry/back-off (architecture.md §3.3).
	if !s.disp.Submit(dispatcher.Payload{Source: source, Data: data}) {
		metrics.RecordEventRejected(source, "buffer_full")
		return c.Status(fiber.StatusTooManyRequests).JSON(errorResponse{Error: "buffer full, retry later"})
	}

	metrics.RecordEventIngested(source)
	return c.Status(fiber.StatusAccepted).JSON(ingestResponse{
		Status: "accepted",
		ID:     event.ID,
	})
}
