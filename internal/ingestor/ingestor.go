// Package ingestor provides HTTP and gRPC handlers that accept raw telemetry
// payloads and push them onto the internal buffered channel for processing.
package ingestor

import (
	"context"

	"github.com/yourusername/xyllo/config"
	"github.com/yourusername/xyllo/internal/dispatcher"
	"github.com/yourusername/xyllo/internal/translator"
)

// Server wraps the HTTP (Fiber) and optional gRPC listeners.
type Server struct {
	port string
	cfg  *config.Config
	disp *dispatcher.Dispatcher
	reg  *translator.Registry
}

// New constructs a Server with the given port, config, dispatcher, and
// translator registry.
func New(port string, cfg *config.Config, disp *dispatcher.Dispatcher, reg *translator.Registry) *Server {
	return &Server{port: port, cfg: cfg, disp: disp, reg: reg}
}

// Start launches the HTTP (and optionally gRPC) listener and blocks until ctx
// is cancelled, then performs a graceful shutdown.
//
// TODO: Wire up Fiber router with /ingest and /health endpoints.
// TODO: Add gRPC server alongside HTTP when proto stubs are generated.
func (s *Server) Start(ctx context.Context) error {
	// Placeholder — replace with real Fiber app setup.
	<-ctx.Done()
	return nil
}
