// Package xlerr defines Xyllo's standard pipeline error type.
//
// All errors that travel between pipeline stages (ingestor → dispatcher →
// middleware → DLQ) should be wrapped in PipelineError so that logs, metrics,
// and DLQ entries carry structured context (stage, code, source) rather than
// opaque strings.
//
// Usage:
//
//	if err := chain.Run(r); err != nil {
//	    pe := xlerr.New(xlerr.StageValidation, xlerr.CodeValidation, source, err)
//	    dlq.Push(source, pe.Error(), data)
//	}
package xlerr

import "fmt"

// Stage identifies which pipeline stage produced the error.
type Stage string

const (
	// StageIngest is the HTTP/gRPC boundary where raw bytes are received.
	StageIngest Stage = "ingest"
	// StageValidation is the middleware chain that checks schema and types.
	StageValidation Stage = "validation"
	// StageDispatch is the worker pool that fans payloads to middleware.
	StageDispatch Stage = "dispatch"
	// StageDLQ is the dead-letter sink itself.
	StageDLQ Stage = "dlq"
)

// Code is a machine-readable error category suitable for use as a metric label
// and DLQ filter key.
type Code string

const (
	// CodeValidation covers schema and type-check failures from middleware.
	CodeValidation Code = "validation_error"
	// CodeTranslation covers ACL/translation failures in the ingestor.
	CodeTranslation Code = "translation_error"
	// CodeBufferFull indicates the dispatcher channel was at capacity.
	CodeBufferFull Code = "buffer_full"
	// CodePanic covers goroutine panics recovered by the dispatcher.
	CodePanic Code = "panic"
	// CodeDLQWrite covers I/O errors when persisting a DLQ entry.
	CodeDLQWrite Code = "dlq_write_error"
	// CodeSerialization covers json.Marshal failures on canonical events.
	CodeSerialization Code = "serialization_error"
)

// PipelineError is the standard error type for Xyllo pipeline failures.
// It wraps an underlying error with structured metadata so that every
// log line and DLQ entry can be parsed, aggregated, and alerted on.
//
// PipelineError is compatible with errors.Is and errors.As via Unwrap.
type PipelineError struct {
	Stage  Stage
	Code   Code
	Source string
	Err    error
}

// Error returns a human-readable description that includes stage, code, and
// source so that plain log lines contain full context without a JSON parser.
func (e *PipelineError) Error() string {
	if e.Source != "" {
		return fmt.Sprintf("[%s/%s] source=%q: %v", e.Stage, e.Code, e.Source, e.Err)
	}
	return fmt.Sprintf("[%s/%s]: %v", e.Stage, e.Code, e.Err)
}

// Unwrap returns the underlying error so that errors.Is and errors.As can
// traverse the chain transparently.
func (e *PipelineError) Unwrap() error { return e.Err }

// New creates a PipelineError from its component parts.
func New(stage Stage, code Code, source string, err error) *PipelineError {
	return &PipelineError{Stage: stage, Code: code, Source: source, Err: err}
}

// Wrap is a nil-safe convenience wrapper around New.
// Returns nil when err is nil.
func Wrap(err error, stage Stage, code Code, source string) error {
	if err == nil {
		return nil
	}
	return New(stage, code, source, err)
}
