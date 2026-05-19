// Package middleware provides the pluggable validation middleware chain.
// Each Handler inspects a payload and either passes it through or returns an
// error that triggers routing to the DLQ.
package middleware

import (
	"encoding/json"
	"fmt"
)

// Result carries a payload through the chain together with any accumulated
// validation errors.
type Result struct {
	Source string
	Data   []byte
	Errors []string
}

// Handler is a single middleware step in the chain.
type Handler func(r *Result) error

// Chain is an ordered sequence of Handlers executed left-to-right.
type Chain struct {
	handlers []Handler
}

// NewChain creates a Chain from the provided Handlers.
func NewChain(handlers ...Handler) *Chain {
	return &Chain{handlers: handlers}
}

// Run executes each handler in order.  Processing stops on the first error.
func (c *Chain) Run(r *Result) error {
	for _, h := range c.handlers {
		if err := h(r); err != nil {
			return err
		}
	}
	return nil
}

// SchemaValidator returns a Handler that checks the payload contains all
// required top-level fields and that none are empty / zero-valued.
// Required fields: "id", "source", "event_type", "timestamp_ms".
func SchemaValidator() Handler {
	const op = "SchemaValidator"
	required := []string{"id", "source", "event_type", "timestamp_ms"}

	return func(r *Result) error {
		var m map[string]any
		if err := json.Unmarshal(r.Data, &m); err != nil {
			return fmt.Errorf("%s: invalid JSON: %w", op, err)
		}
		for _, key := range required {
			v, ok := m[key]
			if !ok || v == nil {
				return fmt.Errorf("%s: missing required field %q", op, key)
			}
			// Reject empty string fields.
			if s, isStr := v.(string); isStr && s == "" {
				return fmt.Errorf("%s: field %q must not be empty", op, key)
			}
			// Reject zero numeric fields (timestamp_ms == 0 is not a valid time).
			if n, isNum := v.(float64); isNum && n == 0 {
				return fmt.Errorf("%s: field %q must not be zero", op, key)
			}
		}
		return nil
	}
}

// TypeChecker returns a Handler that verifies the Go types of well-known
// fields after SchemaValidator has confirmed they are present.
// Expected types: id/source/event_type → string; timestamp_ms → number (float64).
func TypeChecker() Handler {
	const op = "TypeChecker"

	return func(r *Result) error {
		var m map[string]any
		if err := json.Unmarshal(r.Data, &m); err != nil {
			return fmt.Errorf("%s: invalid JSON: %w", op, err)
		}
		stringFields := []string{"id", "source", "event_type"}
		for _, key := range stringFields {
			if v, ok := m[key]; ok {
				if _, isStr := v.(string); !isStr {
					return fmt.Errorf("%s: field %q must be a string, got %T", op, key, v)
				}
			}
		}
		if v, ok := m["timestamp_ms"]; ok {
			if _, isNum := v.(float64); !isNum {
				return fmt.Errorf("%s: field %q must be a number, got %T", op, "timestamp_ms", v)
			}
		}
		return nil
	}
}
