// Package middleware provides the pluggable validation middleware chain.
// Each Handler inspects a payload and either passes it through or returns an
// error that triggers routing to the DLQ.
package middleware

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

// SchemaValidator returns a Handler that checks the payload against the
// registered JSON schema for the given event type.
//
// TODO: Integrate go-playground/validator or a JSON schema library.
func SchemaValidator() Handler {
	return func(r *Result) error {
		// Placeholder — implement schema validation logic.
		return nil
	}
}

// TypeChecker returns a Handler that verifies field types after the schema
// check has confirmed the top-level structure.
//
// TODO: Implement per-field type assertions.
func TypeChecker() Handler {
	return func(r *Result) error {
		// Placeholder — implement type checking logic.
		return nil
	}
}
