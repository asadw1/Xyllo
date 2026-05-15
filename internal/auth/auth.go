// Package auth provides authentication middleware for the Xyllo ingestor.
// It supports two modes: static API Key validation and JWT Bearer token
// validation.  Both are implemented as middleware.Handler values so they
// can be composed into the standard middleware chain.
package auth

import (
	"errors"
	"strings"

	"github.com/yourusername/xyllo/config"
	"github.com/yourusername/xyllo/internal/middleware"
)

var (
	ErrMissingCredentials = errors.New("auth: missing credentials")
	ErrInvalidAPIKey      = errors.New("auth: invalid API key")
	ErrInvalidJWT         = errors.New("auth: invalid or expired JWT")
)

// APIKeyValidator returns a middleware.Handler that checks the X-API-Key
// header against the configured static key.
//
// TODO: Support multiple keys (map[source]key) for per-client rotation.
func APIKeyValidator(cfg config.AuthConfig) middleware.Handler {
	return func(r *middleware.Result) error {
		// In a real HTTP handler the key would be extracted from the request
		// header before the payload reaches the middleware chain.  The Result
		// struct will need an APIKey field added once the ingestor is wired up.
		//
		// Placeholder — implement header extraction and constant-time comparison.
		_ = cfg
		return nil
	}
}

// JWTValidator returns a middleware.Handler that validates a Bearer token
// using the configured HMAC secret.
//
// TODO: Parse and verify the JWT signature, expiry (exp), and issuer (iss).
// TODO: Consider RS256 support for asymmetric key validation.
func JWTValidator(cfg config.AuthConfig) middleware.Handler {
	return func(r *middleware.Result) error {
		// Placeholder — implement JWT parsing and claim validation.
		_ = cfg
		return nil
	}
}

// extractBearer strips the "Bearer " prefix from an Authorization header value.
func extractBearer(header string) (string, error) {
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return "", ErrMissingCredentials
	}
	return strings.TrimPrefix(header, prefix), nil
}
