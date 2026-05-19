// Package auth provides authentication middleware for the Xyllo ingestor.
// It supports two modes: static API Key validation and JWT Bearer token
// validation.  Authentication is enforced at the HTTP boundary so that
// unauthorised requests are rejected before they consume pipeline resources.
package auth

import (
	"crypto/subtle"
	"errors"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/golang-jwt/jwt/v5"
	"github.com/yourusername/xyllo/config"
)

var (
	ErrMissingCredentials = errors.New("auth: missing credentials")
	ErrInvalidAPIKey      = errors.New("auth: invalid API key")
	ErrInvalidJWT         = errors.New("auth: invalid or expired JWT")
)

// ValidateAPIKey verifies key against cfg.APIKey using a constant-time
// comparison to prevent timing-based side-channel attacks (CWE-208).
func ValidateAPIKey(cfg config.AuthConfig, key string) error {
	if key == "" {
		return ErrMissingCredentials
	}
	// ConstantTimeCompare returns 1 only when slices are equal in both
	// length and content.  Differing lengths are handled implicitly.
	if subtle.ConstantTimeCompare([]byte(cfg.APIKey), []byte(key)) != 1 {
		return ErrInvalidAPIKey
	}
	return nil
}

// ValidateJWT parses and verifies a signed JWT string against cfg.JWTSecret
// using HMAC-SHA256 (HS256).  It checks:
//   - Signature validity
//   - Token expiry (exp claim) — required
//   - Issuer (iss claim), when cfg.JWTIssuer is non-empty
func ValidateJWT(cfg config.AuthConfig, tokenStr string) error {
	if tokenStr == "" {
		return ErrMissingCredentials
	}

	token, err := jwt.Parse(tokenStr, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			// Reject non-HMAC algorithms to prevent algorithm-confusion attacks.
			return nil, ErrInvalidJWT
		}
		return []byte(cfg.JWTSecret), nil
	}, jwt.WithExpirationRequired(), jwt.WithIssuedAt())

	if err != nil || !token.Valid {
		return ErrInvalidJWT
	}

	if cfg.JWTIssuer != "" {
		claims, ok := token.Claims.(jwt.MapClaims)
		if !ok {
			return ErrInvalidJWT
		}
		iss, _ := claims["iss"].(string)
		if iss != cfg.JWTIssuer {
			return ErrInvalidJWT
		}
	}
	return nil
}

// Middleware returns a Fiber handler that enforces the configured auth mode.
// Register it on each protected route; healthz/readyz are intentionally excluded.
//
//   - "none"   — pass-through, no credentials required.
//   - "apikey" — X-API-Key header must match cfg.APIKey.
//   - "jwt"    — Authorization: Bearer <token> must carry a valid HS256 JWT.
//
// Unknown modes fail closed (401) rather than open.
func Middleware(cfg config.AuthConfig) fiber.Handler {
	return func(c *fiber.Ctx) error {
		switch cfg.Mode {
		case "none":
			return c.Next()
		case "apikey":
			if err := ValidateAPIKey(cfg, c.Get("X-API-Key")); err != nil {
				return fiber.NewError(fiber.StatusUnauthorized, err.Error())
			}
			return c.Next()
		case "jwt":
			raw, err := extractBearer(c.Get("Authorization"))
			if err != nil {
				return fiber.NewError(fiber.StatusUnauthorized, err.Error())
			}
			if err := ValidateJWT(cfg, raw); err != nil {
				return fiber.NewError(fiber.StatusUnauthorized, err.Error())
			}
			return c.Next()
		default:
			// Fail closed: unknown mode denies access by default.
			return fiber.NewError(fiber.StatusUnauthorized, "auth: unknown mode")
		}
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
