package auth

import (
	"io"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/golang-jwt/jwt/v5"
	"github.com/yourusername/xyllo/config"
)

// ── helpers ───────────────────────────────────────────────────────────────────

func makeAuthCfg(mode, apiKey, jwtSecret, jwtIssuer string) config.AuthConfig {
	return config.AuthConfig{
		Mode:      mode,
		APIKey:    apiKey,
		JWTSecret: jwtSecret,
		JWTIssuer: jwtIssuer,
	}
}

// signHS256 creates a signed HS256 JWT for tests.
func signHS256(t *testing.T, secret string, claims jwt.MapClaims) string {
	t.Helper()
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	s, err := tok.SignedString([]byte(secret))
	if err != nil {
		t.Fatalf("signHS256: %v", err)
	}
	return s
}

func validClaims() jwt.MapClaims {
	return jwt.MapClaims{
		"sub": "test-client",
		"iat": time.Now().Unix(),
		"exp": time.Now().Add(time.Hour).Unix(),
	}
}

// ── ValidateAPIKey ─────────────────────────────────────────────────────────────

func TestValidateAPIKey_ValidKey(t *testing.T) {
	cfg := makeAuthCfg("apikey", "secret-key", "", "")
	if err := ValidateAPIKey(cfg, "secret-key"); err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestValidateAPIKey_WrongKey(t *testing.T) {
	cfg := makeAuthCfg("apikey", "secret-key", "", "")
	if err := ValidateAPIKey(cfg, "wrong-key"); err != ErrInvalidAPIKey {
		t.Errorf("want ErrInvalidAPIKey, got %v", err)
	}
}

func TestValidateAPIKey_EmptyProvidedKey(t *testing.T) {
	cfg := makeAuthCfg("apikey", "secret-key", "", "")
	if err := ValidateAPIKey(cfg, ""); err != ErrMissingCredentials {
		t.Errorf("want ErrMissingCredentials, got %v", err)
	}
}

func TestValidateAPIKey_ConstantTimeNotShortCircuit(t *testing.T) {
	// Provide a key that shares a prefix with the correct key to verify
	// constant-time compare doesn't accept partial matches.
	cfg := makeAuthCfg("apikey", "abcdefgh", "", "")
	if err := ValidateAPIKey(cfg, "abcd"); err != ErrInvalidAPIKey {
		t.Errorf("want ErrInvalidAPIKey for prefix match, got %v", err)
	}
}

// ── ValidateJWT ───────────────────────────────────────────────────────────────

func TestValidateJWT_ValidToken(t *testing.T) {
	const secret = "test-secret"
	cfg := makeAuthCfg("jwt", "", secret, "")
	tok := signHS256(t, secret, validClaims())
	if err := ValidateJWT(cfg, tok); err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestValidateJWT_ExpiredToken(t *testing.T) {
	const secret = "test-secret"
	cfg := makeAuthCfg("jwt", "", secret, "")
	claims := jwt.MapClaims{
		"sub": "test-client",
		"iat": time.Now().Add(-2 * time.Hour).Unix(),
		"exp": time.Now().Add(-1 * time.Hour).Unix(), // expired
	}
	tok := signHS256(t, secret, claims)
	if err := ValidateJWT(cfg, tok); err != ErrInvalidJWT {
		t.Errorf("want ErrInvalidJWT for expired token, got %v", err)
	}
}

func TestValidateJWT_WrongSecret(t *testing.T) {
	cfg := makeAuthCfg("jwt", "", "correct-secret", "")
	tok := signHS256(t, "wrong-secret", validClaims())
	if err := ValidateJWT(cfg, tok); err != ErrInvalidJWT {
		t.Errorf("want ErrInvalidJWT for wrong secret, got %v", err)
	}
}

func TestValidateJWT_EmptyToken(t *testing.T) {
	cfg := makeAuthCfg("jwt", "", "secret", "")
	if err := ValidateJWT(cfg, ""); err != ErrMissingCredentials {
		t.Errorf("want ErrMissingCredentials, got %v", err)
	}
}

func TestValidateJWT_MalformedToken(t *testing.T) {
	cfg := makeAuthCfg("jwt", "", "secret", "")
	if err := ValidateJWT(cfg, "not.a.jwt"); err != ErrInvalidJWT {
		t.Errorf("want ErrInvalidJWT for malformed token, got %v", err)
	}
}

func TestValidateJWT_MissingExpClaim(t *testing.T) {
	const secret = "test-secret"
	cfg := makeAuthCfg("jwt", "", secret, "")
	claims := jwt.MapClaims{
		"sub": "test-client",
		"iat": time.Now().Unix(),
		// no "exp"
	}
	tok := signHS256(t, secret, claims)
	if err := ValidateJWT(cfg, tok); err != ErrInvalidJWT {
		t.Errorf("want ErrInvalidJWT for missing exp, got %v", err)
	}
}

func TestValidateJWT_CorrectIssuer(t *testing.T) {
	const secret = "test-secret"
	cfg := makeAuthCfg("jwt", "", secret, "xyllo")
	claims := validClaims()
	claims["iss"] = "xyllo"
	tok := signHS256(t, secret, claims)
	if err := ValidateJWT(cfg, tok); err != nil {
		t.Errorf("expected nil for correct issuer, got %v", err)
	}
}

func TestValidateJWT_WrongIssuer(t *testing.T) {
	const secret = "test-secret"
	cfg := makeAuthCfg("jwt", "", secret, "xyllo")
	claims := validClaims()
	claims["iss"] = "other-service"
	tok := signHS256(t, secret, claims)
	if err := ValidateJWT(cfg, tok); err != ErrInvalidJWT {
		t.Errorf("want ErrInvalidJWT for wrong issuer, got %v", err)
	}
}

// ── Middleware (Fiber integration) ────────────────────────────────────────────

// newTestApp builds a minimal Fiber app with the auth middleware protecting /test.
func newTestApp(cfg config.AuthConfig) *fiber.App {
	app := fiber.New(fiber.Config{
		DisableStartupMessage: true,
		ErrorHandler: func(c *fiber.Ctx, err error) error {
			code := fiber.StatusInternalServerError
			if fe, ok := err.(*fiber.Error); ok {
				code = fe.Code
			}
			return c.Status(code).SendString(err.Error())
		},
	})
	app.Post("/test", Middleware(cfg), func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusOK)
	})
	return app
}

func doRequest(t *testing.T, app *fiber.App, headers map[string]string) int {
	t.Helper()
	req := httptest.NewRequest("POST", "/test", nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := app.Test(req)
	if err != nil {
		t.Fatalf("app.Test: %v", err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func TestMiddleware_ModeNone_AlwaysPasses(t *testing.T) {
	app := newTestApp(makeAuthCfg("none", "", "", ""))
	if got := doRequest(t, app, nil); got != fiber.StatusOK {
		t.Errorf("mode=none: want 200, got %d", got)
	}
}

func TestMiddleware_APIKey_ValidKey(t *testing.T) {
	app := newTestApp(makeAuthCfg("apikey", "my-key", "", ""))
	if got := doRequest(t, app, map[string]string{"X-API-Key": "my-key"}); got != fiber.StatusOK {
		t.Errorf("apikey valid: want 200, got %d", got)
	}
}

func TestMiddleware_APIKey_MissingHeader(t *testing.T) {
	app := newTestApp(makeAuthCfg("apikey", "my-key", "", ""))
	if got := doRequest(t, app, nil); got != fiber.StatusUnauthorized {
		t.Errorf("apikey missing: want 401, got %d", got)
	}
}

func TestMiddleware_APIKey_WrongKey(t *testing.T) {
	app := newTestApp(makeAuthCfg("apikey", "my-key", "", ""))
	if got := doRequest(t, app, map[string]string{"X-API-Key": "bad-key"}); got != fiber.StatusUnauthorized {
		t.Errorf("apikey wrong: want 401, got %d", got)
	}
}

func TestMiddleware_JWT_ValidToken(t *testing.T) {
	const secret = "jwt-secret"
	app := newTestApp(makeAuthCfg("jwt", "", secret, ""))
	tok := signHS256(t, secret, validClaims())
	if got := doRequest(t, app, map[string]string{"Authorization": "Bearer " + tok}); got != fiber.StatusOK {
		t.Errorf("jwt valid: want 200, got %d", got)
	}
}

func TestMiddleware_JWT_MissingHeader(t *testing.T) {
	app := newTestApp(makeAuthCfg("jwt", "", "secret", ""))
	if got := doRequest(t, app, nil); got != fiber.StatusUnauthorized {
		t.Errorf("jwt missing: want 401, got %d", got)
	}
}

func TestMiddleware_JWT_InvalidToken(t *testing.T) {
	app := newTestApp(makeAuthCfg("jwt", "", "secret", ""))
	if got := doRequest(t, app, map[string]string{"Authorization": "Bearer garbage"}); got != fiber.StatusUnauthorized {
		t.Errorf("jwt invalid: want 401, got %d", got)
	}
}

func TestMiddleware_JWT_NoBearerPrefix(t *testing.T) {
	const secret = "jwt-secret"
	app := newTestApp(makeAuthCfg("jwt", "", secret, ""))
	tok := signHS256(t, secret, validClaims())
	// Omit "Bearer " prefix — should fail with missing credentials path.
	if got := doRequest(t, app, map[string]string{"Authorization": tok}); got != fiber.StatusUnauthorized {
		t.Errorf("jwt no prefix: want 401, got %d", got)
	}
}

func TestMiddleware_UnknownMode_DeniesAccess(t *testing.T) {
	app := newTestApp(makeAuthCfg("oauth2", "", "", ""))
	if got := doRequest(t, app, nil); got != fiber.StatusUnauthorized {
		t.Errorf("unknown mode: want 401, got %d", got)
	}
}
