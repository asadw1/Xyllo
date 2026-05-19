//go:build ignore

// gen_jwt.go prints a signed HS256 JWT for manual testing.
// Run with: go run ./scripts/gen_jwt.go
//
// The token is signed with the same secret and issuer used in Section 4
// of tests/manual.http.  Copy the output into the @validJWT variable there.
package main

import (
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func main() {
	const (
		secret = "manual-test-secret"
		issuer = "xyllo"
		ttl    = 24 * time.Hour
	)

	claims := jwt.MapClaims{
		"sub": "test-client",
		"iss": issuer,
		"iat": time.Now().Unix(),
		"exp": time.Now().Add(ttl).Unix(),
	}

	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := tok.SignedString([]byte(secret))
	if err != nil {
		panic(err)
	}

	fmt.Println(signed)
}
