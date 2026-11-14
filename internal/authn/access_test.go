package authn

import (
	"testing"
	"time"

	"github.com/Kaaushikk/real-time-payments-ledger/internal/config"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
)

func TestParseAccess(t *testing.T) {
	cfg := config.Config{
		JWTSigningKey: "test-access-signing-key",
		JWTIssuer:     "test-issuer",
		JWTAudience:   "test-audience",
	}
	service := New(nil, cfg)
	now := time.Now()
	claims := jwt.RegisteredClaims{
		Issuer:    cfg.JWTIssuer,
		Subject:   uuid.NewString(),
		Audience:  jwt.ClaimStrings{cfg.JWTAudience},
		ExpiresAt: jwt.NewNumericDate(now.Add(time.Minute)),
	}
	for _, tc := range []struct {
		name   string
		edit   func(*jwt.RegisteredClaims)
		method jwt.SigningMethod
		key    string
		valid  bool
	}{
		{name: "valid", valid: true},
		{name: "expired", edit: func(c *jwt.RegisteredClaims) { c.ExpiresAt = jwt.NewNumericDate(now.Add(-time.Minute)) }},
		{name: "missing expiration", edit: func(c *jwt.RegisteredClaims) { c.ExpiresAt = nil }},
		{name: "wrong issuer", edit: func(c *jwt.RegisteredClaims) { c.Issuer = "other" }},
		{name: "wrong audience", edit: func(c *jwt.RegisteredClaims) { c.Audience = jwt.ClaimStrings{"other"} }},
		{name: "wrong signature", key: "other-test-signing-key"},
		{name: "unsupported algorithm", method: jwt.SigningMethodHS512},
		{name: "missing subject", edit: func(c *jwt.RegisteredClaims) { c.Subject = "" }},
		{name: "invalid subject", edit: func(c *jwt.RegisteredClaims) { c.Subject = "invalid" }},
		{name: "zero subject", edit: func(c *jwt.RegisteredClaims) { c.Subject = uuid.Nil.String() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testClaims := claims
			if tc.edit != nil {
				tc.edit(&testClaims)
			}
			method := tc.method
			if method == nil {
				method = jwt.SigningMethodHS256
			}
			key := tc.key
			if key == "" {
				key = cfg.JWTSigningKey
			}
			signed, err := jwt.NewWithClaims(method, testClaims).SignedString([]byte(key))
			if err != nil {
				t.Fatal(err)
			}
			got, err := service.ParseAccess(signed)
			if tc.valid {
				if err != nil || got.Subject != claims.Subject {
					t.Fatalf("valid access token rejected: %v", err)
				}
			} else if err == nil || got != nil {
				t.Fatal("invalid access token accepted")
			}
		})
	}
}
