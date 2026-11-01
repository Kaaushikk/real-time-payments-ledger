package ledger_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Kaaushikk/real-time-payments-ledger/internal/authn"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/config"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/httpserver"
	"github.com/golang-jwt/jwt/v5"
)

func TestHTTPAccountOwnership(t *testing.T) {
	svc, d, pool := fixture(t)
	cfg := config.Config{JWTSigningKey: "unit-test-only-signing-key-not-a-secret", JWTIssuer: "test", JWTAudience: "ledger-api"}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, authn.Claims{RegisteredClaims: jwt.RegisteredClaims{
		Subject: d.AliceUser.String(), Issuer: cfg.JWTIssuer, Audience: jwt.ClaimStrings{cfg.JWTAudience}, ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute)),
	}}).SignedString([]byte(cfg.JWTSigningKey))
	if err != nil {
		t.Fatal(err)
	}
	h := (httpserver.LedgerAPI{Ledger: svc, Auth: authn.New(pool, cfg)}).Handler()
	tests := []struct {
		method, path, body string
		want               int
	}{
		{"GET", "/v1/accounts/" + d.AliceAccount.String(), "", 200},
		{"GET", "/v1/accounts/" + d.BobAccount.String(), "", 404},
		{"GET", "/v1/accounts/" + d.BobAccount.String() + "/audit", "", 404},
		{"POST", "/v1/accounts", `{"owner_user_id":"` + d.BobUser.String() + `","currency":"USD"}`, 403},
		{"POST", "/v1/accounts", `{"currency":"EUR"}`, 400},
		{"POST", "/v1/transfers", `{"from_account_id":"` + d.BobAccount.String() + `","to_account_id":"` + d.AliceAccount.String() + `","amount_minor":1,"currency":"USD"}`, 403},
		{"POST", "/v1/transfers", `{"unknown":true}`, 400},
		{"POST", "/v1/transfers", `{"from_account_id":"` + d.AliceAccount.String() + `","to_account_id":"` + d.BobAccount.String() + `","amount_minor":1.5}`, 400},
		{"POST", "/v1/transfers", `{"from_account_id":"` + d.AliceAccount.String() + `","to_account_id":"` + d.BobAccount.String() + `","amount_minor":9223372036854775808}`, 400},
		{"GET", "/readyz", "", 200},
	}
	for _, tc := range tests {
		t.Run(tc.method+tc.path, func(t *testing.T) {
			req := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			req.Header.Set("Authorization", "Bearer "+token)
			req.Header.Set("Idempotency-Key", "http-test")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)
			if w.Code != tc.want {
				t.Fatalf("got %d want %d body=%s", w.Code, tc.want, w.Body.String())
			}
		})
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/v1/accounts/"+d.AliceAccount.String(), nil))
	if w.Code != 401 {
		t.Fatalf("missing token: %d", w.Code)
	}
}
