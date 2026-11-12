package ledger_test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"testing"
	"time"

	"github.com/Kaaushikk/real-time-payments-ledger/internal/authn"
)

func TestAuthorizationCodeBindingAndSingleUse(t *testing.T) {
	_, demo, pool := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	cfg := grpcTestConfig
	cfg.AccessTTL = 5 * time.Minute
	cfg.RefreshTTL = time.Hour
	service := authn.New(pool, cfg)
	const client = "ledger-cli"
	const redirect = "http://localhost:8080/callback"
	const otherRedirect = "http://127.0.0.1:8080/callback"
	if _, err := pool.Exec(ctx, "INSERT INTO oauth_clients (id,name,redirect_uris) VALUES ('other-cli','Other test client',ARRAY[$1::text])", redirect); err != nil {
		t.Fatal(err)
	}
	verifier := authn.RandomString(32)
	digest := sha256.Sum256([]byte(verifier))
	challenge := base64.RawURLEncoding.EncodeToString(digest[:])
	for _, tc := range []struct{ uri, challenge, method string }{
		{"", challenge, "S256"}, {redirect, "bad", "S256"}, {redirect, challenge, "plain"},
	} {
		if _, err := service.CreateAuthCode(ctx, demo.AliceUser, client, tc.uri, tc.challenge, tc.method); !errors.Is(err, authn.ErrInvalidRequest) {
			t.Fatalf("invalid issuance request: %v", err)
		}
	}
	code, err := service.CreateAuthCode(ctx, demo.AliceUser, client, redirect, challenge, "S256")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ client, uri, verifier string }{
		{"other-cli", redirect, verifier},
		{client, otherRedirect, verifier},
		{client, "", verifier},
		{client, redirect, authn.RandomString(32)},
		{client, redirect, "short"},
	} {
		if _, err := service.ExchangeCode(ctx, tc.client, tc.uri, code, tc.verifier); !errors.Is(err, authn.ErrInvalidGrant) {
			t.Fatalf("invalid exchange request: %v", err)
		}
	}

	const callers = 8
	type result struct {
		pair authn.TokenPair
		err  error
	}
	start := make(chan struct{})
	results := make(chan result, callers)
	for i := 0; i < callers; i++ {
		go func() {
			<-start
			pair, err := service.ExchangeCode(ctx, client, redirect, code, verifier)
			results <- result{pair, err}
		}()
	}
	close(start)
	successes := 0
	for i := 0; i < callers; i++ {
		got := <-results
		switch {
		case got.err == nil:
			successes++
			claims, err := service.ParseAccess(got.pair.AccessToken)
			if err != nil || claims.Subject != demo.AliceUser.String() || claims.ClientID != client || got.pair.RefreshToken == "" {
				t.Error("successful exchange did not produce tokens bound to the expected user and client")
			}
		case !errors.Is(got.err, authn.ErrInvalidGrant):
			t.Errorf("concurrent exchange: %v", got.err)
		}
	}
	var issued int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM refresh_tokens").Scan(&issued); err != nil {
		t.Fatal(err)
	}
	if successes != 1 || issued != 1 {
		t.Fatalf("successful exchanges=%d, refresh records=%d; want one each", successes, issued)
	}
}

func TestAuthorizationCodeSurvivesFailedTokenInsert(t *testing.T) {
	_, demo, pool := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cfg := grpcTestConfig
	cfg.AccessTTL = 5 * time.Minute
	cfg.RefreshTTL = time.Hour
	service := authn.New(pool, cfg)
	const client = "ledger-cli"
	const redirect = "http://localhost:8080/callback"
	verifier := authn.RandomString(32)
	digest := sha256.Sum256([]byte(verifier))
	code, err := service.CreateAuthCode(ctx, demo.AliceUser, client, redirect,
		base64.RawURLEncoding.EncodeToString(digest[:]), "S256")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "ALTER TABLE refresh_tokens ADD CONSTRAINT test_reject_issue CHECK (false)"); err != nil {
		t.Fatal(err)
	}
	pair, err := service.ExchangeCode(ctx, client, redirect, code, verifier)
	if err == nil || errors.Is(err, authn.ErrInvalidGrant) || pair != (authn.TokenPair{}) {
		t.Fatal("expected a storage failure without returned tokens")
	}
	var consumed, issued int
	if err := pool.QueryRow(ctx, "SELECT (SELECT count(*) FROM oauth_auth_codes WHERE used_at IS NOT NULL), (SELECT count(*) FROM refresh_tokens)").Scan(&consumed, &issued); err != nil {
		t.Fatal(err)
	}
	if consumed != 0 || issued != 0 {
		t.Fatal("failed token insertion left a partial exchange")
	}
	if _, err := pool.Exec(ctx, "ALTER TABLE refresh_tokens DROP CONSTRAINT test_reject_issue"); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ExchangeCode(ctx, client, redirect, code, verifier); err != nil {
		t.Fatalf("retry after rollback: %v", err)
	}

	expired, err := service.CreateAuthCode(ctx, demo.AliceUser, client, redirect,
		base64.RawURLEncoding.EncodeToString(digest[:]), "S256")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "UPDATE oauth_auth_codes SET expires_at = now() - interval '1 second' WHERE used_at IS NULL"); err != nil {
		t.Fatal(err)
	}
	if _, err := service.ExchangeCode(ctx, client, redirect, expired, verifier); !errors.Is(err, authn.ErrInvalidGrant) {
		t.Fatalf("expired code: %v", err)
	}
}
