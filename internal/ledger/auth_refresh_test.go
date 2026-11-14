package ledger_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Kaaushikk/real-time-payments-ledger/internal/authn"
)

func TestRefreshRotationAndSessionIsolation(t *testing.T) {
	_, demo, pool := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	cfg := grpcTestConfig
	cfg.AccessTTL = 5 * time.Minute
	cfg.RefreshTTL = time.Hour
	service := authn.New(pool, cfg)
	const client = "ledger-cli"

	first, err := service.DevIssue(ctx, demo.AliceUser, client)
	if err != nil {
		t.Fatal(err)
	}
	separate, err := service.DevIssue(ctx, demo.AliceUser, client)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Refresh(ctx, "other-client", first.RefreshToken); !errors.Is(err, authn.ErrInvalidGrant) {
		t.Fatalf("wrong client: %v", err)
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
			pair, err := service.Refresh(ctx, client, first.RefreshToken)
			results <- result{pair, err}
		}()
	}
	close(start)
	var rotated authn.TokenPair
	var successes, reuses int
	for i := 0; i < callers; i++ {
		got := <-results
		switch {
		case got.err == nil:
			rotated = got.pair
			successes++
		case errors.Is(got.err, authn.ErrTokenReuse):
			if got.pair != (authn.TokenPair{}) {
				t.Error("reuse returned credentials")
			}
			reuses++
		default:
			t.Errorf("concurrent refresh: %v", got.err)
		}
	}
	if successes != 1 || reuses != callers-1 {
		t.Fatalf("successful rotations=%d, reuse rejections=%d", successes, reuses)
	}
	if _, err := service.Refresh(ctx, client, rotated.RefreshToken); !errors.Is(err, authn.ErrTokenReuse) {
		t.Fatalf("replacement should be revoked: %v", err)
	}
	child, err := service.Refresh(ctx, client, separate.RefreshToken)
	if err != nil {
		t.Fatalf("independent session should still rotate: %v", err)
	}
	grandchild, err := service.Refresh(ctx, client, child.RefreshToken)
	if err != nil {
		t.Fatalf("replacement should rotate before reuse: %v", err)
	}
	if _, err := service.Refresh(ctx, client, separate.RefreshToken); !errors.Is(err, authn.ErrTokenReuse) {
		t.Fatalf("expected reuse on session root: %v", err)
	}
	if _, err := service.Refresh(ctx, client, grandchild.RefreshToken); !errors.Is(err, authn.ErrTokenReuse) {
		t.Fatalf("root reuse should revoke the latest refresh token: %v", err)
	}
}

func TestRefreshSurvivesFailedReplacementInsert(t *testing.T) {
	_, demo, pool := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cfg := grpcTestConfig
	cfg.AccessTTL = 5 * time.Minute
	cfg.RefreshTTL = time.Hour
	service := authn.New(pool, cfg)
	const client = "ledger-cli"

	first, err := service.DevIssue(ctx, demo.AliceUser, client)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, "ALTER TABLE refresh_tokens ADD CONSTRAINT test_reject_child CHECK (parent_id IS NULL)"); err != nil {
		t.Fatal(err)
	}
	pair, err := service.Refresh(ctx, client, first.RefreshToken)
	if err == nil || errors.Is(err, authn.ErrInvalidGrant) || errors.Is(err, authn.ErrTokenReuse) || pair != (authn.TokenPair{}) {
		t.Fatal("expected a storage failure without returned tokens")
	}
	if _, err := pool.Exec(ctx, "ALTER TABLE refresh_tokens DROP CONSTRAINT test_reject_child"); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Refresh(ctx, client, first.RefreshToken); err != nil {
		t.Fatalf("original token should survive rollback: %v", err)
	}
	if _, err := pool.Exec(ctx, "UPDATE refresh_tokens SET expires_at = now() - interval '1 second'"); err != nil {
		t.Fatal(err)
	}
	if _, err := service.Refresh(ctx, client, first.RefreshToken); !errors.Is(err, authn.ErrInvalidGrant) {
		t.Fatalf("expired refresh token: %v", err)
	}
}
