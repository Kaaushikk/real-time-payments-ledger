package ledger_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Kaaushikk/real-time-payments-ledger/internal/authn"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/config"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/httpserver"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/idempotency"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/ledger"
)

func TestReadinessReflectsDatabaseWithoutFailingLiveness(t *testing.T) {
	_, _, fixturePool := fixture(t)
	pool, err := pgxpool.NewWithConfig(context.Background(), fixturePool.Config())
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	auth := authn.New(pool, config.Load("auth"))
	handlers := map[string]http.Handler{
		"auth":   (httpserver.AuthAPI{Auth: auth}).Handler(),
		"ledger": (httpserver.LedgerAPI{Auth: auth, Ledger: ledger.New(pool, idempotency.New(nil, pool, 0), "test")}).Handler(),
	}
	check := func(path string, want int) {
		t.Helper()
		for name, handler := range handlers {
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
			if w.Code != want {
				t.Fatalf("%s %s status=%d body=%s, want %d", name, path, w.Code, w.Body, want)
			}
		}
	}
	check("/healthz", http.StatusOK)
	check("/readyz", http.StatusOK)
	pool.Close()
	check("/readyz", http.StatusServiceUnavailable)
	check("/healthz", http.StatusOK)
}
