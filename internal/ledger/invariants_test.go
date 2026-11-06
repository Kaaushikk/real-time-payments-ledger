package ledger_test

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"testing/quick"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Kaaushikk/real-time-payments-ledger/internal/db"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/idempotency"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/ledger"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/seed"
	"github.com/Kaaushikk/real-time-payments-ledger/migrations"
)

func integration(t *testing.T) (*ledger.Service, seed.Demo) {
	svc, demo, _ := fixture(t)
	return svc, demo
}

func unmigratedPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set")
	}
	ctx := context.Background()
	admin, err := db.Connect(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	schema := "test_" + uuid.New().String()
	schema = strings.ReplaceAll(schema, "-", "")
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`); admin.Close() })
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema + ",public"
	cfg.MaxConns = 16
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)

	return pool
}

func fixture(t *testing.T) (*ledger.Service, seed.Demo, *pgxpool.Pool) {
	t.Helper()
	pool := unmigratedPool(t)
	ctx := context.Background()
	if err := migrations.Apply(ctx, pool); err != nil {
		t.Fatal(err)
	}
	demo, err := seed.DemoData(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	keys := idempotency.New(nil, pool, 24*time.Hour)
	return ledger.New(pool, keys, "ledger.transactions"), demo, pool
}

func TestConcurrentTransfersNeverNegative(t *testing.T) {
	svc, demo := integration(t)
	ctx := context.Background()
	const n = 200
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			from, to := demo.AliceAccount, demo.BobAccount
			actor := demo.AliceUser
			if i%2 == 1 {
				from, to = to, from
				actor = demo.BobUser
			}
			_, err := svc.Transfer(ctx, ledger.TransferCmd{
				IdempotencyKey: fmt.Sprintf("conc-%d-%s", i, uuid.NewString()),
				FromAccountID:  from,
				ActorUserID:    actor,
				ToAccountID:    to,
				AmountMinor:    1,
				Currency:       "USD",
			})
			errCh <- err
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil && err != ledger.ErrInsufficientFunds {
			t.Fatal(err)
		}
	}
	min, err := svc.MinUserBalance(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if min < 0 {
		t.Fatalf("negative user balance %d", min)
	}
	sum, err := svc.SumBalances(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if sum != 0 {
		t.Fatalf("conservation violated: sum=%d want 0", sum)
	}
}

func TestIdempotentReplay(t *testing.T) {
	svc, demo := integration(t)
	ctx := context.Background()
	cmd := ledger.TransferCmd{
		IdempotencyKey: "replay-" + uuid.NewString(),
		FromAccountID:  demo.AliceAccount,
		ActorUserID:    demo.AliceUser,
		ToAccountID:    demo.BobAccount,
		AmountMinor:    7,
		Currency:       "USD",
	}
	a, err := svc.Transfer(ctx, cmd)
	if err != nil {
		t.Fatal(err)
	}
	b, err := svc.Transfer(ctx, cmd)
	if err != nil {
		t.Fatal(err)
	}
	if !b.Replayed || a.Tx.ID != b.Tx.ID {
		t.Fatalf("expected replay of %s, got replayed=%v id=%s", a.Tx.ID, b.Replayed, b.Tx.ID)
	}
	cmd.AmountMinor = 8
	if _, err := svc.Transfer(ctx, cmd); err != ledger.ErrIdempotencyConflict {
		t.Fatalf("expected conflict, got %v", err)
	}
}

func TestPropertyRandomTransfersConserveSum(t *testing.T) {
	svc, demo := integration(t)
	ctx := context.Background()
	before, err := svc.SumBalances(ctx)
	if err != nil {
		t.Fatal(err)
	}
	fn := func(x uint16) bool {
		from, to := demo.AliceAccount, demo.BobAccount
		actor := demo.AliceUser
		if x%2 == 1 {
			from, to = to, from
			actor = demo.BobUser
		}
		amt := int64(x%50) + 1
		_, err := svc.Transfer(ctx, ledger.TransferCmd{
			IdempotencyKey: uuid.NewString(),
			FromAccountID:  from,
			ActorUserID:    actor,
			ToAccountID:    to,
			AmountMinor:    amt,
			Currency:       "USD",
		})
		if err != nil && err != ledger.ErrInsufficientFunds {
			t.Log(err)
			return false
		}
		sum, err := svc.SumBalances(ctx)
		if err != nil {
			return false
		}
		min, err := svc.MinUserBalance(ctx)
		if err != nil {
			return false
		}
		return sum == before && min >= 0
	}
	if err := quick.Check(fn, &quick.Config{MaxCount: 40}); err != nil {
		t.Fatal(err)
	}
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
