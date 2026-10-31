package ledger_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/Kaaushikk/real-time-payments-ledger/internal/ledger"
	"github.com/google/uuid"
)

func TestConcurrentSameKeyPostsOnce(t *testing.T) {
	svc, d := integration(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	before, err := svc.GetAccount(ctx, d.AliceAccount)
	if err != nil {
		t.Fatal(err)
	}
	cmd := ledger.TransferCmd{IdempotencyKey: uuid.NewString(), ActorUserID: d.AliceUser,
		FromAccountID: d.AliceAccount, ToAccountID: d.BobAccount, AmountMinor: 37, Currency: "USD"}
	const n = 32
	start := make(chan struct{})
	results := make(chan ledger.TransferResult, n)
	errs := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			res, err := svc.Transfer(ctx, cmd)
			if err != nil {
				errs <- err
				return
			}
			results <- res
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	var id uuid.UUID
	fresh, count := 0, 0
	for res := range results {
		count++
		if !res.Replayed {
			fresh++
		}
		if id == uuid.Nil {
			id = res.Tx.ID
		}
		if id != res.Tx.ID {
			t.Fatal("duplicate posting")
		}
	}
	after, err := svc.GetAccount(ctx, d.AliceAccount)
	if err != nil {
		t.Fatal(err)
	}
	if count != n || fresh != 1 || after.Balance != before.Balance-37 {
		t.Fatalf("count=%d fresh=%d balance=%d", count, fresh, after.Balance)
	}
}

func TestOversubscribedTransfersCannotOverdraw(t *testing.T) {
	svc, d := integration(t)
	ctx := context.Background()
	initial, err := svc.GetAccount(ctx, d.AliceAccount)
	if err != nil {
		t.Fatal(err)
	}
	const n = 24
	errs := make(chan error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := svc.Transfer(ctx, ledger.TransferCmd{IdempotencyKey: fmt.Sprintf("spend-%d", i), ActorUserID: d.AliceUser,
				FromAccountID: d.AliceAccount, ToAccountID: d.BobAccount, AmountMinor: initial.Balance, Currency: "USD"})
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	posted := 0
	for err := range errs {
		if err == nil {
			posted++
		} else if err != ledger.ErrInsufficientFunds {
			t.Fatal(err)
		}
	}
	acc, err := svc.GetAccount(ctx, d.AliceAccount)
	if err != nil {
		t.Fatal(err)
	}
	if posted != 1 || acc.Balance != 0 {
		t.Fatalf("posted=%d balance=%d", posted, acc.Balance)
	}
	sum, err := svc.SumBalances(ctx)
	if err != nil || sum != 0 {
		t.Fatalf("sum=%d err=%v", sum, err)
	}
}

func TestCannotSpendAnotherUsersFunds(t *testing.T) {
	svc, d := integration(t)
	_, err := svc.Transfer(context.Background(), ledger.TransferCmd{IdempotencyKey: uuid.NewString(), ActorUserID: d.BobUser,
		FromAccountID: d.AliceAccount, ToAccountID: d.BobAccount, AmountMinor: 1, Currency: "USD"})
	if err != ledger.ErrUnauthorized {
		t.Fatalf("expected unauthorized, got %v", err)
	}
}

func TestDescriptionParticipatesInFingerprint(t *testing.T) {
	svc, d := integration(t)
	ctx := context.Background()
	cmd := ledger.TransferCmd{IdempotencyKey: uuid.NewString(), ActorUserID: d.AliceUser,
		FromAccountID: d.AliceAccount, ToAccountID: d.BobAccount, AmountMinor: 1, Currency: "USD", Description: "invoice 1"}
	if _, err := svc.Transfer(ctx, cmd); err != nil {
		t.Fatal(err)
	}
	cmd.Description = "invoice 2"
	if _, err := svc.Transfer(ctx, cmd); err != ledger.ErrIdempotencyConflict {
		t.Fatalf("got %v", err)
	}
}
