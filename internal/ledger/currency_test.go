package ledger_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Kaaushikk/real-time-payments-ledger/internal/ledger"
	"github.com/google/uuid"
)

func TestDefaultCurrencyReplaysExplicitUSD(t *testing.T) {
	svc, d, pool := fixture(t)
	ctx := context.Background()
	before, err := svc.GetAccount(ctx, d.AliceAccount)
	if err != nil {
		t.Fatal(err)
	}
	cmd := ledger.TransferCmd{IdempotencyKey: uuid.NewString(), ActorUserID: d.AliceUser,
		FromAccountID: d.AliceAccount, ToAccountID: d.BobAccount, AmountMinor: 19}
	first, err := svc.Transfer(ctx, cmd)
	if err != nil {
		t.Fatal(err)
	}
	if first.Tx.Currency != "USD" || first.Replayed {
		t.Fatalf("unexpected first result: %+v", first)
	}
	cmd.Currency = "USD"
	second, err := svc.Transfer(ctx, cmd)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Replayed || second.Tx.ID != first.Tx.ID {
		t.Fatal("explicit USD did not replay defaulted transfer")
	}
	// Reversing the spelling on another key must also return the same posting.
	cmd.IdempotencyKey = uuid.NewString()
	third, err := svc.Transfer(ctx, cmd)
	if err != nil {
		t.Fatal(err)
	}
	cmd.Currency = ""
	fourth, err := svc.Transfer(ctx, cmd)
	if err != nil {
		t.Fatal(err)
	}
	if !fourth.Replayed || third.Tx.ID != fourth.Tx.ID {
		t.Fatal("omitted currency did not replay explicit USD")
	}
	after, err := svc.GetAccount(ctx, d.AliceAccount)
	if err != nil {
		t.Fatal(err)
	}
	var postings int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM ledger_transactions WHERE type='transfer'`).Scan(&postings); err != nil {
		t.Fatal(err)
	}
	if postings != 2 || after.Balance != before.Balance-38 {
		t.Fatalf("postings=%d balance=%d", postings, after.Balance)
	}
}

func TestUnsupportedCurrenciesLeaveNoTransferState(t *testing.T) {
	svc, d, pool := fixture(t)
	ctx := context.Background()
	before, err := svc.GetAccount(ctx, d.AliceAccount)
	if err != nil {
		t.Fatal(err)
	}
	var auditBefore int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM audit_log`).Scan(&auditBefore); err != nil {
		t.Fatal(err)
	}
	cmd := ledger.TransferCmd{IdempotencyKey: uuid.NewString(), ActorUserID: d.AliceUser,
		FromAccountID: d.AliceAccount, ToAccountID: d.BobAccount, AmountMinor: 7}
	for _, currency := range []string{"EUR", "usd", " USD", "USD "} {
		cmd.Currency = currency
		if _, err := svc.Transfer(ctx, cmd); !errors.Is(err, ledger.ErrInvalidTransfer) {
			t.Fatalf("currency %q: expected invalid transfer, got %v", currency, err)
		}
	}
	var receipts, postings, outbox, audit int
	if err := pool.QueryRow(ctx, `SELECT
        (SELECT count(*) FROM idempotency_keys),
        (SELECT count(*) FROM ledger_transactions WHERE type='transfer'),
        (SELECT count(*) FROM outbox),
        (SELECT count(*) FROM audit_log)`).Scan(&receipts, &postings, &outbox, &audit); err != nil {
		t.Fatal(err)
	}
	after, err := svc.GetAccount(ctx, d.AliceAccount)
	if err != nil {
		t.Fatal(err)
	}
	if receipts != 0 || postings != 0 || outbox != 0 || audit != auditBefore || after.Balance != before.Balance || after.Version != before.Version {
		t.Fatal("rejected currency changed transfer state")
	}
	// A validation failure must not poison the retry key for a corrected request.
	cmd.Currency = "USD"
	if _, err := svc.Transfer(ctx, cmd); err != nil {
		t.Fatalf("corrected request: %v", err)
	}
}
