package ledger_test

import (
	"context"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/ledger"
	"github.com/google/uuid"
	"testing"
)

func TestExpiredReceiptStillReplays(t *testing.T) {
	svc, d, pool := fixture(t)
	ctx := context.Background()
	cmd := ledger.TransferCmd{IdempotencyKey: uuid.NewString(), ActorUserID: d.AliceUser,
		FromAccountID: d.AliceAccount, ToAccountID: d.BobAccount, AmountMinor: 5, Currency: "USD"}
	first, err := svc.Transfer(ctx, cmd)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE idempotency_keys SET expires_at=now()-interval '2 days'`); err != nil {
		t.Fatal(err)
	}
	replay, err := svc.Transfer(ctx, cmd)
	if err != nil {
		t.Fatal(err)
	}
	if !replay.Replayed || replay.Tx.ID != first.Tx.ID {
		t.Fatal("expired receipt executed again")
	}
}

func TestOutboxFailureRollsBackWholeTransfer(t *testing.T) {
	svc, d, pool := fixture(t)
	ctx := context.Background()
	before, err := svc.GetAccount(ctx, d.AliceAccount)
	if err != nil {
		t.Fatal(err)
	}
	// A database constraint failure late in the transaction must not leave money,
	// journal rows, audit records, or a processing receipt behind.
	if _, err := pool.Exec(ctx, `ALTER TABLE outbox ADD CONSTRAINT reject_test_event CHECK (false)`); err != nil {
		t.Fatal(err)
	}
	cmd := ledger.TransferCmd{IdempotencyKey: uuid.NewString(), ActorUserID: d.AliceUser,
		FromAccountID: d.AliceAccount, ToAccountID: d.BobAccount, AmountMinor: 5, Currency: "USD"}
	if _, err := svc.Transfer(ctx, cmd); err == nil {
		t.Fatal("expected injected failure")
	}
	after, err := svc.GetAccount(ctx, d.AliceAccount)
	if err != nil {
		t.Fatal(err)
	}
	if after.Balance != before.Balance || after.Version != before.Version {
		t.Fatal("partial balance update")
	}
	var receipts, transfers int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM idempotency_keys`).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM ledger_transactions WHERE type='transfer'`).Scan(&transfers); err != nil {
		t.Fatal(err)
	}
	if receipts != 0 || transfers != 0 {
		t.Fatalf("partial receipts=%d transfers=%d", receipts, transfers)
	}
	if _, err := pool.Exec(ctx, `ALTER TABLE outbox DROP CONSTRAINT reject_test_event`); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Transfer(ctx, cmd); err != nil {
		t.Fatalf("retry after rollback: %v", err)
	}
}

func TestDatabaseRejectsIncompletePosting(t *testing.T) {
	_, d, pool := fixture(t)
	ctx := context.Background()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	id := uuid.New()
	if _, err := tx.Exec(ctx, `INSERT INTO ledger_transactions (id,type,status,amount_minor,currency)
        VALUES ($1,'transfer','posted',7,'USD')`, id); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO journal_entries (transaction_id,account_id,direction,amount_minor)
        VALUES ($1,$2,'debit',7)`, id, d.AliceAccount); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err == nil {
		t.Fatal("accepted one-sided posting")
	}
}

func TestAppendOnlyAndReconciliation(t *testing.T) {
	svc, d, pool := fixture(t)
	ctx := context.Background()
	_, err := svc.Transfer(ctx, ledger.TransferCmd{IdempotencyKey: uuid.NewString(), ActorUserID: d.AliceUser,
		FromAccountID: d.AliceAccount, ToAccountID: d.BobAccount, AmountMinor: 11, Currency: "USD"})
	if err != nil {
		t.Fatal(err)
	}
	for _, sql := range []string{`UPDATE journal_entries SET amount_minor=1`, `DELETE FROM audit_log`,
		`DELETE FROM ledger_transactions`, `TRUNCATE audit_log`} {
		if _, err := pool.Exec(ctx, sql); err == nil {
			t.Fatalf("accepted mutation: %s", sql)
		}
	}
	var mismatches int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM accounts a WHERE balance_minor <> (
        SELECT coalesce(sum(CASE WHEN direction='credit' THEN amount_minor::numeric ELSE -amount_minor::numeric END),0)
        FROM journal_entries e WHERE e.account_id=a.id)`).Scan(&mismatches); err != nil {
		t.Fatal(err)
	}
	if mismatches != 0 {
		t.Fatalf("journal does not reconcile: %d", mismatches)
	}
}
