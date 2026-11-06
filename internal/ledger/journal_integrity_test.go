package ledger_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/Kaaushikk/real-time-payments-ledger/internal/ledger"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestMalformedPostingsRollBackAtCommit(t *testing.T) {
	type entry struct {
		source    bool
		direction string
		amount    int64
	}
	debit := entry{true, "debit", 7}
	credit := entry{false, "credit", 7}
	for _, tc := range []struct {
		name     string
		amount   int64
		currency string
		entries  []entry
	}{
		{"no entries", 7, "USD", nil},
		{"one sided", 7, "USD", []entry{debit}},
		{"unbalanced amounts", 7, "USD", []entry{debit, {false, "credit", 6}}},
		{"two debits", 7, "USD", []entry{debit, {false, "debit", 7}}},
		{"balanced but wrong transaction amount", 8, "USD", []entry{debit, credit}},
		{"transaction and account currency mismatch", 7, "EUR", []entry{debit, credit}},
		{"both entries on same account", 7, "USD", []entry{debit, {true, "credit", 7}}},
		{"four balanced entries", 7, "USD", []entry{debit, credit, debit, credit}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, d, pool := fixture(t)
			ctx := context.Background()
			before := readIsolationState(t, ctx, pool, d.AliceAccount, d.BobAccount)
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback(ctx) }()
			id := uuid.New()
			// Stage every posting side effect so a late constraint failure has
			// to undo more than just the journal. This is isolated test data.
			if _, err := tx.Exec(ctx, `INSERT INTO ledger_transactions (id,type,status,amount_minor,currency)
                VALUES ($1,'transfer','posted',$2,$3)`, id, tc.amount, tc.currency); err != nil {
				t.Fatalf("header should stage before deferred validation: %v", err)
			}
			if _, err := tx.Exec(ctx, `UPDATE accounts SET balance_minor=CASE WHEN id=$1 THEN balance_minor-7 ELSE balance_minor+7 END,
                version=version+1 WHERE id IN ($1,$2)`, d.AliceAccount, d.BobAccount); err != nil {
				t.Fatal(err)
			}
			for _, e := range tc.entries {
				account := d.BobAccount
				if e.source {
					account = d.AliceAccount
				}
				if _, err := tx.Exec(ctx, `INSERT INTO journal_entries (transaction_id,account_id,direction,amount_minor)
                    VALUES ($1,$2,$3,$4)`, id, account, e.direction, e.amount); err != nil {
					t.Fatalf("entry should stage before deferred validation: %v", err)
				}
			}
			if _, err := tx.Exec(ctx, `INSERT INTO audit_log (account_id,transaction_id,event_type,balance_before,balance_after,amount_minor)
                VALUES ($1,$3,'debit',$4,$5,7),($2,$3,'credit',$6,$7,7)`, d.AliceAccount, d.BobAccount, id,
				before.AliceBalance, before.AliceBalance-7, before.BobBalance, before.BobBalance+7); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(ctx, `INSERT INTO outbox (topic,payload) VALUES ('test.invalid.posting','{}'::jsonb)`); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(ctx, `INSERT INTO idempotency_keys (key,request_fingerprint,status,expires_at)
                VALUES ($1,'test-only-invalid-posting','processing',now()+interval '1 day')`, uuid.NewString()); err != nil {
				t.Fatal(err)
			}
			requireJournalConstraintFailure(t, tx.Commit(ctx))
			if after := readIsolationState(t, ctx, pool, d.AliceAccount, d.BobAccount); after != before {
				t.Fatalf("rejected posting left partial state:\nbefore %+v\nafter  %+v", before, after)
			}
			assertMoneyReconciles(t, ctx, pool)
		})
	}
}

func TestExtraEntriesCannotChangePostedTransaction(t *testing.T) {
	svc, d, pool := fixture(t)
	ctx := context.Background()
	cmd := ledger.TransferCmd{IdempotencyKey: uuid.NewString(), ActorUserID: d.AliceUser,
		FromAccountID: d.AliceAccount, ToAccountID: d.BobAccount, AmountMinor: 7, Currency: "USD"}
	posted, err := svc.Transfer(ctx, cmd)
	if err != nil {
		t.Fatal(err)
	}
	original, err := svc.GetTransaction(ctx, posted.Tx.ID)
	if err != nil {
		t.Fatal(err)
	}
	before := readIsolationState(t, ctx, pool, d.AliceAccount, d.BobAccount)
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// Even a balanced extra pair must not extend a committed two-entry posting.
	if _, err := tx.Exec(ctx, `INSERT INTO journal_entries (transaction_id,account_id,direction,amount_minor)
        VALUES ($1,$2,'debit',7),($1,$3,'credit',7)`, posted.Tx.ID, d.AliceAccount, d.BobAccount); err != nil {
		t.Fatalf("extra entries should be checked at commit: %v", err)
	}
	requireJournalConstraintFailure(t, tx.Commit(ctx))
	afterTx, err := svc.GetTransaction(ctx, posted.Tx.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(afterTx, original) {
		t.Fatalf("committed transaction changed:\nbefore %+v\nafter  %+v", original, afterTx)
	}
	replay, err := svc.Transfer(ctx, cmd)
	if err != nil || !replay.Replayed || replay.Tx.ID != posted.Tx.ID {
		t.Fatalf("original receipt no longer replays: result=%+v err=%v", replay, err)
	}
	if after := readIsolationState(t, ctx, pool, d.AliceAccount, d.BobAccount); after != before {
		t.Fatalf("rejected extension/replay changed state:\nbefore %+v\nafter  %+v", before, after)
	}
	assertMoneyReconciles(t, ctx, pool)
}

func requireJournalConstraintFailure(t *testing.T, err error) {
	t.Helper()
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
		t.Fatalf("expected deferred check-constraint failure (23514), got %v", err)
	}
}
