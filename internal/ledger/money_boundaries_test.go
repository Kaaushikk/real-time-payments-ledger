package ledger_test

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/Kaaushikk/real-time-payments-ledger/internal/ledger"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestExactBalanceSpendReplaysAfterWalletIsEmpty(t *testing.T) {
	svc, d, pool := fixture(t)
	ctx := context.Background()
	before := readIsolationState(t, ctx, pool, d.AliceAccount, d.BobAccount)
	cmd := ledger.TransferCmd{IdempotencyKey: uuid.NewString(), ActorUserID: d.AliceUser,
		FromAccountID: d.AliceAccount, ToAccountID: d.BobAccount, AmountMinor: before.AliceBalance, Currency: "USD"}
	first, err := svc.Transfer(ctx, cmd)
	if err != nil || first.Replayed {
		t.Fatalf("exact balance spend: result=%+v err=%v", first, err)
	}
	want := before
	want.AliceBalance = 0
	want.BobBalance += before.AliceBalance
	want.AliceVersion++
	want.BobVersion++
	want.Receipts++
	want.Transfers++
	want.JournalEntries += 2
	want.AuditRecords += 2
	want.OutboxEvents++
	if got := readIsolationState(t, ctx, pool, d.AliceAccount, d.BobAccount); got != want {
		t.Fatalf("exact spend state:\ngot  %+v\nwant %+v", got, want)
	}
	replay, err := svc.Transfer(ctx, cmd)
	if err != nil || !replay.Replayed || replay.Tx.ID != first.Tx.ID {
		t.Fatalf("empty-wallet replay: result=%+v err=%v", replay, err)
	}
	cmd.IdempotencyKey = uuid.NewString()
	cmd.AmountMinor = 1
	if _, err := svc.Transfer(ctx, cmd); !errors.Is(err, ledger.ErrInsufficientFunds) {
		t.Fatalf("new spend from empty wallet: %v", err)
	}
	if got := readIsolationState(t, ctx, pool, d.AliceAccount, d.BobAccount); got != want {
		t.Fatalf("replay/rejected spend changed state: %+v", got)
	}
	assertMoneyReconciles(t, ctx, pool)
}

func TestMoneyAmountBoundariesLeaveNoTransferState(t *testing.T) {
	svc, d, pool := fixture(t)
	ctx := context.Background()
	before := readIsolationState(t, ctx, pool, d.AliceAccount, d.BobAccount)
	cmd := ledger.TransferCmd{IdempotencyKey: uuid.NewString(), ActorUserID: d.AliceUser,
		FromAccountID: d.AliceAccount, ToAccountID: d.BobAccount, Currency: "USD"}
	for _, tc := range []struct {
		name   string
		amount int64
		want   error
	}{
		{"minimum int64", math.MinInt64, ledger.ErrInvalidTransfer},
		{"negative unit", -1, ledger.ErrInvalidTransfer},
		{"zero", 0, ledger.ErrInvalidTransfer},
		{"one above available", before.AliceBalance + 1, ledger.ErrInsufficientFunds},
		{"maximum int64 is representable but unaffordable", math.MaxInt64, ledger.ErrInsufficientFunds},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd.AmountMinor = tc.amount
			if _, err := svc.Transfer(ctx, cmd); !errors.Is(err, tc.want) {
				t.Fatalf("amount %d: got %v want %v", tc.amount, err, tc.want)
			}
			if after := readIsolationState(t, ctx, pool, d.AliceAccount, d.BobAccount); after != before {
				t.Fatalf("rejected amount changed state: %+v", after)
			}
		})
	}
	cmd.AmountMinor = 1
	if got, err := svc.Transfer(ctx, cmd); err != nil || got.Replayed {
		t.Fatalf("rejected amounts consumed retry key: result=%+v err=%v", got, err)
	}
	assertMoneyReconciles(t, ctx, pool)
}

func TestDestinationInt64LimitAndOverflowRecovery(t *testing.T) {
	svc, d, pool := fixture(t)
	ctx := context.Background()
	alice, err := svc.GetAccount(ctx, d.AliceAccount)
	if err != nil {
		t.Fatal(err)
	}
	// Leave two units with Alice, then add balanced test-only funding to Bob.
	// Total user balances become MaxInt64+1, offset by MinInt64 issuance.
	if _, err := svc.Transfer(ctx, ledger.TransferCmd{IdempotencyKey: uuid.NewString(), ActorUserID: d.AliceUser,
		FromAccountID: d.AliceAccount, ToAccountID: d.BobAccount, AmountMinor: alice.Balance - 2, Currency: "USD"}); err != nil {
		t.Fatal(err)
	}
	bob, err := svc.GetAccount(ctx, d.BobAccount)
	if err != nil {
		t.Fatal(err)
	}
	fundBoundaryWallet(t, ctx, pool, d.Issuance, d.BobAccount, math.MaxInt64-1-bob.Balance)
	assertMoneyReconciles(t, ctx, pool)

	cmd := ledger.TransferCmd{IdempotencyKey: uuid.NewString(), ActorUserID: d.AliceUser,
		FromAccountID: d.AliceAccount, ToAccountID: d.BobAccount, AmountMinor: 1, Currency: "USD"}
	posted, err := svc.Transfer(ctx, cmd)
	if err != nil || posted.Replayed {
		t.Fatalf("reaching exactly MaxInt64: result=%+v err=%v", posted, err)
	}
	atLimit := readIsolationState(t, ctx, pool, d.AliceAccount, d.BobAccount)
	if atLimit.AliceBalance != 1 || atLimit.BobBalance != math.MaxInt64 {
		t.Fatalf("unexpected boundary balances: %+v", atLimit)
	}
	replay, err := svc.Transfer(ctx, cmd)
	if err != nil || !replay.Replayed || replay.Tx.ID != posted.Tx.ID {
		t.Fatalf("replay at destination limit: result=%+v err=%v", replay, err)
	}
	cmd.IdempotencyKey = uuid.NewString()
	if _, err := svc.Transfer(ctx, cmd); !errors.Is(err, ledger.ErrAmountOverflow) {
		t.Fatalf("one unit beyond destination limit: %v", err)
	}
	if got := readIsolationState(t, ctx, pool, d.AliceAccount, d.BobAccount); got != atLimit {
		t.Fatalf("overflow/replay changed state:\ngot  %+v\nwant %+v", got, atLimit)
	}
	// Make room, then retry the exact previously rejected request and key.
	if _, err := svc.Transfer(ctx, ledger.TransferCmd{IdempotencyKey: uuid.NewString(), ActorUserID: d.BobUser,
		FromAccountID: d.BobAccount, ToAccountID: d.AliceAccount, AmountMinor: 1, Currency: "USD"}); err != nil {
		t.Fatal(err)
	}
	if got, err := svc.Transfer(ctx, cmd); err != nil || got.Replayed {
		t.Fatalf("retry after making room: result=%+v err=%v", got, err)
	}
	final := readIsolationState(t, ctx, pool, d.AliceAccount, d.BobAccount)
	if final.AliceBalance != 1 || final.BobBalance != math.MaxInt64 {
		t.Fatalf("unexpected final balances: %+v", final)
	}
	assertMoneyReconciles(t, ctx, pool)
}

// Test-only balanced issuance in the fixture's isolated schema. No funding API
// is exposed; both wallet and issuance retain matching journal/audit records.
func fundBoundaryWallet(t *testing.T, ctx context.Context, pool *pgxpool.Pool, issuance, wallet uuid.UUID, amount int64) {
	t.Helper()
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var issuanceBefore, walletBefore int64
	if err := tx.QueryRow(ctx, `SELECT a.balance_minor,b.balance_minor FROM accounts a CROSS JOIN accounts b
        WHERE a.id=$1 AND b.id=$2 FOR UPDATE OF a,b`, issuance, wallet).Scan(&issuanceBefore, &walletBefore); err != nil {
		t.Fatal(err)
	}
	if amount <= 0 || issuanceBefore < math.MinInt64+amount || walletBefore > math.MaxInt64-amount {
		t.Fatal("invalid test funding boundary")
	}
	tag, err := tx.Exec(ctx, `UPDATE accounts SET balance_minor=CASE WHEN id=$1 THEN balance_minor-$3 ELSE balance_minor+$3 END,
        version=version+1 WHERE id IN ($1,$2)`, issuance, wallet, amount)
	if err != nil {
		t.Fatal(err)
	}
	if tag.RowsAffected() != 2 {
		t.Fatal("test funding did not update both accounts")
	}
	id := uuid.New()
	if _, err := tx.Exec(ctx, `INSERT INTO ledger_transactions (id,type,status,amount_minor,currency,description)
        VALUES ($1,'deposit','posted',$2,'USD','test-only boundary funding')`, id, amount); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO journal_entries (transaction_id,account_id,direction,amount_minor)
        VALUES ($1,$2,'debit',$4),($1,$3,'credit',$4)`, id, issuance, wallet, amount); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO audit_log (account_id,transaction_id,event_type,balance_before,balance_after,amount_minor)
        VALUES ($1,$3,'boundary_funding',$4,$5,$8),($2,$3,'boundary_funding',$6,$7,$8)`,
		issuance, wallet, id, issuanceBefore, issuanceBefore-amount, walletBefore, walletBefore+amount, amount); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
}

func assertMoneyReconciles(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()
	var mismatches int
	var conserved bool
	if err := pool.QueryRow(ctx, `SELECT
        (SELECT count(*) FROM accounts a WHERE a.balance_minor <> (
            SELECT coalesce(sum(CASE WHEN direction='credit' THEN amount_minor::numeric ELSE -amount_minor::numeric END),0)
            FROM journal_entries e WHERE e.account_id=a.id)),
        (SELECT coalesce(sum(balance_minor::numeric),0)=0 FROM accounts)`).Scan(&mismatches, &conserved); err != nil {
		t.Fatal(err)
	}
	if mismatches != 0 || !conserved {
		t.Fatalf("money reconciliation: mismatches=%d conserved=%v", mismatches, conserved)
	}
}
