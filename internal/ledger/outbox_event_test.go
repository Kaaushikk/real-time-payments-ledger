package ledger_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/Kaaushikk/real-time-payments-ledger/internal/events"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/ledger"
)

func TestPostedEventContractAndAtomicity(t *testing.T) {
	svc, demo, pool := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	before := readIsolationState(t, ctx, pool, demo.AliceAccount, demo.BobAccount)
	cmd := ledger.TransferCmd{
		IdempotencyKey: uuid.NewString(), ActorUserID: demo.AliceUser,
		FromAccountID: demo.AliceAccount, ToAccountID: demo.BobAccount,
		AmountMinor: 17, Currency: "USD",
	}

	// Fail receipt completion, which normally runs after the outbox insert.
	if _, err := pool.Exec(ctx, `ALTER TABLE idempotency_keys ADD CONSTRAINT test_reject_completion CHECK (status <> 'complete')`); err != nil {
		t.Fatal(err)
	}
	_, err := svc.Transfer(ctx, cmd)
	var dbErr *pgconn.PgError
	if !errors.As(err, &dbErr) || dbErr.ConstraintName != "test_reject_completion" {
		t.Fatalf("expected receipt completion failure, got %v", err)
	}
	if got := readIsolationState(t, ctx, pool, demo.AliceAccount, demo.BobAccount); got != before {
		t.Fatalf("rolled back transfer left state: got=%+v want=%+v", got, before)
	}
	if _, err := pool.Exec(ctx, `ALTER TABLE idempotency_keys DROP CONSTRAINT test_reject_completion`); err != nil {
		t.Fatal(err)
	}

	first, err := svc.Transfer(ctx, cmd)
	if err != nil || first.Replayed {
		t.Fatalf("initial posting after rollback: %+v, %v", first, err)
	}
	replay, err := svc.Transfer(ctx, cmd)
	if err != nil || !replay.Replayed || replay.Tx.ID != first.Tx.ID {
		t.Fatalf("repeated transfer: %+v, %v", replay, err)
	}

	var id uuid.UUID
	var topic string
	var pending bool
	var payload []byte
	if err := pool.QueryRow(ctx, `SELECT id, topic, published_at IS NULL, payload FROM outbox`).Scan(&id, &topic, &pending, &payload); err != nil {
		t.Fatal(err)
	}
	var event events.TransactionPosted
	if err := json.Unmarshal(payload, &event); err != nil {
		t.Fatal(err)
	}
	// Check the public JSON field names independently of the Go type.
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(payload, &fields); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"event_id", "event_type", "schema_version", "transaction_id",
		"from_account_id", "to_account_id", "amount_minor", "currency", "occurred_at"} {
		if _, ok := fields[name]; !ok {
			t.Errorf("event is missing %q", name)
		}
	}
	if len(fields) != 9 {
		t.Errorf("event has %d fields, want 9", len(fields))
	}
	if topic != "ledger.transactions" || !pending || id == uuid.Nil || event.EventID != id ||
		event.EventType != "ledger.transaction.posted" || event.SchemaVersion != 1 ||
		event.TransactionID != first.Tx.ID || event.FromAccountID != cmd.FromAccountID ||
		event.ToAccountID != cmd.ToAccountID || event.AmountMinor != cmd.AmountMinor ||
		event.Currency != cmd.Currency || !event.OccurredAt.Equal(first.Tx.CreatedAt) {
		t.Fatalf("unexpected posted event (topic=%s pending=%t): %+v", topic, pending, event)
	}
	if got := readIsolationState(t, ctx, pool, demo.AliceAccount, demo.BobAccount); got.OutboxEvents != before.OutboxEvents+1 {
		t.Fatalf("outbox events=%d, want %d", got.OutboxEvents, before.OutboxEvents+1)
	}
	assertMoneyReconciles(t, ctx, pool)
}
