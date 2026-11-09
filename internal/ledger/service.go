package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/Kaaushikk/real-time-payments-ledger/internal/idempotency"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/metrics"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/obs"
)

func prometheusTimer() func() {
	start := time.Now()
	return func() { metrics.TransferLatency.Observe(time.Since(start).Seconds()) }
}

type Service struct {
	pool  *pgxpool.Pool
	keys  *idempotency.Store
	topic string
}

func New(pool *pgxpool.Pool, keys *idempotency.Store, topic string) *Service {
	return &Service{pool: pool, keys: keys, topic: topic}
}

func (s *Service) CreateAccount(ctx context.Context, owner uuid.UUID, currency string) (Account, error) {
	if currency == "" {
		currency = "USD"
	}
	if currency != "USD" {
		return Account{}, ErrCurrencyMismatch
	}
	var a Account
	err := s.pool.QueryRow(ctx, `
		INSERT INTO accounts (owner_user_id, currency, account_type)
		VALUES ($1, $2, 'user')
		RETURNING id, owner_user_id, currency, account_type, balance_minor, version
	`, owner, currency).Scan(&a.ID, &a.OwnerUserID, &a.Currency, &a.Type, &a.Balance, &a.Version)
	return a, err
}

func (s *Service) GetAccount(ctx context.Context, id uuid.UUID) (Account, error) {
	var a Account
	err := s.pool.QueryRow(ctx, `
		SELECT id, owner_user_id, currency, account_type, balance_minor, version
		FROM accounts WHERE id = $1
	`, id).Scan(&a.ID, &a.OwnerUserID, &a.Currency, &a.Type, &a.Balance, &a.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		return Account{}, ErrAccountNotFound
	}
	return a, err
}

func (s *Service) GetTransaction(ctx context.Context, id uuid.UUID) (Transaction, error) {
	var tx Transaction
	err := s.pool.QueryRow(ctx, `
		SELECT id, type, status, amount_minor, currency, description, created_at
		FROM ledger_transactions WHERE id = $1
	`, id).Scan(&tx.ID, &tx.Type, &tx.Status, &tx.Amount, &tx.Currency, &tx.Description, &tx.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Transaction{}, ErrAccountNotFound
	}
	if err != nil {
		return Transaction{}, err
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, account_id, direction, amount_minor
		FROM journal_entries WHERE transaction_id = $1 ORDER BY direction
	`, id)
	if err != nil {
		return Transaction{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var e JournalEntry
		if err := rows.Scan(&e.ID, &e.AccountID, &e.Direction, &e.Amount); err != nil {
			return Transaction{}, err
		}
		tx.Entries = append(tx.Entries, e)
	}
	return tx, rows.Err()
}

// GetOwnedAccount looks up a wallet within the caller's account collection.
func (s *Service) GetOwnedAccount(ctx context.Context, id, caller uuid.UUID) (Account, error) {
	acc, err := s.GetAccount(ctx, id)
	if err != nil {
		return Account{}, err
	}
	if caller == uuid.Nil || acc.OwnerUserID == nil || *acc.OwnerUserID != caller {
		return Account{}, ErrAccountNotFound
	}
	return acc, nil
}

// GetParticipantTransaction looks up a transaction visible to a journal participant.
func (s *Service) GetParticipantTransaction(ctx context.Context, id, caller uuid.UUID) (Transaction, error) {
	tx, err := s.GetTransaction(ctx, id)
	if err != nil {
		return Transaction{}, err
	}
	if caller == uuid.Nil {
		return Transaction{}, ErrAccountNotFound
	}
	for _, entry := range tx.Entries {
		acc, err := s.GetAccount(ctx, entry.AccountID)
		if err != nil {
			return Transaction{}, err
		}
		if acc.OwnerUserID != nil && *acc.OwnerUserID == caller {
			return tx, nil
		}
	}
	return Transaction{}, ErrAccountNotFound
}

func (s *Service) ListAudit(ctx context.Context, accountID uuid.UUID, limit int) ([]AuditEvent, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := s.pool.Query(ctx, `
		SELECT id, account_id, transaction_id, event_type, balance_before, balance_after, amount_minor, created_at
		FROM audit_log WHERE account_id = $1 ORDER BY id DESC LIMIT $2
	`, accountID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []AuditEvent
	for rows.Next() {
		var e AuditEvent
		if err := rows.Scan(&e.ID, &e.AccountID, &e.TransactionID, &e.EventType, &e.BalanceBefore, &e.BalanceAfter, &e.AmountMinor, &e.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func (s *Service) Transfer(ctx context.Context, cmd TransferCmd) (TransferResult, error) {
	// Normalize before validation and fingerprinting so every transport shares
	// the same default and an omitted currency replays an explicit USD request.
	if cmd.Currency == "" {
		cmd.Currency = "USD"
	}
	timer := prometheusTimer()
	defer timer()
	ctx, span := obs.Tracer().Start(ctx, "ledger.Transfer",
		trace.WithAttributes(
			attribute.String("from", cmd.FromAccountID.String()),
			attribute.String("to", cmd.ToAccountID.String()),
			attribute.Int64("amount_minor", cmd.AmountMinor),
		),
	)
	defer span.End()

	if err := cmd.Validate(); err != nil {
		metrics.Transfers.WithLabelValues("invalid").Inc()
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return TransferResult{}, err
	}

	var result TransferResult
	var lastErr error
	for attempt := 0; attempt < 32; attempt++ {
		result, lastErr = s.transferOnce(ctx, cmd)
		if lastErr == nil {
			break
		}
		if !isRetryableTx(lastErr) {
			break
		}
		span.AddEvent("serialization_retry", trace.WithAttributes(attribute.Int("attempt", attempt+1)))
		delay := time.Duration(1<<min(attempt, 8)) * time.Millisecond
		select {
		case <-ctx.Done():
			return TransferResult{}, ctx.Err()
		case <-time.After(delay):
		}
	}
	if lastErr != nil {
		metrics.Transfers.WithLabelValues("error").Inc()
		span.RecordError(lastErr)
		span.SetStatus(codes.Error, lastErr.Error())
		return TransferResult{}, lastErr
	}

	span.SetAttributes(attribute.String("transaction_id", result.Tx.ID.String()))
	if result.Replayed {
		metrics.Transfers.WithLabelValues("replayed").Inc()
	} else {
		metrics.Transfers.WithLabelValues("posted").Inc()
	}
	return result, nil
}

func (s *Service) transferOnce(ctx context.Context, cmd TransferCmd) (TransferResult, error) {
	ctx, span := obs.Tracer().Start(ctx, "ledger.transferOnce")
	defer span.End()

	// Both mutable account rows are locked before checking funds. READ COMMITTED
	// returns their latest committed values after waiting and avoids SSI retry
	// storms under hot-account contention. Range-based rules would need review.
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return TransferResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Scope keys to the authenticated principal and operation, never globally.
	receiptKey := cmd.ActorUserID.String() + ":transfer:" + cmd.IdempotencyKey
	cached, err := s.keys.BeginTx(ctx, tx, receiptKey, cmd.Fingerprint())
	if err != nil {
		return TransferResult{}, mapIdempotency(err)
	}
	if cached != nil {
		var posted Transaction
		if err := json.Unmarshal(cached.Body, &posted); err != nil {
			return TransferResult{}, err
		}
		return TransferResult{Tx: posted, Replayed: true}, nil
	}

	// Lock rows in UUID order to avoid deadlocks when Alice↔Bob transfer concurrently.
	ids := []uuid.UUID{cmd.FromAccountID, cmd.ToAccountID}
	if ids[0].String() > ids[1].String() {
		ids[0], ids[1] = ids[1], ids[0]
	}
	locked := make(map[uuid.UUID]Account, 2)
	for _, id := range ids {
		acc, err := lockAccount(ctx, tx, id)
		if err != nil {
			return TransferResult{}, err
		}
		locked[id] = acc
	}
	fromAcc := locked[cmd.FromAccountID]
	toAcc := locked[cmd.ToAccountID]

	if fromAcc.Type != AccountUser || fromAcc.OwnerUserID == nil || *fromAcc.OwnerUserID != cmd.ActorUserID {
		return TransferResult{}, ErrUnauthorized
	}
	if toAcc.Type != AccountUser {
		return TransferResult{}, ErrUnauthorized
	}

	if fromAcc.Currency != cmd.Currency || toAcc.Currency != cmd.Currency {
		return TransferResult{}, ErrCurrencyMismatch
	}
	if fromAcc.Type == AccountUser && fromAcc.Balance < cmd.AmountMinor {
		return TransferResult{}, ErrInsufficientFunds
	}

	if toAcc.Balance > math.MaxInt64-cmd.AmountMinor {
		return TransferResult{}, ErrAmountOverflow
	}
	fromBefore, toBefore := fromAcc.Balance, toAcc.Balance
	fromAcc.Balance -= cmd.AmountMinor
	toAcc.Balance += cmd.AmountMinor
	if fromAcc.Type == AccountUser && fromAcc.Balance < 0 {
		return TransferResult{}, ErrNegativeBalance
	}

	if err := updateBalance(ctx, tx, fromAcc); err != nil {
		return TransferResult{}, err
	}
	if err := updateBalance(ctx, tx, toAcc); err != nil {
		return TransferResult{}, err
	}

	txID := uuid.New()
	now := time.Now().UTC()
	if _, err := tx.Exec(ctx, `
		INSERT INTO ledger_transactions (id, type, status, amount_minor, currency, description)
		VALUES ($1, 'transfer', 'posted', $2, $3, $4)
	`, txID, cmd.AmountMinor, cmd.Currency, cmd.Description); err != nil {
		return TransferResult{}, err
	}

	debitID, creditID := uuid.New(), uuid.New()
	if _, err := tx.Exec(ctx, `
		INSERT INTO journal_entries (id, transaction_id, account_id, direction, amount_minor)
		VALUES ($1, $5, $2, 'debit', $4), ($3, $5, $6, 'credit', $4)
	`, debitID, cmd.FromAccountID, creditID, cmd.AmountMinor, txID, cmd.ToAccountID); err != nil {
		return TransferResult{}, err
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO audit_log (account_id, transaction_id, event_type, balance_before, balance_after, amount_minor, metadata)
		VALUES
		  ($1, $3, 'debit', $4, $5, $7, $8),
		  ($2, $3, 'credit', $6, $9, $7, $8)
	`, cmd.FromAccountID, cmd.ToAccountID, txID, fromBefore, fromAcc.Balance, toBefore, cmd.AmountMinor,
		fmt.Sprintf(`{"actor":"%s"}`, cmd.ActorUserID), toAcc.Balance); err != nil {
		return TransferResult{}, err
	}

	payload, _ := json.Marshal(map[string]any{
		"event_type":      "ledger.transaction.posted",
		"transaction_id":  txID.String(),
		"from_account_id": cmd.FromAccountID.String(),
		"to_account_id":   cmd.ToAccountID.String(),
		"amount_minor":    cmd.AmountMinor,
		"currency":        cmd.Currency,
		"occurred_at":     now.Format(time.RFC3339Nano),
	})
	if _, err := tx.Exec(ctx, `
		INSERT INTO outbox (topic, payload) VALUES ($1, $2)
	`, s.topic, payload); err != nil {
		return TransferResult{}, err
	}

	result := TransferResult{Tx: Transaction{
		ID:          txID,
		Type:        "transfer",
		Status:      "posted",
		Amount:      cmd.AmountMinor,
		Currency:    cmd.Currency,
		Description: cmd.Description,
		CreatedAt:   now,
		Entries: []JournalEntry{
			{ID: debitID, AccountID: cmd.FromAccountID, Direction: "debit", Amount: cmd.AmountMinor},
			{ID: creditID, AccountID: cmd.ToAccountID, Direction: "credit", Amount: cmd.AmountMinor},
		},
	}}
	body, err := json.Marshal(result.Tx)
	if err != nil {
		return TransferResult{}, err
	}
	if err := s.keys.CompleteTx(ctx, tx, receiptKey, 201, body); err != nil {
		return TransferResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return TransferResult{}, err
	}
	return result, nil

}

func lockAccount(ctx context.Context, tx pgx.Tx, id uuid.UUID) (Account, error) {
	var a Account
	err := tx.QueryRow(ctx, `
		SELECT id, owner_user_id, currency, account_type, balance_minor, version
		FROM accounts WHERE id = $1 FOR UPDATE
	`, id).Scan(&a.ID, &a.OwnerUserID, &a.Currency, &a.Type, &a.Balance, &a.Version)
	if errors.Is(err, pgx.ErrNoRows) {
		return Account{}, ErrAccountNotFound
	}
	return a, err
}

func updateBalance(ctx context.Context, tx pgx.Tx, a Account) error {
	ct, err := tx.Exec(ctx, `
		UPDATE accounts
		SET balance_minor = $2, version = version + 1, updated_at = now()
		WHERE id = $1
	`, a.ID, a.Balance)
	if err != nil {
		return err
	}
	if ct.RowsAffected() != 1 {
		return fmt.Errorf("account update affected %d rows", ct.RowsAffected())
	}
	return nil
}

func mapIdempotency(err error) error {
	switch {
	case errors.Is(err, idempotency.ErrConflict):
		return ErrIdempotencyConflict
	case errors.Is(err, idempotency.ErrInFlight):
		return ErrIdempotencyInFlight
	default:
		return err
	}
}

func isRetryableTx(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "40001" || pgErr.Code == "40P01"
	}
	return false
}

func (s *Service) SumBalances(ctx context.Context) (int64, error) {
	var sum int64
	err := s.pool.QueryRow(ctx, `SELECT COALESCE(SUM(balance_minor), 0) FROM accounts`).Scan(&sum)
	return sum, err
}

func (s *Service) MinUserBalance(ctx context.Context) (int64, error) {
	var v int64
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(MIN(balance_minor), 0) FROM accounts WHERE account_type = 'user'
	`).Scan(&v)
	return v, err
}

func (s *Service) Ready(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	return s.pool.Ping(ctx)
}
