// Package idempotency stores receipts atomically with ledger postings.
package idempotency

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"time"
)

var (
	ErrConflict = errors.New("idempotency key reused with a different request")
	ErrInFlight = errors.New("request with this idempotency key is already in flight")
)

type Cached struct {
	Status int
	Body   []byte
}
type Store struct{ ttl time.Duration }

// Redis is intentionally outside the correctness path. Constructor arguments
// remain compatible with the draft's wiring; caching is a later milestone.
func New(_ *redis.Client, _ *pgxpool.Pool, ttl time.Duration) *Store { return &Store{ttl: ttl} }

// BeginTx claims or replays a receipt inside the journal transaction. Expiry is
// retention metadata, not permission to execute a previously posted transfer.
func (s *Store) BeginTx(ctx context.Context, tx pgx.Tx, key, fingerprint string) (*Cached, error) {
	tag, err := tx.Exec(ctx, `INSERT INTO idempotency_keys
        (key,request_fingerprint,status,expires_at)
        VALUES ($1,$2,'processing',now()+$3::interval) ON CONFLICT (key) DO NOTHING`,
		key, fingerprint, fmt.Sprintf("%d seconds", int64(s.ttl.Seconds())))
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 1 {
		return nil, nil
	}
	var fp, status string
	var code *int
	var body []byte
	err = tx.QueryRow(ctx, `SELECT request_fingerprint,status,response_status,response_body
        FROM idempotency_keys WHERE key=$1`, key).Scan(&fp, &status, &code, &body)
	if err != nil {
		return nil, err
	}
	if fp != fingerprint {
		return nil, ErrConflict
	}
	if status != "complete" || code == nil {
		return nil, ErrInFlight
	}
	return &Cached{Status: *code, Body: body}, nil
}
func (s *Store) CompleteTx(ctx context.Context, tx pgx.Tx, key string, status int, body []byte) error {
	tag, err := tx.Exec(ctx, `UPDATE idempotency_keys SET status='complete',response_status=$2,
        response_body=$3 WHERE key=$1 AND status='processing'`, key, status, body)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errors.New("idempotency receipt missing")
	}
	return nil
}
