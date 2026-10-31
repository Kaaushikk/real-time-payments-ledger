package idempotency

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/beybl/realtime-payments-ledger/internal/obs"
)

var (
	ErrConflict = errors.New("idempotency key reused with a different request")
	ErrInFlight = errors.New("request with this idempotency key is already in flight")
)

type Cached struct {
	Status int
	Body   []byte
}

type Store struct {
	rdb *redis.Client
	pg  *pgxpool.Pool
	ttl time.Duration
}

func New(rdb *redis.Client, pg *pgxpool.Pool, ttl time.Duration) *Store {
	return &Store{rdb: rdb, pg: pg, ttl: ttl}
}

func (s *Store) redisKey(key string) string {
	return "idempotency:" + key
}

type record struct {
	Fingerprint string `json:"fp"`
	Status      string `json:"status"`
	HTTPStatus  int    `json:"http,omitempty"`
	Body        []byte `json:"body,omitempty"`
}

// Begin claims the key or returns a cached completed response.
func (s *Store) Begin(ctx context.Context, key, fingerprint string) (*Cached, error) {
	ctx, span := obs.Tracer().Start(ctx, "idempotency.Begin",
		trace.WithAttributes(attribute.String("key", key)))
	defer span.End()

	if cached, err := s.fromRedis(ctx, key, fingerprint); err == nil && cached != nil {
		span.SetAttributes(attribute.Bool("redis_hit", true))
		return cached, nil
	} else if errors.Is(err, ErrConflict) || errors.Is(err, ErrInFlight) {
		return nil, err
	}

	if cached, err := s.fromPostgres(ctx, key, fingerprint); err == nil && cached != nil {
		_ = s.writeRedis(ctx, key, record{Fingerprint: fingerprint, Status: "complete", HTTPStatus: cached.Status, Body: cached.Body})
		return cached, nil
	} else if errors.Is(err, ErrConflict) || errors.Is(err, ErrInFlight) {
		return nil, err
	}

	rec := record{Fingerprint: fingerprint, Status: "processing"}
	ok, err := s.rdb.SetNX(ctx, s.redisKey(key), mustJSON(rec), s.ttl).Result()
	if err != nil {
		return s.beginPostgres(ctx, key, fingerprint)
	}
	if !ok {
		cached, err := s.fromRedis(ctx, key, fingerprint)
		if err != nil {
			return nil, err
		}
		if cached != nil {
			return cached, nil
		}
		return nil, ErrInFlight
	}
	if err := s.insertPostgres(ctx, key, fingerprint); err != nil {
		if errors.Is(err, ErrConflict) || errors.Is(err, ErrInFlight) {
			return nil, err
		}
		if cached, cErr := s.fromPostgres(ctx, key, fingerprint); cErr == nil && cached != nil {
			return cached, nil
		}
	}
	return nil, nil
}

func (s *Store) Complete(ctx context.Context, key string, status int, body []byte) error {
	ctx, span := obs.Tracer().Start(ctx, "idempotency.Complete")
	defer span.End()
	_, err := s.pg.Exec(ctx, `
		UPDATE idempotency_keys
		SET status = 'complete', response_status = $2, response_body = $3
		WHERE key = $1
	`, key, status, body)
	if err != nil {
		return err
	}
	var fp string
	_ = s.pg.QueryRow(ctx, `SELECT request_fingerprint FROM idempotency_keys WHERE key = $1`, key).Scan(&fp)
	return s.writeRedis(ctx, key, record{Fingerprint: fp, Status: "complete", HTTPStatus: status, Body: body})
}

func (s *Store) Fail(ctx context.Context, key string) error {
	_, _ = s.pg.Exec(ctx, `DELETE FROM idempotency_keys WHERE key = $1 AND status = 'processing'`, key)
	return s.rdb.Del(ctx, s.redisKey(key)).Err()
}

func (s *Store) fromRedis(ctx context.Context, key, fingerprint string) (*Cached, error) {
	raw, err := s.rdb.Get(ctx, s.redisKey(key)).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var rec record
	if err := json.Unmarshal(raw, &rec); err != nil {
		return nil, err
	}
	if rec.Fingerprint != fingerprint {
		return nil, ErrConflict
	}
	if rec.Status == "complete" {
		return &Cached{Status: rec.HTTPStatus, Body: rec.Body}, nil
	}
	return nil, ErrInFlight
}

func (s *Store) fromPostgres(ctx context.Context, key, fingerprint string) (*Cached, error) {
	var fp, status string
	var httpStatus *int
	var body []byte
	err := s.pg.QueryRow(ctx, `
		SELECT request_fingerprint, status, response_status, response_body
		FROM idempotency_keys
		WHERE key = $1 AND expires_at > now()
	`, key).Scan(&fp, &status, &httpStatus, &body)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if fp != fingerprint {
		return nil, ErrConflict
	}
	if status == "complete" && httpStatus != nil {
		return &Cached{Status: *httpStatus, Body: body}, nil
	}
	if status == "processing" {
		return nil, ErrInFlight
	}
	return nil, nil
}

func (s *Store) insertPostgres(ctx context.Context, key, fingerprint string) error {
	_, err := s.pg.Exec(ctx, `
		INSERT INTO idempotency_keys (key, request_fingerprint, status, expires_at)
		VALUES ($1, $2, 'processing', now() + $3::interval)
		ON CONFLICT (key) DO NOTHING
	`, key, fingerprint, fmt.Sprintf("%d seconds", int(s.ttl.Seconds())))
	return err
}

func (s *Store) beginPostgres(ctx context.Context, key, fingerprint string) (*Cached, error) {
	cached, err := s.fromPostgres(ctx, key, fingerprint)
	if err != nil || cached != nil {
		return cached, err
	}
	tag, err := s.pg.Exec(ctx, `
		INSERT INTO idempotency_keys (key, request_fingerprint, status, expires_at)
		VALUES ($1, $2, 'processing', now() + interval '24 hours')
		ON CONFLICT (key) DO NOTHING
	`, key, fingerprint)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 0 {
		return s.fromPostgres(ctx, key, fingerprint)
	}
	return nil, nil
}

func (s *Store) writeRedis(ctx context.Context, key string, rec record) error {
	return s.rdb.Set(ctx, s.redisKey(key), mustJSON(rec), s.ttl).Err()
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
