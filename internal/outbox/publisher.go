package outbox

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"

	"github.com/Kaaushikk/real-time-payments-ledger/internal/obs"
)

type Publisher struct {
	pool   *pgxpool.Pool
	writer *kafka.Writer
}

func New(pool *pgxpool.Pool, brokers, topic string) *Publisher {
	return &Publisher{
		pool: pool,
		writer: &kafka.Writer{
			Addr:         kafka.TCP(strings.Split(brokers, ",")...),
			Topic:        topic,
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireOne,
			BatchTimeout: 50 * time.Millisecond,
		},
	}
}

func (p *Publisher) Close() error {
	return p.writer.Close()
}

func (p *Publisher) Run(ctx context.Context) error {
	t := time.NewTicker(200 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if err := p.flush(ctx); err != nil {
				slog.Error("outbox flush", "err", err)
			}
		}
	}
}

func (p *Publisher) flush(ctx context.Context) error {
	ctx, span := obs.Tracer().Start(ctx, "kafka.PublishOutbox")
	defer span.End()

	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		SELECT id, topic, payload FROM outbox
		WHERE published_at IS NULL
		ORDER BY created_at
		LIMIT 100
		FOR UPDATE SKIP LOCKED
	`)
	if err != nil {
		return err
	}

	type rec struct {
		id      uuid.UUID
		payload []byte
	}
	var batch []rec
	for rows.Next() {
		var r rec
		var topic string
		if err := rows.Scan(&r.id, &topic, &r.payload); err != nil {
			rows.Close()
			return err
		}
		batch = append(batch, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	span.SetAttributes(attribute.Int("batch_size", len(batch)))
	if len(batch) == 0 {
		return tx.Commit(ctx)
	}

	msgs := make([]kafka.Message, 0, len(batch))
	ids := make([]uuid.UUID, 0, len(batch))
	for _, r := range batch {
		ids = append(ids, r.id)
		msgs = append(msgs, kafka.Message{
			Key:   []byte(r.id.String()),
			Value: r.payload,
		})
	}
	if err := p.writer.WriteMessages(ctx, msgs...); err != nil {
		span.RecordError(err)
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, ids); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
