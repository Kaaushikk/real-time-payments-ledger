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
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/Kaaushikk/real-time-payments-ledger/internal/obs"
)

const (
	pollInterval   = 200 * time.Millisecond
	maxRetryDelay  = 5 * time.Second
	publishTimeout = 5 * time.Second
)

type Publisher struct {
	pool   *pgxpool.Pool
	writer *kafka.Writer
}

func New(pool *pgxpool.Pool, brokers string) *Publisher {
	return &Publisher{
		pool: pool,
		writer: &kafka.Writer{
			Addr:         kafka.TCP(strings.Split(brokers, ",")...),
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireAll,
			MaxAttempts:  1,
			ReadTimeout:  2 * time.Second,
			WriteTimeout: 2 * time.Second,
			BatchTimeout: 50 * time.Millisecond,
		},
	}
}

func (p *Publisher) Close() error {
	return p.writer.Close()
}

func (p *Publisher) Run(ctx context.Context) error {
	timer := time.NewTimer(pollInterval)
	defer timer.Stop()
	delay := pollInterval
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
		if err := p.flush(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			delay = min(2*delay, maxRetryDelay)
			slog.Error("outbox flush", "err", err, "retry_in", delay)
		} else {
			delay = pollInterval
		}
		timer.Reset(delay)
	}
}

func (p *Publisher) flush(ctx context.Context) (err error) {
	ctx, span := obs.Tracer().Start(ctx, "kafka.PublishOutbox")
	defer span.End()

	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	rows, err := tx.Query(ctx, `
		SELECT id, topic, payload, traceparent, tracestate FROM outbox
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
		topic   string
		payload []byte
		parent  string
		state   string
	}
	var batch []rec
	for rows.Next() {
		var r rec
		if err := rows.Scan(&r.id, &r.topic, &r.payload, &r.parent, &r.state); err != nil {
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
	var eventSpans []trace.Span
	defer func() {
		for _, eventSpan := range eventSpans {
			if err != nil {
				eventSpan.RecordError(err)
				eventSpan.SetStatus(codes.Error, "publication failed")
			}
			eventSpan.End()
		}
	}()
	for _, r := range batch {
		eventCtx := propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier{
			"traceparent": r.parent, "tracestate": r.state,
		})
		eventCtx, eventSpan := obs.Tracer().Start(eventCtx, "kafka.PublishTransfer",
			trace.WithSpanKind(trace.SpanKindProducer),
			trace.WithAttributes(attribute.String("event_id", r.id.String())))
		eventSpans = append(eventSpans, eventSpan)
		ids = append(ids, r.id)
		msgs = append(msgs, kafka.Message{
			Topic:   r.topic,
			Key:     []byte(r.id.String()),
			Value:   r.payload,
			Headers: obs.KafkaTraceHeaders(eventCtx),
		})
	}
	publishCtx, cancel := context.WithTimeout(ctx, publishTimeout)
	defer cancel()
	if err := p.writer.WriteMessages(publishCtx, msgs...); err != nil {
		span.RecordError(err)
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, ids); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
