// Package consumer maintains a durable projection of posted transfer events.
package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/Kaaushikk/real-time-payments-ledger/internal/events"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/obs"
)

var (
	ErrInvalidEvent  = errors.New("invalid transfer event")
	ErrEventConflict = errors.New("event identity reused with different transfer data")
)

// Reader uses explicit acknowledgements after successful database processing.
type Reader interface {
	FetchMessage(context.Context) (kafka.Message, error)
	CommitMessages(context.Context, ...kafka.Message) error
}

type Consumer struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Consumer {
	return &Consumer{pool: pool}
}

func (c *Consumer) Run(ctx context.Context, reader Reader) error {
	for {
		message, err := reader.FetchMessage(ctx)
		if err != nil {
			return err
		}
		if err := c.Handle(ctx, message); err != nil {
			// Stop before a later offset can acknowledge this failed message.
			return fmt.Errorf("process partition %d offset %d: %w", message.Partition, message.Offset, err)
		}
		if err := reader.CommitMessages(ctx, message); err != nil {
			return fmt.Errorf("commit partition %d offset %d: %w", message.Partition, message.Offset, err)
		}
	}
}

// Handle stores the event and its deduplication identity in a single insert.
func (c *Consumer) Handle(ctx context.Context, message kafka.Message) (err error) {
	ctx = obs.ExtractKafkaTrace(ctx, message.Headers)
	ctx, span := obs.Tracer().Start(ctx, "kafka.ProcessTransfer", trace.WithSpanKind(trace.SpanKindConsumer))
	var eventID, transactionID string
	var duplicate bool
	defer func() {
		result := "processed"
		level := slog.LevelInfo
		if err != nil {
			result = "error"
			level = slog.LevelError
			span.RecordError(err)
			span.SetStatus(codes.Error, "event processing failed")
		} else if duplicate {
			result = "duplicate"
		}
		obs.Log(ctx, level, "transfer event processed",
			slog.String("event_id", eventID), slog.String("transaction_id", transactionID),
			slog.String("result", result), slog.Int("partition", message.Partition), slog.Int64("offset", message.Offset))
		span.End()
	}()
	var event events.TransactionPosted
	if err := json.Unmarshal(message.Value, &event); err != nil {
		return fmt.Errorf("%w: malformed JSON", ErrInvalidEvent)
	}
	if event.EventType != events.TransactionPostedType || event.SchemaVersion != events.TransactionPostedVersion ||
		event.EventID == uuid.Nil || event.TransactionID == uuid.Nil ||
		event.FromAccountID == uuid.Nil || event.ToAccountID == uuid.Nil ||
		event.FromAccountID == event.ToAccountID || event.AmountMinor <= 0 ||
		event.Currency != "USD" || event.OccurredAt.IsZero() ||
		string(message.Key) != event.EventID.String() {
		return ErrInvalidEvent
	}
	eventID, transactionID = event.EventID.String(), event.TransactionID.String()
	span.SetAttributes(attribute.String("event_id", eventID))
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	tag, err := c.pool.Exec(ctx, `
		INSERT INTO consumed_transfers (event_id, transaction_id, payload)
		VALUES ($1, $2, $3)
		ON CONFLICT DO NOTHING
	`, event.EventID, event.TransactionID, payload)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		return nil
	}

	// Compare typed data so harmless JSON whitespace and field order can vary.
	var matches bool
	err = c.pool.QueryRow(ctx, `
		SELECT payload = $2::jsonb FROM consumed_transfers WHERE event_id = $1
	`, event.EventID, payload).Scan(&matches)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrEventConflict
	}
	if err != nil {
		return err
	}
	span.SetAttributes(attribute.Bool("duplicate", matches))
	duplicate = matches
	if !matches {
		return ErrEventConflict
	}
	return nil
}
