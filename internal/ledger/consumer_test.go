package ledger_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/Kaaushikk/real-time-payments-ledger/internal/consumer"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/events"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/ledger"
)

func postedConsumerMessage(t *testing.T) kafka.Message {
	t.Helper()
	event := events.TransactionPosted{
		EventID: uuid.New(), EventType: events.TransactionPostedType,
		SchemaVersion: events.TransactionPostedVersion, TransactionID: uuid.New(),
		FromAccountID: uuid.New(), ToAccountID: uuid.New(),
		AmountMinor: 11, Currency: "USD", OccurredAt: time.Now().UTC(),
	}
	body, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	return kafka.Message{Key: []byte(event.EventID.String()), Value: body}
}

func TestConsumerDuplicateDeliveries(t *testing.T) {
	svc, demo, pool := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := svc.Transfer(ctx, ledger.TransferCmd{
		IdempotencyKey: uuid.NewString(), ActorUserID: demo.AliceUser,
		FromAccountID: demo.AliceAccount, ToAccountID: demo.BobAccount,
		AmountMinor: 11, Currency: "USD",
	}); err != nil {
		t.Fatal(err)
	}
	before := readIsolationState(t, ctx, pool, demo.AliceAccount, demo.BobAccount)
	var id uuid.UUID
	var body []byte
	if err := pool.QueryRow(ctx, "SELECT id, payload FROM outbox").Scan(&id, &body); err != nil {
		t.Fatal(err)
	}
	message := kafka.Message{Key: []byte(id.String()), Value: body}
	service := consumer.New(pool)
	const callers = 8
	results := make(chan error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- service.Handle(ctx, message)
		}()
	}
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	var event events.TransactionPosted
	if err := json.Unmarshal(body, &event); err != nil {
		t.Fatal(err)
	}
	// A fresh service instance models redelivery after process restart.
	reformatted, err := json.MarshalIndent(event, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := consumer.New(pool).Handle(ctx, kafka.Message{Key: message.Key, Value: reformatted}); err != nil {
		t.Fatalf("reformatted duplicate: %v", err)
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM consumed_transfers").Scan(&count); err != nil || count != 1 {
		t.Fatalf("projection rows=%d err=%v", count, err)
	}
	event.AmountMinor++
	changed, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Handle(ctx, kafka.Message{Key: message.Key, Value: changed}); !errors.Is(err, consumer.ErrEventConflict) {
		t.Fatalf("changed duplicate: %v", err)
	}
	event.EventID = uuid.New()
	changed, err = json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Handle(ctx, kafka.Message{Key: []byte(event.EventID.String()), Value: changed}); !errors.Is(err, consumer.ErrEventConflict) {
		t.Fatalf("same transaction with a new event ID: %v", err)
	}
	if got := readIsolationState(t, ctx, pool, demo.AliceAccount, demo.BobAccount); got != before {
		t.Fatal("consumer processing changed ledger state")
	}
}

type consumerTestReader struct {
	messages []kafka.Message
	commits  []int64
	commit   func() error
}

func (r *consumerTestReader) FetchMessage(context.Context) (kafka.Message, error) {
	if len(r.messages) == 0 {
		return kafka.Message{}, io.EOF
	}
	message := r.messages[0]
	r.messages = r.messages[1:]
	return message, nil
}

func (r *consumerTestReader) CommitMessages(_ context.Context, messages ...kafka.Message) error {
	if r.commit != nil {
		if err := r.commit(); err != nil {
			return err
		}
	}
	for _, message := range messages {
		r.commits = append(r.commits, message.Offset)
	}
	return nil
}

func TestConsumerCommitsAfterStorageAndStopsOnFailure(t *testing.T) {
	_, _, pool := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	service := consumer.New(pool)
	message := postedConsumerMessage(t)
	message.Offset = 7
	reader := &consumerTestReader{messages: []kafka.Message{message}}
	if _, err := pool.Exec(ctx, "ALTER TABLE consumed_transfers ADD CONSTRAINT test_reject_event CHECK (false)"); err != nil {
		t.Fatal(err)
	}
	if err := service.Run(ctx, reader); err == nil || len(reader.commits) != 0 {
		t.Fatal("storage failure should stop without acknowledging")
	}
	if _, err := pool.Exec(ctx, "ALTER TABLE consumed_transfers DROP CONSTRAINT test_reject_event"); err != nil {
		t.Fatal(err)
	}
	commitFailure := errors.New("test commit failure")
	reader = &consumerTestReader{
		messages: []kafka.Message{message},
		commit: func() error {
			var count int
			if err := pool.QueryRow(ctx, "SELECT count(*) FROM consumed_transfers").Scan(&count); err != nil {
				return err
			}
			if count != 1 {
				t.Fatal("offset commit attempted before storage")
			}
			return commitFailure
		},
	}
	if err := service.Run(ctx, reader); !errors.Is(err, commitFailure) {
		t.Fatalf("expected offset commit failure: %v", err)
	}
	invalid := kafka.Message{Offset: 8, Key: message.Key, Value: []byte("{}")}
	later := postedConsumerMessage(t)
	later.Offset = 9
	reader = &consumerTestReader{messages: []kafka.Message{message, invalid, later}}
	if err := consumer.New(pool).Run(ctx, reader); !errors.Is(err, consumer.ErrInvalidEvent) {
		t.Fatalf("invalid event: %v", err)
	}
	if len(reader.commits) != 1 || reader.commits[0] != 7 || len(reader.messages) != 1 {
		t.Fatalf("commits=%v remaining=%d", reader.commits, len(reader.messages))
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM consumed_transfers").Scan(&count); err != nil || count != 1 {
		t.Fatalf("redelivery after failed commit created rows=%d err=%v", count, err)
	}
}

func TestConsumerRejectsInvalidEnvelopes(t *testing.T) {
	service := consumer.New(nil) // Validation must finish before database access.
	valid := postedConsumerMessage(t)
	var original events.TransactionPosted
	if err := json.Unmarshal(valid.Value, &original); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name string
		edit func(*events.TransactionPosted)
	}{
		{"unsupported version", func(e *events.TransactionPosted) { e.SchemaVersion++ }},
		{"legacy version missing", func(e *events.TransactionPosted) { e.SchemaVersion = 0 }},
		{"wrong type", func(e *events.TransactionPosted) { e.EventType = "other" }},
		{"missing event ID", func(e *events.TransactionPosted) { e.EventID = uuid.Nil }},
		{"missing transaction", func(e *events.TransactionPosted) { e.TransactionID = uuid.Nil }},
		{"missing source", func(e *events.TransactionPosted) { e.FromAccountID = uuid.Nil }},
		{"same account", func(e *events.TransactionPosted) { e.ToAccountID = e.FromAccountID }},
		{"invalid amount", func(e *events.TransactionPosted) { e.AmountMinor = -1 }},
		{"unsupported currency", func(e *events.TransactionPosted) { e.Currency = "EUR" }},
		{"missing timestamp", func(e *events.TransactionPosted) { e.OccurredAt = time.Time{} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			event := original
			tc.edit(&event)
			body, err := json.Marshal(event)
			if err != nil {
				t.Fatal(err)
			}
			if err := service.Handle(context.Background(), kafka.Message{Key: valid.Key, Value: body}); !errors.Is(err, consumer.ErrInvalidEvent) {
				t.Fatalf("expected invalid event: %v", err)
			}
		})
	}
	for _, message := range []kafka.Message{
		{Key: valid.Key, Value: []byte("{")},
		{Key: []byte(uuid.NewString()), Value: valid.Value},
	} {
		if err := service.Handle(context.Background(), message); !errors.Is(err, consumer.ErrInvalidEvent) {
			t.Fatalf("expected invalid JSON or mismatched key: %v", err)
		}
	}
}

func TestConsumerKafkaRedeliveryKeepsFailedOffset(t *testing.T) {
	broker := os.Getenv("KAFKA_TEST_BROKER")
	if broker == "" {
		t.Skip("set KAFKA_TEST_BROKER to run the Kafka consumer integration test")
	}
	_, _, pool := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	topic := "consumer-replay-" + uuid.NewString()
	group := topic
	admin := &kafka.Client{Addr: kafka.TCP(broker)}
	response, err := admin.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := response.Errors[topic]; err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		deleted, err := admin.DeleteTopics(cleanupCtx, &kafka.DeleteTopicsRequest{Topics: []string{topic}})
		if err != nil {
			t.Error(err)
		} else if err := deleted.Errors[topic]; err != nil {
			t.Error(err)
		}
	}()

	first := postedConsumerMessage(t)
	invalid := kafka.Message{Key: first.Key, Value: []byte("{}")}
	later := postedConsumerMessage(t)
	writer := &kafka.Writer{Addr: kafka.TCP(broker), Topic: topic, RequiredAcks: kafka.RequireAll}
	defer writer.Close()
	if err := writer.WriteMessages(ctx, first, first, invalid, later); err != nil {
		t.Fatal(err)
	}
	newReader := func() *kafka.Reader {
		return kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{broker}, Topic: topic, GroupID: group,
			MinBytes: 1, MaxBytes: 1 << 20,
			StartOffset: kafka.FirstOffset, CommitInterval: 0,
		})
	}
	reader := newReader()
	defer reader.Close()
	if err := consumer.New(pool).Run(ctx, reader); !errors.Is(err, consumer.ErrInvalidEvent) {
		t.Fatalf("expected consumer to stop on malformed event: %v", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	resumed := newReader()
	defer resumed.Close()
	message, err := resumed.FetchMessage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if message.Offset != 2 || string(message.Value) != "{}" {
		t.Fatalf("failed event was skipped after restart: offset=%d", message.Offset)
	}
	var count int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM consumed_transfers").Scan(&count); err != nil || count != 1 {
		t.Fatalf("Kafka duplicate delivery created rows=%d err=%v", count, err)
	}
}
