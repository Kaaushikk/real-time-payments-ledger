package outbox

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	"github.com/Kaaushikk/real-time-payments-ledger/migrations"
)

func TestPublisherRecoversWhenKafkaIsReachable(t *testing.T) {
	broker := os.Getenv("KAFKA_TEST_BROKER")
	dbURL := os.Getenv("DATABASE_URL")
	if broker == "" || dbURL == "" {
		t.Skip("set KAFKA_TEST_BROKER and DATABASE_URL to run the broker integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cfg, err := pgxpool.ParseConfig(dbURL)
	if err != nil {
		t.Fatal(err)
	}
	schema := "test_outbox_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	admin, err := pgxpool.NewWithConfig(ctx, cfg.Copy())
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := admin.Exec(dropCtx, "DROP SCHEMA "+schema+" CASCADE"); err != nil {
			t.Error(err)
		}
	}()
	cfg.ConnConfig.RuntimeParams["search_path"] = schema + ",public"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := migrations.Apply(ctx, pool); err != nil {
		t.Fatal(err)
	}

	id := uuid.New()
	topic := "outbox-recovery-" + id.String()
	kafkaAdmin := &kafka.Client{Addr: kafka.TCP(broker)}
	topics, err := kafkaAdmin.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := topics.Errors[topic]; err != nil {
		t.Fatal(err)
	}
	defer func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		response, err := kafkaAdmin.DeleteTopics(cleanupCtx, &kafka.DeleteTopicsRequest{Topics: []string{topic}})
		if err != nil {
			t.Error(err)
		} else if err := response.Errors[topic]; err != nil {
			t.Error(err)
		}
	}()
	if _, err := pool.Exec(ctx, `INSERT INTO outbox (id, topic, payload) VALUES ($1, $2, jsonb_build_object('event_id', $1::uuid, 'schema_version', 1))`, id, topic); err != nil {
		t.Fatal(err)
	}

	pending := func() (bool, []byte) {
		t.Helper()
		var isPending bool
		var body []byte
		// Wait for any in-flight publish transaction to release its row lock.
		if err := pool.QueryRow(ctx, `SELECT published_at IS NULL, payload FROM outbox WHERE id=$1 FOR UPDATE`, id).Scan(&isPending, &body); err != nil {
			t.Fatal(err)
		}
		return isPending, body
	}

	var reachable atomic.Bool
	var firstFailure sync.Once
	dialFailed := make(chan struct{})
	dialer := &net.Dialer{}
	transport := &kafka.Transport{
		Dial: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if !reachable.Load() {
				firstFailure.Do(func() { close(dialFailed) })
				return nil, errors.New("broker unreachable during recovery test")
			}
			return dialer.DialContext(ctx, network, addr)
		},
	}
	defer transport.CloseIdleConnections()
	publisher := New(pool, broker)
	publisher.writer.Transport = transport
	defer publisher.Close()
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- publisher.Run(runCtx) }()
	defer func() {
		stop()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("publisher stopped with %v", err)
		}
	}()

	select {
	case <-dialFailed:
	case <-ctx.Done():
		t.Fatal("publisher never attempted to reach Kafka")
	}
	isPending, original := pending()
	if !isPending {
		t.Fatal("unreachable broker must leave the event pending")
	}
	reachable.Store(true)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		isPending, body := pending()
		if !isPending {
			if !bytes.Equal(body, original) {
				t.Fatal("publisher changed the stored event payload")
			}
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("event stayed pending after broker recovery")
		}
	}
	reader, err := kafka.DialLeader(ctx, "tcp", broker, topic, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if err := reader.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	msg, err := reader.ReadMessage(1 << 20)
	if err != nil {
		t.Fatal(err)
	}
	if string(msg.Key) != id.String() || !bytes.Equal(msg.Value, original) {
		t.Fatal("Kafka did not receive the original event key and payload")
	}
}
