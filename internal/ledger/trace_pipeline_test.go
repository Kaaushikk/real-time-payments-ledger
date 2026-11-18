package ledger_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/encoding/protojson"

	ledgerv1 "github.com/Kaaushikk/real-time-payments-ledger/gen/go/ledger/v1"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/consumer"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/httpserver"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/idempotency"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/ledger"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/obs"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/outbox"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/seed"
)

func recordPipelineSpans(t *testing.T) (*tracetest.SpanRecorder, *sdktrace.TracerProvider) {
	t.Helper()
	previous := otel.GetTracerProvider()
	previousPropagator := otel.GetTextMapPropagator()
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	if endpoint := os.Getenv("TRACE_TEST_OTLP_ENDPOINT"); endpoint != "" {
		if _, err := obs.Setup(context.Background(), "ledger-trace-verification", endpoint); err != nil {
			t.Fatal(err)
		}
		var ok bool
		provider, ok = otel.GetTracerProvider().(*sdktrace.TracerProvider)
		if !ok {
			t.Fatal("setup did not install the SDK tracer provider")
		}
		provider.RegisterSpanProcessor(recorder)
	}
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := provider.Shutdown(ctx); err != nil {
			t.Error(err)
		}
		otel.SetTracerProvider(previous)
		otel.SetTextMapPropagator(previousPropagator)
	})
	return recorder, provider
}

func postTracedGateway(t *testing.T, ctx context.Context, svc *ledger.Service, demo seed.Demo) trace.TraceID {
	t.Helper()
	conn := ledgerRPCConn(t, svc)
	handler, err := httpserver.WithLedgerGateway(ctx, conn, http.NotFoundHandler())
	if err != nil {
		t.Fatal(err)
	}
	parentCtx, parent := obs.Tracer().Start(ctx, "test.TransferRequest")
	body, err := protojson.Marshal(&ledgerv1.TransferRequest{
		IdempotencyKey: uuid.NewString(),
		FromAccountId:  demo.AliceAccount.String(), ToAccountId: demo.BobAccount.String(),
		AmountMinor: 13, Currency: "USD",
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/gateway/v1/transfers", strings.NewReader(string(body))).WithContext(ctx)
	req.Header.Set("Authorization", rpcBearer(t, demo.AliceUser.String()))
	req.Header.Set("Content-Type", "application/json")
	propagation.TraceContext{}.Inject(parentCtx, propagation.HeaderCarrier(req.Header))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, req)
	parent.End()
	if response.Code != http.StatusOK {
		t.Fatalf("gateway status=%d body=%s", response.Code, response.Body.String())
	}
	return parent.SpanContext().TraceID()
}

func assertPipelineParents(t *testing.T, recorder *tracetest.SpanRecorder, traceID trace.TraceID, kafkaStages bool) {
	t.Helper()
	spans := map[string]sdktrace.ReadOnlySpan{}
	for _, span := range recorder.Ended() {
		if span.SpanContext().TraceID() == traceID {
			spans[span.Name()] = span
		}
	}
	names := []string{"test.TransferRequest", "ledger.gateway.http", "/ledger.v1.LedgerService/Transfer", "ledger.Transfer", "ledger.transferOnce"}
	if kafkaStages {
		names = append(names, "kafka.PublishTransfer", "kafka.ProcessTransfer")
	}
	for i, name := range names {
		span, ok := spans[name]
		if !ok {
			t.Fatalf("missing span %q in trace %s", name, traceID)
		}
		if i > 0 && span.Parent().SpanID() != spans[names[i-1]].SpanContext().SpanID() {
			t.Errorf("span %q has wrong parent", name)
		}
	}
}

func TestGatewayPersistsTraceParent(t *testing.T) {
	svc, demo, pool := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	recorder, _ := recordPipelineSpans(t)
	traceID := postTracedGateway(t, ctx, svc, demo)
	var parent, state string
	if err := pool.QueryRow(ctx, "SELECT traceparent, tracestate FROM outbox").Scan(&parent, &state); err != nil {
		t.Fatal(err)
	}
	storedCtx := propagation.TraceContext{}.Extract(ctx, propagation.MapCarrier{"traceparent": parent, "tracestate": state})
	stored := trace.SpanContextFromContext(storedCtx)
	if !stored.IsValid() || stored.TraceID() != traceID {
		t.Fatalf("outbox did not save request trace %s", traceID)
	}
	assertPipelineParents(t, recorder, traceID, false)
	for _, span := range recorder.Ended() {
		if span.Name() == "ledger.transferOnce" && span.SpanContext().SpanID() != stored.SpanID() {
			t.Fatal("outbox did not save the transfer attempt span as its parent")
		}
	}
}

func TestTracePipelineThroughKafka(t *testing.T) {
	broker := os.Getenv("KAFKA_TEST_BROKER")
	if broker == "" {
		t.Skip("set KAFKA_TEST_BROKER to verify traces across Kafka")
	}
	_, demo, pool := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	recorder, provider := recordPipelineSpans(t)
	topic := "trace-pipeline-" + uuid.NewString()
	admin := &kafka.Client{Addr: kafka.TCP(broker)}
	created, err := admin.CreateTopics(ctx, &kafka.CreateTopicsRequest{
		Topics: []kafka.TopicConfig{{Topic: topic, NumPartitions: 1, ReplicationFactor: 1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := created.Errors[topic]; err != nil {
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
	service := ledger.New(pool, idempotency.New(nil, pool, 24*time.Hour), topic)
	traceID := postTracedGateway(t, ctx, service, demo)
	publisher := outbox.New(pool, broker)
	defer publisher.Close()
	publishCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- publisher.Run(publishCtx) }()
	defer func() { stop(); <-done }()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		var published bool
		if err := pool.QueryRow(ctx, "SELECT published_at IS NOT NULL FROM outbox").Scan(&published); err != nil {
			t.Fatal(err)
		}
		if published {
			break
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("publisher did not deliver the traced event")
		}
	}
	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: []string{broker}, Topic: topic, Partition: 0, MinBytes: 1, MaxBytes: 1 << 20})
	defer reader.Close()
	message, err := reader.FetchMessage(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := consumer.New(pool).Handle(ctx, message); err != nil {
		t.Fatal(err)
	}
	assertPipelineParents(t, recorder, traceID, true)
	if err := provider.ForceFlush(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("verified trace_id=%s", traceID)
}
