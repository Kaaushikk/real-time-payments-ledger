package obs

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/baggage"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/trace"
)

type captureLogs struct {
	records []sdklog.Record
}

func (c *captureLogs) Export(_ context.Context, records []sdklog.Record) error {
	for _, record := range records {
		c.records = append(c.records, record.Clone())
	}
	return nil
}
func (*captureLogs) Shutdown(context.Context) error   { return nil }
func (*captureLogs) ForceFlush(context.Context) error { return nil }

func TestLogCorrelatesStdoutAndOTLPWithoutContextBaggage(t *testing.T) {
	previous := slog.Default()
	defer slog.SetDefault(previous)
	var stdout bytes.Buffer
	capture := new(captureLogs)
	provider := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewSimpleProcessor(capture)))
	defer provider.Shutdown(context.Background())
	slog.SetDefault(slog.New(slog.NewMultiHandler(
		slog.NewJSONHandler(&stdout, nil),
		otelslog.NewHandler("test", otelslog.WithLoggerProvider(provider)),
	)))
	parent := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled,
	})
	member, err := baggage.NewMember("private-context", "must-stay-private")
	if err != nil {
		t.Fatal(err)
	}
	bag, err := baggage.New(member)
	if err != nil {
		t.Fatal(err)
	}
	ctx := baggage.ContextWithBaggage(trace.ContextWithSpanContext(context.Background(), parent), bag)
	Log(ctx, slog.LevelInfo, "transfer completed", slog.String("transaction_id", "test-transaction"))
	var output map[string]any
	if err := json.Unmarshal(stdout.Bytes(), &output); err != nil {
		t.Fatal(err)
	}
	if output["trace_id"] != parent.TraceID().String() || output["span_id"] != parent.SpanID().String() ||
		output["transaction_id"] != "test-transaction" {
		t.Fatalf("missing stdout correlation: %v", output)
	}
	if strings.Contains(stdout.String(), "must-stay-private") {
		t.Fatal("context baggage leaked into stdout")
	}
	if len(capture.records) != 1 {
		t.Fatalf("OTLP records=%d", len(capture.records))
	}
	record := capture.records[0]
	if record.TraceID() != parent.TraceID() || record.SpanID() != parent.SpanID() ||
		record.Body().AsString() != "transfer completed" {
		t.Fatal("OTLP log lost its native trace context")
	}
}
