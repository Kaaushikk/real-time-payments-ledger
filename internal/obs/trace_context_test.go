package obs

import (
	"context"
	"testing"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/trace"
)

func TestKafkaTraceContextExcludesBaggage(t *testing.T) {
	parent := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}, TraceFlags: trace.FlagsSampled,
	})
	member, err := baggage.NewMember("private-value", "must-stay-local")
	if err != nil {
		t.Fatal(err)
	}
	bag, err := baggage.New(member)
	if err != nil {
		t.Fatal(err)
	}
	ctx := baggage.ContextWithBaggage(trace.ContextWithSpanContext(context.Background(), parent), bag)
	headers := KafkaTraceHeaders(ctx)
	if len(headers) != 1 || headers[0].Key != "traceparent" {
		t.Fatalf("unexpected Kafka trace headers: %v", headers)
	}
	extracted := ExtractKafkaTrace(context.Background(), headers)
	got := trace.SpanContextFromContext(extracted)
	if got.TraceID() != parent.TraceID() || got.SpanID() != parent.SpanID() || !got.IsRemote() {
		t.Fatal("Kafka did not preserve W3C trace parent")
	}
	if baggage.FromContext(extracted).Len() != 0 {
		t.Fatal("baggage crossed the Kafka boundary")
	}
	for _, headers := range [][]kafka.Header{nil, {{Key: "traceparent", Value: []byte("invalid")}}} {
		if trace.SpanContextFromContext(ExtractKafkaTrace(context.Background(), headers)).IsValid() {
			t.Fatal("missing or malformed trace metadata should not create a valid parent")
		}
	}
}
