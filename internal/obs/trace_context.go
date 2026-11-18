package obs

import (
	"context"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel/propagation"
)

// TraceCarrier contains only W3C trace fields, excluding baggage and credentials.
func TraceCarrier(ctx context.Context) propagation.MapCarrier {
	carrier := propagation.MapCarrier{}
	propagation.TraceContext{}.Inject(ctx, carrier)
	return carrier
}

func KafkaTraceHeaders(ctx context.Context) []kafka.Header {
	carrier := TraceCarrier(ctx)
	headers := make([]kafka.Header, 0, len(carrier))
	for _, key := range []string{"traceparent", "tracestate"} {
		if value := carrier.Get(key); value != "" {
			headers = append(headers, kafka.Header{Key: key, Value: []byte(value)})
		}
	}
	return headers
}

func ExtractKafkaTrace(ctx context.Context, headers []kafka.Header) context.Context {
	carrier := propagation.MapCarrier{}
	for _, header := range headers {
		if header.Key == "traceparent" || header.Key == "tracestate" {
			carrier.Set(header.Key, string(header.Value))
		}
	}
	return propagation.TraceContext{}.Extract(ctx, carrier)
}
