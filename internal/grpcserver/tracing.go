package grpcserver

import (
	"context"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/Kaaushikk/real-time-payments-ledger/internal/obs"
)

// TraceUnary carries the HTTP gateway's W3C parent into the business RPC.
func TraceUnary(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	carrier := propagation.MapCarrier{}
	for _, key := range []string{"traceparent", "tracestate"} {
		values := metadata.ValueFromIncomingContext(ctx, key)
		if len(values) == 1 {
			carrier.Set(key, values[0])
		}
	}
	ctx = propagation.TraceContext{}.Extract(ctx, carrier)
	ctx, span := obs.Tracer().Start(ctx, info.FullMethod, trace.WithSpanKind(trace.SpanKindServer))
	defer span.End()
	response, err := handler(ctx, req)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "RPC failed")
	}
	return response, err
}
