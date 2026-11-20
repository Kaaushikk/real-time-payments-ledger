package obs

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"time"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

func Setup(ctx context.Context, serviceName, otlpEndpoint string) (func(context.Context) error, error) {
	stdout := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})
	slog.SetDefault(slog.New(stdout))
	res, err := resource.Merge(resource.Default(), resource.NewWithAttributes(
		semconv.SchemaURL, semconv.ServiceName(serviceName),
	))
	if err != nil {
		return nil, err
	}
	traceExporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(otlpEndpoint), otlptracegrpc.WithInsecure(),
		otlptracegrpc.WithTimeout(3*time.Second))
	if err != nil {
		return nil, err
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter), sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	logExporter, err := otlploggrpc.New(ctx,
		otlploggrpc.WithEndpoint(otlpEndpoint), otlploggrpc.WithInsecure(),
		otlploggrpc.WithTimeout(3*time.Second))
	if err != nil {
		_ = tp.Shutdown(ctx)
		return nil, err
	}
	lp := sdklog.NewLoggerProvider(sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(logExporter,
			sdklog.WithMaxQueueSize(512), sdklog.WithExportMaxBatchSize(128),
			sdklog.WithExportInterval(time.Second), sdklog.WithExportTimeout(3*time.Second))))
	slog.SetDefault(slog.New(slog.NewMultiHandler(stdout,
		otelslog.NewHandler(serviceName, otelslog.WithLoggerProvider(lp)))))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))
	return func(ctx context.Context) error {
		return errors.Join(lp.Shutdown(ctx), tp.Shutdown(ctx))
	}, nil
}

func Tracer() trace.Tracer {
	return otel.Tracer("payments-ledger")
}

// Log attaches correlation fields to the explicitly supplied application data.
func Log(ctx context.Context, level slog.Level, message string, attrs ...slog.Attr) {
	parent := trace.SpanContextFromContext(ctx)
	if parent.IsValid() {
		attrs = append(attrs, slog.String("trace_id", parent.TraceID().String()),
			slog.String("span_id", parent.SpanID().String()))
	}
	slog.LogAttrs(ctx, level, message, attrs...)
}
