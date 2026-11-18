package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/Kaaushikk/real-time-payments-ledger/internal/config"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/consumer"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/db"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/obs"
	"github.com/Kaaushikk/real-time-payments-ledger/migrations"
)

func main() {
	if err := run(); err != nil {
		slog.Error("consumer stopped", "err", err)
		os.Exit(1)
	}
}

func run() error {
	cfg := config.Load("consumer")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	shutdown, err := obs.Setup(ctx, cfg.ServiceName, cfg.OTLPEndpoint)
	if err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdown(shutdownCtx)
	}()

	pool, err := db.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := migrations.Apply(ctx, pool); err != nil {
		return err
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: strings.Split(cfg.KafkaBrokers, ","),
		Topic:   cfg.KafkaTopic, GroupID: cfg.KafkaConsumerGroup,
		MinBytes: 1, MaxBytes: 1 << 20,
		StartOffset:    kafka.FirstOffset,
		CommitInterval: 0,
	})
	defer reader.Close()
	if err := consumer.New(pool).Run(ctx, reader); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}
