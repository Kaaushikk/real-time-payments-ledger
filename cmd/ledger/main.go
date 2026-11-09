package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/Kaaushikk/real-time-payments-ledger/internal/authn"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/config"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/db"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/grpcserver"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/httpserver"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/idempotency"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/ledger"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/obs"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/outbox"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/seed"
	"github.com/Kaaushikk/real-time-payments-ledger/migrations"
)

func main() {
	cfg := config.Load("ledger")
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdown, err := obs.Setup(ctx, cfg.ServiceName, cfg.OTLPEndpoint)
	if err != nil {
		slog.Error("otel", "err", err)
		os.Exit(1)
	}
	defer func() { _ = shutdown(context.Background()) }()

	pool, err := db.Connect(ctx, cfg.DatabaseURL)
	if err != nil {
		slog.Error("postgres", "err", err)
		os.Exit(1)
	}
	defer pool.Close()
	if err := migrations.Apply(ctx, pool); err != nil {
		slog.Error("migrate", "err", err)
		os.Exit(1)
	}
	demo, err := seed.DemoData(ctx, pool)
	if err != nil {
		slog.Error("seed", "err", err)
		os.Exit(1)
	}
	slog.Info("seeded demo accounts",
		"alice_account", demo.AliceAccount,
		"bob_account", demo.BobAccount,
	)

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr, Password: cfg.RedisPassword})
	defer rdb.Close()

	keys := idempotency.New(rdb, pool, cfg.IdempotencyTTL)
	svc := ledger.New(pool, keys, cfg.KafkaTopic)
	auth := authn.New(pool, cfg)
	api := httpserver.LedgerAPI{Ledger: svc, Auth: auth}

	pub := outbox.New(pool, cfg.KafkaBrokers, cfg.KafkaTopic)
	defer pub.Close()
	go func() {
		if err := pub.Run(ctx); err != nil && ctx.Err() == nil {
			slog.Error("outbox", "err", err)
		}
	}()

	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		slog.Info("ledger http listening", "addr", cfg.HTTPAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http", "err", err)
			stop()
		}
	}()

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		slog.Error("grpc listen", "err", err)
		os.Exit(1)
	}
	grpcSrv := grpc.NewServer()
	grpcserver.RegisterLedger(grpcSrv, svc, auth)
	hs := health.NewServer()
	healthpb.RegisterHealthServer(grpcSrv, hs)
	hs.SetServingStatus("ledger.v1.LedgerService", healthpb.HealthCheckResponse_SERVING)
	reflection.Register(grpcSrv)
	go func() {
		slog.Info("ledger grpc listening", "addr", cfg.GRPCAddr)
		if err := grpcSrv.Serve(lis); err != nil {
			slog.Error("grpc", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	hs.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
	shCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shCtx)
	grpcSrv.GracefulStop()
}
