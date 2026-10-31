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

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"

	"github.com/Kaaushikk/real-time-payments-ledger/internal/authn"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/config"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/db"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/httpserver"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/obs"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/seed"
	"github.com/Kaaushikk/real-time-payments-ledger/migrations"
)

func main() {
	cfg := config.Load("auth")
	if cfg.HTTPAddr == ":8080" {
		cfg.HTTPAddr = ":8081"
	}
	if cfg.GRPCAddr == ":9090" {
		cfg.GRPCAddr = ":9091"
	}

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
	if _, err := seed.DemoData(ctx, pool); err != nil {
		slog.Error("seed", "err", err)
		os.Exit(1)
	}

	api := httpserver.AuthAPI{Auth: authn.New(pool, cfg)}
	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		slog.Info("auth http listening", "addr", cfg.HTTPAddr)
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
	hs := health.NewServer()
	healthpb.RegisterHealthServer(grpcSrv, hs)
	hs.SetServingStatus("auth.v1.AuthService", healthpb.HealthCheckResponse_SERVING)
	reflection.Register(grpcSrv)
	go func() {
		slog.Info("auth grpc listening", "addr", cfg.GRPCAddr)
		if err := grpcSrv.Serve(lis); err != nil {
			slog.Error("grpc", "err", err)
			stop()
		}
	}()

	<-ctx.Done()
	shCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(shCtx)
	grpcSrv.GracefulStop()
}
