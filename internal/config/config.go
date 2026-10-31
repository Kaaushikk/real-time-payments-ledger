package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	ServiceName    string
	HTTPAddr       string
	GRPCAddr       string
	DatabaseURL    string
	RedisAddr      string
	RedisPassword  string
	KafkaBrokers   string
	KafkaTopic     string
	OTLPEndpoint   string
	JWTIssuer      string
	JWTAudience    string
	JWTSigningKey  string
	AccessTTL      time.Duration
	RefreshTTL     time.Duration
	IdempotencyTTL time.Duration
	AuthPublicURL  string
}

func Load(service string) Config {
	return Config{
		ServiceName:    service,
		HTTPAddr:       env("HTTP_ADDR", ":8080"),
		GRPCAddr:       env("GRPC_ADDR", ":9090"),
		DatabaseURL:    env("DATABASE_URL", "postgres://ledger:ledger@localhost:5432/ledger?sslmode=disable"),
		RedisAddr:      env("REDIS_ADDR", "localhost:6379"),
		RedisPassword:  env("REDIS_PASSWORD", ""),
		KafkaBrokers:   env("KAFKA_BROKERS", "localhost:9092"),
		KafkaTopic:     env("KAFKA_TOPIC", "ledger.transactions"),
		OTLPEndpoint:   env("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317"),
		JWTIssuer:      env("JWT_ISSUER", "http://localhost:8081"),
		JWTAudience:    env("JWT_AUDIENCE", "ledger-api"),
		JWTSigningKey:  env("JWT_SIGNING_KEY", "dev-only-change-me-32bytes-minimum!!"),
		AccessTTL:      envDuration("ACCESS_TTL", 5*time.Minute),
		RefreshTTL:     envDuration("REFRESH_TTL", 7*24*time.Hour),
		IdempotencyTTL: envDuration("IDEMPOTENCY_TTL", 24*time.Hour),
		AuthPublicURL:  env("AUTH_PUBLIC_URL", "http://localhost:8081"),
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
		if n, err := strconv.Atoi(v); err == nil {
			return time.Duration(n) * time.Second
		}
	}
	return fallback
}
