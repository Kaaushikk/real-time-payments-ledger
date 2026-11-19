package ledger_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/Kaaushikk/real-time-payments-ledger/internal/authn"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/httpserver"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/metrics"
)

func TestTransferMetricsMatchHTTPOutcomes(t *testing.T) {
	svc, demo, pool := fixture(t)
	handler := (httpserver.LedgerAPI{Ledger: svc, Auth: authn.New(pool, grpcTestConfig)}).Handler()
	counter := func(label string) float64 {
		t.Helper()
		var metric dto.Metric
		if err := metrics.Transfers.WithLabelValues(label).Write(&metric); err != nil {
			t.Fatal(err)
		}
		return metric.GetCounter().GetValue()
	}
	count := func() uint64 {
		t.Helper()
		var metric dto.Metric
		if err := metrics.TransferLatency.(prometheus.Metric).Write(&metric); err != nil {
			t.Fatal(err)
		}
		return metric.GetHistogram().GetSampleCount()
	}
	beforeCount := count()
	key := uuid.NewString()
	for _, tc := range []struct {
		name   string
		key    string
		amount int64
		status int
	}{
		{"posted", key, 13, http.StatusCreated},
		{"replayed", key, 13, http.StatusOK},
		{"invalid", uuid.NewString(), 0, http.StatusBadRequest},
		{"error", uuid.NewString(), 1 << 60, http.StatusConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before := counter(tc.name)
			body, err := json.Marshal(map[string]any{
				"from_account_id": demo.AliceAccount.String(),
				"to_account_id":   demo.BobAccount.String(),
				"amount_minor":    tc.amount, "currency": "USD",
			})
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest(http.MethodPost, "/v1/transfers", strings.NewReader(string(body)))
			req.Header.Set("Authorization", rpcBearer(t, demo.AliceUser.String()))
			req.Header.Set("Idempotency-Key", tc.key)
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, req)
			if response.Code != tc.status {
				t.Fatalf("status=%d want=%d body=%s", response.Code, tc.status, response.Body.String())
			}
			if got := counter(tc.name); got != before+1 {
				t.Fatalf("counter changed from %g to %g, want one request", before, got)
			}
		})
	}
	if got := count(); got != beforeCount+4 {
		t.Fatalf("latency observations=%d, want %d", got, beforeCount+4)
	}
}

func TestOutboxMetricsReflectDatabaseAndCollectionFailure(t *testing.T) {
	_, _, pool := fixture(t)
	registry := prometheus.NewRegistry()
	registry.MustRegister(metrics.NewOutboxCollector(pool))
	values := func() map[string]float64 {
		t.Helper()
		families, err := registry.Gather()
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]float64{}
		for _, family := range families {
			if len(family.Metric) != 1 {
				t.Fatal("outbox metrics should have no variable labels")
			}
			out[family.GetName()] = family.Metric[0].GetGauge().GetValue()
		}
		return out
	}
	empty := values()
	if empty["ledger_outbox_collection_success"] != 1 || empty["ledger_outbox_pending_events"] != 0 ||
		empty["ledger_outbox_oldest_pending_age_seconds"] != 0 {
		t.Fatalf("empty outbox: %v", empty)
	}
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `INSERT INTO outbox (topic,payload,created_at)
		VALUES ('test','{}',now()-interval '2 minutes'),('test','{}',now())`); err != nil {
		t.Fatal(err)
	}
	pending := values()
	if pending["ledger_outbox_pending_events"] != 2 || pending["ledger_outbox_oldest_pending_age_seconds"] < 120 {
		t.Fatalf("pending outbox: %v", pending)
	}
	if _, err := pool.Exec(ctx, "UPDATE outbox SET published_at=now()"); err != nil {
		t.Fatal(err)
	}
	drained := values()
	if drained["ledger_outbox_pending_events"] != 0 || drained["ledger_outbox_oldest_pending_age_seconds"] != 0 {
		t.Fatalf("drained outbox: %v", drained)
	}
	if _, err := pool.Exec(ctx, "DROP TABLE outbox"); err != nil {
		t.Fatal(err)
	}
	failed := values()
	if len(failed) != 1 || failed["ledger_outbox_collection_success"] != 0 {
		t.Fatalf("collection failure should omit backlog values: %v", failed)
	}
}
