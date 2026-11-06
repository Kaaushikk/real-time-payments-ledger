package ledger_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/Kaaushikk/real-time-payments-ledger/internal/authn"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/config"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/httpserver"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/idempotency"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/ledger"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/seed"
	"github.com/Kaaushikk/real-time-payments-ledger/migrations"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestConcurrentFreshMigrationsAndRepeatedApplyPreservePostings(t *testing.T) {
	pool := unmigratedPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// This schema has no tables yet. All callers start from the same gate.
	const instances = 8
	start := make(chan struct{})
	results := make(chan error, instances)
	for i := 0; i < instances; i++ {
		go func() { <-start; results <- migrations.Apply(ctx, pool) }()
	}
	close(start)
	for i := 0; i < instances; i++ {
		if err := <-results; err != nil {
			t.Errorf("concurrent migration: %v", err)
		}
	}
	if t.Failed() {
		t.FailNow()
	}
	first := migrationRecords(t, ctx, pool)
	files, err := migrations.Files.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	expected := 0
	for _, file := range files {
		if file.IsDir() || !strings.HasSuffix(file.Name(), ".sql") {
			continue
		}
		expected++
		if _, ok := first[file.Name()]; !ok {
			t.Fatalf("missing migration %s", file.Name())
		}
	}
	if expected == 0 || len(first) != expected {
		t.Fatalf("migration records=%d embedded SQL files=%d", len(first), expected)
	}
	d, err := seed.DemoData(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	svc := ledger.New(pool, idempotency.New(nil, pool, 24*time.Hour), "ledger.transactions")
	cmd := ledger.TransferCmd{IdempotencyKey: uuid.NewString(), ActorUserID: d.AliceUser,
		FromAccountID: d.AliceAccount, ToAccountID: d.BobAccount, AmountMinor: 19, Currency: "USD"}
	posted, err := svc.Transfer(ctx, cmd)
	if err != nil {
		t.Fatal(err)
	}
	before := readIsolationState(t, ctx, pool, d.AliceAccount, d.BobAccount)
	for i := 0; i < 3; i++ {
		if err := migrations.Apply(ctx, pool); err != nil {
			t.Fatal(err)
		}
	}
	if got := migrationRecords(t, ctx, pool); !reflect.DeepEqual(got, first) {
		t.Fatalf("repeated apply replaced migration records: before=%v after=%v", first, got)
	}
	replay, err := svc.Transfer(ctx, cmd)
	if err != nil || !replay.Replayed || replay.Tx.ID != posted.Tx.ID {
		t.Fatalf("migration lost original receipt: result=%+v err=%v", replay, err)
	}
	if got := readIsolationState(t, ctx, pool, d.AliceAccount, d.BobAccount); got != before {
		t.Fatalf("repeat migrations/replay changed ledger: before=%+v after=%+v", before, got)
	}
	assertMoneyReconciles(t, ctx, pool)
}

func migrationRecords(t *testing.T, ctx context.Context, pool *pgxpool.Pool) map[string]time.Time {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT filename,applied_at FROM schema_migrations`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	records := make(map[string]time.Time)
	for rows.Next() {
		var name string
		var applied time.Time
		if err := rows.Scan(&name, &applied); err != nil {
			t.Fatal(err)
		}
		records[name] = applied
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return records
}

// Emulates a response-body transport failure after the service commits. This
// is an in-process HTTP test, not a TCP outage or a process-crash experiment.
type droppedResponse struct {
	header         http.Header
	status, writes int
}

func (w *droppedResponse) Header() http.Header         { return w.header }
func (w *droppedResponse) WriteHeader(status int)      { w.status = status }
func (w *droppedResponse) Write(_ []byte) (int, error) { w.writes++; return 0, io.ErrClosedPipe }

func TestWeekOneHTTPAcceptanceAfterLostResponse(t *testing.T) {
	svc, d, pool := fixture(t)
	ctx := context.Background()
	cfg := config.Config{JWTSigningKey: "unit-test-only-signing-key-not-a-secret", JWTIssuer: "test", JWTAudience: "ledger-api"}
	tokenFor := func(user uuid.UUID) string {
		t.Helper()
		token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, authn.Claims{RegisteredClaims: jwt.RegisteredClaims{
			Subject: user.String(), Issuer: cfg.JWTIssuer, Audience: jwt.ClaimStrings{cfg.JWTAudience},
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(5 * time.Minute)),
		}}).SignedString([]byte(cfg.JWTSigningKey))
		if err != nil {
			t.Fatal(err)
		}
		return token
	}
	aliceToken, bobToken := tokenFor(d.AliceUser), tokenFor(d.BobUser)
	key := uuid.NewString()
	body := func(amount int64) string {
		return fmt.Sprintf(`{"from_account_id":%q,"to_account_id":%q,"amount_minor":%d,"currency":"USD"}`,
			d.AliceAccount.String(), d.BobAccount.String(), amount)
	}
	request := func(method, path, token, retryKey, content string) *http.Request {
		req := httptest.NewRequest(method, path, strings.NewReader(content))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Idempotency-Key", retryKey)
		return req
	}
	h := (httpserver.LedgerAPI{Ledger: svc, Auth: authn.New(pool, cfg)}).Handler()
	before := readIsolationState(t, ctx, pool, d.AliceAccount, d.BobAccount)
	lost := &droppedResponse{header: make(http.Header)}
	h.ServeHTTP(lost, request(http.MethodPost, "/v1/transfers", aliceToken, key, body(19)))
	if lost.status != http.StatusCreated || lost.writes == 0 {
		t.Fatalf("expected committed posting followed by a failed response write: %+v", lost)
	}
	want := before
	want.AliceBalance -= 19
	want.BobBalance += 19
	want.AliceVersion++
	want.BobVersion++
	want.Receipts++
	want.Transfers++
	want.JournalEntries += 2
	want.AuditRecords += 2
	want.OutboxEvents++
	if got := readIsolationState(t, ctx, pool, d.AliceAccount, d.BobAccount); got != want {
		t.Fatalf("lost response posting: got=%+v want=%+v", got, want)
	}
	var postedID uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT id FROM ledger_transactions WHERE type='transfer'`).Scan(&postedID); err != nil {
		t.Fatal(err)
	}
	// New service/store objects must recover the persisted receipt without
	// depending on the original service instance or Redis.
	freshService := ledger.New(pool, idempotency.New(nil, pool, 24*time.Hour), "ledger.transactions")
	h = (httpserver.LedgerAPI{Ledger: freshService, Auth: authn.New(pool, cfg)}).Handler()
	for _, tc := range []struct {
		name, token, key string
		amount           int64
		status           int
	}{
		{"retry recovers original", aliceToken, key, 19, http.StatusOK},
		{"changed request conflicts", aliceToken, key, 20, http.StatusUnprocessableEntity},
		{"another user cannot spend", bobToken, key, 19, http.StatusForbidden},
		{"overspending rejects", aliceToken, uuid.NewString(), before.AliceBalance + 1, http.StatusConflict},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			h.ServeHTTP(w, request(http.MethodPost, "/v1/transfers", tc.token, tc.key, body(tc.amount)))
			if w.Code != tc.status {
				t.Fatalf("status=%d want=%d body=%s", w.Code, tc.status, w.Body.String())
			}
			if tc.status == http.StatusOK {
				var result struct {
					Replayed    bool `json:"replayed"`
					Transaction struct {
						ID uuid.UUID `json:"transaction_id"`
					} `json:"transaction"`
				}
				if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
					t.Fatal(err)
				}
				if !result.Replayed || result.Transaction.ID != postedID {
					t.Fatal("retry did not recover original transaction")
				}
			}
			if got := readIsolationState(t, ctx, pool, d.AliceAccount, d.BobAccount); got != want {
				t.Fatalf("retry/rejection changed state: got=%+v want=%+v", got, want)
			}
		})
	}
	audit := httptest.NewRecorder()
	h.ServeHTTP(audit, request(http.MethodGet, "/v1/accounts/"+d.AliceAccount.String()+"/audit", aliceToken, "", ""))
	if audit.Code != http.StatusOK {
		t.Fatalf("audit lookup: %d %s", audit.Code, audit.Body.String())
	}
	var history struct {
		Events []struct {
			ID     uuid.UUID `json:"transaction_id"`
			Before int64     `json:"balance_before"`
			After  int64     `json:"balance_after"`
		} `json:"events"`
	}
	if err := json.Unmarshal(audit.Body.Bytes(), &history); err != nil {
		t.Fatal(err)
	}
	matches := 0
	for _, event := range history.Events {
		if event.ID == postedID {
			matches++
			if event.Before != before.AliceBalance || event.After != want.AliceBalance {
				t.Fatal("audit balances disagree with posting")
			}
		}
	}
	if matches != 1 {
		t.Fatalf("audit records for transfer=%d want=1", matches)
	}
	assertMoneyReconciles(t, ctx, pool)
}
