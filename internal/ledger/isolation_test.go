package ledger_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Kaaushikk/real-time-payments-ledger/internal/authn"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/config"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/httpserver"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/ledger"
	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestConcurrentRetryKeyIsolationBetweenUsers(t *testing.T) {
	svc, d, pool := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	before := readIsolationState(t, ctx, pool, d.AliceAccount, d.BobAccount)
	key := uuid.NewString()
	commands := [2]ledger.TransferCmd{
		{IdempotencyKey: key, ActorUserID: d.AliceUser, FromAccountID: d.AliceAccount,
			ToAccountID: d.BobAccount, AmountMinor: 11, Currency: "USD"},
		{IdempotencyKey: key, ActorUserID: d.BobUser, FromAccountID: d.BobAccount,
			ToAccountID: d.AliceAccount, AmountMinor: 7, Currency: "USD"},
	}
	type outcome struct {
		user   int
		result ledger.TransferResult
		err    error
	}
	const retriesPerUser = 8
	start := make(chan struct{})
	results := make(chan outcome, 2*retriesPerUser)
	for user, cmd := range commands {
		for i := 0; i < retriesPerUser; i++ {
			go func(user int, cmd ledger.TransferCmd) {
				<-start
				result, err := svc.Transfer(ctx, cmd)
				results <- outcome{user, result, err}
			}(user, cmd)
		}
	}
	close(start)
	var ids [2]uuid.UUID
	var fresh [2]int
	for i := 0; i < 2*retriesPerUser; i++ {
		got := <-results
		if got.err != nil {
			t.Errorf("user %d: %v", got.user, got.err)
			continue
		}
		if ids[got.user] == uuid.Nil {
			ids[got.user] = got.result.Tx.ID
		}
		if got.result.Tx.ID != ids[got.user] || got.result.Tx.Amount != commands[got.user].AmountMinor {
			t.Errorf("user %d received another posting: %+v", got.user, got.result)
		}
		if !got.result.Replayed {
			fresh[got.user]++
		}
	}
	if ids[0] == uuid.Nil || ids[1] == uuid.Nil || ids[0] == ids[1] || fresh != [2]int{1, 1} {
		t.Fatalf("expected two independent postings: ids=%v fresh=%v", ids, fresh)
	}
	want := before
	want.AliceBalance -= 4
	want.BobBalance += 4
	want.AliceVersion += 2
	want.BobVersion += 2
	want.Receipts += 2
	want.Transfers += 2
	want.JournalEntries += 4
	want.AuditRecords += 4
	want.OutboxEvents += 2
	if got := readIsolationState(t, ctx, pool, d.AliceAccount, d.BobAccount); got != want {
		t.Fatalf("unexpected state after concurrent retries:\ngot  %+v\nwant %+v", got, want)
	}
}

func TestUnauthorizedRetryCannotReplayOrReserveAnotherUsersKey(t *testing.T) {
	svc, d, pool := fixture(t)
	ctx := context.Background()
	legitimate := ledger.TransferCmd{IdempotencyKey: uuid.NewString(), ActorUserID: d.AliceUser,
		FromAccountID: d.AliceAccount, ToAccountID: d.BobAccount, AmountMinor: 23,
		Currency: "USD", Description: "private invoice reference"}
	stolen := legitimate
	stolen.ActorUserID = d.BobUser
	var original ledger.TransferResult
	for _, phase := range []string{"before_owner_posting", "after_owner_posting"} {
		t.Run(phase, func(t *testing.T) {
			before := readIsolationState(t, ctx, pool, d.AliceAccount, d.BobAccount)
			got, err := svc.Transfer(ctx, stolen)
			if !errors.Is(err, ledger.ErrUnauthorized) || got.Replayed || got.Tx.ID != uuid.Nil {
				t.Fatalf("unauthorized request returned a posting: result=%+v err=%v", got, err)
			}
			if after := readIsolationState(t, ctx, pool, d.AliceAccount, d.BobAccount); after != before {
				t.Fatalf("unauthorized request changed state:\nbefore %+v\nafter  %+v", before, after)
			}
		})
		if phase == "before_owner_posting" {
			var err error
			original, err = svc.Transfer(ctx, legitimate)
			if err != nil || original.Replayed {
				t.Fatalf("owner's key was blocked by rejected request: result=%+v err=%v", original, err)
			}
		}
	}
	// Rejection must not consume Bob's own namespace or disturb Alice's receipt.
	own := stolen
	own.FromAccountID, own.ToAccountID = d.BobAccount, d.AliceAccount
	posted, err := svc.Transfer(ctx, own)
	if err != nil || posted.Replayed || posted.Tx.ID == original.Tx.ID {
		t.Fatalf("independent authorized use failed: result=%+v err=%v", posted, err)
	}
	before := readIsolationState(t, ctx, pool, d.AliceAccount, d.BobAccount)
	replay, err := svc.Transfer(ctx, legitimate)
	if err != nil || !replay.Replayed || replay.Tx.ID != original.Tx.ID {
		t.Fatalf("owner receipt changed: result=%+v err=%v", replay, err)
	}
	if after := readIsolationState(t, ctx, pool, d.AliceAccount, d.BobAccount); after != before {
		t.Fatal("owner replay changed financial state")
	}
}

func TestHTTPRetryKeysUseAuthenticatedUser(t *testing.T) {
	svc, d, pool := fixture(t)
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
	h := (httpserver.LedgerAPI{Ledger: svc, Auth: authn.New(pool, cfg)}).Handler()
	key := uuid.NewString()
	aliceToken, bobToken := tokenFor(d.AliceUser), tokenFor(d.BobUser)
	aliceBody := fmt.Sprintf(`{"from_account_id":%q,"to_account_id":%q,"amount_minor":31,"description":"private invoice reference"}`,
		d.AliceAccount.String(), d.BobAccount.String())
	bobBody := fmt.Sprintf(`{"from_account_id":%q,"to_account_id":%q,"amount_minor":13}`,
		d.BobAccount.String(), d.AliceAccount.String())
	post := func(token, body string, wantStatus int) (string, bool) {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, "/v1/transfers", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Idempotency-Key", key)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		if w.Code != wantStatus {
			t.Fatalf("got status %d want %d: %s", w.Code, wantStatus, w.Body.String())
		}
		if wantStatus == http.StatusForbidden {
			if strings.Contains(w.Body.String(), "private invoice reference") || strings.Contains(w.Body.String(), "transaction_id") {
				t.Fatal("rejected request exposed cached transaction details")
			}
			return "", false
		}
		var bodyOut struct {
			Transaction struct {
				ID string `json:"transaction_id"`
			} `json:"transaction"`
			Replayed bool `json:"replayed"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &bodyOut); err != nil {
			t.Fatal(err)
		}
		if id, err := uuid.Parse(bodyOut.Transaction.ID); err != nil || id == uuid.Nil {
			t.Fatal("missing transaction ID")
		}
		return bodyOut.Transaction.ID, bodyOut.Replayed
	}
	post(bobToken, aliceBody, http.StatusForbidden)
	aliceID, replayed := post(aliceToken, aliceBody, http.StatusCreated)
	if replayed {
		t.Fatal("first Alice transfer was replayed")
	}
	post(bobToken, aliceBody, http.StatusForbidden)
	bobID, replayed := post(bobToken, bobBody, http.StatusCreated)
	if replayed || bobID == aliceID {
		t.Fatal("Bob received Alice's posting")
	}
	before := readIsolationState(t, context.Background(), pool, d.AliceAccount, d.BobAccount)
	for _, tc := range []struct{ token, body, id string }{
		{aliceToken, aliceBody, aliceID}, {bobToken, bobBody, bobID},
	} {
		id, replayed := post(tc.token, tc.body, http.StatusOK)
		if id != tc.id || !replayed {
			t.Fatal("HTTP replay returned a different user's posting")
		}
	}
	if after := readIsolationState(t, context.Background(), pool, d.AliceAccount, d.BobAccount); after != before {
		t.Fatal("HTTP replay changed financial state")
	}
}

// Snapshot balances and all posting side effects so rejection/replay checks
// detect orphaned receipts, journal rows, audit records and outbox events.
type isolationState struct {
	AliceBalance, BobBalance, AliceVersion, BobVersion              int64
	Receipts, Transfers, JournalEntries, AuditRecords, OutboxEvents int
}

func readIsolationState(t *testing.T, ctx context.Context, pool *pgxpool.Pool, alice, bob uuid.UUID) isolationState {
	t.Helper()
	var s isolationState
	err := pool.QueryRow(ctx, `SELECT a.balance_minor, b.balance_minor, a.version, b.version,
        (SELECT count(*) FROM idempotency_keys),
        (SELECT count(*) FROM ledger_transactions WHERE type='transfer'),
        (SELECT count(*) FROM journal_entries),
        (SELECT count(*) FROM audit_log),
        (SELECT count(*) FROM outbox)
        FROM accounts a CROSS JOIN accounts b WHERE a.id=$1 AND b.id=$2`, alice, bob).Scan(
		&s.AliceBalance, &s.BobBalance, &s.AliceVersion, &s.BobVersion,
		&s.Receipts, &s.Transfers, &s.JournalEntries, &s.AuditRecords, &s.OutboxEvents)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
