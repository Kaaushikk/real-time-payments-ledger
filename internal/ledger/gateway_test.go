package ledger_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	ledgerv1 "github.com/Kaaushikk/real-time-payments-ledger/gen/go/ledger/v1"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/authn"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/httpserver"
)

func TestGeneratedGatewayRoutesAndReceipt(t *testing.T) {
	svc, d, pool := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn := ledgerRPCConn(t, svc)
	legacy := (httpserver.LedgerAPI{Ledger: svc, Auth: authn.New(pool, grpcTestConfig)}).Handler()
	handler, err := httpserver.WithLedgerGateway(ctx, conn, legacy)
	if err != nil {
		t.Fatal(err)
	}
	alice := rpcBearer(t, d.AliceUser.String())
	bob := rpcBearer(t, d.BobUser.String())
	send := func(method, path, bearer, body string, keys ...string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(body)).WithContext(ctx)
		if bearer != "" {
			req.Header.Set("Authorization", bearer)
		}
		req.Header.Set("Content-Type", "application/json")
		for _, key := range keys {
			req.Header.Add("Idempotency-Key", key)
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, req)
		return w
	}
	decode := func(w *httptest.ResponseRecorder, response proto.Message) {
		t.Helper()
		if w.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
		}
		if err := protojson.Unmarshal(w.Body.Bytes(), response); err != nil {
			t.Fatal(err)
		}
	}

	const transfers = "/gateway/v1/transfers"
	const amount = 31
	req := &ledgerv1.TransferRequest{
		FromAccountId: d.AliceAccount.String(), ToAccountId: d.BobAccount.String(),
		AmountMinor: amount, Currency: "USD",
	}
	body := func() string {
		t.Helper()
		out, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(req)
		if err != nil {
			t.Fatal(err)
		}
		return string(out)
	}
	before := readIsolationState(t, ctx, pool, d.AliceAccount, d.BobAccount)
	key := uuid.NewString()
	headerOnly := body()
	first := new(ledgerv1.TransferResponse)
	decode(send(http.MethodPost, transfers, alice, headerOnly, key), first)
	txID := first.GetTransaction().GetTransactionId()
	if first.GetReplayed() || txID == "" || first.GetTransaction().GetAmountMinor() != amount {
		t.Fatalf("gateway posting: %v", first)
	}

	req.IdempotencyKey = key
	client := ledgerv1.NewLedgerServiceClient(conn)
	replayed, err := client.Transfer(metadata.AppendToOutgoingContext(ctx, "authorization", alice), req)
	if err != nil || !replayed.GetReplayed() || replayed.GetTransaction().GetTransactionId() != txID {
		t.Fatalf("gRPC replay of gateway posting: %v, %v", replayed, err)
	}
	replayBody := body()
	second := new(ledgerv1.TransferResponse)
	decode(send(http.MethodPost, transfers, alice, replayBody, key), second)
	if !second.GetReplayed() || second.GetTransaction().GetTransactionId() != txID {
		t.Fatalf("gateway replay: %v", second)
	}

	accountPath := "/gateway/v1/accounts/" + d.AliceAccount.String()
	account := new(ledgerv1.Account)
	decode(send(http.MethodGet, accountPath, alice, ""), account)
	if account.GetAccountId() != d.AliceAccount.String() || account.GetBalanceMinor() != before.AliceBalance-amount {
		t.Fatalf("gateway account: %v", account)
	}
	transaction := new(ledgerv1.Transaction)
	decode(send(http.MethodGet, "/gateway/v1/transactions/"+txID, bob, ""), transaction)
	if transaction.GetTransactionId() != txID || len(transaction.GetEntries()) != 2 {
		t.Fatalf("gateway transaction: %v", transaction)
	}
	audit := new(ledgerv1.ListAuditLogResponse)
	decode(send(http.MethodGet, accountPath+"/audit?limit=1", alice, ""), audit)
	if len(audit.GetEvents()) != 1 || audit.GetEvents()[0].GetTransactionId() != txID {
		t.Fatalf("gateway audit: %v", audit)
	}

	req.AmountMinor++
	conflictingBody := body()
	for _, tc := range []struct {
		name, method, path, bearer, body string
		keys                             []string
		want                             int
	}{
		{"missing bearer", http.MethodGet, accountPath, "", "", nil, http.StatusUnauthorized},
		{"other owner", http.MethodGet, accountPath, bob, "", nil, http.StatusNotFound},
		{"other spender", http.MethodPost, transfers, bob, headerOnly, []string{key}, http.StatusForbidden},
		{"changed request", http.MethodPost, transfers, alice, conflictingBody, []string{key}, http.StatusConflict},
		{"conflicting key sources", http.MethodPost, transfers, alice, replayBody, []string{uuid.NewString()}, http.StatusBadRequest},
		{"invalid header characters", http.MethodPost, transfers, alice, replayBody, []string{"caf\u00e9"}, http.StatusBadRequest},
		{"duplicate headers", http.MethodPost, transfers, alice, headerOnly, []string{key, key}, http.StatusBadRequest},
		{"unknown field", http.MethodPost, transfers, alice, "{\"unknown\":true}", []string{key}, http.StatusBadRequest},
		{"oversized body", http.MethodPost, transfers, alice, strings.Repeat(" ", 16<<10) + headerOnly, []string{key}, http.StatusBadRequest},
		{"legacy health", http.MethodGet, "/healthz", "", "", nil, http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := send(tc.method, tc.path, tc.bearer, tc.body, tc.keys...)
			if w.Code != tc.want {
				t.Fatalf("status=%d want=%d body=%s", w.Code, tc.want, w.Body.String())
			}
		})
	}

	prefixed := httptest.NewRequest(http.MethodGet, accountPath, nil).WithContext(ctx)
	prefixed.Header.Set("Grpc-Metadata-Authorization", alice)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, prefixed)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("gateway must require the normal bearer header: status=%d", w.Code)
	}

	want := before
	want.AliceBalance -= amount
	want.BobBalance += amount
	want.AliceVersion++
	want.BobVersion++
	want.Receipts++
	want.Transfers++
	want.JournalEntries += 2
	want.AuditRecords += 2
	want.OutboxEvents++
	if got := readIsolationState(t, ctx, pool, d.AliceAccount, d.BobAccount); got != want {
		t.Fatalf("expected exactly one posting: got=%+v want=%+v", got, want)
	}
	assertMoneyReconciles(t, ctx, pool)
}
