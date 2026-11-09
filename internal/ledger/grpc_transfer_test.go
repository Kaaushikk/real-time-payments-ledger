package ledger_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	ledgerv1 "github.com/Kaaushikk/real-time-payments-ledger/gen/go/ledger/v1"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/authn"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/httpserver"
)

func TestTransferRPCSharesDurableReceiptWithHTTP(t *testing.T) {
	svc, d, pool := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	client := ledgerRPCClient(t, svc)
	aliceBearer := rpcBearer(t, d.AliceUser.String())
	alice := metadata.AppendToOutgoingContext(ctx, "authorization", aliceBearer)
	bob := metadata.AppendToOutgoingContext(ctx, "authorization", rpcBearer(t, d.BobUser.String()))
	const amount = 27
	req := &ledgerv1.TransferRequest{
		IdempotencyKey: uuid.NewString(),
		FromAccountId:  d.AliceAccount.String(), ToAccountId: d.BobAccount.String(),
		AmountMinor: amount, Description: "shared receipt",
	}
	before := readIsolationState(t, ctx, pool, d.AliceAccount, d.BobAccount)
	first, err := client.Transfer(alice, req)
	if err != nil || first.GetReplayed() {
		t.Fatalf("initial posting: response=%v err=%v", first, err)
	}
	tx := first.GetTransaction()
	if tx.GetTransactionId() == "" || tx.GetAmountMinor() != amount || tx.GetCurrency() != "USD" ||
		tx.GetDescription() != req.GetDescription() || len(tx.GetEntries()) != 2 {
		t.Fatalf("unexpected transfer response: %v", tx)
	}

	retry := proto.Clone(req).(*ledgerv1.TransferRequest)
	retry.Currency = "USD"
	replayed, err := client.Transfer(alice, retry)
	if err != nil || !replayed.GetReplayed() || !proto.Equal(tx, replayed.GetTransaction()) {
		t.Fatalf("gRPC replay: response=%v err=%v", replayed, err)
	}

	body, err := json.Marshal(map[string]any{
		"from_account_id": req.GetFromAccountId(), "to_account_id": req.GetToAccountId(),
		"amount_minor": amount, "currency": "USD", "description": req.GetDescription(),
	})
	if err != nil {
		t.Fatal(err)
	}
	httpReq := httptest.NewRequest(http.MethodPost, "/v1/transfers", strings.NewReader(string(body))).WithContext(ctx)
	httpReq.Header.Set("Authorization", aliceBearer)
	httpReq.Header.Set("Idempotency-Key", req.GetIdempotencyKey())
	w := httptest.NewRecorder()
	(httpserver.LedgerAPI{Ledger: svc, Auth: authn.New(pool, grpcTestConfig)}).Handler().ServeHTTP(w, httpReq)
	if w.Code != http.StatusOK {
		t.Fatalf("REST replay status=%d body=%s", w.Code, w.Body.String())
	}
	var httpReplay struct {
		Replayed    bool
		Transaction struct {
			ID string `json:"transaction_id"`
		}
	}
	if err := json.Unmarshal(w.Body.Bytes(), &httpReplay); err != nil {
		t.Fatal(err)
	}
	if !httpReplay.Replayed || httpReplay.Transaction.ID != tx.GetTransactionId() {
		t.Fatalf("REST did not recover the same receipt: %+v", httpReplay)
	}

	for _, tc := range []struct {
		name   string
		ctx    context.Context
		change func(*ledgerv1.TransferRequest)
		want   codes.Code
	}{
		{"missing bearer", ctx, nil, codes.Unauthenticated},
		{"other user with same key", bob, nil, codes.PermissionDenied},
		{"changed request", alice, func(r *ledgerv1.TransferRequest) { r.AmountMinor++ }, codes.AlreadyExists},
		{"insufficient funds", alice, func(r *ledgerv1.TransferRequest) {
			r.IdempotencyKey = uuid.NewString()
			r.AmountMinor = before.AliceBalance + 1
		}, codes.FailedPrecondition},
		{"invalid destination", alice, func(r *ledgerv1.TransferRequest) { r.ToAccountId = "bad-id" }, codes.InvalidArgument},
		{"missing destination", alice, func(r *ledgerv1.TransferRequest) {
			r.IdempotencyKey = uuid.NewString()
			r.ToAccountId = uuid.NewString()
		}, codes.NotFound},
		{"missing retry key", alice, func(r *ledgerv1.TransferRequest) { r.IdempotencyKey = "" }, codes.InvalidArgument},
	} {
		t.Run(tc.name, func(t *testing.T) {
			attempt := proto.Clone(req).(*ledgerv1.TransferRequest)
			if tc.change != nil {
				tc.change(attempt)
			}
			if _, err := client.Transfer(tc.ctx, attempt); status.Code(err) != tc.want {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
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
		t.Fatalf("expected exactly one committed posting: got=%+v want=%+v", got, want)
	}
	assertMoneyReconciles(t, ctx, pool)
}
