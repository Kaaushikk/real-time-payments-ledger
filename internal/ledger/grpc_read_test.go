package ledger_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	ledgerv1 "github.com/Kaaushikk/real-time-payments-ledger/gen/go/ledger/v1"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/authn"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/config"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/grpcserver"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/ledger"
)

var grpcTestConfig = config.Config{
	JWTSigningKey: "unit-test-only-signing-key-not-a-secret",
	JWTIssuer:     "test", JWTAudience: "ledger-api",
}

func ledgerRPCClient(t *testing.T, svc *ledger.Service) ledgerv1.LedgerServiceClient {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	grpcserver.RegisterLedger(server, svc, authn.New(nil, grpcTestConfig))
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	t.Cleanup(func() { _ = listener.Close() })
	conn, err := grpc.NewClient("passthrough:///ledger-test",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return ledgerv1.NewLedgerServiceClient(conn)
}

func rpcBearer(t *testing.T, subject string) string {
	t.Helper()
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, authn.Claims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject: subject, Issuer: grpcTestConfig.JWTIssuer,
			Audience:  jwt.ClaimStrings{grpcTestConfig.JWTAudience},
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(5 * time.Minute)),
		},
	}).SignedString([]byte(grpcTestConfig.JWTSigningKey))
	if err != nil {
		t.Fatal(err)
	}
	return "Bearer " + token
}

func TestLedgerReadRPCsRejectInvalidIdentity(t *testing.T) {
	client := ledgerRPCClient(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	valid := rpcBearer(t, uuid.NewString())
	for _, tc := range []struct {
		name   string
		values []string
	}{
		{name: "missing"},
		{name: "invalid", values: []string{"Bearer invalid"}},
		{name: "non-UUID subject", values: []string{rpcBearer(t, "not-a-user")}},
		{name: "duplicate", values: []string{valid, valid}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requestCtx := ctx
			if len(tc.values) > 0 {
				requestCtx = metadata.NewOutgoingContext(ctx, metadata.MD{"authorization": tc.values})
			}
			_, accountErr := client.GetAccount(requestCtx, &ledgerv1.GetAccountRequest{})
			_, txErr := client.GetTransaction(requestCtx, &ledgerv1.GetTransactionRequest{})
			_, auditErr := client.ListAuditLog(requestCtx, &ledgerv1.ListAuditLogRequest{})
			for _, err := range []error{accountErr, txErr, auditErr} {
				if status.Code(err) != codes.Unauthenticated {
					t.Fatalf("expected authentication rejection before reading, got %v", err)
				}
			}
		})
	}
}

func TestLedgerReadRPCsScopeRecordsToCaller(t *testing.T) {
	svc, d, pool := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var outsider uuid.UUID
	if err := pool.QueryRow(ctx, "INSERT INTO users (email, password_hash) VALUES ('outsider@example.test','unused-test-hash') RETURNING id").Scan(&outsider); err != nil {
		t.Fatal(err)
	}
	const amount = 27
	posted, err := svc.Transfer(ctx, ledger.TransferCmd{
		IdempotencyKey: uuid.NewString(), ActorUserID: d.AliceUser,
		FromAccountID: d.AliceAccount, ToAccountID: d.BobAccount, AmountMinor: amount,
	})
	if err != nil {
		t.Fatal(err)
	}
	client := ledgerRPCClient(t, svc)
	alice := metadata.AppendToOutgoingContext(ctx, "authorization", rpcBearer(t, d.AliceUser.String()))
	bob := metadata.AppendToOutgoingContext(ctx, "authorization", rpcBearer(t, d.BobUser.String()))
	other := metadata.AppendToOutgoingContext(ctx, "authorization", rpcBearer(t, outsider.String()))

	accountID, transactionID := d.AliceAccount.String(), posted.Tx.ID.String()
	a, err := client.GetAccount(alice, &ledgerv1.GetAccountRequest{AccountId: accountID})
	if err != nil || a.GetAccountId() != accountID || a.GetOwnerUserId() != d.AliceUser.String() || a.GetCurrency() != "USD" {
		t.Fatalf("owner account read: %v, %v", a, err)
	}
	for _, participant := range []context.Context{alice, bob} {
		tx, err := client.GetTransaction(participant, &ledgerv1.GetTransactionRequest{TransactionId: transactionID})
		if err != nil || tx.GetTransactionId() != transactionID || tx.GetAmountMinor() != amount || len(tx.GetEntries()) != 2 {
			t.Fatalf("participant transaction read: %v, %v", tx, err)
		}
	}
	audit, err := client.ListAuditLog(alice, &ledgerv1.ListAuditLogRequest{AccountId: accountID, Limit: 1})
	if err != nil || len(audit.GetEvents()) != 1 {
		t.Fatalf("limited audit read: %v, %v", audit, err)
	}
	event := audit.GetEvents()[0]
	if event.GetTransactionId() != transactionID || event.GetBalanceAfter() != a.GetBalanceMinor() ||
		event.GetBalanceBefore()-event.GetBalanceAfter() != amount {
		t.Fatalf("unexpected audit record: %v", event)
	}

	for _, tc := range []struct {
		name, id string
		call     func(context.Context, string) error
	}{
		{"account", accountID, func(ctx context.Context, id string) error {
			_, err := client.GetAccount(ctx, &ledgerv1.GetAccountRequest{AccountId: id})
			return err
		}},
		{"transaction", transactionID, func(ctx context.Context, id string) error {
			_, err := client.GetTransaction(ctx, &ledgerv1.GetTransactionRequest{TransactionId: id})
			return err
		}},
		{"audit", accountID, func(ctx context.Context, id string) error {
			_, err := client.ListAuditLog(ctx, &ledgerv1.ListAuditLogRequest{AccountId: id})
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.call(alice, "invalid-id"); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("invalid ID: %v", err)
			}
			missing := status.Convert(tc.call(other, uuid.NewString()))
			hidden := status.Convert(tc.call(other, tc.id))
			if missing.Code() != codes.NotFound || hidden.Code() != codes.NotFound || hidden.Message() != missing.Message() {
				t.Fatalf("missing and hidden reads must be indistinguishable: missing=%v hidden=%v", missing, hidden)
			}
		})
	}
}
