package grpcserver

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	ledgerv1 "github.com/Kaaushikk/real-time-payments-ledger/gen/go/ledger/v1"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/authn"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/ledger"
)

type ledgerServer struct {
	ledgerv1.UnimplementedLedgerServiceServer
	ledger *ledger.Service
	auth   *authn.Service
}

func RegisterLedger(server grpc.ServiceRegistrar, service *ledger.Service, auth *authn.Service) {
	ledgerv1.RegisterLedgerServiceServer(server, &ledgerServer{ledger: service, auth: auth})
}

func (s *ledgerServer) caller(ctx context.Context) (uuid.UUID, error) {
	values := metadata.ValueFromIncomingContext(ctx, "authorization")
	if len(values) != 1 {
		return uuid.Nil, status.Error(codes.Unauthenticated, "one bearer token is required")
	}
	token := authn.ParseBearer(values[0])
	if token == "" {
		return uuid.Nil, status.Error(codes.Unauthenticated, "a bearer token is required")
	}
	claims, err := s.auth.ParseAccess(token)
	if err != nil {
		return uuid.Nil, status.Error(codes.Unauthenticated, "invalid access token")
	}
	caller, err := uuid.Parse(claims.Subject)
	if err != nil || caller == uuid.Nil {
		return uuid.Nil, status.Error(codes.Unauthenticated, "invalid access token")
	}
	return caller, nil
}

func (s *ledgerServer) GetAccount(ctx context.Context, req *ledgerv1.GetAccountRequest) (*ledgerv1.Account, error) {
	caller, err := s.caller(ctx)
	if err != nil {
		return nil, err
	}
	id, err := parseID(req.GetAccountId(), "account_id")
	if err != nil {
		return nil, err
	}
	a, err := s.ledger.GetOwnedAccount(ctx, id, caller)
	if err != nil {
		return nil, ledgerError(err, "ledger read failed")
	}
	return &ledgerv1.Account{
		AccountId: a.ID.String(), OwnerUserId: a.OwnerUserID.String(),
		Currency: a.Currency, BalanceMinor: a.Balance, Version: a.Version, AccountType: string(a.Type),
	}, nil
}

func (s *ledgerServer) GetTransaction(ctx context.Context, req *ledgerv1.GetTransactionRequest) (*ledgerv1.Transaction, error) {
	caller, err := s.caller(ctx)
	if err != nil {
		return nil, err
	}
	id, err := parseID(req.GetTransactionId(), "transaction_id")
	if err != nil {
		return nil, err
	}
	tx, err := s.ledger.GetParticipantTransaction(ctx, id, caller)
	if err != nil {
		return nil, ledgerError(err, "ledger read failed")
	}
	return transactionMessage(tx), nil
}

func (s *ledgerServer) Transfer(ctx context.Context, req *ledgerv1.TransferRequest) (*ledgerv1.TransferResponse, error) {
	caller, err := s.caller(ctx)
	if err != nil {
		return nil, err
	}
	from, err := parseID(req.GetFromAccountId(), "from_account_id")
	if err != nil {
		return nil, err
	}
	to, err := parseID(req.GetToAccountId(), "to_account_id")
	if err != nil {
		return nil, err
	}
	key := req.GetIdempotencyKey()
	header := metadata.ValueFromIncomingContext(ctx, "idempotency-key")
	switch len(header) {
	case 0:
	case 1:
		if header[0] == "" || (key != "" && key != header[0]) {
			return nil, status.Error(codes.InvalidArgument, "idempotency header and request key must agree")
		}
		key = header[0]
	default:
		return nil, status.Error(codes.InvalidArgument, "send at most one idempotency header")
	}
	result, err := s.ledger.Transfer(ctx, ledger.TransferCmd{
		ActorUserID: caller, IdempotencyKey: key,
		FromAccountID: from, ToAccountID: to, AmountMinor: req.GetAmountMinor(),
		Currency: req.GetCurrency(), Description: req.GetDescription(),
	})
	if err != nil {
		return nil, transferError(err)
	}
	return &ledgerv1.TransferResponse{
		Transaction: transactionMessage(result.Tx), Replayed: result.Replayed,
	}, nil
}

func transactionMessage(tx ledger.Transaction) *ledgerv1.Transaction {
	entries := make([]*ledgerv1.JournalEntry, 0, len(tx.Entries))
	for _, entry := range tx.Entries {
		entries = append(entries, &ledgerv1.JournalEntry{
			EntryId: entry.ID.String(), AccountId: entry.AccountID.String(),
			Direction: entry.Direction, AmountMinor: entry.Amount,
		})
	}
	return &ledgerv1.Transaction{
		TransactionId: tx.ID.String(), Type: tx.Type, Status: tx.Status,
		AmountMinor: tx.Amount, Currency: tx.Currency, Description: tx.Description,
		Entries: entries, CreatedAt: tx.CreatedAt.Format(time.RFC3339Nano),
	}
}

func (s *ledgerServer) ListAuditLog(ctx context.Context, req *ledgerv1.ListAuditLogRequest) (*ledgerv1.ListAuditLogResponse, error) {
	caller, err := s.caller(ctx)
	if err != nil {
		return nil, err
	}
	id, err := parseID(req.GetAccountId(), "account_id")
	if err != nil {
		return nil, err
	}
	if _, err := s.ledger.GetOwnedAccount(ctx, id, caller); err != nil {
		return nil, ledgerError(err, "ledger read failed")
	}
	events, err := s.ledger.ListAudit(ctx, id, int(req.GetLimit()))
	if err != nil {
		return nil, ledgerError(err, "ledger read failed")
	}
	out := &ledgerv1.ListAuditLogResponse{Events: make([]*ledgerv1.AuditEvent, 0, len(events))}
	for _, event := range events {
		record := &ledgerv1.AuditEvent{
			Id: event.ID, AccountId: event.AccountID.String(), EventType: event.EventType,
			BalanceBefore: event.BalanceBefore, BalanceAfter: event.BalanceAfter,
			AmountMinor: event.AmountMinor, CreatedAt: event.CreatedAt.Format(time.RFC3339Nano),
		}
		if event.TransactionID != nil {
			record.TransactionId = event.TransactionID.String()
		}
		out.Events = append(out.Events, record)
	}
	return out, nil
}

func parseID(value, field string) (uuid.UUID, error) {
	id, err := uuid.Parse(value)
	if err != nil || id == uuid.Nil {
		return uuid.Nil, status.Errorf(codes.InvalidArgument, "%s must be a nonzero UUID", field)
	}
	return id, nil
}

func ledgerError(err error, message string) error {
	switch {
	case errors.Is(err, ledger.ErrAccountNotFound):
		return status.Error(codes.NotFound, "resource not found")
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "request canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "request deadline exceeded")
	default:
		return status.Error(codes.Internal, message)
	}
}

func transferError(err error) error {
	switch {
	case errors.Is(err, ledger.ErrUnauthorized):
		return status.Error(codes.PermissionDenied, "transfer is not permitted")
	case errors.Is(err, ledger.ErrIdempotencyConflict):
		return status.Error(codes.AlreadyExists, ledger.ErrIdempotencyConflict.Error())
	case errors.Is(err, ledger.ErrIdempotencyInFlight):
		return status.Error(codes.Aborted, ledger.ErrIdempotencyInFlight.Error())
	case errors.Is(err, ledger.ErrInsufficientFunds), errors.Is(err, ledger.ErrNegativeBalance),
		errors.Is(err, ledger.ErrAmountOverflow):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, ledger.ErrInvalidTransfer), errors.Is(err, ledger.ErrSameAccount),
		errors.Is(err, ledger.ErrCurrencyMismatch):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return ledgerError(err, "ledger transfer failed")
	}
}
