package ledger

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

var (
	ErrInsufficientFunds   = errors.New("insufficient funds")
	ErrSameAccount         = errors.New("source and destination must differ")
	ErrAccountNotFound     = errors.New("account not found")
	ErrCurrencyMismatch    = errors.New("currency mismatch")
	ErrIdempotencyConflict = errors.New("idempotency key reused with a different request")
	ErrIdempotencyInFlight = errors.New("request with this idempotency key is already in flight")
	ErrNegativeBalance     = errors.New("transfer would produce a negative user balance")
	ErrAmountOverflow      = errors.New("destination balance exceeds integer range")
	ErrUnauthorized        = errors.New("unauthorized")
)

type AccountType string

const (
	AccountUser     AccountType = "user"
	AccountIssuance AccountType = "issuance"
)

type Account struct {
	ID          uuid.UUID
	OwnerUserID *uuid.UUID
	Currency    string
	Type        AccountType
	Balance     int64
	Version     int64
}

type JournalEntry struct {
	ID        uuid.UUID
	AccountID uuid.UUID
	Direction string
	Amount    int64
}

type Transaction struct {
	ID          uuid.UUID
	Type        string
	Status      string
	Amount      int64
	Currency    string
	Description string
	Entries     []JournalEntry
	CreatedAt   time.Time
}

type TransferCmd struct {
	IdempotencyKey string
	FromAccountID  uuid.UUID
	ToAccountID    uuid.UUID
	AmountMinor    int64
	Currency       string
	Description    string
	ActorUserID    uuid.UUID
}

func (c TransferCmd) Fingerprint() string {
	payload, _ := json.Marshal(struct {
		From        string `json:"from"`
		To          string `json:"to"`
		Amount      int64  `json:"amount"`
		Ccy         string `json:"ccy"`
		Description string `json:"description"`
	}{c.FromAccountID.String(), c.ToAccountID.String(), c.AmountMinor, c.Currency, c.Description})
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func (c TransferCmd) Validate() error {
	if c.IdempotencyKey == "" || len(c.IdempotencyKey) > 255 {
		return fmt.Errorf("idempotency key is required and must not exceed 255 bytes")
	}
	if c.FromAccountID == c.ToAccountID {
		return ErrSameAccount
	}
	if c.ActorUserID == uuid.Nil {
		return ErrUnauthorized
	}
	if c.FromAccountID == uuid.Nil || c.ToAccountID == uuid.Nil {
		return ErrAccountNotFound
	}
	if c.AmountMinor <= 0 {
		return fmt.Errorf("amount must be positive")
	}
	if c.Currency != "USD" {
		return fmt.Errorf("currency must be USD")
	}
	if len(c.Description) > 500 {
		return fmt.Errorf("description exceeds 500 bytes")
	}
	return nil
}

type AuditEvent struct {
	ID            int64
	AccountID     uuid.UUID
	TransactionID *uuid.UUID
	EventType     string
	BalanceBefore int64
	BalanceAfter  int64
	AmountMinor   int64
	CreatedAt     time.Time
}

type TransferResult struct {
	Tx       Transaction
	Replayed bool
}
