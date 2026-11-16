package events

import (
	"time"

	"github.com/google/uuid"
)

const (
	TransactionPostedType    = "ledger.transaction.posted"
	TransactionPostedVersion = 1
)

// TransactionPosted is the Kafka JSON contract for a committed transfer.
type TransactionPosted struct {
	EventID       uuid.UUID `json:"event_id"`
	EventType     string    `json:"event_type"`
	SchemaVersion int       `json:"schema_version"`
	TransactionID uuid.UUID `json:"transaction_id"`
	FromAccountID uuid.UUID `json:"from_account_id"`
	ToAccountID   uuid.UUID `json:"to_account_id"`
	AmountMinor   int64     `json:"amount_minor"`
	Currency      string    `json:"currency"`
	OccurredAt    time.Time `json:"occurred_at"`
}
