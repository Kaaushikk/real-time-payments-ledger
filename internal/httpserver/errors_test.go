package httpserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Kaaushikk/real-time-payments-ledger/internal/ledger"
	"github.com/google/uuid"
)

func TestValidationErrorsHaveStableHTTPStatus(t *testing.T) {
	base := ledger.TransferCmd{IdempotencyKey: "key", ActorUserID: uuid.New(),
		FromAccountID: uuid.New(), ToAccountID: uuid.New(), AmountMinor: 1, Currency: "USD"}
	tests := []struct {
		name   string
		change func(*ledger.TransferCmd)
	}{
		{"missing key", func(c *ledger.TransferCmd) { c.IdempotencyKey = "" }},
		{"zero amount", func(c *ledger.TransferCmd) { c.AmountMinor = 0 }},
		{"negative amount", func(c *ledger.TransferCmd) { c.AmountMinor = -1 }},
		{"unsupported currency", func(c *ledger.TransferCmd) { c.Currency = "EUR" }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cmd := base
			tc.change(&cmd)
			err := cmd.Validate()
			if !errors.Is(err, ledger.ErrInvalidTransfer) {
				t.Fatalf("not a validation error: %v", err)
			}
			w := httptest.NewRecorder()
			writeLedgerErr(w, fmt.Errorf("validate request: %w", err))
			if w.Code != http.StatusBadRequest {
				t.Fatalf("got %d: %s", w.Code, w.Body.String())
			}
			var body map[string]string
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || body["error"] == "" {
				t.Fatalf("missing JSON error: %s", w.Body.String())
			}
		})
	}
}

func TestErrorTextCannotTurnInternalFailureIntoClientError(t *testing.T) {
	w := httptest.NewRecorder()
	writeLedgerErr(w, errors.New("database failure: description exceeds backend capacity"))
	if w.Code != http.StatusInternalServerError || w.Body.String() != "{\"error\":\"internal server error\"}\n" {
		t.Fatalf("unexpected status/body: %d %s", w.Code, w.Body.String())
	}
}
