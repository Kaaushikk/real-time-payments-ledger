package ledger

import (
	"github.com/google/uuid"
	"math"
	"strings"
	"testing"
	"testing/quick"
)

func TestTransferValidationBoundaryCases(t *testing.T) {
	base := TransferCmd{IdempotencyKey: "key", ActorUserID: uuid.New(), FromAccountID: uuid.New(), ToAccountID: uuid.New(), AmountMinor: 1, Currency: "USD"}
	cases := []struct {
		name   string
		change func(*TransferCmd)
	}{
		{"missing actor", func(c *TransferCmd) { c.ActorUserID = uuid.Nil }},
		{"missing source", func(c *TransferCmd) { c.FromAccountID = uuid.Nil }},
		{"zero", func(c *TransferCmd) { c.AmountMinor = 0 }},
		{"negative", func(c *TransferCmd) { c.AmountMinor = math.MinInt64 }},
		{"currency", func(c *TransferCmd) { c.Currency = "EUR" }},
		{"key length", func(c *TransferCmd) { c.IdempotencyKey = strings.Repeat("x", 256) }},
		{"description length", func(c *TransferCmd) { c.Description = strings.Repeat("x", 501) }},
	}
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := base
			tc.change(&c)
			if c.Validate() == nil {
				t.Fatal("accepted invalid transfer")
			}
		})
	}
}

func TestFingerprintChangesWithAmount(t *testing.T) {
	if err := quick.Check(func(n uint32) bool {
		a := TransferCmd{AmountMinor: int64(n), Currency: "USD"}
		b := a
		b.AmountMinor++
		return a.Fingerprint() != b.Fingerprint()
	}, &quick.Config{MaxCount: 200}); err != nil {
		t.Fatal(err)
	}
}
