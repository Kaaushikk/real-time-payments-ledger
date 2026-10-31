package ledger

import (
	"testing"
	"testing/quick"
)

func TestTransferCmdValidate(t *testing.T) {
	cmd := TransferCmd{IdempotencyKey: "k", AmountMinor: 1, Currency: "USD"}
	cmd.FromAccountID = [16]byte{1}
	cmd.ToAccountID = [16]byte{1}
	if err := cmd.Validate(); err != ErrSameAccount {
		t.Fatalf("expected same account, got %v", err)
	}
}

func TestFingerprintStable(t *testing.T) {
	a := TransferCmd{FromAccountID: [16]byte{1}, ToAccountID: [16]byte{2}, AmountMinor: 50, Currency: "USD"}
	b := TransferCmd{FromAccountID: [16]byte{1}, ToAccountID: [16]byte{2}, AmountMinor: 50, Currency: "USD"}
	c := TransferCmd{FromAccountID: [16]byte{1}, ToAccountID: [16]byte{2}, AmountMinor: 51, Currency: "USD"}
	if a.Fingerprint() != b.Fingerprint() {
		t.Fatal("expected identical fingerprints")
	}
	if a.Fingerprint() == c.Fingerprint() {
		t.Fatal("expected different fingerprints")
	}
}

func TestFingerprintProperty(t *testing.T) {
	fn := func(amount uint32) bool {
		if amount == 0 {
			return true
		}
		cmd := TransferCmd{FromAccountID: [16]byte{1}, ToAccountID: [16]byte{2}, AmountMinor: int64(amount), Currency: "USD"}
		return cmd.Fingerprint() == cmd.Fingerprint()
	}
	if err := quick.Check(fn, &quick.Config{MaxCount: 200}); err != nil {
		t.Fatal(err)
	}
}
