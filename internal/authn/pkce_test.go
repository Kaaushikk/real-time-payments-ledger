package authn

import (
	"crypto/sha256"
	"encoding/base64"
	"testing"
)

func TestPKCERoundTrip(t *testing.T) {
	verifier := RandomString(32)
	sum := sha256.Sum256([]byte(verifier))
	challenge := base64.RawURLEncoding.EncodeToString(sum[:])
	if !verifyPKCE(challenge, "S256", verifier) {
		t.Fatal("pkce should verify")
	}
	if verifyPKCE(challenge, "S256", verifier+"x") {
		t.Fatal("pkce should reject wrong verifier")
	}
	if verifyPKCE(challenge, "plain", verifier) {
		t.Fatal("only S256 is allowed")
	}
}
