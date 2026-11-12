package authn

import (
	"strings"
	"testing"
)

func TestPKCEVerifierAndChallenge(t *testing.T) {
	const verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
	const challenge = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
	if !validPKCEVerifier(verifier) || pkceChallenge(verifier) != challenge {
		t.Fatal("S256 result differs from the RFC 7636 example")
	}
	if !validPKCEVerifier(strings.Repeat("a", 124) + "-._~") {
		t.Fatal("rejected the maximum valid length or unreserved characters")
	}
	for _, invalid := range []string{verifier[:42], strings.Repeat("a", 129), verifier + "=", verifier + " ", verifier + "é"} {
		if validPKCEVerifier(invalid) {
			t.Fatal("accepted an invalid verifier")
		}
	}
}
