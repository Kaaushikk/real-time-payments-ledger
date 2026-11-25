package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
)

func TestLoadUsesOwnerTokensAndVerifiesReplay(t *testing.T) {
	alice, bob := uuid.NewString(), uuid.NewString()
	var mu sync.Mutex
	ids := map[string]string{}
	counts := map[string]int{}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/oauth/dev-token" {
			if err := r.ParseForm(); err != nil {
				t.Error(err)
			}
			if r.Form.Get("client_id") != "ledger-cli" || r.Form.Get("password") != "password" {
				t.Error("missing local auth form parameters")
			}
			email := r.Form.Get("email")
			if email != "alice@ledger.dev" && email != "bob@ledger.dev" {
				http.Error(w, "unknown owner", 401)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"access_token": email})
			return
		}
		if r.Method == http.MethodGet {
			id := strings.TrimPrefix(r.URL.Path, "/v1/accounts/")
			want := "alice@ledger.dev"
			if id == bob {
				want = "bob@ledger.dev"
			}
			if r.Header.Get("Authorization") != "Bearer "+want {
				http.Error(w, "wrong owner", 403)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]string{"account_id": id, "currency": "USD"})
			return
		}
		var body struct {
			From   string `json:"from_account_id"`
			To     string `json:"to_account_id"`
			Amount int    `json:"amount_minor"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		want := "alice@ledger.dev"
		if body.From == bob {
			want = "bob@ledger.dev"
		}
		if r.Header.Get("Authorization") != "Bearer "+want {
			http.Error(w, "wrong owner", 403)
			return
		}
		if body.Amount != 1 || (body.From == alice && body.To != bob) || (body.From == bob && body.To != alice) {
			t.Error("unexpected transfer body")
		}
		key := r.Header.Get("Idempotency-Key")
		mu.Lock()
		defer mu.Unlock()
		counts[body.From]++
		id, replay := ids[key]
		if !replay {
			id = uuid.NewString()
			ids[key] = id
			w.WriteHeader(201)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"transaction": map[string]any{"transaction_id": id, "amount_minor": 1, "currency": "USD"}, "replayed": replay})
	}))
	defer s.Close()
	c := config{ledgerURL: s.URL, authURL: s.URL, from: alice, to: bob, requests: 8, concurrency: 3, replayEvery: 2, devAuth: true}
	if err := c.validate(); err != nil {
		t.Fatal(err)
	}
	var err error
	c.aliceToken, err = devToken(context.Background(), s.Client(), s.URL, "alice@ledger.dev")
	if err != nil {
		t.Fatal(err)
	}
	c.bobToken, err = devToken(context.Background(), s.Client(), s.URL, "bob@ledger.dev")
	if err != nil {
		t.Fatal(err)
	}
	if err = checkOwner(context.Background(), s.Client(), s.URL, bob, c.aliceToken); err == nil {
		t.Fatal("preflight accepted a token for the wrong owner")
	}
	for _, account := range []struct{ id, token string }{{alice, c.aliceToken}, {bob, c.bobToken}} {
		if err = checkOwner(context.Background(), s.Client(), s.URL, account.id, account.token); err != nil {
			t.Fatal(err)
		}
	}
	r := run(context.Background(), s.Client(), c)
	if r.Posted != 8 || r.Replayed != 4 || r.Failed != 0 || r.Requests != 12 || r.Statuses["201"] != 8 || r.Statuses["200"] != 4 || r.Peak > 3 {
		t.Fatalf("unexpected result: %+v", r)
	}
	mu.Lock()
	defer mu.Unlock()
	if counts[alice] != 4 || counts[bob] != 8 || len(ids) != 8 {
		t.Fatalf("wrong workload: counts=%v unique=%d", counts, len(ids))
	}
}

func TestLoadRejectsFalseSuccesses(t *testing.T) {
	for _, kind := range []string{"invalid_json", "changed_replay", "duplicate_new_id"} {
		t.Run(kind, func(t *testing.T) {
			shared := uuid.NewString()
			seen := map[string]bool{}
			var mu sync.Mutex
			s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				if kind == "invalid_json" {
					w.WriteHeader(201)
					_, _ = w.Write([]byte("not a receipt"))
					return
				}
				id := shared
				replay := false
				if kind == "changed_replay" {
					key := r.Header.Get("Idempotency-Key")
					replay = seen[key]
					seen[key] = true
					id = uuid.NewString()
				}
				if !replay {
					w.WriteHeader(201)
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"transaction": map[string]any{"transaction_id": id, "amount_minor": 1, "currency": "USD"}, "replayed": replay})
			}))
			defer s.Close()
			c := config{ledgerURL: s.URL, from: uuid.NewString(), to: uuid.NewString(), requests: 2, concurrency: 1, aliceToken: "alice", bobToken: "bob"}
			if kind == "changed_replay" {
				c.replayEvery = 1
			}
			r := run(context.Background(), s.Client(), c)
			if r.Failed == 0 {
				t.Fatalf("false success accepted for %s", kind)
			}
			if kind == "changed_replay" && r.Failures[0].Reason != "receipt_identity_mismatch" {
				t.Fatalf("wrong replay failure: %+v", r.Failures)
			}
		})
	}
}

func TestDevAuthStatusAndLoopbackRestriction(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Error(w, "private-token-value", 401) }))
	defer s.Close()
	_, err := devToken(context.Background(), s.Client(), s.URL, "alice@ledger.dev")
	if err == nil || !strings.Contains(err.Error(), "401") || strings.Contains(err.Error(), "private-token-value") {
		t.Fatalf("unsafe auth error: %v", err)
	}
	c := config{ledgerURL: "http://example.com", authURL: s.URL, from: uuid.NewString(), to: uuid.NewString(), requests: 2, concurrency: 1, devAuth: true}
	if c.validate() == nil {
		t.Fatal("demo auth allowed a non-loopback target")
	}
}

func TestNearestRankLatency(t *testing.T) {
	samples := []float64{1, 2, 3, 4, 100}
	if percentile(samples, .5) != 3 || percentile(samples, .95) != 100 || percentile(samples, .99) != 100 {
		t.Fatal("incorrect nearest-rank percentile")
	}
}
