package main

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	ledgerURL := getenv("LEDGER_URL", "http://localhost:8080")
	authURL := getenv("AUTH_URL", "http://localhost:8081")
	from := os.Getenv("FROM_ACCOUNT")
	to := os.Getenv("TO_ACCOUNT")
	n := 1000
	if from == "" || to == "" {
		fmt.Fprintln(os.Stderr, "FROM_ACCOUNT and TO_ACCOUNT are required")
		os.Exit(2)
	}

	token := mustDevToken(authURL)
	var ok, fail atomic.Int64
	var wg sync.WaitGroup
	sem := make(chan struct{}, 64)
	start := time.Now()
	for i := 0; i < n; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			src, dst := from, to
			if i%2 == 1 {
				src, dst = to, from
			}
			key := randomKey()
			body, _ := json.Marshal(map[string]any{
				"from_account_id": src,
				"to_account_id":   dst,
				"amount_minor":    1,
				"currency":        "USD",
				"description":     "loadtest",
			})
			req, _ := http.NewRequest(http.MethodPost, ledgerURL+"/v1/transfers", bytes.NewReader(body))
			req.Header.Set("Authorization", "Bearer "+token)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Idempotency-Key", key)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				fail.Add(1)
				return
			}
			resp.Body.Close()
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				ok.Add(1)
			} else {
				fail.Add(1)
			}
		}(i)
	}
	wg.Wait()
	fmt.Printf("transfers=%d ok=%d fail=%d elapsed=%s\n", n, ok.Load(), fail.Load(), time.Since(start))
}

func mustDevToken(authURL string) string {
	body, _ := json.Marshal(map[string]string{
		"email":    getenv("EMAIL", "alice@ledger.dev"),
		"password": getenv("PASSWORD", "password"),
	})
	resp, err := http.Post(authURL+"/oauth/dev-token", "application/json", bytes.NewReader(body))
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()
	var out struct {
		AccessToken string `json:"access_token"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&out)
	if out.AccessToken == "" {
		panic("dev token empty — is ALLOW_DEV_TOKEN=true?")
	}
	return out.AccessToken
}

func randomKey() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
