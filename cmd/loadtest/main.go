// Command loadtest measures a bounded, two-wallet REST workload.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type config struct {
	ledgerURL, authURL, from, to, aliceToken, bobToken string
	requests, concurrency, replayEvery                 int
	devAuth                                            bool
}

type attempt struct {
	index             int
	kind, id, problem string
	status            int
	ms                float64
}

type failure struct {
	Index  int    `json:"transfer_index"`
	Kind   string `json:"request_kind"`
	Status int    `json:"http_status"`
	Reason string `json:"reason"`
}

type result struct {
	StartedAt         string             `json:"started_at_utc"`
	FinishedAt        string             `json:"finished_at_utc"`
	UniqueRequested   int                `json:"unique_transfers_requested"`
	Concurrency       int                `json:"worker_limit"`
	Peak              int64              `json:"peak_in_flight"`
	ReplayEvery       int                `json:"replay_every"`
	Requests          int                `json:"http_requests"`
	Posted            int                `json:"posted"`
	Replayed          int                `json:"replayed"`
	Failed            int                `json:"failed_requests"`
	Statuses          map[string]int     `json:"http_status_counts"`
	Failures          []failure          `json:"failures"`
	ElapsedSeconds    float64            `json:"elapsed_seconds"`
	RequestsPerSecond float64            `json:"observed_requests_per_second"`
	Latency           map[string]float64 `json:"latency_ms"`
	Samples           []float64          `json:"sorted_latency_samples_ms"`
}

func main() {
	c := config{aliceToken: os.Getenv("ALICE_ACCESS_TOKEN"), bobToken: os.Getenv("BOB_ACCESS_TOKEN")}
	flag.StringVar(&c.ledgerURL, "ledger-url", getenv("LEDGER_URL", "http://localhost:8080"), "ledger origin")
	flag.StringVar(&c.authURL, "auth-url", getenv("AUTH_URL", "http://localhost:8081"), "local demo auth origin")
	flag.StringVar(&c.from, "from", os.Getenv("FROM_ACCOUNT"), "Alice's account UUID")
	flag.StringVar(&c.to, "to", os.Getenv("TO_ACCOUNT"), "Bob's account UUID")
	flag.IntVar(&c.requests, "requests", 200, "unique one-cent transfers, alternating direction")
	flag.IntVar(&c.concurrency, "concurrency", 8, "maximum concurrent workers")
	flag.IntVar(&c.replayEvery, "replay-every", 4, "replay every Nth transfer; zero disables")
	flag.BoolVar(&c.devAuth, "dev-auth", false, "obtain separate demo-user tokens; loopback only")
	flag.Parse()
	if err := c.validate(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = c.concurrency
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if c.devAuth {
		var err error
		c.aliceToken, err = devToken(ctx, client, c.authURL, "alice@ledger.dev")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
		c.bobToken, err = devToken(ctx, client, c.authURL, "bob@ledger.dev")
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
	}
	if err := checkOwner(ctx, client, c.ledgerURL, c.from, c.aliceToken); err != nil {
		fmt.Fprintln(os.Stderr, "Alice account preflight:", err)
		os.Exit(2)
	}
	if err := checkOwner(ctx, client, c.ledgerURL, c.to, c.bobToken); err != nil {
		fmt.Fprintln(os.Stderr, "Bob account preflight:", err)
		os.Exit(2)
	}
	r := run(ctx, client, c)
	if err := json.NewEncoder(os.Stdout).Encode(r); err != nil {
		fmt.Fprintln(os.Stderr, "write result:", err)
		os.Exit(2)
	}
	if r.Failed > 0 {
		os.Exit(1)
	}
}

func (c config) validate() error {
	if c.requests < 2 || c.requests > 100000 || c.concurrency < 1 || c.concurrency > 256 || c.concurrency > c.requests || c.replayEvery < 0 {
		return errors.New("requests must be 2..100000, workers 1..min(requests,256), replay-every >= 0")
	}
	a, e1 := uuid.Parse(c.from)
	b, e2 := uuid.Parse(c.to)
	if e1 != nil || e2 != nil || a == uuid.Nil || b == uuid.Nil || a == b {
		return errors.New("two distinct nonzero account UUIDs are required")
	}
	for _, origin := range []string{c.ledgerURL, c.authURL} {
		u, err := url.Parse(origin)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
			return errors.New("URLs must be HTTP(S) origins without credentials, queries or paths")
		}
		if c.devAuth && u.Hostname() != "localhost" && !net.ParseIP(u.Hostname()).IsLoopback() {
			return errors.New("demo authentication requires loopback auth and ledger origins")
		}
	}
	if !c.devAuth && (c.aliceToken == "" || c.bobToken == "") {
		return errors.New("provide ALICE_ACCESS_TOKEN and BOB_ACCESS_TOKEN, or explicitly select local -dev-auth")
	}
	return nil
}

func decodeResponse(resp *http.Response, out any) error {
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 65537))
	if err != nil || len(b) > 65536 {
		return errors.New("unreadable or oversized response")
	}
	return json.Unmarshal(b, out)
}

func devToken(ctx context.Context, client *http.Client, origin, email string) (string, error) {
	form := url.Values{"email": {email}, "password": {"password"}, "client_id": {"ledger-cli"}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(origin, "/")+"/oauth/dev-token", strings.NewReader(form.Encode()))
	if err != nil {
		return "", errors.New("invalid auth request")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := client.Do(req)
	if err != nil {
		return "", errors.New("demo authentication transport failed")
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return "", fmt.Errorf("demo authentication returned HTTP %d", resp.StatusCode)
	}
	var out struct {
		Token string `json:"access_token"`
	}
	if decodeResponse(resp, &out) != nil || out.Token == "" {
		return "", errors.New("demo authentication returned an invalid token response")
	}
	return out.Token, nil
}

func checkOwner(ctx context.Context, client *http.Client, origin, id, token string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(origin, "/")+"/v1/accounts/"+id, nil)
	if err != nil {
		return errors.New("invalid account request")
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := client.Do(req)
	if err != nil {
		return errors.New("account lookup transport failed")
	}
	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return fmt.Errorf("account lookup returned HTTP %d", resp.StatusCode)
	}
	var out struct {
		ID       string `json:"account_id"`
		Currency string `json:"currency"`
	}
	if decodeResponse(resp, &out) != nil || out.ID != id || out.Currency != "USD" {
		return errors.New("account lookup did not confirm the expected USD wallet")
	}
	return nil
}

func run(ctx context.Context, client *http.Client, c config) result {
	started := time.Now()
	jobs := make(chan int)
	observations := make(chan attempt, 2*c.requests)
	var workers sync.WaitGroup
	var active, peak atomic.Int64
	post := func(i int, kind, key, src, dst, token string) attempt {
		body, _ := json.Marshal(map[string]any{"from_account_id": src, "to_account_id": dst, "amount_minor": 1, "currency": "USD", "description": "scoped load experiment"})
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(c.ledgerURL, "/")+"/v1/transfers", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", key)
		// Keep each measured HTTP/1 POST to one transport attempt.
		req.GetBody = nil
		begin := time.Now()
		n := active.Add(1)
		for old := peak.Load(); n > old; old = peak.Load() {
			if peak.CompareAndSwap(old, n) {
				break
			}
		}
		defer active.Add(-1)
		o := attempt{index: i, kind: kind}
		resp, err := client.Do(req)
		if err != nil {
			o.problem = "transport_error"
		} else {
			o.status = resp.StatusCode
			var out struct {
				Transaction struct {
					ID       string `json:"transaction_id"`
					Amount   int64  `json:"amount_minor"`
					Currency string `json:"currency"`
				} `json:"transaction"`
				Replayed *bool `json:"replayed"`
			}
			err = decodeResponse(resp, &out)
			want := http.StatusCreated
			if kind == "replay" {
				want = http.StatusOK
			}
			id, idErr := uuid.Parse(out.Transaction.ID)
			switch {
			case o.status != want:
				o.problem = "unexpected_status"
			case err != nil || idErr != nil || id == uuid.Nil || out.Replayed == nil || out.Transaction.Amount != 1 || out.Transaction.Currency != "USD":
				o.problem = "invalid_receipt"
			case *out.Replayed != (kind == "replay"):
				o.problem = "unexpected_replay_flag"
			default:
				o.id = id.String()
			}
		}
		o.ms = float64(time.Since(begin).Nanoseconds()) / 1e6
		return o
	}
	for w := 0; w < c.concurrency; w++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for i := range jobs {
				src, dst, token := c.from, c.to, c.aliceToken
				if i%2 == 1 {
					src, dst, token = c.to, c.from, c.bobToken
				}
				key := uuid.NewString()
				first := post(i, "post", key, src, dst, token)
				observations <- first
				if c.replayEvery > 0 && (i+1)%c.replayEvery == 0 {
					replay := post(i, "replay", key, src, dst, token)
					if replay.problem == "" && (first.problem != "" || replay.id != first.id) {
						replay.problem = "receipt_identity_mismatch"
					}
					observations <- replay
				}
			}
		}()
	}
	for i := 0; i < c.requests; i++ {
		jobs <- i
	}
	close(jobs)
	workers.Wait()
	close(observations)
	elapsed := time.Since(started).Seconds()
	r := result{StartedAt: started.UTC().Format(time.RFC3339Nano), FinishedAt: time.Now().UTC().Format(time.RFC3339Nano), UniqueRequested: c.requests, Concurrency: c.concurrency, Peak: peak.Load(), ReplayEvery: c.replayEvery, Statuses: map[string]int{}, Failures: []failure{}, ElapsedSeconds: elapsed}
	seen := map[string]bool{}
	for o := range observations {
		r.Requests++
		r.Statuses[strconv.Itoa(o.status)]++
		r.Samples = append(r.Samples, o.ms)
		if o.problem == "" && o.kind == "post" {
			if seen[o.id] {
				o.problem = "duplicate_transaction_for_distinct_keys"
			} else {
				seen[o.id] = true
			}
		}
		if o.problem != "" {
			r.Failed++
			r.Failures = append(r.Failures, failure{o.index, o.kind, o.status, o.problem})
		} else if o.kind == "post" {
			r.Posted++
		} else {
			r.Replayed++
		}
	}
	sort.Float64s(r.Samples)
	r.Latency = map[string]float64{"min": r.Samples[0], "p50": percentile(r.Samples, .50), "p95": percentile(r.Samples, .95), "p99": percentile(r.Samples, .99), "max": r.Samples[len(r.Samples)-1]}
	r.RequestsPerSecond = float64(r.Requests) / elapsed
	return r
}

func percentile(sorted []float64, p float64) float64 {
	return sorted[int(math.Ceil(float64(len(sorted))*p))-1]
}
func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
