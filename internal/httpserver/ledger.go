package httpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/Kaaushikk/real-time-payments-ledger/internal/authn"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/ledger"
	"github.com/Kaaushikk/real-time-payments-ledger/internal/obs"
)

type principalKey struct{}

type LedgerAPI struct {
	Ledger *ledger.Service
	Auth   *authn.Service
}

func (a LedgerAPI) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := a.Ledger.Ready(r.Context()); err != nil {
			writeJSON(w, 503, map[string]string{"status": "unavailable"})
			return
		}
		writeJSON(w, 200, map[string]string{"status": "ready"})
	})
	mux.HandleFunc("POST /v1/accounts", a.createAccount)
	mux.HandleFunc("GET /v1/accounts/{account_id}", a.getAccount)
	mux.HandleFunc("POST /v1/transfers", a.transfer)
	mux.HandleFunc("GET /v1/transactions/{transaction_id}", a.getTx)
	mux.HandleFunc("GET /v1/accounts/{account_id}/audit", a.audit)
	return otelhttp.NewHandler(a.withAuth(mux), "ledger.http")
}

func (a LedgerAPI) withAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" || r.URL.Path == "/metrics" || r.URL.Path == "/readyz" {
			next.ServeHTTP(w, r)
			return
		}
		raw := authn.ParseBearer(r.Header.Get("Authorization"))
		if raw == "" {
			http.Error(w, `{"error":"missing bearer token"}`, http.StatusUnauthorized)
			return
		}
		claims, err := a.Auth.ParseAccess(raw)
		if err != nil {
			http.Error(w, `{"error":"invalid token"}`, http.StatusUnauthorized)
			return
		}
		ctx, span := obs.Tracer().Start(r.Context(), "auth.ValidateJWT",
			trace.WithAttributes(attribute.String("sub", claims.Subject)))
		span.End()
		principal, err := uuid.Parse(claims.Subject)
		if err != nil || principal == uuid.Nil {
			writeErr(w, 401, ledger.ErrUnauthorized)
			return
		}
		ctx = context.WithValue(ctx, principalKey{}, principal)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (a LedgerAPI) createAccount(w http.ResponseWriter, r *http.Request) {
	var body struct {
		OwnerUserID string `json:"owner_user_id"`
		Currency    string `json:"currency"`
	}
	if err := decodeBody(w, r, &body); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	owner := r.Context().Value(principalKey{}).(uuid.UUID)
	if body.OwnerUserID != "" && body.OwnerUserID != owner.String() {
		writeErr(w, 403, ledger.ErrUnauthorized)
		return
	}

	acc, err := a.Ledger.CreateAccount(r.Context(), owner, body.Currency)
	if err != nil {
		writeLedgerErr(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, accountJSON(acc))
}

func (a LedgerAPI) getAccount(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("account_id"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	acc, err := a.Ledger.GetAccount(r.Context(), id)
	if err != nil {
		writeLedgerErr(w, err)
		return
	}
	if acc.OwnerUserID == nil || *acc.OwnerUserID != r.Context().Value(principalKey{}).(uuid.UUID) {
		writeErr(w, 404, ledger.ErrAccountNotFound)
		return
	}
	writeJSON(w, http.StatusOK, accountJSON(acc))
}

func (a LedgerAPI) transfer(w http.ResponseWriter, r *http.Request) {
	var body struct {
		FromAccountID string `json:"from_account_id"`
		ToAccountID   string `json:"to_account_id"`
		AmountMinor   int64  `json:"amount_minor"`
		Currency      string `json:"currency"`
		Description   string `json:"description"`
	}
	if err := decodeBody(w, r, &body); err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	from, err := uuid.Parse(body.FromAccountID)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	to, err := uuid.Parse(body.ToAccountID)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		key = bodyFromQuery(r, "idempotency_key")
	}
	actor := r.Context().Value(principalKey{}).(uuid.UUID)

	ccy := body.Currency
	if ccy == "" {
		ccy = "USD"
	}
	res, err := a.Ledger.Transfer(r.Context(), ledger.TransferCmd{
		IdempotencyKey: key,
		FromAccountID:  from,
		ToAccountID:    to,
		AmountMinor:    body.AmountMinor,
		Currency:       ccy,
		Description:    body.Description,
		ActorUserID:    actor,
	})
	if err != nil {
		writeLedgerErr(w, err)
		return
	}
	status := http.StatusCreated
	if res.Replayed {
		status = http.StatusOK
	}
	writeJSON(w, status, map[string]any{
		"transaction": txJSON(res.Tx),
		"replayed":    res.Replayed,
	})
}

func (a LedgerAPI) getTx(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("transaction_id"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	tx, err := a.Ledger.GetTransaction(r.Context(), id)
	if err != nil {
		writeLedgerErr(w, err)
		return
	}
	participant := false
	for _, entry := range tx.Entries {
		acc, err := a.Ledger.GetAccount(r.Context(), entry.AccountID)
		if err != nil {
			writeLedgerErr(w, err)
			return
		}
		if acc.OwnerUserID != nil && *acc.OwnerUserID == r.Context().Value(principalKey{}).(uuid.UUID) {
			participant = true
		}
	}
	if !participant {
		writeErr(w, 404, ledger.ErrAccountNotFound)
		return
	}
	writeJSON(w, http.StatusOK, txJSON(tx))
}

func (a LedgerAPI) audit(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("account_id"))
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	acc, err := a.Ledger.GetAccount(r.Context(), id)
	if err != nil {
		writeLedgerErr(w, err)
		return
	}
	if acc.OwnerUserID == nil || *acc.OwnerUserID != r.Context().Value(principalKey{}).(uuid.UUID) {
		writeErr(w, 404, ledger.ErrAccountNotFound)
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	events, err := a.Ledger.ListAudit(r.Context(), id, limit)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
		return
	}
	out := make([]map[string]any, 0, len(events))
	for _, e := range events {
		item := map[string]any{
			"id":             e.ID,
			"account_id":     e.AccountID.String(),
			"event_type":     e.EventType,
			"balance_before": e.BalanceBefore,
			"balance_after":  e.BalanceAfter,
			"amount_minor":   e.AmountMinor,
			"created_at":     e.CreatedAt.Format(time.RFC3339Nano),
		}
		if e.TransactionID != nil {
			item["transaction_id"] = e.TransactionID.String()
		}
		out = append(out, item)
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": out})
}

func accountJSON(a ledger.Account) map[string]any {
	m := map[string]any{
		"account_id":    a.ID.String(),
		"currency":      a.Currency,
		"balance_minor": a.Balance,
		"version":       a.Version,
		"account_type":  string(a.Type),
	}
	if a.OwnerUserID != nil {
		m["owner_user_id"] = a.OwnerUserID.String()
	}
	return m
}

func txJSON(t ledger.Transaction) map[string]any {
	entries := make([]map[string]any, 0, len(t.Entries))
	for _, e := range t.Entries {
		entries = append(entries, map[string]any{
			"entry_id":     e.ID.String(),
			"account_id":   e.AccountID.String(),
			"direction":    e.Direction,
			"amount_minor": e.Amount,
		})
	}
	return map[string]any{
		"transaction_id": t.ID.String(),
		"type":           t.Type,
		"status":         t.Status,
		"amount_minor":   t.Amount,
		"currency":       t.Currency,
		"description":    t.Description,
		"entries":        entries,
		"created_at":     t.CreatedAt.Format(time.RFC3339Nano),
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func writeLedgerErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ledger.ErrUnauthorized):
		writeErr(w, http.StatusForbidden, err)
	case errors.Is(err, ledger.ErrInsufficientFunds), errors.Is(err, ledger.ErrNegativeBalance):
		writeErr(w, http.StatusConflict, err)
	case errors.Is(err, ledger.ErrAccountNotFound):
		writeErr(w, http.StatusNotFound, err)
	case errors.Is(err, ledger.ErrIdempotencyConflict):
		writeErr(w, http.StatusUnprocessableEntity, err)
	case errors.Is(err, ledger.ErrIdempotencyInFlight):
		writeErr(w, http.StatusConflict, err)
	case errors.Is(err, ledger.ErrInvalidTransfer), errors.Is(err, ledger.ErrAmountOverflow), errors.Is(err, ledger.ErrSameAccount), errors.Is(err, ledger.ErrCurrencyMismatch):
		writeErr(w, http.StatusBadRequest, err)
	default:
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
	}
}

func bodyFromQuery(r *http.Request, k string) string {
	if v := r.Header.Get("X-Idempotency-Key"); v != "" {
		return v
	}
	return r.URL.Query().Get(k)
}

// Bound decoding and reject unknown/trailing fields instead of silently changing intent.
func decodeBody(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 16*1024)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return err
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return fmt.Errorf("expected one JSON object")
	}
	return nil
}
