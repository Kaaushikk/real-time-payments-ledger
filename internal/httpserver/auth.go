package httpserver

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"html/template"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/beybl/realtime-payments-ledger/internal/authn"
)

type AuthAPI struct {
	Auth *authn.Service
}

func (a AuthAPI) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /oauth/authorize", a.authorize)
	mux.HandleFunc("POST /oauth/login", a.login)
	mux.HandleFunc("POST /oauth/token", a.token)
	mux.HandleFunc("POST /oauth/introspect", a.introspect)
	mux.HandleFunc("GET /oauth/pkce", pkceHelper)
	if os.Getenv("ALLOW_DEV_TOKEN") == "true" {
		mux.HandleFunc("POST /oauth/dev-token", a.devToken)
	}
	return otelhttp.NewHandler(mux, "auth.http")
}

func (a AuthAPI) authorize(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = loginTmpl.Execute(w, map[string]string{
		"ClientID":      q.Get("client_id"),
		"RedirectURI":   q.Get("redirect_uri"),
		"State":         q.Get("state"),
		"Challenge":     q.Get("code_challenge"),
		"ChallengeMeth": q.Get("code_challenge_method"),
		"Scope":         q.Get("scope"),
	})
}

func (a AuthAPI) login(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	user, err := a.Auth.Authenticate(r.Context(), r.FormValue("email"), r.FormValue("password"))
	if err != nil {
		http.Error(w, "invalid credentials", http.StatusUnauthorized)
		return
	}
	code, err := a.Auth.CreateAuthCode(r.Context(), user.ID,
		r.FormValue("client_id"),
		r.FormValue("redirect_uri"),
		r.FormValue("code_challenge"),
		r.FormValue("code_challenge_method"),
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	u, err := url.Parse(r.FormValue("redirect_uri"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	q := u.Query()
	q.Set("code", code)
	q.Set("state", r.FormValue("state"))
	u.RawQuery = q.Encode()
	http.Redirect(w, r, u.String(), http.StatusFound)
}

func (a AuthAPI) token(w http.ResponseWriter, r *http.Request) {
	vals := readTokenParams(r)
	grant := vals["grant_type"]
	clientID := vals["client_id"]
	switch grant {
	case "authorization_code":
		pair, err := a.Auth.ExchangeCode(r.Context(), clientID, vals["redirect_uri"], vals["code"], vals["code_verifier"])
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, tokenJSON(pair))
	case "refresh_token":
		pair, err := a.Auth.Refresh(r.Context(), clientID, vals["refresh_token"])
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, tokenJSON(pair))
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unsupported_grant_type"})
	}
}

func (a AuthAPI) introspect(w http.ResponseWriter, r *http.Request) {
	vals := readTokenParams(r)
	claims, err := a.Auth.ParseAccess(vals["token"])
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"active": false})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"active":    true,
		"sub":       claims.Subject,
		"client_id": claims.ClientID,
		"exp":       claims.ExpiresAt.Time.Unix(),
	})
}

func (a AuthAPI) devToken(w http.ResponseWriter, r *http.Request) {
	vals := readTokenParams(r)
	user, err := a.Auth.Authenticate(r.Context(), vals["email"], vals["password"])
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid credentials"})
		return
	}
	clientID := vals["client_id"]
	if clientID == "" {
		clientID = "ledger-cli"
	}
	pair, err := a.Auth.DevIssue(r.Context(), user.ID, clientID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, tokenJSON(pair))
}

func tokenJSON(p authn.TokenPair) map[string]any {
	return map[string]any{
		"access_token":  p.AccessToken,
		"refresh_token": p.RefreshToken,
		"token_type":    p.TokenType,
		"expires_in":    p.ExpiresIn,
		"scope":         p.Scope,
	}
}

func pkceHelper(w http.ResponseWriter, _ *http.Request) {
	verifier := authn.RandomString(32)
	sum := sha256.Sum256([]byte(verifier))
	writeJSON(w, http.StatusOK, map[string]string{
		"code_verifier":         verifier,
		"code_challenge":        base64.RawURLEncoding.EncodeToString(sum[:]),
		"code_challenge_method": "S256",
	})
}

func readTokenParams(r *http.Request) map[string]string {
	out := map[string]string{}
	ct := r.Header.Get("Content-Type")
	if strings.HasPrefix(ct, "application/json") {
		b, _ := io.ReadAll(r.Body)
		var m map[string]string
		_ = json.Unmarshal(b, &m)
		for k, v := range m {
			out[k] = v
		}
		return out
	}
	_ = r.ParseForm()
	for k := range r.Form {
		out[k] = r.Form.Get(k)
	}
	return out
}

var loginTmpl = template.Must(template.New("login").Parse(`<!doctype html>
<html><head><title>Ledger Sign In</title>
<style>body{font-family:system-ui;margin:4rem auto;max-width:24rem}input{width:100%;padding:.5rem;margin:.3rem 0}</style>
</head><body>
<h1>Sign in</h1>
<p>Demo users: alice@ledger.dev / password or bob@ledger.dev / password</p>
<form method="post" action="/oauth/login">
<input type="hidden" name="client_id" value="{{.ClientID}}">
<input type="hidden" name="redirect_uri" value="{{.RedirectURI}}">
<input type="hidden" name="state" value="{{.State}}">
<input type="hidden" name="code_challenge" value="{{.Challenge}}">
<input type="hidden" name="code_challenge_method" value="{{.ChallengeMeth}}">
<label>Email <input name="email" value="alice@ledger.dev"></label>
<label>Password <input name="password" type="password" value="password"></label>
<button type="submit">Authorize</button>
</form>
</body></html>`))
