package authn

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"

	"github.com/Kaaushikk/real-time-payments-ledger/internal/config"
)

var (
	ErrInvalidGrant   = errors.New("invalid_grant")
	ErrInvalidClient  = errors.New("invalid_client")
	ErrInvalidRequest = errors.New("invalid_request")
	ErrInvalidCreds   = errors.New("invalid credentials")
	ErrTokenReuse     = errors.New("refresh token reuse detected")
)

type Service struct {
	pool *pgxpool.Pool
	cfg  config.Config
}

func New(pool *pgxpool.Pool, cfg config.Config) *Service {
	return &Service{pool: pool, cfg: cfg}
}

type User struct {
	ID           uuid.UUID
	Email        string
	PasswordHash string
}

type TokenPair struct {
	AccessToken  string
	RefreshToken string
	ExpiresIn    int64
	TokenType    string
	Scope        string
}

type Claims struct {
	ClientID string `json:"cid"`
	jwt.RegisteredClaims
}

func (s *Service) Authenticate(ctx context.Context, email, password string) (User, error) {
	var u User
	err := s.pool.QueryRow(ctx, `SELECT id, email, password_hash FROM users WHERE email = $1`, email).
		Scan(&u.ID, &u.Email, &u.PasswordHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return User{}, ErrInvalidCreds
	}
	if err != nil {
		return User{}, err
	}
	if bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(password)) != nil {
		return User{}, ErrInvalidCreds
	}
	return u, nil
}

func (s *Service) CreateAuthCode(ctx context.Context, userID uuid.UUID, clientID, redirectURI, challenge, method string) (string, error) {
	if method != "S256" {
		return "", fmt.Errorf("%w: code_challenge_method must be S256", ErrInvalidRequest)
	}
	if err := s.checkClient(ctx, clientID, redirectURI); err != nil {
		return "", err
	}
	raw := randomB64(32)
	hash := sha256Hex(raw)
	_, err := s.pool.Exec(ctx, `
		INSERT INTO oauth_auth_codes (code_hash, client_id, user_id, redirect_uri, code_challenge, code_challenge_method, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, now() + interval '10 minutes')
	`, hash, clientID, userID, redirectURI, challenge, method)
	return raw, err
}

func (s *Service) ExchangeCode(ctx context.Context, clientID, redirectURI, code, verifier string) (TokenPair, error) {
	if err := s.checkClient(ctx, clientID, redirectURI); err != nil {
		return TokenPair{}, err
	}
	codeHash := sha256Hex(code)
	var userID uuid.UUID
	var storedRedirect, challenge, method string
	var usedAt *time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT user_id, redirect_uri, code_challenge, code_challenge_method, used_at
		FROM oauth_auth_codes WHERE code_hash = $1 AND expires_at > now()
	`, codeHash).Scan(&userID, &storedRedirect, &challenge, &method, &usedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return TokenPair{}, ErrInvalidGrant
	}
	if err != nil {
		return TokenPair{}, err
	}
	if usedAt != nil {
		return TokenPair{}, ErrInvalidGrant
	}
	if storedRedirect != redirectURI {
		return TokenPair{}, ErrInvalidGrant
	}
	if !verifyPKCE(challenge, method, verifier) {
		return TokenPair{}, ErrInvalidGrant
	}
	if _, err := s.pool.Exec(ctx, `UPDATE oauth_auth_codes SET used_at = now() WHERE code_hash = $1`, codeHash); err != nil {
		return TokenPair{}, err
	}
	return s.issueTokens(ctx, userID, clientID)
}

func (s *Service) Refresh(ctx context.Context, clientID, refreshToken string) (TokenPair, error) {
	hash := sha256Hex(refreshToken)
	var id, userID uuid.UUID
	var revoked *time.Time
	err := s.pool.QueryRow(ctx, `
		SELECT id, user_id, revoked_at FROM refresh_tokens
		WHERE token_hash = $1 AND client_id = $2 AND expires_at > now()
	`, hash, clientID).Scan(&id, &userID, &revoked)
	if errors.Is(err, pgx.ErrNoRows) {
		return TokenPair{}, ErrInvalidGrant
	}
	if err != nil {
		return TokenPair{}, err
	}
	if revoked != nil {
		_, _ = s.pool.Exec(ctx, `UPDATE refresh_tokens SET revoked_at = now() WHERE user_id = $1 AND revoked_at IS NULL`, userID)
		return TokenPair{}, ErrTokenReuse
	}
	if _, err := s.pool.Exec(ctx, `UPDATE refresh_tokens SET revoked_at = now() WHERE id = $1`, id); err != nil {
		return TokenPair{}, err
	}
	pair, err := s.issueTokens(ctx, userID, clientID)
	if err != nil {
		return TokenPair{}, err
	}
	_, _ = s.pool.Exec(ctx, `UPDATE refresh_tokens SET parent_id = $1 WHERE token_hash = $2`, id, sha256Hex(pair.RefreshToken))
	return pair, nil
}

func (s *Service) DevIssue(ctx context.Context, userID uuid.UUID, clientID string) (TokenPair, error) {
	return s.issueTokens(ctx, userID, clientID)
}

func (s *Service) ParseAccess(token string) (*Claims, error) {
	parsed, err := jwt.ParseWithClaims(token, &Claims{}, func(t *jwt.Token) (any, error) {
		if t.Method != jwt.SigningMethodHS256 {
			return nil, fmt.Errorf("unexpected signing method")
		}
		return []byte(s.cfg.JWTSigningKey), nil
	}, jwt.WithIssuer(s.cfg.JWTIssuer), jwt.WithAudience(s.cfg.JWTAudience), jwt.WithExpirationRequired())
	if err != nil {
		return nil, err
	}
	claims, ok := parsed.Claims.(*Claims)
	if !ok || !parsed.Valid {
		return nil, ErrInvalidGrant
	}
	return claims, nil
}

func (s *Service) issueTokens(ctx context.Context, userID uuid.UUID, clientID string) (TokenPair, error) {
	now := time.Now()
	claims := Claims{
		ClientID: clientID,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    s.cfg.JWTIssuer,
			Subject:   userID.String(),
			Audience:  jwt.ClaimStrings{s.cfg.JWTAudience},
			ExpiresAt: jwt.NewNumericDate(now.Add(s.cfg.AccessTTL)),
			IssuedAt:  jwt.NewNumericDate(now),
			ID:        uuid.NewString(),
		},
	}
	tok := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	access, err := tok.SignedString([]byte(s.cfg.JWTSigningKey))
	if err != nil {
		return TokenPair{}, err
	}
	refresh := randomB64(32)
	_, err = s.pool.Exec(ctx, `
		INSERT INTO refresh_tokens (user_id, client_id, token_hash, expires_at)
		VALUES ($1, $2, $3, $4)
	`, userID, clientID, sha256Hex(refresh), now.Add(s.cfg.RefreshTTL))
	if err != nil {
		return TokenPair{}, err
	}
	return TokenPair{
		AccessToken:  access,
		RefreshToken: refresh,
		ExpiresIn:    int64(s.cfg.AccessTTL.Seconds()),
		TokenType:    "Bearer",
		Scope:        "ledger:write ledger:read",
	}, nil
}

func (s *Service) checkClient(ctx context.Context, clientID, redirectURI string) error {
	var uris []string
	err := s.pool.QueryRow(ctx, `SELECT redirect_uris FROM oauth_clients WHERE id = $1`, clientID).Scan(&uris)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrInvalidClient
	}
	if err != nil {
		return err
	}
	if redirectURI == "" {
		return nil
	}
	u, err := url.Parse(redirectURI)
	if err != nil {
		return ErrInvalidRequest
	}
	_ = u
	for _, allowed := range uris {
		if allowed == redirectURI {
			return nil
		}
	}
	return ErrInvalidClient
}

func verifyPKCE(challenge, method, verifier string) bool {
	if method != "S256" {
		return false
	}
	sum := sha256.Sum256([]byte(verifier))
	computed := base64.RawURLEncoding.EncodeToString(sum[:])
	return computed == challenge
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func RandomString(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}

func randomB64(n int) string { return RandomString(n) }

func ParseBearer(h string) string {
	const p = "Bearer "
	if strings.HasPrefix(h, p) {
		return strings.TrimPrefix(h, p)
	}
	return ""
}
