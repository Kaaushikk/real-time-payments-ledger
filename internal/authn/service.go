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
	"github.com/jackc/pgx/v5/pgconn"
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
	if redirectURI == "" {
		return "", ErrInvalidRequest
	}
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(challenge)
	if err != nil || len(decoded) != sha256.Size || base64.RawURLEncoding.EncodeToString(decoded) != challenge {
		return "", fmt.Errorf("%w: invalid S256 code challenge", ErrInvalidRequest)
	}
	if err := s.checkClient(ctx, clientID, redirectURI); err != nil {
		return "", err
	}
	raw := randomB64(32)
	hash := sha256Hex(raw)
	_, err = s.pool.Exec(ctx, `
		INSERT INTO oauth_auth_codes (code_hash, client_id, user_id, redirect_uri, code_challenge, code_challenge_method, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, now() + interval '10 minutes')
	`, hash, clientID, userID, redirectURI, challenge, method)
	return raw, err
}

func (s *Service) ExchangeCode(ctx context.Context, clientID, redirectURI, code, verifier string) (TokenPair, error) {
	if code == "" || redirectURI == "" || !validPKCEVerifier(verifier) {
		return TokenPair{}, ErrInvalidGrant
	}
	if err := s.checkClient(ctx, clientID, redirectURI); err != nil {
		return TokenPair{}, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return TokenPair{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// The conditional update claims the code once, including under concurrent exchange.
	var userID uuid.UUID
	err = tx.QueryRow(ctx, `
		UPDATE oauth_auth_codes SET used_at = clock_timestamp()
		WHERE code_hash = $1 AND client_id = $2 AND redirect_uri = $3
		  AND code_challenge = $4 AND code_challenge_method = 'S256'
		  AND used_at IS NULL AND expires_at > clock_timestamp()
		RETURNING user_id
	`, sha256Hex(code), clientID, redirectURI, pkceChallenge(verifier)).Scan(&userID)
	if errors.Is(err, pgx.ErrNoRows) {
		return TokenPair{}, ErrInvalidGrant
	}
	if err != nil {
		return TokenPair{}, err
	}
	pair, err := s.issueTokens(ctx, tx, userID, clientID, nil)
	if err != nil {
		return TokenPair{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return TokenPair{}, err
	}
	return pair, nil
}

func (s *Service) DevIssue(ctx context.Context, userID uuid.UUID, clientID string) (TokenPair, error) {
	return s.issueTokens(ctx, s.pool, userID, clientID, nil)
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
	userID, err := uuid.Parse(claims.Subject)
	if err != nil || userID == uuid.Nil {
		return nil, ErrInvalidGrant
	}
	return claims, nil
}

type tokenWriter interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func (s *Service) issueTokens(ctx context.Context, writer tokenWriter, userID uuid.UUID, clientID string, parentID *uuid.UUID) (TokenPair, error) {
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
	_, err = writer.Exec(ctx, `
		INSERT INTO refresh_tokens (user_id, client_id, token_hash, expires_at, parent_id)
		VALUES ($1, $2, $3, $4, $5)
	`, userID, clientID, sha256Hex(refresh), now.Add(s.cfg.RefreshTTL), parentID)
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

func validPKCEVerifier(verifier string) bool {
	if len(verifier) < 43 || len(verifier) > 128 {
		return false
	}
	for _, c := range verifier {
		if !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' ||
			c >= '0' && c <= '9' || strings.ContainsRune("-._~", c)) {
			return false
		}
	}
	return true
}

func pkceChallenge(verifier string) string {
	sum := sha256.Sum256([]byte(verifier))
	return base64.RawURLEncoding.EncodeToString(sum[:])
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
