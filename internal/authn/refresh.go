package authn

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func (s *Service) Refresh(ctx context.Context, clientID, refreshToken string) (TokenPair, error) {
	if clientID == "" || refreshToken == "" {
		return TokenPair{}, ErrInvalidGrant
	}
	hash := sha256Hex(refreshToken)
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return TokenPair{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	// Serialize rotations and reuse checks for this login session on the same
	// root row. Parent links are set at insertion and are immutable afterward.
	var root uuid.UUID
	err = tx.QueryRow(ctx, `
		WITH RECURSIVE ancestors AS (
			SELECT id, parent_id FROM refresh_tokens
			WHERE token_hash = $1 AND client_id = $2
			UNION ALL
			SELECT p.id, p.parent_id FROM refresh_tokens p
			JOIN ancestors a ON p.id = a.parent_id
		)
		SELECT t.id FROM refresh_tokens t
		JOIN ancestors a ON t.id = a.id
		WHERE a.parent_id IS NULL
		FOR UPDATE OF t
	`, hash, clientID).Scan(&root)
	if errors.Is(err, pgx.ErrNoRows) {
		return TokenPair{}, ErrInvalidGrant
	}
	if err != nil {
		return TokenPair{}, err
	}

	// Read again after acquiring the lock to see the last rotation's result.
	var id, userID uuid.UUID
	var revoked *time.Time
	err = tx.QueryRow(ctx, `
		SELECT id, user_id, revoked_at FROM refresh_tokens
		WHERE token_hash = $1 AND client_id = $2 AND expires_at > clock_timestamp()
	`, hash, clientID).Scan(&id, &userID, &revoked)
	if errors.Is(err, pgx.ErrNoRows) {
		return TokenPair{}, ErrInvalidGrant
	}
	if err != nil {
		return TokenPair{}, err
	}
	if revoked != nil {
		_, err := tx.Exec(ctx, `
			WITH RECURSIVE family AS (
				SELECT id FROM refresh_tokens WHERE id = $1
				UNION ALL
				SELECT t.id FROM refresh_tokens t JOIN family f ON t.parent_id = f.id
			)
			UPDATE refresh_tokens SET revoked_at = clock_timestamp()
			WHERE id IN (SELECT id FROM family) AND revoked_at IS NULL
		`, root)
		if err != nil {
			return TokenPair{}, err
		}
		if err := tx.Commit(ctx); err != nil {
			return TokenPair{}, err
		}
		return TokenPair{}, ErrTokenReuse
	}
	if _, err := tx.Exec(ctx, `UPDATE refresh_tokens SET revoked_at = clock_timestamp() WHERE id = $1`, id); err != nil {
		return TokenPair{}, err
	}
	pair, err := s.issueTokens(ctx, tx, userID, clientID, &id)
	if err != nil {
		return TokenPair{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return TokenPair{}, err
	}
	return pair, nil
}
