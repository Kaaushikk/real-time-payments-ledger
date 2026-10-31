package seed

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
)

type Demo struct {
	AliceUser    uuid.UUID
	BobUser      uuid.UUID
	AliceAccount uuid.UUID
	BobAccount   uuid.UUID
	Issuance     uuid.UUID
}

func DemoData(ctx context.Context, pool *pgxpool.Pool) (Demo, error) {
	hash, err := bcrypt.GenerateFromPassword([]byte("password"), bcrypt.DefaultCost)
	if err != nil {
		return Demo{}, err
	}

	var d Demo
	if err := pool.QueryRow(ctx, `
		INSERT INTO users (email, password_hash) VALUES ('alice@ledger.dev', $1)
		ON CONFLICT (email) DO UPDATE SET email = EXCLUDED.email
		RETURNING id
	`, string(hash)).Scan(&d.AliceUser); err != nil {
		return Demo{}, err
	}
	if err := pool.QueryRow(ctx, `
		INSERT INTO users (email, password_hash) VALUES ('bob@ledger.dev', $1)
		ON CONFLICT (email) DO UPDATE SET email = EXCLUDED.email
		RETURNING id
	`, string(hash)).Scan(&d.BobUser); err != nil {
		return Demo{}, err
	}

	_, err = pool.Exec(ctx, `
		INSERT INTO oauth_clients (id, name, redirect_uris)
		VALUES ('ledger-cli', 'Ledger CLI', ARRAY['http://127.0.0.1:8080/callback', 'http://localhost:8080/callback'])
		ON CONFLICT (id) DO NOTHING
	`)
	if err != nil {
		return Demo{}, err
	}

	if err := pool.QueryRow(ctx, `
		INSERT INTO accounts (owner_user_id, currency, account_type, balance_minor)
		SELECT NULL, 'USD', 'issuance', 0
		WHERE NOT EXISTS (SELECT 1 FROM accounts WHERE account_type = 'issuance')
		RETURNING id
	`).Scan(&d.Issuance); err != nil {
		if err := pool.QueryRow(ctx, `SELECT id FROM accounts WHERE account_type = 'issuance' LIMIT 1`).Scan(&d.Issuance); err != nil {
			return Demo{}, err
		}
	}

	d.AliceAccount, err = ensureUserAccount(ctx, pool, d.AliceUser, d.Issuance)
	if err != nil {
		return Demo{}, err
	}
	d.BobAccount, err = ensureUserAccount(ctx, pool, d.BobUser, d.Issuance)
	if err != nil {
		return Demo{}, err
	}
	return d, nil
}

func ensureUserAccount(ctx context.Context, pool *pgxpool.Pool, owner, issuance uuid.UUID) (uuid.UUID, error) {
	var id uuid.UUID
	err := pool.QueryRow(ctx, `
		SELECT id FROM accounts WHERE owner_user_id = $1 AND account_type = 'user' LIMIT 1
	`, owner).Scan(&id)
	if err == nil {
		return id, nil
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return uuid.Nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if err := tx.QueryRow(ctx, `
		INSERT INTO accounts (owner_user_id, currency, account_type, balance_minor)
		VALUES ($1, 'USD', 'user', 1000000)
		RETURNING id
	`, owner).Scan(&id); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			_ = tx.Rollback(ctx)
			err = pool.QueryRow(ctx, `
				SELECT id FROM accounts WHERE owner_user_id = $1 AND account_type = 'user' LIMIT 1
			`, owner).Scan(&id)
			return id, err
		}
		return uuid.Nil, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE accounts SET balance_minor = balance_minor - 1000000, version = version + 1
		WHERE id = $1
	`, issuance); err != nil {
		return uuid.Nil, err
	}
	txID := uuid.New()
	if _, err := tx.Exec(ctx, `
		INSERT INTO ledger_transactions (id, type, status, amount_minor, currency, description)
		VALUES ($1, 'deposit', 'posted', 1000000, 'USD', 'seed funding')
	`, txID); err != nil {
		return uuid.Nil, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO journal_entries (transaction_id, account_id, direction, amount_minor)
		VALUES ($1, $2, 'debit', 1000000), ($1, $3, 'credit', 1000000)
	`, txID, issuance, id); err != nil {
		return uuid.Nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, err
	}
	return id, nil
}

