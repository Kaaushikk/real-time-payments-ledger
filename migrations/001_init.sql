-- Double-entry payments ledger schema.
-- Journal and audit tables are append-only (triggers reject UPDATE/DELETE).

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE users (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email         TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE oauth_clients (
    id            TEXT PRIMARY KEY,
    name          TEXT NOT NULL,
    redirect_uris TEXT[] NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE oauth_auth_codes (
    code_hash              TEXT PRIMARY KEY,
    client_id              TEXT NOT NULL REFERENCES oauth_clients(id),
    user_id                UUID NOT NULL REFERENCES users(id),
    redirect_uri           TEXT NOT NULL,
    code_challenge         TEXT NOT NULL,
    code_challenge_method  TEXT NOT NULL,
    expires_at             TIMESTAMPTZ NOT NULL,
    used_at                TIMESTAMPTZ
);

CREATE TABLE refresh_tokens (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id    UUID NOT NULL REFERENCES users(id),
    client_id  TEXT NOT NULL REFERENCES oauth_clients(id),
    token_hash TEXT NOT NULL UNIQUE,
    parent_id  UUID REFERENCES refresh_tokens(id),
    revoked_at TIMESTAMPTZ,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE accounts (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_user_id UUID REFERENCES users(id),
    currency      CHAR(3) NOT NULL,
    account_type  TEXT NOT NULL CHECK (account_type IN ('user', 'issuance')),
    balance_minor BIGINT NOT NULL DEFAULT 0,
    version       BIGINT NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT user_accounts_non_negative
        CHECK (account_type = 'issuance' OR balance_minor >= 0)
);

CREATE INDEX accounts_owner_idx ON accounts (owner_user_id);
CREATE UNIQUE INDEX accounts_one_issuance ON accounts (account_type) WHERE account_type = 'issuance';
CREATE UNIQUE INDEX accounts_one_user_wallet ON accounts (owner_user_id) WHERE account_type = 'user';

CREATE TABLE ledger_transactions (
    id            UUID PRIMARY KEY,
    type          TEXT NOT NULL CHECK (type IN ('transfer', 'deposit', 'withdrawal')),
    status        TEXT NOT NULL CHECK (status IN ('posted', 'failed')),
    amount_minor  BIGINT NOT NULL CHECK (amount_minor > 0),
    currency      CHAR(3) NOT NULL,
    description   TEXT NOT NULL DEFAULT '',
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE journal_entries (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    transaction_id UUID NOT NULL REFERENCES ledger_transactions(id),
    account_id     UUID NOT NULL REFERENCES accounts(id),
    direction      TEXT NOT NULL CHECK (direction IN ('debit', 'credit')),
    amount_minor   BIGINT NOT NULL CHECK (amount_minor > 0),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX journal_tx_idx ON journal_entries (transaction_id);
CREATE INDEX journal_account_idx ON journal_entries (account_id, created_at);

CREATE TABLE audit_log (
    id              BIGSERIAL PRIMARY KEY,
    account_id      UUID NOT NULL REFERENCES accounts(id),
    transaction_id  UUID REFERENCES ledger_transactions(id),
    event_type      TEXT NOT NULL,
    balance_before  BIGINT NOT NULL,
    balance_after   BIGINT NOT NULL,
    amount_minor    BIGINT NOT NULL,
    metadata        JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX audit_account_idx ON audit_log (account_id, created_at DESC);

CREATE TABLE idempotency_keys (
    key                  TEXT PRIMARY KEY,
    request_fingerprint  TEXT NOT NULL,
    status               TEXT NOT NULL CHECK (status IN ('processing', 'complete', 'failed')),
    response_status      INT,
    response_body        JSONB,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at           TIMESTAMPTZ NOT NULL
);

CREATE INDEX idempotency_expires_idx ON idempotency_keys (expires_at);

CREATE TABLE outbox (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    topic        TEXT NOT NULL,
    payload      JSONB NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);

CREATE INDEX outbox_unpublished_idx ON outbox (created_at) WHERE published_at IS NULL;

CREATE OR REPLACE FUNCTION forbid_mutation() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'table % is append-only', TG_TABLE_NAME;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER journal_entries_immutable
    BEFORE UPDATE OR DELETE ON journal_entries
    FOR EACH ROW EXECUTE PROCEDURE forbid_mutation();

CREATE TRIGGER audit_log_immutable
    BEFORE UPDATE OR DELETE ON audit_log
    FOR EACH ROW EXECUTE PROCEDURE forbid_mutation();

CREATE TRIGGER ledger_transactions_immutable
    BEFORE UPDATE OR DELETE ON ledger_transactions
    FOR EACH ROW EXECUTE PROCEDURE forbid_mutation();
