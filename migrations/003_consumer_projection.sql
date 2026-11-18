CREATE TABLE consumed_transfers (
    event_id UUID PRIMARY KEY,
    transaction_id UUID NOT NULL UNIQUE,
    payload JSONB NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TRIGGER consumed_transfers_append_only
    BEFORE UPDATE OR DELETE OR TRUNCATE ON consumed_transfers
    FOR EACH STATEMENT EXECUTE FUNCTION forbid_mutation();
