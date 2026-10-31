-- Validate complete journal transactions at COMMIT, after both entries exist.
-- This is application-role protection, not tamper-proof storage against a DBA.
CREATE FUNCTION validate_posting() RETURNS trigger AS $$
DECLARE target UUID;
DECLARE n BIGINT;
DECLARE net NUMERIC;
BEGIN
    IF TG_TABLE_NAME = 'ledger_transactions' THEN target := NEW.id;
    ELSE target := NEW.transaction_id; END IF;
    SELECT count(*), coalesce(sum(CASE WHEN direction='credit' THEN amount_minor::numeric ELSE -amount_minor::numeric END),0)
    INTO n, net FROM journal_entries WHERE transaction_id=target;
    IF n <> 2 OR net <> 0 THEN
        RAISE EXCEPTION 'posting must contain two balanced entries' USING ERRCODE='23514';
    END IF;
    IF EXISTS (
        SELECT 1 FROM journal_entries e JOIN accounts a ON a.id=e.account_id
        JOIN ledger_transactions t ON t.id=e.transaction_id
        WHERE e.transaction_id=target AND (e.amount_minor<>t.amount_minor OR a.currency<>t.currency)
    ) OR (SELECT count(DISTINCT account_id) FROM journal_entries WHERE transaction_id=target)<>2 THEN
        RAISE EXCEPTION 'posting amounts, currency or accounts invalid' USING ERRCODE='23514';
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER check_transaction_posting
AFTER INSERT ON ledger_transactions DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_posting();

CREATE CONSTRAINT TRIGGER check_journal_posting
AFTER INSERT ON journal_entries DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION validate_posting();

CREATE TRIGGER journal_no_truncate BEFORE TRUNCATE ON journal_entries
FOR EACH STATEMENT EXECUTE FUNCTION forbid_mutation();
CREATE TRIGGER audit_no_truncate BEFORE TRUNCATE ON audit_log
FOR EACH STATEMENT EXECUTE FUNCTION forbid_mutation();
CREATE TRIGGER transactions_no_truncate BEFORE TRUNCATE ON ledger_transactions
FOR EACH STATEMENT EXECUTE FUNCTION forbid_mutation();
