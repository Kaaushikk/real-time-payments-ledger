# Development and debugging log

## October 30, 2026 — foundation

Found an uncommitted Go draft in the existing folder; GitHub repo was empty.
Preserved the draft before edits, aligned module paths, started Docker Desktop,
and brought PostgreSQL 16 and Redis 7 to healthy state. Local Go is 1.26.7.

The draft committed transfers before saving retry responses. Moved the receipt
claim/completion into the posting transaction and removed Redis from correctness.
Scoped keys to user/operation and included description in fingerprints. Completed
receipts remain replayable beyond expiry metadata. Added ownership checks,
bounded strict JSON, USD validation, overflow protection, JWT-expiration checks,
and ledger database readiness. Auth code/refresh hardening remains week 2.

Added deferred two-entry/balanced/amount/currency/account checks, TRUNCATE guards,
funding audit rows, and atomic migrations with a transaction-scoped advisory lock.

### Debugging

- Seed audit SQL UNION ALL inferred an untyped UUID as text. Fixed with an explicit
  UUID cast after PostgreSQL rejected it.
- SERIALIZABLE under 200 competing goroutines exhausted bounded retries (40001).
  Replaced it for this two-wallet path with READ COMMITTED plus ordered FOR UPDATE
  locks. Both mutable rows are locked before checking balances; new business rules
  require isolation review.
- Found README claims of business gRPC/gateway behavior that the draft did not
  implement. Marked these as future work rather than completed features.

### Tests

Added concurrent duplicate, overspending, authorization, changed-description,
expired receipt, injected outbox failure/rollback, incomplete posting rejection,
append-only and reconciliation regressions. Tests use isolated generated schemas.
Final local/CI results are appended after checks complete.

### Local verification result

`go test ./... -count=1 -timeout 180s` passed with DATABASE_URL pointing to the
project's PostgreSQL 16 container. Ledger package took 7.047 seconds in that run.
The suite exercised 200 competing transfer goroutines, 32 simultaneous requests
sharing one key (one fresh posting), 24 competing attempts to spend one entire
balance (one successful spend), authorization, rollback, expiry replay and journal
reconciliation. These are correctness-test workloads, not throughput benchmarks.
`go vet ./...` passed. Race detector and container builds are delegated to Linux CI.

Daily development will proceed one bounded improvement at a time through
November 27. Initial scope is a foundation checkpoint, not a finished system.
