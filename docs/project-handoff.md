# Real-Time Payments Ledger — handoff

## Objective

Build a Go backend that records simulated money movements correctly under
retries, concurrent requests and failures. It is a portfolio ledger, not a bank
integration or payment processor. It uses no real money or card data.

The user has authorized building the project over four weeks, maintaining clear
development/debugging documentation, and pushing verified checkpoints to GitHub.
Weekly CICStep reflections are due Fridays and must reflect actual work.

## Workspace

- Folder: `C:\projects\Real-time-payments-ledger`
- Repository: https://github.com/Kaaushikk/real-time-payments-ledger
- Go module: `github.com/Kaaushikk/real-time-payments-ledger`
- Use existing repository Git identity and factual commit messages.
- An existing uncommitted draft was preserved in local commit `ab894be` before
  editing. The remote was empty when checked on October 30, 2026.
- Current checks and publication status: see `development-log.md` and git status.

## Product behavior

A verified user transfers positive integer USD cents from their own wallet to
another user wallet. Every transfer produces two equal journal entries: debit
reduces the source wallet, credit increases the destination. This is a wallet
liability convention. An issuance account offsets initial demo funding, so total
account balances are zero while user wallets remain non-negative.

A network retry with the same user, operation, key and body returns the original
transaction without moving money again. Reusing the key for different intent
returns an error. Every committed transfer also creates audit records and an
outbox event. Background publication and replay-safe consumption are planned.

## Corrected design

1. **PostgreSQL owns idempotency.** Claim and complete the receipt in the SAME
   transaction as balances, journal, audit and outbox. Redis cannot be the
   correctness boundary: TTL expiry or cache loss must not duplicate money.
   Scope keys to authenticated user plus operation. Include description in the
   request fingerprint. Retain completed receipts beyond their expiry metadata;
   no automated pruning/reuse is implemented. Validation/rejected transfers roll
   back and are not permanently cached. This is not an exact Stripe clone.
2. **Lock accounts in UUID order under READ COMMITTED.** The draft SERIALIZABLE
   strategy exhausted retries under 200 competing transfer goroutines. Ordered
   FOR UPDATE locks protect both mutable rows before authorization/funds checks
   and avoid ABBA deadlocks. READ COMMITTED returns latest committed balances
   after waiting. New range-based business rules or alternative writers require
   a fresh isolation review. The version column is bookkeeping, not optimistic
   concurrency control.
3. **Enforce journal structure at commit.** Deferred PostgreSQL checks require two
   balanced entries with matching transaction amount/currency and distinct
   accounts. Triggers reject UPDATE/DELETE/TRUNCATE on journal, transactions and
   audit. These protect application behavior; a DBA can disable them. Do not
   claim cryptographic immutability or tamper-proof storage.
4. **Authorize, not just authenticate.** Users may debit only their own wallet.
   Account and audit reads require ownership; transaction reads require being a
   participant. Destination wallets may belong to another user.
5. **At-least-once events.** Outbox delivery can duplicate after Kafka acknowledgement
   followed by a database failure. Stable event IDs and consumer receipts must
   make database-side effects replay-safe. Do not claim exactly-once delivery.
6. **Auth needs separate hardening.** PKCE/JWT do not alone solve all CSRF/replay
   threats. Auth codes need atomic consumption/client binding; refresh tokens
   need atomic rotation and reuse handling. Bearer tokens can be replayed if
   stolen. Consider a proven local identity provider rather than overclaiming
   homemade security. Never expose development auth/demo credentials publicly.

## Target architecture

```text
Client -> HTTP gateway / gRPC ledger -> PostgreSQL
             |                           |-- balances and transactions
             |                           |-- journal and audit
             |                           |-- idempotency receipts
             |                           `-- outbox -> publisher -> Kafka -> consumer
             |-- local JWT validation
             `-- Redis (optional rate limiting/cache)

Development auth -> PostgreSQL
Services -> OTel collector -> Jaeger
Services -> Prometheus -> Grafana
Structured logs -> collector/agent -> Loki -> Grafana
Toxiproxy -> controlled dependency-failure experiments
```

This is the TARGET diagram, not proof that every connection works. Local JWT
validation does not call auth over the network on every transfer. Asynchronous
publication requires persisted trace context to connect spans with the request.

## Stack and reasons

Go: typed money operations, cancellation, goroutines, race detector. PostgreSQL:
atomic writes, constraints, locking and durable receipts. pgx: SQL driver.
Protobuf/gRPC: typed API contracts; grpc-gateway: HTTP access to actual RPCs.
Redis: optional protection/performance. Kafka: asynchronous events. OpenTelemetry,
Jaeger, Prometheus, Grafana, Loki: traces, metrics and logs. Docker Compose:
repeatable local integration. Helm/Kubernetes: workload configuration and later
deployment checks. GitHub Actions: automated validation. Toxiproxy: network faults.

## Four-week plan

### Week 1 — November 6, 2026: ledger correctness

Define money rules, ownership, schema/migrations, journal/audit, ordered locks and
atomic durable idempotency. Validate positive integer amounts, supported currency,
UUIDs, bounded keys/descriptions and destination overflow. Add real PostgreSQL
tests: competing transfers, duplicate requests, changed-body conflicts,
overspending, unauthorized spending, old receipt replay, late-failure rollback,
incomplete postings, append-only controls and journal reconciliation. Add CI.

### Week 2 — November 13: API contracts and identity

Generate and register business gRPC handlers and real grpc-gateway routes. Ledger
read and transfer RPCs are registered. Generated routes under /gateway/v1 call
those RPCs. The original /v1 routes serve existing clients. Harden
auth code consumption, refresh rotation, JWT validation, client binding, access
checks and error mapping. Test HTTP/gRPC parity and concurrent token use. Add
Redis only where its outage cannot duplicate or lose postings.

### Week 3 — November 20: events and observability

Verify Kafka outbox publication, stable IDs, schema versions, broker acknowledgements,
bounded retries and replay-safe consumers. Persist and propagate trace context.
Prove Jaeger spans, Prometheus metrics, Grafana dashboards and Loki log ingestion
using actual requests. Existing configuration files are draft scaffolds.

### Week 4 — November 27: failure experiments and delivery

Run Toxiproxy latency/timeout/disconnection/outage/recovery scenarios. Reconcile
money after each case. Build a load harness with correct user tokens, declared
concurrency and recorded failures. Validate Helm rendering and a local Kubernetes
rollout if feasible. Finish reproducible demo, results, limitations, runbook and
resume bullets. Cloud hosting/billing is a separate decision.

## Current implementation

The first session found Go 1.26.7 installed, started Docker Desktop and brought
PostgreSQL 16 and Redis 7 to healthy state. Preserved and reviewed the draft.
Implemented atomic receipt/posting transactions, ownership checks, strict bounded
JSON decoding, USD-only validation, overflow protection, expiration-required JWT
parsing, database readiness, transactional/advisory-locked migration execution,
deferred balanced-posting constraints and demo funding audit rows.

Regression tests were added. Check the log for final outcomes; never assume tests
passed because a test file exists. Database tests create generated test schemas
and remove only those schemas. Unit-only runs skip them without DATABASE_URL.

## API contract for the first milestone

- GET `/healthz`: process health; GET `/readyz`: ledger database readiness.
- POST `/v1/accounts`: authenticated user's USD wallet.
- GET `/v1/accounts/{id}`: owned wallet.
- POST `/v1/transfers`: bearer token, idempotency key, owned source, distinct user
  destination, positive int64 USD cents.
- GET `/v1/transactions/{id}`: participant-only transaction.
- GET `/v1/accounts/{id}/audit`: owned wallet's append-only history.
- GET `/metrics`: development monitoring, restrict access at deployment.

Initial transfer returns 201. Replay returns 200 with the same transaction and
`replayed=true`; this is semantic replay, not byte-identical HTTP replay. Funds
errors return 409; changed-body conflicts 422; unauthorized spends 403; hidden
accounts 404; invalid inputs 400; unexpected server errors use a generic body.

## Known gaps and boundaries

- Auth code exchange and refresh rotation still need concurrency hardening.
- Startup seeds demo users; keys/passwords in config are local examples only.
- Account creation currently uses the handwritten REST API.
- Kafka consumers, bounded publishing retries, async traces and Loki ingestion
  need implementation/verification.
- Existing load script uses one token for both directions; fix before use.
- Chaos scripts and Helm/Kubernetes files are drafts, not passed experiments.
- Audit/upgrade inherited dependency versions before deployment.
- No real bank rails, FX, holds, refunds, reversals, disputes, card data or compliance.

## Evidence and reporting rules

Do not claim 1,000+ concurrent transfers passed, 100% trace coverage, 12 passed
chaos scenarios, three discovered races, or CI/deployment under six minutes from
the proposed resume text. Measure workload-specific results and record hardware,
duration, failures, retries and reconciliation. Do not backdate commits or invent
weekly accomplishments. This project demonstrates backend/distributed systems,
not ML model training.

## Continue safely in another chat

Read this document, the development log and git status first. Keep uncommitted
changes and avoid simultaneous edits from two chats. Continue the next incomplete
milestone. Document decisions and fixes in plain language, run meaningful checks,
inspect the diff for credentials, and push small verified checkpoints. Use the
configured Git identity and factual commit messages. Only describe
implemented behavior and measured results as complete.
