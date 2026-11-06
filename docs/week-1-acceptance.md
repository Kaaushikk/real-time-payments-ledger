# Week 1 acceptance: ledger correctness

Verified November 6, 2026. This closes the first engineering milestone for the
simulated USD ledger. API contracts/identity, event delivery/observability, and
fault/deployment experiments remain the next three milestones.

## Evidence

- Tested code: [`f105388`](https://github.com/Kaaushikk/real-time-payments-ledger/commit/f10538839d856aaf00fa5cccb6f8b19ce5b07221).
- [Verified GitHub Actions run](https://github.com/Kaaushikk/real-time-payments-ledger/actions/runs/42314059762).
- Formatting, vet, full PostgreSQL suite with the Linux race detector: **passed**.
- Ledger and auth service image builds: **passed**. Builds do not establish a running deployment.
- Local Go/non-database checks passed. Docker's Linux engine was unavailable on
  November 6, so database tests skipped locally and were verified in CI instead.
  The full local PostgreSQL suite had also passed on November 5.

## Acceptance matrix

| Requirement | Verified behavior | Test evidence |
|---|---|---|
| Integer money and currency | USD cents, positive amounts, bounded inputs; omitted currency and explicit USD share a receipt; unsupported currency leaves no state | [validation](../internal/ledger/validation_test.go), [currency](../internal/ledger/currency_test.go), [HTTP inputs](../internal/ledger/http_security_test.go) |
| Competing transfers | Ordered account locks preserve non-negative wallets and total balances; oversubscribed spending cannot overdraw | [concurrency/invariants](../internal/ledger/invariants_test.go), [correctness](../internal/ledger/correctness_test.go) |
| Durable retries | Same-user retries post once; changed request content conflicts; receipt expiry metadata does not authorize a second posting | [correctness](../internal/ledger/correctness_test.go), [storage](../internal/ledger/storage_test.go) |
| Principal isolation | Two users share a raw retry key independently; unauthorized copies cannot reserve or replay another user's posting | [isolation](../internal/ledger/isolation_test.go) |
| Money boundaries | Exact-balance spending, empty-wallet replay, amount limits, destination overflow and retry after rejection have explicit outcomes | [money boundaries](../internal/ledger/money_boundaries_test.go) |
| Complete posting and rollback | Eight malformed journal shapes reject at commit with error 23514 and no partial balances, versions, journal, audit, receipt or outbox records | [journal integrity](../internal/ledger/journal_integrity_test.go) |
| Existing posting integrity | Extra balanced entries cannot extend a posted transaction; tested update/delete/truncate attempts reject; original receipt remains valid | [journal integrity](../internal/ledger/journal_integrity_test.go), [append-only checks](../internal/ledger/storage_test.go) |
| Fresh/repeated migrations | Eight concurrent callers initialize one empty schema; each embedded migration is recorded once; three later applications preserve timestamps, posted data and receipts | [migration acceptance](../internal/ledger/recovery_acceptance_test.go) |
| Lost HTTP response | A committed transfer survives a failed response write; fresh service/store objects recover the persisted receipt, returning the original transaction without another posting | [HTTP acceptance](../internal/ledger/recovery_acceptance_test.go) |
| HTTP workflow and audit | Retry returns 200, changed-body reuse 422, unauthorized spending 403 and insufficient funds 409; one audit entry agrees with source balances; journal and total balances reconcile | [HTTP acceptance](../internal/ledger/recovery_acceptance_test.go) |

## Reproduce the acceptance scenarios

Use the existing development dependencies and run from the repository root:

```powershell
docker compose up -d --wait postgres
$env:DATABASE_URL='postgres://ledger:ledger@localhost:5432/ledger?sslmode=disable'
go test ./internal/ledger -run 'TestConcurrentFreshMigrationsAndRepeatedApplyPreservePostings|TestWeekOneHTTPAcceptanceAfterLostResponse' -v -count=1 -timeout 120s
go test ./... -count=1 -timeout 180s
go vet ./...
```

CI runs `go test -race ./... -count=1 -timeout 180s -coverprofile=coverage.out`
against PostgreSQL 16. The generated test schemas isolate and clean up test data.
Redis is not required for ledger correctness. Without DATABASE_URL, database
tests skip; a local PASS line alone must not be treated as database verification.

The HTTP scenario is an executable in-process demonstration: it sends a transfer
through the authenticated handler, fails the response-body write, rebuilds the
service/store objects, then checks retry/rejection behavior, audit and reconciliation.
Migration concurrency likewise uses simultaneous callers in one test process.

## Limits and next milestone

This is simulated USD accounting, with demo issuance and one wallet per user.
The response-write experiment is not a real TCP outage or a process restart.
Database constraints protect ordinary writes; they do not make the database
tamper-proof against its administrator. These tests establish specific correctness
scenarios, not a throughput, availability or real-money production guarantee.

The current HTTP routes are handwritten. Business gRPC registration, a generated
REST gateway, and authorization-code/refresh/JWT hardening are week 2 work.
Outbox insertion is atomic with a posting; Kafka delivery, consumer replay safety
and connected telemetry still require week 3 verification. Chaos/load experiments,
Helm/local Kubernetes validation and the final demo remain week 4 work. No live
cloud deployment or complete payment-provider integration is claimed.
