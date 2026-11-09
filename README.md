# Real-Time Payments Ledger

A four-week Go project for correct simulated transfers under retries and
concurrency, with event delivery and full observability planned next.
**Status: week 1 correctness milestone verified; API/identity, event/telemetry, and failure/deployment milestones remain planned.**

Read the [complete handoff](docs/project-handoff.md),
[four-week plan](docs/four-week-plan.md), and [development log](docs/development-log.md).

For a Notepad-friendly daily record, open [daily progress](docs/daily-progress.txt).

See [Week 1 acceptance evidence and reproduction steps](docs/week-1-acceptance.md).

## Core rules

- Integer USD cents; no floating-point money or real payments.
- Omitted transfer currency defaults to USD before validation and fingerprinting;
  non-USD codes, fractional cents and amounts outside int64 range are rejected.
- Non-negative user wallets; issuance offsets demo funding.
- Two balanced entries per posting, validated at commit.
- READ COMMITTED with ordered FOR UPDATE locks for two-account transfers.
- PostgreSQL receipts, journal, balances, audit and outbox commit atomically.
- User/operation-scoped keys; changed bodies conflict; receipts stay durable.
- Ownership checks on spending and account/audit reads.

## Foundation tests

Requires Go 1.26 and Docker Desktop Linux engine:

```powershell
docker compose up -d --wait postgres redis
$env:DATABASE_URL='postgres://ledger:ledger@localhost:5432/ledger?sslmode=disable'
go test ./... -count=1 -timeout 180s
go vet ./...
```

Without DATABASE_URL, database tests are skipped, not passed. Tests create isolated
generated schemas. Stop local dependencies without deleting data:
`docker compose stop postgres redis`.

## Generate the Protobuf Go packages

From the repository root, run `make proto` or this command in PowerShell:

```powershell
go run github.com/bufbuild/buf/cmd/buf@v1.50.0 generate
```

Buf reads the ledger and auth definitions under `proto/`. The pinned Go plugins
in `buf.gen.yaml` generate message types and gRPC client/server interfaces under
`gen/go/`. Go downloads the tools on first use. Generation needs Go and access
to its module downloads. CI regenerates into an empty directory and compares
the result with the checked-in packages. Edit the `.proto` sources and rerun
generation to update the generated files. The Week 2 plan tracks service
registration and REST gateway wiring separately.

## Friday milestones

| Date (2026) | Scope |
|---|---|
| November 6 | Ledger correctness, idempotency, tests |
| November 13 | Actual gRPC/REST integration and identity hardening |
| November 20 | Kafka consumers and verified telemetry |
| November 27 | Chaos/load evidence, deployment validation, demo |

## Draft boundaries

The initial scaffold includes auth, publishing, Protobuf contracts, observability,
Compose, chaos/load tools, Helm and Kubernetes. Presence is not integration proof.
The ledger gRPC server registers authenticated account, transaction and audit reads.
Send one access token as bearer authorization metadata. Account and audit lookups
are scoped to the owner, and transaction lookups to journal participants.
Missing and inaccessible records return the same `NotFound` response.
Ledger writes currently use the handwritten REST routes. The gRPC write handlers
and generated REST gateway are later Week 2 checkpoints.
OAuth/refresh concurrency, Kafka tracing, log ingestion and full deployment need
later milestones. Do not expose local demo auth/credentials publicly.

No claimed 1,000-user capacity, 100% trace coverage, passed chaos count, cloud
deployment or CI duration. Verified outcomes belong in the development log.
