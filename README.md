# Real-Time Payments Ledger

A four-week Go project for correct simulated transfers under retries and
concurrency, with event delivery and full observability planned next.
**Status: Week 1 ledger correctness and Week 2 API/identity acceptance verified. Week 3 event delivery and telemetry are in progress.**

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
generation to update the generated files. The same command generates the REST
bindings from the route map in proto/ledger.gateway.yaml.

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
The Transfer RPC takes an idempotency key in its request and shares the REST
posting service. An identical retry returns the original transaction with
replayed set to true. Changed requests with the same key return AlreadyExists,
insufficient funds return FailedPrecondition, and unauthorized spending returns
PermissionDenied.

## REST gateway

The ledger HTTP listener exposes the generated routes below. They call the
registered gRPC service over its configured listener. Use a bearer access token
and send Idempotency-Key on transfer requests. A request may also supply
idempotency_key in its JSON body. When both are present, the values must agree.

| Method | Route |
|---|---|
| GET | /gateway/v1/accounts/{account_id} |
| GET | /gateway/v1/transactions/{transaction_id} |
| GET | /gateway/v1/accounts/{account_id}/audit |
| POST | /gateway/v1/transfers |

Gateway responses use Protobuf JSON with snake_case field names and string
encoding for int64 values. The gateway uses standard gRPC-to-HTTP status
mapping and returns 200 for successful transfer posts and replays. Existing
/v1 callers can continue to use the handwritten API, including account creation.

The Week 2 acceptance test sends equivalent transfer requests through gRPC
and the generated REST gateway. Successful retries return the same transaction
through either interface. Error bodies from the gateway retain the gRPC code
and message, while HTTP clients also receive a transport-specific status.

| Transfer outcome | gRPC result | Gateway HTTP status |
|---|---|---|
| Missing or invalid bearer token | Unauthenticated | 401 |
| Source wallet owned by another user | PermissionDenied | 403 |
| Idempotency key reused with different data | AlreadyExists | 409 |
| Insufficient funds | FailedPrecondition | 400 |
| Invalid destination ID or missing idempotency key | InvalidArgument | 400 |
| Destination wallet does not exist | NotFound | 404 |

The existing handwritten /v1 routes preserve their own response format and
status codes. PostgreSQL stores idempotency receipts with the ledger posting.
Redis may be present in the development stack, but the receipt store does not
use it for deduplication. The API parity test uses PostgreSQL alone.

## Transfer event contract

A posted transfer writes one JSON event to the PostgreSQL outbox in the same
transaction as the ledger changes and receipt. The default topic is
`ledger.transactions` (configured with `KAFKA_TOPIC`).

| Field | Version 1 value |
|---|---|
| `event_id` | UUID matching the outbox row ID and Kafka message key |
| `event_type` | `ledger.transaction.posted` |
| `schema_version` | Integer `1` |
| `transaction_id` | UUID of the posted ledger transaction |
| `from_account_id`, `to_account_id` | Source and destination wallet UUIDs |
| `amount_minor`, `currency` | Positive signed 64-bit integer amount in cents and `USD` |
| `occurred_at` | UTC timestamp in RFC 3339 format |

Transfer retries use the stored receipt without creating another event.
Publisher retries can redeliver the same event, so consumers should deduplicate
by `event_id`. Existing unversioned outbox rows retain their original JSON
and Kafka key. This contract applies to newly committed transfers.

### Publisher recovery

Create the configured topic before starting the publisher. For the local
Compose broker, start Kafka and run the topic command after it is ready.

```powershell
docker compose up -d kafka
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic ledger.transactions --partitions 1 --replication-factor 1
```

The publisher uses the destination recorded with each event and marks a batch
published only after Kafka acknowledges the write. A failed attempt leaves the
rows pending. Retry delay starts at 400 ms, doubles on consecutive failures,
and caps at 5 seconds. The publisher returns to polling every 200 ms after
success. An acknowledgement followed by a database failure can still lead to
redelivery with the same event ID.

To run the recovery test, set `DATABASE_URL` and `KAFKA_TEST_BROKER` to a
test database and broker reachable from the Go test process. The broker must
advertise an address reachable by that process and allow creation and deletion
of a temporary topic. Run
`go test ./internal/outbox -run TestPublisherRecoversWhenKafkaIsReachable -count=1 -v`.
The test disables the publisher's broker connection, restores it, and checks
the event read back from Kafka.

### Consumer projection

The consumer stores validated version 1 events in `consumed_transfers`.
Each row contains the event ID, transaction ID and typed JSON payload.
PostgreSQL uniqueness checks let repeated deliveries produce one row, including
after a process restart. The consumer also rejects a reused identity with
different transfer data. The ledger balances and journal remain the transfer
service's responsibility.

After creating the topic as described above, start the local consumer with:

```powershell
docker compose up -d --build consumer
```

Its default group is `ledger-transfer-projection-v1`, configured through
`KAFKA_CONSUMER_GROUP`. It processes messages sequentially and synchronously
commits the Kafka offset after database storage succeeds. If the database
commit succeeds and offset acknowledgement fails, redelivery finds the stored
row. The consumer stops on validation, storage or acknowledgement failure.

Malformed JSON, unsupported schema versions, missing or invalid fields and
a Kafka key that differs from the event ID are rejected. Older unversioned
events also stop this consumer for investigation. It reports the partition
and offset without logging the event body. Inspect the failing event and
decide how to correct or migrate it before restarting. Dead-letter routing
and automatic handling of older schemas remain future work.

Run `go test ./internal/ledger -run TestConsumer -count=1 -v` with
`DATABASE_URL`. Set `KAFKA_TEST_BROKER` as well to run the real Kafka test.
That test sends duplicate events followed by a malformed event, restarts the
consumer group, and verifies that the malformed event is still unacknowledged.

### Connected request and event traces

The generated gateway starts an HTTP span and forwards W3C trace context to
the gRPC interceptor. Each committed transfer stores its trace parent and
trace state beside the outbox event. The publisher creates a producer span
under that saved parent and carries it in Kafka headers. The consumer extracts
the headers and creates its processing span.

The metadata uses separate outbox columns, keeping the version 1 event payload
stable. It carries trace context only. Credentials and baggage are excluded.
Older rows default to empty trace fields and can still publish. Missing or
malformed trace context starts a local trace.

The consumer exports through `OTEL_EXPORTER_OTLP_ENDPOINT`, like the ledger.
To verify the connection with a reachable test broker and PostgreSQL, set
`KAFKA_TEST_BROKER` and `DATABASE_URL`. Optionally set
`TRACE_TEST_OTLP_ENDPOINT` to a local Jaeger OTLP gRPC endpoint, such as
`localhost:4317`, then run:

```powershell
go test ./internal/ledger -run 'Test(GatewayPersistsTraceParent|TracePipelineThroughKafka)$' -count=1 -v
```

The Kafka test prints its verified trace ID for lookup in Jaeger. It checks
the parent chain from the HTTP gateway through gRPC, transfer, publication
and consumption. The local verification uses one test process. Separate
service deployment and full telemetry acceptance remain later checks.

### Metrics and dashboard

Prometheus scrapes the ledger's `/metrics` endpoint directly. Transfer counters
use four outcomes: `posted`, `replayed`, `invalid` and `error`. The histogram
measures calls to the transfer service, including internal retries. HTTP
requests rejected before reaching that service are outside these metrics.

The outbox collector queries PostgreSQL during each scrape with a two-second
timeout. It exposes pending-event count and the oldest pending event's age.
An empty outbox has zero age. Query failure reports
`ledger_outbox_collection_success=0` and omits the backlog values for that
scrape. Publication counters and a histogram cover nonempty batch attempts.
The only application metric label is a bounded outcome, keeping account,
transaction, event and retry-key values out of metric labels.

The provisioned Payments Ledger dashboard uses the Prometheus datasource UID
`prometheus`. Its six panels show transfer outcomes, service p99 latency,
pending events, oldest pending age, publication outcomes and collection status.
Start Prometheus and Grafana alongside the local ledger, then open the Payments
Ledger dashboard in the Ledger folder. The default Compose Grafana address is
`http://localhost:3000`. Rates need multiple scrapes and latency quantiles
need requests in the selected interval.

The November 19 local check ran separate ledger, auth, database, Kafka,
Prometheus and Grafana containers. It submitted four HTTP requests covering
the four transfer outcomes, observed one pending event during broker
unavailability, and verified drainage after recovery. Grafana's query API
returned numeric data for all six provisioned panel queries. These small
functional checks establish instrumentation behaviour without estimating
production latency or capacity.

## Remaining milestones

The demo authorization server uses registered public clients and S256 PKCE.
Authorization codes require the registered redirect URI and are redeemed once.
Code consumption and refresh-token creation share one database transaction.
Refresh rotation invalidates the old token and saves its replacement in one
transaction. Reuse of a revoked, unexpired refresh token closes that refresh
session. Previously issued access tokens remain valid until expiry. The demo
uses shared JWT signing configuration and a plaintext gRPC connection for
local development. Keep the demo auth and credentials local.

Log ingestion, full telemetry acceptance and deployment validation are later checkpoints.

No claimed 1,000-user capacity, 100% trace coverage, passed chaos count, cloud
deployment or CI duration. Verified outcomes belong in the development log.
