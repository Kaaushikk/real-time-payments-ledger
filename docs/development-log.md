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

Foundation CI completed successfully, including Linux race checks and both
container builds:
https://github.com/Kaaushikk/real-time-payments-ledger/actions/runs/41161990399

Daily plain-text journal: `docs/daily-progress.txt` (open in Notepad). Each session
records actual changes, reasons, debugging, test results, commit/push status,
blockers and the next task.

## October 31, 2026 — stable validation errors

Today's bounded week 1 task was making invalid-request responses reliable.
Previously, HTTP classification searched error-message text for phrases such as
"description exceeds". An unrelated internal failure containing that phrase
could be misreported as a client error. Separately, attempting to create a
non-USD wallet returned 500 even though unsupported currency is invalid input.

Introduced `ledger.ErrInvalidTransfer`, wrapped validation reasons using `%w`,
and switched the HTTP boundary to `errors.Is`. Account creation now uses the
same domain-error mapping as other ledger routes. Unexpected failures still
produce a generic 500 response without exposing the underlying message.

Added tests for zero/negative amounts, missing keys, unsupported currency,
wrapped validation errors, JSON error bodies, and an internal error whose message
resembles a validation error. The PostgreSQL-backed account test now verifies
that an unsupported wallet currency returns 400.

Focused regressions passed. The full suite passed with the real PostgreSQL
DATABASE_URL and `-count=1 -timeout 180s`; ledger package duration was 11.711s.
`go vet ./...` and `git diff --check` also passed. No throughput claim is inferred.
Next: examine currency/default and money-boundary behavior, keeping work in week 1.


## November 1, 2026 — currency defaults and money input boundaries

Continued the week 1 correctness milestone with one small change. The REST route
defaulted an omitted currency to USD, but the underlying Transfer service rejected
it. Moved this normalization into Transfer, before validation and fingerprinting,
so service callers and HTTP requests follow the same contract. Removed the
duplicated HTTP default. Unsupported currencies, lowercase and whitespace-padded
codes remain invalid; this does not introduce currency conversion.

Added PostgreSQL regressions that replay omitted currency against explicit USD
in both directions and check that only two intended postings affect the source
balance. Another regression checks rejected currencies leave balances, versions,
journal transactions, audit records, outbox and idempotency receipts unchanged,
then confirms a corrected request can reuse the rejected request's key. Added
HTTP cases rejecting fractional cents and an amount above signed int64 range.

Focused local checks passed. `go test ./... -count=1 -timeout 180s`, `go vet ./...`
and `git diff --check` passed locally. DATABASE_URL was not set for that local
run, so database tests were skipped, not validated locally. Docker Desktop failed
to initialize a local socket and its Linux engine did not start. No data reset
or socket deletion was performed. The full PostgreSQL and Linux race checks
subsequently PASSED in GitHub CI for code commit `35ead2c`, along with formatting,
vet, and both container image builds:
https://github.com/Kaaushikk/real-time-payments-ledger/actions/runs/41355381615

Next small task: strengthen transfer authorization/idempotency isolation tests
between different users, remaining within week 1. The daily schedule is now a
check-in only; project work starts after a user message.


## November 3, 2026 — cross-user retry isolation

Added three PostgreSQL-backed regressions in `internal/ledger/isolation_test.go`
to verify the existing user-scoped durable-idempotency boundary. This checkpoint
adds correctness evidence; it does not add a new payment feature or change the
posting implementation.

The concurrent case starts eight calls per user against the same raw retry key.
Alice sends 11 minor units and Bob sends 7 in the opposite direction. Assertions
require a distinct transaction for each user, exactly one fresh response per
user, a net four-unit movement, two receipts/events and four journal/audit rows.
This is a correctness scenario, not a throughput benchmark.

The authorization case copies Alice's exact request but authenticates it as Bob,
both before and after Alice's posting. It requires rejection without a returned
transaction or changes to either balance, version, receipt, journal, audit or
outbox counts. A rejected attempt must not reserve the key: Bob can subsequently
use the same raw key for his own authorized transfer, and Alice can still replay
her original receipt. The HTTP case uses separately signed test tokens to verify
403 rejection, distinct 201 postings, and each user's own 200 replay. Rejected
responses must not expose cached transaction details.

Local `go test ./... -count=1 -timeout 180s` and `go vet ./...` passed. No local
DATABASE_URL is configured, so PostgreSQL cases were skipped locally. Docker's
Linux engine pipe remains unavailable; no reset or data deletion was attempted.
The existing GitHub Actions job runs all tests against PostgreSQL with the Linux
race detector, then builds both service images. Its outcome is pending at this
commit and must be checked before calling this milestone verified.

November 2 had a check-in reminder but no development session. The unfinished
isolation task was carried forward to today's user-started session. The personal
task tracker remains local and ignored by Git. Next: money boundary cases,
including exact-balance spending and destination overflow, within week 1.


## November 4, 2026 — money boundaries and overflow recovery

First reconfirmed that November 3's `fabf33a` is on origin/main and its full
PostgreSQL/race suite and both image builds passed:
https://github.com/Kaaushikk/real-time-payments-ledger/actions/runs/41742651003
The private tracker still showed that checkpoint pending, so its evidence was
brought up to date. No work was backdated.

Added three regressions in `internal/ledger/money_boundaries_test.go`:

- Spending the exact available balance succeeds once, leaves the source at zero,
  and remains replayable even though that wallet can no longer fund a new spend.
- Minimum int64, negative and zero amounts are invalid. One above the available
  balance and MaxInt64 are valid integer inputs but unaffordable in the ordinary
  fixture. Rejection leaves balances, versions, receipts, journal, audit and
  outbox counts unchanged; a corrected request may reuse the rejected key.
- A destination can reach exactly MaxInt64. Another unit fails with the domain
  overflow error without partial changes. After a reverse transfer makes room,
  the previously rejected request/key can succeed. The original successful
  receipt also replays safely while the destination is full.

The extreme fixture leaves two units with Alice and uses a balanced test-only
deposit to bring Bob to MaxInt64-1. The issuance balance reaches MinInt64, so
the setup still conserves money and every balance matches its journal. Funding
exists only inside the generated test schema; no funding endpoint was added.
Reconciliation uses PostgreSQL NUMERIC arithmetic to avoid overflowing an
intermediate sum while checking int64 account balances.

`gofmt`, `go test ./... -count=1 -timeout 180s`, `go vet ./...` and the diff check
passed locally. The focused invocation explicitly skipped all three new cases
because DATABASE_URL is unset. Docker's Linux engine remains unavailable; no
reset or deletion was attempted. Real PostgreSQL, Linux race and image-build
results are pending the existing GitHub Actions job at commit time. These are
boundary-correctness tests, not performance measurements. No production-code
defect has been demonstrated by local checks alone.

Next bounded task: journal integrity cases on November 5. Migration/retry review
and the weekly acceptance review are planned for Friday; any unfinished work
must be carried forward and reflected accurately.


## November 5, 2026 — deferred journal integrity

Confirmed November 4's `1dfede7` and its successful PostgreSQL/race checks and
both service builds:
https://github.com/Kaaushikk/real-time-payments-ledger/actions/runs/41919940618

Docker was available at session start. Started the existing project PostgreSQL
container, preserving its data. The first readiness probe rejected connections
during startup; the subsequent probe succeeded. No reset, volume deletion or
change to the unrelated running database was needed.

Added `internal/ledger/journal_integrity_test.go`. Eight malformed postings cover
missing entries, a single entry, unequal amounts, two debits, a balanced pair
with the wrong transaction amount, currency mismatch, the same account on both
sides, and four balanced entries. Each test deliberately stages balances,
versions, journal rows, audit records, an outbox event and a processing receipt.
It then requires PostgreSQL check-constraint error 23514 at COMMIT, not an earlier
SQL or connection error, and verifies that every staged financial side effect
was rolled back. Existing balances must still match the journal and sum to zero.

A separate regression starts with a valid service transfer, tries to append an
extra balanced pair, and requires rejection at commit. It verifies the original
transaction's full content is unchanged and its original receipt still replays
without changing balances or records. Deferred validation allows the two valid
entries to be assembled in one transaction while enforcing completeness at its
boundary. No production-code change was required for these cases.

Focused PostgreSQL checks passed locally (ledger package 5.430s). The complete
`go test ./... -count=1 -timeout 180s` suite also passed with DATABASE_URL set;
ledger package duration was 18.426s. `go vet ./...`, formatting and diff checks
passed. These durations describe this local correctness test run, not throughput.
Linux race checks and image builds remain pending CI at commit time. Generated
test schemas isolate all deliberately invalid data and are cleaned up by the
existing fixture. The local PostgreSQL container remains running for development.

Next: November 6 migration/retry review and week 1 acceptance/reflection. API
contracts, identity hardening and the later event/telemetry milestones remain
on their respective weeks. The personal task tracker remains ignored locally.


## November 6, 2026 — migration and lost-response acceptance checks

Confirmed November 5's `749bfc9` and its successful PostgreSQL/race checks and
both service builds:
https://github.com/Kaaushikk/real-time-payments-ledger/actions/runs/42168512064

Extracted the existing isolated-schema setup into `unmigratedPool`, preserving
the ordinary fixture's migration/seed behavior. This lets the new migration test
begin with an empty schema instead of exercising only an already migrated one.
Eight callers begin Apply from the same gate. The test requires all to succeed
and every embedded SQL migration to have one record. After seeding and posting a
transfer, three repeated Apply calls must preserve migration timestamps, ledger
records and the original durable receipt. Reconciliation checks the journal and
total balances afterward.

Added an in-process HTTP acceptance scenario. A response writer returns a closed-
pipe error after the handler commits a transfer. The test checks the committed
side effects, constructs fresh ledger/store objects, and retries the original
request. It requires HTTP 200 replay of the persisted transaction, HTTP 422 for a
changed body, HTTP 403 for unauthorized spending, and HTTP 409 for overspending,
without any further financial changes. The audit endpoint must show exactly one
record for the transfer with matching before/after balances. This simulates a
failed response write; it is not a real TCP fault or a process-crash experiment.

Docker's Linux engine pipe was unavailable today. Attempted a normal background
Docker Desktop start; the engine remained unavailable. No reset, volume deletion,
process termination or unrelated database use was attempted. Local Go compilation,
non-database tests, vet and formatting/diff checks passed; the two new database
tests explicitly skipped without DATABASE_URL. PostgreSQL/race checks and image
builds are pending GitHub CI at this commit. Week 1 acceptance is only complete
after that run passes; verified results will be recorded in a separate acceptance
document after CI. The personal daily tracker remains excluded from Git.


### November 6 acceptance outcome

The full PostgreSQL suite and Linux race checks **passed** for code commit
`f105388`, including the fresh concurrent migration and lost-response HTTP cases.
Formatting/vet and both service image builds also passed:
https://github.com/Kaaushikk/real-time-payments-ledger/actions/runs/42314059762

Recorded the scope, test mapping, reproduction steps and limitations in
[week 1 acceptance](week-1-acceptance.md). This closes the week 1 engineering
checkpoint. No production-code change was required by today's recovery cases.
Docker availability is again a local environment limitation; database behavior
was verified in CI, not represented as locally tested today. Next: week 2 API
contracts and identity hardening. The weekly reflection was drafted in the chat;
external form submission is controlled by the user and is not independently verified.


## November 7, 2026 — reproducible Protobuf generation

The draft had workspace configuration in the generation template, an outdated
module name, and a Make target declaration without a recipe. Configured `buf.yaml`
to read the existing contracts from `proto/`. Pinned Buf 1.50.0, the Go message
generator 1.36.1, and the Go gRPC generator 1.5.1. The local plugins run through
Go and generate source-relative paths matching the contracts' Go import paths.
The existing Protobuf runtime version is now a direct dependency because the
generated packages import it.

Generated four Go files for the ledger and auth message types and gRPC
client/server interfaces. The API schemas are unchanged. `make proto` generates
the packages, and the README includes the equivalent PowerShell command. CI
uses a fresh output directory and compares the complete generated tree with the
repository. This catches outdated output and files left over after schema changes.

Buf's first run downloaded and built its pinned dependencies. Both generation
runs produced identical bytes. `go build ./gen/go/...`, `go vet ./...`, generated
source formatting and `git diff --check` passed. The full `go test ./... -count=1
-timeout 180s` suite passed with local PostgreSQL. Started the existing project
database container and retried readiness after it finished starting.

The new CI generation check and existing Linux PostgreSQL/race checks and builds
are pending at commit time. Next task is to connect the generated read RPCs to
the ledger service and verify ownership checks. The private tracker stays local.


## November 8, 2026 — authenticated ledger read RPCs

Registered GetAccount, GetTransaction and ListAuditLog on the ledger gRPC
server. Each handler derives the caller from one bearer access token in gRPC
metadata. The existing JWT parser validates the token, and the subject must
identify a nonzero UUID. The handler validates the resource UUID before reading.

Moved wallet ownership and transaction participant rules into the ledger service
for both HTTP and gRPC. An account and its audit records belong to its owner.
Either journal participant can read a transfer. Reads resolve within the caller's
visible collection, so missing and inaccessible IDs share a NotFound response.
Authentication failures return Unauthenticated, malformed IDs return
InvalidArgument, and unexpected read errors return a generic Internal status.

Generated-client tests use a real gRPC server over an in-memory connection.
They cover missing, malformed and duplicate authorization, a signed token with
an invalid subject, account ownership, both transfer participants, an unrelated
caller, malformed IDs and audit limit and balance mapping. The access tests use
isolated PostgreSQL schemas. The local Linux Docker engine became available after
starting the installed Docker Desktop, and the project's existing database
accepted connections after startup. Local full PostgreSQL tests, vet and diff
checks passed. Linux regeneration, race checks and image builds are verified
after the push. The next checkpoint is the transfer RPC.


## November 9, 2026 — authenticated transfer RPC

The generated Transfer RPC validates bearer metadata and account UUIDs, then
passes the caller and request fields to the existing PostgreSQL posting service.
The request's idempotency key uses the same caller-scoped receipt as REST. The
response shares the transaction mapping used by GetTransaction and includes the
replay flag. Transfer errors use gRPC statuses for invalid input, missing records,
ownership, changed-key conflicts, in-flight retries and insufficient funds.

The generated-client test posted a transfer, retried it through gRPC with explicit
USD, and retried it through the existing REST handler with the same key. All
responses identified the same transaction. Missing credentials, unauthorized
spending, changed requests, insufficient funds, bad and missing account IDs,
and missing keys returned the expected statuses. A database snapshot confirmed
one posting, receipt and outbox event, two journal and audit rows, the expected
balance changes and reconciliation.

The first test compile found that the pinned Protobuf runtime lacks CloneOf.
Used the supported Clone API for test requests and reran the full local suite
against the healthy project PostgreSQL container. All tests, vet and diff checks
passed. GitHub verifies regeneration, PostgreSQL race tests and image builds
after the push. Next is generated REST gateway integration.


## November 10, 2026 — generated REST gateway

Pinned the gateway generator to the existing grpc-gateway runtime version.
The YAML route map generates account, transaction, audit and transfer bindings.
The ledger process mounts them under /gateway/v1 and dials its own registered
gRPC listener. The original /v1 paths continue to serve existing clients.
The new routes use Protobuf JSON and the standard gRPC-to-HTTP status mapping.

The gateway forwards the bearer header through the runtime and allows
Idempotency-Key as application metadata. The transfer handler accepts a key
from the body or metadata and rejects conflicts and duplicate headers.
The HTTP boundary rejects non-printable metadata values before the runtime
could discard them. The gateway bounds request bodies and rejects unknown
JSON fields.

The PostgreSQL integration test called all four routes, retried a gateway
transfer over native gRPC and then HTTP, and verified one durable receipt and
one reconciled posting. It checked authorization, ownership, key conflicts,
malformed requests and the existing health route. PostgreSQL was healthy.
Clean regeneration matched all five Go files byte for byte. The full local
PostgreSQL suite, vet and diff checks passed. The push triggers Linux
regeneration, PostgreSQL race tests and image builds. Next is the authorization
code safety checkpoint.


## November 12, 2026 — authorization-code safety

The demo authorization server uses registered public clients with S256 PKCE.
Issuance requires a registered, nonempty redirect URI and a canonical S256
challenge. Exchange validates the verifier format and conditionally consumes
a live code bound to the same client, redirect URI and derived challenge.
PostgreSQL claims the code and persists the refresh token in one transaction,
so a storage failure rolls the claim back and a concurrent redemption can
succeed only once. The server returns tokens after commit.

Tests checked the RFC 7636 example and verifier bounds. PostgreSQL tests
verified client and redirect binding, invalid verifiers, expired codes,
eight simultaneous exchanges with one successful issuance, and recovery
after a simulated refresh-token storage failure. The full local suite and vet
passed. Docker Desktop started from the existing user installation and the
project PostgreSQL container accepted connections. Next is refresh-token
rotation and JWT acceptance.

## November 13, 2026 — refresh rotation and JWT validation

Refresh requests lock the original token row for the login session. The service
then rereads the presented token, invalidates it, and inserts the replacement
with its parent link in the same PostgreSQL transaction. The root lock also
serializes reuse checks with rotations of descendant tokens. Reuse of a
revoked, unexpired token revokes any active refresh tokens in the same session.
A separate login for the same account and client can still refresh. Existing
access tokens are checked locally and expire on their original schedule.
The shared JWT parser now also requires a nonzero UUID subject.

The first database check found that the existing PostgreSQL container was
stopped. After starting it and waiting for readiness, the focused tests passed.
They verified one issuance across eight simultaneous refresh requests, session
isolation, revocation across multiple generations, client binding, expiry, and
rollback after a failed replacement insert. JWT tests checked a valid token and
rejections for missing or expired claims, wrong issuer or audience, an invalid
signature, an unsupported algorithm and missing or invalid subjects. The full
local PostgreSQL suite, Go vet and diff checks passed. Next is Week 2 API
acceptance.

## November 14, 2026 — Week 2 API acceptance

The generated gateway test now uses the same Protobuf request for each of eight
rejection cases across direct gRPC and REST. It checks both the HTTP status
and the gateway's error body against the direct gRPC response. Insufficient
funds maps to HTTP 400 through the gateway's standard FailedPrecondition
mapping. The existing handwritten routes keep their response contracts.

Successful retries recover an identical transaction, including the journal
entries. After the retries and rejected requests, PostgreSQL shows one transfer
and receipt, two journal entries and audit records, one outbox event, and
reconciled balances. The focused integration test, full PostgreSQL suite, vet,
and diff checks passed. The README records the status mappings, local auth
limits, and PostgreSQL's role as the durable idempotency store. The Week 3
event-contract checkpoint is next.

## November 16, 2026 — versioned transfer event contract

New transfer events use a typed version 1 JSON payload. The event UUID is
generated when the transfer is posted and saved as both the outbox row ID and
the payload event ID. The publisher already uses the row ID as the Kafka key.
The change adds two fields to the existing payload and leaves older stored
events intact. The README records the v1 contract and retry semantics.

The PostgreSQL test forced receipt completion to fail after the outbox insert.
It verified rollback of balances, journal, audit, receipt and event together.
After removing the test constraint, the same request committed and its replay
left one pending outbox row. The test checked the JSON field names and values,
then reconciled balances. Docker Desktop and the existing PostgreSQL container
needed to start. The focused and full PostgreSQL suites, vet and diff checks
passed. Broker retry and recovery verification is the next checkpoint.

## November 16, 2026 — publisher recovery checkpoint

The publisher sends to the topic stored with the event, requires Kafka
acknowledgement from all in-sync replicas and uses one Kafka write attempt
before handing failures back to the outbox loop. It bounds the wait for
publication, leaves failed rows pending and backs off on repeated failures.
Acknowledgement and the PostgreSQL update are separate operations, so a retry
can still deliver a duplicate.

The opt-in integration test used PostgreSQL and a temporary single-node
Apache Kafka 3.7.1 broker on a loopback port. The publisher's dialer refused
connections during the outage phase. The row stayed pending, and after the
test restored connectivity, the same publisher retried. PostgreSQL showed
the row marked published and Kafka returned the original key and JSON body.
The full suite with both services, vet and diff checks passed. This verifies
the single-node recovery path. Consumer deduplication is next.

## November 17, 2026 — replay-safe consumer

Added a consumer command, Compose service and an append-only PostgreSQL
projection for version 1 transfer events. One insert stores the projection and
its unique event and transaction identities. Repeated identical deliveries
succeed; conflicting data with reused identity fails. Processing is sequential,
and Kafka acknowledgement happens only after successful database processing.
Invalid events or failures stop the worker before it reads a later event.

PostgreSQL tests verified eight concurrent duplicates, restart/replay, conflicting
identities, validation failures, database failure and acknowledgement failure.
The consumer left ledger balances, journal, audit and outbox state unchanged.
An initial Compose insertion matched a dependency name as well as the service
name; narrowing the edit to the service block resolved it. Compose validation
passed.

The Kafka test used a temporary Apache Kafka 3.7.1 broker and a consumer group.
Two copies produced one projection. A malformed third event stopped processing,
and a fresh group reader resumed at that same failed offset. The complete
local suite ran with Kafka and PostgreSQL, including publisher recovery.
Go vet, Compose validation and diff checks passed. The temporary broker was
removed. CI also builds the consumer image. Trace propagation is next.

## November 18, 2026 — request-to-consumer trace propagation

The generated gateway now creates an HTTP span and injects trace context into
gRPC metadata. The server interceptor extracts that parent. A new migration
adds traceparent and tracestate to the outbox. The transfer saves those values
atomically with its event. Each publisher event span uses its own stored parent,
and Kafka headers carry the producer context to the consumer. Consumer processing
records its span and errors. The consumer command initializes and shuts down
the configured OTLP exporter.

Focused tests checked gateway/gRPC/transfer parent relationships, persistence,
Kafka propagation and exclusion of baggage. A local Kafka integration test
exported to a temporary Jaeger instance. Its API returned trace
a10c679360b94b8a3ba97354a27b9cc9 with all seven expected spans and the parent
chain from test request through gateway, gRPC, transfer, transfer attempt,
publisher and consumer. This verification used a single test process.
The complete local PostgreSQL/Kafka/Jaeger suite, vet, Compose validation and
diff checks passed. Temporary containers were removed. Metrics and dashboard
verification is the next checkpoint.

## November 19, 2026 — Prometheus metrics and Grafana verification

Added PostgreSQL-backed gauges for outbox backlog and oldest pending age.
A failed query exposes collection failure and omits backlog values. Nonempty
publication batches now record outcome and duration. Transfer metric help text
defines the service boundary; cancellation during retry backoff reaches the
shared error counter path. Outcome label values are fixed and initialized.

The dashboard now uses an explicit datasource UID and six panels covering
transfer outcomes, p99 service duration, backlog, pending age, publication
outcomes and database collection status. Prometheus scrapes the implemented
ledger endpoint. Regression tests verified all four transfer outcomes through
HTTP, histogram observations, backlog growth/drainage and collection failure.

An isolated local Docker stack ran the actual auth/ledger images with
PostgreSQL, Kafka, Prometheus and Grafana. HTTP returned 201, 200, 400 and 409
for the posted, replayed, invalid and insufficient-funds requests. Before Kafka
started, Prometheus showed one pending event and two failed batch attempts.
After broker startup and topic creation, it showed zero pending events and
one successful batch. Grafana's datasource query API returned numeric values
for each of the six provisioned panels. This verified the dashboard queries.
The full PostgreSQL suite, vet, Compose and diff checks passed. Temporary
verification resources were removed. Correlated log ingestion is next.

## November 20, 2026 — correlated logs and Week 3 acceptance

Installed logging packages matching OpenTelemetry 1.33: otelslog 0.8 and the
0.9 logging SDK/exporter. Logs go to stdout and a bounded OTLP batch processor.
Transfer, publisher and consumer events carry trace correlation fields and
selected record IDs/outcomes. The collector uses Loki's native OTLP endpoint.
The unit test verified matching stdout/native OTLP trace IDs and exclusion of
context baggage from stdout.

Added a reproducible isolated acceptance stack and PowerShell script. Separate
service processes verified receipt replay, one pending event during broker
unavailability, recovery, forced redelivery and one consumer projection.
Balances reconciled with no negative user wallets. Loki found ledger/consumer
records by the shared trace ID. The sampled logs omitted both returned tokens
and the private description. Jaeger parent relationships and all six Grafana
panel queries passed; Grafana could also query the Loki records.

The initial local regression stalled on PostgreSQL connection close; its test
and the full suite passed on retry. The initial acceptance attempt reached
the p99 panel after a long gap and found no current sample. Range queries now
anchor to scenario time. The successful repeat and evidence are recorded in
week-3-acceptance.md. Temporary resources were removed. Collector/Compose
validation, vet and diff checks passed. Week 4 failure experiments are next.

## November 22–23, 2026 — Toxiproxy harness and baseline

Completed the Week 4 baseline checkpoint carried forward from November 21.
A Compose override reuses the acceptance stack and routes only the ledger's
database connection through an isolated PostgreSQL proxy. The new script
requires an enabled proxy with no toxics and uses separate user tokens for
the forward and reverse transfers.

The first run cleaned up after readiness failed. A small container probe
showed HTTP 403 from Toxiproxy's browser-user-agent guard. The harness now
uses its own API client identifier. The repeat passed: three sequential
requests created two transfers and two receipts, four journal entries, four
audit records, two events and two projections. Balances changed by minus/plus six cents, reconciled,
and remained nonnegative for users. PostgreSQL showed two connections from
the proxy. No events remained pending.

The measured JSON includes UTC timestamps and individual request durations.
This is a functional baseline with no injected fault. PowerShell parsing and
merged Compose validation passed. Docker Desktop needed starting, and all
temporary baseline resources were removed. Next is bounded database delay
and disconnect testing.

## November 23, 2026 — database latency, timeout and disconnect

Extended the existing fault harness with an opt-in database scenario, leaving
the baseline command and its result file intact. Each case injects one fault,
records the initial state, clears the fault, retries the identical request,
and confirms a further replay. Recovery attempts are bounded and recorded.

The live run passed all three cases. A 100 ms downstream delay allowed the
transfer to complete. A 300 ms downstream drop/close returned HTTP 500 with
no committed transfer. For the disconnect, the harness observed an assigned
PostgreSQL transaction ID on a proxied idle-in-transaction session before
cutting the connection; the request returned 500 and rolled back. Restoring
the connection allowed each interrupted request to commit once. All recovered
receipts replayed the same transaction, with matching table deltas and events.

The disconnect uses asynchronous HttpClient so direct database polling can
run while the request is active. The initial and recovery bodies share the
same description to preserve the idempotency fingerprint. This checkpoint
needed no application-code change. The live run passed on its first attempt;
the prior baseline's user-agent fix remains in use.

Final state: five transfers, five receipts, five outbox events/projections,
zero pending events, no negative user balances and zero journal mismatches.
The temporary Docker project was removed. The JSON records individual
observations and UTC times; these are small functional experiments, not load
results or exhaustive crash tests. PowerShell parsing, merged Compose and
diff checks passed. Next is the broker/cache outage checkpoint.

## November 24, 2026 — broker outage and process-independent replay

Added an opt-in broker scenario to the existing fault harness. It stops Kafka,
posts a 37-cent transfer, verifies its receipt replay and one pending event,
and requires a publication error. It restarts the ledger while Kafka is still
stopped to verify durable receipt replay and retention of the event identity,
schema version and payload. Redis remains outside the receipt implementation;
the harness runs with no Redis service and claims no cache failover.

The first live run passed. The consumer was still running after the broker
outage; the harness explicitly restarted it after broker readiness as a
controlled recovery step. The original event drained into one projection.
Resetting that event's publication marker caused a second actual Kafka
delivery: partition zero, offsets two and three. The second was logged as a
duplicate. Counts, balances and journal reconciliation were unchanged.

The complete run used seven transfer requests and finished with three
transfers, receipts, outbox events and consumer projections. No events remained
pending, user balances were nonnegative and journal totals matched balances.
No application-code change was needed. The temporary Docker project was
removed. PowerShell parsing, merged Compose and diff checks passed. Evidence
and limitations are in broker-faults.md and results/broker-faults.json.
Next is a scoped load experiment with correct user-token handling.

## November 25, 2026 — scoped load runner and measured reconciliation

Replaced the draft one-token load tool with a bounded two-owner workload.
Both wallet identities are checked before measurement. The runner validates
status/receipt content, confirms replay IDs, rejects duplicate IDs for new
keys, bounds client timeouts and reports errors and nearest-rank latencies.
Added targeted fake-server regression tests and a self-cleaning local wrapper
that records machine resources and reconciles PostgreSQL and consumer effects.

The first run passed 200 transfers plus 50 planned retries at eight workers.
Review identified possible hidden HTTP/1 transport replay for rewindable POST
bodies, so body rewind was disabled. An edit command first failed on quoting
without modifying source; applying it from a file fixed that. The repeat also
passed: 250 requests, 200 new transfers, 50 replays, zero failed requests,
peak eight in flight. The original result is retained with clarified retry
metadata. The repeat measured 1.237 seconds and
202.03 completed client calls/s; p50/p95/p99
were 18.218/109.279/616.181 ms.
The differing tails between short runs are recorded, with no capacity or
performance-change claim.

Both databases ended with 200 transfers/receipts/events/projections, no pending
events, unchanged user balances and no journal, posting or payload mismatches.
All 16 acceptance checks passed and temporary projects were removed. Unit
tests, vet, PowerShell parse, Compose and diff checks passed. No ledger/auth
application-code change was required. Evidence is in load-testing.md and
results/load-test*.json. Next is deployment validation.
