# Scoped local load experiment

Verified November 25, 2026. This is a short correctness/measurement experiment
against two heavily shared wallets, with an eight-worker limit. It is not
a saturation test or evidence of 1,000 simultaneous transfers.

## Reproduce

Requires PowerShell 7, Go 1.26 and Docker Desktop's Linux engine on Windows.
From the repository root:

```powershell
pwsh -File scripts/verify-load.ps1 -Requests 200 -Concurrency 8 -ReplayEvery 4
```

The script builds the Go runner and an isolated ledger-load-check Compose
stack. It uses the loopback-only acceptance configuration, refuses to reuse
existing harness containers, and removes its temporary project by default.
It records host/Docker resource information, checks PostgreSQL before and
after, waits for event consumption, and writes
[load-test.json](results/load-test.json). Repeating the command replaces the
latest result. -KeepStack retains the services for inspection; clean up with:

```powershell
docker compose -p ledger-load-check -f deployments/docker-compose.acceptance.yml down --volumes
```

For an already-running local demo, the runner also supports:

```powershell
go run ./cmd/loadtest -from ALICE_ACCOUNT_UUID -to BOB_ACCOUNT_UUID -requests 200 -concurrency 8 -replay-every 4 -dev-auth
```

Replace the account placeholders with the corresponding seeded UUIDs. Defaults
are ledger port 8080 and auth port 8081; -ledger-url and -auth-url override them.
Demo authentication is explicit and requires both origins to be loopback.
Alternatively, supply ALICE_ACCESS_TOKEN and BOB_ACCESS_TOKEN through the
environment and omit -dev-auth. The Makefile accepts these flags via LOAD_ARGS.
The standalone runner does not perform the wrapper's database reconciliation.

## Workload and corrected behavior

- 200 unique one-cent USD transfers alternate between Alice and Bob: 100 in
  each direction. Each source wallet uses its owner's token, confirmed by an
  authenticated account lookup before measurement.
- Every fourth transfer is immediately retried with the same key and body,
  giving 50 planned retries and 250 measured client calls in total. Each worker
  finishes a request before sending its next one; at most eight are in flight.
- This is the handwritten REST route /v1/transfers. Fresh receipts must return
  201 and replayed receipts 200. The tool validates receipt JSON, transaction
  UUID, amount, currency and replay flag. A retry must return the original
  transaction ID, and independent keys must not share transaction IDs.
- No extra application retry loop is used. Transfer bodies cannot be rewound,
  preventing Go's HTTP/1 transport from silently replaying a failed POST in
  this local run. Server-side transaction retry logic remains enabled.
- Ten-second per-call timeout and a two-minute overall client limit. There
  are no warmup transfers, injected faults or external payment connections.
  Authentication and account preflight are outside the measured interval.

## Measured observations

| Run | Timed interval (s) | Completed client calls/s | p50 ms | p95 ms | p99 ms | Max ms | Failed calls |
|---|---|---|---|---|---|---|---|
| Initial (transport replay not disabled) | 1.523 | 164.12 | 40.072 | 111.477 | 194.811 | 275.111 | 0 |
| Repeat (HTTP/1 transport replay disabled) | 1.237 | 202.03 | 18.218 | 109.279 | 616.181 | 1224.144 | 0 |

Both runs completed 200 new transfers and 50 receipt replays; both reached
eight requests in flight. Each recorded 200 HTTP 201 responses and 50 HTTP
200 responses. Results are separate observations, not a controlled performance
comparison. The repeat's higher request rate and worse tail latency do not
establish a performance improvement or regression from the retry-policy change.

Latency covers each complete client call including response reading and
validation, excluding worker queue wait. Percentiles use nearest rank over all
250 calls, mixing new transfers with cheaper replays. Sorted individual samples
are retained in JSON. Request rate is client-call count divided by the timed
interval; it excludes setup and waiting for asynchronous event completion.
It is not a unique-transfers-per-second capacity figure or a wire-level packet
count. With only 250 samples, p99 is particularly sensitive to a few slow calls.

## Database and event checks

Both runs started from seeded wallets and finished with:

- 200 transfers, 200 complete receipts, 200 outbox events and 200 projections.
- 404 journal entries and 404 audit rows, including four seeded rows each.
- Alice and Bob each at 1000000 cents, unchanged by the balanced workload.
- Zero pending events, negative user balances, incomplete receipts,
  journal/balance mismatches, unbalanced transfer postings or projection-payload
  mismatches. Each transfer had exactly two entries with a zero signed sum.
- Total balances, including the issuance account, remained zero. Planned
  duplicate requests caused no extra postings. All 16 wrapper checks passed.

## Environment and provenance

| Item | Recorded value |
|---|---|
| Host OS description | Microsoft Windows 10.0.26300 |
| CPU | Intel(R) Core(TM) Ultra 9 185H; 22 logical CPUs |
| Host reported RAM | 33736040448 bytes (31.42 GiB) |
| Docker engine resources | 22 CPUs; 16459141120 bytes (15.33 GiB) |
| Docker engine | 29.7.2, linux/x86_64 |
| Load client | go version go1.26.7 windows/amd64 |

The API services, PostgreSQL, Kafka and telemetry ran together on the same
Docker engine; the Windows client and other local processes shared the host.
The acceptance stack has no per-service CPU/memory quotas. These numbers
describe this environment, not dedicated resources available to each service.

Repeat run setup/cleanup interval: 2026-11-25T13:15:09.512066-08:00 through
2026-11-25T13:16:16.055181-08:00. The measured client interval is recorded separately.
The application revision was d4734541b0883449aa1e29e22e2cf6c520e41863.
The runner source hash distinguishes the two versions; source and results
accompany this checkpoint's commit. No tokens, passwords, request bodies or
machine identifiers are stored in the result.

## Debugging, tests and limits

The old runner authenticated only Alice even while spending from Bob's wallet.
It also counted any successful HTTP status without validating receipt content
and printed no useful latency distribution. The replacement uses separate
owner credentials, bounded requests, structured failures and verified receipts.

The first live run passed. Review of Go 1.26.7's HTTP transport showed that a
rewindable POST with an Idempotency-Key could be retried on some connection
errors. The repeat disables transfer-body rewind to make the intended policy
explicit. The [initial result](results/load-test-initial.json) is preserved;
its retry metadata now distinguishes an absent application retry loop from
transport behavior. Observed counts and timings were not changed. A local
edit command initially failed due to quoting before changing any source;
applying the edit from a file resolved it.

Targeted regression tests cover both owner tokens, ownership preflight,
malformed success bodies, wrong replay IDs, duplicate IDs for distinct keys,
auth rejection without secret disclosure, loopback-only demo auth, and
nearest-rank calculations. Go tests, vet, PowerShell parsing, Compose and
diff checks passed locally. Full PostgreSQL/race checks run in GitHub CI.

The two runs are too short to establish stable throughput, sustained resource
behavior or a service-level latency guarantee. They do not cover thousands of
wallets, open-loop arrival rates, long-duration saturation, or failures under
load. Broader performance work is a stretch goal. Deployment validation and
the clean-checkout demonstration remain the next checkpoints.
