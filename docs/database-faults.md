# Database fault experiments

Verified November 23, 2026, as the Week 4 database-failure checkpoint.
These are local simulated-money experiments, with three configurations and
one transfer per configuration. They do not establish production availability
or a load-capacity claim.

## Reproduce

Requires PowerShell 7 and Docker Desktop's Linux engine. From the repository root:

```powershell
pwsh -File scripts/verify-fault-baseline.ps1 -Scenario database
```

The script first repeats the three-request no-fault baseline. It then injects
each fault separately, restores the proxy, retries the identical request/key,
and checks a further receipt replay. The default output is
[database-faults.json](results/database-faults.json); the earlier standalone
baseline result is preserved. In this JSON, `workload`, `requests`, `before`
and `after` describe the baseline; `cases` contains the fault experiments and
`final_after_cases` describes the complete run.

The isolated ledger-fault-check project refuses to reuse existing containers.
Default cleanup removes its containers, network and volumes, including on a
failed assertion. Use -KeepStack for investigation; cleanup instructions are
in [the baseline guide](fault-baseline.md). The API is local, and only the
ledger's database route is proxied. Auth and consumer database connections
remain direct so this experiment can isolate the ledger connection.

## Faults and observed results

| Case | Injection | Initial HTTP result | Committed before recovery | Recovery |
|---|---|---|---|---|
| Latency | 100 ms downstream latency, zero jitter | 200; observed 1255.410 ms | One | Replayed the original transaction |
| Timeout | Drop downstream data and close after 300 ms | 500; observed 304.364 ms | Zero | One new transfer, followed by receipt replay |
| In-flight disconnect | 500 ms downstream delay, then disable the proxy after observing a writing transaction | 500; observed 895.170 ms | Zero | One new transfer, followed by receipt replay |

Each recovery succeeded on its first attempt. The harness allows at most five
recovery attempts for server/transport failures; it records every attempt and
fails immediately on client errors. Initial requests have an eight-second
deadline. After recovery, every case had one transfer, one receipt, two journal
entries, two audit records, one outbox event and one consumer projection.
No event remained pending, no user balance was negative, and each account
balance equalled its signed journal total.

The complete run made 12 transfer requests: three baseline requests and three
per fault (initial attempt, recovery and replay). It finished with five new
transfers and five receipts, five published/consumed events, and 14 journal
entries and audit records including four seeded entries in each table.
Alice finished at 999923 cents and Bob at 1000077 cents. Including the funding
account, total balances remained zero.

Recorded UTC run: 2026-11-24T00:08:02.2944159+00:00 through
2026-11-24T00:09:15.9904221+00:00 (November 23, Pacific time).
The application revision was 15debcaaaaac6e16a7aa8ef6bd450a58f8dd75a6.
The harness additions and raw observations accompany this checkpoint's commit.
The temporary project was removed and no access tokens or request bodies are
stored in the result.

## How the checks establish correctness

For the disconnect case, an asynchronous HTTP request runs while the harness
queries PostgreSQL directly. It waits for a proxied session to be idle inside
a transaction with an assigned transaction ID, then disables the proxy to cut
the open connection. Requiring an assigned ID distinguishes a writing
transaction from an idle outbox poll. The result records that this gate was
observed; the script fails if it cannot observe it within five seconds.

Snapshots are taken after the initial request and again after recovery. Table
counts, balances and journal totals must all change together. A failure may
have committed before its response was lost, so the general recovery check
allows zero or one coherent commit and requires the receipt to agree with
that state. The deliberately early disconnect additionally requires zero.
The request body and idempotency key stay identical across all attempts.
After event processing drains, the script requires exactly one projection.

Proxy controls are restored in a finally block for each case. The timeout
configuration uses Toxiproxy's downstream data drop/close behavior; it does
not configure a PostgreSQL statement timeout. See the pinned
[timeout implementation](https://github.com/Shopify/toxiproxy/blob/v2.9.0/toxics/timeout.go)
and [latency implementation](https://github.com/Shopify/toxiproxy/blob/v2.9.0/toxics/latency.go).

## Debugging and limits

- A retry must keep the same description as well as the amount, accounts and
  key. Otherwise a request-fingerprint conflict is expected. The fault helper
  constructs the same body for initial, recovery and replay requests.
- Fault HTTP requests use .NET HttpClient so the harness can observe database
  activity while a request is running. Only transport/cancellation exceptions
  are recorded as status zero; unexpected harness errors fail the run.
- Elapsed times are observations for individual requests. The disconnect
  observation includes the database polling/control step; it is not a latency
  benchmark. No percentiles or throughput are inferred from these samples.
- Both interrupted requests returned HTTP 500. Callers must retry with the
  original idempotency key after recovery. This checkpoint added evidence and
  a harness; no application implementation change was needed.
- This run did not cut the connection specifically after COMMIT but before
  receipt delivery. It does not claim exhaustive crash-boundary coverage.
- Kafka/Redis outages, declared-workload load tests, Helm/Kubernetes validation
  and a clean-checkout demonstration remain separate checkpoints.
