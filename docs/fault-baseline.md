# Toxiproxy baseline

The baseline was verified on November 23, 2026, during the session requested
on November 22. It is the Week 4 checkpoint carried forward from November 21.

The harness reuses the Week 3 acceptance stack with a Compose override that
routes the ledger's PostgreSQL connection through Toxiproxy. Authentication
and the event consumer use the database directly so that later fault cases
can isolate the ledger connection.

## Run the baseline

Requires PowerShell 7 and Docker Desktop with the Linux engine. From the
repository root:

```powershell
pwsh -File scripts/verify-fault-baseline.ps1
```

The script creates an isolated `ledger-fault-check` project, configures an
enabled PostgreSQL proxy with zero toxics, then performs three sequential
gateway requests:

1. Alice transfers 13 USD cents to Bob.
2. Alice retries the identical request and receives its original transaction.
3. Bob transfers 7 USD cents to Alice using Bob's authenticated token.

It checks the table deltas, account balances, journal reconciliation and
completion of background event processing. PostgreSQL's client connection
addresses confirm that the ledger traffic traverses the proxy.

The script saves [the latest measured result](results/fault-baseline.json)
after verification and cleanup. Repeating the command replaces that file
with the new run's data. Request durations are individual client observations
from this three-request scenario.

## Recorded result

| Check | Observed result |
|---|---|
| Requests | Three sequential requests, all HTTP 200 |
| Transfers and receipts | Two new transfers and two durable receipts |
| Retry | Original transaction ID returned with replayed=true |
| Balances | Alice decreased by 6 cents; Bob increased by 6 cents |
| Journal and audit | Four new journal entries and four audit records |
| Events | Two outbox events, two consumer projections, zero pending |
| Money rules | Zero reconciliation mismatches and no negative user balances |
| Proxy | Enabled, zero toxics, two database connections observed through it |
| Cleanup | Temporary project removed |

The JSON records the actual UTC start/end time, application revision,
request observations and before/after state. It contains no access tokens,
refresh tokens or authentication passwords.

## Inspect and clean up

Add `-KeepStack` to retain the services for inspection. The control API is
on loopback port 18474. The harness identifies itself as an API client because
Toxiproxy 2.9 rejects browser-style user agents.

Remove only the harness project afterward with:

```powershell
docker compose -p ledger-fault-check -f deployments/docker-compose.acceptance.yml -f deployments/docker-compose.faults.yml down --volumes
```

The script refuses to reuse an existing harness project. Its default cleanup
also runs if verification fails.

## Next checkpoint and debugging

Database latency and disconnection experiments follow this baseline. The
three requests provide a functional comparison point; throughput and load
measurements need the later declared-workload experiment.

The first run could not pass control-API readiness because PowerShell's
default user agent starts with Mozilla. Toxiproxy returned HTTP 403. Setting
the harness's own API client identifier fixed the request, and the complete
baseline repeat passed.

The user-agent requirement is implemented in the pinned
[Toxiproxy 2.9 API](https://github.com/Shopify/toxiproxy/blob/v2.9.0/api.go).
