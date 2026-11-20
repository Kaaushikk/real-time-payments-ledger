# Week 3 acceptance

Verified locally on November 20, 2026.

The acceptance stack runs separate auth, ledger and consumer processes with
PostgreSQL, Kafka, OpenTelemetry Collector, Jaeger, Loki, Prometheus and Grafana.
Its published ports bind to loopback. Each run uses an isolated Compose project.

## Reproduce

Requires Docker Desktop with the Linux engine and PowerShell 7. Run from the
repository root.

```powershell
pwsh -File scripts/verify-week3.ps1
```

The script builds the service images, submits simulated HTTP requests, starts
Kafka after observing a pending event, and verifies redelivery and telemetry.
By default it removes its containers, network and volumes when it finishes.
It refuses to reuse an existing acceptance project.

To retain the services for inspection, add `-KeepStack`. Clean that project
up afterward with:

```powershell
docker compose -p ledger-week3-check -f deployments/docker-compose.acceptance.yml down --volumes
```

## Verified outcomes

| Check | Observed result |
|---|---|
| HTTP retry | Original and replay responses identified the same transaction |
| Broker recovery | One pending event was published after Kafka became available |
| Forced redelivery | One ledger transfer and one consumer projection remained |
| Reconciliation | Zero balance/journal mismatches and no negative user wallets |
| Cross-service trace | Ledger and consumer spans shared the request trace ID |
| Parent relationships | Publisher spans referenced the transfer attempt; consumer spans referenced publication |
| Correlated logs | Loki returned ledger and consumer records by trace ID |
| Log privacy sample | Access token, refresh token and private description were absent from queried records |
| Grafana | Six Prometheus panel queries returned numeric data; its Loki query returned logs |

The successful run used trace `1abed21a6e2a4facb70d0cb82a7ec874` and
transaction `a0009123-da87-43f1-8119-4b5ea1b5c004`. The trace contained eight
publication-attempt spans and two consumer spans. These IDs describe the
temporary local run; a fresh run produces new IDs.

## Searching the logs

Application event logs include explicitly selected transaction/event IDs,
outcomes and trace correlation fields. Network export uses a bounded queue.
The collector sends logs to Loki's native OTLP endpoint. Loki stores trace and
event IDs as structured metadata.

In Grafana Explore, select Loki and replace the example trace ID.

```logql
{service_name=~"ledger|consumer"} | trace_id="1abed21a6e2a4facb70d0cb82a7ec874"
```

Use the same ID in Jaeger at `http://localhost:16687`. Acceptance-stack
Grafana is at `http://localhost:13000`; its local credentials are configured in
the acceptance Compose file.

## Boundaries and debugging

These are small functional checks. Logs use best-effort telemetry delivery and
the exporter can drop records if its bounded queue fills. Financial state and
audit records persist in PostgreSQL. Deployment, load and Toxiproxy experiments
remain Week 4 work.

One local regression run stalled while closing a PostgreSQL connection.
The affected test and the complete PostgreSQL suite passed on retry.
An earlier acceptance run lost its current p99 sample after a long gap;
the script now queries the scenario's recorded time range.
