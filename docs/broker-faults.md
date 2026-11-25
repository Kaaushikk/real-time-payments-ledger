# Broker outage and durable replay

Verified November 24, 2026, for the Week 4 broker/cache checkpoint. The
experiment stops the local Kafka process, keeps transferring through the
ledger, restarts the ledger, restores Kafka and forces one duplicate delivery.
Redis caching is not implemented; PostgreSQL receipts are authoritative.

## Reproduce

Requires PowerShell 7 and Docker Desktop's Linux engine. From the repository root:

```powershell
pwsh -File scripts/verify-fault-baseline.ps1 -Scenario broker
```

The isolated ledger-fault-check stack first runs the three-request no-fault
baseline. The broker scenario then sends one 37-cent transfer and three
identical retries: during the outage, after a ledger restart while Kafka is
still stopped, and after broker recovery. All seven transfer requests in
the complete run returned HTTP 200. Four were receipt replays in total.

The script refuses to reuse existing harness containers and removes its
temporary project by default, including on failed assertions. It preserves
the standalone baseline and database-fault result files. The new result is
[broker-faults.json](results/broker-faults.json). Its top-level workload,
requests, before and after fields refer to the baseline; broker_case records
the outage/replay experiment, and final_after_cases covers the complete run.
For inspection, -KeepStack retains the stack; cleanup instructions are in
[the baseline guide](fault-baseline.md).

## Verified observations

| Checkpoint | Observed result |
|---|---|
| Kafka stopped | Container state confirmed exited |
| Transfer/retry while stopped | One transfer and receipt; one pending event; no new projection |
| Publisher | One publication error observed before restarting the ledger |
| Ledger restart with Kafka still stopped | Same receipt replayed; event remained pending with the same ID/version/payload hash |
| Broker recovery | Pending event published and stored once by the consumer |
| Consumer recovery action | Consumer was running; harness explicitly restarted it after Kafka became ready |
| Forced duplicate | Partition 0, offset 2 processed, offset 3 recognized as duplicate |
| Final ledger | Three transfers/receipts/events/projections; no pending event |
| Reconciliation | Zero journal mismatches and zero negative user balances |

The final journal and audit tables each held ten rows, including four seeded
rows. Alice's balance was 999957 cents and Bob's was 1000043 cents. Including
the funding account, total balances were zero. Every durable count and balance
was unchanged by the forced redelivery. The consumer's payload matched the
outbox payload in PostgreSQL.

Recorded local run: 2026-11-24T21:25:04.902384-08:00 through
2026-11-24T21:26:47.101751-08:00. The application revision was
86548ff87da2f3264e6c71aa0d251da2a924b236; the harness additions and raw results
accompany this checkpoint's commit. Temporary resources were removed.

## How the experiment works

The transfer and its event commit atomically in PostgreSQL, so broker
unavailability does not discard the pending event. Before restarting the
ledger, the script requires the publication error counter to increase and
checks that the event has not been acknowledged or consumed. Restarting the
process removes its in-memory state; replay must still recover the original
receipt from PostgreSQL. Event ID, schema version and a hash of the JSONB
payload are compared across restart and recovery. The hash checks consistency
of this fixture; it is not a security mechanism.

After Kafka becomes ready, the harness explicitly restarts the consumer and
waits for publication and projection completion. It then clears published_at
for this specific event to model a lost publication marker after delivery.
That causes a second Kafka publication with the same identity. The script
requires a consumer duplicate log at a strictly later offset in the same
partition and requires all financial counts/balances to remain unchanged.

## Redis scope, debugging and limitations

- The receipt store's constructor accepts a Redis client for compatibility
  with earlier wiring, but ignores it. No Redis commands are used by receipt
  processing, and this harness has no Redis service. Restart/replay verifies
  the existing PostgreSQL-only behavior. No cache failover is claimed.
- The consumer was explicitly restarted as a controlled recovery action.
  This test does not establish unattended consumer recovery or an orchestrator
  restart policy. Broker readiness checks use logs from the new startup so
  old readiness messages cannot satisfy them.
- Forced redelivery resets a test database publication marker; it is a model
  of that failure boundary, not an actual process crash at a precise instruction.
- This is one local broker-stop scenario with one forced duplicate. It is not
  a Kafka partition, multi-broker failover, throughput or availability benchmark.
- Elapsed request times and run timestamps are observations, not percentiles
  or a recovery-time service guarantee. The counter resets with the ledger;
  the recorded publication-error count is only the pre-restart observation.
- The first live run passed; no application-code correction was required.
  PowerShell parsing, merged Compose validation and diff checks passed.
- Scoped load measurements, deployment validation and a clean-checkout
  demonstration remain the next checkpoints.
