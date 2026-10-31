# Four-week plan

Start October 30, 2026. Friday checkpoints: November 6, 13, 20, 27.

| Week | Focus | Completion evidence |
|---|---|---|
| 1 | Money rules, locking, durable idempotency, audit/journal, migration safety | Real PostgreSQL correctness tests, ownership checks and CI |
| 2 | Actual gRPC/REST gateway and identity hardening | Registered business RPCs; API parity; token concurrency and access tests |
| 3 | Kafka/outbox/consumer and telemetry | Recovery/replay tests; connected traces; metrics/dashboard/log examples |
| 4 | Chaos/load, deployment validation and demo | Reconciliation per scenario; scoped results; validated manifests; runbook |

Every checkpoint: explain purpose, build, test, fix failures, record actual results,
review for secrets, commit and push. Weekly reflections follow work actually done.
Scope: USD only, simulated funding, one wallet/user; no real payment rails or FX.
Live billable cloud deployment is a separate environment decision.

Full design and acceptance details: [project handoff](project-handoff.md).

## Daily cadence

Continue one small meaningful task per day through November 27. Keep work in the
current week's milestone, run relevant checks, update the log, then push verified
changes. Do not generate empty commits, backdate work, or invent results. Record
blockers when a task cannot be verified. The first foundation checkpoint was
developed on October 30; follow-up hardening and evidence remain week 1 work.
