# Deployment validation

The November 26 checkpoint validates application manifests and API readiness.
Helm rendering and Kubernetes schema checks passed. A live Kubernetes rollout
was not performed: kubectl has no configured context on this laptop, including
after Docker Desktop started. No cluster, cloud resources or public endpoints
were created.

## What the chart deploys

The chart renders three Deployments (ledger, auth and consumer) and two
ClusterIP Services (ledger and auth). Resource names and selectors include
the Helm release so separate releases do not select each other's pods.
The checked-in deployments/k8s/apps.yaml is a generated example, not a second
independently maintained deployment configuration.

PostgreSQL, Kafka, the Kafka topic and telemetry services remain external
prerequisites. The chart does not provision these, persistent storage, ingress,
TLS, backups, a registry or real payment integrations. Images must be built and
accessible to the cluster or preloaded into it. The default repository is a
local image namespace; it does not assert that published images exist.

## Required configuration

Default rendering intentionally fails until an image tag and existing Secret
name are supplied. The values schema rejects unknown settings, latest/empty
tags, invalid Secret names, invalid policies, negative resources and invalid
replica counts. A supplied Secret name does not prove that the Secret exists.

The Secret must be present in the release namespace with these keys:

| Key | Used by |
|---|---|
| database-url | Ledger, auth and consumer |
| jwt-signing-key | Ledger and auth |
| kafka-brokers | Ledger and consumer |

No credential values are created by or embedded in the chart. The consumer
does not receive the signing key. Redis configuration was removed because
the receipt store does not use it. JWT issuer/audience, public auth URL,
Kafka topic/group and collector endpoint are configurable through values.
The development-token endpoint is disabled. The application still seeds demo
accounts and uses a demo authorization server; this chart is not a production
identity or internet-exposure solution.

Every container has CPU/memory requests and limits. The defaults use one
replica per service. Pods run as UID/GID 65532, with a read-only root filesystem,
no privilege escalation, dropped capabilities, RuntimeDefault seccomp and no
mounted service-account token. These are validated manifest settings; their
behavior inside live Kubernetes pods remains unverified.

## Readiness and liveness

Auth now has GET /readyz backed by a database ping with a two-second deadline.
Ledger already had the same database-aware endpoint. Both APIs retain a
process-level /healthz. Startup/liveness probes use /healthz; readiness uses
/readyz with a three-second probe timeout. A failed database check can remove
an API pod from service routing without making its liveness probe fail.
Startup probes allow initialization before the other probes run. This follows
the [Kubernetes probe guidance](https://kubernetes.io/docs/concepts/workloads/pods/probes/).

The real-PostgreSQL regression test first observes 200 from both endpoints,
closes a separate test connection pool, then observes 503 from /readyz while
/healthz remains 200 for both APIs. It tests dependency-unavailable behavior
using a closed pool; it is not a live Kubernetes probe or database restart test.

The consumer has no HTTP server and gets no fabricated HTTP probes or Service.
Kubernetes can restart an exited worker, but a running pod does not prove that
Kafka processing is caught up. Check event progress, errors and logs separately.
Consumer recovery inside Kubernetes has not been demonstrated here.

## Repeat the offline checks

Requires Python 3.12, PyYAML 6.0.3, Helm 4.3.2 and kubeconform 0.8.0. Validators
need network access to obtain Kubernetes schemas. From the repository root:

```powershell
python -m pip install PyYAML==6.0.3
python scripts/verify-deployment.py --helm PATH_TO_HELM --kubeconform PATH_TO_KUBECONFORM
```

The verified local tool paths are bin/tools/windows-amd64/helm.exe and
bin/tools/kubeconform/kubeconform.exe. These portable tools are ignored by Git.
Windows archives came from the official
[Helm release](https://github.com/helm/helm/releases/tag/v4.3.2) and
[kubeconform release](https://github.com/yannh/kubeconform/releases/tag/v0.8.0),
verified against their published checksums before extraction:

- Helm Windows archive SHA256:
  77c5c782aaef1abbfafe75b88412d2b6df8e4a36d14522273397f0d02c6ecc29.
- kubeconform Windows archive SHA256:
  e3f56102bcf4f50b034a567e2482a1c5330799983ddd655952310211aef73d93.

CI installs the pinned Linux releases through
scripts/install-deployment-tools.sh, verifies published checksums, then runs
the same checks. It saves a separate report as an Actions artifact.

The validator performs strict Helm lint, renders two release/namespace/value
variants, checks Kubernetes 1.36.0 schemas and compares the generated snapshot.
It also checks selectors, Service targets, Secret references, probes, security
settings and resource request/limit ordering. Seven invalid value sets must
fail. A deliberately regressed auth readiness path must fail its policy guard.
The schema version is a declared validation target, not an observed cluster
version. Schema validity does not prove admission, scheduling or runtime success.

After intentionally editing the chart, review the new render and regenerate:

```powershell
python scripts/verify-deployment.py --helm PATH_TO_HELM --kubeconform PATH_TO_KUBECONFORM --refresh-snapshot
```

The [local result](results/deployment-validation.json) records exact timestamps,
versions, fingerprints, rejection cases and counts. Both variants had five
valid resources, zero invalid/errors/skipped. Fingerprints normalize chart
line endings for Windows/Linux consistency. See the
[Helm validation documentation](https://helm.sh/docs/helm/helm_lint/).

## Runtime work still required

For an existing disposable local cluster, supply the dependencies and Secret
above, build/load all three images, and set actual values in a private values
file. Select the context explicitly. The following is a runbook example and
was not executed in this checkpoint:

```powershell
helm upgrade --install payments deployments/helm/payments-ledger --kube-context LOCAL_CONTEXT --namespace ledger-demo --create-namespace -f PRIVATE_VALUES_FILE --wait --timeout 5m
kubectl --context LOCAL_CONTEXT -n ledger-demo rollout status deployment/payments-ledger
kubectl --context LOCAL_CONTEXT -n ledger-demo rollout status deployment/payments-auth
kubectl --context LOCAL_CONTEXT -n ledger-demo rollout status deployment/payments-consumer
```

Check pod events/logs, port-forward the internal API Services, complete the
normal demo OAuth flow, run an authenticated transfer/retry, verify one event
projection and reconcile the journal. Also check probe failure/recovery and
restart behavior in that cluster before claiming deployment success.
Private registry access, database connection budgets, production migrations,
storage, network policy, TLS and dependency upgrades need environment-specific
review. A clean-checkout demo and final portfolio review are the next checkpoint.
