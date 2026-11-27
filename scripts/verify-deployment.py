"""Render and validate application manifests without contacting a cluster."""
import argparse
import copy
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile

import yaml

ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / 'deployments/helm/payments-ledger'
VALUES = ROOT / 'deployments/helm/values.validation.yaml'
SNAPSHOT = ROOT / 'deployments/k8s/apps.yaml'


def command(args, success=True):
    result = subprocess.run([str(x) for x in args], cwd=ROOT, capture_output=True, text=True, timeout=120)
    if (result.returncode == 0) != success:
        raise RuntimeError(f'Unexpected command result: {args[0]} {args[1]}\n{result.stdout}{result.stderr}')
    return result.stdout


def policy(documents, release, namespace, secret='ledger-runtime', tag='local-check'):
    assert len(documents) == 5, 'expected three deployments and two services'
    deployments = {d['metadata']['labels']['app.kubernetes.io/component']: d for d in documents if d['kind'] == 'Deployment'}
    services = {d['metadata']['labels']['app.kubernetes.io/component']: d for d in documents if d['kind'] == 'Service'}
    assert set(deployments) == {'ledger', 'auth', 'consumer'}
    assert set(services) == {'ledger', 'auth'}
    for d in documents:
        component = d['metadata']['labels']['app.kubernetes.io/component']
        assert d['metadata']['name'] == f'{release}-{component}'
        assert d['metadata']['namespace'] == namespace
        assert d['metadata']['labels']['app.kubernetes.io/instance'] == release
    for name, d in deployments.items():
        spec = d['spec']['template']['spec']
        labels = d['spec']['template']['metadata']['labels']
        assert all(labels[k] == v for k, v in d['spec']['selector']['matchLabels'].items())
        assert spec['automountServiceAccountToken'] is False
        assert spec['securityContext']['runAsNonRoot'] is True
        assert spec['securityContext']['runAsUser'] == 65532
        assert spec['securityContext']['seccompProfile']['type'] == 'RuntimeDefault'
        assert spec['terminationGracePeriodSeconds'] >= 15
        containers = spec['containers']
        assert len(containers) == 1
        c = containers[0]
        assert c['image'] == f'payments-ledger/{name}:{tag}'
        assert c['securityContext']['allowPrivilegeEscalation'] is False
        assert c['securityContext']['readOnlyRootFilesystem'] is True
        assert c['securityContext']['capabilities']['drop'] == ['ALL']
        assert 'envFrom' not in c
        env = {e['name']: e for e in c['env']}
        assert len(env) == len(c['env']), 'duplicate environment name'
        expected = {'DATABASE_URL': 'database-url'}
        if name != 'consumer':
            expected['JWT_SIGNING_KEY'] = 'jwt-signing-key'
        if name != 'auth':
            expected['KAFKA_BROKERS'] = 'kafka-brokers'
        refs = {k: e['valueFrom']['secretKeyRef'] for k, e in env.items() if 'valueFrom' in e}
        assert set(refs) == set(expected)
        assert all(refs[k] == {'name': secret, 'key': v} for k, v in expected.items())
        assert 'REDIS_ADDR' not in env
        if name == 'auth':
            assert env['ALLOW_DEV_TOKEN']['value'] == 'false'
            assert 'AUTH_PUBLIC_URL' in env
        if name == 'consumer':
            assert 'JWT_SIGNING_KEY' not in env and 'KAFKA_CONSUMER_GROUP' in env
            assert not any(k in c for k in ['ports', 'readinessProbe', 'livenessProbe'])
        else:
            assert c['startupProbe']['httpGet'] == {'path': '/healthz', 'port': 'http'}
            assert c['livenessProbe']['httpGet'] == {'path': '/healthz', 'port': 'http'}
            assert c['readinessProbe']['httpGet'] == {'path': '/readyz', 'port': 'http'}
            assert c['readinessProbe']['timeoutSeconds'] > 2
            service = services[name]['spec']
            assert service['type'] == 'ClusterIP'
            assert all(labels[k] == v for k, v in service['selector'].items())
            ports = {p['name']: p['containerPort'] for p in c['ports']}
            assert all(p['targetPort'] in ports and p['port'] == ports[p['targetPort']] for p in service['ports'])
        for resource in ['cpu', 'memory']:
            quantities = []
            for bound in ['requests', 'limits']:
                value = c['resources'][bound][resource]
                if resource == 'cpu':
                    quantities.append(Decimal(value[:-1]) / 1000 if value.endswith('m') else Decimal(value))
                else:
                    quantities.append(Decimal(value[:-2]) * (2**20 if value.endswith('Mi') else 2**30))
            assert 0 < quantities[0] <= quantities[1], 'resource requests must be positive and <= limits'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--helm', default='helm')
    parser.add_argument('--kubeconform', default='kubeconform')
    parser.add_argument('--kube-version', default='1.36.0')
    parser.add_argument('--refresh-snapshot', action='store_true')
    parser.add_argument('--output', default='docs/results/deployment-validation.json')
    args = parser.parse_args()
    started = datetime.now(timezone.utc).isoformat()
    command([args.helm, 'lint', CHART, '--strict', '-f', VALUES])

    def render(release='payments', namespace='ledger-demo', overrides=(), with_values=True, success=True):
        cmd = [args.helm, 'template', release, CHART, '--namespace', namespace, '--kube-version', args.kube_version]
        if with_values:
            cmd += ['-f', VALUES]
        for override in overrides:
            cmd += ['--set', override]
        return command(cmd, success=success)

    base = render()
    docs = list(yaml.safe_load_all(base))
    policy(docs, 'payments', 'ledger-demo')
    snapshot = '# Generated by scripts/verify-deployment.py; edit the Helm chart.\n' + base.rstrip() + '\n'
    if args.refresh_snapshot:
        SNAPSHOT.write_text(snapshot, encoding='utf-8', newline='\n')
    assert SNAPSHOT.read_text(encoding='utf-8-sig') == snapshot, 'snapshot drift; review then use --refresh-snapshot'
    variant = render('second', 'other-demo', ['replicaCount.ledger=2', 'existingSecret=other-runtime', 'image.tag=review-check'])
    policy(list(yaml.safe_load_all(variant)), 'second', 'other-demo', 'other-runtime', 'review-check')
    assert {d['metadata']['name'] for d in docs}.isdisjoint({d['metadata']['name'] for d in yaml.safe_load_all(variant)})
    invalid = [('missing-required-values', None), ('latest-tag', 'image.tag=latest'),
               ('negative-replicas', 'replicaCount.ledger=-1'), ('invalid-secret', 'existingSecret=INVALID_name'),
               ('invalid-pull-policy', 'image.pullPolicy=Sometimes'), ('unknown-setting', 'resorces.foo=1'),
               ('negative-cpu', 'resources.consumer.requests.cpu=-1')]
    for _, override in invalid:
        render(overrides=[] if override is None else [override], with_values=override is not None, success=False)
    malformed = copy.deepcopy(docs)
    auth = next(d for d in malformed if d['kind'] == 'Deployment' and d['metadata']['name'] == 'payments-auth')
    auth['spec']['template']['spec']['containers'][0]['readinessProbe']['httpGet']['path'] = '/healthz'
    try:
        policy(malformed, 'payments', 'ledger-demo')
    except AssertionError:
        pass
    else:
        raise AssertionError('regression guard accepted a liveness-only readiness probe')
    summaries = []
    with tempfile.TemporaryDirectory(prefix='ledger-manifests-') as tmp:
        for name, content in [('base', base), ('variant', variant)]:
            file = Path(tmp) / f'{name}.yaml'
            file.write_text(content, encoding='utf-8')
            raw = command([args.kubeconform, '-strict', '-summary', '-output', 'json', '-kubernetes-version', args.kube_version, file])
            summary = json.loads(raw)['summary']
            assert summary == {'valid': 5, 'invalid': 0, 'errors': 0, 'skipped': 0}, summary
            summaries.append({'variant': name, **summary})
    chart_hash = hashlib.sha256()
    for path in sorted(CHART.rglob('*')):
        if path.is_file():
            chart_hash.update(path.relative_to(CHART).as_posix().encode())
            chart_hash.update(path.read_text(encoding='utf-8-sig').encode())
    report = {'started_at_utc': started, 'finished_at_utc': datetime.now(timezone.utc).isoformat(),
              'helm': command([args.helm, 'version', '--short']).strip(),
              'kubeconform': command([args.kubeconform, '-v']).strip(), 'kubernetes_schema_version': args.kube_version,
              'helm_lint': 'passed', 'valid_variants': 2, 'schema_validation': summaries,
              'rejected_values': [name for name, _ in invalid], 'probe_regression_guard': 'passed',
              'snapshot_matches': True, 'chart_sha256': chart_hash.hexdigest(),
              'snapshot_sha256': hashlib.sha256(SNAPSHOT.read_bytes()).hexdigest(),
              'rollout': 'not performed by this offline validator; a running cluster and dependencies are required'}
    target = Path(args.output)
    if not target.is_absolute():
        target = ROOT / target
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
