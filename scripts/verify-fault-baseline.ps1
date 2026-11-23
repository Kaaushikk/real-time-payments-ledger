param(
    [switch]$KeepStack,
    [string]$OutputPath = 'docs/results/fault-baseline.json'
)
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$root = Split-Path $PSScriptRoot -Parent
$base = Join-Path $root 'deployments/docker-compose.acceptance.yml'
$faults = Join-Path $root 'deployments/docker-compose.faults.yml'
$project = 'ledger-fault-check'
$control = 'http://127.0.0.1:18474'
$userAgent = 'payments-ledger-fault-harness/1.0'
$started = [DateTimeOffset]::UtcNow
function Invoke-Stack {
    & docker compose -p $project -f $base -f $faults @args
    if ($LASTEXITCODE -ne 0) { throw 'Fault baseline stack command failed' }
}
function Wait-Ready([string]$Url) {
    for ($i=0; $i -lt 60; $i++) {
        try {
            if ((Invoke-WebRequest -Uri $Url -UserAgent $userAgent -TimeoutSec 2 -SkipHttpErrorCheck).StatusCode -eq 200) { return }
        } catch {}
        Start-Sleep -Seconds 1
    }
    throw "Endpoint did not become ready: $Url"
}
function Sql([string]$Query) {
    $value = Invoke-Stack exec -T postgres psql -U ledger -d ledger -At -v ON_ERROR_STOP=1 -c $Query
    return ($value -join [Environment]::NewLine).Trim()
}
function Wait-Check([scriptblock]$Check, [string]$Failure) {
    for ($i=0; $i -lt 60; $i++) {
        if (& $Check) { return }
        Start-Sleep -Seconds 1
    }
    throw $Failure
}
function Assert-ClearProxy {
    $proxy = Invoke-RestMethod -UserAgent $userAgent -Uri "$control/proxies/postgres" -TimeoutSec 5
    $toxics = Invoke-RestMethod -UserAgent $userAgent -Uri "$control/proxies/postgres/toxics" -TimeoutSec 5
    if (!$proxy.enabled -or $proxy.upstream -ne 'postgres:5432' -or @($toxics).Count -ne 0) {
        throw 'The baseline requires an enabled PostgreSQL proxy with zero toxics'
    }
}
function Snapshot {
    $query = @"
SELECT jsonb_build_object(
 'alice_balance', (SELECT balance_minor FROM accounts WHERE id='$alice'::uuid),
 'bob_balance', (SELECT balance_minor FROM accounts WHERE id='$bob'::uuid),
 'transfers', (SELECT count(*) FROM ledger_transactions WHERE type='transfer'),
 'receipts', (SELECT count(*) FROM idempotency_keys),
 'journal_entries', (SELECT count(*) FROM journal_entries),
 'audit_records', (SELECT count(*) FROM audit_log),
 'outbox_events', (SELECT count(*) FROM outbox),
 'projections', (SELECT count(*) FROM consumed_transfers),
 'pending_events', (SELECT count(*) FROM outbox WHERE published_at IS NULL),
 'total_balance', (SELECT sum(balance_minor::numeric) FROM accounts),
 'negative_users', (SELECT count(*) FROM accounts WHERE account_type='user' AND balance_minor<0),
 'reconciliation_mismatches', (SELECT count(*) FROM accounts a WHERE balance_minor <>
   (SELECT coalesce(sum(CASE WHEN direction='credit' THEN amount_minor::numeric ELSE -amount_minor::numeric END),0)
    FROM journal_entries e WHERE e.account_id=a.id))
)::text
"@
    return (Sql $query | ConvertFrom-Json)
}
function Transfer([string]$Name, [string]$Token, [string]$From, [string]$To, [string]$Key, [long]$Amount) {
    $body = @{from_account_id=$From;to_account_id=$To;amount_minor=$Amount;currency='USD';description='fault baseline'} | ConvertTo-Json -Compress
    $watch = [Diagnostics.Stopwatch]::StartNew()
    try {
        $response = Invoke-WebRequest -Method Post -Uri 'http://127.0.0.1:18080/gateway/v1/transfers' -TimeoutSec 15 -ContentType 'application/json' -Body $body -Headers @{
            Authorization='Bearer '+$Token;'Idempotency-Key'=$Key
        }
    } finally { $watch.Stop() }
    $receipt = $response.Content | ConvertFrom-Json
    if ($response.StatusCode -ne 200) { throw 'Unexpected gateway status' }
    return [ordered]@{name=$Name;http_status=$response.StatusCode;duration_ms=[Math]::Round($watch.Elapsed.TotalMilliseconds,3)
        transaction_id=$receipt.transaction.transaction_id;replayed=[bool]$receipt.replayed}
}
$existing = docker ps -aq --filter "label=com.docker.compose.project=$project"
if ($LASTEXITCODE -ne 0) { throw 'Docker is unavailable' }
if ($existing) { throw 'Inspect or remove the existing ledger-fault-check project before another run' }
$result = $null
try {
    Invoke-Stack build auth ledger consumer
    Invoke-Stack up -d postgres toxiproxy jaeger loki otel-collector auth kafka
    Wait-Ready "$control/version"
    $null = Invoke-RestMethod -UserAgent $userAgent -Method Post -Uri "$control/proxies" -TimeoutSec 5 -ContentType 'application/json' -Body (
        @{name='postgres';listen='0.0.0.0:15432';upstream='postgres:5432';enabled=$true} | ConvertTo-Json -Compress)
    Assert-ClearProxy
    Wait-Check { [bool]((Invoke-Stack logs --no-color --tail 14 kafka) -match 'Kafka Server started') } 'Kafka did not become ready'
    Invoke-Stack exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic ledger.transactions --partitions 1 --replication-factor 1
    Invoke-Stack up -d ledger consumer
    Wait-Ready 'http://127.0.0.1:18080/readyz'
    Wait-Ready 'http://127.0.0.1:18081/healthz'
    $alice = [guid]::Parse((Sql "SELECT a.id FROM accounts a JOIN users u ON a.owner_user_id=u.id WHERE u.email='alice@ledger.dev'")).ToString()
    $bob = [guid]::Parse((Sql "SELECT a.id FROM accounts a JOIN users u ON a.owner_user_id=u.id WHERE u.email='bob@ledger.dev'")).ToString()
    $aliceTokens = Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:18081/oauth/dev-token' -TimeoutSec 5 -Body @{
        email='alice@ledger.dev';password='password';client_id='ledger-cli'
    }
    $bobTokens = Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:18081/oauth/dev-token' -TimeoutSec 5 -Body @{
        email='bob@ledger.dev';password='password';client_id='ledger-cli'
    }
    $before = Snapshot
    $key = [guid]::NewGuid().ToString()
    $requests = @(
        (Transfer 'alice-to-bob' $aliceTokens.access_token $alice $bob $key 13),
        (Transfer 'same-request-retry' $aliceTokens.access_token $alice $bob $key 13),
        (Transfer 'bob-to-alice' $bobTokens.access_token $bob $alice ([guid]::NewGuid().ToString()) 7)
    )
    if ($requests[0].replayed -or !$requests[1].replayed -or $requests[2].replayed -or
        $requests[0].transaction_id -ne $requests[1].transaction_id -or
        $requests[0].transaction_id -eq $requests[2].transaction_id) { throw 'Transfer or retry receipt mismatch' }
    $expectedProjections = [long]$before.projections + 2
    Wait-Check {
        (Sql 'SELECT count(*) FROM consumed_transfers') -eq [string]$expectedProjections -and
        (Sql 'SELECT count(*) FROM outbox WHERE published_at IS NULL') -eq '0'
    } 'Baseline events were not fully published and consumed'
    $after = Snapshot
    $expected = @{transfers=2;receipts=2;journal_entries=4;audit_records=4;outbox_events=2;projections=2}
    foreach ($field in $expected.Keys) {
        if ([long]$after.$field - [long]$before.$field -ne $expected[$field]) { throw "Unexpected $field delta" }
    }
    if ($after.alice_balance -ne ([long]$before.alice_balance - 6) -or $after.bob_balance -ne ([long]$before.bob_balance + 6) -or
        $after.total_balance -ne 0 -or $after.negative_users -ne 0 -or $after.reconciliation_mismatches -ne 0) {
        throw 'Baseline financial reconciliation failed'
    }
    $proxyIp = docker inspect --format '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$project-toxiproxy-1"
    if ($LASTEXITCODE -ne 0) { throw 'Could not inspect proxy route' }
    $proxyIp = [Net.IPAddress]::Parse($proxyIp.Trim()).ToString()
    $connections = [int](Sql "SELECT count(*) FROM pg_stat_activity WHERE datname='ledger' AND client_addr='$proxyIp'::inet")
    if ($connections -lt 1) { throw 'No PostgreSQL connection traversed Toxiproxy' }
    Assert-ClearProxy
    $commit = git -C $root rev-parse HEAD
    if ($LASTEXITCODE -ne 0) { throw 'Could not record the application revision' }
    $result = [ordered]@{
        scenario='no-fault-baseline';started_at_utc=$started.ToString('o');application_commit=$commit.Trim()
        workload=@{concurrency=1;requests=3;new_transfers=2;retry_requests=1;currency='USD';amount_unit='cents'}
        proxy=@{enabled=$true;toxics=0;upstream='postgres:5432';observed_database_connections=$connections}
        requests=$requests;before=$before;after=$after
    }
} finally {
    if (!$KeepStack) {
        & docker compose -p $project -f $base -f $faults down --volumes
        if ($LASTEXITCODE -ne 0) { throw 'Baseline stack cleanup failed' }
    }
}
$result['finished_at_utc'] = [DateTimeOffset]::UtcNow.ToString('o')
$result['cleanup'] = if ($KeepStack) { 'retained for inspection' } else { 'temporary project removed' }
$target = [IO.Path]::GetFullPath((Join-Path $root $OutputPath))
$null = New-Item -ItemType Directory -Force -Path (Split-Path $target -Parent)
$result | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $target -Encoding utf8
$result | ConvertTo-Json -Depth 8
