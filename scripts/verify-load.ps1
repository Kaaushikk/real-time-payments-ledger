param(
    [ValidateRange(2,100000)][ValidateScript({$_ % 2 -eq 0})][int]$Requests = 200,
    [ValidateRange(1,256)][int]$Concurrency = 8,
    [ValidateRange(0,100000)][int]$ReplayEvery = 4,
    [switch]$KeepStack,
    [string]$OutputPath = 'docs/results/load-test.json'
)
$ErrorActionPreference='Stop'
$ProgressPreference='SilentlyContinue'
if ($Concurrency -gt $Requests) { throw 'Workers cannot exceed unique requests' }
$root=Split-Path $PSScriptRoot -Parent
$composeFile=Join-Path $root 'deployments/docker-compose.acceptance.yml'
$project='ledger-load-check'
$started=[DateTimeOffset]::UtcNow
function Invoke-Stack {
    & docker compose -p $project -f $composeFile @args
    if ($LASTEXITCODE -ne 0) { throw 'Load experiment stack command failed' }
}
function Wait-Check([scriptblock]$Check,[string]$Failure) {
    for ($i=0;$i -lt 60;$i++) { if (& $Check) { return }; Start-Sleep -Seconds 1 }
    throw $Failure
}
function Wait-Ready([string]$Url) {
    Wait-Check {
        try { (Invoke-WebRequest -Uri $Url -TimeoutSec 2 -SkipHttpErrorCheck).StatusCode -eq 200 } catch { $false }
    } "Endpoint did not become ready: $Url"
}
function Sql([string]$Query) {
    return ((Invoke-Stack exec -T postgres psql -U ledger -d ledger -At -v ON_ERROR_STOP=1 -c $Query) -join "`n").Trim()
}
function Snapshot {
    return (Sql @"
SELECT jsonb_build_object(
 'alice_balance',(SELECT balance_minor FROM accounts WHERE id='$alice'::uuid),
 'bob_balance',(SELECT balance_minor FROM accounts WHERE id='$bob'::uuid),
 'transfers',(SELECT count(*) FROM ledger_transactions WHERE type='transfer'),
 'receipts',(SELECT count(*) FROM idempotency_keys),
 'journal_entries',(SELECT count(*) FROM journal_entries),
 'audit_records',(SELECT count(*) FROM audit_log),
 'outbox_events',(SELECT count(*) FROM outbox),
 'projections',(SELECT count(*) FROM consumed_transfers),
 'pending_events',(SELECT count(*) FROM outbox WHERE published_at IS NULL),
 'incomplete_receipts',(SELECT count(*) FROM idempotency_keys WHERE status<>'complete'),
 'total_balance',(SELECT sum(balance_minor::numeric) FROM accounts),
 'negative_users',(SELECT count(*) FROM accounts WHERE account_type='user' AND balance_minor<0),
 'reconciliation_mismatches',(SELECT count(*) FROM accounts a WHERE balance_minor <>
   (SELECT coalesce(sum(CASE WHEN direction='credit' THEN amount_minor::numeric ELSE -amount_minor::numeric END),0) FROM journal_entries e WHERE e.account_id=a.id)),
 'unbalanced_transfers',(SELECT count(*) FROM (
   SELECT t.id FROM ledger_transactions t LEFT JOIN journal_entries e ON e.transaction_id=t.id WHERE t.type='transfer'
   GROUP BY t.id HAVING count(e.id)<>2 OR sum(CASE WHEN e.direction='credit' THEN e.amount_minor::numeric ELSE -e.amount_minor::numeric END)<>0) invalid),
 'projection_mismatches',(SELECT count(*) FROM consumed_transfers c LEFT JOIN outbox o ON c.event_id=o.id WHERE o.id IS NULL OR c.payload<>o.payload)
)::text
"@ | ConvertFrom-Json)
}
$existing=docker ps -aq --filter "label=com.docker.compose.project=$project"
if ($LASTEXITCODE -ne 0) { throw 'Docker is unavailable' }
if ($existing) { throw 'Inspect or remove the existing ledger-load-check project before another run' }
$result=$null
try {
    $engine=docker info --format '{{json .}}' | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0) { throw 'Could not inspect Docker resource allocation' }
    $cpu=Get-CimInstance Win32_Processor | Select-Object -First 1
    $machine=Get-CimInstance Win32_ComputerSystem
    $goVersion=go version
    if ($LASTEXITCODE -ne 0) { throw 'Go is unavailable' }
    $null=New-Item -ItemType Directory -Force -Path (Join-Path $root 'bin')
    $binary=Join-Path $root 'bin/ledger-loadtest.exe'
    & go -C $root build -o $binary ./cmd/loadtest
    if ($LASTEXITCODE -ne 0) { throw 'Load runner build failed' }
    Invoke-Stack build auth ledger consumer
    Invoke-Stack up -d postgres jaeger loki otel-collector auth kafka
    Wait-Check { [bool]((Invoke-Stack logs --no-color --tail 14 kafka) -match 'Kafka Server started') } 'Kafka did not become ready'
    Invoke-Stack exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic ledger.transactions --partitions 1 --replication-factor 1
    Invoke-Stack up -d ledger consumer
    Wait-Ready 'http://127.0.0.1:18080/readyz'
    Wait-Ready 'http://127.0.0.1:18081/healthz'
    $alice=[guid]::Parse((Sql "SELECT a.id FROM accounts a JOIN users u ON a.owner_user_id=u.id WHERE u.email='alice@ledger.dev'")).ToString()
    $bob=[guid]::Parse((Sql "SELECT a.id FROM accounts a JOIN users u ON a.owner_user_id=u.id WHERE u.email='bob@ledger.dev'")).ToString()
    $before=Snapshot
    $raw=& $binary -ledger-url http://127.0.0.1:18080 -auth-url http://127.0.0.1:18081 -from $alice -to $bob -requests $Requests -concurrency $Concurrency -replay-every $ReplayEvery -dev-auth
    $runnerExit=$LASTEXITCODE
    if ($runnerExit -gt 1 -or !$raw) { throw 'Load runner failed before producing a measurement' }
    $measurement=($raw -join "`n") | ConvertFrom-Json
    Wait-Check { (Sql 'SELECT count(*) FROM outbox WHERE published_at IS NULL') -eq '0' } 'Events did not publish after the load'
    Wait-Check {
        (Sql 'SELECT count(*) FROM consumed_transfers') -eq (Sql 'SELECT count(*) FROM outbox')
    } 'Consumer projections did not catch up'
    $after=Snapshot
    $expectedReplays=if ($ReplayEvery -eq 0) {0} else {[int][Math]::Floor($Requests/$ReplayEvery)}
    $checks=[ordered]@{
        runner_passed=($runnerExit -eq 0 -and $measurement.failed_requests -eq 0)
        requests_completed=($measurement.http_requests -eq $Requests+$expectedReplays)
        receipts_validated=($measurement.posted -eq $Requests -and $measurement.replayed -eq $expectedReplays)
        worker_limit=($measurement.peak_in_flight -gt 0 -and $measurement.peak_in_flight -le $Concurrency)
        balances_unchanged=($after.alice_balance -eq $before.alice_balance -and $after.bob_balance -eq $before.bob_balance)
        money_conserved=($after.total_balance -eq 0 -and $after.total_balance -eq $before.total_balance)
        no_negative_users=($after.negative_users -eq 0)
        journal_reconciles=($after.reconciliation_mismatches -eq 0 -and $after.unbalanced_transfers -eq 0)
        complete_receipts=($after.incomplete_receipts -eq 0)
        events_complete=($after.pending_events -eq 0 -and $after.projection_mismatches -eq 0)
    }
    foreach ($field in @{transfers=1;receipts=1;journal_entries=2;audit_records=2;outbox_events=1;projections=1}.GetEnumerator()) {
        $checks[$field.Key+'_delta']=([long]$after.($field.Key)-[long]$before.($field.Key) -eq $Requests*$field.Value)
    }
    $commit=git -C $root rev-parse HEAD
    if ($LASTEXITCODE -ne 0) { throw 'Could not record the application revision' }
    $result=[ordered]@{
        scenario='two-wallet-rest-contention';started_at_utc=$started.ToString('o');application_commit=$commit.Trim()
        load_runner_sha256=(Get-FileHash -LiteralPath (Join-Path $root 'cmd/loadtest/main.go') -Algorithm SHA256).Hash
        environment=@{host_os=[System.Runtime.InteropServices.RuntimeInformation]::OSDescription;host_cpu=$cpu.Name;host_logical_cpus=$cpu.NumberOfLogicalProcessors
            host_memory_bytes=$machine.TotalPhysicalMemory;client_go=$goVersion;docker_cpus=$engine.NCPU;docker_memory_bytes=$engine.MemTotal
            docker_server=$engine.ServerVersion;docker_os=$engine.OSType;docker_architecture=$engine.Architecture}
        workload=@{route='/v1/transfers';amount_minor=1;currency='USD';wallets=2;unique_transfers=$Requests;workers=$Concurrency;replay_every=$ReplayEvery
            expected_replays=$expectedReplays;client_timeout_seconds=10;overall_client_limit_seconds=120;application_retry_loops=0;http1_post_transport_retries_disabled=$true;fault_injection=$false;warmup_transfers=0}
        measurement=$measurement;before=$before;after=$after;checks=$checks
    }
} finally {
    if (!$KeepStack) {
        & docker compose -p $project -f $composeFile down --volumes
        if ($LASTEXITCODE -ne 0) { throw 'Load experiment cleanup failed' }
    }
}
$result['finished_at_utc']=[DateTimeOffset]::UtcNow.ToString('o')
$result['cleanup']=if ($KeepStack) {'retained for inspection'} else {'temporary project removed'}
$target=[IO.Path]::GetFullPath((Join-Path $root $OutputPath))
$null=New-Item -ItemType Directory -Force -Path (Split-Path $target -Parent)
$result | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $target -Encoding utf8
if (@($checks.Values | Where-Object {$_ -ne $true}).Count) { throw 'Load measurements saved, but acceptance checks failed; inspect the result' }
Write-Host 'Verified the bounded workload, receipt counts, event completion and financial reconciliation.'
$measurement | Select-Object http_requests,posted,replayed,failed_requests,peak_in_flight,elapsed_seconds,observed_requests_per_second,latency_ms | ConvertTo-Json -Depth 4
