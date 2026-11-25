param(
    [ValidateSet('baseline','database','broker')][string]$Scenario = 'baseline',
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
if ($Scenario -eq 'database' -and !$PSBoundParameters.ContainsKey('OutputPath')) {
    $OutputPath = 'docs/results/database-faults.json'
}
if ($Scenario -eq 'broker' -and !$PSBoundParameters.ContainsKey('OutputPath')) {
    $OutputPath = 'docs/results/broker-faults.json'
}
$http = [Net.Http.HttpClient]::new()
$http.Timeout = [TimeSpan]::FromSeconds(15)
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
function Add-DatabaseFault([string]$Name, [string]$Type, [hashtable]$Attributes) {
    $null = Invoke-RestMethod -UserAgent $userAgent -Method Post -Uri "$control/proxies/postgres/toxics" -TimeoutSec 5 -ContentType 'application/json' -Body (
        @{name=$Name;type=$Type;stream='downstream';toxicity=1.0;attributes=$Attributes} | ConvertTo-Json -Depth 4 -Compress)
}
function Set-DatabaseProxy([bool]$Enabled) {
    $null = Invoke-RestMethod -UserAgent $userAgent -Method Patch -Uri "$control/proxies/postgres" -TimeoutSec 5 -ContentType 'application/json' -Body (
        @{enabled=$Enabled} | ConvertTo-Json -Compress)
}
function Clear-DatabaseFaults {
    $toxics = Invoke-RestMethod -UserAgent $userAgent -Uri "$control/proxies/postgres/toxics" -TimeoutSec 5
    foreach ($toxic in $toxics) {
        $name = [Uri]::EscapeDataString($toxic.name)
        $null = Invoke-RestMethod -UserAgent $userAgent -Method Delete -Uri "$control/proxies/postgres/toxics/$name" -TimeoutSec 5
    }
    Set-DatabaseProxy $true
    Assert-ClearProxy
}
function Begin-FaultTransfer([string]$Key, [long]$Amount, [int]$TimeoutSeconds) {
    $body = @{from_account_id=$alice;to_account_id=$bob;amount_minor=$Amount;currency='USD';description='database fault experiment'} | ConvertTo-Json -Compress
    $request = [Net.Http.HttpRequestMessage]::new([Net.Http.HttpMethod]::Post,'http://127.0.0.1:18080/gateway/v1/transfers')
    $request.Headers.Authorization = [Net.Http.Headers.AuthenticationHeaderValue]::new('Bearer',$aliceTokens.access_token)
    $null = $request.Headers.TryAddWithoutValidation('Idempotency-Key',$Key)
    $request.Content = [Net.Http.StringContent]::new($body,[Text.Encoding]::UTF8,'application/json')
    $deadline = [Threading.CancellationTokenSource]::new([TimeSpan]::FromSeconds($TimeoutSeconds))
    $watch = [Diagnostics.Stopwatch]::StartNew()
    $task = $http.SendAsync($request,$deadline.Token)
    return @{task=$task;request=$request;deadline=$deadline;watch=$watch}
}
function Finish-FaultTransfer([string]$Name, $Pending) {
    $response = $null
    $status = 0
    $transport = $null
    $receipt = $null
    try {
        try {
            $response = $Pending.task.GetAwaiter().GetResult()
            $status = [int]$response.StatusCode
        } catch {
            $cause = $_.Exception.GetBaseException()
            if ($cause -isnot [Net.Http.HttpRequestException] -and $cause -isnot [OperationCanceledException]) { throw }
            $transport = $cause.GetType().Name
        }
        $Pending.watch.Stop()
        if ($status -eq 200) {
            $receipt = $response.Content.ReadAsStringAsync().GetAwaiter().GetResult() | ConvertFrom-Json
        }
        return [ordered]@{name=$Name;http_status=$status;transport_error=$transport
            observed_elapsed_ms=[Math]::Round($Pending.watch.Elapsed.TotalMilliseconds,3)
            transaction_id=$receipt.transaction.transaction_id;replayed=$receipt.replayed}
    } finally {
        $Pending.watch.Stop()
        if ($response) { $response.Dispose() }
        $Pending.request.Dispose()
        $Pending.deadline.Dispose()
    }
}
function Assert-TransferState($Before, $After, [long]$Amount, [int]$Count, [switch]$Drained) {
    if ($Count -lt 0 -or $Count -gt 1) { throw 'One request produced an invalid transfer count' }
    foreach ($field in @{transfers=1;receipts=1;journal_entries=2;audit_records=2;outbox_events=1}.GetEnumerator()) {
        if ([long]$After.($field.Key) - [long]$Before.($field.Key) -ne $Count*$field.Value) {
            throw "Partial or duplicate state in $($field.Key)"
        }
    }
    $projections = [long]$After.projections - [long]$Before.projections
    if ($projections -lt 0 -or $projections -gt $Count -or $After.pending_events -gt $Count) {
        throw 'Unexpected event processing state'
    }
    if ($Drained -and ($projections -ne $Count -or $After.pending_events -ne 0)) { throw 'Events did not drain' }
    if ($After.alice_balance -ne ([long]$Before.alice_balance - $Amount*$Count) -or
        $After.bob_balance -ne ([long]$Before.bob_balance + $Amount*$Count) -or
        $After.total_balance -ne 0 -or $After.negative_users -ne 0 -or $After.reconciliation_mismatches -ne 0) {
        throw 'Financial state failed reconciliation'
    }
}
function Run-DatabaseCase([string]$Kind, [long]$Amount) {
    Assert-ClearProxy
    $beforeCase = Snapshot
    $key = [guid]::NewGuid().ToString()
    $pending = $null
    $inFlight = $false
    try {
        switch ($Kind) {
            'latency' { Add-DatabaseFault 'request-latency' 'latency' @{latency=100;jitter=0} }
            'timeout' { Add-DatabaseFault 'request-timeout' 'timeout' @{timeout=300} }
            'disconnect' { Add-DatabaseFault 'disconnect-gate' 'latency' @{latency=500;jitter=0} }
        }
        $pending = Begin-FaultTransfer $key $Amount 8
        if ($Kind -eq 'disconnect') {
            $until = [DateTimeOffset]::UtcNow.AddSeconds(5)
            do {
                $active = [int](Sql "SELECT count(*) FROM pg_stat_activity WHERE datname='ledger' AND client_addr='$proxyIp'::inet AND state='idle in transaction' AND backend_xid IS NOT NULL")
                if ($active -gt 0) { $inFlight=$true; break }
                Start-Sleep -Milliseconds 50
            } while ([DateTimeOffset]::UtcNow -lt $until)
            if (!$inFlight) { throw 'Did not observe an in-flight writing transaction before disconnect' }
            Set-DatabaseProxy $false
        }
        $attempt = Finish-FaultTransfer "$Kind-attempt" $pending
        $pending = $null
        if ($Kind -eq 'latency' -and $attempt.http_status -ne 200) { throw 'Latency case did not complete within its deadline' }
        if ($Kind -ne 'latency' -and $attempt.http_status -eq 200) { throw 'The injected failure did not interrupt the request' }
        $during = Snapshot
        $committed = [int]$during.transfers - [int]$beforeCase.transfers
        Assert-TransferState $beforeCase $during $Amount $committed
        if ($Kind -eq 'disconnect' -and $committed -ne 0) { throw 'In-flight disconnect committed unexpectedly' }
    } finally {
        if ($pending) {
            $pending.deadline.Cancel()
            $null = Finish-FaultTransfer 'cancelled-experiment' $pending
        }
        Clear-DatabaseFaults
    }
    Wait-Ready 'http://127.0.0.1:18080/readyz'
    $recoveryAttempts = @()
    for ($i=0; $i -lt 5; $i++) {
        $pendingRecovery = Begin-FaultTransfer $key $Amount 8
        $recovery = Finish-FaultTransfer "$Kind-recovery-$i" $pendingRecovery
        $recoveryAttempts += $recovery
        if ($recovery.http_status -eq 200) { break }
        if ($recovery.http_status -gt 0 -and $recovery.http_status -lt 500) { throw 'Recovery returned a client error' }
        Start-Sleep -Milliseconds 500
    }
    if ($recovery.http_status -ne 200) { throw 'Recovery exhausted its bounded retries' }
    $pendingRetry = Begin-FaultTransfer $key $Amount 8
    $retry = Finish-FaultTransfer "$Kind-replay" $pendingRetry
    if ($retry.http_status -ne 200 -or !$retry.replayed -or $retry.transaction_id -ne $recovery.transaction_id -or
        [bool]$recovery.replayed -ne ($committed -eq 1)) { throw 'Recovery did not preserve the durable receipt' }
    if ($attempt.http_status -eq 200 -and $attempt.transaction_id -ne $recovery.transaction_id) { throw 'Recovery changed the original transaction' }
    $targetCount = [long]$beforeCase.projections + 1
    Wait-Check {
        (Sql 'SELECT count(*) FROM consumed_transfers') -eq [string]$targetCount -and
        (Sql 'SELECT count(*) FROM outbox WHERE published_at IS NULL') -eq '0'
    } 'Recovered event did not fully drain'
    $afterCase = Snapshot
    Assert-TransferState $beforeCase $afterCase $Amount 1 -Drained
    Assert-ClearProxy
    Write-Host "Verified database case: $Kind"
    return [ordered]@{fault=$Kind;amount_minor=$Amount;in_flight_transaction_observed=$inFlight
        parameters=$(switch ($Kind) {
            'latency' { @{type='latency';downstream_ms=100;jitter_ms=0} }
            'timeout' { @{type='timeout';close_after_ms=300} }
            'disconnect' { @{type='proxy-disabled';gate_downstream_ms=500} }
        })
        initial_attempt=$attempt;committed_after_attempt=$committed;state_after_attempt=$during
        recovery_attempts=$recoveryAttempts;recovery=$recovery;replay=$retry;before=$beforeCase;after=$afterCase}
}

function PublicationErrors {
    $metrics = (Invoke-WebRequest -Uri 'http://127.0.0.1:18080/metrics' -TimeoutSec 5).Content
    $match = [regex]::Match($metrics, '(?m)^ledger_outbox_publish_batches_total\{result="error"\} ([0-9.eE+\-]+)\s*$')
    if (!$match.Success) { throw 'Publication error counter is missing' }
    return [double]::Parse($match.Groups[1].Value,[Globalization.CultureInfo]::InvariantCulture)
}
function BrokerEvent([string]$TransactionId) {
    $id = [guid]::Parse($TransactionId).ToString()
    return (Sql "SELECT jsonb_build_object('event_id',id,'schema_version',(payload->>'schema_version')::int,'payload_hash',md5(payload::text),'pending',published_at IS NULL,'published_at',published_at) FROM outbox WHERE payload->>'transaction_id'='$id'" | ConvertFrom-Json)
}
function Assert-SameEvent($Before, $After) {
    if (!$Before.event_id -or $Before.event_id -ne $After.event_id -or $Before.schema_version -ne 1 -or
        $Before.schema_version -ne $After.schema_version -or $Before.payload_hash -ne $After.payload_hash) {
        throw 'Event identity, version or payload changed during recovery'
    }
}
function Assert-SameReceipt($First, $Retry) {
    if (!$Retry.replayed -or $Retry.transaction_id -ne $First.transaction_id) { throw 'Expected the original durable receipt' }
}
function ConsumerDelivery([string]$EventId, [string]$Result) {
    $lines = Invoke-Stack logs --no-color --tail 200 consumer
    foreach ($line in $lines) {
        $start = $line.IndexOf('{')
        if ($start -lt 0) { continue }
        $entry = $line.Substring($start) | ConvertFrom-Json
        if ($entry.event_id -eq $EventId -and $entry.result -eq $Result) {
            return [ordered]@{event_id=$entry.event_id;transaction_id=$entry.transaction_id;result=$entry.result
                partition=$entry.partition;offset=$entry.offset}
        }
    }
    return $null
}
function Run-BrokerCase {
    Assert-ClearProxy
    $beforeCase = Snapshot
    $errorsBefore = PublicationErrors
    $key = [guid]::NewGuid().ToString()
    $stoppedAt = [DateTimeOffset]::UtcNow
    Invoke-Stack stop kafka | Out-Host
    $state = docker inspect --format '{{.State.Status}}' "$project-kafka-1"
    if ($LASTEXITCODE -ne 0 -or $state -ne 'exited') { throw 'Kafka did not stop' }
    try {
        $first = Transfer 'broker-down-transfer' $aliceTokens.access_token $alice $bob $key 37
        $retry = Transfer 'broker-down-replay' $aliceTokens.access_token $alice $bob $key 37
        if ($first.replayed) { throw 'Fresh outage request unexpectedly replayed' }
        Assert-SameReceipt $first $retry
        $eventBefore = BrokerEvent $first.transaction_id
        if (!$eventBefore.pending) { throw 'Event was acknowledged while the broker was stopped' }
        Wait-Check { (PublicationErrors) -gt $errorsBefore } 'Publisher did not observe the broker outage'
        $errorsDuringOutage = PublicationErrors
        $during = Snapshot
        Assert-TransferState $beforeCase $during 37 1
        if ($during.pending_events -ne 1 -or $during.projections -ne $beforeCase.projections) {
            throw 'Expected one pending event and no consumer effect during the outage'
        }

        # Prove recovery depends on PostgreSQL rather than the original process or Redis.
        Invoke-Stack restart ledger | Out-Host
        Wait-Ready 'http://127.0.0.1:18080/readyz'
        $restartRetry = Transfer 'broker-down-ledger-restart-replay' $aliceTokens.access_token $alice $bob $key 37
        Assert-SameReceipt $first $restartRetry
        $afterRestart = Snapshot
        Assert-TransferState $beforeCase $afterRestart 37 1
        $eventAfterRestart = BrokerEvent $first.transaction_id
        Assert-SameEvent $eventBefore $eventAfterRestart
        if (!$eventAfterRestart.pending -or $afterRestart.pending_events -ne 1 -or
            $afterRestart.projections -ne $beforeCase.projections) { throw 'Restart lost or prematurely delivered the event' }
    } finally {
        $brokerRestartAt = [DateTimeOffset]::UtcNow
        Invoke-Stack start kafka | Out-Host
    }
    Wait-Check {
        [bool]((Invoke-Stack logs --no-color --since $brokerRestartAt.ToString('o') kafka) -match 'Kafka Server started')
    } 'Kafka did not become ready after restart'
    $consumerState = docker inspect --format '{{.State.Status}}' "$project-consumer-1"
    if ($LASTEXITCODE -ne 0) { throw 'Could not inspect consumer state' }
    # Explicit recovery step: do not claim unattended consumer recovery from this experiment.
    Invoke-Stack restart consumer | Out-Host
    $targetCount = [long]$beforeCase.projections + 1
    Wait-Check {
        (Sql 'SELECT count(*) FROM consumed_transfers') -eq [string]$targetCount -and
        (Sql 'SELECT count(*) FROM outbox WHERE published_at IS NULL') -eq '0'
    } 'Broker recovery did not drain the event'
    $recoveredAt = [DateTimeOffset]::UtcNow
    $eventRecovered = BrokerEvent $first.transaction_id
    Assert-SameEvent $eventBefore $eventRecovered
    $recoveryRetry = Transfer 'broker-restored-replay' $aliceTokens.access_token $alice $bob $key 37
    Assert-SameReceipt $first $recoveryRetry
    $afterRecovery = Snapshot
    Assert-TransferState $beforeCase $afterRecovery 37 1 -Drained
    $script:processed = $null
    Wait-Check {
        $script:processed = ConsumerDelivery $eventBefore.event_id 'processed'
        $null -ne $script:processed
    } 'Original consumer delivery was not observed'

    # Model publication success followed by loss of its database acknowledgement marker.
    $eventId = [guid]::Parse($eventBefore.event_id).ToString()
    $null = Sql "UPDATE outbox SET published_at=NULL WHERE id='$eventId'::uuid"
    $script:duplicate = $null
    Wait-Check {
        $script:duplicate = ConsumerDelivery $eventId 'duplicate'
        $null -ne $script:duplicate -and
        (Sql 'SELECT count(*) FROM outbox WHERE published_at IS NULL') -eq '0'
    } 'Consumer did not observe the forced duplicate publication'
    $eventRedelivered = BrokerEvent $first.transaction_id
    Assert-SameEvent $eventBefore $eventRedelivered
    if ($eventRedelivered.published_at -eq $eventRecovered.published_at -or
        $script:duplicate.partition -ne $script:processed.partition -or
        $script:duplicate.offset -le $script:processed.offset -or
        $script:duplicate.transaction_id -ne $first.transaction_id) { throw 'Distinct duplicate delivery was not proven' }
    $afterDuplicate = Snapshot
    foreach ($field in $afterRecovery.PSObject.Properties.Name) {
        if ($afterRecovery.$field -ne $afterDuplicate.$field) { throw "Redelivery changed $field" }
    }
    $samePayload = Sql "SELECT count(*) FROM consumed_transfers c JOIN outbox o ON c.event_id=o.id WHERE o.id='$eventId'::uuid AND c.payload=o.payload"
    if ($samePayload -ne '1') { throw 'Consumer stored an unexpected event payload' }
    Assert-ClearProxy
    Write-Host 'Verified broker outage, process restart replay and duplicate publication.'
    return [ordered]@{
        outage=@{stopped_at_utc=$stoppedAt.ToString('o');broker_restart_at_utc=$brokerRestartAt.ToString('o');drained_at_utc=$recoveredAt.ToString('o')
            publisher_errors_before_restart=$errorsDuringOutage-$errorsBefore;consumer_state_before_explicit_restart=$consumerState.Trim();consumer_restarted_by_harness=$true}
        redis=@{used_by_receipt_store=$false;service_in_harness=$false;note='PostgreSQL receipts remain authoritative; no Redis failover is implemented or claimed'}
        requests=@($first,$retry,$restartRetry,$recoveryRetry)
        event_before=$eventBefore;event_after_ledger_restart=$eventAfterRestart;event_recovered=$eventRecovered;event_redelivered=$eventRedelivered
        processed_delivery=$script:processed;duplicate_delivery=$script:duplicate
        before=$beforeCase;during_outage=$during;after_ledger_restart=$afterRestart;after_recovery=$afterRecovery;after_duplicate=$afterDuplicate
    }
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
    if ($Scenario -eq 'database') {
        $cases = @(
            (Run-DatabaseCase 'latency' 19),
            (Run-DatabaseCase 'timeout' 23),
            (Run-DatabaseCase 'disconnect' 29)
        )
        $result['scenario'] = 'database-faults'
        $result['cases'] = $cases
        $result['final_after_cases'] = Snapshot
    }
    if ($Scenario -eq 'broker') {
        $result['scenario'] = 'broker-faults'
        $result['broker_case'] = Run-BrokerCase
        $result['final_after_cases'] = Snapshot
    }
} finally {
    $http.Dispose()
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
