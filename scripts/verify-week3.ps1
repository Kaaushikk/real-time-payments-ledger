param([switch]$KeepStack)
$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'
$composeFile = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '../deployments/docker-compose.acceptance.yml')).Path
$project = 'ledger-week3-check'
function Invoke-Stack {
    & docker compose -p $project -f $composeFile @args
    if ($LASTEXITCODE -ne 0) { throw 'Acceptance stack command failed' }
}
function Wait-Ready([string]$Url) {
    for ($i=0; $i -lt 60; $i++) {
        try {
            if ((Invoke-WebRequest -Uri $Url -TimeoutSec 2 -SkipHttpErrorCheck).StatusCode -eq 200) { return }
        } catch {}
        Start-Sleep -Seconds 1
    }
    throw "Endpoint did not become ready: $Url"
}
function Sql([string]$Query) {
    $value = Invoke-Stack exec -T postgres psql -U ledger -d ledger -At -v ON_ERROR_STOP=1 -c $Query
    return ($value -join [Environment]::NewLine).Trim()
}
function Prom([string]$Expression) {
    $r = Invoke-RestMethod -Uri ('http://127.0.0.1:19093/api/v1/query?query=' + [Uri]::EscapeDataString($Expression))
    if ($r.status -ne 'success') { throw 'Prometheus query failed' }
    if (@($r.data.result).Count -ne 1) { return $null }
    return [double]$r.data.result[0].value[1]
}
function Loki([string]$Query) {
    $start = [long]($requestTime.AddSeconds(-30).ToUnixTimeMilliseconds()) * 1000000
    $r = Invoke-RestMethod -Uri ('http://127.0.0.1:13100/loki/api/v1/query_range?start=' + $start + '&limit=200&query=' + [Uri]::EscapeDataString($Query))
    if ($r.status -ne 'success') { throw 'Loki query failed' }
    return $r.data.result
}
function Wait-Check([scriptblock]$Check, [string]$Failure) {
    for ($i=0; $i -lt 60; $i++) {
        if (& $Check) { return }
        Start-Sleep -Seconds 1
    }
    throw $Failure
}
$existing = docker ps -aq --filter "label=com.docker.compose.project=$project"
if ($LASTEXITCODE -ne 0) { throw 'Docker is unavailable' }
if ($existing) { throw 'The acceptance project already exists. Inspect or remove it before another run.' }
try {
    Invoke-Stack build auth ledger consumer
    Invoke-Stack up -d postgres jaeger loki otel-collector auth ledger prometheus grafana
    foreach ($url in @('http://127.0.0.1:18080/readyz','http://127.0.0.1:18081/healthz',
        'http://127.0.0.1:13100/ready','http://127.0.0.1:19093/-/ready','http://127.0.0.1:13000/api/health')) { Wait-Ready $url }
    Wait-Check { (Prom 'up{job="ledger"}') -eq 1 } 'Ledger scrape failed'
    Start-Sleep -Seconds 12
    $alice = [guid]::Parse((Sql "SELECT a.id FROM accounts a JOIN users u ON a.owner_user_id=u.id WHERE u.email='alice@ledger.dev'")).ToString()
    $bob = [guid]::Parse((Sql "SELECT a.id FROM accounts a JOIN users u ON a.owner_user_id=u.id WHERE u.email='bob@ledger.dev'")).ToString()
    $pair = Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:18081/oauth/dev-token' -Body @{
        email='alice@ledger.dev';password='password';client_id='ledger-cli'
    }
    $traceId = [guid]::NewGuid().ToString('N')
    $parentId = [guid]::NewGuid().ToString('N').Substring(0,16)
    $description = 'private-acceptance-description-do-not-log'
    $headers = @{
        Authorization='Bearer ' + $pair.access_token;'Idempotency-Key'=[guid]::NewGuid().ToString()
        traceparent="00-$traceId-$parentId-01"
    }
    $body = @{from_account_id=$alice;to_account_id=$bob;amount_minor=13;currency='USD';description=$description} | ConvertTo-Json -Compress
    $requestTime = [DateTimeOffset]::UtcNow
    $first = Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:18080/gateway/v1/transfers' -Headers $headers -ContentType 'application/json' -Body $body
    $replay = Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:18080/gateway/v1/transfers' -Headers $headers -ContentType 'application/json' -Body $body
    $transactionId = [guid]::Parse($first.transaction.transaction_id).ToString()
    if ($first.replayed -or !$replay.replayed -or $replay.transaction.transaction_id -ne $transactionId) { throw 'HTTP receipt replay failed' }
    foreach ($amount in @(0,1000000000000000L)) {
        $headers['Idempotency-Key'] = [guid]::NewGuid().ToString()
        $invalidBody = @{from_account_id=$alice;to_account_id=$bob;amount_minor=$amount;currency='USD'} | ConvertTo-Json -Compress
        $r = Invoke-WebRequest -Method Post -Uri 'http://127.0.0.1:18080/gateway/v1/transfers' -Headers $headers -ContentType 'application/json' -Body $invalidBody -SkipHttpErrorCheck
        if ($r.StatusCode -ne 400) { throw 'Expected rejected gateway transfer' }
    }
    Wait-Check {
        (Prom 'ledger_outbox_pending_events{job="ledger"}') -eq 1 -and
        (Prom 'ledger_outbox_publish_batches_total{job="ledger",result="error"}') -ge 1
    } 'Pending event or publication failure not visible'
    Write-Output 'Observed the pending event and publishing failure during broker unavailability.'
    Invoke-Stack up -d kafka
    Wait-Check {
        [bool]((Invoke-Stack logs --no-color --tail 14 kafka) -match 'Kafka Server started')
    } 'Kafka did not become ready'
    Invoke-Stack exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic ledger.transactions --partitions 1 --replication-factor 1
    Invoke-Stack up -d consumer
    Wait-Check { (Sql 'SELECT count(*) FROM consumed_transfers') -eq '1' } 'Consumer did not store the event'
    Wait-Check { (Prom 'ledger_outbox_pending_events{job="ledger"}') -eq 0 } 'Backlog did not drain'

    # Model redelivery after acknowledgement but before a saved publication marker.
    $null = Sql 'UPDATE outbox SET published_at=NULL'
    $duplicateQuery = '{service_name="consumer"} | trace_id="' + $traceId + '" | result="duplicate"'
    Wait-Check { @(Loki $duplicateQuery).Count -gt 0 } 'Consumer duplicate log not found'
    if ((Sql 'SELECT count(*) FROM consumed_transfers') -ne '1' -or
        (Sql "SELECT count(*) FROM ledger_transactions WHERE type='transfer'") -ne '1') { throw 'Redelivery changed durable row counts' }
    $mismatches = Sql "SELECT (SELECT count(*) FROM accounts a WHERE a.balance_minor <> (SELECT coalesce(sum(CASE WHEN e.direction='credit' THEN e.amount_minor::numeric ELSE -e.amount_minor::numeric END),0) FROM journal_entries e WHERE e.account_id=a.id)) + (SELECT count(*) FROM accounts WHERE account_type='user' AND balance_minor<0)"
    if ($mismatches -ne '0') { throw 'Ledger reconciliation failed' }
    $traceQuery = '{service_name=~"ledger|consumer"} | trace_id="' + $traceId + '"'
    $logs = @(Loki $traceQuery)
    $services = @($logs | ForEach-Object { $_.stream.service_name } | Sort-Object -Unique)
    if ($services -notcontains 'ledger' -or $services -notcontains 'consumer') { throw 'Correlated logs are missing a service' }
    $allLogs = @(Loki '{service_name=~"auth|ledger|consumer"}') | ConvertTo-Json -Depth 30 -Compress
    foreach ($secret in @($pair.access_token,$pair.refresh_token,$description)) {
        if ($allLogs.Contains($secret)) { throw 'Private request data appeared in Loki' }
    }
    $script:jaeger = $null
    Wait-Check {
        $script:jaeger = Invoke-RestMethod -Uri "http://127.0.0.1:16687/api/traces/$traceId"
        $operations = @($script:jaeger.data[0].spans.operationName)
        $operations -contains 'kafka.PublishTransfer' -and $operations -contains 'kafka.ProcessTransfer'
    } 'Connected Jaeger trace not found'
    $traceServices = @($script:jaeger.data[0].processes.PSObject.Properties | ForEach-Object { $_.Value.serviceName })
    if ($traceServices -notcontains 'ledger' -or $traceServices -notcontains 'consumer') { throw 'Jaeger did not contain separate services' }

    $auth = @{Authorization='Basic ' + [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes('admin:local-acceptance-check'))}
    $dashboard = Invoke-RestMethod -Uri 'http://127.0.0.1:13000/api/dashboards/uid/payments-ledger' -Headers $auth
    if ($dashboard.dashboard.panels.Count -ne 6) { throw 'Expected six dashboard panels' }
    foreach ($panel in $dashboard.dashboard.panels) {
        $query = @{
            from=$requestTime.AddSeconds(-30).ToUnixTimeMilliseconds().ToString();to='now'
            queries=@(@{refId='A';datasource=@{type='prometheus';uid='prometheus'}
                expr=$panel.targets[0].expr.Replace('$__rate_interval','5m')
                instant=$false;range=$true;intervalMs=10000;maxDataPoints=300})
        } | ConvertTo-Json -Depth 8 -Compress
        $r = Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:13000/api/ds/query' -Headers $auth -ContentType 'application/json' -Body $query
        if ($r.results.A.error -or !$r.results.A.frames.Count) { throw "Grafana panel query failed: $($panel.title)" }
        $finite = $false
        foreach ($frame in $r.results.A.frames) {
            foreach ($values in @($frame.data.values | Select-Object -Skip 1)) {
                foreach ($number in $values) {
                    if ($null -ne $number -and $number -is [ValueType] -and
                        ![double]::IsNaN([double]$number) -and ![double]::IsInfinity([double]$number)) { $finite=$true }
                }
            }
        }
        if (!$finite) { throw "No numeric data in panel: $($panel.title)" }
    }
    $query = @{
        from=$requestTime.AddSeconds(-30).ToUnixTimeMilliseconds().ToString();to='now'
        queries=@(@{refId='A';datasource=@{type='loki';uid='loki'};expr=$traceQuery;queryType='range';maxLines=100})
    } | ConvertTo-Json -Depth 8 -Compress
    $lokiResult = Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:13000/api/ds/query' -Headers $auth -ContentType 'application/json' -Body $query
    if ($lokiResult.results.A.error -or !$lokiResult.results.A.frames.Count) { throw 'Grafana Loki query failed' }
    [ordered]@{
        trace_id=$traceId;transaction_id=$transactionId;transfer_rows=1;projection_rows=1;reconciliation_mismatches=0
        correlated_services=$services;grafana_panels=6;duplicate_delivery='verified';private_request_data_in_logs=$false
    } | ConvertTo-Json -Depth 5
} finally {
    if (!$KeepStack) {
        & docker compose -p $project -f $composeFile down --volumes
        if ($LASTEXITCODE -ne 0) { throw 'Acceptance stack cleanup failed' }
    }
}
