function Invoke-Checked {
    param([scriptblock]$Command)
    & $Command
    if ($LASTEXITCODE -ne 0) {
        throw "Command failed with exit code $LASTEXITCODE"
    }
}

Invoke-Checked { docker compose build nifi nifi-init producer }
docker compose up -d postgres kafka nifi metabase

$containers = @(
    "bd_nifi_postgres",
    "bd_nifi_kafka",
    "bd_nifi",
    "bd_nifi_metabase"
)

$deadline = (Get-Date).AddMinutes(12)
do {
    $notReady = @()
    foreach ($container in $containers) {
        $health = docker inspect -f "{{.State.Health.Status}}" $container 2>$null
        if ($health -ne "healthy") {
            $notReady += "$container=$health"
        }
    }
    if ($notReady.Count -eq 0) {
        break
    }
    Write-Host ("Waiting for services: " + ($notReady -join ", "))
    Start-Sleep -Seconds 10
} while ((Get-Date) -lt $deadline)

if ($notReady.Count -ne 0) {
    docker compose ps
    throw "Some services did not become healthy in time"
}

Invoke-Checked { docker compose run --rm nifi-init }
Invoke-Checked { docker compose run --rm producer }

$deadline = (Get-Date).AddMinutes(8)
do {
    $rawRows = docker compose exec -T postgres psql -U lab -d nifi_lab -tAc "select count(*) from stage.sales_raw" 2>$null
    $rawRows = ($rawRows | Select-Object -Last 1).Trim()
    if ($rawRows -eq "10000") {
        break
    }
    Write-Host "Waiting for NiFi ingestion: $rawRows rows"
    Start-Sleep -Seconds 10
} while ((Get-Date) -lt $deadline)

if ($rawRows -ne "10000") {
    docker compose logs --tail 100 nifi
    throw "NiFi did not load all rows in time"
}

Invoke-Checked { docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U lab -d nifi_lab -f /docker-entrypoint-initdb.d/01_build_reports.sql }
