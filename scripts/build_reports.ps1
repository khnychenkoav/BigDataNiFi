docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U lab -d nifi_lab -f /docker-entrypoint-initdb.d/01_build_reports.sql
if ($LASTEXITCODE -ne 0) {
    throw "Report build failed with exit code $LASTEXITCODE"
}
