docker compose exec -T postgres psql -U lab -d nifi_lab -f /sql/validation/postgres.sql
if ($LASTEXITCODE -ne 0) {
    throw "Validation failed with exit code $LASTEXITCODE"
}
