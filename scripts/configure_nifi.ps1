docker compose build nifi-init
if ($LASTEXITCODE -ne 0) {
    throw "NiFi init image build failed with exit code $LASTEXITCODE"
}

docker compose run --rm nifi-init
if ($LASTEXITCODE -ne 0) {
    throw "NiFi configuration failed with exit code $LASTEXITCODE"
}
