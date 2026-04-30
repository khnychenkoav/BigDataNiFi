docker compose build producer
if ($LASTEXITCODE -ne 0) {
    throw "Producer image build failed with exit code $LASTEXITCODE"
}

docker compose run --rm producer
if ($LASTEXITCODE -ne 0) {
    throw "Producer failed with exit code $LASTEXITCODE"
}
