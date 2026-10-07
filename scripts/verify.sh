#!/bin/sh
# One-shot acceptance run: build, start app + 2 workers, run verify, restart the
# app process, then verify confirmation durability against the restarted app.
set -u
cd "$(dirname "$0")/.."

docker compose up -d --build --wait app worker

docker compose run --rm -e VERIFY_PHASE=main verify
code=$?

if [ $code -eq 0 ]; then
  echo "--- restarting app service for confirmation persistence check ---"
  docker compose restart app
  docker compose up --wait app
  docker compose run --rm -e VERIFY_PHASE=restart verify
  code=$?
fi

docker compose down -v
exit $code
