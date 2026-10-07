#!/bin/sh
# Local smoke without docker: server + 2 supervised workers + one-shot verify.
# Runs the main acceptance phase, restarts the app process, then re-runs the
# post-restart persistence phase against the same SQLite/artifact data.
set -u
cd "$(dirname "$0")/.."

DATA="${DATA_DIR:-/tmp/track-export-smoke}"
PORT="${PORT:-8080}"
rm -rf "$DATA" .verify-state
mkdir -p "$DATA"

DATA_DIR="$DATA" PORT="$PORT" TEST_HOOKS=1 python3 -m app.server &
APP_PID=$!

# restart loop mimics compose `restart: on-failure` for crash-injection scenarios
DATA_DIR="$DATA" TEST_HOOKS=1 LEASE_TTL_SECONDS=8 POLL_INTERVAL_SECONDS=0.3 \
  sh -c 'while true; do python3 -m app.worker; sleep 1; done' &
W1_PID=$!
DATA_DIR="$DATA" TEST_HOOKS=1 LEASE_TTL_SECONDS=8 POLL_INTERVAL_SECONDS=0.3 \
  sh -c 'while true; do python3 -m app.worker; sleep 1; done' &
W2_PID=$!

cleanup() {
  [ -n "${RESTARTED_APP_PID:-}" ] && kill "$RESTARTED_APP_PID" 2>/dev/null
  kill $APP_PID $W1_PID $W2_PID 2>/dev/null; pkill -f 'app.worker' 2>/dev/null; wait 2>/dev/null
}
trap cleanup EXIT

sleep 1
API_BASE="http://localhost:$PORT" DATA_DIR="$DATA" VERIFY_PHASE=main \
  python3 -m verify.verify
code=$?

if [ $code -eq 0 ]; then
  echo "--- restarting app server for confirmation persistence check ---"
  kill $APP_PID 2>/dev/null
  wait $APP_PID 2>/dev/null
  DATA_DIR="$DATA" PORT="$PORT" TEST_HOOKS=1 python3 -m app.server &
  RESTARTED_APP_PID=$!
  sleep 2
  API_BASE="http://localhost:$PORT" DATA_DIR="$DATA" VERIFY_PHASE=restart \
    python3 -m verify.verify
  code=$?
fi

exit $code
