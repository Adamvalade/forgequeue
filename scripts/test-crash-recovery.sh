#!/usr/bin/env bash
# Conclusive crash-recovery test:
# 1. Create a long-running job and wait until worker has claimed it (status=running).
# 2. Kill the worker so the job is never acked.
# 3. Wait past VISIBILITY_TIMEOUT (15s in docker-compose) so recovery can re-queue it.
# 4. Start the worker again and wait for the job to complete.
# 5. Assert job ended with status=succeeded and attempts=2 (claimed twice).

set -e
API="${API:-http://localhost:8000}"
VISIBILITY_TIMEOUT="${VISIBILITY_TIMEOUT:-15}"
# Wait a bit longer than visibility timeout so recovery pass has run
RECOVERY_WAIT=$((VISIBILITY_TIMEOUT + 5))
# Total time we're willing to wait for job to be running, then to complete
POLL_INTERVAL=0.5
MAX_WAIT_RUNNING=10
MAX_WAIT_DONE=30

echo "=== Crash recovery test ==="
echo "API=$API VISIBILITY_TIMEOUT=${VISIBILITY_TIMEOUT}s recovery_wait=${RECOVERY_WAIT}s"
echo

# Ensure worker image has visibility-timeout / in-flight recovery code (set SKIP_BUILD=1 to skip)
if [[ -z "${SKIP_BUILD}" ]]; then
  echo "Building worker image (use SKIP_BUILD=1 to skip)..."
  docker compose build worker --quiet
  echo "Starting services..."
  docker compose up -d
  sleep 2
  echo
fi

# Create job (sleep 3s so it doesn't finish before we kill the worker)
resp=$(curl -s -X POST "$API/jobs" -H "Content-Type: application/json" \
  -d '{"type":"sleep","payload":{"ms":3000},"max_attempts":2}')
job_id=$(echo "$resp" | jq -r '.job_id')
if [[ "$job_id" == "null" || -z "$job_id" ]]; then
  echo "Failed to create job: $resp"
  exit 1
fi
echo "Created job_id=$job_id"

# Wait until status is running (worker claimed it)
echo -n "Waiting for job to be running (max ${MAX_WAIT_RUNNING}s)... "
for ((i=0; i<MAX_WAIT_RUNNING*2; i++)); do
  status=$(curl -s "$API/jobs/$job_id" | jq -r '.status')
  if [[ "$status" == "running" ]]; then
    echo "ok (status=$status)"
    break
  fi
  if [[ "$status" == "succeeded" ]]; then
    echo "Job already succeeded before we could kill worker. Increase sleep ms and retry."
    exit 1
  fi
  sleep $POLL_INTERVAL
done
if [[ "$status" != "running" ]]; then
  echo "Timed out. Last status=$status"
  exit 1
fi

# Kill worker (job stays in IN_FLIGHT, never acked)
echo "Killing worker..."
docker compose kill worker 2>/dev/null || true
sleep 1

# Wait past visibility timeout so recovery can run
echo "Waiting ${RECOVERY_WAIT}s for visibility timeout and recovery..."
sleep $RECOVERY_WAIT

# Start worker again
echo "Starting worker..."
docker compose up -d worker
sleep 2

# Wait for job to complete (re-queued, re-claimed, then sleep finishes)
echo -n "Waiting for job to complete (max ${MAX_WAIT_DONE}s)... "
for ((i=0; i<MAX_WAIT_DONE*2; i++)); do
  job=$(curl -s "$API/jobs/$job_id")
  status=$(echo "$job" | jq -r '.status')
  attempts=$(echo "$job" | jq -r '.attempts')
  if [[ "$status" == "succeeded" ]]; then
    echo "ok (status=$status attempts=$attempts)"
    break
  fi
  if [[ "$status" == "failed" ]]; then
    echo "Job failed: $job"
    exit 1
  fi
  sleep $POLL_INTERVAL
done
if [[ "$status" != "succeeded" ]]; then
  echo "Timed out. Last status=$status attempts=$attempts"
  echo "Job: $job"
  exit 1
fi

# Conclusive check: must have been attempted twice (once before kill, once after recovery)
attempts=$(echo "$job" | jq -r '.attempts')
if [[ "$attempts" != "2" ]]; then
  echo "FAIL: expected attempts=2 (claimed, killed, re-queued, re-claimed). got attempts=$attempts"
  exit 1
fi

echo
echo "PASS: Crash recovery works. Job succeeded with attempts=2."
exit 0
