#!/usr/bin/env bash
# Live demo for ForgeQueue: enqueue, retries, DLQ, stats (requires `docker compose up`).
set -euo pipefail

BASE="${API_URL:-http://127.0.0.1:8000}"

job_id_from_create() {
  if command -v jq >/dev/null 2>&1; then
    jq -r '.job_id'
  else
    python3 -c "import json,sys; print(json.load(sys.stdin)['job_id'])"
  fi
}

echo "== ForgeQueue terminal demo (API=${BASE}) =="
echo "For recruiters: open ${BASE}/demo/ in a browser after docker compose up — no script required."
echo

echo "→ GET /"
curl -sS "${BASE}/" | (command -v jq >/dev/null && jq . || cat)
echo

echo "→ GET /health"
curl -sS "${BASE}/health" | (command -v jq >/dev/null && jq . || cat)
echo

echo "→ POST /jobs (sleep 200ms)"
JOB_SLEEP=$(curl -sS -X POST "${BASE}/jobs" \
  -H "Content-Type: application/json" \
  -d '{"type":"sleep","payload":{"ms":200},"max_attempts":1}' | job_id_from_create)
echo "   job_id=${JOB_SLEEP}"
echo "   (polling until succeeded…)"
for _ in $(seq 1 40); do
  st=$(curl -sS "${BASE}/jobs/${JOB_SLEEP}" | jq -r '.status' 2>/dev/null || curl -sS "${BASE}/jobs/${JOB_SLEEP}" | python3 -c "import json,sys; print(json.load(sys.stdin).get('status',''))")
  if [[ "${st}" == "succeeded" ]]; then
    echo "   status=succeeded"
    break
  fi
  sleep 0.5
done
echo

echo "→ POST /jobs (fail_once, max_attempts=3 — expect retry then success)"
JOB_RETRY=$(curl -sS -X POST "${BASE}/jobs" \
  -H "Content-Type: application/json" \
  -d '{"type":"fail_once","payload":{},"max_attempts":3}' | job_id_from_create)
echo "   job_id=${JOB_RETRY}"
for _ in $(seq 1 60); do
  st=$(curl -sS "${BASE}/jobs/${JOB_RETRY}" | jq -r '.status' 2>/dev/null || curl -sS "${BASE}/jobs/${JOB_RETRY}" | python3 -c "import json,sys; print(json.load(sys.stdin).get('status',''))")
  if [[ "${st}" == "succeeded" ]]; then
    attempts=$(curl -sS "${BASE}/jobs/${JOB_RETRY}" | jq -r '.attempts' 2>/dev/null || curl -sS "${BASE}/jobs/${JOB_RETRY}" | python3 -c "import json,sys; print(json.load(sys.stdin).get('attempts',''))")
    echo "   status=succeeded attempts=${attempts}"
    break
  fi
  sleep 0.5
done
echo

echo "→ POST /jobs (always_fail, max_attempts=2 — expect DLQ)"
JOB_DLQ=$(curl -sS -X POST "${BASE}/jobs" \
  -H "Content-Type: application/json" \
  -d '{"type":"always_fail","payload":{},"max_attempts":2}' | job_id_from_create)
echo "   job_id=${JOB_DLQ}"
for _ in $(seq 1 80); do
  st=$(curl -sS "${BASE}/jobs/${JOB_DLQ}" | jq -r '.status' 2>/dev/null || curl -sS "${BASE}/jobs/${JOB_DLQ}" | python3 -c "import json,sys; print(json.load(sys.stdin).get('status',''))")
  if [[ "${st}" == "failed" ]]; then
    echo "   status=failed (exhausted retries → DLQ)"
    break
  fi
  sleep 0.5
done
echo

echo "→ GET /dlq (first page)"
curl -sS "${BASE}/dlq?limit=5" | (command -v jq >/dev/null && jq . || cat)
echo

echo "→ POST /dlq/${JOB_DLQ}/retry (force=true for repeatable demos)"
curl -sS -X POST "${BASE}/dlq/${JOB_DLQ}/retry" \
  -H "Content-Type: application/json" \
  -d '{"force":true,"max_attempts":1}' | (command -v jq >/dev/null && jq . || cat)
echo "(Job will fail again with max_attempts=1 — shows retry path from DLQ.)"
echo

echo "→ GET /stats"
curl -sS "${BASE}/stats" | (command -v jq >/dev/null && jq . || cat)
echo

echo "→ GET /metrics (first 25 lines)"
curl -sS "${BASE}/metrics" | head -n 25
echo
echo "== Done. Prefer the visual demo: ${BASE}/demo/ (or ${BASE}/docs for OpenAPI). =="
