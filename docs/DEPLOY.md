# Deployment

Public demo: **`https://<api-host>/demo/`** with API, worker, and Redis all running. The UI calls same-origin APIs only.

## Render (blueprint)

1. Push the repo to GitHub.
2. [Render](https://dashboard.render.com/) → **New** → **Blueprint** → select repo → apply `render.yaml`.
3. Open the web service URL with path **`/demo/`**.

**Plans:** Blueprint uses **Starter** for the web service and worker so instances are not spun down on idle (avoids long first request after quiet periods). Key Value may stay on the free tier; see [pricing](https://render.com/pricing).

**Health check:** use **`/health`**. Avoid **`/ready`** for the public load balancer unless a worker is always up—it requires a recent worker heartbeat.

**GitHub link in `/demo/`:** set **`FORGEQUEUE_GITHUB_URL`** on the API (e.g. your fork URL).

## Other hosts

Use an always-on or **min instances ≥ 1** tier if you need predictable latency.

| Part | Notes |
|------|--------|
| Redis | `REDIS_URL` |
| API | `api/Dockerfile`, **`PORT`** |
| Worker | `worker/Dockerfile`, same `REDIS_URL`, `VISIBILITY_TIMEOUT_SECONDS` e.g. `300` in production |

### `rediss://` (e.g. Upstash)

If TLS verification fails: **`REDIS_SSL_CERT_REQS_NONE=1`**

## Abuse

A public `POST /jobs` can be spammed. For a long-lived demo consider API keys or similar; not included by default.
