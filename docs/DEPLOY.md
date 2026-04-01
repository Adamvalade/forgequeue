# Deploying ForgeQueue (public demo)

Recruiters can try the app at **`https://<your-api-host>/demo/`** once the **API**, **worker**, and **Redis** are all running. The demo UI uses relative URLs, so it works on any host without extra configuration.

## Option A — Render (Blueprint in this repo)

1. Push this repository to GitHub (if it is not already).
2. In [Render](https://dashboard.render.com/), choose **New → Blueprint**, select the repo, and apply `render.yaml`.
3. When the **web** service is live, open **`https://<forgequeue-api>.onrender.com/demo/`** (use the hostname shown in the Render dashboard).

**Cost / cold starts:** The blueprint uses **Starter** for both the **web** (API) and **worker** services so Render does **not** put the API to sleep when idle—recruiters get a normal first paint without a 30–60s wake-up. You pay roughly **two Starter instances** plus **Key Value** (the blueprint keeps Redis on the **free** KV tier to limit spend; bump KV in the dashboard if you outgrow it). Check [Render pricing](https://render.com/pricing) for current numbers in your region.

**Health checks:** Render should use **`/health`** (Redis reachable). Do not point load balancers at **`/ready`** unless you always run a worker: `/ready` requires at least one worker heartbeat.

## Option B — Railway, Fly.io, or a VPS

For **no cold starts**, use a **paid / always-on** tier (or **minimum instances ≥ 1** where the platform allows it)—same idea as Render Starter.

Components:

| Piece    | Role |
|----------|------|
| Redis    | Queue + job state (`REDIS_URL`) |
| API      | Docker image from `api/Dockerfile`; must listen on **`PORT`** (already supported) |
| Worker   | Docker image from `worker/Dockerfile`; same `REDIS_URL` as the API |

Set `VISIBILITY_TIMEOUT_SECONDS` on the worker in production (e.g. `300`); keep lower values only for local crash-recovery tests.

### TLS Redis (`rediss://`)

For providers like **Upstash**, use the TLS URL they give you. If the client fails certificate verification, set:

`REDIS_SSL_CERT_REQS_NONE=1`

(Use only when required by the provider; it disables certificate verification.)

## After deploy

- Put the public URL in your resume or portfolio: **`https://…/demo/`**
- Optional: add **`https://…/docs`** for API explorers

## Optional hardening

A public queue can be abused (spam jobs). For a long-lived demo you may want to add API key checks on `POST /jobs` and DLQ retry, or front the app with a known recruiter-only path. That is not implemented in the stock project.
