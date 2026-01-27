# ForgeQueue

A fault-tolerant job queue with retries, crash recovery, and observability.

## Components
- **api**: FastAPI service for creating jobs + checking status
- **worker**: background worker that executes jobs
- **redis**: queue + job state storage

## Local run
```bash
docker compose up --build
