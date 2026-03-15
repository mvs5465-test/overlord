# overlord

Localhost-first agent coordination control plane.

This repo now ships the first real MVP slice: a small FastAPI control plane with durable local worker state, validated phase transitions, short phase notes, and a server-rendered operator board.

## What is here

- FastAPI app with a server-rendered dashboard
- health and metadata endpoints for quick local checks
- durable SQLite state under `./data/overlord.db`
- validated worker phase transitions and short phase notes
- worker roster and worker detail APIs
- form-based self-report intake on the dashboard for manual delegated-worker updates
- Python packaging via `pyproject.toml`
- Dockerfile for containerizing the app
- Helm chart aligned with the user's other local cluster app repos
- basic pytest coverage for the initial surface

## Local run

```bash
pip install -e .[dev]
python app.py
```

Then open `http://127.0.0.1:8080`.

Useful endpoints:

- `GET /`
- `POST /report`
- `GET /healthz`
- `GET /api/meta`
- `POST /api/workers/events`
- `POST /api/workers/{worker_id}/notes`
- `GET /api/workers`
- `GET /api/workers/{worker_id}`

## Configuration

Environment variables:

- `HOST` default `127.0.0.1`
- `PORT` default `8080`
- `OVERLORD_APP_NAME` default `Overlord`
- `OVERLORD_DATA_DIR` default `data`
- `OVERLORD_DEFAULT_ENVIRONMENT` default `local`
- `OVERLORD_DEFAULT_WORKSPACE` default `default`
- `OVERLORD_ALLOWED_REPO_ROOTS` default `~/projects`

## Worker status updates

Workers can post phase transitions and short notes to the local control plane with the bundled helper:

```bash
overlord-worker-status \
  --worker-id worker-20260315-overlord-agent-client \
  --worker-token worker-secret-token \
  --current-phase implementing \
  --previous-phase planned \
  --status-line "writing the CLI helper" \
  --note "adding cli write path" \
  --repo-path /Users/matthewschwartz/projects/overlord \
  --owned-artifact overlord/worker_status.py \
  --next-step "run pytest for worker status api"
```

The helper posts to `http://127.0.0.1:8080/api/workers/events` by default. Set `OVERLORD_CONTROL_PLANE_URL` to target a different local instance. Each worker is expected to keep its own `worker_token` and reuse it on later writes.

For quick demos, the dashboard also exposes a `Self Report Intake` form that writes through the same state store and validation rules.

Accepted phases in the MVP:

- `assigned`
- `scouting`
- `planned`
- `implementing`
- `validating`
- `blocked`
- `handoff-ready`
- `terminal`

## Current MVP seams

- stale-heartbeat tracking and richer liveness rules
- browser-side operator mutations
- agent dispatch and coordination workflows
- optional cluster deployment wiring through `local-k8s-apps`
