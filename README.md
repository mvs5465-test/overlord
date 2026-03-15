# overlord

Localhost-first agent coordination app scaffold.

This repo is intentionally just the starting shell. It sets up a small app surface, local-first configuration, packaging, and Kubernetes deployment scaffolding without pretending the coordination product exists yet.

## What is here

- FastAPI app with a server-rendered placeholder homepage
- health and metadata endpoints for quick local checks
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
- `GET /healthz`
- `GET /api/meta`
- `POST /api/worker-status`

## Configuration

Environment variables:

- `HOST` default `127.0.0.1`
- `PORT` default `8080`
- `OVERLORD_APP_NAME` default `Overlord`
- `OVERLORD_DATA_DIR` default `data`
- `OVERLORD_DEFAULT_ENVIRONMENT` default `local`
- `OVERLORD_DEFAULT_WORKSPACE` default `default`
- `OVERLORD_WORKER_WRITE_TOKEN` optional bearer token for worker write requests

## Worker status updates

Workers can post phase transitions and short notes to the local control plane with the bundled helper:

```bash
overlord-worker-status \
  --worker-id worker-20260315-overlord-agent-client \
  --status implementing \
  --note "adding cli write path" \
  --repo-path /Users/matthewschwartz/projects/overlord \
  --artifact overlord/worker_status.py \
  --next-step "run pytest for worker status api"
```

The helper posts to `http://127.0.0.1:8080/api/worker-status` by default. Set `OVERLORD_CONTROL_PLANE_URL` to target a different local instance. If `OVERLORD_WORKER_WRITE_TOKEN` is configured on the server, pass the same value with `OVERLORD_WORKER_TOKEN` or `--token`.

Accepted phases in the MVP:

- `assigned`
- `scouting`
- `planned`
- `implementing`
- `validating`
- `blocked`
- `handoff-ready`
- `terminal`

## Next implementation seams

- worker registry and liveness model
- durable local task and richer event storage
- agent dispatch and coordination workflows
- optional cluster deployment wiring through `local-k8s-apps`
