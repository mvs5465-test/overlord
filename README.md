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

## Configuration

Environment variables:

- `HOST` default `127.0.0.1`
- `PORT` default `8080`
- `OVERLORD_APP_NAME` default `Overlord`
- `OVERLORD_DATA_DIR` default `data`
- `OVERLORD_DEFAULT_ENVIRONMENT` default `local`
- `OVERLORD_DEFAULT_WORKSPACE` default `default`

## Next implementation seams

- worker registry and liveness model
- durable local task and event storage
- agent dispatch and coordination workflows
- optional cluster deployment wiring through `local-k8s-apps`
