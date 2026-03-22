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
- form-based general dispatch that launches a real local `codex exec` command
- Python packaging via `pyproject.toml`
- Dockerfile for containerizing the app
- Helm chart aligned with the user's other local cluster app repos
- basic pytest coverage for the initial surface

## Local run

```bash
pip install -e .[dev]
./scripts/dev-server.sh
```

Then open `http://127.0.0.1:8080`.

For hot-reload during UI/backend editing:

```bash
OVERLORD_RELOAD=1 ./scripts/dev-server.sh
```

Useful endpoints:

- `GET /`
- `POST /report`
- `GET /healthz`
- `GET /api/meta`
- `POST /dispatch`
- `POST /api/workers/events`
- `POST /api/workers/{worker_id}/notes`
- `POST /api/commands`
- `GET /api/commands`
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

Registration and parent accountability reports should use the same helper instead of hand-written curl payloads:

```bash
overlord-worker-status register-member \
  --member-id captain-20260315-example \
  --member-token captain-secret-token \
  --role captain \
  --parent-member-id general-20260315-example \
  --repo-path /Users/matthewschwartz/projects/overlord \
  --status-line "captain registered with overlord" \
  --process-id 12345 \
  --process-started-at 2026-03-15T22:00:00Z
```

```bash
overlord-worker-status parent-report \
  --member-id worker-20260315-example \
  --reporter-member-id captain-20260315-example \
  --reporter-token captain-secret-token \
  --observed-status-line "replacement worker spawned after drift" \
  --event-type replaced_underling \
  --related-member-id worker-20260315-example-retry \
  --observed-state active
```

Required live messages use the same helper too:

```bash
overlord-worker-status post-message \
  --member-id worker-20260315-example \
  --sender-member-id captain-20260315-example \
  --sender-token captain-secret-token \
  --message-type check \
  --body "checked worker and it is still progressing"
```

Phase note:

- `planned`, `implementing`, and `validating` require `--next-step`
- if that field is not ready yet, report `assigned` or `scouting` first
- launch/start/progress/blocker/complete messages are required under the Overlord communication contract

For quick demos, the dashboard also exposes a `Self Report Intake` form that writes through the same state store and validation rules.

## Operator dispatch

The dashboard now includes an Overlord mission planner that can launch either a `general` or a `captain`. It writes a prompt file under `data/dispatches/`, launches:

```bash
codex exec -m gpt-5.4 --dangerously-bypass-approvals-and-sandbox --skip-git-repo-check -
```

and stores the pid plus log path in the local SQLite database so the operator can see which general order was launched most recently.

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
- browser-side operator mutations beyond the server-rendered forms
- optional cluster deployment wiring through `local-k8s-apps`
