# Overlord

## Scope
- Localhost-first agent coordination app scaffold.
- Keep this repo focused on a lightweight local control plane with a small web UI and clear extension seams.

## Local Development
- Install dependencies with `pip install -e .[dev]`
- Run locally with `python app.py`
- The default local UI is served at `http://127.0.0.1:8080`

## App Rules
- Preserve the current lightweight FastAPI plus server-rendered HTML approach unless a larger architecture change is explicitly requested.
- Keep dependencies minimal and avoid introducing frontend build tooling unless there is a concrete need.
- Prefer keeping localhost-first defaults safe and explicit: bind to loopback by default and store local state under `./data`.
- Treat worker orchestration, persistence, and remote integrations as future implementation work; keep placeholders honest rather than speculative.

## Helm And Releases
- If a PR changes anything under `chart/`, bump `chart/Chart.yaml` `version` in the same PR.
- Bump `appVersion` when the deployed application behavior materially changes.
- Treat chart and app versions as release metadata, not deployment selectors.

## Verification
- Run `pytest` for app changes.
- Run `helm template overlord ./chart` for chart changes.

