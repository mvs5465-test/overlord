from pathlib import Path
from urllib.parse import parse_qs, urlencode

import uvicorn
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from overlord.config import Settings
from overlord.dashboard import (
    format_relative_time,
    format_timestamp,
    grouped_phase_notes,
    pick_focus_worker,
    worker_freshness,
)
from overlord.models import PHASE_ORDER, WorkerEventCreate, WorkerNoteCreate, WorkerPhase
from overlord.store import InvalidTransitionError, StateStore, WorkerAuthError


BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))
templates.env.filters["relative_time"] = format_relative_time


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()
    state_store = StateStore(settings.data_dir)

    app = FastAPI(title=settings.app_name)
    app.state.settings = settings
    app.state.store = state_store
    app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

    @app.get("/", response_class=HTMLResponse)
    async def index(request: Request) -> HTMLResponse:
        return _render_dashboard(request, state_store, settings)

    @app.post("/report")
    async def post_worker_report(request: Request) -> RedirectResponse:
        body = (await request.body()).decode("utf-8")
        form = {
            key: values[-1]
            for key, values in parse_qs(body, keep_blank_values=True).items()
        }
        worker_id = _clean_optional(form.get("worker_id"))
        if worker_id is None:
            return _redirect_with_status(error="worker_id is required")

        payload = {
            "worker_id": worker_id,
            "worker_token": _clean_optional(form.get("worker_token")),
            "current_phase": _clean_optional(form.get("current_phase")),
            "previous_phase": _clean_optional(form.get("previous_phase")),
            "repo_path": _clean_optional(form.get("repo_path")),
            "branch": _clean_optional(form.get("branch")),
            "worktree": _clean_optional(form.get("worktree")),
            "owned_artifact": _clean_optional(form.get("owned_artifact")),
            "status_line": _clean_optional(form.get("status_line")),
            "next_irreversible_step": _clean_optional(form.get("next_irreversible_step")),
            "blocker": _clean_optional(form.get("blocker")),
            "note": _clean_optional(form.get("note")),
            "pr_url": _clean_optional(form.get("pr_url")),
        }
        payload = {key: value for key, value in payload.items() if value is not None}

        try:
            event = WorkerEventCreate.model_validate(payload)
            _ensure_repo_path_allowed(settings, event.repo_path)
            state_store.record_event(event)
        except HTTPException as exc:
            return _redirect_with_status(worker_id=worker_id, error=str(exc.detail))
        except (InvalidTransitionError, WorkerAuthError, ValueError) as exc:
            return _redirect_with_status(worker_id=worker_id, error=str(exc))

        return _redirect_with_status(worker_id=worker_id, report="accepted")

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/meta")
    async def meta() -> dict[str, object]:
        return {
            "app": settings.app_name,
            "mode": "control-plane-mvp",
            "defaults": {
                "environment": settings.default_environment,
                "workspace": settings.default_workspace,
                "dataDir": str(settings.data_dir),
                "allowedRepoRoots": [str(path) for path in settings.allowed_repo_roots],
            },
            "phases": [phase.value for phase in WorkerPhase],
            "api": {
                "events": "/api/workers/events",
                "notes": "/api/workers/{worker_id}/notes",
                "workers": "/api/workers",
                "worker": "/api/workers/{worker_id}",
            },
        }

    @app.post("/api/workers/events", status_code=status.HTTP_201_CREATED)
    async def post_worker_event(event: WorkerEventCreate) -> dict[str, object]:
        _ensure_repo_path_allowed(settings, event.repo_path)
        try:
            detail = state_store.record_event(event)
        except WorkerAuthError as exc:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
        except InvalidTransitionError as exc:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

        return {"worker": jsonable_encoder(detail)}

    @app.post("/api/workers/{worker_id}/notes", status_code=status.HTTP_201_CREATED)
    async def post_worker_note(worker_id: str, note: WorkerNoteCreate) -> dict[str, object]:
        try:
            created_note = state_store.add_note(worker_id, note)
        except KeyError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="worker not found") from exc
        except WorkerAuthError as exc:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc

        return {"note": jsonable_encoder(created_note)}

    @app.get("/api/workers")
    async def list_workers() -> dict[str, object]:
        snapshot = state_store.snapshot()
        return {
            "workers": jsonable_encoder(snapshot.workers),
            "conflicts": jsonable_encoder(snapshot.conflicts),
            "totals": snapshot.totals,
        }

    @app.get("/api/workers/{worker_id}")
    async def get_worker(worker_id: str) -> dict[str, object]:
        try:
            worker = state_store.get_worker(worker_id)
        except KeyError as exc:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="worker not found") from exc
        return {"worker": jsonable_encoder(worker)}

    return app


def run() -> None:
    settings = Settings()
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    uvicorn.run(create_app(settings), host=settings.host, port=settings.port)


def _render_dashboard(
    request: Request,
    state_store: StateStore,
    settings: Settings,
) -> HTMLResponse:
    snapshot = state_store.snapshot()
    requested_worker_id = request.query_params.get("worker")
    selected_worker_id = pick_focus_worker(snapshot, requested_worker_id)
    selected_worker = (
        state_store.get_worker(selected_worker_id) if selected_worker_id is not None else None
    )
    worker_states = {
        worker.worker_id: worker_freshness(worker.updated_at)
        for worker in snapshot.workers
    }
    report_defaults = {
        "worker_id": selected_worker.worker_id if selected_worker else "",
        "repo_path": selected_worker.repo_path if selected_worker else "",
        "branch": selected_worker.branch if selected_worker and selected_worker.branch else "",
        "worktree": selected_worker.worktree if selected_worker and selected_worker.worktree else "",
        "owned_artifact": (
            selected_worker.owned_artifact
            if selected_worker and selected_worker.owned_artifact
            else ""
        ),
        "previous_phase": (
            selected_worker.phase.value if selected_worker else WorkerPhase.ASSIGNED.value
        ),
    }
    return templates.TemplateResponse(
        request,
        "index.html",
        {
            "app_name": settings.app_name,
            "environment": settings.default_environment,
            "workspace": settings.default_workspace,
            "data_dir": str(settings.data_dir),
            "allowed_repo_roots": [str(path) for path in settings.allowed_repo_roots],
            "phase_order": PHASE_ORDER,
            "phase_values": [phase.value for phase in WorkerPhase],
            "snapshot": snapshot,
            "selected_worker": selected_worker,
            "selected_worker_id": selected_worker_id,
            "selected_phase_notes": (
                grouped_phase_notes(selected_worker) if selected_worker is not None else []
            ),
            "worker_states": worker_states,
            "timestamp_format": format_timestamp,
            "report_status": request.query_params.get("report"),
            "report_error": request.query_params.get("error"),
            "report_defaults": report_defaults,
        },
    )


def _clean_optional(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _redirect_with_status(
    *,
    worker_id: str | None = None,
    report: str | None = None,
    error: str | None = None,
) -> RedirectResponse:
    query: dict[str, str] = {}
    if worker_id:
        query["worker"] = worker_id
    if report:
        query["report"] = report
    if error:
        query["error"] = error
    suffix = f"?{urlencode(query)}" if query else ""
    return RedirectResponse(url=f"/{suffix}#self-report", status_code=status.HTTP_303_SEE_OTHER)


def _ensure_repo_path_allowed(settings: Settings, repo_path: str) -> None:
    resolved = Path(repo_path).expanduser().resolve()
    for allowed_root in settings.allowed_repo_roots:
        try:
            resolved.relative_to(allowed_root)
            return
        except ValueError:
            continue

    allowed = ", ".join(str(path) for path in settings.allowed_repo_roots)
    raise HTTPException(
        status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        detail=f"repo_path must be inside allowed roots: {allowed}",
    )
