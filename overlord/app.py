from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from overlord.config import Settings
from overlord.dashboard import format_relative_time
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
        snapshot = state_store.snapshot()
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
                "snapshot": snapshot,
            },
        )

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
