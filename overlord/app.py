from pathlib import Path

from fastapi import FastAPI, Header, HTTPException, Request, status
import uvicorn
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from overlord.config import Settings
from overlord.worker_events import WorkerEventStore, WorkerStatusEventIn


BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()
    worker_event_store = WorkerEventStore(settings.data_dir)

    app = FastAPI(title=settings.app_name)
    app.state.settings = settings
    app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")

    @app.get("/", response_class=HTMLResponse)
    async def index(request: Request) -> HTMLResponse:
        return templates.TemplateResponse(
            request,
            "index.html",
            {
                "app_name": settings.app_name,
                "environment": settings.default_environment,
                "workspace": settings.default_workspace,
                "data_dir": str(settings.data_dir),
            },
        )

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/meta")
    async def meta() -> dict[str, object]:
        return {
            "app": settings.app_name,
            "mode": "agent-write-mvp",
            "defaults": {
                "environment": settings.default_environment,
                "workspace": settings.default_workspace,
                "dataDir": str(settings.data_dir),
            },
            "workerWrite": {
                "endpoint": "/api/worker-status",
                "tokenRequired": bool(settings.worker_write_token),
            },
            "nextSteps": [
                "define worker registry model",
                "add durable local state",
                "implement coordination flows",
            ],
        }

    @app.post("/api/worker-status")
    async def post_worker_status(
        event: WorkerStatusEventIn,
        authorization: str | None = Header(default=None),
    ) -> dict[str, object]:
        if settings.worker_write_token:
            expected = f"Bearer {settings.worker_write_token}"
            if authorization != expected:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="missing or invalid worker token",
                )

        record = worker_event_store.append_status_event(event)
        return {
            "status": "accepted",
            "event": record.model_dump(by_alias=True),
        }

    return app


def run() -> None:
    settings = Settings()
    settings.data_dir.mkdir(parents=True, exist_ok=True)
    uvicorn.run(create_app(settings), host=settings.host, port=settings.port)
