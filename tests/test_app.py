import json
from pathlib import Path

from fastapi.testclient import TestClient

from overlord.app import create_app
from overlord.config import Settings
from overlord.worker_status import build_parser, build_payload


def build_client(tmp_path: Path, token: str | None = None) -> TestClient:
    settings = Settings(
        OVERLORD_APP_NAME="Overlord Test",
        OVERLORD_DEFAULT_ENVIRONMENT="test",
        OVERLORD_DEFAULT_WORKSPACE="sandbox",
        OVERLORD_DATA_DIR=tmp_path,
        OVERLORD_WORKER_WRITE_TOKEN=token,
    )
    return TestClient(create_app(settings))


def test_healthz(tmp_path: Path) -> None:
    client = build_client(tmp_path)
    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_meta_endpoint_exposes_scaffold_defaults(tmp_path: Path) -> None:
    client = build_client(tmp_path)
    response = client.get("/api/meta")

    assert response.status_code == 200
    assert response.json()["mode"] == "agent-write-mvp"
    assert response.json()["defaults"]["environment"] == "test"
    assert response.json()["defaults"]["workspace"] == "sandbox"
    assert response.json()["workerWrite"]["endpoint"] == "/api/worker-status"


def test_homepage_renders_placeholder_ui(tmp_path: Path) -> None:
    client = build_client(tmp_path)
    response = client.get("/")

    assert response.status_code == 200
    assert "Scaffold ready" in response.text
    assert "Overlord Test" in response.text


def test_worker_status_event_is_accepted_and_persisted(tmp_path: Path) -> None:
    client = build_client(tmp_path)
    response = client.post(
        "/api/worker-status",
        json={
            "workerId": "worker-123",
            "status": "implementing",
            "previousStatus": "planned",
            "repoPath": str(tmp_path),
            "artifact": "overlord/worker_status.py",
            "note": "adding local cli helper",
            "nextStep": "run pytest",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "accepted"
    assert body["event"]["workerId"] == "worker-123"
    assert body["event"]["status"] == "implementing"

    events_path = tmp_path / "worker-status-events.jsonl"
    persisted = [json.loads(line) for line in events_path.read_text(encoding="utf-8").splitlines()]
    assert persisted[0]["workerId"] == "worker-123"
    assert persisted[0]["previousStatus"] == "planned"


def test_worker_status_requires_token_when_configured(tmp_path: Path) -> None:
    client = build_client(tmp_path, token="secret-token")
    response = client.post(
        "/api/worker-status",
        json={
            "workerId": "worker-123",
            "status": "scouting",
            "repoPath": str(tmp_path),
        },
    )

    assert response.status_code == 401
    assert response.json()["detail"] == "missing or invalid worker token"


def test_worker_status_accepts_valid_token(tmp_path: Path) -> None:
    client = build_client(tmp_path, token="secret-token")
    response = client.post(
        "/api/worker-status",
        headers={"Authorization": "Bearer secret-token"},
        json={
            "workerId": "worker-123",
            "status": "scouting",
            "repoPath": str(tmp_path),
        },
    )

    assert response.status_code == 200


def test_worker_status_cli_builds_payload_with_abs_repo_path(tmp_path: Path) -> None:
    parser = build_parser()
    args = parser.parse_args(
        [
            "--worker-id",
            "worker-123",
            "--status",
            "validating",
            "--repo-path",
            str(tmp_path),
            "--note",
            "running tests",
        ]
    )

    payload = build_payload(args)

    assert payload["workerId"] == "worker-123"
    assert payload["status"] == "validating"
    assert payload["repoPath"] == str(tmp_path.resolve())
    assert payload["note"] == "running tests"
